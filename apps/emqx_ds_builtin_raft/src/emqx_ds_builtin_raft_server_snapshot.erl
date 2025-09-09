%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ds_builtin_raft_server_snapshot).

-include_lib("snabbkaffe/include/trace.hrl").

-behaviour(ra_snapshot).
-export([
    prepare/2,
    write/4,
    sync/1,

    begin_read/2,
    read_chunk/3,

    begin_accept/2,
    accept_chunk/2,
    complete_accept/2,

    recover/1,
    validate/1,
    read_meta/1
]).

%% Read state.
-record(rs, {
    phase :: machine_state | storage_snapshot,
    started_at :: _Time :: integer(),
    state :: emqx_ds_builtin_raft_machine:ra_state() | undefined,
    reader :: emqx_ds_storage_snapshot:reader() | undefined
}).

%% Write state.
-record(ws, {
    phase :: machine_state | storage_snapshot,
    started_at :: _Time :: integer(),
    dir :: file:filename(),
    meta :: ra_snapshot:meta(),
    state :: emqx_ds_builtin_raft_machine:ra_state() | undefined,
    writer :: emqx_ds_storage_snapshot:writer() | undefined
}).

-type rs() :: #rs{}.
-type ws() :: #ws{}.

-type ra_state() :: emqx_ds_builtin_raft_machine:ra_state().

%% Writing a snapshot.
%% This process is exactly the same as writing a ra log snapshot: store the
%% log meta and the machine state in a single snapshot file.

-spec prepare(_RaftIndex, ra_state()) -> _State :: ra_state().
prepare(Index, State) ->
    ra_log_snapshot:prepare(Index, State).

-spec write(
    _SnapshotDir :: file:filename(),
    ra_snapshot:meta(),
    _State :: ra_state(),
    _Sync :: boolean()
) ->
    ok | {ok, _BytesWritten :: non_neg_integer()} | {error, ra_snapshot:file_err()}.
write(Dir, Meta, MachineState, Sync) ->
    ?tp(dsrepl_snapshot_write, #{meta => Meta, state => MachineState}),
    ok = emqx_ds_storage_layer:flush(shard_id(MachineState)),
    ra_log_snapshot:write(Dir, Meta, MachineState, Sync).

sync(Dir) ->
    ra_log_snapshot:sync(Dir).

%% Reading a snapshot.
%%
%% This is triggered by the leader when it finds out that a follower is
%% behind so much that there are no log segments covering the gap anymore.
%% This process, on the other hand, MUST involve reading the storage snapshot,
%% (in addition to the log snapshot) to reconstruct the storage state on the
%% target server.
%%
%% Currently, a snapshot reader is owned by a special "snapshot sender" process
%% spawned by the leader `ra` server, which sends chunks to the target server
%% in a tight loop. This process terminates under the following conditions:
%% 1. The snapshot is completely read and sent.
%% 2. Remote server fails to accept a chunk, either due to network failure (most
%%    likely) or a logic error (very unlikely).
%%
%% TODO
%% In the latter case the process terminates without the chance to clean up the
%% snapshot reader resource, which will cause the snapshot to linger indefinitely.
%% For better control over resources, observability, and niceties like flow
%% control and backpressure we need to move this into a dedicated process tree.

-spec begin_read(_SnapshotDir :: file:filename(), _Context :: #{}) ->
    {ok, ra_snapshot:meta(), rs()} | {error, _Reason :: term()}.
begin_read(Dir, _Context) ->
    RS = #rs{
        phase = machine_state,
        started_at = erlang:monotonic_time(millisecond)
    },
    case ra_log_snapshot:recover(Dir) of
        {ok, Meta, MachineState} ->
            start_snapshot_reader(Meta, RS#rs{state = MachineState});
        Error ->
            Error
    end.

start_snapshot_reader(Meta, RS) ->
    ShardId = shard_id(RS),
    {ok, SnapReader} = emqx_ds_storage_layer:take_snapshot(ShardId),
    emqx_ds_builtin_raft_metrics:snapshot_reader_started(ShardId),
    ?tp(info, "dsrepl_snapshot_read_started", #{shard => ShardId}),
    {ok, Meta, RS#rs{reader = SnapReader}}.

-spec read_chunk(rs(), _Size :: non_neg_integer(), _SnapshotDir :: file:filename()) ->
    {ok, binary(), {next, rs()} | last} | {error, _Reason :: term()}.
read_chunk(RS = #rs{phase = machine_state, state = MachineState}, _Size, _Dir) ->
    Chunk = term_to_binary(MachineState),
    emqx_ds_builtin_raft_metrics:snapshot_chunk_read(shard_id(MachineState), Chunk),
    {ok, Chunk, {next, RS#rs{phase = storage_snapshot}}};
read_chunk(RS = #rs{phase = storage_snapshot, reader = SnapReader0}, Size, _Dir) ->
    ShardId = shard_id(RS),
    case emqx_ds_storage_snapshot:read_chunk(SnapReader0, Size) of
        {next, Chunk, SnapReader} ->
            ?tp(dsrepl_snapshot_read, #{shard => ShardId, reader => SnapReader, last => false}),
            emqx_ds_builtin_raft_metrics:snapshot_chunk_read(ShardId, Chunk),
            {ok, Chunk, {next, RS#rs{reader = SnapReader}}};
        {last, Chunk, SnapReader} ->
            %% TODO: idempotence?
            ?tp(dsrepl_snapshot_read, #{shard => ShardId, reader => SnapReader, last => true}),
            emqx_ds_builtin_raft_metrics:snapshot_chunk_read(ShardId, Chunk),
            _ = complete_read(RS#rs{reader = SnapReader}),
            {ok, Chunk, last};
        {error, Reason} ->
            ?tp(dsrepl_snapshot_read_error, #{reason => Reason, reader => SnapReader0}),
            emqx_ds_builtin_raft_metrics:snapshot_reader_error(ShardId, Reason),
            _ = emqx_ds_storage_snapshot:release_reader(SnapReader0),
            error(Reason)
    end.

complete_read(RS = #rs{reader = SnapReader, started_at = StartedAt}) ->
    ShardId = shard_id(RS),
    _ = emqx_ds_storage_snapshot:release_reader(SnapReader),
    ?tp(info, "dsrepl_snapshot_read_complete", #{
        shard => ShardId,
        duration_ms => erlang:monotonic_time(millisecond) - StartedAt,
        read_bytes => emqx_ds_storage_snapshot:reader_info(bytes_read, SnapReader)
    }),
    emqx_ds_builtin_raft_metrics:snapshot_reader_complete(ShardId),
    ok.

%% Accepting a snapshot.
%%
%% This process is triggered by the target server, when the leader finds out
%% that the target server is severely lagging behind. This is receiving side of
%% `begin_read/2` and `read_chunk/3`.
%%
%% Currently, a snapshot writer is owned by the follower `ra` server process
%% residing in dedicated `receive_snapshot` state. This process reverts back
%% to the regular `follower` state under the following conditions:
%% 1. The snapshot is completely accepted, and the machine state is recovered.
%% 2. The process times out waiting for the next chunk.
%% 3. The process encounters a logic error (very unlikely).
%%
%% TODO
%% In the latter cases, the snapshot writer will not have a chance to clean up.
%% For better control over resources, observability, and niceties like flow
%% control and backpressure we need to move this into a dedicated process tree.

-spec begin_accept(_SnapshotDir :: file:filename(), ra_snapshot:meta()) ->
    {ok, ws()}.
begin_accept(Dir, Meta) ->
    ?tp(dsrepl_snapshot_accept_started, #{meta => Meta}),
    WS = #ws{
        phase = machine_state,
        started_at = erlang:monotonic_time(millisecond),
        dir = Dir,
        meta = Meta
    },
    {ok, WS}.

-spec accept_chunk(binary(), ws()) ->
    {ok, ws()} | {error, _Reason :: term()}.
accept_chunk(Chunk, WS = #ws{phase = machine_state}) ->
    start_snapshot_writer(Chunk, WS);
accept_chunk(Chunk, WS = #ws{phase = storage_snapshot, writer = SnapWriter0}) ->
    ShardId = shard_id(WS),
    case emqx_ds_storage_snapshot:write_chunk(SnapWriter0, Chunk) of
        {next, SnapWriter} ->
            ?tp(dsrepl_snapshot_write, #{shard => ShardId, writer => SnapWriter, last => false}),
            emqx_ds_builtin_raft_metrics:snapshot_chunk_written(ShardId, Chunk),
            {ok, WS#ws{writer = SnapWriter}};
        {error, Reason} ->
            ?tp(dsrepl_snapshot_write_error, #{
                shard => ShardId,
                reason => Reason,
                writer => SnapWriter0
            }),
            emqx_ds_builtin_raft_metrics:snapshot_writer_error(ShardId, Reason),
            _ = emqx_ds_storage_snapshot:abort_writer(SnapWriter0),
            error(Reason)
    end.

start_snapshot_writer(Chunk, WS) ->
    MachineState = binary_to_term(Chunk),
    ShardId = shard_id(MachineState),
    ?tp(info, "dsrepl_snapshot_write_started", #{shard => ShardId}),
    emqx_ds_builtin_raft_metrics:snapshot_writer_started(ShardId),
    _ = emqx_ds_builtin_raft_db_sup:terminate_storage(ShardId),
    {ok, SnapWriter} = emqx_ds_storage_layer:accept_snapshot(ShardId),
    emqx_ds_builtin_raft_metrics:snapshot_chunk_written(ShardId, Chunk),
    {ok, WS#ws{phase = storage_snapshot, state = MachineState, writer = SnapWriter}}.

-spec complete_accept(ws()) -> ok | {error, ra_snapshot:file_err()}.
complete_accept(Chunk, WS = #ws{phase = storage_snapshot, writer = SnapWriter0}) ->
    ShardId = shard_id(WS),
    case emqx_ds_storage_snapshot:write_chunk(SnapWriter0, Chunk) of
        {last, SnapWriter} ->
            ?tp(dsrepl_snapshot_write, #{shard => ShardId, writer => SnapWriter, last => true}),
            emqx_ds_builtin_raft_metrics:snapshot_chunk_written(ShardId, Chunk),
            _ = emqx_ds_storage_snapshot:release_writer(SnapWriter),
            Result = complete_accept(WS#ws{writer = SnapWriter}),
            ?tp(dsrepl_snapshot_accepted, #{shard => ShardId, state => WS#ws.state}),
            Result;
        {error, Reason} ->
            ?tp(dsrepl_snapshot_write_error, #{
                shard => shard_id(WS),
                reason => Reason,
                writer => SnapWriter0
            }),
            emqx_ds_builtin_raft_metrics:snapshot_writer_error(ShardId, Reason),
            _ = emqx_ds_storage_snapshot:abort_writer(SnapWriter0),
            error(Reason)
    end.

complete_accept(WS = #ws{started_at = StartedAt, writer = SnapWriter}) ->
    ShardId = shard_id(WS),
    ?tp(info, "dsrepl_snapshot_write_complete", #{
        shard => ShardId,
        duration_ms => erlang:monotonic_time(millisecond) - StartedAt,
        bytes_written => emqx_ds_storage_snapshot:writer_info(bytes_written, SnapWriter)
    }),
    {ok, _} = emqx_ds_builtin_raft_db_sup:restart_storage(ShardId),
    write_machine_snapshot(WS).

write_machine_snapshot(#ws{dir = Dir, meta = Meta, state = MachineState}) ->
    {ok, _Bytes} = ra_log_snapshot:write(Dir, Meta, MachineState, _Sync = false),
    emqx_ds_builtin_raft_metrics:snapshot_writer_complete(shard_id(MachineState)),
    ok.

%% Restoring machine state from a snapshot.
%% This is equivalent to restoring from a log snapshot.

-spec recover(_SnapshotDir :: file:filename()) ->
    {ok, ra_snapshot:meta(), ra_state()} | {error, _Reason}.
recover(Dir) ->
    %% TODO: Verify that storage layer is online?
    ra_log_snapshot:recover(Dir).

-spec validate(_SnapshotDir :: file:filename()) ->
    ok | {error, _Reason}.
validate(Dir) ->
    ra_log_snapshot:validate(Dir).

-spec read_meta(_SnapshotDir :: file:filename()) ->
    {ok, ra_snapshot:meta()} | {error, _Reason}.
read_meta(Dir) ->
    ra_log_snapshot:read_meta(Dir).

shard_id(#rs{state = MachineState}) ->
    shard_id(MachineState);
shard_id(#ws{state = MachineState}) ->
    shard_id(MachineState);
shard_id(MachineState) ->
    maps:get(db_shard, MachineState).
