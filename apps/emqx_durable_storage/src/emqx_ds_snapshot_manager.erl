%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------
-module(emqx_ds_snapshot_manager).

-behaviour(gen_server).

%% API
-export([
    start_link/2,

    begin_read/2,
    read_chunk/3,

    begin_accept/2,
    accept_chunk/2,
    complete_accept/2
]).

%% `gen_server' API
-export([
    init/1,

    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-include_lib("snabbkaffe/include/trace.hrl").

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(via(SHARD_ID), {via, gproc, {n, l, {?MODULE, SHARD_ID}}}).

-type shard_id() :: {emqx_ds:db(), emqx_ds_replication_layer:shard_id()}.

-type state() :: #{
    rs := rs() | undefined,
    ws := ws() | undefined
}.

-type ra_state() :: emqx_ds_replication_layer:ra_state().

%% Read state.
-record(rs, {
    phase :: machine_state | storage_snapshot,
    started_at :: _Time :: integer(),
    state :: ra_state() | undefined,
    reader :: emqx_ds_storage_snapshot:reader() | undefined
}).

%% Write state.
-record(ws, {
    phase :: machine_state | storage_snapshot,
    started_at :: _Time :: integer(),
    dir :: file:filename(),
    meta :: ra_snapshot:meta(),
    state :: ra_state() | undefined,
    writer :: emqx_ds_storage_snapshot:writer() | undefined
}).

-type rs() :: #rs{}.
-type ws() :: #ws{}.

-type ext_rs() :: #{shard_id := shard_id()}.
-type ext_ws() :: #{phase := machine_state, ws := ws()} | #{shard_id := shard_id()}.

%% call/cast/info events
-record(start_reader, {rs :: rs()}).
-record(read_chunk, {size :: non_neg_integer()}).
-record(begin_accept, {ws :: ws()}).
-record(accept_chunk, {chunk :: binary()}).
-record(complete_accept, {chunk :: binary()}).

-export_type([ext_rs/0, ext_ws/0]).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> {ok, pid()}.
start_link(DB, ShardId) ->
    gen_server:start_link(?via({DB, ShardId}), ?MODULE, #{db => DB, shard_id => ShardId}, []).

-spec begin_read(_SnapshotDir :: file:filename(), _Context :: #{}) ->
    {ok, ra_snapshot:meta(), ext_rs()} | {error, _Reason :: term()}.
begin_read(Dir, _Context) ->
    RS0 = #rs{
        phase = machine_state,
        started_at = erlang:monotonic_time(millisecond)
    },
    case ra_log_snapshot:recover(Dir) of
        {ok, Meta, MachineState} ->
            ShardId = shard_id(MachineState),
            RS1 = RS0#rs{state = MachineState},
            ok = gen_server:call(?via(ShardId), #start_reader{rs = RS1}, infinity),
            ReadState = #{shard_id => ShardId},
            {ok, Meta, ReadState};
        Error ->
            Error
    end.

-spec read_chunk(ext_rs(), _Size :: non_neg_integer(), _SnapshotDir :: file:filename()) ->
    {ok, binary(), {next, ext_rs()} | last} | {error, _Reason :: term()}.
read_chunk(#{shard_id := ShardId} = ExtRS, Size, _Dir) ->
    case gen_server:call(?via(ShardId), #read_chunk{size = Size}, infinity) of
        {ok, Chunk, next} ->
            {ok, Chunk, {next, ExtRS}};
        {ok, Chunk, last} ->
            {ok, Chunk, last};
        {error, Reason} ->
            error(Reason)
    end.

-spec begin_accept(_SnapshotDir :: file:filename(), ra_snapshot:meta()) ->
    {ok, ext_ws()}.
begin_accept(Dir, Meta) ->
    WS = #ws{
        phase = machine_state,
        started_at = erlang:monotonic_time(millisecond),
        dir = Dir,
        meta = Meta
    },
    ExtWS = #{phase => machine_state, ws => WS},
    {ok, ExtWS}.

-spec accept_chunk(binary(), ext_ws()) ->
    {ok, ext_ws()} | {error, _Reason :: term()}.
accept_chunk(Chunk, ExtWS = #{shard_id := ShardId}) ->
    case gen_server:call(?via(ShardId), #accept_chunk{chunk = Chunk}, infinity) of
        ok ->
            {ok, ExtWS};
        {error, Reason} ->
            error(Reason)
    end;
accept_chunk(Chunk, _ExtWS = #{phase := machine_state, ws := WS0}) ->
    %% No shard id defined yet.
    MachineState = binary_to_term(Chunk),
    WS = WS0#ws{state = MachineState},
    ShardId = shard_id(MachineState),
    case gen_server:call(?via(ShardId), #begin_accept{ws = WS}, infinity) of
        ok ->
            %% Transferred `ws()` to manager; no need to keep it around
            NewExtWS = #{shard_id => ShardId},
            {ok, NewExtWS};
        {error, Reason} ->
            error(Reason)
    end.

-spec complete_accept(binary(), ext_ws()) -> ok | {error, ra_snapshot:file_err()}.
complete_accept(Chunk, _ExtWS = #{shard_id := ShardId}) ->
    gen_server:call(?via(ShardId), #complete_accept{chunk = Chunk}, infinity).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(#{db := DB, shard_id := ShardId}) ->
    logger:set_process_metadata(#{
        db => DB,
        shard_id => ShardId,
        domain => [ds, storage_layer, snapshot]
    }),
    clear_all_checkpoints(DB, ShardId),
    {ok, init_state()}.

%% Reader
handle_call(#start_reader{rs = RS0}, _From, State0) ->
    State = handle_start_reader(RS0, State0),
    {reply, ok, State};
handle_call(#read_chunk{size = Size}, _From, State0) ->
    {Reply, State} = handle_read_chunk(State0, Size),
    {reply, Reply, State};
%% Writer
handle_call(#begin_accept{ws = WS}, _From, State0) ->
    State = State0#{ws := start_snapshot_writer(WS)},
    {reply, ok, State};
handle_call(#accept_chunk{chunk = Chunk}, _From, State0) ->
    {Reply, State} = handle_accept_chunk(Chunk, State0),
    {reply, Reply, State};
handle_call(#complete_accept{chunk = Chunk}, _From, State0) ->
    {Reply, State} = handle_complete_accept(Chunk, State0),
    {reply, Reply, State};
handle_call(_Call, _From, State) ->
    {reply, {error, bad_call}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

-spec init_state() -> state().
init_state() ->
    #{
        rs => undefined,
        ws => undefined
    }.

clear_all_checkpoints(DB, ShardId) ->
    CheckpointBaseDir = emqx_ds_storage_layer:checkpoints_dir({DB, ShardId}),
    ok = filelib:ensure_path(CheckpointBaseDir),
    {ok, AllFiles} = file:list_dir(CheckpointBaseDir),
    CheckpointDirs = [Dir || Dir <- AllFiles, filelib:is_dir(Dir)],
    lists:foreach(
        fun(Dir) ->
            logger:debug(#{
                msg => "dsrepl_deleting_previous_snapshot",
                dir => Dir
            }),
            ok = file:del_dir_r(Dir)
        end,
        CheckpointDirs
    ).

%% Reader (leader state)

-spec handle_start_reader(rs(), state()) -> state().
handle_start_reader(RS0, State0) ->
    ShardId = shard_id(RS0),
    logger:info(#{
        msg => "dsrepl_snapshot_read_started",
        shard => ShardId
    }),
    {ok, SnapReader} = emqx_ds_storage_layer:take_snapshot(ShardId),
    RS = RS0#rs{reader = SnapReader},
    State0#{rs := RS}.

-spec handle_read_chunk(state(), _Size :: non_neg_integer()) ->
    {{ok, binary(), next | last} | {error, _Reason :: term()}, state()}.
handle_read_chunk(#{rs := RS0 = #rs{phase = machine_state, state = MachineState}} = State0, _Size) ->
    Chunk = term_to_binary(MachineState),
    RS = RS0#rs{phase = storage_snapshot},
    State = State0#{rs := RS},
    Reply = {ok, Chunk, next},
    {Reply, State};
handle_read_chunk(
    #{rs := RS0 = #rs{phase = storage_snapshot, reader = SnapReader0}} = State0, Size
) ->
    case emqx_ds_storage_snapshot:read_chunk(SnapReader0, Size) of
        {next, Chunk, SnapReader} ->
            RS = RS0#rs{reader = SnapReader},
            State = State0#{rs := RS},
            Reply = {ok, Chunk, next},
            {Reply, State};
        {last, Chunk, SnapReader} ->
            %% TODO: idempotence?
            ?tp(dsrepl_snapshot_read_complete, #{reader => SnapReader}),
            _ = complete_read(RS0#rs{reader = SnapReader}),
            State = State0#{rs := undefined},
            Reply = {ok, Chunk, last},
            {Reply, State};
        {error, Reason} ->
            ?tp(dsrepl_snapshot_read_error, #{reason => Reason, reader => SnapReader0}),
            _ = emqx_ds_storage_snapshot:release_reader(SnapReader0),
            State = State0#{rs := undefined},
            Reply = {error, Reason},
            {Reply, State}
    end.

complete_read(RS = #rs{reader = SnapReader, started_at = StartedAt}) ->
    _ = emqx_ds_storage_snapshot:release_reader(SnapReader),
    logger:info(#{
        msg => "dsrepl_snapshot_read_complete",
        shard => shard_id(RS),
        duration_ms => erlang:monotonic_time(millisecond) - StartedAt,
        read_bytes => emqx_ds_storage_snapshot:reader_info(bytes_read, SnapReader)
    }).

%% Writer (follower state)

-spec handle_accept_chunk(binary(), state()) ->
    {ok | {error, _Reason :: term()}, state()}.
handle_accept_chunk(
    Chunk, #{ws := WS0 = #ws{phase = storage_snapshot, writer = SnapWriter0}} = State0
) ->
    %% TODO: idempotence?
    case emqx_ds_storage_snapshot:write_chunk(SnapWriter0, Chunk) of
        {next, SnapWriter} ->
            WS = WS0#ws{writer = SnapWriter},
            State = State0#{ws := WS},
            {ok, State};
        {error, Reason} ->
            ?tp(dsrepl_snapshot_write_error, #{reason => Reason, writer => SnapWriter0}),
            _ = emqx_ds_storage_snapshot:abort_writer(SnapWriter0),
            State = State0#{ws := undefined},
            {{error, Reason}, State}
    end.

start_snapshot_writer(WS) ->
    ShardId = shard_id(WS),
    logger:info(#{
        msg => "dsrepl_snapshot_write_started",
        shard => ShardId
    }),
    _ = emqx_ds_builtin_db_sup:terminate_storage(ShardId),
    {ok, SnapWriter} = emqx_ds_storage_layer:accept_snapshot(ShardId),
    WS#ws{phase = storage_snapshot, writer = SnapWriter}.

-spec handle_complete_accept(binary(), state()) ->
    {ok | {error, ra_snapshot:file_err()}, state()}.
handle_complete_accept(
    Chunk, #{ws := WS0 = #ws{phase = storage_snapshot, writer = SnapWriter0}} = State0
) ->
    %% TODO: idempotence?
    case emqx_ds_storage_snapshot:write_chunk(SnapWriter0, Chunk) of
        {last, SnapWriter} ->
            ?tp(dsrepl_snapshot_write_complete, #{writer => SnapWriter}),
            _ = emqx_ds_storage_snapshot:release_writer(SnapWriter),
            Result = do_complete_accept(WS0#ws{writer = SnapWriter}),
            ?tp(dsrepl_snapshot_accepted, #{shard => shard_id(WS0)}),
            State = State0#{ws := undefined},
            {Result, State};
        {error, Reason} ->
            ?tp(dsrepl_snapshot_write_error, #{reason => Reason, writer => SnapWriter0}),
            _ = emqx_ds_storage_snapshot:abort_writer(SnapWriter0),
            State = State0#{ws := undefined},
            {{error, Reason}, State}
    end.

-spec do_complete_accept(ws()) -> ok | {error, ra_snapshot:file_err()}.
do_complete_accept(WS = #ws{started_at = StartedAt, writer = SnapWriter}) ->
    ShardId = shard_id(WS),
    logger:info(#{
        msg => "dsrepl_snapshot_read_complete",
        shard => ShardId,
        duration_ms => erlang:monotonic_time(millisecond) - StartedAt,
        bytes_written => emqx_ds_storage_snapshot:writer_info(bytes_written, SnapWriter)
    }),
    {ok, _} = emqx_ds_builtin_db_sup:restart_storage(ShardId),
    write_machine_snapshot(WS).

write_machine_snapshot(#ws{dir = Dir, meta = Meta, state = MachineState}) ->
    emqx_ds_replication_snapshot:write(Dir, Meta, MachineState).

%%

shard_id(#rs{state = MachineState}) ->
    shard_id(MachineState);
shard_id(#ws{state = MachineState}) ->
    shard_id(MachineState);
shard_id(MachineState) ->
    maps:get(db_shard, MachineState).
