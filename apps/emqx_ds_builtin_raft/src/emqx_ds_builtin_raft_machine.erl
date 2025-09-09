%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_machine).
-moduledoc """
Ra Machine implementation

This code decides how successfully replicated and committed log entries (e.g.
commands) are applied to the shard storage state. This state is actually comprised
logically of 2 parts:

1. RocksDB database managed through `emqx_ds_storage_layer`.

2. Machine state (`ra_state()`) that holds very minimal state needed to ensure
   higher-level semantics, most importantly strictly monotonic quasi-wallclock
   timestamp used to assign unique message timestamps to fulfill "append-only"
   guarantees.

There are few subtleties in how storage state is persisted and recovered.
When the shard recovers from a shutdown or crash, this is what usually happens:

1. Shard storage layer starts up the RocksDB database.
2. Ra recovers the Raft log.
3. Ra recovers the latest machine snapshot (`ra_state()`), taken at some point
   in time (`RaftIdx`).
4. Ra applies existing Raft log entries starting from `RaftIdx`.

While most of the time storage layer state, machine snapshot and log entries are
consistent with each other, there are situations when they are not. Namely:
 * RocksDB decides to flush memtables to disk by itself, which is unexpected but
   possible.
 * Lagging replica accepts a storage snapshot sourced from a RocksDB checkpoint,
   and RocksDB database is always implicitly flushed before checkpointing.
In both of those cases, the Raft log would contain entries that were already
applied from the point of view of the storage layer, and we must anticipate that.

The process running Ra machine also keeps auxiliary ephemeral state in the process
dictionary, see `?pd_ra_*` macrodefs for details.
""".

-behaviour(ra_machine).

%% API:
-export([
    add_generation/1,
    otx_new_leader/1,
    otx_commit/5,
    drop_generation/1,
    update_schema/3
]).

%% behavior callbacks:
-export([
    init/1,
    apply/3,
    tick/2,
    state_enter/2,
    snapshot_module/0
]).

%% internal exports:
-export([]).

-export_type([ra_state/0, ra_command/0]).

-include("emqx_ds_builtin_raft.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

%% keys:
-define(tag, 1).
%% OTX:
-define(commit_otx, 1).
-define(prev_serial, 2).
-define(serial, 3).
-define(batches, 4).
-define(otx_leader_pid, 5).
-define(otx_timestamp, 6).
%% Storage event:
-define(storage_event_payload, 2).
-define(now, 3).

%% Core state of the replication, i.e. the state of ra machine.
-type ra_state() :: #{
    %% Shard ID.
    db_shard := {emqx_ds:db(), emqx_ds:shard()},

    %% Map that stores last schema change id for each site, it is used
    %% to discard obsolete schema updates.
    last_schema_changes := #{emqx_dsch:site() => emqx_dsch:pending_id()},

    schema := emqx_ds_builtin_raft:db_schema(),

    %% Unique timestamp tracking real time closely.
    %% With microsecond granularity it should be nearly impossible for it to run
    %% too far ahead of the real time clock.
    latest := emqx_ds:time(),

    %% Transaction serial.
    tx_serial => emqx_ds_optimistic_tx:serial(),

    %% Pid of the OTX leader process (used to verify that transaction
    %% was initiated during the term of the correct leader):
    otx_leader_pid => pid() | undefined
}.

%% Commands. Each command is an entry in the replication log.
-type cmd_otx_new_leader() :: #{
    ?tag := new_otx_leader,
    pid := pid()
}.

-type cmd_commit_tx() :: #{
    ?tag := ?commit_otx,
    ?prev_serial := emqx_ds_optimistic_tx:serial(),
    ?serial := emqx_ds_optimistic_tx:serial(),
    ?otx_timestamp := emqx_ds:time(),
    ?batches := emqx_ds_optimistic_tx:batch(),
    ?otx_leader_pid := pid()
}.

-type cmd_update_schema() :: #{
    ?tag := update_schema,
    pending_id := emqx_dsch:pending_id(),
    originator := emqx_dsch:site(),
    schema := map()
}.

-type cmd_add_generation() :: #{
    ?tag := add_generation,
    since := emqx_ds:time()
}.

-type cmd_drop_generation() :: #{
    ?tag := drop_generation,
    generation := emqx_ds:generation()
}.

-type ra_command() ::
    cmd_otx_new_leader()
    | cmd_commit_tx()
    | cmd_update_schema()
    | cmd_add_generation()
    | cmd_drop_generation().

%% Index of the last yet unreleased Ra log entry.
-define(pd_ra_idx_need_release, '$emqx_ds_raft_idx_need_release').

%% Approximate number of bytes occupied by yet unreleased Ra log entries.
-define(pd_ra_bytes_need_release, '$emqx_ds_raft_bytes_need_release').

%% Cached value of the `append_only` DS DB configuration setting.
-define(pd_ra_force_monotonic, '$emqx_ds_raft_force_monotonic').

%% How often to release Raft logs?
%% Each time we written approximately this number of bytes.
%% Close to the RocksDB's default of 64 MiB.
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000_000).
%% ...Or at least each N log entries.
-define(RA_RELEASE_LOG_MIN_FREQ, 64_000).

-ifdef(TEST).
-undef(RA_RELEASE_LOG_APPROX_SIZE).
-undef(RA_RELEASE_LOG_MIN_FREQ).
-define(RA_RELEASE_LOG_APPROX_SIZE, 50_000).
-define(RA_RELEASE_LOG_MIN_FREQ, 1_000).
-endif.

%%================================================================================
%% API functions
%%================================================================================

-spec add_generation(emqx_ds:time()) -> cmd_add_generation().
add_generation(Since) when is_integer(Since) ->
    #{?tag => add_generation, since => Since}.

-spec update_schema(emqx_dsch:pending_id(), emqx_dsch:site(), emqx_ds_builtin_raft:db_schema()) ->
    cmd_update_schema().
update_schema(PendingId, Originator, NewSchema) when
    is_integer(PendingId), is_binary(Originator), is_map(NewSchema)
->
    #{
        ?tag => update_schema,
        pending_id => PendingId,
        originator => Originator,
        schema => NewSchema
    }.

-spec drop_generation(emqx_ds:generation()) -> cmd_drop_generation().
drop_generation(Gen) when is_integer(Gen) ->
    #{?tag => drop_generation, generation => Gen}.

-spec otx_new_leader(pid()) -> cmd_otx_new_leader().
otx_new_leader(Pid) when is_pid(Pid) ->
    #{?tag => new_otx_leader, pid => Pid}.

-spec otx_commit(
    emqx_ds_optimistic_tx:serial(),
    emqx_ds_optimistic_tx:serial(),
    emqx_ds:time(),
    emqx_ds_optimistic_tx:batch(),
    pid()
) -> cmd_commit_tx().
otx_commit(PrevSerial, Serial, Time, Batch, Leader) when
    is_integer(PrevSerial), is_integer(Serial), is_integer(Time), is_list(Batch), is_pid(Leader)
->
    #{
        ?tag => ?commit_otx,
        ?prev_serial => PrevSerial,
        ?serial => Serial,
        ?otx_timestamp => Time,
        ?batches => Batch,
        ?otx_leader_pid => Leader
    }.

%%================================================================================
%% behavior callbacks
%%================================================================================

-spec init(#{
    name := _,
    db := emqx_ds:db(),
    shard := emqx_ds:shard(),
    schema := emqx_ds_builtin_raft:db_schema()
}) -> ra_state().
init(#{db := DB, shard := Shard, schema := Schema}) ->
    #{
        db_shard => {DB, Shard},
        last_schema_changes => #{},
        schema => Schema,
        latest => 0,
        tx_serial => 0,
        otx_leader_pid => undefined
    }.

snapshot_module() ->
    emqx_ds_builtin_raft_server_snapshot.

-spec apply(ra_machine:command_meta_data(), ra_command(), ra_state()) ->
    {ra_state(), _Reply, _Effects}.
apply(
    RaftMeta,
    #{
        ?tag := ?commit_otx,
        ?prev_serial := SerCtl,
        ?serial := Serial,
        ?otx_timestamp := Timestamp,
        ?batches := Batches,
        ?otx_leader_pid := From
    },
    State0 = #{db_shard := DBShard, tx_serial := ExpectedSerial, otx_leader_pid := Leader}
) ->
    case From of
        Leader when SerCtl =:= ExpectedSerial ->
            case emqx_ds_storage_layer_ttv:commit_batch(DBShard, Batches, #{durable => false}) of
                ok ->
                    emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial),
                    State = State0#{tx_serial := Serial, latest := Timestamp},
                    Result = ok,
                    set_ts(DBShard, Timestamp + 1),
                    DispatchF = fun(Stream) ->
                        emqx_ds_beamformer:shard_event(DBShard, [Stream])
                    end,
                    emqx_ds_storage_layer_ttv:dispatch_events(DBShard, Batches, DispatchF),
                    Effects = try_release_log({Serial, length(Batches)}, RaftMeta, State);
                Err = ?err_unrec(_) ->
                    State = State0,
                    Result = Err,
                    Effects = []
            end;
        Leader ->
            %% Leader pid matches, but not the serial:
            State = State0,
            Result = ?err_unrec({serial_mismatch, SerCtl, ExpectedSerial}),
            Effects = [];
        _ ->
            %% Leader mismatch:
            State = State0,
            Result = ?err_unrec({not_the_leader, #{got => From, expect => Leader}}),
            Effects = []
    end,
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := add_generation, since := Since},
    #{db_shard := DBShard} = State
) ->
    ?tp(
        info,
        ds_ra_add_generation,
        #{
            shard => DBShard,
            since => Since
        }
    ),
    Result = emqx_ds_storage_layer:add_generation(DBShard, Since),
    emqx_ds_beamformer:generation_event(DBShard),
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, Result, Effects};
apply(
    RaftMeta,
    #{?tag := update_schema, pending_id := PendingId, originator := Site, schema := Schema},
    #{db_shard := DBShard, last_schema_changes := LSC, latest := Latest} = State0
) ->
    ?tp(
        notice,
        ds_ra_update_config,
        #{
            shard => DBShard,
            schema => Schema,
            originator => Site,
            pending_id => PendingId
        }
    ),
    State =
        case LSC of
            #{Site := NewerId} when NewerId >= PendingId ->
                %% This update has been already applied. Ignore
                %% it:
                State0;
            #{} ->
                ok = emqx_ds_storage_layer:update_config(DBShard, Latest, Schema),
                State0#{schema := Schema, last_schema_changes := LSC#{Site => PendingId}}
        end,
    Effects = release_log(RaftMeta, State),
    Effects =/= [] andalso ?tp(ds_ra_effects, #{effects => Effects, meta => RaftMeta}),
    {State, ok, Effects};
apply(
    _RaftMeta,
    #{?tag := drop_generation, generation := GenId},
    #{db_shard := DBShard} = State
) ->
    ?tp(
        info,
        ds_ra_drop_generation,
        #{
            shard => DBShard,
            generation => GenId
        }
    ),
    Result = emqx_ds_storage_layer:drop_slab(DBShard, GenId),
    {State, Result};
apply(
    _RaftMeta,
    #{
        ?tag := new_otx_leader,
        pid := Pid
    },
    State = #{db_shard := DBShard, tx_serial := Serial, latest := Timestamp}
) ->
    set_otx_leader(DBShard, Pid),
    Reply = {Serial, Timestamp},
    {State#{otx_leader_pid => Pid}, Reply}.

-spec tick(integer(), ra_state()) -> ra_machine:effects().
tick(_TimeMs, #{db_shard := _DBShard}) ->
    [].

-spec state_enter(ra_server:ra_state() | eol, ra_state()) -> ra_machine:effects().
state_enter(MemberState, State = #{db_shard := {DB, Shard}}) ->
    ?tp(
        debug,
        ds_ra_state_enter,
        State#{state => MemberState}
    ),
    emqx_ds_builtin_raft_metrics:rasrv_state_changed(DB, Shard, MemberState),
    set_cache(MemberState, State),
    _ =
        case MemberState of
            leader ->
                start_otx_leader(DB, Shard);
            _ ->
                emqx_ds_builtin_raft_db_sup:stop_shard_leader_sup(DB, Shard)
        end,
    [].

%%================================================================================
%% Internal exports
%%================================================================================

%%================================================================================
%% Internal functions
%%================================================================================

start_otx_leader(DB, Shard) ->
    ?tp_span(
        debug,
        dsrepl_start_otx_leader,
        #{db => DB, shard => Shard},
        emqx_ds_builtin_raft_db_sup:start_shard_leader_sup(DB, Shard)
    ).

set_cache(MemberState, State = #{db_shard := DBShard, latest := Latest}) when
    MemberState =:= leader; MemberState =:= follower
->
    set_ts(DBShard, Latest),
    case State of
        #{tx_serial := Serial} ->
            emqx_ds_storage_layer_ttv:set_read_tx_serial(DBShard, Serial);
        #{} ->
            ok
    end,
    case State of
        #{otx_leader_pid := Pid} ->
            set_otx_leader(DBShard, Pid);
        #{} ->
            ok
    end;
set_cache(_, _) ->
    ok.

-doc """
Set PID of the optimistic transaction leader at the time of the last
Raft log entry applied locally. Since log replication may be delayed,
this pid may belong to a process long gone, and the pid can be even
reclaimed by other process if the node had restarted. Because of that,
DON'T SEND MESSAGES to this pid.

This pid is used ONLY to verify that the transaction context has been
created during the term of the current leader.
""".
set_otx_leader({DB, Shard}, Pid) ->
    ?tp(info, dsrepl_set_otx_leader, #{db => DB, shard => Shard, pid => Pid}),
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_otx_leader_pid, Pid).

set_ts({DB, Shard}, TS) ->
    emqx_dsch:gvar_set(DB, Shard, ?gv_sc_replica, ?gv_timestamp, TS).

try_release_log({_N, BatchSize}, RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Because cursor release means storage flush (see
    %% `emqx_ds_builtin_raft_server_snapshot:write/3`), we should do that not too often
    %% (so the storage is happy with L0 SST sizes) and not too rarely (so we don't
    %% accumulate huge Raft logs).
    case inc_bytes_need_release(BatchSize) of
        AccSize when AccSize > ?RA_RELEASE_LOG_APPROX_SIZE ->
            release_log(RaftMeta, State);
        _NotYet ->
            case get_log_need_release(RaftMeta) of
                undefined ->
                    [];
                PrevIdx when CurrentIdx - PrevIdx > ?RA_RELEASE_LOG_MIN_FREQ ->
                    %% Release everything up to the last log entry, but only if there were
                    %% more than %% `?RA_RELEASE_LOG_MIN_FREQ` new entries since the last
                    %% release.
                    release_log(RaftMeta, State);
                _ ->
                    []
            end
    end.

release_log(RaftMeta = #{index := CurrentIdx}, State) ->
    %% NOTE
    %% Release everything up to the last log entry. This is important: any log entries
    %% following `CurrentIdx` should not contribute to `State` (that will be recovered
    %% from a snapshot).
    update_log_need_release(RaftMeta),
    reset_bytes_need_release(),
    {release_cursor, CurrentIdx, State}.

get_log_need_release(RaftMeta) ->
    case erlang:get(?pd_ra_idx_need_release) of
        undefined ->
            update_log_need_release(RaftMeta),
            undefined;
        LastIdx ->
            LastIdx
    end.

update_log_need_release(#{index := CurrentIdx}) ->
    erlang:put(?pd_ra_idx_need_release, CurrentIdx).

get_bytes_need_release() ->
    emqx_maybe:define(erlang:get(?pd_ra_bytes_need_release), 0).

inc_bytes_need_release(Size) ->
    Acc = get_bytes_need_release() + Size,
    erlang:put(?pd_ra_bytes_need_release, Acc),
    Acc.

reset_bytes_need_release() ->
    erlang:put(?pd_ra_bytes_need_release, 0).
