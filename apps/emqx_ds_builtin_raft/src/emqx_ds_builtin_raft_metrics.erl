%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_builtin_raft_metrics).

%% DS-facing API:
-export([
    child_spec/0,
    init_local_shard/2
]).

%% Events:
-export([
    shard_transition_started/3,
    shard_transition_complete/3,
    shard_transition_skipped/3,
    shard_transition_crashed/3,
    shard_transition_error/3
]).
-export([
    snapshot_reader_started/1,
    snapshot_chunk_read/2,
    snapshot_reader_complete/1,
    snapshot_reader_error/2,
    snapshot_writer_started/1,
    snapshot_chunk_written/2,
    snapshot_writer_complete/1,
    snapshot_writer_error/2
]).
-export([
    rasrv_state_changed/3
]).

%% Cluster-wide metrics & metadata:
-export([
    cluster_meta/0,
    cluster/0,
    dbs_meta/0,
    dbs/0,
    db/1,
    shards_meta/0,
    shards/0,
    shard/2
]).

%% Local (node-wide) metrics & metadata:
-export([
    local_dbs_meta/0,
    local_dbs/1,
    local_shards_meta/0,
    local_shards/1
]).

-type metric_type() :: gauge | counter.
-type metric_meta() :: {_Name :: atom(), metric_type(), _Desc :: iodata()}.

%% Single metric:
%% Compatible with what `prometheus` / `emqx_prometheus` expects.
-type metric() :: {[label()], _Point :: number()}.
-type label() :: {_Name :: atom(), _Value :: atom() | iodata()}.

%% Set of metrics:
-type metrics() :: #{atom() => [metric()]}.

%%================================================================================
%% Type declarations
%%================================================================================

-define(WORKER, ?MODULE).

-define(MID_SHARD(DB, SHARD), <<(atom_to_binary(DB))/binary, "/", (SHARD)/binary>>).

-define(rasrv_state_changed__candidate, 'rasrv.state_changed.candidate').
-define(rasrv_state_changed__follower, 'rasrv.state_changed.follower').
-define(rasrv_state_changed__leader, 'rasrv.state_changed.leader').
-define(rasrv_started, 'rasrv.started').
-define(rasrv_terminated, 'rasrv.terminated').

-define(shard_transitions__started__add, 'transitions.started.add').
-define(shard_transitions__started__del, 'transitions.started.del').
-define(shard_transitions__completed__add, 'transitions.completed.add').
-define(shard_transitions__completed__del, 'transitions.completed.del').
-define(shard_transitions__skipped__add, 'transitions.skipped.add').
-define(shard_transitions__skipped__del, 'transitions.skipped.del').
-define(shard_transitions__crashed__add, 'transitions.crashed.add').
-define(shard_transitions__crashed__del, 'transitions.crashed.del').

-define(snap_reads__started, 'snapshot_reader.started').
-define(snap_read__chunks, 'snapshot_reader.chunks').
-define(snap_read__chunk_bs, 'snapshot_reader.chunks.bytes').
-define(snap_reads__completed, 'snapshot_reader.completed').
-define(snap_read__errors, 'snapshot_reader.errors').
-define(snap_writes__started, 'snapshot_writer.started').
-define(snap_write__chunks, 'snapshot_writer.chunks').
-define(snap_write__chunk_bs, 'snapshot_writer.chunks.bytes').
-define(snap_writes__completed, 'snapshot_writer.completed').
-define(snap_write__errors, 'snapshot_writer.errors').

-define(shard_transition_errors, 'transitions_errors').

-define(LOCAL_SHARD_METRICS,
    ?RASRV_STATE_METRICS ++
        ?RASRV_LIFECYCLE_METRICS ++
        ?SHARD_TRANSITIONS_METRICS ++
        ?SHARD_TRANSITION_ERRORS_METRICS ++
        ?SNAPSHOT_READS_METRICS ++
        ?SNAPSHOT_WRITES_METRICS ++
        ?SNAPSHOT_TRANSFER_METRICS
).
-define(RASRV_STATE_METRICS, [
    {counter, ?rasrv_state_changed__candidate, [{state, candidate}]},
    {counter, ?rasrv_state_changed__follower, [{state, follower}]},
    {counter, ?rasrv_state_changed__leader, [{state, leader}]}
]).
-define(RASRV_LIFECYCLE_METRICS, [
    {counter, ?rasrv_started, []},
    {counter, ?rasrv_terminated, []}
]).
-define(SHARD_TRANSITIONS_METRICS, [
    {counter, ?shard_transitions__started__add, [{type, add}, {status, started}]},
    {counter, ?shard_transitions__started__del, [{type, del}, {status, started}]},
    {counter, ?shard_transitions__completed__add, [{type, add}, {status, completed}]},
    {counter, ?shard_transitions__completed__del, [{type, del}, {status, completed}]},
    {counter, ?shard_transitions__skipped__add, [{type, add}, {status, skipped}]},
    {counter, ?shard_transitions__skipped__del, [{type, del}, {status, skipped}]},
    {counter, ?shard_transitions__crashed__add, [{type, add}, {status, crashed}]},
    {counter, ?shard_transitions__crashed__del, [{type, del}, {status, crashed}]}
]).
-define(SHARD_TRANSITION_ERRORS_METRICS, [
    {counter, ?shard_transition_errors, []}
]).
-define(SNAPSHOT_TRANSFER_METRICS, [
    {counter, ?snap_read__chunks, []},
    {counter, ?snap_read__chunk_bs, []},
    {counter, ?snap_read__errors, []},
    {counter, ?snap_write__chunks, []},
    {counter, ?snap_write__chunk_bs, []},
    {counter, ?snap_write__errors, []}
]).
-define(SNAPSHOT_READS_METRICS, [
    {counter, ?snap_reads__started, [{status, started}]},
    {counter, ?snap_reads__completed, [{status, completed}]}
]).
-define(SNAPSHOT_WRITES_METRICS, [
    {counter, ?snap_writes__started, [{status, started}]},
    {counter, ?snap_writes__completed, [{status, completed}]}
]).

-define(CATCH(BODY),
    try
        BODY
    catch
        _:_ -> ok
    end
).

%%

-spec child_spec() -> supervisor:child_spec().
child_spec() ->
    emqx_metrics_worker:child_spec(?MODULE, ?WORKER, []).

init_local_shard(DB, Shard) ->
    Metrics = [{Type, Name} || {Type, Name, _} <- ?LOCAL_SHARD_METRICS],
    emqx_metrics_worker:create_metrics(?WORKER, ?MID_SHARD(DB, Shard), Metrics).

%%

rasrv_state_changed(DB, Shard, RaState) ->
    case RaState of
        recover ->
            inc_shard_metric(DB, Shard, ?rasrv_started);
        candidate ->
            inc_shard_metric(DB, Shard, ?rasrv_state_changed__candidate);
        follower ->
            inc_shard_metric(DB, Shard, ?rasrv_state_changed__follower);
        leader ->
            inc_shard_metric(DB, Shard, ?rasrv_state_changed__leader);
        terminating_leader ->
            inc_shard_metric(DB, Shard, ?rasrv_terminated);
        terminating_follower ->
            inc_shard_metric(DB, Shard, ?rasrv_terminated);
        eol ->
            %% NOTE: Not really expected during normal operations.
            ok;
        _ ->
            ok
    end.

%%

shard_transition_started(DB, Shard, {add, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__started__add);
shard_transition_started(DB, Shard, {del, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__started__del).

shard_transition_complete(DB, Shard, {add, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__completed__add);
shard_transition_complete(DB, Shard, {del, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__completed__del).

shard_transition_skipped(DB, Shard, {add, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__skipped__add);
shard_transition_skipped(DB, Shard, {del, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__skipped__del).

shard_transition_crashed(DB, Shard, {add, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__crashed__add);
shard_transition_crashed(DB, Shard, {del, _}) ->
    inc_shard_metric(DB, Shard, ?shard_transitions__crashed__del).

shard_transition_error(DB, Shard, _Transition) ->
    inc_shard_metric(DB, Shard, ?shard_transition_errors).

%%

snapshot_reader_started({DB, Shard}) ->
    inc_shard_metric(DB, Shard, ?snap_reads__started).

snapshot_chunk_read({DB, Shard}, Chunk) ->
    MID = ?MID_SHARD(DB, Shard),
    ?CATCH(emqx_metrics_worker:inc(?WORKER, MID, ?snap_read__chunks)),
    ?CATCH(emqx_metrics_worker:inc(?WORKER, MID, ?snap_read__chunk_bs, byte_size(Chunk))).

snapshot_reader_complete({DB, Shard}) ->
    inc_shard_metric(DB, Shard, ?snap_reads__completed).

snapshot_reader_error({DB, Shard}, _Reason) ->
    inc_shard_metric(DB, Shard, ?snap_read__errors).

snapshot_writer_started({DB, Shard}) ->
    inc_shard_metric(DB, Shard, ?snap_writes__started).

snapshot_chunk_written({DB, Shard}, Chunk) ->
    MID = ?MID_SHARD(DB, Shard),
    ?CATCH(emqx_metrics_worker:inc(?WORKER, MID, ?snap_write__chunks)),
    ?CATCH(emqx_metrics_worker:inc(?WORKER, MID, ?snap_write__chunk_bs, byte_size(Chunk))).

snapshot_writer_complete({DB, Shard}) ->
    inc_shard_metric(DB, Shard, ?snap_writes__completed).

snapshot_writer_error({DB, Shard}, _Reason) ->
    inc_shard_metric(DB, Shard, ?snap_write__errors).

inc_shard_metric(DB, Shard, Counter) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, ?MID_SHARD(DB, Shard), Counter)).

%%

-spec cluster_meta() -> [metric_meta()].
cluster_meta() ->
    [
        {cluster_sites_num, gauge, <<"Number of sites in the DS cluster.">>}
    ].

-spec cluster() -> metrics().
cluster() ->
    #{
        cluster_sites_num => [sites_num(any), sites_num(lost)]
    }.

sites_num(any) ->
    {[{status, any}], length(emqx_ds_builtin_raft_meta:sites())};
sites_num(lost) ->
    {[{status, lost}], length(emqx_ds_builtin_raft_meta:sites(lost))}.

%%

-spec dbs_meta() -> [metric_meta()].
dbs_meta() ->
    [
        {db_shards_num, gauge, <<"Number of shards DB is split into.">>},
        {db_sites_num, gauge, <<"Number of current / assigned sites a DB is replicated across.">>}
    ].

%% Cluster-wide metrics of all known DBs.
-spec dbs() -> metrics().
dbs() ->
    lists:foldl(
        fun(DB, Acc) -> merge_metrics(db(DB), Acc) end,
        #{},
        emqx_ds_builtin_raft_meta:dbs()
    ).

-spec db(emqx_ds:db()) -> metrics().
db(DB) ->
    Labels = [{db, DB}],
    #{
        db_shards_num => [db_shards_num(DB, Labels)],
        db_sites_num => [db_sites_num(Status, DB, Labels) || Status <- [current, assigned]]
    }.

db_shards_num(DB, Ls) ->
    {Ls, length(emqx_ds_builtin_raft_meta:shards(DB))}.

db_sites_num(current, DB, Ls) ->
    {[{status, current} | Ls], length(emqx_ds_builtin_raft_meta:db_sites(DB))};
db_sites_num(assigned, DB, Ls) ->
    {[{status, assigned} | Ls], length(emqx_ds_builtin_raft_meta:db_target_sites(DB))}.

%%

-spec shards_meta() -> [metric_meta()].
shards_meta() ->
    [
        {shard_replication_factor, gauge, <<"Number of replicas in a DB shard replica set.">>},
        {shard_transition_queue_len, gauge,
            <<"Number of pending replica set transitions for a DB shard.">>}
    ].

%% Cluster-wide metrics of each DB's each shard.
-spec shards() -> metrics().
shards() ->
    gather_metrics(fun shards/1, emqx_ds_builtin_raft_meta:dbs()).

-spec shards(emqx_ds:db()) -> metrics().
shards(DB) ->
    gather_metrics(fun(Shard) -> shard(DB, Shard) end, emqx_ds_builtin_raft_meta:shards(DB)).

-spec shard(emqx_ds:db(), emqx_ds:shard()) -> metrics().
shard(DB, Shard) ->
    Labels = [{db, DB}, {shard, Shard}],
    #{
        shard_replication_factor => shard_replication_factor(DB, Shard, Labels),
        shard_transition_queue_len => shard_transition_queue_lengths(DB, Shard, Labels)
    }.

shard_replication_factor(DB, Shard, Ls) ->
    RS = emqx_maybe:define(emqx_ds_builtin_raft_meta:replica_set(DB, Shard), []),
    [{Ls, length(RS)}].

shard_transition_queue_lengths(DB, Shard, Ls) ->
    Transitions = emqx_maybe:define(
        emqx_ds_builtin_raft_meta:replica_set_transitions(DB, Shard),
        []
    ),
    [
        {[{type, add} | Ls], length([S || {add, S} <- Transitions])},
        {[{type, del} | Ls], length([S || {del, S} <- Transitions])}
    ].

%%

-spec local_dbs_meta() -> [metric_meta()].
local_dbs_meta() ->
    [
        {db_shards_online_num, gauge, <<"Number of DB shards actively mananged on this node.">>}
    ].

-spec local_dbs(_Labels0 :: [label()]) -> metrics().
local_dbs(Labels) ->
    gather_metrics(
        fun(DB) -> local_db(DB, Labels) end,
        emqx_ds_builtin_raft_db_sup:which_dbs()
    ).

-spec local_db(emqx_ds:db(), _Labels0 :: [label()]) -> metrics().
local_db(DB, Labels0) ->
    Labels = [{db, DB} | Labels0],
    #{
        db_shards_online_num => db_shards_online(DB, Labels)
    }.

db_shards_online(DB, Ls) ->
    ShardsOnline = emqx_ds_builtin_raft_db_sup:which_shards(DB),
    [{Ls, length(ShardsOnline)}].

-spec local_shards_meta() -> [metric_meta()].
local_shards_meta() ->
    [
        %% Replication:
        {current_timestamp_us, gauge, <<
            "Latest operation timestamp currently replicated by a shard server (us)."
        >>},
        {rasrvs_started, counter, <<
            "Counts number of times a shard Raft server have been started."
        >>},
        {rasrvs_terminated, counter, <<
            "Counts number of times a shard Raft server has been terminated."
        >>},
        {rasrv_state_changes, counter, <<
            "Counts number of times Raft server turned into canidate / follower / leader."
        >>},
        {rasrv_commands, counter, <<
            "Counts number of commands received by a shard server as Raft leader."
        >>},
        {rasrv_replication_msgs, counter, <<
            "Counts number of (sent in total / dropped due to unreachability) "
            "replication protocol messages."
        >>},
        {rasrv_term, gauge, <<"Current Raft term, as seen by a shard server.">>},
        {rasrv_index, gauge, <<
            "Current commit / last applied / last fully written and fsynced / latest "
            "snapshot Raft index."
        >>},
        {rasrv_snapshot_writes, counter, <<
            "Counts number of times a shard server written Raft machine snapshot to disk."
        >>},
        {rasrv_commit_latency_ms, gauge, <<
            "Latest observed approximate time taken for an entry written to the Raft "
            "log to be committed (ms)."
        >>},
        %% Shard replica set transitions:
        {shard_transitions, counter, <<
            "Counts number of started / completed / skipped / crashed replica set "
            "transitions of a DB shard."
        >>},
        {shard_transition_errors, counter, <<
            "Counts number of transient errors occured during orchestration of "
            "replica set transitions of a DB shard."
        >>},
        %% Shard snapshot transfers:
        {snapshot_reads, counter, <<
            "Counts number of started / completed snapshot reads for a DB shard, "
            "when a shard was the source of snapshot replication."
        >>},
        {snapshot_read_errors, counter, <<
            "Counts number of errors occured during reading snapshot on source DB "
            "shard, which caused snapshot replication to be aborted."
        >>},
        {snapshot_read_chunks, counter, <<
            "Counts number of individual chunks read on source DB shard, and later "
            "transferred to the recepient."
        >>},
        {snapshot_read_chunk_bytes, counter, <<
            "Counts number of bytes read as chunks on source DB shard."
        >>},
        {snapshot_writes, counter, <<
            "Counts number of started / completed snapshot writes for a DB shard, "
            "when a shard was the recepient of snapshot replication."
        >>},
        {snapshot_write_errors, counter, <<
            "Counts number of errors occured during writing snapshot to recepient "
            "DB shard, which caused snapshot replication to be aborted."
        >>},
        {snapshot_write_chunks, counter, <<
            "Counts number of individual chunks received from source DB shard, and "
            "written on the recepient."
        >>},
        {snapshot_write_chunk_bytes, counter, <<
            "Counts number of bytes written as chunks on recepient DB shard."
        >>}
    ].

%% Node-local metrics of each opened DB's each shard.
-spec local_shards(_Labels0 :: [label()]) -> metrics().
local_shards(Labels) ->
    gather_metrics(
        fun(DB) -> local_shards(DB, Labels) end,
        emqx_ds_builtin_raft_db_sup:which_dbs()
    ).

-spec local_shards(emqx_ds:db(), _Labels0 :: [label()]) -> metrics().
local_shards(DB, Labels) ->
    Shards = emqx_ds_builtin_raft_meta:shards(DB),
    ShardsActive = emqx_ds_builtin_raft_db_sup:which_shards(DB),
    gather_metrics(
        fun(Shard) ->
            local_shard(DB, Shard, lists:member(Shard, ShardsActive), Labels)
        end,
        Shards
    ).

-spec local_shard(emqx_ds:db(), emqx_ds:shard(), boolean(), [label()]) ->
    metrics().
local_shard(DB, Shard, IsActive, Labels0) ->
    Labels = [{db, DB}, {shard, Shard} | Labels0],
    Counters = emqx_metrics_worker:get_counters(?WORKER, ?MID_SHARD(DB, Shard)),
    Metrics0 = #{
        shard_transitions => shard_transitions(Counters, Labels),
        shard_transition_errors => [metric(?shard_transition_errors, Counters, Labels)]
    },
    case IsActive of
        %% Report following metrics only for shards operating on the node:
        true ->
            Metrics1 = Metrics0#{
                current_timestamp_us => current_timestamp(DB, Shard, Labels)
            },
            Metrics2 = rasrv_lifecycle(Counters, Labels, Metrics1),
            Metrics3 = rasrv_raft_metrics(DB, Shard, Labels, Metrics2),
            Metrics = snapshot_transfers(Counters, Labels, Metrics3),
            Metrics;
        false ->
            Metrics0
    end.

current_timestamp(DB, Shard, Ls) ->
    [{Ls, emqx_ds_builtin_raft:current_timestamp(DB, Shard)}].

rasrv_lifecycle(Counters, Ls, Acc) ->
    Acc#{
        rasrvs_started => [metric(?rasrv_started, Counters, Ls)],
        rasrvs_terminated => [metric(?rasrv_terminated, Counters, Ls)],
        rasrv_state_changes => metrics(?RASRV_STATE_METRICS, Counters, Ls)
    }.

rasrv_raft_metrics(DB, Shard, Ls, Acc) ->
    Server = emqx_ds_builtin_raft_shard:local_server(DB, Shard),
    SMs = emqx_maybe:define(emqx_ds_builtin_raft_shard:server_metrics(Server), #{}),
    Acc#{
        rasrv_commands => [metric(commands, SMs, Ls)],
        rasrv_replication_msgs => [
            metric(msgs_sent, SMs, [{kind, sent} | Ls]),
            metric(dropped_sends, SMs, [{kind, dropped} | Ls])
        ],
        rasrv_term => [metric(term, SMs, Ls)],
        rasrv_index => [
            metric(commit_index, SMs, [{kind, commit} | Ls]),
            metric(last_applied, SMs, [{kind, last_applied} | Ls]),
            metric(last_written_index, SMs, [{kind, last_written} | Ls]),
            metric(snapshot_index, SMs, [{kind, snapshot} | Ls])
        ],
        rasrv_snapshot_writes => [metric(snapshots_written, SMs, Ls)],
        rasrv_commit_latency_ms => [metric(commit_latency, SMs, Ls)]
    }.

snapshot_transfers(Counters, Ls, Acc) ->
    Acc#{
        snapshot_reads => snapshot_reads(Counters, Ls),
        snapshot_read_errors => [metric(?snap_read__errors, Counters, Ls)],
        snapshot_read_chunks => [metric(?snap_read__chunks, Counters, Ls)],
        snapshot_read_chunk_bytes => [metric(?snap_read__chunk_bs, Counters, Ls)],
        snapshot_writes => snapshot_writes(Counters, Ls),
        snapshot_write_errors => [metric(?snap_write__errors, Counters, Ls)],
        snapshot_write_chunks => [metric(?snap_write__chunks, Counters, Ls)],
        snapshot_write_chunk_bytes => [metric(?snap_write__chunk_bs, Counters, Ls)]
    }.

shard_transitions(Counters, Ls) ->
    metrics(?SHARD_TRANSITIONS_METRICS, Counters, Ls).

snapshot_reads(Counters, Ls) ->
    metrics(?SNAPSHOT_READS_METRICS, Counters, Ls).

snapshot_writes(Counters, Ls) ->
    metrics(?SNAPSHOT_WRITES_METRICS, Counters, Ls).

metrics(List, Metrics, Ls) ->
    [
        {MLabels ++ Ls, maps:get(CName, Metrics, 0)}
     || {_, CName, MLabels} <- List
    ].

metric(Name, Metrics, Ls) ->
    {Ls, maps:get(Name, Metrics, 0)}.

%%

-spec gather_metrics(fun((E) -> metrics()), [E]) -> metrics().
gather_metrics(MFun, List) ->
    lists:foldr(fun(E, Acc) -> merge_metrics(MFun(E), Acc) end, #{}, List).

-spec merge_metrics(Ms, Ms) -> Ms when Ms :: metrics().
merge_metrics(M1, M2) ->
    maps:merge_with(fun(_Name, Vs1, Vs2) -> Vs1 ++ Vs2 end, M1, M2).
