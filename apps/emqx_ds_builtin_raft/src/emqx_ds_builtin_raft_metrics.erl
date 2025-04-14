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
    local_shards_meta/0,
    local_shards/1,
    local_shard/3
]).

-type metric_type() :: gauge | counter.
-type metric_meta() :: {_Name :: atom(), metric_type()}.

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

-define(shard_transitions__started__add, 'transitions.started.add').
-define(shard_transitions__started__del, 'transitions.started.del').
-define(shard_transitions__completed__add, 'transitions.completed.add').
-define(shard_transitions__completed__del, 'transitions.completed.del').
-define(shard_transitions__skipped__add, 'transitions.skipped.add').
-define(shard_transitions__skipped__del, 'transitions.skipped.del').
-define(shard_transitions__crashed__add, 'transitions.crashed.add').
-define(shard_transitions__crashed__del, 'transitions.crashed.del').

-define(shard_transition_errors, 'transitions_errors').

-define(LOCAL_SHARD_METRICS, ?SHARD_TRANSITIONS_METRICS ++ ?SHARD_TRANSITION_ERRORS_METRICS).
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

shard_transition_started(DB, Shard, {add, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__started__add);
shard_transition_started(DB, Shard, {del, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__started__del).

shard_transition_complete(DB, Shard, {add, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__completed__add);
shard_transition_complete(DB, Shard, {del, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__completed__del).

shard_transition_skipped(DB, Shard, {add, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__skipped__add);
shard_transition_skipped(DB, Shard, {del, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__skipped__del).

shard_transition_crashed(DB, Shard, {add, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__crashed__add);
shard_transition_crashed(DB, Shard, {del, _}) ->
    inc_shard_transition(DB, Shard, ?shard_transitions__crashed__del).

inc_shard_transition(DB, Shard, Counter) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, ?MID_SHARD(DB, Shard), Counter, 1)).

shard_transition_error(DB, Shard, _Transition) ->
    ?CATCH(emqx_metrics_worker:inc(?WORKER, ?MID_SHARD(DB, Shard), ?shard_transition_errors, 1)).

%%

-spec cluster_meta() -> [metric_meta()].
cluster_meta() ->
    [
        {cluster_sites_num, gauge}
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
        {db_shards_num, gauge},
        {db_sites_num, gauge}
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
        {shard_replication_factor, gauge},
        {shard_transition_queue_len, gauge}
    ].

%% Cluster-wide metrics of each DB's each shard.
-spec shards() -> metrics().
shards() ->
    gather_metrics(fun shards/1, emqx_ds_builtin_raft_meta:dbs()).

-spec shards(emqx_ds:db()) -> metrics().
shards(DB) ->
    gather_metrics(fun(Shard) -> shard(DB, Shard) end, emqx_ds_builtin_raft_meta:shards(DB)).

-spec shard(emqx_ds:db(), emqx_ds_replication_layer:shard_id()) -> metrics().
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

-spec local_shards_meta() -> [metric_meta()].
local_shards_meta() ->
    [
        {shard_transitions, counter},
        {shard_transition_errors, counter}
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
    gather_metrics(
        fun(Shard) -> local_shard(DB, Shard, Labels) end,
        emqx_ds_builtin_raft_shard_allocator:shards(DB)
    ).

-spec local_shard(emqx_ds:db(), emqx_ds_replication_layer:shard_id(), _Labels0 :: [label()]) ->
    metrics().
local_shard(DB, Shard, Labels0) ->
    Labels = [{db, DB}, {shard, Shard} | Labels0],
    Counters = emqx_metrics_worker:get_counters(?WORKER, ?MID_SHARD(DB, Shard)),
    #{
        shard_transitions => shard_transitions(Counters, Labels),
        shard_transition_errors => shard_transition_errors(Counters, Labels)
    }.

shard_transitions(Counters, Ls) ->
    [
        {MLabels ++ Ls, maps:get(CName, Counters)}
     || {_, CName, MLabels} <- ?SHARD_TRANSITIONS_METRICS
    ].

shard_transition_errors(Metrics, Ls) ->
    [{Ls, maps:get(?shard_transition_errors, Metrics, 0)}].

%%

-spec gather_metrics(fun((E) -> metrics()), [E]) -> metrics().
gather_metrics(MFun, List) ->
    lists:foldr(fun(E, Acc) -> merge_metrics(MFun(E), Acc) end, #{}, List).

-spec merge_metrics(Ms, Ms) -> Ms when Ms :: metrics().
merge_metrics(M1, M2) ->
    maps:merge_with(fun(_Name, Vs1, Vs2) -> Vs1 ++ Vs2 end, M1, M2).
