%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_ds_builtin_raft).

%% for bpapi
-behaviour(emqx_prometheus_cluster).

-include("emqx_prometheus.hrl").

-define(METRIC_PREFIX, <<"emqx_ds_raft_">>).

%% Please don't remove this attribute, prometheus uses it to
%% automatically register collectors.
-behaviour(prometheus_collector).
-export([
    deregister_cleanup/1,
    collect_mf/2
]).

%% `emqx_prometheus' API
-export([collect/1]).

%% `emqx_prometheus_cluster' API
-export([
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
]).

%% `emqx_prometheus_cluster' API

fetch_from_local_node(Mode) ->
    Labels0 = with_node_label(Mode, []),
    Acc1 = emqx_ds_builtin_raft_metrics:local_dbs(Labels0),
    Acc = maps:merge(emqx_ds_builtin_raft_metrics:local_shards(Labels0), Acc1),
    {node(), Acc}.

fetch_cluster_consistented_data() ->
    Acc1 = emqx_ds_builtin_raft_metrics:cluster(),
    Acc2 = maps:merge(emqx_ds_builtin_raft_metrics:dbs(), Acc1),
    Acc = maps:merge(emqx_ds_builtin_raft_metrics:shards(), Acc2),
    Acc.

aggre_or_zip_init_acc() ->
    Meta = local_metrics_meta(),
    maps:from_keys([element(1, M) || M <- Meta], []).

logic_sum_metrics() ->
    [].

%%--------------------------------------------------------------------
%% Collector API
%%--------------------------------------------------------------------

%% @private
deregister_cleanup(_) -> ok.

%% @private
-spec collect_mf(_Registry, Callback) -> ok when
    _Registry :: prometheus_registry:registry(),
    Callback :: prometheus_collector:collect_mf_callback().
collect_mf(?PROMETHEUS_DSRAFT_REGISTRY, Callback) ->
    Metrics = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    collect_metric_families(Callback, metrics_meta(), Metrics);
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    #{
        %% TODO
        ds_builtin_raft => #{}
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_DSRAFT_REGISTRY).

collect_metric_families(Callback, [Meta | Rest], Metrics) ->
    collect_metric_family(Callback, Meta, Metrics),
    collect_metric_families(Callback, Rest, Metrics);
collect_metric_families(_Callback, [], _Metrics) ->
    ok.

collect_metric_family(Callback, {Name, Type, Desc}, Metrics) ->
    MVs = collect_mvs(Name, Type, Metrics),
    PromName = [?METRIC_PREFIX, atom_to_binary(Name)],
    Callback(prometheus_model_helpers:create_mf(PromName, Desc, Type, MVs)).

collect_mvs(Name, Type, Metrics) ->
    collect_mvs(Type, maps:get(Name, Metrics, [])).

collect_mvs(counter, MVs) ->
    [prometheus_model_helpers:counter_metric(MV) || MV <- MVs];
collect_mvs(gauge, MVs) ->
    [prometheus_model_helpers:gauge_metric(MV) || MV <- MVs].

metrics_meta() ->
    emqx_ds_builtin_raft_metrics:cluster_meta() ++
        emqx_ds_builtin_raft_metrics:dbs_meta() ++
        emqx_ds_builtin_raft_metrics:shards_meta() ++
        local_metrics_meta().

local_metrics_meta() ->
    emqx_ds_builtin_raft_metrics:local_dbs_meta() ++
        emqx_ds_builtin_raft_metrics:local_shards_meta().

%% Helpers

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node()} | Labels].
