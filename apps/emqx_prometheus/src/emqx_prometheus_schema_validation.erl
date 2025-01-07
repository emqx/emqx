%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_schema_validation).

-if(?EMQX_RELEASE_EDITION == ee).
%% for bpapi
-behaviour(emqx_prometheus_cluster).

%% Please don't remove this attribute, prometheus uses it to
%% automatically register collectors.
-behaviour(prometheus_collector).

-include("emqx_prometheus.hrl").
-include_lib("prometheus/include/prometheus.hrl").

-import(
    prometheus_model_helpers,
    [
        create_mf/5,
        gauge_metrics/1,
        counter_metrics/1
    ]
).

-export([
    deregister_cleanup/1,
    collect_mf/2,
    collect_metrics/2
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

%%--------------------------------------------------------------------
%% Type definitions
%%--------------------------------------------------------------------

-define(MG(K, MAP), maps:get(K, MAP)).
-define(MG0(K, MAP), maps:get(K, MAP, 0)).

-define(metrics_data_key, schema_validation_metrics_data).

-define(key_enabled, emqx_schema_validation_enable).
-define(key_matched, emqx_schema_validation_matched).
-define(key_failed, emqx_schema_validation_failed).
-define(key_succeeded, emqx_schema_validation_succeeded).

%%--------------------------------------------------------------------
%% `emqx_prometheus_cluster' API
%%--------------------------------------------------------------------

fetch_from_local_node(Mode) ->
    Validations = emqx_schema_validation:list(),
    {node(), #{
        ?metrics_data_key => to_validation_data(Mode, Validations)
    }}.

fetch_cluster_consistented_data() ->
    #{}.

aggre_or_zip_init_acc() ->
    #{
        ?metrics_data_key => maps:from_keys(schema_validation_metric(names), [])
    }.

logic_sum_metrics() ->
    [
        ?key_enabled
    ].

%%--------------------------------------------------------------------
%% Collector API
%%--------------------------------------------------------------------

%% @private
deregister_cleanup(_) -> ok.

%% @private
-spec collect_mf(_Registry, Callback) -> ok when
    _Registry :: prometheus_registry:registry(),
    Callback :: prometheus_collector:collect_mf_callback().
collect_mf(?PROMETHEUS_SCHEMA_VALIDATION_REGISTRY, Callback) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),

    %% Schema Validation Metrics
    RuleMetricDs = ?MG(?metrics_data_key, RawData),
    ok = add_collect_family(Callback, schema_validation_metrics_meta(), RuleMetricDs),

    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    #{
        schema_validations => collect_json_data(?MG(?metrics_data_key, RawData))
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_SCHEMA_VALIDATION_REGISTRY).

%%====================
%% API Helpers

add_collect_family(Callback, MetricWithType, Data) ->
    _ = [add_collect_family(Name, Data, Callback, Type) || {Name, Type} <- MetricWithType],
    ok.

add_collect_family(Name, Data, Callback, Type) ->
    %% TODO: help document from Name
    Callback(create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

collect_metrics(Name, Metrics) ->
    collect_mv(Name, Metrics).

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%========================================
%% Schema Validation Metrics
%%========================================
collect_mv(K = ?key_enabled, Data) -> gauge_metrics(?MG(K, Data));
collect_mv(K = ?key_matched, Data) -> counter_metrics(?MG(K, Data));
collect_mv(K = ?key_failed, Data) -> counter_metrics(?MG(K, Data));
collect_mv(K = ?key_succeeded, Data) -> counter_metrics(?MG(K, Data)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%========================================
%% Schema Validation Metrics
%%========================================

schema_validation_metrics_meta() ->
    [
        {?key_enabled, gauge},
        {?key_matched, counter},
        {?key_failed, counter},
        {?key_succeeded, counter}
    ].

schema_validation_metric(names) ->
    emqx_prometheus_cluster:metric_names(schema_validation_metrics_meta()).

to_validation_data(Mode, Validations) ->
    lists:foldl(
        fun(#{name := Name} = Validation, Acc) ->
            merge_acc_with_validations(Mode, Name, get_validation_metrics(Validation), Acc)
        end,
        maps:from_keys(schema_validation_metric(names), []),
        Validations
    ).

merge_acc_with_validations(Mode, Id, ValidationMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [validation_point(Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        ValidationMetrics
    ).

validation_point(Mode, Name, V) ->
    {with_node_label(Mode, [{validation_name, Name}]), V}.

get_validation_metrics(#{name := Name, enable := Enabled} = _Rule) ->
    #{counters := Counters} = emqx_schema_validation_registry:get_metrics(Name),
    #{
        ?key_enabled => emqx_prometheus_cluster:boolean_to_number(Enabled),
        ?key_matched => ?MG0('matched', Counters),
        ?key_failed => ?MG0('failed', Counters),
        ?key_succeeded => ?MG0('succeeded', Counters)
    }.

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`

collect_json_data(Data) ->
    emqx_prometheus_cluster:collect_json_data(Data, fun zip_json_schema_validation_metrics/3).

zip_json_schema_validation_metrics(Key, Points, [] = _AccIn) ->
    lists:foldl(
        fun({Labels, Metric}, AccIn2) ->
            LabelsKVMap = maps:from_list(Labels),
            Point = LabelsKVMap#{Key => Metric},
            [Point | AccIn2]
        end,
        [],
        Points
    );
zip_json_schema_validation_metrics(Key, Points, AllResultsAcc) ->
    ThisKeyResult = lists:foldl(emqx_prometheus_cluster:point_to_map_fun(Key), [], Points),
    lists:zipwith(fun maps:merge/2, AllResultsAcc, ThisKeyResult).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper funcs

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node()} | Labels].

%% END if(?EMQX_RELEASE_EDITION == ee).
-endif.
