%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_data_integration).

-export([
    deregister_cleanup/1,
    collect_mf/2,
    collect_metrics/2
]).

-export([collect/1, collect_ns/2]).

%% for bpapi
-behaviour(emqx_prometheus_cluster).
-export([
    fetch_namespaced_metrics_v1/2,
    fetch_cluster_wide_namespaced_metrics/2,
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
]).

-export([add_collect_family/4]).

-ifdef(TEST).
-export([put_namespace_pd/1, erase_namespace_pd/0]).
-endif.

-include("emqx_prometheus.hrl").
-include_lib("prometheus/include/prometheus.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(
    prometheus_model_helpers,
    [
        create_mf/5,
        gauge_metric/1,
        gauge_metrics/1,
        counter_metrics/1
    ]
).

%% Please don't remove this attribute, prometheus uses it to
%% automatically register collectors.
-behaviour(prometheus_collector).

-deprecated([fetch_from_local_node/1, fetch_cluster_consistented_data/0]).

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------

-define(MG(K, MAP), maps:get(K, MAP)).
-define(MG0(K, MAP), maps:get(K, MAP, 0)).

%%--------------------------------------------------------------------
%% Callback for emqx_prometheus_cluster
%%--------------------------------------------------------------------

-define(ROOT_KEY_ACTIONS, actions).

fetch_namespaced_metrics_v1(Namespace, Mode) ->
    Rules = get_rules(Namespace),
    BridgeV2Actions = emqx_bridge_v2:list(Namespace, ?ROOT_KEY_ACTIONS),
    Connectors = emqx_connector:list(Namespace),
    {node(self()), #{
        rule_metric_data => rule_metric_data(Mode, Rules),
        action_metric_data => action_metric_data(Mode, BridgeV2Actions),
        connector_metric_data => connector_metric_data(Mode, Connectors)
    }}.

-spec fetch_cluster_wide_namespaced_metrics(all | emqx_config:namespace(), _Mode) -> map().
fetch_cluster_wide_namespaced_metrics(Namespace, _Mode) ->
    Rules = get_rules(Namespace),
    Connectors = get_connectors(Namespace),
    (schema_registry_data())#{
        rules_ov_data => rules_ov_data(Rules),
        %% note: "actions" here means rule action, not actual Actions (as in Connector
        %% channels)....
        actions_ov_data => actions_ov_data(Rules),
        connectors_ov_data => connectors_ov_data(Connectors)
    }.

%% deprecated; return empty set for backwards compatibility. (prometheus_proto_v{1,2} bpapi)
fetch_from_local_node(Mode) ->
    Rules = [],
    BridgeV2Actions = [],
    Connectors = [],
    {node(self()), #{
        rule_metric_data => rule_metric_data(Mode, Rules),
        action_metric_data => action_metric_data(Mode, BridgeV2Actions),
        connector_metric_data => connector_metric_data(Mode, Connectors)
    }}.

%% deprecated; return empty set for backwards compatibility. (prometheus_proto_v{1,2} bpapi)
fetch_cluster_consistented_data() ->
    Rules = [],
    Connectors = [],
    (empty_schema_registry_data())#{
        rules_ov_data => rules_ov_data(Rules),
        %% note: "actions" here means rule action, not actual Actions (as in Connector
        %% channels)....
        actions_ov_data => actions_ov_data(Rules),
        connectors_ov_data => connectors_ov_data(Connectors)
    }.

aggre_or_zip_init_acc() ->
    #{
        rule_metric_data => maps:from_keys(rule_metric(names), []),
        action_metric_data => maps:from_keys(action_metric(names), []),
        connector_metric_data => maps:from_keys(connector_metric(names), [])
    }.

logic_sum_metrics() ->
    [
        emqx_rule_enable,
        emqx_connector_enable,
        emqx_connector_status
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
collect_mf(?PROMETHEUS_DATA_INTEGRATION_REGISTRY, Callback) ->
    RawData = emqx_prometheus_cluster:raw_data_ns(
        ?MODULE, get_namespace_pd(), ?GET_PROM_DATA_MODE()
    ),

    %% Data Integration Overview
    ok = add_collect_family(
        Callback,
        rules_ov_metric_meta(),
        ?MG(rules_ov_data, RawData)
    ),
    ok = add_collect_family(
        Callback,
        actions_ov_metric_meta(),
        ?MG(actions_ov_data, RawData)
    ),
    ok = add_collect_family(
        Callback,
        connectors_ov_metric_meta(),
        ?MG(connectors_ov_data, RawData)
    ),
    ok = maybe_collect_family_schema_registry(Callback),

    %% Rule Metric
    RuleMetricDs = ?MG(rule_metric_data, RawData),
    ok = add_collect_family(Callback, rule_metric_meta(), RuleMetricDs),

    %% Action Metric
    ActionMetricDs = ?MG(action_metric_data, RawData),
    ok = add_collect_family(Callback, action_metric_meta(), ActionMetricDs),

    %% Connector Metric
    ConnectorMetricDs = ?MG(connector_metric_data, RawData),
    ok = add_collect_family(Callback, connector_metric_meta(), ConnectorMetricDs),

    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_DATA_INTEGRATION_REGISTRY).

collect_ns(Namespace, Mode) ->
    try
        ?PUT_PROM_DATA_MODE(Mode),
        put_namespace_pd(Namespace),
        prometheus_text_format:format(?PROMETHEUS_DATA_INTEGRATION_REGISTRY)
    after
        _ = erase(?PROM_DATA_MODE_KEY__),
        _ = erase_namespace_pd()
    end.

%%====================
%% API Helpers

add_collect_family(Callback, MetricWithType, Data) ->
    _ = [add_collect_family(Name, Data, Callback, Type) || {Name, Type} <- MetricWithType],
    ok.

add_collect_family(Name, Data, Callback, Type) ->
    %% TODO: help document from Name
    Callback(create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

collect_metrics(Name, Metrics) ->
    collect_di(Name, Metrics).

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%========================================
%% Data Integration Overview
%%========================================

%%====================
%% All Rules
%% Rules
collect_di(K = emqx_rules_count, Data) -> gauge_metric(?MG(K, Data));
%%====================
%% Actions
collect_di(K = emqx_actions_count, Data) -> gauge_metric(?MG(K, Data));
%%====================
%% Schema Registry
collect_di(K = emqx_schema_registrys_count, Data) -> gauge_metric(?MG(K, Data));
%%====================
%% Connectors
collect_di(K = emqx_connectors_count, Data) -> gauge_metric(?MG(K, Data));
%%========================================
%% Data Integration Metric for: Rule && Action && Connector
%%========================================

%%====================
%% Rule Metric
collect_di(K = emqx_rule_enable, Data) -> gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_matched, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_passed, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_exception, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_no_result, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_total, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_success, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_out_of_service, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_unknown, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_discarded, Data) -> counter_metrics(?MG(K, Data));
%%====================
%% Action Metric
collect_di(K = emqx_action_enable, Data) -> gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_status, Data) -> gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_matched, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_success, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_failed, Data) -> counter_metrics(?MG(K, Data));
%% inflight type: gauge
collect_di(K = emqx_action_inflight, Data) -> gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_received, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_late_reply, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_success, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_failed, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_stopped, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_not_found, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_queue_full, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_other, Data) -> counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_expired, Data) -> counter_metrics(?MG(K, Data));
%% queuing type: gauge
collect_di(K = emqx_action_queuing, Data) -> gauge_metrics(?MG(K, Data));
%%====================
%% Connector Metric
collect_di(K = emqx_connector_enable, Data) -> gauge_metrics(?MG(K, Data));
collect_di(K = emqx_connector_status, Data) -> gauge_metrics(?MG(K, Data)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%========================================
%% Data Integration Overview
%%========================================

%%====================
%% All Rules

rules_ov_metric_meta() ->
    [
        {emqx_rules_count, gauge}
    ].

rules_ov_data(Rules) ->
    #{
        emqx_rules_count => length(Rules)
    }.

%%====================
%% Actions

actions_ov_metric_meta() ->
    [
        {emqx_actions_count, gauge}
    ].

actions_ov_data(Rules) ->
    ActionsCount = lists:foldl(
        fun
            (#{actions := Actions} = _Rule, AccIn) ->
                AccIn + length(Actions);
            (_, AccIn) ->
                AccIn
        end,
        0,
        Rules
    ),
    #{
        emqx_actions_count => ActionsCount
    }.

%%====================
%% Schema Registry

maybe_collect_family_schema_registry(Callback) ->
    ok = add_collect_family(Callback, schema_registry_metric_meta(), schema_registry_data()),
    ok.

schema_registry_metric_meta() ->
    [
        {emqx_schema_registrys_count, gauge}
    ].

schema_registry_data() ->
    #{
        emqx_schema_registrys_count => erlang:map_size(emqx_schema_registry:list_schemas())
    }.

empty_schema_registry_data() ->
    #{
        emqx_schema_registrys_count => 0
    }.

%%====================
%% Connectors

connectors_ov_metric_meta() ->
    [
        {emqx_connectors_count, gauge}
    ].

connectors_ov_data(Connectors) ->
    #{
        emqx_connectors_count => erlang:length(Connectors)
    }.

%%========================================
%% Data Integration Metric for: Rule && Action && Connector
%%========================================

%%====================
%% Rule Metric
%% With rule_id as label key: `rule_id`

rule_metric_meta() ->
    [
        {emqx_rule_enable, gauge},
        {emqx_rule_matched, counter},
        {emqx_rule_failed, counter},
        {emqx_rule_passed, counter},
        {emqx_rule_failed_exception, counter},
        {emqx_rule_failed_no_result, counter},
        {emqx_rule_actions_total, counter},
        {emqx_rule_actions_success, counter},
        {emqx_rule_actions_failed, counter},
        {emqx_rule_actions_failed_out_of_service, counter},
        {emqx_rule_actions_failed_unknown, counter},
        {emqx_rule_actions_discarded, counter}
    ].

rule_metric(names) ->
    emqx_prometheus_cluster:metric_names(rule_metric_meta()).

rule_metric_data(Mode, Rules) ->
    lists:foldl(
        fun(#{id := Id, namespace := Namespace} = Rule, AccIn) ->
            merge_acc_with_rules(Namespace, Mode, Id, get_metric(Rule), AccIn)
        end,
        maps:from_keys(rule_metric(names), []),
        Rules
    ).

merge_acc_with_rules(Namespace, Mode, Id, RuleMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [rule_point(Namespace, Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        RuleMetrics
    ).

rule_point(Namespace, Mode, Id, V) ->
    Labels = mk_labels(Namespace, Id),
    {with_node_label(Mode, Labels), V}.

mk_labels(Namespace, Id) ->
    case Namespace of
        ?global_ns -> [{id, Id}];
        _ -> [{id, Id}, {namespace, Namespace}]
    end.

get_metric(#{enable := Bool} = Rule) ->
    RuleResId = emqx_rule_engine:rule_resource_id(Rule),
    #{counters := Counters} =
        emqx_metrics_worker:get_metrics(rule_metrics, RuleResId),
    #{
        emqx_rule_enable => emqx_prometheus_cluster:boolean_to_number(Bool),
        emqx_rule_matched => ?MG(matched, Counters),
        emqx_rule_failed => ?MG(failed, Counters),
        emqx_rule_passed => ?MG(passed, Counters),
        emqx_rule_failed_exception => ?MG('failed.exception', Counters),
        emqx_rule_failed_no_result => ?MG('failed.no_result', Counters),
        emqx_rule_actions_total => ?MG('actions.total', Counters),
        emqx_rule_actions_success => ?MG('actions.success', Counters),
        emqx_rule_actions_failed => ?MG('actions.failed', Counters),
        emqx_rule_actions_failed_out_of_service => ?MG('actions.failed.out_of_service', Counters),
        emqx_rule_actions_failed_unknown => ?MG('actions.failed.unknown', Counters),
        emqx_rule_actions_discarded => ?MG('actions.discarded', Counters)
    }.

%%====================
%% Action Metric
%% With action_id: `{type}:{name}` as label key: `action_id`

action_metric_meta() ->
    [
        {emqx_action_enable, gauge},
        {emqx_action_status, gauge},
        {emqx_action_matched, counter},
        {emqx_action_dropped, counter},
        {emqx_action_success, counter},
        {emqx_action_failed, counter},
        {emqx_action_inflight, gauge},
        {emqx_action_received, counter},
        {emqx_action_late_reply, counter},
        {emqx_action_retried, counter},
        {emqx_action_retried_success, counter},
        {emqx_action_retried_failed, counter},
        {emqx_action_dropped_resource_stopped, counter},
        {emqx_action_dropped_resource_not_found, counter},
        {emqx_action_dropped_queue_full, counter},
        {emqx_action_dropped_other, counter},
        {emqx_action_dropped_expired, counter},
        {emqx_action_queuing, gauge}
    ].

action_metric(names) ->
    emqx_prometheus_cluster:metric_names(action_metric_meta()).

action_metric_data(Mode, Bridges) ->
    lists:foldl(
        fun(Action, AccIn) ->
            #{
                type := Type,
                name := Name,
                namespace := Namespace
            } = Action,
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            Status = get_action_status(Action),
            Metrics = get_action_metric(Namespace, Type, Name),
            merge_acc_with_bridges(Namespace, Mode, Id, maps:merge(Status, Metrics), AccIn)
        end,
        maps:from_keys(action_metric(names), []),
        Bridges
    ).

merge_acc_with_bridges(Namespace, Mode, Id, BridgeMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [action_point(Namespace, Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        BridgeMetrics
    ).

get_action_status(#{raw_config := RawConfig, resource_data := ResourceData} = _Action) ->
    Enable = maps:get(<<"enable">>, RawConfig, true),
    Status = maps:get(status, ResourceData, disconnected),
    #{
        emqx_action_enable => emqx_prometheus_cluster:boolean_to_number(Enable),
        emqx_action_status => emqx_prometheus_cluster:status_to_number(Status)
    }.

action_point(Namespace, Mode, Id, V) ->
    Labels = mk_labels(Namespace, Id),
    {with_node_label(Mode, Labels), V}.

get_action_metric(Namespace, Type, Name) ->
    #{counters := Counters, gauges := Gauges} = emqx_bridge_v2:get_metrics(
        Namespace, actions, Type, Name
    ),
    #{
        emqx_action_matched => ?MG0(matched, Counters),
        emqx_action_dropped => ?MG0(dropped, Counters),
        emqx_action_success => ?MG0(success, Counters),
        emqx_action_failed => ?MG0(failed, Counters),
        emqx_action_inflight => ?MG0(inflight, Gauges),
        emqx_action_received => ?MG0(received, Counters),
        emqx_action_late_reply => ?MG0(late_reply, Counters),
        emqx_action_retried => ?MG0(retried, Counters),
        emqx_action_retried_success => ?MG0('retried.success', Counters),
        emqx_action_retried_failed => ?MG0('retried.failed', Counters),
        emqx_action_dropped_resource_stopped => ?MG0('dropped.resource_stopped', Counters),
        emqx_action_dropped_resource_not_found => ?MG0('dropped.resource_not_found', Counters),
        emqx_action_dropped_queue_full => ?MG0('dropped.queue_full', Counters),
        emqx_action_dropped_other => ?MG0('dropped.other', Counters),
        emqx_action_dropped_expired => ?MG0('dropped.expired', Counters),
        emqx_action_queuing => ?MG0(queuing, Gauges)
    }.

%%====================
%% Connector Metric
%% With connector_id: `{type}:{name}` as label key: `connector_id`

connector_metric_meta() ->
    [
        {emqx_connector_enable, gauge},
        {emqx_connector_status, gauge}
    ].

connector_metric(names) ->
    emqx_prometheus_cluster:metric_names(connector_metric_meta()).

connector_metric_data(Mode, Connectors) ->
    AccIn0 = maps:from_keys(connector_metric(names), []),
    lists:foldl(
        fun(Connector, AccIn) ->
            #{
                type := Type,
                name := Name,
                resource_data := ResourceData,
                namespace := Namespace
            } = Connector,
            Id = emqx_connector_resource:connector_id(Type, Name),
            merge_acc_with_connectors(
                Namespace, Mode, Id, get_connector_status(ResourceData), AccIn
            )
        end,
        AccIn0,
        Connectors
    ).

merge_acc_with_connectors(Namespace, Mode, Id, ConnectorMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [connector_point(Namespace, Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        ConnectorMetrics
    ).

connector_point(Namespace, Mode, Id, V) ->
    Labels = mk_labels(Namespace, Id),
    {with_node_label(Mode, Labels), V}.

get_connector_status(ResourceData) ->
    Enabled = emqx_utils_maps:deep_get([config, enable], ResourceData),
    Status = ?MG(status, ResourceData),
    #{
        emqx_connector_enable => emqx_prometheus_cluster:boolean_to_number(Enabled),
        emqx_connector_status => emqx_prometheus_cluster:status_to_number(Status)
    }.

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper funcs

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node(self())} | Labels].

get_rules_all_namespaces() ->
    NamespaceToRules = emqx_rule_engine:get_rules_from_all_namespaces(),
    lists:append(maps:values(NamespaceToRules)).

put_namespace_pd(Namespace) when
    is_binary(Namespace);
    Namespace == ?global_ns;
    Namespace == all
->
    _ = put({?MODULE, ns}, Namespace),
    ok.

erase_namespace_pd() ->
    _ = erase({?MODULE, ns}),
    ok.

get_namespace_pd() ->
    get({?MODULE, ns}).

get_rules(all) ->
    get_rules_all_namespaces();
get_rules(Namespace) ->
    emqx_rule_engine:get_rules(Namespace).

get_connectors(Namespace) ->
    emqx_connector:list(Namespace).
