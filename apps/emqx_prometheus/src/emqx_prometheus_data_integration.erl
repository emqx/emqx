%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_prometheus_data_integration).

-export([
    deregister_cleanup/1,
    collect_mf/2,
    collect_metrics/2
]).

-export([collect/1]).

-export([
    zip_json_data_integration_metrics/3
]).

%% for bpapi
-behaviour(emqx_prometheus_cluster).
-export([
    fetch_from_local_node/1,
    fetch_cluster_consistented_data/0,
    aggre_or_zip_init_acc/0,
    logic_sum_metrics/0
]).

-export([add_collect_family/4]).

-include("emqx_prometheus.hrl").
-include_lib("prometheus/include/prometheus.hrl").

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

%%--------------------------------------------------------------------
%% Macros
%%--------------------------------------------------------------------

-define(METRIC_NAME_PREFIX, "emqx_data_integration_").

-define(MG(K, MAP), maps:get(K, MAP)).
-define(MG0(K, MAP), maps:get(K, MAP, 0)).

%%--------------------------------------------------------------------
%% Callback for emqx_prometheus_cluster
%%--------------------------------------------------------------------

-define(ROOT_KEY_ACTIONS, actions).

fetch_from_local_node(Mode) ->
    Rules = emqx_rule_engine:get_rules(),
    BridgesV1 = emqx:get_config([bridges], #{}),
    BridgeV2Actions = emqx_bridge_v2:list(?ROOT_KEY_ACTIONS),
    Connectors = emqx_connector:list(),
    {node(self()), #{
        rule_metric_data => rule_metric_data(Mode, Rules),
        action_metric_data => action_metric_data(Mode, BridgeV2Actions),
        connector_metric_data => connector_metric_data(Mode, BridgesV1, Connectors)
    }}.

fetch_cluster_consistented_data() ->
    Rules = emqx_rule_engine:get_rules(),
    %% for bridge v1
    BridgesV1 = emqx:get_config([bridges], #{}),
    Connectors = emqx_connector:list(),
    (maybe_collect_schema_registry())#{
        rules_ov_data => rules_ov_data(Rules),
        actions_ov_data => actions_ov_data(Rules),
        connectors_ov_data => connectors_ov_data(BridgesV1, Connectors)
    }.

aggre_or_zip_init_acc() ->
    #{
        rule_metric_data => maps:from_keys(rule_metric(names), []),
        action_metric_data => maps:from_keys(action_metric(names), []),
        connector_metric_data => maps:from_keys(connectr_metric(names), [])
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
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),

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
collect(<<"json">>) ->
    RawData = emqx_prometheus_cluster:raw_data(?MODULE, ?GET_PROM_DATA_MODE()),
    Rules = emqx_rule_engine:get_rules(),
    Connectors = emqx_connector:list(),
    %% for bridge v1
    BridgesV1 = emqx:get_config([bridges], #{}),
    #{
        data_integration_overview => collect_data_integration_overview(
            Rules, BridgesV1, Connectors
        ),
        rules => collect_json_data(?MG(rule_metric_data, RawData)),
        actions => collect_json_data(?MG(action_metric_data, RawData)),
        connectors => collect_json_data(?MG(connector_metric_data, RawData))
    };
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_DATA_INTEGRATION_REGISTRY).

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

rules_ov_metric(names) ->
    emqx_prometheus_cluster:metric_names(rules_ov_metric_meta()).

-define(RULE_TAB, emqx_rule_engine).
rules_ov_data(_Rules) ->
    #{
        emqx_rules_count => ets:info(?RULE_TAB, size)
    }.

%%====================
%% Actions

actions_ov_metric_meta() ->
    [
        {emqx_actions_count, gauge}
    ].

actions_ov_metric(names) ->
    emqx_prometheus_cluster:metric_names(actions_ov_metric_meta()).

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

-if(?EMQX_RELEASE_EDITION == ee).

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

maybe_collect_schema_registry() ->
    schema_registry_data().

-else.

maybe_collect_family_schema_registry(_) ->
    ok.

maybe_collect_schema_registry() ->
    #{}.

-endif.

%%====================
%% Connectors

connectors_ov_metric_meta() ->
    [
        {emqx_connectors_count, gauge}
    ].

connectors_ov_metric(names) ->
    emqx_prometheus_cluster:metric_names(connectors_ov_metric_meta()).

connectors_ov_data(BridgesV1, Connectors) ->
    %% Both Bridge V1 and V2
    V1ConnectorsCnt = maps:fold(
        fun(_Type, NameAndConf, AccIn) ->
            AccIn + maps:size(NameAndConf)
        end,
        0,
        BridgesV1
    ),
    #{
        emqx_connectors_count => erlang:length(Connectors) + V1ConnectorsCnt
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
        fun(#{id := Id} = Rule, AccIn) ->
            merge_acc_with_rules(Mode, Id, get_metric(Rule), AccIn)
        end,
        maps:from_keys(rule_metric(names), []),
        Rules
    ).

merge_acc_with_rules(Mode, Id, RuleMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [rule_point(Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        RuleMetrics
    ).

rule_point(Mode, Id, V) ->
    {with_node_label(Mode, [{id, Id}]), V}.

get_metric(#{id := Id, enable := Bool} = _Rule) ->
    #{counters := Counters} =
        emqx_metrics_worker:get_metrics(rule_metrics, Id),
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
        fun(#{type := Type, name := Name} = Action, AccIn) ->
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            Status = get_action_status(Action),
            Metrics = get_action_metric(Type, Name),
            merge_acc_with_bridges(Mode, Id, maps:merge(Status, Metrics), AccIn)
        end,
        maps:from_keys(action_metric(names), []),
        Bridges
    ).

merge_acc_with_bridges(Mode, Id, BridgeMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [action_point(Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        BridgeMetrics
    ).

get_action_status(#{resource_data := ResourceData} = _Action) ->
    Enable = emqx_utils_maps:deep_get([config, enable], ResourceData),
    Status = ?MG(status, ResourceData),
    #{
        emqx_action_enable => emqx_prometheus_cluster:boolean_to_number(Enable),
        emqx_action_status => emqx_prometheus_cluster:status_to_number(Status)
    }.

action_point(Mode, Id, V) ->
    {with_node_label(Mode, [{id, Id}]), V}.

get_action_metric(Type, Name) ->
    #{counters := Counters, gauges := Gauges} = emqx_bridge_v2:get_metrics(Type, Name),
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

connectr_metric(names) ->
    emqx_prometheus_cluster:metric_names(connector_metric_meta()).

connector_metric_data(Mode, BridgesV1, Connectors) ->
    AccIn = maps:from_keys(connectr_metric(names), []),
    Acc0 = connector_metric_data_v1(Mode, BridgesV1, AccIn),
    _AccOut = connector_metric_data_v2(Mode, Connectors, Acc0).

connector_metric_data_v2(Mode, Connectors, InitAcc) ->
    lists:foldl(
        fun(#{type := Type, name := Name, resource_data := ResourceData} = _Connector, AccIn) ->
            Id = emqx_connector_resource:connector_id(Type, Name),
            merge_acc_with_connectors(Mode, Id, get_connector_status(ResourceData), AccIn)
        end,
        InitAcc,
        Connectors
    ).

connector_metric_data_v1(Mode, BridgesV1, InitAcc) ->
    maps:fold(
        fun(Type, NameAndConfMap, Acc0) ->
            maps:fold(
                fun(Name, _Conf, Acc1) ->
                    BridgeV1Id = emqx_bridge_resource:resource_id(Type, Name),
                    case emqx_resource:get_instance(BridgeV1Id) of
                        {error, not_found} ->
                            Acc1;
                        {ok, _, ResourceData} ->
                            merge_acc_with_connectors(
                                Mode, BridgeV1Id, get_connector_status(ResourceData), Acc1
                            )
                    end
                end,
                Acc0,
                NameAndConfMap
            )
        end,
        InitAcc,
        BridgesV1
    ).

merge_acc_with_connectors(Mode, Id, ConnectorMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [connector_point(Mode, Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        ConnectorMetrics
    ).

connector_point(Mode, Id, V) ->
    {with_node_label(Mode, [{id, Id}]), V}.

get_connector_status(ResourceData) ->
    Enabled = emqx_utils_maps:deep_get([config, enable], ResourceData),
    Status = ?MG(status, ResourceData),
    #{
        emqx_connector_enable => emqx_prometheus_cluster:boolean_to_number(Enabled),
        emqx_connector_status => emqx_prometheus_cluster:status_to_number(Status)
    }.

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`
collect_data_integration_overview(Rules, BridgesV1, Connectors) ->
    RulesD = rules_ov_data(Rules),
    ActionsD = actions_ov_data(Rules),
    ConnectorsD = connectors_ov_data(BridgesV1, Connectors),

    M1 = lists:foldl(
        fun(K, AccIn) -> AccIn#{K => ?MG(K, RulesD)} end,
        #{},
        rules_ov_metric(names)
    ),
    M2 = lists:foldl(
        fun(K, AccIn) -> AccIn#{K => ?MG(K, ActionsD)} end,
        #{},
        actions_ov_metric(names)
    ),
    M3 = lists:foldl(
        fun(K, AccIn) -> AccIn#{K => ?MG(K, ConnectorsD)} end,
        #{},
        connectors_ov_metric(names)
    ),
    M4 = maybe_collect_schema_registry(),

    lists:foldl(fun(M, AccIn) -> maps:merge(M, AccIn) end, #{}, [M1, M2, M3, M4]).

collect_json_data(Data) ->
    emqx_prometheus_cluster:collect_json_data(Data, fun zip_json_data_integration_metrics/3).

%% for initialized empty AccIn
%% The following fields will be put into Result
%% For Rules:
%%     `id` => [RULE_ID]
%% For Actions
%%     `id` => [ACTION_ID]
%% FOR Connectors
%%     `id` => [CONNECTOR_ID] %% CONNECTOR_ID = BRIDGE_ID
%%     formatted with {type}:{name}
zip_json_data_integration_metrics(Key, Points, [] = _AccIn) ->
    lists:foldl(
        fun({Lables, Metric}, AccIn2) ->
            LablesKVMap = maps:from_list(Lables),
            Point = LablesKVMap#{Key => Metric},
            [Point | AccIn2]
        end,
        [],
        Points
    );
zip_json_data_integration_metrics(Key, Points, AllResultedAcc) ->
    ThisKeyResult = lists:foldl(emqx_prometheus_cluster:point_to_map_fun(Key), [], Points),
    lists:zipwith(fun maps:merge/2, AllResultedAcc, ThisKeyResult).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper funcs

with_node_label(?PROM_DATA_MODE__NODE, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, Labels) ->
    Labels;
with_node_label(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, Labels) ->
    [{node, node(self())} | Labels].
