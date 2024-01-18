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
-export([
    fetch_metric_data_from_local_node/0
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

-define(RULES_WITH_TYPE, [
    {emqx_rules_count, gauge}
]).

-define(CONNECTORS_WITH_TYPE, [
    {emqx_connectors_count, gauge}
]).

-define(RULES_SPECIFIC_WITH_TYPE, [
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
    {emqx_rule_actions_failed_unknown, counter}
]).

-define(ACTION_SPECIFIC_WITH_TYPE, [
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
]).

-define(CONNECTOR_SPECIFIC_WITH_TYPE, [
    {emqx_connector_enable, gauge},
    {emqx_connector_status, gauge}
]).

-if(?EMQX_RELEASE_EDITION == ee).
-define(SCHEMA_REGISTRY_WITH_TYPE, [
    emqx_schema_registrys_count
]).
-else.
-endif.

-define(LOGICAL_SUM_METRIC_NAMES, [
    emqx_rule_enable,
    emqx_connector_enable,
    emqx_connector_status
]).

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
    RawData = raw_data(erlang:get(format_mode)),

    %% Data Integration Overview
    ok = add_collect_family(Callback, ?RULES_WITH_TYPE, ?MG(rules_data, RawData)),
    ok = add_collect_family(Callback, ?CONNECTORS_WITH_TYPE, ?MG(connectors_data, RawData)),
    ok = maybe_collect_family_schema_registry(Callback),

    %% Rule Specific
    RuleSpecificDs = ?MG(rule_specific_data, RawData),
    ok = add_collect_family(Callback, ?RULES_SPECIFIC_WITH_TYPE, RuleSpecificDs),

    %% Action Specific
    ActionSpecificDs = ?MG(action_specific_data, RawData),
    ok = add_collect_family(Callback, ?ACTION_SPECIFIC_WITH_TYPE, ActionSpecificDs),

    %% Connector Specific
    ConnectorSpecificDs = ?MG(connector_specific_data, RawData),
    ok = add_collect_family(Callback, ?CONNECTOR_SPECIFIC_WITH_TYPE, ConnectorSpecificDs),

    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    RawData = raw_data(erlang:get(format_mode)),
    Rules = emqx_rule_engine:get_rules(),
    Bridges = emqx_bridge:list(),
    #{
        data_integration_overview => collect_data_integration_overview(Rules, Bridges),
        rules => collect_json_data(?MG(rule_specific_data, RawData)),
        actions => collect_json_data(?MG(action_specific_data, RawData)),
        connectors => collect_json_data(?MG(connector_specific_data, RawData))
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

%% @private
fetch_metric_data_from_local_node() ->
    Rules = emqx_rule_engine:get_rules(),
    Bridges = emqx_bridge:list(),
    {node(self()), #{
        rule_specific_data => rule_specific_data(Rules),
        action_specific_data => action_specific_data(Bridges),
        connector_specific_data => connector_specific_data(Bridges)
    }}.

fetch_cluster_consistented_metric_data() ->
    Rules = emqx_rule_engine:get_rules(),
    Bridges = emqx_bridge:list(),
    (maybe_collect_schema_registry())#{
        rules_data => rules_data(Rules),
        connectors_data => connectors_data(Bridges)
    }.

-if(?EMQX_RELEASE_EDITION == ee).
maybe_collect_family_schema_registry(Callback) ->
    ok = add_collect_family(Callback, ?SCHEMA_REGISTRY_WITH_TYPE, schema_registry_data()),
    ok.

maybe_collect_schema_registry() ->
    schema_registry_data().
-else.
maybe_collect_family_schema_registry(_) ->
    ok.

maybe_collect_schema_registry() ->
    #{}.
-endif.

%% raw data for different format modes
raw_data(nodes_aggregated) ->
    AggregatedNodesMetrics = aggre_cluster(metrics_data_from_all_nodes()),
    maps:merge(AggregatedNodesMetrics, fetch_cluster_consistented_metric_data());
raw_data(nodes_unaggregated) ->
    %% then fold from all nodes
    AllNodesMetrics = with_node_name_label(metrics_data_from_all_nodes()),
    maps:merge(AllNodesMetrics, fetch_cluster_consistented_metric_data());
raw_data(node) ->
    {_Node, LocalNodeMetrics} = fetch_metric_data_from_local_node(),
    maps:merge(LocalNodeMetrics, fetch_cluster_consistented_metric_data()).

metrics_data_from_all_nodes() ->
    Nodes = mria:running_nodes(),
    _ResL = emqx_prometheus_proto_v2:raw_prom_data(
        Nodes, ?MODULE, fetch_metric_data_from_local_node, []
    ).

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%========================================
%% Data Integration Overview
%%========================================

%%====================
%% All Rules
%% Rules
collect_di(K = emqx_rules_count, Data) ->
    gauge_metric(?MG(K, Data));
%%====================
%% Schema Registry
collect_di(K = emqx_schema_registrys_count, Data) ->
    gauge_metric(?MG(K, Data));
%%====================
%% Connectors
collect_di(K = emqx_connectors_count, Data) ->
    gauge_metric(?MG(K, Data));
%%========================================
%% Data Integration for Specific: Rule && Action && Connector
%%========================================

%%====================
%% Specific Rule
collect_di(K = emqx_rule_enable, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_matched, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_passed, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_exception, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_no_result, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_total, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_success, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_out_of_service, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_unknown, Data) ->
    counter_metrics(?MG(K, Data));
%%====================
%% Specific Action

collect_di(K = emqx_action_matched, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_success, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_failed, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_inflight, Data) ->
    %% inflight type: gauge
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_received, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_late_reply, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_success, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_failed, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_stopped, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_not_found, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_queue_full, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_other, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_expired, Data) ->
    counter_metrics(?MG(K, Data));
collect_di(K = emqx_action_queuing, Data) ->
    %% queuing type: gauge
    gauge_metrics(?MG(K, Data));
%%====================
%% Specific Connector

collect_di(K = emqx_connector_enable, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_connector_status, Data) ->
    gauge_metrics(?MG(K, Data)).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%========================================
%% Data Integration Overview
%%========================================

%%====================
%% All Rules

-define(RULE_TAB, emqx_rule_engine).
rules_data(_Rules) ->
    #{
        emqx_rules_count => ets:info(?RULE_TAB, size)
    }.

%%====================
%% Schema Registry

-if(?EMQX_RELEASE_EDITION == ee).
schema_registry_data() ->
    #{
        emqx_schema_registrys_count => erlang:map_size(emqx_schema_registry:list_schemas())
    }.
-else.
-endif.

%%====================
%% Connectors

connectors_data(Brdiges) ->
    #{
        %% Both Bridge V1 and V2
        emqx_connectors_count => erlang:length(Brdiges)
    }.

%%========================================
%% Data Integration for Specific: Rule && Action && Connector
%%========================================

%%====================
%% Specific Rule
%% With rule_id as label key: `rule_id`

rule_specific_data(Rules) ->
    lists:foldl(
        fun(#{id := Id} = Rule, AccIn) ->
            merge_acc_with_rules(Id, get_metric(Rule), AccIn)
        end,
        maps:from_keys(rule_specific_metric_names(), []),
        Rules
    ).

merge_acc_with_rules(Id, RuleMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [rule_point(Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        RuleMetrics
    ).

rule_point(Id, V) ->
    {[{id, Id}], V}.

get_metric(#{id := Id, enable := Bool} = _Rule) ->
    case emqx_metrics_worker:get_metrics(rule_metrics, Id) of
        #{counters := Counters} ->
            #{
                emqx_rule_enable => emqx_prometheus_utils:boolean_to_number(Bool),
                emqx_rule_matched => ?MG(matched, Counters),
                emqx_rule_failed => ?MG(failed, Counters),
                emqx_rule_passed => ?MG(passed, Counters),
                emqx_rule_failed_exception => ?MG('failed.exception', Counters),
                emqx_rule_failed_no_result => ?MG('failed.no_result', Counters),
                emqx_rule_actions_total => ?MG('actions.total', Counters),
                emqx_rule_actions_success => ?MG('actions.success', Counters),
                emqx_rule_actions_failed => ?MG('actions.failed', Counters),
                emqx_rule_actions_failed_out_of_service => ?MG(
                    'actions.failed.out_of_service', Counters
                ),
                emqx_rule_actions_failed_unknown => ?MG('actions.failed.unknown', Counters)
            }
    end.

rule_specific_metric_names() ->
    emqx_prometheus_utils:metric_names(?RULES_SPECIFIC_WITH_TYPE).

%%====================
%% Specific Action
%% With action_id: `{type}:{name}` as label key: `action_id`

action_specific_data(Bridges) ->
    lists:foldl(
        fun(#{type := Type, name := Name} = _Bridge, AccIn) ->
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            merge_acc_with_bridges(Id, get_bridge_metric(Type, Name), AccIn)
        end,
        maps:from_keys(action_specific_metric_names(), []),
        Bridges
    ).

merge_acc_with_bridges(Id, BridgeMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [action_point(Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        BridgeMetrics
    ).

action_point(Id, V) ->
    {[{id, Id}], V}.

get_bridge_metric(Type, Name) ->
    case emqx_bridge:get_metrics(Type, Name) of
        #{counters := Counters, gauges := Gauges} ->
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
                emqx_action_dropped_resource_not_found => ?MG0(
                    'dropped.resource_not_found', Counters
                ),
                emqx_action_dropped_queue_full => ?MG0('dropped.queue_full', Counters),
                emqx_action_dropped_other => ?MG0('dropped.other', Counters),
                emqx_action_dropped_expired => ?MG0('dropped.expired', Counters),
                emqx_action_queuing => ?MG0(queuing, Gauges)
            }
    end.

action_specific_metric_names() ->
    emqx_prometheus_utils:metric_names(?ACTION_SPECIFIC_WITH_TYPE).

%%====================
%% Specific Connector
%% With connector_id: `{type}:{name}` as label key: `connector_id`

connector_specific_data(Bridges) ->
    lists:foldl(
        fun(#{type := Type, name := Name} = Bridge, AccIn) ->
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            merge_acc_with_connectors(Id, get_connector_status(Bridge), AccIn)
        end,
        maps:from_keys(connectr_specific_metric_names(), []),
        Bridges
    ).

merge_acc_with_connectors(Id, ConnectorMetrics, PointsAcc) ->
    maps:fold(
        fun(K, V, AccIn) ->
            AccIn#{K => [connector_point(Id, V) | ?MG(K, AccIn)]}
        end,
        PointsAcc,
        ConnectorMetrics
    ).

connector_point(Id, V) ->
    {[{id, Id}], V}.

get_connector_status(#{resource_data := ResourceData} = _Bridge) ->
    Enabled = emqx_utils_maps:deep_get([config, enable], ResourceData),
    Status = ?MG(status, ResourceData),
    #{
        emqx_connector_enable => emqx_prometheus_utils:boolean_to_number(Enabled),
        emqx_connector_status => emqx_prometheus_utils:status_to_number(Status)
    }.

connectr_specific_metric_names() ->
    emqx_prometheus_utils:metric_names(?CONNECTOR_SPECIFIC_WITH_TYPE).

%%--------------------------------------------------------------------
%% Collect functions
%%--------------------------------------------------------------------

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `application/json`
collect_data_integration_overview(Rules, Bridges) ->
    RulesD = rules_data(Rules),
    ConnectorsD = connectors_data(Bridges),

    M1 = lists:foldl(
        fun(K, AccIn) -> AccIn#{K => ?MG(K, RulesD)} end,
        #{},
        emqx_prometheus_utils:metric_names(?RULES_WITH_TYPE)
    ),
    M2 = lists:foldl(
        fun(K, AccIn) -> AccIn#{K => ?MG(K, ConnectorsD)} end,
        #{},
        emqx_prometheus_utils:metric_names(?CONNECTORS_WITH_TYPE)
    ),
    M3 = maybe_collect_schema_registry(),

    lists:foldl(fun(M, AccIn) -> maps:merge(M, AccIn) end, #{}, [M1, M2, M3]).

collect_json_data(Data) ->
    emqx_prometheus_utils:collect_json_data(Data, fun zip_json_data_integration_metrics/3).

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
    ThisKeyResult = lists:foldl(emqx_prometheus_utils:point_to_map_fun(Key), [], Points),
    lists:zipwith(fun maps:merge/2, AllResultedAcc, ThisKeyResult).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% merge / zip formatting funcs for type `text/plain`
aggre_cluster(ResL) ->
    emqx_prometheus_utils:aggre_cluster(?LOGICAL_SUM_METRIC_NAMES, ResL, aggre_or_zip_init_acc()).

with_node_name_label(ResL) ->
    emqx_prometheus_utils:with_node_name_label(ResL, aggre_or_zip_init_acc()).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%% Helper funcs

aggre_or_zip_init_acc() ->
    #{
        rule_specific_data => maps:from_keys(rule_specific_metric_names(), []),
        action_specific_data => maps:from_keys(action_specific_metric_names(), []),
        connector_specific_data => maps:from_keys(connectr_specific_metric_names(), [])
    }.
