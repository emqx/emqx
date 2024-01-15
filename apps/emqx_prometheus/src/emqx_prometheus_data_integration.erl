%%--------------------------------------------------------------------
%% Copyright (c) 2022-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-export([add_collect_family/4]).

-export([
    rules/0,
    rules_data/1,
    actions/0,
    actions_data/1,
    actions_exec_count/0,
    actions_exec_count_data/0,
    schema_registry/0,
    schema_registry_data/0,
    connectors/0,
    connectors_data/1,
    rule_specific/0,
    rule_specific_data/1,
    action_specific/0,
    action_specific_data/1,
    connector_specific/0,
    connector_specific_data/1
]).

-include("emqx_prometheus.hrl").
-include_lib("prometheus/include/prometheus.hrl").

-import(
    prometheus_model_helpers,
    [
        create_mf/5,
        gauge_metric/1,
        gauge_metrics/1
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
%% Collector API
%%--------------------------------------------------------------------

%% @private
deregister_cleanup(_) -> ok.

%% @private
-spec collect_mf(_Registry, Callback) -> ok when
    _Registry :: prometheus_registry:registry(),
    Callback :: prometheus_collector:collect_mf_callback().
%% erlfmt-ignore
collect_mf(?PROMETHEUS_DATA_INTEGRATION_REGISTRY, Callback) ->
    Rules = emqx_rule_engine:get_rules(),
    Bridges =emqx_bridge:list(),
    _ = [add_collect_family(Name, rules_data(Rules), Callback, gauge) || Name <- rules()],
    _ = [add_collect_family(Name, actions_data(Rules), Callback, gauge) || Name <- actions()],
    _ = [add_collect_family(Name, connectors_data(Bridges), Callback, gauge) || Name <- connectors()],
    _ = [add_collect_family(Name, rule_specific_data(Rules), Callback, gauge) || Name <- rule_specific()],
    _ = [add_collect_family(Name, action_specific_data(Bridges), Callback, gauge) || Name <- action_specific()],
    _ = [add_collect_family(Name, connector_specific_data(Bridges), Callback, gauge) || Name <- connector_specific()],
    ok = maybe_collect_family_schema_registry(Callback),
    ok;
collect_mf(_, _) ->
    ok.

%% @private
collect(<<"json">>) ->
    %% TODO
    #{};
collect(<<"prometheus">>) ->
    prometheus_text_format:format(?PROMETHEUS_DATA_INTEGRATION_REGISTRY).

add_collect_family(Name, Data, Callback, Type) ->
    Callback(create_mf(Name, _Help = <<"">>, Type, ?MODULE, Data)).

collect_metrics(Name, Metrics) ->
    collect_di(Name, Metrics).

-if(?EMQX_RELEASE_EDITION == ee).
maybe_collect_family_schema_registry(Callback) ->
    _ = [
        add_collect_family(Name, schema_registry_data(), Callback, gauge)
     || Name <- schema_registry()
    ],
    ok.
-else.
maybe_collect_family_schema_registry(_) ->
    ok.
-endif.

%%--------------------------------------------------------------------
%% Collector
%%--------------------------------------------------------------------

%%========================================
%% Data Integration Overview
%%========================================

%%====================
%% All Rules
%% Rules
collect_di(K = emqx_rule_count, Data) ->
    gauge_metric(?MG(K, Data));
collect_di(K = emqx_rules_matched_rate, Data) ->
    gauge_metric(?MG(K, Data));
collect_di(K = emqx_rules_matched_rate_last5m, Data) ->
    gauge_metric(?MG(K, Data));
%%====================
%% All Actions
collect_di(K = emqx_rules_actions_rate, Data) ->
    gauge_metric(?MG(K, Data));
collect_di(K = emqx_rules_actions_rate_last5m, Data) ->
    gauge_metric(?MG(K, Data));
%%====================
%% Schema Registry
collect_di(K = emqx_schema_registry_count, Data) ->
    gauge_metric(?MG(K, Data));
%%====================
%% Connectors
collect_di(K = emqx_connector_count, Data) ->
    gauge_metric(?MG(K, Data));
%%========================================
%% Data Integration for Specific: Rule && Action && Connector
%%========================================

%%====================
%% Specific Rule
collect_di(K = emqx_rule_matched, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_passed, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_exception, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_failed_no_result, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_total, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_success, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_out_of_service, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_actions_failed_unknown, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_matched_rate, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_matched_rate_last5m, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_rule_matched_rate_max, Data) ->
    gauge_metrics(?MG(K, Data));
%%====================
%% Specific Action

collect_di(K = emqx_action_matched, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_success, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_failed, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_rate, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_inflight, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_received, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_late_reply, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_success, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_retried_failed, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_stopped, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_resource_not_found, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_queue_full, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_other, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_dropped_expired, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_queuing, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_rate_last5m, Data) ->
    gauge_metrics(?MG(K, Data));
collect_di(K = emqx_action_rate_max, Data) ->
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

rules() ->
    [
        emqx_rule_count,
        emqx_rules_matched_rate,
        emqx_rules_matched_rate_last5m
    ].

-define(RULE_TAB, emqx_rule_engine).

rules_data(Rules) ->
    Rate = lists:foldl(
        fun(
            #{id := Id},
            #{emqx_rules_matched_rate := Rate, emqx_rules_matched_rate_last5m := RateLast5m} = AccIn
        ) ->
            RuleMetrics = emqx_metrics_worker:get_metrics(rule_metrics, Id),
            AccIn#{
                emqx_rules_matched_rate => Rate +
                    emqx_utils_maps:deep_get([rate, matched, current], RuleMetrics, 0),
                emqx_rules_matched_rate_last5m => RateLast5m +
                    emqx_utils_maps:deep_get([rate, matched, last5m], RuleMetrics, 0)
            }
        end,
        _InitAcc = maps:from_keys(rules(), 0),
        Rules
    ),
    Rate#{emqx_rule_count => ets:info(?RULE_TAB, size)}.

%%====================
%% All Actions

actions() ->
    [
        emqx_rules_actions_rate,
        emqx_rules_actions_rate_last5m
    ].

actions_data(Rules) ->
    lists:foldl(
        fun(
            #{id := Id},
            #{emqx_rules_actions_rate := Rate, emqx_rules_actions_rate_last5m := RateLast5m} =
                _AccIn
        ) ->
            RuleMetrics = emqx_metrics_worker:get_metrics(rule_metrics, Id),
            _AccIn#{
                emqx_rules_actions_rate => Rate +
                    emqx_utils_maps:deep_get([rate, matched, current], RuleMetrics, 0),
                emqx_rules_actions_rate_last5m => RateLast5m +
                    emqx_utils_maps:deep_get([rate, matched, last5m], RuleMetrics, 0)
            }
        end,
        _InitAcc = maps:from_keys(actions(), 0),
        Rules
    ).

actions_exec_count() ->
    [
        emqx_action_sink,
        emqx_action_source
    ].

actions_exec_count_data() ->
    [].

%%====================
%% Schema Registry

-if(?EMQX_RELEASE_EDITION == ee).
schema_registry() ->
    [
        emqx_schema_registry_count
    ].

schema_registry_data() ->
    #{
        emqx_schema_registry_count => erlang:map_size(emqx_schema_registry:list_schemas())
    }.
-else.

-endif.

%%====================
%% Connectors

connectors() ->
    [
        emqx_connector_count
    ].

connectors_data(Brdiges) ->
    #{
        %% Both Bridge V1 and V2
        emqx_connector_count => erlang:length(Brdiges)
    }.

%%========================================
%% Data Integration for Specific: Rule && Action && Connector
%%========================================

%%====================
%% Specific Rule
%% With rule_id as label key: `rule_id`

rule_specific() ->
    [
        emqx_rule_matched,
        emqx_rule_failed,
        emqx_rule_passed,
        emqx_rule_failed_exception,
        emqx_rule_failed_no_result,
        emqx_rule_actions_total,
        emqx_rule_actions_success,
        emqx_rule_actions_failed,
        emqx_rule_actions_failed_out_of_service,
        emqx_rule_actions_failed_unknown,
        emqx_rule_matched_rate,
        emqx_rule_matched_rate_last5m,
        emqx_rule_matched_rate_max
    ].

rule_specific_data(Rules) ->
    lists:foldl(
        fun(#{id := Id} = Rule, AccIn) ->
            merge_acc_with_rules(Id, get_metric(Rule), AccIn)
        end,
        maps:from_keys(rule_specific(), []),
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

get_metric(#{id := Id} = _Rule) ->
    case emqx_metrics_worker:get_metrics(rule_metrics, Id) of
        #{counters := Counters, rate := #{matched := MatchedRate}} ->
            #{
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
                emqx_rule_actions_failed_unknown => ?MG('actions.failed.unknown', Counters),
                emqx_rule_matched_rate => ?MG(current, MatchedRate),
                emqx_rule_matched_rate_last5m => ?MG(last5m, MatchedRate),
                emqx_rule_matched_rate_max => ?MG(max, MatchedRate)
            }
    end.

%%====================
%% Specific Action
%% With action_id: `{type}:{name}` as label key: `action_id`

action_specific() ->
    [
        emqx_action_matched,
        emqx_action_dropped,
        emqx_action_success,
        emqx_action_failed,
        emqx_action_rate,
        emqx_action_inflight,
        emqx_action_received,
        emqx_action_late_reply,
        emqx_action_retried,
        emqx_action_retried_success,
        emqx_action_retried_failed,
        emqx_action_dropped_resource_stopped,
        emqx_action_dropped_resource_not_found,
        emqx_action_dropped_queue_full,
        emqx_action_dropped_other,
        emqx_action_dropped_expired,
        emqx_action_queuing,
        emqx_action_rate_last5m,
        emqx_action_rate_max
    ].

action_specific_data(Bridges) ->
    lists:foldl(
        fun(#{type := Type, name := Name} = _Bridge, AccIn) ->
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            merge_acc_with_bridges(Id, get_bridge_metric(Type, Name), AccIn)
        end,
        maps:from_keys(action_specific(), []),
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
        #{counters := Counters, rate := #{matched := MatchedRate}, gauges := Gauges} ->
            #{
                emqx_action_matched => ?MG0(matched, Counters),
                emqx_action_dropped => ?MG0(dropped, Counters),
                emqx_action_success => ?MG0(success, Counters),
                emqx_action_failed => ?MG0(failed, Counters),
                emqx_action_rate => ?MG0(current, MatchedRate),
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
                emqx_action_queuing => ?MG0(queuing, Gauges),
                emqx_action_rate_last5m => ?MG0(last5m, MatchedRate),
                emqx_action_rate_max => ?MG0(max, MatchedRate)
            }
    end.

%% TODO: Bridge V2

%%====================
%% Specific Connector
%% With connector_id: `{type}:{name}` as label key: `connector_id`

connector_specific() ->
    [
        emqx_connector_enable,
        emqx_connector_status
    ].

connector_specific_data(Bridges) ->
    lists:foldl(
        fun(#{type := Type, name := Name} = Bridge, AccIn) ->
            Id = emqx_bridge_resource:bridge_id(Type, Name),
            merge_acc_with_connectors(Id, get_connector_status(Bridge), AccIn)
        end,
        maps:from_keys(connector_specific(), []),
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
        emqx_connector_enable => boolean_to_number(Enabled),
        emqx_connector_status => status_to_number(Status)
    }.

%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Help funcs

boolean_to_number(true) -> 1;
boolean_to_number(false) -> 0.

status_to_number(connected) -> 1;
status_to_number(disconnected) -> 0.
