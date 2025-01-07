%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_rule_api_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").
-include("rule_engine.hrl").

-export([check_params/2]).

-export([namespace/0, roots/0, fields/1]).

-type tag() :: rule_creation | rule_test | rule_engine | rule_apply_test.

-spec check_params(map(), tag()) -> {ok, map()} | {error, term()}.
check_params(Params, Tag) ->
    BTag = atom_to_binary(Tag),
    Opts = #{atom_key => true, required => false},
    try hocon_tconf:check_plain(?MODULE, #{BTag => Params}, Opts, [Tag]) of
        #{Tag := Checked} -> {ok, Checked}
    catch
        throw:Reason ->
            ?SLOG(
                info,
                #{
                    msg => "check_rule_params_failed",
                    reason => Reason
                },
                #{tag => ?TAG}
            ),
            {error, Reason}
    end.

%%======================================================================================
%% Hocon Schema Definitions

namespace() -> "rule_engine".

roots() ->
    [
        {"rule_engine", sc(ref("rule_engine"), #{desc => ?DESC("root_rule_engine")})},
        {"rule_creation", sc(ref("rule_creation"), #{desc => ?DESC("root_rule_creation")})},
        {"rule_info", sc(ref("rule_info"), #{desc => ?DESC("root_rule_info")})},
        {"rule_events", sc(ref("rule_events"), #{desc => ?DESC("root_rule_events")})},
        {"rule_test", sc(ref("rule_test"), #{desc => ?DESC("root_rule_test")})},
        {"rule_apply_test", sc(ref("rule_apply_test"), #{desc => ?DESC("root_apply_rule_test")})}
    ].

fields("rule_engine") ->
    emqx_rule_engine_schema:rule_engine_settings();
fields("rule_creation") ->
    emqx_rule_engine_schema:fields("rules");
fields("rule_info") ->
    [
        rule_id(),
        {"from",
            sc(
                hoconsc:array(binary()),
                #{desc => ?DESC("ri_from"), example => "t/#"}
            )},
        {"created_at",
            sc(
                binary(),
                #{
                    desc => ?DESC("ri_created_at"),
                    example => "2021-12-01T15:00:43.153+08:00"
                }
            )}
    ] ++ fields("rule_creation");
fields("rule_metrics") ->
    [
        rule_id(),
        {"metrics", sc(ref("metrics"), #{desc => ?DESC("ri_metrics")})},
        {"node_metrics",
            sc(
                hoconsc:array(ref("node_metrics")),
                #{desc => ?DESC("ri_node_metrics")}
            )}
    ];
%% TODO: we can delete this API if the Dashboard not depends on it
fields("rule_events") ->
    ETopics = emqx_rule_events:event_topics_enum(),
    [
        {"event", sc(hoconsc:enum(ETopics), #{desc => ?DESC("rs_event"), required => true})},
        {"title", sc(binary(), #{desc => ?DESC("rs_title"), example => "some title"})},
        {"description", sc(binary(), #{desc => ?DESC("rs_description"), example => "some desc"})},
        {"columns", sc(map(), #{desc => ?DESC("rs_columns")})},
        {"test_columns", sc(map(), #{desc => ?DESC("rs_test_columns")})},
        {"sql_example", sc(binary(), #{desc => ?DESC("rs_sql_example")})}
    ];
fields("rule_test") ->
    [
        rule_input_message_context(),
        {"sql", sc(binary(), #{desc => ?DESC("test_sql"), required => true})}
    ];
fields("rule_apply_test") ->
    [
        rule_input_message_context(),
        {"stop_action_after_template_rendering",
            sc(
                typerefl:boolean(),
                #{
                    desc =>
                        ?DESC("stop_action_after_template_render"),
                    default => true
                }
            )}
    ];
fields("metrics") ->
    [
        {"matched",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_sql_matched")
            })},
        {"matched.rate", sc(float(), #{desc => ?DESC("metrics_sql_matched_rate")})},
        {"matched.rate.max", sc(float(), #{desc => ?DESC("metrics_sql_matched_rate_max")})},
        {"matched.rate.last5m",
            sc(
                float(),
                #{desc => ?DESC("metrics_sql_matched_rate_last5m")}
            )},
        {"passed", sc(non_neg_integer(), #{desc => ?DESC("metrics_sql_passed")})},
        {"failed", sc(non_neg_integer(), #{desc => ?DESC("metrics_sql_failed")})},
        {"failed.exception",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_sql_failed_exception")
            })},
        {"failed.unknown",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_sql_failed_unknown")
            })},
        {"actions.total",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_total")
            })},
        {"actions.success",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_success")
            })},
        {"actions.failed",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_failed")
            })},
        {"actions.failed.out_of_service",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_failed_out_of_service")
            })},
        {"actions.failed.unknown",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_failed_unknown")
            })},
        {"actions.discarded",
            sc(non_neg_integer(), #{
                desc => ?DESC("metrics_actions_discarded")
            })}
    ];
fields("node_metrics") ->
    [{"node", sc(binary(), #{desc => ?DESC("node_node"), example => "emqx@127.0.0.1"})}] ++
        fields("metrics");
fields("ctx_pub") ->
    Event = 'message.publish',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})}
        | msg_event_common_fields()
    ];
fields("ctx_sub") ->
    Event = 'session.subscribed',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)}
        | msg_event_common_fields()
    ];
fields("ctx_unsub") ->
    Event = 'session.unsubscribed',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)}
        | without(["event_type", "event_topic", "event"], fields("ctx_sub"))
    ];
fields("ctx_delivered") ->
    Event = 'message.delivered',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"from_clientid", sc(binary(), #{desc => ?DESC("event_from_clientid")})},
        {"from_username", sc(binary(), #{desc => ?DESC("event_from_username")})}
        | msg_event_common_fields()
    ];
fields("ctx_acked") ->
    Event = 'message.acked',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)}
        | without(["event_type", "event_topic", "event"], fields("ctx_delivered"))
    ];
fields("ctx_dropped") ->
    Event = 'message.dropped',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"reason", sc(binary(), #{desc => ?DESC("event_ctx_dropped")})}
        | msg_event_common_fields()
    ];
fields("ctx_connected") ->
    Event = 'client.connected',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"mountpoint", sc(binary(), #{desc => ?DESC("event_mountpoint")})},
        {"peername", sc(binary(), #{desc => ?DESC("event_peername")})},
        {"sockname", sc(binary(), #{desc => ?DESC("event_sockname")})},
        {"proto_name", sc(binary(), #{desc => ?DESC("event_proto_name")})},
        {"proto_ver", sc(binary(), #{desc => ?DESC("event_proto_ver")})},
        {"keepalive", sc(integer(), #{desc => ?DESC("event_keepalive")})},
        {"clean_start", sc(boolean(), #{desc => ?DESC("event_clean_start"), default => true})},
        {"expiry_interval", sc(integer(), #{desc => ?DESC("event_expiry_interval")})},
        {"is_bridge", sc(boolean(), #{desc => ?DESC("event_is_bridge"), default => false})},
        {"connected_at",
            sc(integer(), #{
                desc => ?DESC("event_connected_at")
            })}
    ];
fields("ctx_disconnected") ->
    Event = 'client.disconnected',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"reason", sc(binary(), #{desc => ?DESC("event_ctx_disconnected_reason")})},
        {"peername", sc(binary(), #{desc => ?DESC("event_peername")})},
        {"sockname", sc(binary(), #{desc => ?DESC("event_sockname")})},
        {"disconnected_at",
            sc(integer(), #{
                desc => ?DESC("event_ctx_disconnected_da")
            })}
    ];
fields("ctx_connack") ->
    Event = 'client.connack',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"reason_code", sc(binary(), #{desc => ?DESC("event_ctx_connack_reason_code")})},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"clean_start", sc(boolean(), #{desc => ?DESC("event_clean_start"), default => true})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"peername", sc(binary(), #{desc => ?DESC("event_peername")})},
        {"sockname", sc(binary(), #{desc => ?DESC("event_sockname")})},
        {"proto_name", sc(binary(), #{desc => ?DESC("event_proto_name")})},
        {"proto_ver", sc(binary(), #{desc => ?DESC("event_proto_ver")})},
        {"keepalive", sc(integer(), #{desc => ?DESC("event_keepalive")})},
        {"expiry_interval", sc(integer(), #{desc => ?DESC("event_expiry_interval")})},
        {"connected_at",
            sc(integer(), #{
                desc => ?DESC("event_connected_at")
            })}
    ];
fields("ctx_check_authz_complete") ->
    Event = 'client.check_authz_complete',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"peerhost", sc(binary(), #{desc => ?DESC("event_peerhost")})},
        {"topic", sc(binary(), #{desc => ?DESC("event_topic")})},
        {"action", sc(binary(), #{desc => ?DESC("event_action")})},
        {"authz_source", sc(binary(), #{desc => ?DESC("event_authz_source")})},
        {"result", sc(binary(), #{desc => ?DESC("event_result")})}
    ];
fields("ctx_check_authn_complete") ->
    Event = 'client.check_authn_complete',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"reason_code", sc(binary(), #{desc => ?DESC("event_ctx_authn_reason_code")})},
        {"peername", sc(binary(), #{desc => ?DESC("event_peername")})},
        {"is_anonymous", sc(boolean(), #{desc => ?DESC("event_is_anonymous"), required => false})},
        {"is_superuser", sc(boolean(), #{desc => ?DESC("event_is_superuser"), required => false})}
    ];
fields("ctx_bridge_mqtt") ->
    Event = '$bridges/mqtt:*',
    EventBin = atom_to_binary(Event),
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(EventBin)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"payload", sc(binary(), #{desc => ?DESC("event_payload")})},
        {"topic", sc(binary(), #{desc => ?DESC("event_topic")})},
        {"server", sc(binary(), #{desc => ?DESC("event_server")})},
        {"dup", sc(binary(), #{desc => ?DESC("event_dup")})},
        {"retain", sc(binary(), #{desc => ?DESC("event_retain")})},
        {"message_received_at", publish_received_at_sc()},
        qos()
    ];
fields("ctx_delivery_dropped") ->
    Event = 'delivery.dropped',
    [
        {"event_type", event_type_sc(Event)},
        {"event", event_sc(Event)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"reason", sc(binary(), #{desc => ?DESC("event_ctx_dropped")})},
        {"from_clientid", sc(binary(), #{desc => ?DESC("event_from_clientid")})},
        {"from_username", sc(binary(), #{desc => ?DESC("event_from_username")})}
        | msg_event_common_fields()
    ];
fields("ctx_schema_validation_failed") ->
    Event = 'schema.validation_failed',
    [
        {"event_type", event_type_sc(Event)},
        {"validation", sc(binary(), #{desc => ?DESC("event_validation")})}
        | msg_event_common_fields()
    ];
fields("ctx_message_transformation_failed") ->
    Event = 'message.transformation_failed',
    [
        {"event_type", event_type_sc(Event)},
        {"transformation", sc(binary(), #{desc => ?DESC("event_transformation")})}
        | msg_event_common_fields()
    ].

rule_input_message_context() ->
    {"context",
        sc(
            hoconsc:union([
                ref("ctx_pub"),
                ref("ctx_sub"),
                ref("ctx_unsub"),
                ref("ctx_delivered"),
                ref("ctx_acked"),
                ref("ctx_dropped"),
                ref("ctx_connected"),
                ref("ctx_disconnected"),
                ref("ctx_connack"),
                ref("ctx_check_authz_complete"),
                ref("ctx_check_authn_complete"),
                ref("ctx_bridge_mqtt"),
                ref("ctx_delivery_dropped"),
                ref("ctx_schema_validation_failed"),
                ref("ctx_message_transformation_failed")
            ]),
            #{
                desc => ?DESC("test_context"),
                default => #{}
            }
        )}.

qos() ->
    {"qos", sc(emqx_schema:qos(), #{desc => ?DESC("event_qos")})}.

rule_id() ->
    {"id",
        sc(
            binary(),
            #{
                desc => ?DESC("rule_id"),
                required => true,
                example => "293fb66f"
            }
        )}.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).

ref(Field) -> hoconsc:ref(?MODULE, Field).

event_type_sc(Event) ->
    EventType = event_to_event_type(Event),
    sc(EventType, #{desc => ?DESC("event_event_type"), required => true}).

-spec event_to_event_type(atom()) -> atom().
event_to_event_type(Event) ->
    binary_to_atom(binary:replace(atom_to_binary(Event), <<".">>, <<"_">>)).

event_sc(Event) when is_binary(Event) ->
    %% only exception is `$bridges/...'.
    sc(binary(), #{default => Event, importance => ?IMPORTANCE_HIDDEN});
event_sc(Event) ->
    sc(Event, #{default => Event, importance => ?IMPORTANCE_HIDDEN}).

without(FieldNames, Fields) ->
    lists:foldl(fun proplists:delete/2, Fields, FieldNames).

publish_received_at_sc() ->
    sc(integer(), #{desc => ?DESC("event_publish_received_at")}).

msg_event_common_fields() ->
    [
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"payload", sc(binary(), #{desc => ?DESC("event_payload")})},
        {"peerhost", sc(binary(), #{desc => ?DESC("event_peerhost")})},
        {"topic", sc(binary(), #{desc => ?DESC("event_topic")})},
        {"publish_received_at", publish_received_at_sc()},
        qos()
    ].
