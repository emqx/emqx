-module(emqx_rule_api_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").
-include_lib("emqx/include/logger.hrl").

-export([check_params/2]).

-export([roots/0, fields/1]).

-type tag() :: rule_creation | rule_test.

-spec check_params(map(), tag()) -> {ok, map()} | {error, term()}.
check_params(Params, Tag) ->
    BTag = atom_to_binary(Tag),
    Opts = #{atom_key => true, required => false},
    try hocon_tconf:check_plain(?MODULE, #{BTag => Params}, Opts, [Tag]) of
        #{Tag := Checked} -> {ok, Checked}
    catch
        throw:Reason ->
            ?SLOG(error, #{
                msg => "check_rule_params_failed",
                reason => Reason
            }),
            {error, Reason}
    end.

%%======================================================================================
%% Hocon Schema Definitions

roots() ->
    [
        {"rule_creation", sc(ref("rule_creation"), #{desc => ?DESC("root_rule_creation")})},
        {"rule_info", sc(ref("rule_info"), #{desc => ?DESC("root_rule_info")})},
        {"rule_events", sc(ref("rule_events"), #{desc => ?DESC("root_rule_events")})},
        {"rule_test", sc(ref("rule_test"), #{desc => ?DESC("root_rule_test")})}
    ].

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
                    ref("ctx_bridge_mqtt")
                ]),
                #{
                    desc => ?DESC("test_context"),
                    default => #{}
                }
            )},
        {"sql", sc(binary(), #{desc => ?DESC("test_sql"), required => true})}
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
            })}
    ];
fields("node_metrics") ->
    [{"node", sc(binary(), #{desc => ?DESC("node_node"), example => "emqx@127.0.0.1"})}] ++
        fields("metrics");
fields("ctx_pub") ->
    [
        {"event_type", event_type_sc(message_publish)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})}
        | msg_event_common_fields()
    ];
fields("ctx_sub") ->
    [
        {"event_type", event_type_sc(session_subscribed)}
        | msg_event_common_fields()
    ];
fields("ctx_unsub") ->
    [
        {"event_type", event_type_sc(session_unsubscribed)}
        | proplists:delete("event_type", fields("ctx_sub"))
    ];
fields("ctx_delivered") ->
    [
        {"event_type", event_type_sc(message_delivered)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"from_clientid", sc(binary(), #{desc => ?DESC("event_from_clientid")})},
        {"from_username", sc(binary(), #{desc => ?DESC("event_from_username")})}
        | msg_event_common_fields()
    ];
fields("ctx_acked") ->
    [
        {"event_type", event_type_sc(message_acked)}
        | proplists:delete("event_type", fields("ctx_delivered"))
    ];
fields("ctx_dropped") ->
    [
        {"event_type", event_type_sc(message_dropped)},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"reason", sc(binary(), #{desc => ?DESC("event_ctx_dropped")})}
        | msg_event_common_fields()
    ];
fields("ctx_connected") ->
    [
        {"event_type", event_type_sc(client_connected)},
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
    [
        {"event_type", event_type_sc(client_disconnected)},
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
    [
        {"event_type", event_type_sc(client_connack)},
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
    [
        {"event_type", event_type_sc(client_check_authz_complete)},
        {"clientid", sc(binary(), #{desc => ?DESC("event_clientid")})},
        {"username", sc(binary(), #{desc => ?DESC("event_username")})},
        {"peerhost", sc(binary(), #{desc => ?DESC("event_peerhost")})},
        {"topic", sc(binary(), #{desc => ?DESC("event_topic")})},
        {"action", sc(binary(), #{desc => ?DESC("event_action")})},
        {"authz_source", sc(binary(), #{desc => ?DESC("event_authz_source")})},
        {"result", sc(binary(), #{desc => ?DESC("event_result")})}
    ];
fields("ctx_bridge_mqtt") ->
    [
        {"event_type", event_type_sc('$bridges/mqtt:*')},
        {"id", sc(binary(), #{desc => ?DESC("event_id")})},
        {"payload", sc(binary(), #{desc => ?DESC("event_payload")})},
        {"topic", sc(binary(), #{desc => ?DESC("event_topic")})},
        {"server", sc(binary(), #{desc => ?DESC("event_server")})},
        {"dup", sc(binary(), #{desc => ?DESC("event_dup")})},
        {"retain", sc(binary(), #{desc => ?DESC("event_retain")})},
        {"message_received_at", publish_received_at_sc()},
        qos()
    ].

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
    sc(Event, #{desc => ?DESC("event_event_type"), required => true}).

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
