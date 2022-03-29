-module(emqx_rule_api_schema).

-behaviour(hocon_schema).

-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/logger.hrl").

-export([ check_params/2
        ]).

-export([roots/0, fields/1]).

-type tag() :: rule_creation | rule_test.

-spec check_params(map(), tag()) -> {ok, map()} | {error, term()}.
check_params(Params, Tag) ->
    BTag = atom_to_binary(Tag),
    Opts = #{atom_key => true, required => false},
    try hocon_tconf:check_plain(?MODULE, #{BTag => Params}, Opts, [Tag]) of
        #{Tag := Checked} -> {ok, Checked}
    catch
        throw : Reason ->
            ?SLOG(error, #{msg => "check_rule_params_failed",
                           reason => Reason
                          }),
            {error, Reason}
    end.

%%======================================================================================
%% Hocon Schema Definitions

roots() ->
    [ {"rule_creation", sc(ref("rule_creation"), #{desc => "Schema for creating rules"})}
    , {"rule_info", sc(ref("rule_info"), #{desc => "Schema for rule info"})}
    , {"rule_events", sc(ref("rule_events"), #{desc => "Schema for rule events"})}
    , {"rule_test", sc(ref("rule_test"), #{desc => "Schema for testing rules"})}
    ].

fields("rule_creation") ->
    emqx_rule_engine_schema:fields("rules");

fields("rule_info") ->
    [ rule_id()
    , {"metrics", sc(ref("metrics"), #{desc => "The metrics of the rule"})}
    , {"node_metrics", sc(hoconsc:array(ref("node_metrics")),
        #{ desc => "The metrics of the rule for each node"
         })}
    , {"from", sc(hoconsc:array(binary()),
        #{desc => "The topics of the rule", example => "t/#"})}
    , {"created_at", sc(binary(),
        #{ desc => "The created time of the rule"
         , example => "2021-12-01T15:00:43.153+08:00"
         })}
    ] ++ fields("rule_creation");

%% TODO: we can delete this API if the Dashboard not denpends on it
fields("rule_events") ->
    ETopics = [binary_to_atom(emqx_rule_events:event_topic(E)) || E <- emqx_rule_events:event_names()],
    [ {"event", sc(hoconsc:enum(ETopics), #{desc => "The event topics", required => true})}
    , {"title", sc(binary(), #{desc => "The title", example => "some title"})}
    , {"description", sc(binary(), #{desc => "The description", example => "some desc"})}
    , {"columns", sc(map(), #{desc => "The columns"})}
    , {"test_columns", sc(map(), #{desc => "The test columns"})}
    , {"sql_example", sc(binary(), #{desc => "The sql_example"})}
    ];

fields("rule_test") ->
    [ {"context", sc(hoconsc:union([ ref("ctx_pub")
                                   , ref("ctx_sub")
                                   , ref("ctx_unsub")
                                   , ref("ctx_delivered")
                                   , ref("ctx_acked")
                                   , ref("ctx_dropped")
                                   , ref("ctx_connected")
                                   , ref("ctx_disconnected")
                                   , ref("ctx_connack")
                                   , ref("ctx_check_authz_complete")
                                   , ref("ctx_bridge_mqtt")
                                   ]),
        #{desc => "The context of the event for testing",
          default => #{}})}
    , {"sql", sc(binary(), #{desc => "The SQL of the rule for testing", required => true})}
    ];

fields("metrics") ->
    [ {"sql.matched", sc(integer(), #{
            desc => "How much times the FROM clause of the SQL is matched."
        })}
    , {"sql.matched.rate", sc(float(), #{desc => "The rate of matched, times/second"})}
    , {"sql.matched.rate.max", sc(float(), #{desc => "The max rate of matched, times/second"})}
    , {"sql.matched.rate.last5m", sc(float(),
        #{desc => "The average rate of matched in last 5 minutes, times/second"})}
    , {"sql.passed", sc(integer(), #{desc => "How much times the SQL is passed"})}
    , {"sql.failed", sc(integer(), #{desc => "How much times the SQL is failed"})}
    , {"sql.failed.exception", sc(integer(), #{
            desc => "How much times the SQL is failed due to exceptions. "
                    "This may because of a crash when calling a SQL function, or "
                    "trying to do arithmetic operation on undefined variables"
        })}
    , {"sql.failed.unknown", sc(integer(), #{
            desc => "How much times the SQL is failed due to an unknown error."
        })}
    , {"outputs.total", sc(integer(), #{
            desc => "How much times the outputs are called by the rule. "
                    "This value may several times of 'sql.matched', depending on the "
                    "number of the outputs of the rule."
        })}
    , {"outputs.success", sc(integer(), #{
            desc => "How much times the rule success to call the outputs."
        })}
    , {"outputs.failed", sc(integer(), #{
            desc => "How much times the rule failed to call the outputs."
        })}
    , {"outputs.failed.out_of_service", sc(integer(), #{
            desc => "How much times the rule failed to call outputs due to the output is "
                    "out of service. For example, a bridge is disabled or stopped."
        })}
    , {"outputs.failed.unknown", sc(integer(), #{
            desc => "How much times the rule failed to call outputs due to to an unknown error."
        })}
    ];

fields("node_metrics") ->
    [ {"node", sc(binary(), #{desc => "The node name", example => "emqx@127.0.0.1"})}
    ] ++ fields("metrics");

fields("ctx_pub") ->
    [ {"event_type", sc(message_publish, #{desc => "Event Type", required => true})}
    , {"id", sc(binary(), #{desc => "Message ID"})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"publish_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()];

fields("ctx_sub") ->
    [ {"event_type", sc(session_subscribed, #{desc => "Event Type", required => true})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"publish_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()];

fields("ctx_unsub") ->
    [{"event_type", sc(session_unsubscribed, #{desc => "Event Type", required => true})}] ++
    proplists:delete("event_type", fields("ctx_sub"));

fields("ctx_delivered") ->
    [ {"event_type", sc(message_delivered, #{desc => "Event Type", required => true})}
    , {"id", sc(binary(), #{desc => "Message ID"})}
    , {"from_clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"from_username", sc(binary(), #{desc => "The User Name"})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"publish_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()];

fields("ctx_acked") ->
    [{"event_type", sc(message_acked, #{desc => "Event Type", required => true})}] ++
    proplists:delete("event_type", fields("ctx_delivered"));

fields("ctx_dropped") ->
    [ {"event_type", sc(message_dropped, #{desc => "Event Type", required => true})}
    , {"id", sc(binary(), #{desc => "Message ID"})}
    , {"reason", sc(binary(), #{desc => "The Reason for Dropping"})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"publish_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()];

fields("ctx_connected") ->
    [ {"event_type", sc(client_connected, #{desc => "Event Type", required => true})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"mountpoint", sc(binary(), #{desc => "The Mountpoint"})}
    , {"peername", sc(binary(), #{desc => "The IP Address and Port of the Peer Client"})}
    , {"sockname", sc(binary(), #{desc => "The IP Address and Port of the Local Listener"})}
    , {"proto_name", sc(binary(), #{desc => "Protocol Name"})}
    , {"proto_ver", sc(binary(), #{desc => "Protocol Version"})}
    , {"keepalive", sc(integer(), #{desc => "KeepAlive"})}
    , {"clean_start", sc(boolean(), #{desc => "Clean Start", default => true})}
    , {"expiry_interval", sc(integer(), #{desc => "Expiry Interval"})}
    , {"is_bridge", sc(boolean(), #{desc => "Is Bridge", default => false})}
    , {"connected_at", sc(integer(), #{
        desc => "The Time that this Client is Connected"})}
    ];

fields("ctx_disconnected") ->
    [ {"event_type", sc(client_disconnected, #{desc => "Event Type", required => true})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"reason", sc(binary(), #{desc => "The Reason for Disconnect"})}
    , {"peername", sc(binary(), #{desc => "The IP Address and Port of the Peer Client"})}
    , {"sockname", sc(binary(), #{desc => "The IP Address and Port of the Local Listener"})}
    , {"disconnected_at", sc(integer(), #{
        desc => "The Time that this Client is Disconnected"})}
    ];

fields("ctx_connack") ->
    [ {"event_type", sc(client_connack, #{desc => "Event Type", required => true})}
    , {"reason_code", sc(binary(), #{desc => "The reason code"})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"clean_start", sc(boolean(), #{desc => "Clean Start", default => true})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"peername", sc(binary(), #{desc => "The IP Address and Port of the Peer Client"})}
    , {"sockname", sc(binary(), #{desc => "The IP Address and Port of the Local Listener"})}
    , {"proto_name", sc(binary(), #{desc => "Protocol Name"})}
    , {"proto_ver", sc(binary(), #{desc => "Protocol Version"})}
    , {"keepalive", sc(integer(), #{desc => "KeepAlive"})}
    , {"expiry_interval", sc(integer(), #{desc => "Expiry Interval"})}
    , {"connected_at", sc(integer(), #{
        desc => "The Time that this Client is Connected"})}
    ];
fields("ctx_check_authz_complete") ->
    [ {"event_type", sc(client_check_authz_complete, #{desc => "Event Type", required => true})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"action", sc(binary(), #{desc => "Publish or Subscribe"})}
    , {"authz_source", sc(binary(), #{desc => "Cache, Plugs or Default"})}
    , {"result", sc(binary(), #{desc => "Allow or Deny"})}
    ];
fields("ctx_bridge_mqtt") ->
    [ {"event_type", sc('$bridges/mqtt:*', #{desc => "Event Type", required => true})}
    , {"id", sc(binary(), #{desc => "Message ID"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"server", sc(binary(), #{desc => "The IP address (or hostname) and port of the MQTT broker,"
        " in IP:Port format"})}
    , {"dup", sc(binary(), #{desc => "The DUP flag of the MQTT message"})}
    , {"retain", sc(binary(), #{desc => "If is a retain message"})}
    , {"message_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()].

qos() ->
    {"qos", sc(emqx_schema:qos(), #{desc => "The Message QoS"})}.

rule_id() ->
    {"id", sc(binary(),
        #{ desc => "The ID of the rule", required => true
         , example => "293fb66f"
         })}.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
