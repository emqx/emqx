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
    try hocon_schema:check_plain(?MODULE, #{BTag => Params},
            #{atom_key => true, nullable => true}, [BTag]) of
        #{Tag := Checked} -> {ok, Checked}
    catch
        Error:Reason:ST ->
            ?SLOG(error, #{msg => "check_rule_params_failed",
                           exception => Error,
                           reason => Reason,
                           stacktrace => ST}),
            {error, {Reason, ST}}
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
    [ {"id", sc(binary(),
        #{ desc => "The Id of the rule", nullable => false
         , example => "my_rule_id"
         })}
    ] ++ emqx_rule_engine_schema:fields("rules");

fields("rule_info") ->
    [ {"metrics", sc(ref("metrics"), #{desc => "The metrics of the rule"})}
    , {"node_metrics", sc(ref("node_metrics"), #{desc => "The metrics of the rule"})}
    , {"from", sc(hoconsc:array(binary()),
        #{desc => "The topics of the rule", example => "t/#"})}
    , {"created_at", sc(binary(),
        #{ desc => "The created time of the rule"
         , example => "2021-12-01T15:00:43.153+08:00"
         })}
    ] ++ fields("rule_creation");

%% TODO: we can delete this API if the Dashboard not denpends on it
fields("rule_events") ->
    ETopics = [emqx_rule_events:event_topic(E) || E <- emqx_rule_events:event_names()],
    [ {"event", sc(hoconsc:enum(ETopics), #{desc => "The event topics", nullable => false})}
    , {"title", sc(binary(), #{desc => "The title", example => "some title"})}
    , {"description", sc(binary(), #{desc => "The description", example => "some desc"})}
    , {"columns", sc(map(), #{desc => "The columns"})}
    , {"test_columns", sc(map(), #{desc => "The test columns"})}
    , {"sql_example", sc(binary(), #{desc => "The sql_example"})}
    ];

fields("rule_test") ->
    [ {"context", sc(hoconsc:union([ ref("ctx_pub")
                                   , ref("ctx_sub")
                                   , ref("ctx_delivered")
                                   , ref("ctx_acked")
                                   , ref("ctx_dropped")
                                   , ref("ctx_connected")
                                   , ref("ctx_disconnected")
                                   ]),
        #{desc => "The context of the event for testing",
          default => #{}})}
    , {"sql", sc(binary(), #{desc => "The SQL of the rule for testing", nullable => false})}
    ];

fields("metrics") ->
    [ {"matched", sc(integer(), #{desc => "How much times this rule is matched"})}
    , {"rate", sc(float(), #{desc => "The rate of matched, times/second"})}
    , {"rate_max", sc(float(), #{desc => "The max rate of matched, times/second"})}
    , {"rate_last5m", sc(float(),
        #{desc => "The average rate of matched in last 5 mins, times/second"})}
    ];

fields("node_metrics") ->
    [ {"node", sc(binary(), #{desc => "The node name", example => "emqx@127.0.0.1"})}
    ] ++ fields("metrics");

fields("ctx_pub") ->
    [ {"event_type", sc(message_publish, #{desc => "Event Type", nullable => false})}
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
    [ {"event_type", sc(session_subscribed, #{desc => "Event Type", nullable => false})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"payload", sc(binary(), #{desc => "The Message Payload"})}
    , {"peerhost", sc(binary(), #{desc => "The IP Address of the Peer Client"})}
    , {"topic", sc(binary(), #{desc => "Message Topic"})}
    , {"publish_received_at", sc(integer(), #{
        desc => "The Time that this Message is Received"})}
    ] ++ [qos()];

fields("ctx_unsub") ->
    [{"event_type", sc(session_unsubscribed, #{desc => "Event Type", nullable => false})}] ++
    proplists:delete("event_type", fields("ctx_sub"));

fields("ctx_delivered") ->
    [ {"event_type", sc(message_delivered, #{desc => "Event Type", nullable => false})}
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
    [{"event_type", sc(message_acked, #{desc => "Event Type", nullable => false})}] ++
    proplists:delete("event_type", fields("ctx_delivered"));

fields("ctx_dropped") ->
    [ {"event_type", sc(message_dropped, #{desc => "Event Type", nullable => false})}
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
    [ {"event_type", sc(client_connected, #{desc => "Event Type", nullable => false})}
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
    [ {"event_type", sc(client_disconnected, #{desc => "Event Type", nullable => false})}
    , {"clientid", sc(binary(), #{desc => "The Client ID"})}
    , {"username", sc(binary(), #{desc => "The User Name"})}
    , {"reason", sc(binary(), #{desc => "The Reason for Disconnect"})}
    , {"peername", sc(binary(), #{desc => "The IP Address and Port of the Peer Client"})}
    , {"sockname", sc(binary(), #{desc => "The IP Address and Port of the Local Listener"})}
    , {"disconnected_at", sc(integer(), #{
        desc => "The Time that this Client is Disconnected"})}
    ].

qos() ->
    {"qos", sc(hoconsc:union([typerefl:integer(0), typerefl:integer(1), typerefl:integer(2)]),
        #{desc => "The Message QoS"})}.

sc(Type, Meta) -> hoconsc:mk(Type, Meta).
ref(Field) -> hoconsc:ref(?MODULE, Field).
