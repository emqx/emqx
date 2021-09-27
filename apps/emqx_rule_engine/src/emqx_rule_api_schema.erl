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
    , {"rule_test", sc(ref("rule_test"), #{desc => "Schema for testing rules"})}
    ].

fields("rule_creation") ->
    [ {"id", sc(binary(), #{desc => "The Id of the rule", nullable => false})}
    , {"sql", sc(binary(), #{desc => "The SQL of the rule", nullable => false})}
    , {"outputs", sc(hoconsc:array(hoconsc:union(
                                   [ ref("bridge_output")
                                   , ref("builtin_output")
                                   ])),
          #{desc => "The outputs of the rule",
            default => []})}
    , {"enable", sc(boolean(), #{desc => "Enable or disable the rule", default => true})}
    , {"description", sc(binary(), #{desc => "The description of the rule", default => <<>>})}
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

fields("bridge_output") ->
    [ {type, bridge}
    , {target, sc(binary(), #{desc => "The Channel ID of the bridge"})}
    ];

fields("builtin_output") ->
    [ {type, builtin}
    , {target, sc(binary(), #{desc => "The Name of the built-on output"})}
    , {args, sc(map(), #{desc => "The arguments of the built-in output",
        default => #{}})}
    ];

%% TODO: how to use this in "builtin_output".args ?
fields("republish_args") ->
    [ {topic, sc(binary(),
        #{desc => "The target topic of the re-published message."
                  " Template with with variables is allowed.",
          nullable => false})}
    , {qos, sc(binary(),
        #{desc => "The qos of the re-published message."
                  " Template with with variables is allowed. Defaults to ${qos}.",
          default => <<"${qos}">> })}
    , {retain, sc(binary(),
        #{desc => "The retain of the re-published message."
                  " Template with with variables is allowed. Defaults to ${retain}.",
          default => <<"${retain}">> })}
    , {payload, sc(binary(),
        #{desc => "The payload of the re-published message."
                  " Template with with variables is allowed. Defaults to ${payload}.",
          default => <<"${payload}">>})}
    ];

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
