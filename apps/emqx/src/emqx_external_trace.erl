%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_external_trace).

-include("emqx_mqtt.hrl").
-include("emqx_external_trace.hrl").

%% Legacy
-type channel_info() :: #{atom() => _}.
-export_type([channel_info/0]).

%% e2e traces
-type init_attrs() :: attrs().
-type attrs() :: #{atom() => _}.
-type t_fun() :: function().
-type t_args() :: list().
-type t_res() :: any().

-export_type([
    init_attrs/0,
    attrs/0,
    t_fun/0,
    t_args/0,
    t_res/0
]).

%% --------------------------------------------------------------------
%% Trace in Rich mode callbacks

%% Client Connect/Disconnect
-callback client_connect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_disconnect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_subscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_unsubscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authn(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authn_backend(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authz(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_authz_backend(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_disconnect(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_subscribe(init_attrs(), t_fun(), t_args()) -> t_res().

-callback broker_unsubscribe(init_attrs(), t_fun(), t_args()) -> t_res().

%% Message Processing Spans
%% PUBLISH(form Publisher) -> ROUTE -> FORWARD(optional) -> DELIVER(to Subscribers)
-callback client_publish(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_puback(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubrec(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubrel(init_attrs(), t_fun(), t_args()) -> t_res().

-callback client_pubcomp(init_attrs(), t_fun(), t_args()) -> t_res().

-callback msg_route(init_attrs(), t_fun(), t_args()) -> t_res().

%% @doc Trace message forwarding
%% The span `message.forward' always starts in the publisher process and ends in the subscriber process.
%% They are logically two unrelated processes. So the SpanCtx always need to be propagated.
-callback msg_forward(init_attrs(), t_fun(), t_args()) -> t_res().

-callback msg_handle_forward(init_attrs(), t_fun(), t_args()) -> t_res().

%% for broker_publish and outgoing, the process_fun is not needed
%% They Process Ctx in deliver/packet
-callback broker_publish(
    attrs(),
    TraceAction :: ?EXT_TRACE_START,
    list(Deliver)
) -> list(Deliver) when
    Deliver :: emqx_types:deliver().

-callback outgoing(
    attrs(),
    TraceAction :: ?EXT_TRACE_START | ?EXT_TRACE_STOP,
    Packet :: emqx_types:packet()
) -> Res :: t_res().

-callback apply_rule(attrs(), t_fun(), t_args()) -> t_res().

-callback handle_action(attrs(), ?EXT_TRACE_START | ?EXT_TRACE_STOP, map()) -> t_res().

%% --------------------------------------------------------------------
%% Span enrichments APIs

-callback add_span_attrs(Attrs) -> ok when
    Attrs :: attrs().

-callback add_span_attrs(Attrs, Ctx) -> ok when
    Attrs :: attrs(),
    Ctx :: map() | undefined.

-callback set_status_ok() -> ok.

-callback set_status_error() -> ok.

-callback set_status_error(unicode:unicode_binary()) -> ok.

%% --------------------------------------------------------------------
%% Helper APIs

-callback with_action_metadata(map(), map()) -> t_res().

-export([
    provider/0,
    register_provider/1,
    unregister_provider/1
]).

-export([
    connect_attrs/2,
    basic_attrs/1,
    topic_attrs/1,
    authn_attrs/1,
    sub_authz_attrs/1,
    disconnect_attrs/2,
    rule_attrs/1,
    action_attrs/1
]).

%%--------------------------------------------------------------------
%% provider API
%%--------------------------------------------------------------------

-spec register_provider(module()) -> ok | {error, term()}.
register_provider(Module) when is_atom(Module) ->
    case is_valid_provider(Module) of
        true ->
            persistent_term:put(?PROVIDER, Module);
        false ->
            {error, invalid_provider}
    end.

-spec unregister_provider(module()) -> ok | {error, term()}.
unregister_provider(Module) ->
    case persistent_term:get(?PROVIDER, undefined) of
        Module ->
            persistent_term:erase(?PROVIDER),
            ok;
        _ ->
            {error, not_registered}
    end.

-spec provider() -> module() | undefined.
provider() ->
    persistent_term:get(?PROVIDER, undefined).

%%--------------------------------------------------------------------
%% Trace Attrs Helper
%%--------------------------------------------------------------------

%% Client Channel info not be available before `process_connect/2`
%% The initial attrs should be extracted from packet and update them during `process_connect/2`
connect_attrs(
    ?PACKET(?CONNECT, #mqtt_packet_connect{
        proto_name = ProtoName,
        proto_ver = ProtoVer,
        is_bridge = IsBridge,
        clean_start = CleanStart,
        will_flag = WillFlag,
        will_qos = WillQos,
        will_retain = WillRetain,
        keepalive = KeepAlive,
        properties = Properties,
        clientid = ClientId,
        will_props = WillProps,
        will_topic = WillTopic,
        will_payload = _,
        username = Username,
        password = _
    }),
    Channel
) ->
    #{
        'client.clientid' => ClientId,
        'client.username' => Username,
        'client.proto_name' => ProtoName,
        'client.proto_ver' => ProtoVer,
        'client.is_bridge' => IsBridge,
        'client.clean_start' => CleanStart,
        'client.will_flag' => WillFlag,
        'client.will_qos' => WillQos,
        'client.will_retain' => WillRetain,
        'client.keepalive' => KeepAlive,
        'client.conn_props' => json_encode_proplist(Properties),
        'client.will_props' => json_encode(WillProps),
        'client.will_topic' => WillTopic,
        'client.sockname' => emqx_utils:ntoa(emqx_channel:info(sockname, Channel)),
        'client.peername' => emqx_utils:ntoa(emqx_channel:info(peername, Channel))
    }.

basic_attrs(Channel) ->
    #{
        'client.clientid' => emqx_channel:info(clientid, Channel),
        'client.username' => emqx_channel:info(username, Channel)
    }.

topic_attrs(?PACKET(?SUBSCRIBE, PktVar)) ->
    {TFs, SubOpts} = do_topic_filters_attrs(subscribe, emqx_packet:info(topic_filters, PktVar)),
    #{
        'client.subscribe.topics' => json_encode(lists:reverse(TFs)),
        'client.subscribe.sub_opts' => json_encode(lists:reverse(SubOpts))
    };
topic_attrs(?PACKET(?UNSUBSCRIBE, PktVar)) ->
    {TFs, _} = do_topic_filters_attrs(unsubscribe, emqx_packet:info(topic_filters, PktVar)),
    #{'client.unsubscribe.topics' => json_encode(TFs)};
topic_attrs({subscribe, TopicFilters}) ->
    {TFs, SubOpts} = do_topic_filters_attrs(subscribe, TopicFilters),
    #{
        'broker.subscribe.topics' => json_encode(lists:reverse(TFs)),
        'broker.subscribe.sub_opts' => json_encode(lists:reverse(SubOpts))
    };
topic_attrs({unsubscribe, TopicFilters}) ->
    {TFs, _} = do_topic_filters_attrs(unsubscribe, [TF || {TF, _} <- TopicFilters]),
    #{'broker.unsubscribe.topics' => json_encode(TFs)}.

do_topic_filters_attrs(subscribe, TopicFilters) ->
    {_TFs, _SubOpts} = lists:foldl(
        fun({Topic, SubOpts}, {AccTFs, AccSubOpts}) ->
            {[emqx_topic:maybe_format_share(Topic) | AccTFs], [SubOpts | AccSubOpts]}
        end,
        {[], []},
        TopicFilters
    );
do_topic_filters_attrs(unsubscribe, TopicFilters) ->
    TFs = [
        emqx_topic:maybe_format_share(Name)
     || Name <- TopicFilters
    ],
    {TFs, undefined}.

authn_attrs({continue, _Properties, _Channel}) ->
    %% TODO
    #{};
authn_attrs({ok, _Properties, Channel}) ->
    #{
        'client.connect.authn.result' => ok,
        'client.connect.authn.is_superuser' => emqx_channel:info(is_superuser, Channel),
        'client.connect.authn.expire_at' => emqx_channel:info(expire_at, Channel)
    };
authn_attrs({error, _Reason, _Channel}) ->
    #{
        'client.connect.authn.result' => error,
        'client.connect.authn.failure_reason' => emqx_utils:readable_error_msg(_Reason)
    }.

sub_authz_attrs(CheckResult) ->
    {TFs, AuthZRCs} = lists:foldl(
        fun({{TopicFilter, _SubOpts}, RC}, {AccTFs, AccRCs}) ->
            {[emqx_topic:maybe_format_share(TopicFilter) | AccTFs], [RC | AccRCs]}
        end,
        {[], []},
        CheckResult
    ),
    #{
        'authz.subscribe.topics' => json_encode(lists:reverse(TFs)),
        'authz.subscribe.reason_codes' => json_encode(lists:reverse(AuthZRCs))
    }.

-define(ext_trace_disconnect_reason(Reason),
    ?ext_trace_disconnect_reason(Reason, undefined)
).

-define(ext_trace_disconnect_reason(Reason, Description), #{
    'client.proto_name' => emqx_channel:info(proto_name, Channel),
    'client.proto_ver' => emqx_channel:info(proto_ver, Channel),
    'client.is_bridge' => emqx_channel:info(is_bridge, Channel),
    'client.sockname' => emqx_utils:ntoa(emqx_channel:info(sockname, Channel)),
    'client.peername' => emqx_utils:ntoa(emqx_channel:info(peername, Channel)),
    'client.disconnect.reason' => Reason,
    'client.disconnect.reason_desc' => Description
}).

disconnect_attrs(kick, Channel) ->
    ?ext_trace_disconnect_reason(kicked);
disconnect_attrs(discard, Channel) ->
    ?ext_trace_disconnect_reason(discarded);
disconnect_attrs(takeover, Channel) ->
    ?ext_trace_disconnect_reason(takenover);
disconnect_attrs(takeover_kick, Channel) ->
    ?ext_trace_disconnect_reason(takenover_kick);
disconnect_attrs(sock_closed, Channel) ->
    ?ext_trace_disconnect_reason(sock_closed).

-define(MG(K, M), maps:get(K, M, undefined)).

rule_attrs(#{rule := Rule} = RichedRule) ->
    #{
        'rule.id' => ?MG(id, Rule),
        'rule.name' => ?MG(name, Rule),
        'rule.created_at' => format_datetime(?MG(created_at, Rule)),
        'rule.updated_at' => format_datetime(?MG(updated_at, Rule)),
        'rule.description' => ?MG(description, Rule),
        'rule.trigger' => ?MG(trigger, RichedRule),
        'rule.matched' => ?MG(matched, RichedRule),
        'client.clientid' => ?EXT_TRACE__RULE_INTERNAL_CLIENTID
    }.

%% TODO: also rm `action.bridge_type' and `action.bridge_name' after bridge_v1 is deprecated
action_attrs({bridge, BridgeType, BridgeName, ResId}) ->
    #{
        'action.type' => <<"bridge">>,
        'action.bridge_type' => BridgeType,
        'action.bridge_name' => BridgeName,
        'action.resource_id' => ResId,
        'client.clientid' => ?EXT_TRACE__ACTION_INTERNAL_CLIENTID
    };
action_attrs({bridge_v2, BridgeType, BridgeName}) ->
    #{
        'action.type' => <<"bridge_v2">>,
        'action.bridge_type' => BridgeType,
        'action.bridge_name' => BridgeName,
        'client.clientid' => ?EXT_TRACE__ACTION_INTERNAL_CLIENTID
    };
action_attrs(#{mod := Mod, func := Func}) ->
    #{
        'action.type' => <<"function">>,
        'action.function' => printable_function_name(Mod, Func),
        'client.clientid' => ?EXT_TRACE__ACTION_INTERNAL_CLIENTID
    }.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% TODO:
%% 1. Add more checks for the provider module and functions
%% 2. Add more checks for the trace functions
is_valid_provider(Module) ->
    lists:all(
        fun({F, A}) -> erlang:function_exported(Module, F, A) end,
        ?MODULE:behaviour_info(callbacks) -- ?MODULE:behaviour_info(optional_callbacks)
    ).

json_encode(Term) ->
    emqx_utils_json:encode(Term).

%% Properties is a map which may include 'User-Property' of key-value pairs
json_encode_proplist(Properties) ->
    emqx_utils_json:encode_proplist(Properties).

format_datetime(undefined) ->
    undefined;
format_datetime(Timestamp) ->
    format_datetime(Timestamp, millisecond).

format_datetime(Timestamp, Unit) ->
    emqx_utils_calendar:epoch_to_rfc3339(Timestamp, Unit).

printable_function_name(emqx_rule_actions, Func) ->
    Func;
printable_function_name(Mod, Func) ->
    list_to_binary(lists:concat([Mod, ":", Func])).
