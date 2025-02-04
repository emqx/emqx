%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    disconnect_attrs/2
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
        'client.conn_props' => emqx_utils_json:encode(Properties),
        'client.will_props' => emqx_utils_json:encode(WillProps),
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
        'client.subscribe.topics' => emqx_utils_json:encode(lists:reverse(TFs)),
        'client.subscribe.sub_opts' => emqx_utils_json:encode(lists:reverse(SubOpts))
    };
topic_attrs(?PACKET(?UNSUBSCRIBE, PktVar)) ->
    {TFs, _} = do_topic_filters_attrs(unsubscribe, emqx_packet:info(topic_filters, PktVar)),
    #{'client.unsubscribe.topics' => emqx_utils_json:encode(TFs)};
topic_attrs({subscribe, TopicFilters}) ->
    {TFs, SubOpts} = do_topic_filters_attrs(subscribe, TopicFilters),
    #{
        'broker.subscribe.topics' => emqx_utils_json:encode(lists:reverse(TFs)),
        'broker.subscribe.sub_opts' => emqx_utils_json:encode(lists:reverse(SubOpts))
    };
topic_attrs({unsubscribe, TopicFilters}) ->
    {TFs, _} = do_topic_filters_attrs(unsubscribe, [TF || {TF, _} <- TopicFilters]),
    #{'broker.unsubscribe.topics' => emqx_utils_json:encode(TFs)}.

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
authn_attrs({error, _Reason}) ->
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
        'authz.subscribe.topics' => emqx_utils_json:encode(lists:reverse(TFs)),
        'authz.subscribe.reason_codes' => emqx_utils_json:encode(lists:reverse(AuthZRCs))
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
