%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams).

-moduledoc """
The module is responsible for integrating the Streams application into the EMQX core.

The write part is integrated via registering `message.publish` hook.
The read part is integrated via registering an ExtSub handler.
""".

-include("emqx_streams_internal.hrl").

-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish_stream/1,
    on_session_created/2,
    on_session_resumed/2,
    on_client_authorize/4
]).

-define(CONN_INFO_PD_KEY, emqx_streams_conn_info).

%%

-spec register_hooks() -> ok.
register_hooks() ->
    ok = register_stream_hooks(),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    ok = unregister_stream_hooks(),
    ok.

-spec register_stream_hooks() -> ok.
register_stream_hooks() ->
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish_stream, []}, ?HP_HIGHEST),
    _ = emqx_hooks:add('session.created', {?MODULE, on_session_created, []}, ?HP_LOWEST),
    _ = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    _ = emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    _ = emqx_extsub_handler_registry:register(emqx_streams_extsub_handler, #{
        handle_generic_messages => true,
        multi_topic => true
    }),
    ok.

-spec unregister_stream_hooks() -> ok.
unregister_stream_hooks() ->
    _ = emqx_hooks:del('message.publish', {?MODULE, on_message_publish_stream}),
    _ = emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    _ = emqx_hooks:del('session.created', {?MODULE, on_session_created}),
    _ = emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    _ = emqx_extsub_handler_registry:unregister(emqx_streams_extsub_handler),
    ok.
%%

on_message_publish_stream(#message{topic = Topic} = Message) ->
    ?tp_debug(streams_on_message_publish_stream, #{topic => Topic}),
    Streams = emqx_streams_registry:match(Topic),
    ok = lists:foreach(
        fun(Stream) ->
            {Time, Result} = timer:tc(fun() -> publish_to_stream(Stream, Message) end),
            case Result of
                ok ->
                    emqx_streams_metrics:inc(ds, inserted_messages),
                    ?tp_debug(streams_on_message_publish_stream_ok, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, streams_on_message_publish_stream_error, #{
                        topic_filter => emqx_streams_prop:topic_filter(Stream),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        Streams
    ),
    {ok, Message}.

on_session_created(ClientInfo, _SessionInfo) ->
    Ctx = emqx_hooks:context('session.created'),
    ok = save_support_info(Ctx, ClientInfo).

on_session_resumed(ClientInfo, _SessionInfo) ->
    Ctx = emqx_hooks:context('session.resumed'),
    ok = save_support_info(Ctx, ClientInfo).

on_client_authorize(
    _ClientInfo, #{action_type := subscribe} = _Action, <<"$s/", _/binary>> = _Topic, Result
) ->
    deny_if_streams_not_supported(Result);
on_client_authorize(
    _ClientInfo, #{action_type := subscribe} = _Action, <<"$stream/", _/binary>> = _Topic, Result
) ->
    deny_if_streams_not_supported(Result);
on_client_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish_to_stream(Stream, #message{} = Message) ->
    emqx_streams_message_db:insert(Stream, Message).

save_support_info(#{conn_info_fn := ConnInfoFn} = _Ctx, ClientInfo) ->
    Protocol = maps:get(protocol, ClientInfo, undefined),
    ProtoVer = ConnInfoFn(proto_ver),
    Info =
        case {Protocol, ProtoVer} of
            {mqtt, ?MQTT_PROTO_V5} ->
                ok;
            Unsupported ->
                {error, {streams_not_supported_for_protocol, Unsupported}}
        end,
    _ = erlang:put(?CONN_INFO_PD_KEY, Info),
    ok.

validate_streams_support() ->
    case erlang:get(?CONN_INFO_PD_KEY) of
        undefined ->
            {error, unknown};
        Info ->
            Info
    end.

deny_if_streams_not_supported(Result) ->
    case validate_streams_support() of
        ok ->
            {ok, Result};
        {error, Reason} ->
            ?tp(warning, streams_cannot_subscribe_to_streams, #{reason => Reason}),
            {stop, #{result => deny, from => streams}}
    end.
