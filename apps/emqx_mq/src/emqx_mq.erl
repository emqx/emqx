%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq).

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish/1,
    on_session_created/2,
    on_session_resumed/2,
    on_client_authorize/4
]).

-export([
    inspect/2
]).

-define(tp_mq_client(KIND, EVENT), ?tp_debug(KIND, EVENT)).

-define(IS_MQ_SUPPORTED_PD_KEY, {?MODULE, is_mq_supported}).

-spec register_hooks() -> ok.
register_hooks() ->
    %% Idempotent hooks registration
    _ = emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    _ = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    _ = emqx_hooks:add('session.created', {?MODULE, on_session_created, []}, ?HP_LOWEST),
    _ = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    _ = emqx_extsub_handler_registry:register(emqx_mq_extsub_handler, #{
        multi_topic => true,
        handle_generic_messages => true
    }),
    ok.

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('session.created', {?MODULE, on_session_created}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    _ = emqx_extsub_handler_registry:unregister(emqx_mq_extsub_handler),
    ok.

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$SYS/", _/binary>>} = Message) ->
    {ok, Message};
on_message_publish(#message{topic = Topic} = Message) ->
    ?tp_mq_client(mq_on_message_publish, #{topic => Topic}),
    MQHandles = emqx_mq_registry:match(Topic),
    ok = lists:foreach(
        fun(MQHandle) ->
            {Time, Result} = timer:tc(fun() -> publish_to_queue(MQHandle, Message) end),
            case Result of
                ok ->
                    emqx_mq_metrics:inc(ds, inserted_messages),
                    ?tp_mq_client(mq_on_message_publish_to_queue, #{
                        topic_filter => emqx_mq_prop:topic_filter(MQHandle),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, mq_on_message_publish_queue_error, #{
                        topic_filter => emqx_mq_prop:topic_filter(MQHandle),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        MQHandles
    ),
    {ok, Message}.

on_session_created(_ClientInfo, SessionInfo) ->
    SessionCreatedCtx = emqx_hooks:context('session.created'),
    ?tp_mq_client(mq_on_session_created, #{client_info => _ClientInfo, session_info => SessionInfo}),
    ok = set_mq_supported(SessionCreatedCtx, SessionInfo).

on_session_resumed(_ClientInfo, SessionInfo) ->
    SessionResumedCtx = emqx_hooks:context('session.resumed'),
    ?tp_mq_client(mq_on_session_resumed, #{client_info => _ClientInfo, session_info => SessionInfo}),
    ok = set_mq_supported(SessionResumedCtx, SessionInfo).

on_client_authorize(
    ClientInfo, #{action_type := subscribe} = _Action, <<"$q/", _/binary>> = Topic, Result
) ->
    deny_if_mq_not_supported(ClientInfo, Topic, Result);
on_client_authorize(
    ClientInfo, #{action_type := subscribe} = _Action, <<"$queue/", _/binary>> = Topic, Result
) ->
    deny_if_mq_not_supported(ClientInfo, Topic, Result);
on_client_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

deny_if_mq_not_supported(_ClientInfo, _Topic, Result) ->
    ?tp_mq_client(mq_on_client_authorize, #{
        client_info => _ClientInfo, topic => _Topic
    }),
    case validate_mq_supported() of
        ok ->
            {ok, Result};
        {error, Reason} ->
            ?tp(warning, mq_cannot_subscribe_to_mq, #{reason => Reason}),
            {stop, #{result => deny, from => mq}}
    end.

%%
%% Introspection
%%

inspect(ChannelPid, NameTopic) ->
    case split_name_topic(NameTopic) of
        {ok, Name, Topic} ->
            Self = alias([reply]),
            erlang:send(ChannelPid, #info_mq_inspect{
                receiver = Self, name = Name, topic_filter = Topic
            }),
            receive
                {Self, Info} ->
                    Info
            end;
        {error, _} = Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

publish_to_queue(MQHandle, #message{} = Message) ->
    emqx_mq_message_db:insert(MQHandle, Message).

set_mq_supported(Ctx, _SessionInfo) ->
    ProtoVer =
        case Ctx of
            #{conn_info_fn := ConnInfoFn} ->
                ConnInfoFn(proto_ver);
            _ ->
                undefined
        end,
    MQSupported =
        case ProtoVer of
            ?MQTT_PROTO_V5 ->
                ok;
            _ ->
                {error, not_mqtt_v5_protocol}
        end,
    _ = erlang:put(?IS_MQ_SUPPORTED_PD_KEY, MQSupported),
    ok.

validate_mq_supported() ->
    case erlang:get(?IS_MQ_SUPPORTED_PD_KEY) of
        undefined ->
            %% Should never happen
            {error, unknown};
        MQSupported ->
            MQSupported
    end.

split_name_topic(NameTopic) ->
    case binary:split(NameTopic, <<"/">>) of
        [Name, Topic] ->
            ok;
        _ ->
            Name = NameTopic,
            Topic = undefined
    end,
    case emqx_mq_schema:validate_name(Name) of
        ok ->
            {ok, Name, Topic};
        {error, _} = Error ->
            Error
    end.
