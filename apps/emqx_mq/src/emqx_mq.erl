%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    on_session_subscribed/3,
    on_session_unsubscribed/3,
    on_session_resumed/2,
    on_session_disconnected/2,
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3,
    on_client_authorize/4
]).

-export([
    inspect/2
]).

-define(tp_mq_client(KIND, EVENT), ?tp_debug(KIND, EVENT)).

-define(IS_MQ_SUPPORTED_PD_KEY, {?MODULE, is_mq_supported}).

-spec register_hooks() -> ok.
register_hooks() ->
    ok = emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    ok = emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.created', {?MODULE, on_session_created, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.disconnected', {?MODULE, on_session_disconnected, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.created', {?MODULE, on_session_created}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.disconnected', {?MODULE, on_session_disconnected}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$SYS/", _/binary>>} = Message) ->
    {ok, Message};
on_message_publish(#message{topic = Topic} = Message) ->
    ?tp_mq_client(mq_on_message_publish, #{topic => Topic}),
    Queues = emqx_mq_registry:match(Topic),
    ok = lists:foreach(
        fun(Queue) ->
            {Time, Result} = timer:tc(fun() -> publish_to_queue(Queue, Message) end),
            case Result of
                ok ->
                    emqx_mq_metrics:inc(ds, inserted_messages),
                    ?tp_mq_client(mq_on_message_publish_to_queue, #{
                        topic_filter => emqx_mq_prop:topic_filter(Queue),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        result => ok
                    });
                {error, Reason} ->
                    ?tp(error, mq_on_message_publish_queue_error, #{
                        topic_filter => emqx_mq_prop:topic_filter(Queue),
                        message_topic => emqx_message:topic(Message),
                        time_us => Time,
                        reason => Reason
                    })
            end
        end,
        Queues
    ),
    {ok, Message}.

on_delivery_completed(Msg, Info) ->
    _MessageId = emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg, undefined),
    ?tp_mq_client(mq_on_delivery_completed, #{message_id => _MessageId}),
    case emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined) of
        undefined ->
            ok;
        SubscriberRef ->
            ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
            case with_sub(SubscriberRef, handle_ack, [Msg, ack_from_rc(ReasonCode)]) of
                ok ->
                    ok;
                not_found ->
                    ?tp_debug(mq_on_delivery_completed_sub_not_found, #{
                        subscriber_ref => SubscriberRef
                    })
            end
    end.

on_session_subscribed(ClientInfo, <<"$q/", Topic/binary>> = _FullTopic, _SubOpts) ->
    ?tp_mq_client(mq_on_session_subscribed, #{
        full_topic => _FullTopic, handle => true, client_info => ClientInfo
    }),
    case is_mq_supported() of
        true ->
            case emqx_mq_sub_registry:find(Topic) of
                undefined ->
                    ok = maybe_auto_create(Topic),
                    Sub = emqx_mq_sub:handle_connect(ClientInfo, Topic),
                    ok = emqx_mq_sub_registry:register(Sub);
                _Sub ->
                    ok
            end;
        false ->
            ?tp(info, mq_cannot_subscribe_to_mq, #{
                reason => "mq is not supported for this type of session"
            }),
            ok
    end;
on_session_subscribed(_ClientInfo, _FullTopic, _SubOpts) ->
    ?tp_mq_client(mq_on_session_subscribed, #{full_topic => _FullTopic, handle => false}),
    ok.

on_session_unsubscribed(ClientInfo, Topic, _SubOpts) ->
    on_session_unsubscribed(ClientInfo, Topic).

on_session_unsubscribed(_ClientInfo, <<"$q/", Topic/binary>> = _FullTopic) ->
    ?tp_mq_client(mq_on_session_unsubscribed, #{full_topic => _FullTopic}),
    case emqx_mq_sub_registry:delete(Topic) of
        undefined ->
            ok;
        Sub ->
            ?tp_mq_client(mq_on_session_unsubscribed_sub_deleted, #{full_topic => _FullTopic}),
            ok = emqx_mq_sub:handle_disconnect(Sub)
    end;
on_session_unsubscribed(_ClientInfo, _FullTopic) ->
    ?tp_mq_client(mq_on_session_unsubscribed_unknown, #{full_topic => _FullTopic}),
    ok.

on_session_resumed(ClientInfo, #{subscriptions := Subs} = SessionInfo) ->
    ?tp_mq_client(mq_on_session_resumed, #{subscriptions => Subs, session_info => SessionInfo}),
    ok = set_mq_supported(SessionInfo),
    ok = maps:foreach(
        fun
            (<<"$q/", _/binary>> = FullTopic, SubOpts) ->
                on_session_subscribed(ClientInfo, FullTopic, SubOpts);
            (_Topic, _SubOpts) ->
                ok
        end,
        Subs
    ).

on_message_nack(Msg, false) ->
    ?tp_mq_client(mq_on_message_nack, #{msg => Msg}),
    SubscriberRef = emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg),
    case with_sub(SubscriberRef, handle_ack, [Msg, ?MQ_NACK]) of
        not_found ->
            ok;
        ok ->
            {ok, true};
        {error, _Reason} ->
            ok
    end;
%% Already nacked by some other hook
on_message_nack(_Msg, true) ->
    ok.

on_client_handle_info(
    _ClientInfo, #info_mq_inspect{receiver = Receiver, topic_filter = TopicFilter}, Acc
) ->
    ?tp_mq_client(mq_on_client_handle_info_inspect, #{topic_filter => TopicFilter}),
    Info =
        case emqx_mq_sub_registry:find(TopicFilter) of
            undefined ->
                undefined;
            Sub ->
                emqx_mq_sub:inspect(Sub)
        end,
    erlang:send(Receiver, {Receiver, Info}),
    {ok, Acc};
on_client_handle_info(
    ClientInfo,
    #info_to_mq_sub{subscriber_ref = SubscriberRef, info = InfoMsg},
    #{deliver := Delivers} = Acc
) ->
    ?tp_mq_client(mq_on_client_handle_info_to_mq_sub, #{info => InfoMsg}),
    case with_sub(SubscriberRef, handle_info, [InfoMsg]) of
        not_found ->
            ok;
        ok ->
            ok;
        {ok, Messages} ->
            ?tp_mq_client(mq_on_client_handle_info_to_mq_sub_messages, #{messages => Messages}),
            {ok, Acc#{deliver => delivers(SubscriberRef, Messages) ++ Delivers}};
        {error, recreate} ->
            ok = recreate_sub(SubscriberRef, ClientInfo)
    end;
on_client_handle_info(_ClientInfo, _Message, Acc) ->
    ?tp_mq_client(mq_on_client_handle_info_unknown, #{message => _Message}),
    {ok, Acc}.

on_session_disconnected(ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ?tp_mq_client(mq_on_session_disconnected, #{subscriptions => Subs}),
    ok = maps:foreach(
        fun
            (<<"$q/", _/binary>> = FullTopic, _SubOpts) ->
                on_session_unsubscribed(ClientInfo, FullTopic);
            (_Topic, _SubOpts) ->
                ok
        end,
        Subs
    ).

on_session_created(_ClientInfo, SessionInfo) ->
    ?tp_mq_client(mq_on_session_created, #{client_info => _ClientInfo, session_info => SessionInfo}),
    ok = set_mq_supported(SessionInfo).

on_client_authorize(
    _ClientInfo, #{action_type := subscribe} = _Action, <<"$q/", _/binary>> = _Topic, Result
) ->
    ?tp_mq_client(mq_on_client_authorize, #{
        client_info => _ClientInfo, action => _Action, topic => _Topic
    }),
    case is_mq_supported() of
        true ->
            {ok, Result};
        false ->
            {stop, #{result => deny, from => mq}}
    end;
on_client_authorize(_ClientInfo, _Action, _Topic, Result) ->
    {ok, Result}.

%%
%% Introspection
%%

inspect(ChannelPid, TopicFilter) ->
    Self = alias([reply]),
    erlang:send(ChannelPid, #info_mq_inspect{receiver = Self, topic_filter = TopicFilter}),
    receive
        {Self, Info} ->
            Info
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

with_sub(undefined, _Handler, _Args) ->
    not_found;
with_sub(SubscriberRef, Handler, Args) ->
    case emqx_mq_sub_registry:find(SubscriberRef) of
        undefined ->
            not_found;
        Sub ->
            case apply(emqx_mq_sub, Handler, [Sub | Args]) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {ok, NewSub} ->
                    ok = emqx_mq_sub_registry:update(SubscriberRef, NewSub),
                    ok;
                {ok, NewSub, Result} ->
                    ok = emqx_mq_sub_registry:update(SubscriberRef, NewSub),
                    {ok, Result}
            end
    end.

recreate_sub(SubscriberRef, ClientInfo) ->
    OldSub = emqx_mq_sub_registry:delete(SubscriberRef),
    ok = emqx_mq_sub:handle_disconnect(OldSub),
    NewSub = emqx_mq_sub:handle_connect(ClientInfo, emqx_mq_sub:mq_topic_filter(OldSub)),
    emqx_mq_sub_registry:register(NewSub).

ack_from_rc(?RC_SUCCESS) -> ?MQ_ACK;
ack_from_rc(_) -> ?MQ_REJECTED.

publish_to_queue(MQ, #message{} = Message) ->
    emqx_mq_message_db:insert(MQ, Message).

delivers(SubscriberRef, Messages) ->
    lists:map(
        fun(Message0) ->
            Message1 = emqx_message:set_headers(
                #{?MQ_HEADER_SUBSCRIBER_ID => SubscriberRef}, Message0
            ),
            %% Override QoS to 1 to require ack from the client
            Message = Message1#message{qos = ?QOS_1},
            Topic = emqx_message:topic(Message),
            {deliver, Topic, Message}
        end,
        Messages
    ).

set_mq_supported(#{impl := emqx_session_mem} = _SessionInfo) ->
    _ = erlang:put(?IS_MQ_SUPPORTED_PD_KEY, true),
    ok;
set_mq_supported(_SessionInfo) ->
    _ = erlang:put(?IS_MQ_SUPPORTED_PD_KEY, false),
    ok.

is_mq_supported() ->
    case erlang:get(?IS_MQ_SUPPORTED_PD_KEY) of
        undefined ->
            false;
        IsSupported ->
            IsSupported
    end.

maybe_auto_create(Topic) ->
    case emqx_mq_config:auto_create(Topic) of
        {true, MQ} ->
            case emqx_mq_registry:is_present(Topic) of
                true ->
                    ok;
                false ->
                    case emqx_mq_registry:create(MQ) of
                        {ok, _} ->
                            ok;
                        {error, Reason} ->
                            ?tp(error, mq_auto_create_error, #{mq => MQ, reason => Reason}),
                            ok
                    end
            end;
        false ->
            ok
    end.
