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
    on_session_subscribed/3,
    on_session_unsubscribed/2,
    on_session_resumed/2,
    on_session_disonnected/2,
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-spec register_hooks() -> ok.
register_hooks() ->
    %% TODO
    %% Select better priorities for the hooks
    ok = emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    ok = emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('session.disonnected', {?MODULE, on_session_disonnected, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_LOWEST),
    ok = emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_LOWEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.disonnected', {?MODULE, on_session_disonnected}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(#message{topic = <<"$SYS/", _/binary>>} = Message) ->
    {ok, Message};
on_message_publish(#message{topic = Topic} = Message) ->
    Queues = emqx_mq_registry:match(Topic),
    ok = lists:foreach(
        fun(Queue) ->
            publish_to_queue(Queue, Message)
        end,
        Queues
    ),
    {ok, Message}.

on_delivery_completed(Msg, Info) ->
    case emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined) of
        undefined ->
            ok;
        SubscriberRef ->
            ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
            ok = with_sub(SubscriberRef, handle_ack, [Msg, ack_from_rc(ReasonCode)])
    end.

on_session_subscribed(ClientInfo, <<"$q/", Topic/binary>> = _FullTopic, _SubOpts) ->
    ?tp_debug(mq_on_session_subscribed, #{full_topic => _FullTopic, handle => true}),
    Sub = emqx_mq_sub:handle_connect(ClientInfo, Topic),
    ok = emqx_mq_sub_registry:register(Sub);
on_session_subscribed(_ClientInfo, _FullTopic, _SubOpts) ->
    ?tp_debug(mq_on_session_subscribed, #{full_topic => _FullTopic, handle => false}),
    ok.

on_session_unsubscribed(_ClientInfo, <<"$q/", Topic/binary>>) ->
    case emqx_mq_sub_registry:delete(Topic) of
        undefined ->
            ok;
        Sub ->
            ok = emqx_mq_sub:handle_disconnect(Sub)
    end;
on_session_unsubscribed(_ClientInfo, _FullTopic) ->
    ok.

on_session_resumed(ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
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
    _ClientInfo, #info_mq_info{receiver = Receiver, topic_filter = TopicFilter}, Acc
) ->
    Info =
        case emqx_mq_sub_registry:find(TopicFilter) of
            undefined ->
                undefined;
            Sub ->
                emqx_mq_sub:info(Sub)
        end,
    erlang:send(Receiver, {Receiver, Info}),
    {ok, Acc};
on_client_handle_info(
    ClientInfo,
    #info_to_mq_sub{subscriber_ref = SubscriberRef, info = InfoMsg},
    #{deliver := Delivers} = Acc
) ->
    case with_sub(SubscriberRef, handle_info, [InfoMsg]) of
        not_found ->
            ok;
        ok ->
            ok;
        {ok, Messages} ->
            {ok, Acc#{deliver => delivers(SubscriberRef, Messages) ++ Delivers}};
        {error, recreate} ->
            ok = recreate_sub(SubscriberRef, ClientInfo)
    end;
on_client_handle_info(_ClientInfo, _Message, Acc) ->
    ?tp_debug(mq_on_client_handle_info, #{message => _Message}),
    {ok, Acc}.

on_session_disonnected(ClientInfo, #{subscriptions := Subs} = _SessionInfo) ->
    ok = maps:foreach(
        fun
            (<<"$q/", _/binary>> = FullTopic, _SubOpts) ->
                on_session_unsubscribed(ClientInfo, FullTopic);
            (_Topic, _SubOpts) ->
                ok
        end,
        Subs
    ).

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
                    {ok, Result};
                _ ->
                    error({invalid_return_value_from_handler, Handler})
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
    emqx_mq_message_db:insert(MQ, [Message]).

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
