%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq).

-include_lib("emqx/include/emqx_hooks.hrl").
-include("emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([register_hooks/0, unregister_hooks/0]).

-export([
    on_message_publish/1,
    on_client_authorize/4,
    on_session_subscribed/3,
    on_session_unsubscribed/2,
    on_session_resumed/2,
    on_session_terminated/3,
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3
]).

-spec register_hooks() -> ok.
register_hooks() ->
    %% TODO
    %% Select better priorities for the hooks
    emqx_hooks:add('message.publish', {?MODULE, on_message_publish, []}, ?HP_RETAINER + 1),
    emqx_hooks:add('delivery.completed', {?MODULE, on_delivery_completed, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.subscribed', {?MODULE, on_session_subscribed, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.unsubscribed', {?MODULE, on_session_unsubscribed, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.resumed', {?MODULE, on_session_resumed, []}, ?HP_HIGHEST),
    emqx_hooks:add('session.terminated', {?MODULE, on_session_terminated, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_HIGHEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('session.terminated', {?MODULE, on_session_terminated}),
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(Msg) ->
    {ok, Msg}.

on_delivery_completed(Msg, Info) ->
    case emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined) of
        undefined ->
            ok;
        SubscriberRef ->
            ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
            ok = with_sub(SubscriberRef, handle_ack, [Msg, ack_from_rc(ReasonCode)])
    end.

on_session_subscribed(ClientInfo, <<"$q/", Topic/binary>> = FullTopic, _SubOpts) ->
    ?tp(warning, mq_on_session_subscribed, #{full_topic => FullTopic, handle => true}),
    Sub = emqx_mq_sub:handle_connect(ClientInfo, Topic),
    ok = emqx_mq_sub_registry:register(Sub);
on_session_subscribed(_ClientInfo, FullTopic, _SubOpts) ->
    ?tp(warning, mq_on_session_subscribed, #{full_topic => FullTopic, handle => false}),
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

on_client_authorize(
    #{clientid := ClientId},
    #{action_type := publish, qos := _QoS},
    <<"$q/", _/binary>> = Topic,
    _DefaultResult
) ->
    ?tp(info, mq_forbid_direct_publish_to_queue, #{topic => Topic, client_id => ClientId}),
    {stop, #{result => deny, from => emqx_mq}};
on_client_authorize(
    #{clientid := ClientId},
    #{action_type := subscribe, qos := QoS},
    <<"$q/", _/binary>> = Topic,
    _DefaultResult
) when QoS =/= 1 ->
    ?tp(info, mq_forbid_subscribe_to_queue_with_wrong_qos, #{
        topic => Topic, qos => QoS, client_id => ClientId
    }),
    {stop, #{result => deny, from => emqx_mq}};
on_client_authorize(
    #{clientid := _ClientId}, #{action_type := _Action, qos := _QoS}, _Topic, _DefaultResult
) ->
    ignore.

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

on_client_handle_info(_ClientInfo, ?MQ_PING_SUBSCRIBER(SubscriberRef), Acc) ->
    ok = with_sub(SubscriberRef, handle_ping, []),
    {ok, Acc};
on_client_handle_info(_ClientInfo, ?MQ_MESSAGE(Msg), #{deliver := Delivers} = Acc) ->
    SubscriberRef = emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined),
    case with_sub(SubscriberRef, handle_message, [Msg]) of
        not_found ->
            {ok, Acc};
        {ok, NewDelivers} ->
            {ok, Acc#{deliver => NewDelivers ++ Delivers}};
        {error, recreate} ->
            ok = recreate_sub(SubscriberRef, _ClientInfo),
            {ok, Acc}
    end;
on_client_handle_info(ClientInfo, ?MQ_SUB_INFO(SubscriberRef, InfoMsg), Acc) ->
    case with_sub(SubscriberRef, handle_info, [InfoMsg]) of
        not_found ->
            ok;
        ok ->
            ok;
        {error, recreate} ->
            ok = recreate_sub(SubscriberRef, ClientInfo)
    end,
    {ok, Acc};
on_client_handle_info(_ClientInfo, Message, Acc) ->
    ?tp(warning, mq_on_client_handle_info, #{message => Message}),
    {ok, Acc}.

on_session_terminated(ClientInfo, _Reason, #{subscriptions := Subs} = _SessionInfo) ->
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
                    {ok, Result}
            end
    end.

recreate_sub(SubscriberRef, ClientInfo) ->
    OldSub = #{topic := Topic} = emqx_mq_sub_registry:delete(SubscriberRef),
    ok = emqx_mq_sub:handle_disconnect(OldSub),
    NewSub = emqx_mq_sub:handle_connect(ClientInfo, Topic),
    ok = emqx_mq_sub_registry:register(NewSub).

ack_from_rc(?RC_SUCCESS) -> ?MQ_ACK;
ack_from_rc(_) -> ?MQ_NACK.
