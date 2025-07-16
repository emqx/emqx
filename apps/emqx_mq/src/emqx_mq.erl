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
    on_delivery_completed/2,
    on_message_nack/2,
    on_client_handle_info/3,
    on_channel_unregistered/1
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
    emqx_hooks:add('client.authorize', {?MODULE, on_client_authorize, []}, ?HP_AUTHZ + 1),
    emqx_hooks:add('message.nack', {?MODULE, on_message_nack, []}, ?HP_HIGHEST),
    emqx_hooks:add('client.handle_info', {?MODULE, on_client_handle_info, []}, ?HP_HIGHEST),
    emqx_hooks:add('cm.channel.unregistered', {?MODULE, on_channel_unregistered, []}, ?HP_HIGHEST).

-spec unregister_hooks() -> ok.
unregister_hooks() ->
    emqx_hooks:del('message.publish', {?MODULE, on_message_publish}),
    emqx_hooks:del('delivery.completed', {?MODULE, on_delivery_completed}),
    emqx_hooks:del('session.subscribed', {?MODULE, on_session_subscribed}),
    emqx_hooks:del('session.unsubscribed', {?MODULE, on_session_unsubscribed}),
    emqx_hooks:del('session.resumed', {?MODULE, on_session_resumed}),
    emqx_hooks:del('client.authorize', {?MODULE, on_client_authorize}),
    emqx_hooks:del('message.nack', {?MODULE, on_message_nack}),
    emqx_hooks:del('client.handle_info', {?MODULE, on_client_handle_info}),
    emqx_hooks:del('cm.channel.unregistered', {?MODULE, on_channel_unregistered}).

%%--------------------------------------------------------------------
%% Hooks callbacks
%%--------------------------------------------------------------------

on_message_publish(Msg) ->
    {ok, Msg}.

on_delivery_completed(Msg, Info) ->
    case emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined) of
        undefined ->
            ok;
        SubscriberId ->
            ReasonCode = maps:get(reason_code, Info, ?RC_SUCCESS),
            ok = with_sub(SubscriberId, handle_ack, [Msg, ack_from_rc(ReasonCode)])
    end.

on_session_subscribed(ClientInfo, <<"$q/", Topic/binary>> = FullTopic, _SubOpts) ->
    ?tp(warning, mq_on_session_subscribed, #{full_topic => FullTopic, handle => true}),
    %% TODO
    %% Async connect to the consumer
    case emqx_mq_sub:handle_subscribe(ClientInfo, Topic) of
        {ok, Sub} ->
            ?tp(warning, mq_on_session_subscribed_ok, #{sub => Sub}),
            ok = emqx_mq_sub_registry:register_sub(Sub);
        {error, Reason} ->
            %% TODO
            %% Log error
            ?tp(error, mq_on_session_subscribed_error, #{topic => Topic, reason => Reason}),
            ok
    end;
on_session_subscribed(_ClientInfo, FullTopic, _SubOpts) ->
    ?tp(warning, mq_on_session_subscribed, #{full_topic => FullTopic, handle => false}),
    ok.

on_session_unsubscribed(_ClientInfo, <<"$q/", Topic/binary>>) ->
    case emqx_mq_sub_registry:delete_sub(Topic) of
        undefined ->
            ok;
        Sub ->
            ok = emqx_mq_sub:handle_unsubscribe(Sub)
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
    #{clientid := _ClientId}, #{action_type := _ActionType, qos := _QoS}, _Topic, _DefaultResult
) ->
    %% TODO
    %% Forbid direct publish to $q/
    ignore.

on_message_nack(_ClientInfo, Delivers) ->
    {_Nacked, NotNacked} = lists:partition(
        fun({deliver, _, Msg}) ->
            SubscriberId = emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg),
            case with_sub(SubscriberId, handle_ack, [Msg, ?MQ_NACK]) of
                not_found ->
                    false;
                ok ->
                    true;
                {error, _Reason} ->
                    false
            end
        end,
        Delivers
    ),
    {ok, NotNacked}.

on_client_handle_info(_ClientInfo, ?MQ_PING_SUBSCRIBER(SubscriberId), Acc) ->
    ok = with_sub(SubscriberId, handle_ping, []),
    {ok, Acc};
on_client_handle_info(_ClientInfo, ?MQ_MESSAGE(Msg), #{deliver := Delivers} = Acc) ->
    SubscriberId = emqx_message:get_header(?MQ_HEADER_SUBSCRIBER_ID, Msg, undefined),
    case with_sub(SubscriberId, handle_message, [Msg]) of
        {ok, NewDelivers} ->
            {ok, Acc#{deliver => NewDelivers ++ Delivers}};
        not_found ->
            {ok, Acc}
    end;
on_client_handle_info(ClientInfo, ?MQ_TIMEOUT(SubscriberId, TimerMsg), Acc) ->
    case with_sub(SubscriberId, handle_timeout, [TimerMsg]) of
        {error, recreate} ->
            OldSub = #{topic := Topic} = emqx_mq_sub_registry:delete_sub(SubscriberId),
            ok = emqx_mq_sub:handle_unsubscribe(OldSub),
            case emqx_mq_sub:handle_subscribe(ClientInfo, Topic) of
                {ok, NewSub} ->
                    ok = emqx_mq_sub_registry:register_sub(NewSub);
                {error, _Reason} ->
                    %% TODO
                    %% Log error
                    ok
            end;
        not_found ->
            ok;
        ok ->
            ok
    end,
    {ok, Acc};
on_client_handle_info(_ClientInfo, Message, Acc) ->
    ?tp(warning, mq_on_client_handle_info, #{message => Message}),
    {ok, Acc}.

on_channel_unregistered(ChannelPid) ->
    Subs = emqx_mq_sub_registry:cleanup_subs(ChannelPid),
    ok = lists:foreach(
        fun({SubscriberId, ConsumerPid}) ->
            emqx_mq_sub:handle_cleanup(SubscriberId, ConsumerPid)
        end,
        Subs
    ).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

with_sub(undefined, _Handler, _Args) ->
    not_found;
with_sub(SubscriberId, Handler, Args) ->
    case emqx_mq_sub_registry:get_sub(SubscriberId) of
        undefined ->
            not_found;
        Sub ->
            case apply(emqx_mq_sub, Handler, [Sub | Args]) of
                ok ->
                    ok;
                {error, Reason} ->
                    {error, Reason};
                {ok, NewSub} ->
                    ok = emqx_mq_sub_registry:put_sub(SubscriberId, NewSub),
                    ok;
                {ok, NewSub, Result} ->
                    ok = emqx_mq_sub_registry:put_sub(SubscriberId, NewSub),
                    {ok, Result}
            end
    end.

ack_from_rc(?RC_SUCCESS) -> ?MQ_ACK;
ack_from_rc(_) -> ?MQ_NACK.
