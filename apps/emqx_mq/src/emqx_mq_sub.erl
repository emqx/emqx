%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub).

-moduledoc """
The module represents a subscription to a Message Queue consumer.
It handles interactions between a channel and a consumer.
""".

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    handle_subscribe/2,
    handle_ack/3,
    handle_ping/1,
    handle_message/2,
    handle_timeout/2,
    handle_cleanup/2
]).

-type t() :: #{
    subscriber_id := emqx_mq_types:subscriber_id(),
    consumer_pid := emqx_mq_types:consumer_pid(),
    topic := emqx_types:topic(),
    ping_tref := reference() | undefined,
    consumer_timeout_tref := reference() | undefined
}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec handle_subscribe(emqx_types:clientinfo(), emqx_types:topic()) ->
    {ok, t()} | {error, any()}.
handle_subscribe(#{clientid := ClientId}, TopicFilter) ->
    case emqx_mq_consumer:find(TopicFilter) of
        {ok, ConsumerPid} ->
            SubscriberId = alias(),
            Sub0 = #{
                subscriber_id => SubscriberId,
                consumer_pid => ConsumerPid,
                topic => TopicFilter,
                ping_tref => undefined,
                consumer_timeout_tref => undefined
            },
            ok = emqx_mq_consumer:connect(ConsumerPid, SubscriberId, ClientId),
            Sub1 = reset_timers(Sub0),
            {ok, Sub1};
        {error, Reason} ->
            {error, {consumer_not_found, Reason}}
    end.

-spec handle_ack(t(), emqx_types:msg(), emqx_mq_types:ack()) -> ok.
handle_ack(#{consumer_pid := ConsumerPid, subscriber_id := SubscriberId}, Msg, Ack) ->
    case emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg) of
        undefined ->
            ok;
        MessageId ->
            ok = emqx_mq_consumer:ack(ConsumerPid, SubscriberId, MessageId, Ack)
    end.

-spec handle_ping(t()) -> {ok, t()}.
handle_ping(Sub) ->
    ?tp(warning, mq_sub_handle_ping, #{sub => Sub}),
    {ok, reset_consumer_timeout_timer(Sub)}.

-spec handle_message(t(), emqx_types:msg()) -> ok | {ok, t(), emqx_types:deliver()}.
handle_message(Sub0, Msg) ->
    Topic = emqx_message:topic(Msg),
    Sub = reset_consumer_timeout_timer(Sub0),
    {ok, Sub, [{deliver, Topic, Msg}]}.

-spec handle_timeout(t(), term()) -> {ok, t()} | {error, term()}.
handle_timeout(Sub, consumer_timeout) ->
    ?tp(warning, mq_sub_handle_timeout, #{sub => Sub, timer_msg => consumer_timeout}),
    ok = destroy(Sub),
    %% TODO
    %% Log error
    {error, recreate};
handle_timeout(#{consumer_pid := ConsumerPid, subscriber_id := SubscriberId} = Sub, ping) ->
    ?tp(warning, mq_sub_handle_timeout, #{sub => Sub, timer_msg => ping}),
    ok = emqx_mq_consumer:ping(ConsumerPid, SubscriberId),
    {ok, reset_ping_timer(Sub)}.

-spec handle_cleanup(emqx_mq_types:subscriber_id(), emqx_mq_types:consumer_pid()) -> ok.
handle_cleanup(SubscriberId, ConsumerPid) ->
    ?tp(warning, mq_sub_cleanup, #{subscriber_id => SubscriberId, consumer_pid => ConsumerPid}),
    emqx_mq_consumer:disconnect(ConsumerPid, SubscriberId).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

destroy(#{
    consumer_pid := ConsumerPid,
    subscriber_id := SubscriberId,
    consumer_timeout_tref := ConsumerTimeoutTRef,
    ping_tref := PingTRef
}) ->
    _ = emqx_utils:cancel_timer(ConsumerTimeoutTRef),
    _ = emqx_utils:cancel_timer(PingTRef),
    ok = emqx_mq_consumer:disconnect(ConsumerPid, SubscriberId),
    _ = unalias(SubscriberId),
    ok.

reset_consumer_timeout_timer(
    #{consumer_timeout_tref := TRef, subscriber_id := SubscriberId} = Sub
) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        consumer_timeout_tref => erlang:send_after(
            ?DEFAULT_CONSUMER_TIMEOUT, self(), ?MQ_TIMEOUT(SubscriberId, consumer_timeout)
        )
    }.

reset_ping_timer(#{ping_tref := TRef, subscriber_id := SubscriberId} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        ping_tref => erlang:send_after(
            ?DEFAULT_PING_INTERVAL, self(), ?MQ_TIMEOUT(SubscriberId, ping)
        )
    }.

reset_timers(Sub0) ->
    Sub1 = reset_consumer_timeout_timer(Sub0),
    reset_ping_timer(Sub1).
