%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub).

-moduledoc """
The module represents a subscription to a Message Queue consumer.
It handles interactions between a channel and a consumer.

It has two states:
* `connecting` - the subscription is being established.
* `{connected, ConsumerRef}` - the subscription is established and the consumer is connected.

It uses two timers:
* `consumer_timeout` - the timeout for the consumer to report about itself
    (either by ping, message, or connection confirmation).
* `ping` - the timeout for the ping message to be sent to the consumer.
""".

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    handle_connect/2,
    handle_ack/3,
    handle_ping/1,
    handle_message/2,
    handle_info/2,
    handle_disconnect/1
]).

-type state() :: connecting | {connected, emqx_mq_types:consumer_ref()}.

-type t() :: #{
    state := state(),
    subscriber_ref := emqx_mq_types:subscriber_ref(),
    topic := emqx_types:topic(),
    ping_tref := reference() | undefined,
    consumer_timeout_tref := reference() | undefined
}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec handle_connect(emqx_types:clientinfo(), emqx_types:topic()) -> t().
handle_connect(#{clientid := ClientId}, TopicFilter) ->
    SubscriberRef = alias(),
    Sub = #{
        state => connecting,
        subscriber_ref => SubscriberRef,
        topic => TopicFilter,
        ping_tref => undefined,
        consumer_timeout_tref => undefined
    },
    %% NOTE
    %% on error, let's try to reconnect after consumer timeout
    _ = emqx_mq_consumer:connect(TopicFilter, SubscriberRef, ClientId),
    reset_consumer_timeout_timer(Sub).

-spec handle_ack(t(), emqx_types:msg(), emqx_mq_types:ack()) -> ok.
handle_ack(#{state := connecting}, _Msg, _Ack) ->
    %% Should not happen
    ok;
handle_ack(#{state := {connected, ConsumerRef}, subscriber_ref := SubscriberRef}, Msg, Ack) ->
    case emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg) of
        undefined ->
            ok;
        MessageId ->
            ok = emqx_mq_consumer:ack(ConsumerRef, SubscriberRef, MessageId, Ack)
    end.

-spec handle_ping(t()) -> {ok, t()}.
handle_ping(Sub) ->
    ?tp(warning, mq_sub_handle_ping, #{sub => Sub}),
    {ok, reset_consumer_timeout_timer(Sub)}.

-spec handle_message(t(), emqx_types:msg()) -> ok | {ok, t(), emqx_types:deliver()}.
handle_message(#{state := connecting, topic := Topic} = _Sub, Msg) ->
    %% TODO
    ?tp(
        warning,
        mq_sub_handle_message_while_connecting,
        #{queue_topic => Topic, topic => emqx_message:topic(Msg)}
    ),
    {error, recreate};
handle_message(Sub0, Msg) ->
    Topic = emqx_message:topic(Msg),
    Sub = reset_consumer_timeout_timer(Sub0),
    {ok, Sub, [{deliver, Topic, Msg}]}.

-spec handle_info(t(), term()) -> {ok, t()} | {error, term()}.
handle_info(Sub, consumer_timeout) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => consumer_timeout}),
    %% TODO
    %% Log error
    {error, recreate};
handle_info(#{state := {connected, ConsumerRef}, subscriber_ref := SubscriberRef} = Sub, ping) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => ping}),
    ok = emqx_mq_consumer:ping(ConsumerRef, SubscriberRef),
    {ok, reset_ping_timer(Sub)};
handle_info(#{state := connecting} = Sub0, {connected, ConsumerRef}) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub0, info_msg => {connected, ConsumerRef}}),
    Sub = Sub0#{state => {connected, ConsumerRef}},
    {ok, reset_timers(Sub)};
handle_info(Sub, InfoMsg) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => InfoMsg}),
    {ok, Sub}.

-spec handle_disconnect(t()) -> ok.
handle_disconnect(#{state := {connected, ConsumerRef}, subscriber_ref := SubscriberRef} = Sub) ->
    ok = emqx_mq_consumer:disconnect(ConsumerRef, SubscriberRef),
    destroy(Sub);
handle_disconnect(Sub) ->
    destroy(Sub).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

destroy(#{subscriber_ref := SubscriberRef} = Sub) ->
    _ = unalias(SubscriberRef),
    ok = cancel_timers(Sub).

reset_consumer_timeout_timer(
    #{consumer_timeout_tref := TRef, subscriber_ref := SubscriberRef} = Sub
) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        consumer_timeout_tref => erlang:send_after(
            ?DEFAULT_CONSUMER_TIMEOUT, self(), ?MQ_SUB_INFO(SubscriberRef, consumer_timeout)
        )
    }.

reset_ping_timer(#{ping_tref := TRef, subscriber_ref := SubscriberRef} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        ping_tref => erlang:send_after(
            ?DEFAULT_PING_INTERVAL, self(), ?MQ_SUB_INFO(SubscriberRef, ping)
        )
    }.

reset_timers(Sub0) ->
    Sub1 = reset_consumer_timeout_timer(Sub0),
    reset_ping_timer(Sub1).

cancel_timer(Name, Sub) ->
    MaybeTRef = maps:get(Name, Sub),
    _ = emqx_utils:cancel_timer(MaybeTRef),
    ok.

cancel_timers(Sub) ->
    lists:foreach(fun(Name) -> cancel_timer(Name, Sub) end, [consumer_timeout_tref, ping_tref]).
