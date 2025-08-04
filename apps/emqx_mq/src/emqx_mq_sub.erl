%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub).

-moduledoc """
The module represents a subscription to a Message Queue consumer.
It handles interactions between a channel and a consumer.

It has two states:
* `#connecting{}` - the subscription is being established.
* `#connected{consumer_ref = ConsumerRef}` - the subscription is established and the consumer is connected.

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
    handle_info/2,
    handle_disconnect/1
]).

-record(connecting, {}).
-record(connected, {
    consumer_ref :: emqx_mq_types:consumer_ref()
}).

-type state() :: #connecting{} | #connected{}.

-type t() :: #{
    state := state(),
    subscriber_ref := emqx_mq_types:subscriber_ref(),
    topic := emqx_types:topic(),
    ping_tref := reference() | undefined,
    consumer_timeout_tref := reference() | undefined
}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(ping_consumer, {}).
-record(consumer_timeout, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec handle_connect(emqx_types:clientinfo(), emqx_mq_types:mq()) -> t().
handle_connect(#{clientid := ClientId}, #{topic_filter := TopicFilter} = MQ) ->
    SubscriberRef = alias(),
    Sub = #{
        state => #connecting{},
        subscriber_ref => SubscriberRef,
        topic => TopicFilter,
        ping_tref => undefined,
        consumer_timeout_tref => undefined
    },
    %% NOTE
    %% Ignore error.
    %% In case of error, we will reconnect after consumer timeout
    _ = emqx_mq_consumer:connect(MQ, SubscriberRef, ClientId),
    reset_consumer_timeout_timer(Sub).

-spec handle_ack(t(), emqx_types:msg(), emqx_mq_types:ack()) -> ok.
handle_ack(#{state := #connecting{}}, _Msg, _Ack) ->
    %% Should not happen
    ok;
handle_ack(
    #{state := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef}, Msg, Ack
) ->
    case emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg) of
        undefined ->
            ok;
        MessageId ->
            ok = emqx_mq_consumer:ack(ConsumerRef, SubscriberRef, MessageId, Ack)
    end.

-spec handle_info(t(), term()) -> {ok, t()} | {ok, t(), emqx_types:msg()} | {error, term()}.
%%
%% Messages from the consumer
%%
handle_info(Sub, #mq_sub_ping{}) ->
    ?tp(warning, mq_sub_ping, #{sub => Sub}),
    {ok, reset_consumer_timeout_timer(Sub)};
handle_info(#{state := #connecting{}, topic := Topic} = _Sub, #mq_sub_message{message = Msg}) ->
    ?tp(
        warning,
        mq_sub_message_while_connecting,
        #{queue_topic => Topic, topic => emqx_message:topic(Msg)}
    ),
    {error, recreate};
handle_info(Sub0, #mq_sub_message{message = Msg}) ->
    ?tp(warning, mq_sub_message, #{sub => Sub0, message => Msg}),
    Sub = reset_consumer_timeout_timer(Sub0),
    {ok, Sub, [Msg]};
handle_info(#{state := #connecting{}} = Sub0, #mq_sub_connected{consumer_ref = ConsumerRef}) ->
    ?tp(warning, mq_sub_connected, #{sub => Sub0, consumer_ref => ConsumerRef}),
    Sub = Sub0#{state => #connected{consumer_ref = ConsumerRef}},
    {ok, reset_timers(Sub)};
%%
%% Self-initiated messages
%%
handle_info(Sub, #consumer_timeout{}) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => consumer_timeout}),
    %% TODO
    %% Log error
    {error, recreate};
handle_info(
    #{state := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub,
    #ping_consumer{}
) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => ping}),
    ok = emqx_mq_consumer:ping(ConsumerRef, SubscriberRef),
    {ok, reset_ping_timer(Sub)};
handle_info(Sub, InfoMsg) ->
    ?tp(warning, mq_sub_handle_info, #{sub => Sub, info_msg => InfoMsg}),
    {ok, Sub}.

-spec handle_disconnect(t()) -> ok.
handle_disconnect(
    #{state := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub
) ->
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
            ?DEFAULT_CONSUMER_TIMEOUT, self(), #info_to_mq_sub{
                subscriber_ref = SubscriberRef, message = #consumer_timeout{}
            }
        )
    }.

reset_ping_timer(#{ping_tref := TRef, subscriber_ref := SubscriberRef} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        ping_tref => erlang:send_after(
            ?DEFAULT_PING_INTERVAL, self(), #info_to_mq_sub{
                subscriber_ref = SubscriberRef, message = #ping_consumer{}
            }
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
