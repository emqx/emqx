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
    mq_topic_filter/1,
    subscriber_ref/1
]).

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

-type status() :: #connecting{} | #connected{}.

-type t() :: #{
    mq := emqx_mq_types:mq(),
    status := status(),
    subscriber_ref := emqx_mq_types:subscriber_ref(),
    ping_tref := reference() | undefined,
    consumer_timeout_tref := reference() | undefined,
    inflight := #{emqx_mq_types:message_id() => emqx_types:message()},
    buffer := emqx_mq_sub_buffer:t(),
    publish_retry_tref := reference() | undefined
}.

-export_type([t/0]).

-define(BUSY_SESSION_RETRY_INTERVAL, 100).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(ping_consumer, {}).
-record(consumer_timeout, {}).
-record(publish_retry, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec mq_topic_filter(t()) -> emqx_mq_types:mq_topic().
mq_topic_filter(#{mq := #{topic_filter := TopicFilter}}) ->
    TopicFilter.

-spec subscriber_ref(t()) -> emqx_mq_types:subscriber_ref().
subscriber_ref(#{subscriber_ref := SubscriberRef}) ->
    SubscriberRef.

-spec handle_connect(emqx_types:clientinfo(), emqx_mq_types:mq()) -> t().
handle_connect(#{clientid := ClientId}, MQ) ->
    SubscriberRef = alias(),
    Sub = #{
        client_id => ClientId,
        mq => MQ,
        status => #connecting{},
        subscriber_ref => SubscriberRef,
        inflight => #{},
        buffer => emqx_mq_sub_buffer:new(),
        ping_tref => undefined,
        consumer_timeout_tref => undefined,
        publish_retry_tref => undefined
    },
    %% NOTE
    %% Ignore error.
    %% In case of error, we will reconnect after consumer timeout
    _ = emqx_mq_consumer:connect(MQ, SubscriberRef, ClientId),
    reset_consumer_timeout_timer(Sub).

-spec handle_ack(t(), emqx_types:msg(), emqx_mq_types:ack()) -> ok.
handle_ack(#{status := #connecting{}} = _Sub, _Msg, _Ack) ->
    %% Should not happen
    ok;
handle_ack(
    #{status := #connected{}} = Sub, Msg, Ack
) ->
    case emqx_message:get_header(?MQ_HEADER_MESSAGE_ID, Msg) of
        undefined ->
            ok;
        MessageId ->
            {ok, do_handle_ack(Sub, MessageId, Ack)}
    end.

-spec handle_info(t(), term()) -> {ok, t()} | {ok, t(), emqx_types:msg()} | {error, term()}.
%%
%% Messages from the consumer
%%
handle_info(Sub, #mq_sub_ping{}) ->
    ?tp(warning, mq_sub_ping, #{sub => info(Sub)}),
    {ok, reset_consumer_timeout_timer(Sub)};
handle_info(
    #{status := #connecting{}, mq := #{topic_filter := TopicFilter}} = _Sub, #mq_sub_message{
        message = Msg
    }
) ->
    ?tp(
        warning,
        mq_sub_message_while_connecting,
        #{mq_topic_filter => TopicFilter, message_topic => emqx_message:topic(Msg)}
    ),
    {error, recreate};
handle_info(
    #{buffer := Buffer0, inflight := Inflight, publish_retry_tref := PublishRetryTRef} = Sub0,
    #mq_sub_message{message = Msg}
) ->
    ?tp(warning, mq_sub_message, #{sub => info(Sub0), message => Msg}),
    Sub1 = reset_consumer_timeout_timer(Sub0),
    Buffer = emqx_mq_sub_buffer:add(Buffer0, Msg),
    Sub2 =
        case PublishRetryTRef of
            undefined ->
                case map_size(Inflight) < max_inflight(Sub0) of
                    true ->
                        schedule_publish_retry(0, Sub1);
                    false ->
                        Sub1
                end;
            _ ->
                Sub1
        end,
    {ok, Sub2#{buffer => Buffer}};
handle_info(#{status := #connecting{}} = Sub0, #mq_sub_connected{consumer_ref = ConsumerRef}) ->
    ?tp(warning, mq_sub_connected, #{sub => info(Sub0), consumer_ref => ConsumerRef}),
    Sub = Sub0#{status => #connected{consumer_ref = ConsumerRef}},
    {ok, reset_timers(Sub)};
%%
%% Self-initiated messages
%%
handle_info(#{mq := #{topic_filter := TopicFilter}} = _Sub, #consumer_timeout{}) ->
    ?tp(error, mq_sub_consumer_timeout, #{mq_topic_filter => TopicFilter}),
    {error, recreate};
handle_info(
    #{status := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub,
    #ping_consumer{}
) ->
    ?tp(warning, mq_sub_handle_info, #{sub => info(Sub), info_msg => ping}),
    ok = emqx_mq_consumer:ping(ConsumerRef, SubscriberRef),
    {ok, reset_ping_timer(Sub)};
handle_info(
    #{status := #connected{}, inflight := Inflight0, buffer := Buffer0} = Sub0,
    #publish_retry{}
) ->
    ?tp(warning, mq_sub_handle_info, #{sub => info(Sub0), info_msg => publish_retry}),
    NPublish = max_inflight(Sub0) - map_size(Inflight0),
    {MessagesWithIds, Buffer} = emqx_mq_sub_buffer:take(Buffer0, NPublish),
    {Inflight, Messages} = lists:foldl(
        fun({MessageId, Message}, {InflightAcc, Msgs}) ->
            {InflightAcc#{MessageId => Message}, [Message | Msgs]}
        end,
        {Inflight0, []},
        MessagesWithIds
    ),
    Sub1 = cancel_timer(publish_retry_tref, Sub0),
    Sub = Sub1#{inflight => Inflight, buffer => Buffer},
    {ok, Sub, lists:reverse(Messages)};
handle_info(Sub, InfoMsg) ->
    ?tp(warning, mq_sub_handle_info, #{sub => info(Sub), info_msg => InfoMsg}),
    {ok, Sub}.

-spec handle_disconnect(t()) -> ok.
handle_disconnect(
    #{status := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub
) ->
    ok = emqx_mq_consumer:disconnect(ConsumerRef, SubscriberRef),
    destroy(Sub);
handle_disconnect(Sub) ->
    destroy(Sub).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

do_handle_ack(#{inflight := Inflight0, buffer := Buffer0} = Sub0, MessageId, ?MQ_NACK) ->
    ?tp(warning, mq_sub_handle_nack, #{sub => info(Sub0), message_id => MessageId}),
    Message = maps:get(MessageId, Inflight0),
    Buffer = emqx_mq_sub_buffer:add(Buffer0, Message),
    Inflight = maps:remove(MessageId, Inflight0),
    Sub =
        case map_size(Inflight) of
            0 ->
                %% Channel rejected all our messages, probably it's busy.
                %% We will retry to publish the messages later.
                schedule_publish_retry(retry_interval(Sub0), Sub0);
            _ ->
                %% We do not try to refill the inflight buffer on NACK
                %% Threre are some messages in flight, so we
                %% wait for the next ack to refill the buffer.
                Sub0
        end,
    Sub#{
        inflight => Inflight,
        buffer => Buffer
    };
do_handle_ack(
    #{
        status := #connected{consumer_ref = ConsumerRef},
        subscriber_ref := SubscriberRef,
        inflight := Inflight0,
        buffer := Buffer
    } = Sub0,
    MessageId,
    Ack
) ->
    ?tp(warning, mq_sub_handle_ack, #{sub => info(Sub0), message_id => MessageId, ack => Ack}),
    Inflight = maps:remove(MessageId, Inflight0),
    ok = emqx_mq_consumer:ack(ConsumerRef, SubscriberRef, MessageId, Ack),
    Sub1 =
        case emqx_mq_sub_buffer:size(Buffer) of
            0 ->
                Sub0;
            _N ->
                %% We may try to inject messages into channel just from the ack hook,
                %% without an additional message to ourselves.
                %% But we want to compete fairly with the other messages incoming to the channel.
                schedule_publish_retry(0, Sub0)
        end,
    Sub1#{
        inflight => Inflight
    }.

destroy(#{subscriber_ref := SubscriberRef} = Sub) ->
    _ = unalias(SubscriberRef),
    ok = cancel_timers(Sub).

reset_consumer_timeout_timer(#{subscriber_ref := SubscriberRef} = Sub0) ->
    Sub = cancel_timer(consumer_timeout_tref, Sub0),
    Sub#{
        consumer_timeout_tref => erlang:send_after(
            consumer_timeout(Sub), self(), #info_to_mq_sub{
                subscriber_ref = SubscriberRef, message = #consumer_timeout{}
            }
        )
    }.

reset_ping_timer(#{subscriber_ref := SubscriberRef} = Sub0) ->
    Sub = cancel_timer(ping_tref, Sub0),
    Sub#{
        ping_tref => erlang:send_after(
            ping_interval(Sub), self(), #info_to_mq_sub{
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
    Sub#{Name => undefined}.

cancel_timers(Sub) ->
    lists:foldl(fun(Name, SubAcc) -> cancel_timer(Name, SubAcc) end, Sub, [
        consumer_timeout_tref, ping_tref
    ]).

schedule_publish_retry(Interval, #{subscriber_ref := SubscriberRef} = Sub0) ->
    Sub = cancel_timer(publish_retry_tref, Sub0),
    ?tp(warning, mq_sub_schedule_publish_retry, #{sub => info(Sub), interval => Interval}),
    TRef = erlang:send_after(Interval, self(), #info_to_mq_sub{
        subscriber_ref = SubscriberRef, message = #publish_retry{}
    }),
    Sub#{
        publish_retry_tref => TRef
    }.

ping_interval(#{mq := #{ping_interval_ms := ConsumerPingIntervalMs}}) ->
    ConsumerPingIntervalMs.

retry_interval(_Sub) ->
    ?BUSY_SESSION_RETRY_INTERVAL.

consumer_timeout(Sub) ->
    ping_interval(Sub) * 2.

info(#{mq := #{topic_filter := TopicFilter}, buffer := Buffer, inflight := Inflight} = Sub) ->
    maps:merge(maps:without([mq, buffer, inflight], Sub), #{
        mq_topic_filter => TopicFilter,
        buffer_size => emqx_mq_sub_buffer:size(Buffer),
        inflight => sets:to_list(Inflight)
    }).

max_inflight(#{mq := #{local_max_inflight := LocalMaxInflight}}) ->
    LocalMaxInflight.
