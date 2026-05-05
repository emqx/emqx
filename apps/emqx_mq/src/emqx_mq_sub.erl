%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_sub).

-moduledoc """
The module represents a subscription to a Message Queue consumer.
It manages the consumer connection lifecycle.

It has states:
* `#finding_mq{}` - no MQ found, we are waiting for it to be created.
* `#connecting{}` - MQ found, the subscription is trying to connect to the consumer.
* `#connected{}` - the subscription is established and the consumer is connected.

It uses timers:
* In `#finding_mq{}` state:
  * `find_mq_retry` - the timeout for the next retry to find the MQ (if it is not present).
* In `#connecting{}` state:
  * `connect_timeout` - the timeout for the consumer to connect to the MQ.
* In `#connected{}` state:
  * `consumer_timeout` - the timeout for the consumer to report about itself
    (either by ping, message, or connection confirmation).
  * `ping` - the timeout for the ping message to be sent to the consumer.

Message buffering and inflight tracking are handled by emqx_extsub.
""".

-include("emqx_mq_internal.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    name_topic/1,
    subscriber_ref/1,
    inspect/1
]).

-export([
    handle_connect/3,
    handle_info/2,
    handle_disconnect/1,
    ack_consumer/3
]).

-export([
    connected/2,
    ping/1,
    messages/3
]).

-export([
    connected_v1/2,
    ping_v1/1,
    messages_v1/3
]).

-record(finding_mq, {
    find_mq_retry_tref :: reference() | undefined
}).
-record(connecting, {
    mq :: emqx_mq_types:mq(),
    consumer_ref :: emqx_mq_types:consumer_ref(),
    connect_timeout_tref :: reference() | undefined
}).
-record(connected, {
    mq :: emqx_mq_types:mq(),
    consumer_ref :: emqx_mq_types:consumer_ref(),
    ping_tref :: reference() | undefined,
    consumer_timeout_tref :: reference() | undefined
}).

-type status() :: #finding_mq{} | #connecting{} | #connected{}.

-type t() :: #{
    status := status(),
    clientid := emqx_types:clientid(),
    topic_filter := emqx_mq_types:mq_topic(),
    name := emqx_mq_types:mq_name(),
    subscriber_ref := emqx_mq_types:subscriber_ref()
}.

-export_type([t/0]).

%%--------------------------------------------------------------------
%% Constants
%%--------------------------------------------------------------------

-define(CONNECT_RETRY_INTERVAL_UNEXPECTED_ERROR, 1000).
%% Quick retry if conflict occurred when trying to connect to the consumer and spawning a new one.
-define(CONNECT_RETRY_INTERVAL_ALREADY_REGISTERED, 100).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(find_mq_retry, {}).
-record(ping_consumer, {}).
-record(consumer_connect_timeout, {}).
-record(consumer_timeout, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec name_topic(t()) -> {emqx_mq_types:mq_name(), emqx_mq_types:mq_topic() | undefined}.
name_topic(#{name := Name, topic_filter := TopicFilter}) ->
    {Name, TopicFilter}.

-spec subscriber_ref(t()) -> emqx_mq_types:subscriber_ref().
subscriber_ref(#{subscriber_ref := SubscriberRef}) ->
    SubscriberRef.

-spec inspect(t()) -> map().
inspect(#{status := Status} = Sub) ->
    Info = maps:with([clientid, topic_filter, subscriber_ref, name], Sub),
    Info#{status => status_inspect(Status)}.

-spec handle_connect(
    emqx_types:clientinfo(), emqx_mq_types:mq_name(), emqx_mq_types:mq_topic() | undefined
) ->
    t().
handle_connect(#{clientid := ClientId}, Name, MQTopic) ->
    SubscriberRef = alias(),
    Sub = #{
        clientid => ClientId,
        topic_filter => MQTopic,
        name => Name,
        subscriber_ref => SubscriberRef
    },
    case emqx_mq_registry:find(Name) of
        {ok, #{topic_filter := FoundMQTopic} = MQ} when
            FoundMQTopic =:= MQTopic orelse MQTopic =:= undefined
        ->
            case emqx_mq_consumer:connect(MQ, SubscriberRef, ClientId) of
                {error, Reason} ->
                    %% MQ found but something went wrong with the consumer.
                    %% Retry to find the queue later.
                    RetryInterval = connect_retry_interval(Reason),
                    case Reason of
                        R when R =:= already_registered orelse R =:= no_mq ->
                            ?tp_debug(mq_sub_handle_connect_error, #{
                                reason => Reason,
                                mq => MQTopic,
                                subscriber_ref => SubscriberRef,
                                clientid => ClientId,
                                retry_interval => RetryInterval
                            });
                        _ ->
                            ?tp(error, mq_sub_handle_connect_error, #{
                                reason => Reason,
                                mq => MQTopic,
                                subscriber_ref => SubscriberRef,
                                clientid => ClientId,
                                retry_interval => RetryInterval
                            })
                    end,
                    Status = #finding_mq{
                        find_mq_retry_tref = send_after(
                            Sub, RetryInterval, #find_mq_retry{}
                        )
                    },
                    Sub#{status => Status};
                {ok, ConsumerRef} ->
                    %% MQ and its consumer found, let's connect and wait for the consumer to be ready.
                    Status = #connecting{
                        mq = MQ,
                        consumer_ref = ConsumerRef,
                        connect_timeout_tref = send_after(
                            Sub, consumer_timeout(MQ), #consumer_connect_timeout{}
                        )
                    },
                    Sub#{status => Status}
            end;
        _ ->
            %% No queue found, or currently the queue is associated with a different topic filter.
            %% NOTE
            %% We may register the subscription finders somewhere
            %% and react on queue creation immediately.
            Status = #finding_mq{
                find_mq_retry_tref = send_after(
                    Sub, find_mq_retry_interval(), #find_mq_retry{}
                )
            },
            Sub#{status => Status}
    end.

-spec handle_info(t(), term()) ->
    {ok, t()} | {ok, t(), [emqx_types:message()]} | {error, recreate}.
%%
%% Messages from the consumer
%%
handle_info(Sub, #mq_sub_ping{}) ->
    ?tp_debug(mq_sub_ping, #{sub => inspect(Sub)}),
    {ok, reset_consumer_timeout_timer(Sub)};
handle_info(#{status := #connected{}} = Sub, #mq_sub_messages{messages = Msgs}) ->
    {ok, Sub, Msgs};
handle_info(
    #{status := #connecting{}} = Sub0,
    #mq_sub_messages{messages = Msgs, consumer_ref = ConsumerRef}
) ->
    Sub = handle_connected(Sub0, ConsumerRef),
    {ok, Sub, Msgs};
handle_info(#{status := #connecting{}} = Sub, #mq_sub_connected{consumer_ref = ConsumerRef}) ->
    {ok, handle_connected(Sub, ConsumerRef)};
%%
%% Self-initiated messages
%%
handle_info(#{status := #finding_mq{}, topic_filter := _TopicFilter} = _Sub, #find_mq_retry{}) ->
    ?tp_debug(mq_sub_find_mq_retry, #{mq_topic_filter => _TopicFilter, sub => inspect(_Sub)}),
    {error, recreate};
handle_info(
    #{status := #connecting{}, topic_filter := TopicFilter} = Sub, #consumer_connect_timeout{}
) ->
    ?tp(error, mq_sub_consumer_connect_timeout, #{
        mq_topic_filter => TopicFilter, sub => inspect(Sub)
    }),
    {error, recreate};
handle_info(
    #{status := #connected{}, topic_filter := TopicFilter, name := Name} = Sub, #consumer_timeout{}
) ->
    ?tp(error, mq_sub_consumer_timeout, #{
        mq_topic_filter => TopicFilter, mq_name => Name, sub => inspect(Sub)
    }),
    {error, recreate};
handle_info(
    #{status := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub,
    #ping_consumer{}
) ->
    ?tp_debug(mq_sub_handle_info, #{sub => inspect(Sub), info_msg => ping}),
    ok = emqx_mq_consumer:ping(ConsumerRef, SubscriberRef),
    {ok, reset_ping_timer(Sub)};
handle_info(Sub, _InfoMsg) ->
    ?tp_debug(mq_sub_handle_info, #{sub => inspect(Sub), info_msg => _InfoMsg}),
    {ok, Sub}.

-spec handle_disconnect(t()) -> ok.
handle_disconnect(
    #{status := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub
) ->
    ok = emqx_mq_consumer:disconnect(ConsumerRef, SubscriberRef),
    destroy(Sub);
handle_disconnect(
    #{status := #connecting{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef} = Sub
) ->
    ok = emqx_mq_consumer:disconnect(ConsumerRef, SubscriberRef),
    destroy(Sub);
handle_disconnect(Sub) ->
    destroy(Sub).

-spec ack_consumer(t(), emqx_mq_types:message_id(), emqx_mq_types:ack()) -> ok.
ack_consumer(
    #{status := #connected{consumer_ref = ConsumerRef}, subscriber_ref := SubscriberRef},
    MessageId,
    MqAck
) ->
    emqx_mq_consumer:ack(ConsumerRef, SubscriberRef, MessageId, MqAck);
ack_consumer(_Sub, _MessageId, _MqAck) ->
    ok.

%%--------------------------------------------------------------------
%% RPC
%%--------------------------------------------------------------------

-spec connected(emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref()) -> ok.
connected(SubscriberRef, ConsumerRef) when node(SubscriberRef) =:= node() ->
    connected_v1(SubscriberRef, ConsumerRef);
connected(SubscriberRef, ConsumerRef) ->
    emqx_mq_sub_proto_v1:mq_sub_connected(node(SubscriberRef), SubscriberRef, ConsumerRef).

-spec ping(emqx_mq_types:subscriber_ref()) -> ok.
ping(SubscriberRef) when node(SubscriberRef) =:= node() ->
    ping_v1(SubscriberRef);
ping(SubscriberRef) ->
    emqx_mq_sub_proto_v1:mq_sub_ping(node(SubscriberRef), SubscriberRef).

-spec messages(emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref(), [emqx_types:message()]) ->
    ok.
messages(SubscriberRef, ConsumerRef, Messages) when node(SubscriberRef) =:= node() ->
    messages_v1(SubscriberRef, ConsumerRef, Messages);
messages(SubscriberRef, ConsumerRef, Messages) ->
    true = emqx_mq_sub_proto_v1:mq_sub_messages(
        node(SubscriberRef), SubscriberRef, ConsumerRef, Messages
    ),
    ok.

%%--------------------------------------------------------------------
%% RPC targets
%%--------------------------------------------------------------------

-spec connected_v1(emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref()) -> ok.
connected_v1(SubscriberRef, ConsumerRef) ->
    send_info_to_subscriber(SubscriberRef, #mq_sub_connected{consumer_ref = ConsumerRef}).

-spec ping_v1(emqx_mq_types:subscriber_ref()) -> ok.
ping_v1(SubscriberRef) ->
    send_info_to_subscriber(SubscriberRef, #mq_sub_ping{}).

-spec messages_v1(
    emqx_mq_types:subscriber_ref(), emqx_mq_types:consumer_ref(), list(emqx_types:message())
) -> ok.
messages_v1(SubscriberRef, ConsumerRef, Messages) ->
    send_info_to_subscriber(SubscriberRef, #mq_sub_messages{
        consumer_ref = ConsumerRef, messages = Messages
    }).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_connected(#{status := #connecting{mq = MQ}} = Sub0, ConsumerRef) ->
    ?tp_debug(handle_connected, #{sub => inspect(Sub0), consumer_ref => ConsumerRef}),
    Sub1 = cancel_consumer_connect_timeout_timer(Sub0),
    Sub = Sub1#{
        status => #connected{
            mq = MQ,
            consumer_ref = ConsumerRef,
            ping_tref = undefined,
            consumer_timeout_tref = undefined
        }
    },
    reset_consumer_timeout_timer(reset_ping_timer(Sub)).

destroy(#{subscriber_ref := SubscriberRef} = Sub) ->
    _ = unalias(SubscriberRef),
    _Sub = cancel_timers(Sub),
    ok.

send_info_to_subscriber(SubscriberRef, InfoMsg) ->
    _ = erlang:send(SubscriberRef, #info_to_mq_sub{
        subscriber_ref = SubscriberRef, info = InfoMsg
    }),
    ok.

%%--------------------------------------------------------------------
%% Timers
%%--------------------------------------------------------------------

cancel_consumer_connect_timeout_timer(
    #{status := #connecting{connect_timeout_tref = TRef} = Status} = Sub
) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        status => Status#connecting{
            connect_timeout_tref = undefined
        }
    }.

reset_consumer_timeout_timer(
    #{status := #connected{consumer_timeout_tref = TRef, mq = MQ} = Status} = Sub
) ->
    ?tp_debug(mq_sub_reset_consumer_timeout_timer, #{sub => inspect(Sub)}),
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        status => Status#connected{
            consumer_timeout_tref = send_after(
                Sub, consumer_timeout(MQ), #consumer_timeout{}
            )
        }
    }.

reset_ping_timer(#{status := #connected{ping_tref = TRef, mq = MQ} = Status} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        status => Status#connected{
            ping_tref = send_after(Sub, ping_interval(MQ), #ping_consumer{})
        }
    }.

cancel_timers(#{status := #finding_mq{find_mq_retry_tref = TRef} = Status} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        status => Status#finding_mq{
            find_mq_retry_tref = undefined
        }
    };
cancel_timers(#{status := #connecting{connect_timeout_tref = TRef} = Status} = Sub) ->
    _ = emqx_utils:cancel_timer(TRef),
    Sub#{
        status => Status#connecting{
            connect_timeout_tref = undefined
        }
    };
cancel_timers(
    #{
        status := #connected{
            consumer_timeout_tref = TRef,
            ping_tref = PingTRef
        } = Status
    } = Sub
) ->
    _ = emqx_utils:cancel_timer(TRef),
    _ = emqx_utils:cancel_timer(PingTRef),
    Sub#{
        status => Status#connected{
            consumer_timeout_tref = undefined,
            ping_tref = undefined
        }
    }.

send_after(#{subscriber_ref := SubscriberRef}, Interval, InfoMessage) ->
    erlang:send_after(
        Interval, self(), #info_to_mq_sub{
            subscriber_ref = SubscriberRef, info = InfoMessage
        }
    ).

%%--------------------------------------------------------------------
%% MQ settings
%%--------------------------------------------------------------------

%% NOTE
%% Cannot be configured individually for each MQ
%% because MQ is still not known when we try to find it.
find_mq_retry_interval() ->
    emqx_config:get([mq, find_queue_retry_interval]).

ping_interval(#{ping_interval := ConsumerPingIntervalMs} = _MQ) ->
    ConsumerPingIntervalMs.

consumer_timeout(MQ) ->
    ping_interval(MQ) * 2.

connect_retry_interval(no_mq) ->
    find_mq_retry_interval();
connect_retry_interval(already_registered) ->
    ?CONNECT_RETRY_INTERVAL_ALREADY_REGISTERED;
connect_retry_interval(_Reason) ->
    ?CONNECT_RETRY_INTERVAL_UNEXPECTED_ERROR.

%%--------------------------------------------------------------------
%% Introspection helpers
%%--------------------------------------------------------------------

status_inspect(
    #connected{
        consumer_ref = ConsumerRef,
        consumer_timeout_tref = ConsumerTimeoutTRef,
        ping_tref = PingTRef
    }
) ->
    #{
        name => connected,
        consumer_ref => ConsumerRef,
        consumer_timeout_tref => ConsumerTimeoutTRef,
        ping_tref => PingTRef
    };
status_inspect(#connecting{}) ->
    #{name => connecting};
status_inspect(#finding_mq{}) ->
    #{name => finding_mq}.
