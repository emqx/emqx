%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_server).

-moduledoc """
The module is responsible for interacting with the MQ consumer subscribers, i.e.
channels subscribed to a Message Queue.
""".

-include("../emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    new/1,
    handle_messages/2,
    handle_info/2
]).

%%--------------------------------------------------------------------
%% Types
%%--------------------------------------------------------------------

-type subscriber_ref() :: emqx_mq_types:subscriber_ref().
-type message_id() :: emqx_mq_types:message_id().
-type monotonic_timestamp_ms() :: emqx_mq_types:monotonic_timestamp_ms().
-type subscriber_data() :: #{
    timeout_tref := reference(),
    client_id := emqx_types:clientid(),
    %% TODO
    %% add stale message redispatch
    inflight_messages := #{message_id() => monotonic_timestamp_ms()}
}.

-record(st, {
    topic_filter :: binary(),
    subscribers :: #{subscriber_ref() => subscriber_data()},
    messages :: #{message_id() => emqx_types:message()},
    dispatch_queue :: queue:queue(),
    ping_timer_ref :: reference() | undefined
}).

-type t() :: #st{}.

-type event() :: {ds_ack, message_id()}.

-export_type([t/0, event/0]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(ping_subscribers, {}).

-record(subscriber_timeout, {
    subscriber_ref :: subscriber_ref()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec new(binary()) -> t().
new(MQTopicFilter) ->
    #st{
        topic_filter = MQTopicFilter,
        subscribers = #{},
        messages = #{},
        dispatch_queue = queue:new(),
        ping_timer_ref = undefined
    }.

-spec handle_messages(t(), [{message_id(), emqx_types:message()}]) -> t().
handle_messages(State, Messages) ->
    dispatch(enqueue_messages(State, Messages)).

-spec handle_info(t(), term()) -> {ok, [event()], t()}.
handle_info(St, #mq_server_connect{subscriber_ref = SubscriberRef, client_id = ClientId}) ->
    {ok, [], handle_connect(St, SubscriberRef, ClientId)};
handle_info(St, #mq_server_disconnect{subscriber_ref = SubscriberRef}) ->
    {ok, [], handle_disconnect(St, SubscriberRef)};
handle_info(St, #mq_server_ack{subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack}) ->
    handle_ack(St, SubscriberRef, MessageId, Ack);
handle_info(St, #mq_server_ping{subscriber_ref = SubscriberRef}) ->
    {ok, [], handle_ping(St, SubscriberRef)};
handle_info(St, #ping_subscribers{}) ->
    {ok, [], handle_ping_subscribers(St)};
handle_info(St, #subscriber_timeout{subscriber_ref = SubscriberRef}) ->
    {ok, [], handle_subscriber_timeout(SubscriberRef, St)}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

handle_connect(#st{subscribers = Subscribers0} = State, SubscriberRef, ClientId) ->
    ?tp(warning, mq_consumer_handle_connect, #{
        subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case Subscribers0 of
        #{SubscriberRef := SubscriberData0} ->
            SubscriberData1 = refresh_subscriber_timeout(SubscriberRef, SubscriberData0),
            Subscribers1 = Subscribers0#{SubscriberRef => SubscriberData1},
            State#st{subscribers = Subscribers1};
        _ ->
            Subscribers1 = Subscribers0#{
                SubscriberRef => initial_subscriber_data(SubscriberRef, ClientId)
            },
            ok = send_connected_to_subscriber(SubscriberRef),
            dispatch(ensure_ping_timer(State#st{subscribers = Subscribers1}))
    end.

handle_disconnect(#st{subscribers = Subscribers0} = State0, SubscriberRef) ->
    case Subscribers0 of
        #{SubscriberRef := #{inflight_messages := InflightMessages} = SubscriberData} ->
            _ = cancel_subscriber_timeout(SubscriberData),
            Subscribers = maps:remove(SubscriberRef, Subscribers0),
            State1 = State0#st{subscribers = Subscribers},
            State = enqueue_for_dispatch(State1, maps:keys(InflightMessages)),
            dispatch(ensure_ping_timer(State));
        _ ->
            State0
    end.

handle_ack(
    #st{subscribers = Subscribers0, messages = Messages0} = State0,
    SubscriberRef,
    MessageId,
    Ack
) ->
    maybe
        #{SubscriberRef := SubscriberData0} ?= Subscribers0,
        #{inflight_messages := InflightMessages0} ?= SubscriberData0,
        #{MessageId := _} ?= InflightMessages0,
        InflightMessages = maps:remove(MessageId, InflightMessages0),
        SubscriberData1 = SubscriberData0#{inflight_messages => InflightMessages},
        SubscriberData = refresh_subscriber_timeout(SubscriberRef, SubscriberData1),
        Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
        State1 = State0#st{subscribers = Subscribers},
        case Ack of
            ?MQ_ACK ->
                Messages = maps:remove(MessageId, Messages0),
                State = State1#st{messages = Messages},
                {ok, [{ds_ack, MessageId}], State};
            ?MQ_NACK ->
                {ok, [], dispatch(enqueue_for_dispatch(State1, [MessageId]))}
        end
    else
        _ ->
            %% TODO
            %% warn
            State0
    end.

handle_ping(#st{subscribers = Subscribers0} = State, SubscriberRef) ->
    case Subscribers0 of
        #{SubscriberRef := SubscriberData0} ->
            SubscriberData = refresh_subscriber_timeout(SubscriberRef, SubscriberData0),
            Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
            State#st{subscribers = Subscribers};
        _ ->
            State
    end.

handle_ping_subscribers(#st{subscribers = Subscribers} = State) ->
    ?tp(warning, mq_consumer_handle_ping_subscribers, #{subscribers => maps:keys(Subscribers)}),
    ok = maps:foreach(
        fun(SubscriberRef, _SubscriberData) ->
            send_ping_to_subscriber(SubscriberRef)
        end,
        Subscribers
    ),
    ensure_ping_timer(State).

handle_subscriber_timeout(SubscriberRef, State) ->
    handle_disconnect(SubscriberRef, State).

enqueue_messages(State, []) ->
    State;
enqueue_messages(
    #st{messages = Messages0} = State0,
    [{MessageId, MQMessage} | Rest]
) ->
    %% TODO
    %% use real topic and client
    Message = emqx_mq_payload_db:from_mq_message(MQMessage),
    Messages = Messages0#{MessageId => Message},
    State = State0#st{messages = Messages},
    enqueue_for_dispatch(enqueue_messages(State, Rest), [MessageId]).

refresh_subscriber_timeout(SubscriberRef, SubscriberData0) ->
    SubscriberData = cancel_subscriber_timeout(SubscriberData0),
    TimeoutTRef = send_after(
        ?DEFAULT_SUBSCRIBER_TIMEOUT,
        #subscriber_timeout{subscriber_ref = SubscriberRef}
    ),
    SubscriberData#{timeout_tref => TimeoutTRef}.

cancel_subscriber_timeout(#{timeout_tref := TimeoutTRef} = SubscriberData) ->
    emqx_utils:cancel_timer(TimeoutTRef),
    SubscriberData#{timeout_tref => undefined}.

initial_subscriber_data(SubscriberRef, ClientId) ->
    SubscriberData = #{
        timeout_tref => undefined,
        client_id => ClientId,
        inflight_messages => #{}
    },
    refresh_subscriber_timeout(SubscriberRef, SubscriberData).

enqueue_for_dispatch(#st{dispatch_queue = DispatchQueue0} = State, MessageIds) ->
    DispatchQueue1 = lists:foldl(
        fun(MessageId, Acc) ->
            queue:in(MessageId, Acc)
        end,
        DispatchQueue0,
        MessageIds
    ),
    State#st{dispatch_queue = DispatchQueue1}.

dispatch(#st{subscribers = Subscribers} = State) when map_size(Subscribers) =:= 0 ->
    State;
dispatch(#st{dispatch_queue = DispatchQueue} = State0) ->
    case queue:out(DispatchQueue) of
        {{value, MessageId}, DispatchQueue1} ->
            State1 = State0#st{dispatch_queue = DispatchQueue1},
            State = dispatch_message(MessageId, State1),
            dispatch(State);
        {empty, _DispatchQueue1} ->
            State0
    end.

dispatch_message(MessageId, #st{messages = Messages, subscribers = Subscribers} = State) ->
    Message = maps:get(MessageId, Messages),
    SubscriberRef = pick_subscriber(Message, Subscribers),
    dispatch_to_subscriber(MessageId, Message, SubscriberRef, State).

pick_subscriber(_Message, Subscribers) ->
    %% TODO
    %% implement strategies
    N = map_size(Subscribers),
    RandomIndex = rand:uniform(N),
    lists:nth(RandomIndex, maps:keys(Subscribers)).

dispatch_to_subscriber(
    MessageId, Message, SubscriberRef, #st{subscribers = Subscribers0} = State0
) ->
    #{inflight_messages := InflightMessages0} =
        SubscriberData0 = maps:get(SubscriberRef, Subscribers0),
    InflightMessages = InflightMessages0#{MessageId => now_ms_monotonic()},
    SubscriberData = SubscriberData0#{inflight_messages => InflightMessages},
    Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
    State = State0#st{subscribers = Subscribers},
    ok = send_message_to_subscriber(MessageId, Message, SubscriberRef),
    State.

ensure_ping_timer(#st{ping_timer_ref = PingTimerRef, subscribers = Subscribers} = State) when
    map_size(Subscribers) =:= 0
->
    _ = emqx_utils:cancel_timer(PingTimerRef),
    State#st{ping_timer_ref = undefined};
ensure_ping_timer(#st{ping_timer_ref = _PingTimerRef} = State) ->
    TRref = send_after(?DEFAULT_PING_INTERVAL, #ping_subscribers{}),
    State#st{ping_timer_ref = TRref}.

send_after(Timeout, Message) ->
    erlang:send_after(Timeout, self_consumer_ref(), #info_to_mq_server{message = Message}).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

%% TODO
%% use proto

send_info_to_subscriber(SubscriberRef, InfoMsg) ->
    _ = erlang:send(SubscriberRef, #info_to_mq_sub{
        subscriber_ref = SubscriberRef, message = InfoMsg
    }),
    ok.

send_ping_to_subscriber(SubscriberRef) ->
    send_info_to_subscriber(SubscriberRef, #mq_sub_ping{}).

send_message_to_subscriber(MessageId, Message0, SubscriberRef) ->
    Message1 = emqx_message:set_headers(#{?MQ_HEADER_MESSAGE_ID => MessageId}, Message0),
    send_info_to_subscriber(SubscriberRef, #mq_sub_message{message = Message1}).

send_connected_to_subscriber(SubscriberRef) ->
    send_info_to_subscriber(SubscriberRef, #mq_sub_connected{consumer_ref = self_consumer_ref()}).

self_consumer_ref() ->
    self().
