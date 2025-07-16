%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer).

-moduledoc """
The module represents a consumer of the Message Queue.

A consumer exclusively serves a single Message Queue, so for a given
Message Queue, there is only one or zero consumers.

Consumer's responsibilities:
* Consume messages from the Message Queue topic streams.
* Dispatch messages to the consumer clients (channels).
* Receive message acknowledgements from the consumer clients.
* Track consumption progerss of the topic streams.
""".

-include("emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
    find/1,
    start_link/1
]).

-export([
    init/1,
    handle_call/3,
    handle_cast/2,
    handle_info/2,
    terminate/2
]).

-export([
    connect/3,
    disconnect/2,
    ping/2,
    ack/4
]).

-type subscriber_id() :: emqx_mq_types:subscriber_id().
-type message_id() :: integer().
-type monotonic_timestamp_ms() :: integer().
-type subscriber_data() :: #{
    timeout_tref := reference(),
    client_id := emqx_types:clientid(),
    %% TODO
    %% add stale message redispatch
    inflight_messages := #{message_id() => monotonic_timestamp_ms()}
}.

-record(state, {
    subscribers :: #{subscriber_id() => subscriber_data()},
    messages :: #{message_id() => emqx_types:message()},
    topic_filter :: binary(),
    dispatch_queue :: queue:queue(),
    ping_timer_ref :: reference() | undefined
}).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(connect, {
    subscriber_id :: subscriber_id(),
    client_id :: emqx_types:client_id()
}).

-record(disconnect, {
    subscriber_id :: subscriber_id()
}).

-record(ack, {
    subscriber_id :: subscriber_id(),
    message_id :: message_id(),
    ack :: emqx_mq_types:ack()
}).

-record(ping, {
    subscriber_id :: subscriber_id()
}).

-record(ping_subscribers, {}).

-record(subscriber_timeout, {
    subscriber_id :: subscriber_id()
}).

%% TODO
%% Remove, used for tests
-record(gen_message, {n :: non_neg_integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec find(MQTopicFilter :: binary()) -> {ok, emqx_mq_types:consumer_ref()} | {error, term()}.
find(MQTopicFilter) ->
    %% TODO
    %% implement cluster-wide discovery
    emqx_mq_sup:start_consumer(MQTopicFilter, [MQTopicFilter]).

connect(Pid, SubscriberId, ClientId) ->
    gen_server:cast(
        Pid,
        #connect{subscriber_id = SubscriberId, client_id = ClientId}
    ).

disconnect(Pid, SubscriberId) ->
    gen_server:cast(
        Pid,
        #disconnect{subscriber_id = SubscriberId}
    ).

ack(Pid, SubscriberId, MessageId, Ack) ->
    gen_server:cast(
        Pid,
        #ack{subscriber_id = SubscriberId, message_id = MessageId, ack = Ack}
    ).

ping(Pid, SubscriberId) ->
    gen_server:cast(
        Pid,
        #ping{subscriber_id = SubscriberId}
    ).

%%--------------------------------------------------------------------
%% Gen Server Callbacks
%%--------------------------------------------------------------------

init([MQTopicFilter]) ->
    erlang:send_after(1000, self(), #gen_message{n = 1}),
    {ok, #state{
        subscribers = #{},
        messages = #{},
        topic_filter = MQTopicFilter,
        dispatch_queue = queue:new(),
        ping_timer_ref = undefined
    }}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_request}, State}.

handle_cast(#connect{subscriber_id = SubscriberId, client_id = ClientId}, State) ->
    {noreply, handle_connect(SubscriberId, ClientId, State)};
handle_cast(#disconnect{subscriber_id = SubscriberId}, State) ->
    {noreply, handle_disconnect(SubscriberId, State)};
handle_cast(#ack{subscriber_id = SubscriberId, message_id = MessageId, ack = Ack}, State) ->
    {noreply, handle_ack(SubscriberId, MessageId, Ack, State)};
handle_cast(#ping{subscriber_id = SubscriberId}, State) ->
    {noreply, handle_ping(SubscriberId, State)};
handle_cast(Request, State) ->
    ?tp(warning, mq_consumer_cast_unknown_request, #{request => Request}),
    {noreply, State}.

handle_info(#ping_subscribers{}, State) ->
    {noreply, handle_ping_subscribers(State)};
handle_info(#subscriber_timeout{subscriber_id = SubscriberId}, State) ->
    {noreply, handle_subscriber_timeout(SubscriberId, State)};
handle_info(#gen_message{n = N}, State) ->
    {noreply, handle_gen_message(N, State)};
handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

handle_connect(SubscriberId, ClientId, #state{subscribers = Subscribers0} = State) ->
    ?tp(warning, mq_consumer_handle_connect, #{subscriber_id => SubscriberId, client_id => ClientId}),
    case Subscribers0 of
        #{SubscriberId := SubscriberData0} ->
            SubscriberData1 = refresh_subscriber_timeout(SubscriberId, SubscriberData0),
            Subscribers1 = Subscribers0#{SubscriberId => SubscriberData1},
            State#state{subscribers = Subscribers1};
        _ ->
            Subscribers1 = Subscribers0#{
                SubscriberId => initial_subscriber_data(SubscriberId, ClientId)
            },
            dispatch(ensure_ping_timer(State#state{subscribers = Subscribers1}))
    end.

handle_disconnect(SubscriberId, #state{subscribers = Subscribers0} = State0) ->
    case Subscribers0 of
        #{SubscriberId := #{inflight_messages := InflightMessages} = SubscriberData} ->
            _ = cancel_subscriber_timeout(SubscriberData),
            Subscribers = maps:remove(SubscriberId, Subscribers0),
            State1 = State0#state{subscribers = Subscribers},
            State = enqueue_for_dispatch(maps:keys(InflightMessages), State1),
            dispatch(ensure_ping_timer(State));
        _ ->
            State0
    end.

handle_ack(
    SubscriberId, MessageId, Ack, #state{subscribers = Subscribers0, messages = Messages0} = State0
) ->
    maybe
        #{SubscriberId := SubscriberData0} ?= Subscribers0,
        #{inflight_messages := InflightMessages0} ?= SubscriberData0,
        #{MessageId := _} ?= InflightMessages0,
        InflightMessages = maps:remove(MessageId, InflightMessages0),
        SubscriberData1 = SubscriberData0#{inflight_messages => InflightMessages},
        SubscriberData = refresh_subscriber_timeout(SubscriberId, SubscriberData1),
        Subscribers = Subscribers0#{SubscriberId => SubscriberData},
        State = State0#state{subscribers = Subscribers},
        case Ack of
            ?MQ_ACK ->
                Messages = maps:remove(MessageId, Messages0),
                State#state{messages = Messages};
            ?MQ_NACK ->
                dispatch(enqueue_for_dispatch([MessageId], State))
        end
    else
        _ ->
            %% TODO
            %% warn
            State0
    end.

handle_ping(SubscriberId, #state{subscribers = Subscribers0} = State) ->
    case Subscribers0 of
        #{SubscriberId := SubscriberData0} ->
            SubscriberData = refresh_subscriber_timeout(SubscriberId, SubscriberData0),
            Subscribers = Subscribers0#{SubscriberId => SubscriberData},
            State#state{subscribers = Subscribers};
        _ ->
            State
    end.

handle_ping_subscribers(#state{subscribers = Subscribers} = State) ->
    ?tp(warning, mq_consumer_handle_ping_subscribers, #{subscribers => maps:keys(Subscribers)}),
    ok = maps:foreach(
        fun(SubscriberId, _SubscriberData) ->
            send_ping_to_subscriber(SubscriberId)
        end,
        Subscribers
    ),
    ensure_ping_timer(State).

handle_subscriber_timeout(SubscriberId, State) ->
    handle_disconnect(SubscriberId, State).

%% TODO
%% Remove, used for tests
handle_gen_message(N, #state{messages = Messages0, topic_filter = TopicFilter} = State) ->
    Topic = TopicFilter,
    Payload = iolist_to_binary(io_lib:format("dummy message ~p", [N])),
    Message = emqx_message:make(<<"dummy client">>, ?QOS_1, Topic, Payload),
    MessageId = N,
    Messages = Messages0#{MessageId => Message},
    _ = erlang:send_after(500, self(), #gen_message{n = N + 1}),
    ?tp(warning, mq_consumer_handle_gen_message, #{message_id => MessageId}),
    dispatch(enqueue_for_dispatch([MessageId], State#state{messages = Messages})).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

refresh_subscriber_timeout(SubscriberId, SubscriberData0) ->
    SubscriberData = cancel_subscriber_timeout(SubscriberData0),
    TimeoutTRef = erlang:send_after(
        ?DEFAULT_SUBSCRIBER_TIMEOUT,
        self(),
        #subscriber_timeout{subscriber_id = SubscriberId}
    ),
    SubscriberData#{timeout_tref => TimeoutTRef}.

cancel_subscriber_timeout(#{timeout_tref := TimeoutTRef} = SubscriberData) ->
    emqx_utils:cancel_timer(TimeoutTRef),
    SubscriberData#{timeout_tref => undefined}.

initial_subscriber_data(SubscriberId, ClientId) ->
    SubscriberData = #{
        timeout_tref => undefined,
        client_id => ClientId,
        inflight_messages => #{}
    },
    refresh_subscriber_timeout(SubscriberId, SubscriberData).

enqueue_for_dispatch(MessageIds, #state{dispatch_queue = DispatchQueue0} = State) ->
    DispatchQueue1 = lists:foldl(
        fun(MessageId, Acc) ->
            queue:in(MessageId, Acc)
        end,
        DispatchQueue0,
        MessageIds
    ),
    State#state{dispatch_queue = DispatchQueue1}.

dispatch(#state{subscribers = Subscribers} = State) when map_size(Subscribers) =:= 0 ->
    State;
dispatch(#state{dispatch_queue = DispatchQueue} = State0) ->
    case queue:out(DispatchQueue) of
        {{value, MessageId}, DispatchQueue1} ->
            State1 = State0#state{dispatch_queue = DispatchQueue1},
            State = dispatch_message(MessageId, State1),
            dispatch(State);
        {empty, _DispatchQueue1} ->
            State0
    end.

dispatch_message(MessageId, #state{messages = Messages, subscribers = Subscribers} = State) ->
    Message = maps:get(MessageId, Messages),
    SubscriberId = pick_subscriber(Message, Subscribers),
    dispatch_to_subscriber(MessageId, Message, SubscriberId, State).

pick_subscriber(_Message, Subscribers) ->
    %% TODO
    %% implement strategies
    N = map_size(Subscribers),
    RandomIndex = rand:uniform(N),
    lists:nth(RandomIndex, maps:keys(Subscribers)).

dispatch_to_subscriber(
    MessageId, Message, SubscriberId, #state{subscribers = Subscribers0} = State0
) ->
    #{inflight_messages := InflightMessages0} =
        SubscriberData0 = maps:get(SubscriberId, Subscribers0),
    InflightMessages = InflightMessages0#{MessageId => now_ms_monotonic()},
    SubscriberData = SubscriberData0#{inflight_messages => InflightMessages},
    Subscribers = Subscribers0#{SubscriberId => SubscriberData},
    State = State0#state{subscribers = Subscribers},
    ok = send_message_to_subscriber(MessageId, Message, SubscriberId),
    State.

ensure_ping_timer(#state{ping_timer_ref = PingTimerRef, subscribers = Subscribers} = State) when
    map_size(Subscribers) =:= 0
->
    _ = emqx_utils:cancel_timer(PingTimerRef),
    State#state{ping_timer_ref = undefined};
ensure_ping_timer(#state{ping_timer_ref = _PingTimerRef} = State) ->
    TRref = erlang:send_after(?DEFAULT_PING_INTERVAL, self(), #ping_subscribers{}),
    State#state{ping_timer_ref = TRref}.

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

%%--------------------------------------------------------------------
%% Communication with the subscribers
%%--------------------------------------------------------------------

%% TODO
%% use proto

send_ping_to_subscriber(SubscriberId) ->
    _ = erlang:send(SubscriberId, ?MQ_PING_SUBSCRIBER(SubscriberId)),
    ok.

send_message_to_subscriber(MessageId, Message0, SubscriberId) ->
    Message1 = emqx_message:set_headers(
        #{?MQ_HEADER_MESSAGE_ID => MessageId, ?MQ_HEADER_SUBSCRIBER_ID => SubscriberId},
        Message0
    ),
    _ = erlang:send(SubscriberId, ?MQ_MESSAGE(Message1)),
    ok.
