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
* Track consumption progeress of the topic streams.
""".

-include("emqx_mq_internal.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-export([
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

-type subscriber_ref() :: emqx_mq_types:subscriber_ref().
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
    subscribers :: #{subscriber_ref() => subscriber_data()},
    messages :: #{message_id() => emqx_types:message()},
    topic_filter :: binary(),
    dispatch_queue :: queue:queue(),
    ping_timer_ref :: reference() | undefined
}).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(connect, {
    subscriber_ref :: subscriber_ref(),
    client_id :: emqx_types:client_id()
}).

-record(disconnect, {
    subscriber_ref :: subscriber_ref()
}).

-record(ack, {
    subscriber_ref :: subscriber_ref(),
    message_id :: message_id(),
    ack :: emqx_mq_types:ack()
}).

-record(ping, {
    subscriber_ref :: subscriber_ref()
}).

-record(ping_subscribers, {}).

-record(subscriber_timeout, {
    subscriber_ref :: subscriber_ref()
}).

%% TODO
%% Remove, used for tests
-record(gen_message, {n :: non_neg_integer()}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec connect(binary(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()) ->
    ok | {error, term()}.
connect(MQTopicFilter, SubscriberRef, ClientId) ->
    maybe
        {ok, Pid} ?= emqx_mq_sup:start_consumer(MQTopicFilter, [MQTopicFilter]),
        Rnd = rand:uniform(2),
        case Rnd of
            1 ->
                ?tp(warning, mq_consumer_connect_success, #{
                    subscriber_ref => SubscriberRef, client_id => ClientId, rnd => Rnd
                }),
                gen_server:cast(
                    Pid,
                    #connect{subscriber_ref = SubscriberRef, client_id = ClientId}
                );
            2 ->
                ?tp(warning, mq_consumer_connect_fake_failure, #{
                    subscriber_ref => SubscriberRef, client_id => ClientId
                }),
                ok
        end
    end.

-spec disconnect(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
disconnect(Pid, SubscriberRef) ->
    gen_server:cast(
        Pid,
        #disconnect{subscriber_ref = SubscriberRef}
    ).

-spec ack(
    emqx_mq_types:consumer_ref(),
    emqx_mq_types:subscriber_ref(),
    emqx_mq_types:message_id(),
    emqx_mq_types:ack()
) -> ok.
ack(Pid, SubscriberRef, MessageId, Ack) ->
    gen_server:cast(
        Pid,
        #ack{subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack}
    ).

-spec ping(emqx_mq_types:consumer_ref(), emqx_mq_types:subscriber_ref()) -> ok.
ping(Pid, SubscriberRef) ->
    gen_server:cast(
        Pid,
        #ping{subscriber_ref = SubscriberRef}
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

handle_cast(#connect{subscriber_ref = SubscriberRef, client_id = ClientId}, State) ->
    {noreply, handle_connect(SubscriberRef, ClientId, State)};
handle_cast(#disconnect{subscriber_ref = SubscriberRef}, State) ->
    {noreply, handle_disconnect(SubscriberRef, State)};
handle_cast(#ack{subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack}, State) ->
    {noreply, handle_ack(SubscriberRef, MessageId, Ack, State)};
handle_cast(#ping{subscriber_ref = SubscriberRef}, State) ->
    {noreply, handle_ping(SubscriberRef, State)};
handle_cast(Request, State) ->
    ?tp(warning, mq_consumer_cast_unknown_request, #{request => Request}),
    {noreply, State}.

handle_info(#ping_subscribers{}, State) ->
    {noreply, handle_ping_subscribers(State)};
handle_info(#subscriber_timeout{subscriber_ref = SubscriberRef}, State) ->
    {noreply, handle_subscriber_timeout(SubscriberRef, State)};
handle_info(#gen_message{n = N}, State) ->
    {noreply, handle_gen_message(N, State)};
handle_info(_Request, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

%%--------------------------------------------------------------------
%% Handlers
%%--------------------------------------------------------------------

handle_connect(SubscriberRef, ClientId, #state{subscribers = Subscribers0} = State) ->
    ?tp(warning, mq_consumer_handle_connect, #{
        subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case Subscribers0 of
        #{SubscriberRef := SubscriberData0} ->
            SubscriberData1 = refresh_subscriber_timeout(SubscriberRef, SubscriberData0),
            Subscribers1 = Subscribers0#{SubscriberRef => SubscriberData1},
            State#state{subscribers = Subscribers1};
        _ ->
            Subscribers1 = Subscribers0#{
                SubscriberRef => initial_subscriber_data(SubscriberRef, ClientId)
            },
            ok = send_info_to_subscriber(SubscriberRef, {connected, self()}),
            dispatch(ensure_ping_timer(State#state{subscribers = Subscribers1}))
    end.

handle_disconnect(SubscriberRef, #state{subscribers = Subscribers0} = State0) ->
    case Subscribers0 of
        #{SubscriberRef := #{inflight_messages := InflightMessages} = SubscriberData} ->
            _ = cancel_subscriber_timeout(SubscriberData),
            Subscribers = maps:remove(SubscriberRef, Subscribers0),
            State1 = State0#state{subscribers = Subscribers},
            State = enqueue_for_dispatch(maps:keys(InflightMessages), State1),
            dispatch(ensure_ping_timer(State));
        _ ->
            State0
    end.

handle_ack(
    SubscriberRef, MessageId, Ack, #state{subscribers = Subscribers0, messages = Messages0} = State0
) ->
    maybe
        #{SubscriberRef := SubscriberData0} ?= Subscribers0,
        #{inflight_messages := InflightMessages0} ?= SubscriberData0,
        #{MessageId := _} ?= InflightMessages0,
        InflightMessages = maps:remove(MessageId, InflightMessages0),
        SubscriberData1 = SubscriberData0#{inflight_messages => InflightMessages},
        SubscriberData = refresh_subscriber_timeout(SubscriberRef, SubscriberData1),
        Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
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

handle_ping(SubscriberRef, #state{subscribers = Subscribers0} = State) ->
    case Subscribers0 of
        #{SubscriberRef := SubscriberData0} ->
            SubscriberData = refresh_subscriber_timeout(SubscriberRef, SubscriberData0),
            Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
            State#state{subscribers = Subscribers};
        _ ->
            State
    end.

handle_ping_subscribers(#state{subscribers = Subscribers} = State) ->
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

%% TODO
%% Remove, used for tests
handle_gen_message(N, #state{messages = Messages0, topic_filter = TopicFilter} = State) ->
    Topic = TopicFilter,
    Payload = iolist_to_binary(io_lib:format("dummy message ~p", [N])),
    Message = emqx_message:make(<<"dummy client">>, ?QOS_1, Topic, Payload),
    MessageId = N,
    Messages = Messages0#{MessageId => Message},
    _ = erlang:send_after(500, self(), #gen_message{n = N + 1}),
    % ?tp(warning, mq_consumer_handle_gen_message, #{message_id => MessageId}),
    dispatch(enqueue_for_dispatch([MessageId], State#state{messages = Messages})).

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

refresh_subscriber_timeout(SubscriberRef, SubscriberData0) ->
    SubscriberData = cancel_subscriber_timeout(SubscriberData0),
    TimeoutTRef = erlang:send_after(
        ?DEFAULT_SUBSCRIBER_TIMEOUT,
        self(),
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
    SubscriberRef = pick_subscriber(Message, Subscribers),
    dispatch_to_subscriber(MessageId, Message, SubscriberRef, State).

pick_subscriber(_Message, Subscribers) ->
    %% TODO
    %% implement strategies
    N = map_size(Subscribers),
    RandomIndex = rand:uniform(N),
    lists:nth(RandomIndex, maps:keys(Subscribers)).

dispatch_to_subscriber(
    MessageId, Message, SubscriberRef, #state{subscribers = Subscribers0} = State0
) ->
    #{inflight_messages := InflightMessages0} =
        SubscriberData0 = maps:get(SubscriberRef, Subscribers0),
    InflightMessages = InflightMessages0#{MessageId => now_ms_monotonic()},
    SubscriberData = SubscriberData0#{inflight_messages => InflightMessages},
    Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
    State = State0#state{subscribers = Subscribers},
    ok = send_message_to_subscriber(MessageId, Message, SubscriberRef),
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

send_info_to_subscriber(SubscriberRef, InfoMsg) ->
    _ = erlang:send(SubscriberRef, ?MQ_SUB_INFO(SubscriberRef, InfoMsg)),
    ok.

send_ping_to_subscriber(SubscriberRef) ->
    _ = erlang:send(SubscriberRef, ?MQ_PING_SUBSCRIBER(SubscriberRef)),
    ok.

send_message_to_subscriber(MessageId, Message0, SubscriberRef) ->
    Message1 = emqx_message:set_headers(
        #{?MQ_HEADER_MESSAGE_ID => MessageId, ?MQ_HEADER_SUBSCRIBER_ID => SubscriberRef},
        Message0
    ),
    _ = erlang:send(SubscriberRef, ?MQ_MESSAGE(Message1)),
    ok.
