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
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
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
-type message_id() :: emqx_mq_types:message_id().
-type monotonic_timestamp_ms() :: emqx_mq_types:monotonic_timestamp_ms().
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
    consumer :: emqx_mq_consumer_streams:t(),
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

-record(persist_consumer_data, {}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link(MQTopicFilter) ->
    gen_server:start_link(?MODULE, [MQTopicFilter], []).

-spec connect(binary(), emqx_mq_types:subscriber_ref(), emqx_types:clientid()) ->
    ok | {error, term()}.
connect(MQTopicFilter, SubscriberRef, ClientId) ->
    ?tp(warning, mq_consumer_connect, #{
        mq_topic_filter => MQTopicFilter, subscriber_ref => SubscriberRef, client_id => ClientId
    }),
    case find_consumer(MQTopicFilter, 2) of
        {ok, ConsumerRef} ->
            ?tp(warning, mq_consumer_find_consumer_success, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId,
                consumer_ref => ConsumerRef
            }),
            gen_server:cast(ConsumerRef, #connect{
                subscriber_ref = SubscriberRef, client_id = ClientId
            });
        not_found ->
            ?tp(warning, mq_consumer_find_consumer_not_found, #{
                mq_topic_filter => MQTopicFilter,
                subscriber_ref => SubscriberRef,
                client_id => ClientId
            }),
            {error, consumer_not_found}
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
    ClaimRes = emqx_mq_consumer_db:claim_leadership(MQTopicFilter, self_consumer_ref(), now_ms()),
    ?tp(warning, mq_consumer_init, #{mq_topic_filter => MQTopicFilter, claim_res => ClaimRes}),
    case ClaimRes of
        {ok, ConsumerData} ->
            erlang:send_after(
                ?DEFAULT_CONSUMER_PERSISTENCE_INTERVAL, self(), #persist_consumer_data{}
            ),
            Progress = maps:get(progress, ConsumerData, #{}),
            {ok, #state{
                topic_filter = MQTopicFilter,
                subscribers = #{},
                messages = #{},
                dispatch_queue = queue:new(),
                consumer = emqx_mq_consumer_streams:new(MQTopicFilter, Progress),
                ping_timer_ref = undefined
            }};
        {error, _, _} = Error ->
            {stop, Error}
    end.

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
handle_info(#persist_consumer_data{}, State) ->
    handle_persist_consumer_data(State);
handle_info(Request, #state{topic_filter = MQTopicFilter} = State0) ->
    ?tp(warning, mq_consumer_handle_info, #{request => Request, mq_topic => MQTopicFilter}),
    case handle_ds_info(Request, State0) of
        ignore ->
            ?tp(warning, mq_consumer_unknown_info, #{request => Request, mq_topic => MQTopicFilter}),
            {noreply, State0};
        {ok, State} ->
            {noreply, State}
    end.

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
    SubscriberRef,
    MessageId,
    Ack,
    #state{subscribers = Subscribers0, messages = Messages0, consumer = Consumer0} = State0
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
                Consumer = emqx_mq_consumer_streams:handle_ack(Consumer0, MessageId),
                State#state{messages = Messages, consumer = Consumer};
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

handle_ds_info(Request, #state{consumer = Consumer0} = State) ->
    case emqx_mq_consumer_streams:handle_ds_info(Consumer0, Request) of
        {ok, Messages, Consumer} ->
            {ok, dispatch(enqueue_messages(Messages, State#state{consumer = Consumer}))};
        ignore ->
            ignore
    end.

handle_persist_consumer_data(
    #state{topic_filter = MQTopicFilter, consumer = Consumer, subscribers = Subscribers} = State
) ->
    ConsumerData = #{
        progress => emqx_mq_consumer_streams:progress(Consumer)
    },
    ?tp(warning, mq_consumer_handle_persist_consumer_data, #{
        mq_topic_filter => MQTopicFilter,
        consumer_info => emqx_mq_consumer_streams:info(Consumer),
        consumer_data => ConsumerData
    }),
    case
        emqx_mq_consumer_db:update_consumer_data(
            MQTopicFilter,
            self_consumer_ref(),
            ConsumerData,
            now_ms()
        )
    of
        ok ->
            %% TODO
            %% track last disconnect time
            case map_size(Subscribers) > 0 of
                true ->
                    erlang:send_after(
                        ?DEFAULT_CONSUMER_PERSISTENCE_INTERVAL, self(), #persist_consumer_data{}
                    ),
                    {noreply, State};
                false ->
                    ?tp(warning, mq_consumer_drop_leadership, #{mq_topic_filter => MQTopicFilter}),
                    case emqx_mq_consumer_db:drop_leadership(MQTopicFilter) of
                        ok ->
                            {stop, normal, State};
                        Error ->
                            ?tp(error, mq_consumer_persist_consumer_data_error, #{error => Error}),
                            {stop, Error, State}
                    end
            end;
        Error ->
            ?tp(error, mq_consumer_persist_consumer_data_error, #{error => Error}),
            {stop, Error, State}
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

find_consumer(MQTopicFilter, 0) ->
    ?tp(error, mq_consumer_find_consumer_error, #{
        error => retries_exhausted, mq_topic_filter => MQTopicFilter
    }),
    not_found;
find_consumer(MQTopicFilter, Retries) ->
    case emqx_mq_consumer_db:find_consumer(MQTopicFilter, now_ms()) of
        {ok, ConsumerRef} ->
            {ok, ConsumerRef};
        not_found ->
            case emqx_mq_sup:start_consumer(_ChildId = MQTopicFilter, [MQTopicFilter]) of
                {ok, ConsumerRef} ->
                    {ok, ConsumerRef};
                {error, _} = Error ->
                    ?tp(warning, mq_consumer_find_consumer_error, #{
                        error => Error, retries => Retries, mq_topic_filter => MQTopicFilter
                    }),
                    find_consumer(MQTopicFilter, Retries - 1)
            end
    end.

enqueue_messages([], State) ->
    State;
enqueue_messages(
    [{MessageId, Payload} | Rest],
    #state{topic_filter = TopicFilter, messages = Messages0} = State0
) ->
    %% TODO
    %% use real topic and client
    Message = emqx_message:make(<<"mq">>, ?QOS_1, TopicFilter, Payload),
    Messages = Messages0#{MessageId => Message},
    State = State0#state{messages = Messages},
    enqueue_for_dispatch([MessageId], enqueue_messages(Rest, State)).

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

now_ms() ->
    erlang:system_time(millisecond).

self_consumer_ref() ->
    self().

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
