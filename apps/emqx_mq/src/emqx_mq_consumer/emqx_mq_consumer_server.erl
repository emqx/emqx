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
    inflight_messages := #{message_id() => monotonic_timestamp_ms()},
    last_ack_ts := monotonic_timestamp_ms()
}.

-define(ping_timer, ping_timer).
-define(shutdown_timer, shutdown_timer).
-define(dispatch_timer, dispatch_timer).

-type dispatch_strategy() :: random | least_inflight | {hash, emqx_variform:compiled()}.

-record(st, {
    mq :: emqx_mq_types:mq(),
    subscribers :: #{subscriber_ref() => subscriber_data()},
    messages :: #{message_id() => emqx_types:message()},
    dispatch_queue :: emqx_mq_consumer_dispatchq:t(),
    dispatch_strategy :: dispatch_strategy(),
    timers :: #{
        ?ping_timer => emqx_maybe:t(reference()),
        ?shutdown_timer => emqx_maybe:t(reference()),
        ?dispatch_timer => emqx_maybe:t(reference())
    }
}).

-type t() :: #st{}.

-type event() :: {ds_ack, message_id()} | shutdown.

-export_type([t/0, event/0]).

%%--------------------------------------------------------------------
%% Messages
%%--------------------------------------------------------------------

-record(timer_message, {timer_name :: ?ping_timer | ?shutdown_timer | ?dispatch_timer}).

-record(subscriber_timeout, {
    subscriber_ref :: subscriber_ref()
}).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% TODO
%% Tidy up the St vs State naming and position of the state argument

-spec new(emqx_mq_types:mq()) -> t().
new(MQ) ->
    #st{
        mq = MQ,
        subscribers = #{},
        messages = #{},
        dispatch_strategy = dispatch_strategy(MQ),
        dispatch_queue = emqx_mq_consumer_dispatchq:new(),
        timers = #{
            ?ping_timer => undefined,
            ?shutdown_timer => undefined,
            ?dispatch_timer => undefined
        }
    }.

-spec handle_messages(t(), [{message_id(), emqx_types:message()}]) -> t().
handle_messages(State, Messages) ->
    dispatch(add_new_messages(State, Messages)).

-spec handle_info(t(), term()) -> {ok, [event()], t()}.
handle_info(St, #mq_server_connect{subscriber_ref = SubscriberRef, client_id = ClientId}) ->
    {ok, [], handle_connect(St, SubscriberRef, ClientId)};
handle_info(St, #mq_server_disconnect{subscriber_ref = SubscriberRef}) ->
    {ok, [], handle_disconnect(St, SubscriberRef)};
handle_info(St, #mq_server_ack{subscriber_ref = SubscriberRef, message_id = MessageId, ack = Ack}) ->
    handle_ack(St, SubscriberRef, MessageId, Ack);
handle_info(St, #mq_server_ping{subscriber_ref = SubscriberRef}) ->
    {ok, [], handle_ping(St, SubscriberRef)};
handle_info(St, #timer_message{timer_name = ?ping_timer}) ->
    {ok, [], handle_ping_subscribers(St)};
handle_info(St, #timer_message{timer_name = ?shutdown_timer}) ->
    handle_shutdown(St);
handle_info(St, #timer_message{timer_name = ?dispatch_timer}) ->
    {ok, [], handle_dispatch(St)};
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
            SubscriberData1 = refresh_subscriber_timeout(State, SubscriberRef, SubscriberData0),
            Subscribers1 = Subscribers0#{SubscriberRef => SubscriberData1},
            State#st{subscribers = Subscribers1};
        _ ->
            Subscribers1 = Subscribers0#{
                SubscriberRef => initial_subscriber_data(State, SubscriberRef, ClientId)
            },
            ok = send_connected_to_subscriber(SubscriberRef),
            dispatch(update_client_timers(State#st{subscribers = Subscribers1}))
    end.

handle_disconnect(#st{subscribers = Subscribers0} = State0, SubscriberRef) ->
    case Subscribers0 of
        #{SubscriberRef := #{inflight_messages := InflightMessages} = SubscriberData} ->
            _ = cancel_subscriber_timeout(SubscriberData),
            Subscribers = maps:remove(SubscriberRef, Subscribers0),
            State1 = State0#st{subscribers = Subscribers},
            State = schedule_for_dispatch(State1, maps:keys(InflightMessages)),
            dispatch(update_client_timers(State));
        _ ->
            State0
    end.

handle_ack(
    #st{subscribers = Subscribers0, messages = Messages0} = State0,
    SubscriberRef,
    MessageId,
    Ack
) ->
    ?tp(warning, mq_consumer_handle_ack, #{
        subscriber_ref => SubscriberRef, message_id => MessageId, ack => Ack
    }),
    maybe
        #{SubscriberRef := SubscriberData0} ?= Subscribers0,
        #{inflight_messages := InflightMessages0} ?= SubscriberData0,
        #{MessageId := _} ?= InflightMessages0,
        InflightMessages = maps:remove(MessageId, InflightMessages0),
        SubscriberData1 = SubscriberData0#{inflight_messages => InflightMessages},
        SubscriberData2 = refresh_subscriber_timeout(State0, SubscriberRef, SubscriberData1),
        SubscriberData3 = update_last_ack(SubscriberData2),
        Subscribers = Subscribers0#{SubscriberRef => SubscriberData3},
        State1 = State0#st{subscribers = Subscribers},
        case Ack of
            ?MQ_ACK ->
                Messages = maps:remove(MessageId, Messages0),
                State = State1#st{messages = Messages},
                {ok, [{ds_ack, MessageId}], State};
            ?MQ_REJECTED ->
                {ok, [], dispatch(schedule_for_dispatch(State1, [MessageId]))}
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
            SubscriberData = refresh_subscriber_timeout(State, SubscriberRef, SubscriberData0),
            Subscribers = Subscribers0#{SubscriberRef => SubscriberData},
            State#st{subscribers = Subscribers};
        _ ->
            State
    end.

handle_ping_subscribers(#st{subscribers = Subscribers} = State0) ->
    ?tp(warning, mq_consumer_handle_ping_subscribers, #{subscribers => maps:keys(Subscribers)}),
    State1 = cancel_timer(?ping_timer, State0),
    ok = maps:foreach(
        fun(SubscriberRef, _SubscriberData) ->
            send_ping_to_subscriber(SubscriberRef)
        end,
        Subscribers
    ),
    ensure_timer(?ping_timer, State1).

handle_subscriber_timeout(SubscriberRef, State) ->
    handle_disconnect(SubscriberRef, State).

handle_dispatch(State) ->
    dispatch(State).

handle_shutdown(State) ->
    {ok, [shutdown], State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_new_messages(#st{messages = Messages0} = State, MessagesWithIds) ->
    {MessageIds, Messages} = lists:foldl(
        fun({MessageId, Message}, {IdsAcc, MessagesAcc}) ->
            {[MessageId | IdsAcc], MessagesAcc#{MessageId => Message}}
        end,
        {[], Messages0},
        MessagesWithIds
    ),
    schedule_for_dispatch(State#st{messages = Messages}, MessageIds).

refresh_subscriber_timeout(State, SubscriberRef, SubscriberData0) ->
    SubscriberData = cancel_subscriber_timeout(SubscriberData0),
    TimeoutTRef = send_after(
        2 * timeout(?ping_timer, State),
        #subscriber_timeout{subscriber_ref = SubscriberRef}
    ),
    SubscriberData#{timeout_tref => TimeoutTRef}.

update_last_ack(SubscriberData0) ->
    SubscriberData0#{last_ack_ts => now_ms_monotonic()}.

cancel_subscriber_timeout(#{timeout_tref := TimeoutTRef} = SubscriberData) ->
    emqx_utils:cancel_timer(TimeoutTRef),
    SubscriberData#{timeout_tref => undefined}.

initial_subscriber_data(State, SubscriberRef, ClientId) ->
    SubscriberData = #{
        timeout_tref => undefined,
        client_id => ClientId,
        inflight_messages => #{},
        last_ack_ts => now_ms_monotonic()
    },
    refresh_subscriber_timeout(State, SubscriberRef, SubscriberData).

schedule_for_dispatch(#st{dispatch_queue = DispatchQueue0} = State, MessageIds) ->
    DispatchQueue1 = emqx_mq_consumer_dispatchq:add(DispatchQueue0, MessageIds),
    State#st{dispatch_queue = DispatchQueue1}.

dispatch(#st{subscribers = Subscribers} = State) when map_size(Subscribers) =:= 0 ->
    cancel_timer(?dispatch_timer, State);
dispatch(#st{dispatch_queue = DispatchQueue} = State0) ->
    State1 = cancel_timer(?dispatch_timer, State0),
    case emqx_mq_consumer_dispatchq:fetch(DispatchQueue) of
        empty ->
            State1;
        {ok, MessageIds, DispatchQueue1} ->
            State2 = State0#st{dispatch_queue = DispatchQueue1},
            State3 = dispatch_messages(MessageIds, State2),
            dispatch(State3);
        {delay, DelayMs} ->
            ensure_timer(?dispatch_timer, DelayMs, State1)
    end.

dispatch_messages(MessageIds, State) ->
    lists:foldl(
        fun(MessageId, StateAcc) -> dispatch_message(MessageId, StateAcc) end,
        State,
        MessageIds
    ).

dispatch_message(MessageId, #st{messages = Messages, dispatch_queue = DispatchQueue0} = State) ->
    Message = maps:get(MessageId, Messages),
    case pick_subscriber(Message, State) of
        {ok, SubscriberRef} ->
            dispatch_to_subscriber(MessageId, Message, SubscriberRef, State);
        no_subscriber ->
            DispatchQueue = emqx_mq_consumer_dispatchq:add_redispatch(
                DispatchQueue0, MessageId, redispatch_interval(State)
            ),
            State#st{dispatch_queue = DispatchQueue}
    end.

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

update_client_timers(#st{subscribers = Subscribers} = State0) when map_size(Subscribers) =:= 0 ->
    State1 = cancel_timer(?ping_timer, State0),
    State2 = cancel_timer(?dispatch_timer, State1),
    ensure_timer(?shutdown_timer, State2);
update_client_timers(#st{} = State0) ->
    State1 = ensure_timer(?ping_timer, State0),
    cancel_timer(?shutdown_timer, State1).

%%--------------------------------------------------------------------
%% Dispatch strategies
%%--------------------------------------------------------------------

pick_subscriber(Message, #st{dispatch_strategy = random} = State) ->
    pick_subscriber_random(Message, State);
pick_subscriber(Message, #st{dispatch_strategy = least_inflight} = State) ->
    pick_subscriber_least_inflight(Message, State);
pick_subscriber(Message, #st{dispatch_strategy = {hash, Expression}} = State) ->
    pick_subscriber_hash(Message, Expression, State).

%% Random dispatch strategy

pick_subscriber_random(_Message, #st{subscribers = Subscribers} = _State) ->
    %% TODO
    %% cache healthy subscribers
    case maps:keys(Subscribers) of
        [] ->
            no_subscriber;
        SubscriberRefs ->
            RandIndex = rand:uniform(length(SubscriberRefs)),
            {ok, lists:nth(RandIndex, SubscriberRefs)}
    end.

%% Least inflight dispatch strategy

pick_subscriber_least_inflight(_Message, #st{subscribers = Subscribers} = _State) ->
    {SubscriberRef, _} = maps:fold(
        fun(
            SubscriberRef,
            #{inflight_messages := InflightMessages} = _SubscriberData,
            {LeastSubscriberRef, LeastInflightCount}
        ) ->
            case map_size(InflightMessages) of
                InflightCount when InflightCount < LeastInflightCount ->
                    {SubscriberRef, InflightCount};
                _ ->
                    {LeastSubscriberRef, LeastInflightCount}
            end
        end,
        {undefined, infinity},
        Subscribers
    ),
    case SubscriberRef of
        undefined ->
            no_subscriber;
        _ ->
            {ok, SubscriberRef}
    end.

%% Hash dispatch strategy

pick_subscriber_hash(Message, Expression, #st{subscribers = Subscribers} = State) ->
    %% NOTE
    %% We may render hashed value on publish to significantly reduce
    %% the load on the server process.
    case emqx_variform:render(Expression, #{message => Message}, #{eval_as_string => false}) of
        {ok, Value} ->
            SubscriberRefs = maps:keys(Subscribers),
            SubscriberCount = length(SubscriberRefs),
            SelectedIdx = erlang:phash2(Value, SubscriberCount),
            SelectedSubscriberRef = lists:nth(SelectedIdx + 1, SubscriberRefs),
            {ok, SelectedSubscriberRef};
        {error, Error} ->
            ?tp(warning, mq_consumer_pick_subscriber_hash_error, #{
                message => Message, error => Error, mq_topic_filter => mq_topic_filter(State)
            }),
            pick_subscriber_random(Message, State)
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

ensure_timer(TimerName, State) ->
    ensure_timer(TimerName, timeout(TimerName, State), State).

ensure_timer(TimerName, Timeout, #st{timers = Timers0} = State) ->
    Timers =
        case Timers0 of
            #{TimerName := undefined} ->
                TRref = send_after(Timeout, #timer_message{timer_name = TimerName}),
                Timers0#{TimerName => TRref};
            _ ->
                Timers0
        end,
    State#st{timers = Timers}.

cancel_timer(TimerName, #st{timers = Timers0} = State) ->
    Timers =
        case Timers0 of
            #{TimerName := undefined} ->
                Timers0;
            #{TimerName := TimerRef} ->
                _ = emqx_utils:cancel_timer(TimerRef),
                Timers0#{TimerName => undefined}
        end,
    State#st{timers = Timers}.

send_after(Timeout, Message) ->
    erlang:send_after(Timeout, self_consumer_ref(), #info_to_mq_server{message = Message}).

now_ms_monotonic() ->
    erlang:monotonic_time(millisecond).

timeout(?ping_timer, #st{mq = #{ping_interval_ms := ConsumerPingIntervalMs}}) ->
    ConsumerPingIntervalMs;
timeout(?shutdown_timer, #st{mq = #{consumer_max_inactive_ms := ConsumerMaxInactiveMs}}) ->
    ConsumerMaxInactiveMs.

dispatch_strategy(#{dispatch_strategy := random}) ->
    random;
dispatch_strategy(#{dispatch_strategy := least_inflight}) ->
    least_inflight;
dispatch_strategy(#{dispatch_strategy := {hash, Expression}}) ->
    %% Need additionally check if the expression is valid?
    Compiled = emqx_mq_utils:dispatch_variform_compile(Expression),
    {hash, Compiled}.

redispatch_interval(#st{mq = #{redispatch_interval_ms := RedispatchIntervalMs}}) ->
    RedispatchIntervalMs.

mq_topic_filter(#st{mq = #{topic_filter := TopicFilter}}) ->
    TopicFilter.

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
