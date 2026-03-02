%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_buffer).

-behaviour(gen_server).

-include_lib("kernel/include/logger.hrl").

-export([start_link/2, enqueue/3, get_pid/2]).

-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2]).

-define(DEFAULT_BATCH_SIZE, 100).
-define(FLUSH_INTERVAL_MS, 10).
-define(RETRY_DELAY_MS, 1000).
-define(MIN_DISCARD_LOG_INTERVAL, 5000).

-type queue_item() :: {
    _Topic :: emqx_types:topic(),
    _Payload :: binary(),
    _QoS :: emqx_types:qos(),
    _Retain :: boolean(),
    _Timestamp :: integer(),
    _Properties :: emqx_types:properties()
}.

-export_type([queue_item/0]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

-spec start_link(map(), non_neg_integer()) -> emqx_types:startlink_ret().
start_link(BridgeConfig, Index) ->
    gen_server:start_link(?MODULE, {BridgeConfig, Index}, []).

-spec enqueue(pid(), queue_item(), no_ack | reference()) -> ok.
enqueue(Pid, Item, AckAlias) ->
    gen_server:cast(Pid, {enqueue, Item, AckAlias}).

-spec get_pid(binary(), non_neg_integer()) -> pid().
get_pid(BridgeName, Index) ->
    persistent_term:get({?MODULE, BridgeName, Index}).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init({BridgeConfig, Index}) ->
    #{
        name := BridgeName,
        queue_dir := QueueDirBase,
        seg_bytes := SegBytes,
        max_total_bytes := MaxTotalBytes
    } = BridgeConfig,
    QueueDir = queue_dir(QueueDirBase, Index),
    ok = filelib:ensure_dir(filename:join(QueueDir, "dummy")),
    Q = replayq:open(#{
        dir => QueueDir,
        seg_bytes => SegBytes,
        max_total_bytes => MaxTotalBytes,
        marshaller => fun marshaller/1,
        sizer => fun sizer/1,
        mem_queue_module => replayq_mem_ets_exclusive
    }),
    %% Register for fast lookup from hook callback
    persistent_term:put({?MODULE, BridgeName, Index}, self()),
    State = #{
        bridge_name => BridgeName,
        index => Index,
        queue => Q,
        bridge_config => BridgeConfig,
        flush_timer => undefined,
        batch_size => ?DEFAULT_BATCH_SIZE
    },
    %% If queue has items from previous run, flush immediately
    State1 = maybe_schedule_flush(State),
    {ok, State1}.

handle_cast({enqueue, Item, AckAlias}, #{queue := Q} = State) ->
    Q1 = replayq:append(Q, [Item]),
    Q2 = handle_overflow(Q1, State),
    ack_enqueue(AckAlias),
    State1 = State#{queue := Q2},
    State2 = maybe_schedule_flush(State1),
    {noreply, State2};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info(flush, State) ->
    State1 = State#{flush_timer := undefined},
    State2 = do_flush(State1),
    {noreply, State2};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{queue := Q, bridge_name := BridgeName, index := Index}) ->
    _ = replayq:close(Q),
    _ = persistent_term:erase({?MODULE, BridgeName, Index}),
    ok.

%%--------------------------------------------------------------------
%% Internal: flush
%%--------------------------------------------------------------------

do_flush(#{queue := Q} = State) ->
    case replayq:is_empty(Q) of
        true -> State;
        false -> flush_batch(State)
    end.

flush_batch(#{queue := Q, batch_size := BatchSize} = State) ->
    {Q1, AckRef, Items} = replayq:pop(Q, #{count_limit => BatchSize}),
    {Remaining, State1} = publish_batch(Items, State#{queue := Q1}),
    ack_and_continue(Remaining, AckRef, State1).

ack_and_continue([], AckRef, #{queue := Q} = State) ->
    ok = replayq:ack(Q, AckRef),
    maybe_schedule_flush(State);
ack_and_continue(Remaining, AckRef, #{queue := Q} = State) ->
    Q1 = replayq:append(Q, Remaining),
    ok = replayq:ack(Q1, AckRef),
    schedule_retry(State#{queue := Q1}).

publish_batch([], State) ->
    {[], State};
publish_batch([Item | Rest] = All, State) ->
    case publish_one(Item, State) of
        ok ->
            publish_batch(Rest, State);
        {error, network} ->
            %% Network error — return all remaining (including this one) for re-enqueue
            {All, State};
        {error, rejected} ->
            %% Broker rejected — skip this item, continue with rest
            publish_batch(Rest, State)
    end.

publish_one({OrigTopic, Payload, QoS, Retain, _Ts, Props}, State) ->
    #{
        bridge_name := BridgeName,
        bridge_config := #{
            remote_topic := RemoteTopicTemplate,
            remote_qos := ConfigQoS,
            remote_retain := ConfigRetain,
            pool_size := PoolSize
        }
    } = State,
    RenderedTopic = render_topic(RemoteTopicTemplate, OrigTopic),
    PubQoS = resolve_qos(QoS, ConfigQoS),
    PubRetain = resolve_retain(Retain, ConfigRetain),
    ConnIndex = erlang:phash2(OrigTopic, PoolSize),
    case get_connector_pid(BridgeName, ConnIndex) of
        {ok, ConnPid} ->
            PubOpts = [{qos, PubQoS}, {retain, PubRetain}],
            do_publish(ConnPid, RenderedTopic, Props, Payload, PubOpts, BridgeName);
        {error, _} ->
            {error, network}
    end.

do_publish(ConnPid, Topic, Props, Payload, PubOpts, BridgeName) ->
    try emqtt:publish(ConnPid, Topic, Props, Payload, PubOpts) of
        Result -> handle_publish_result(Result, Topic, BridgeName)
    catch
        _:_ -> {error, network}
    end.

handle_publish_result(ok, _Topic, _BridgeName) ->
    ok;
handle_publish_result({ok, #{reason_code := RC}}, _Topic, _BridgeName) when
    RC =:= 0; RC =:= 16
->
    ok;
handle_publish_result({ok, #{reason_code := _RC}}, Topic, BridgeName) ->
    ?LOG_WARNING(#{
        msg => "mqtt_dq_publish_rejected",
        bridge => BridgeName,
        topic => Topic
    }),
    {error, rejected};
handle_publish_result({error, Reason}, Topic, BridgeName) ->
    ?LOG_ERROR(#{
        msg => "mqtt_dq_publish_error",
        bridge => BridgeName,
        topic => Topic,
        reason => Reason
    }),
    classify_error(Reason).

get_connector_pid(BridgeName, ConnIndex) ->
    case ets:lookup(emqx_bridge_mqtt_dq_conns, {BridgeName, ConnIndex}) of
        [{_, Pid}] when is_pid(Pid) -> check_alive(Pid);
        _ -> {error, not_found}
    end.

check_alive(Pid) ->
    case is_process_alive(Pid) of
        true -> {ok, Pid};
        false -> {error, not_alive}
    end.

classify_error(disconnected) -> {error, network};
classify_error(tcp_closed) -> {error, network};
classify_error(closed) -> {error, network};
classify_error(einval) -> {error, network};
classify_error({shutdown, _}) -> {error, network};
classify_error({transport_error, _}) -> {error, network};
classify_error(_) -> {error, network}.

%%--------------------------------------------------------------------
%% Internal: scheduling
%%--------------------------------------------------------------------

maybe_schedule_flush(#{flush_timer := undefined, queue := Q} = State) ->
    case replayq:is_empty(Q) of
        true -> State;
        false -> schedule_flush(State)
    end;
maybe_schedule_flush(State) ->
    %% Timer already pending
    State.

schedule_flush(State) ->
    Ref = erlang:send_after(?FLUSH_INTERVAL_MS, self(), flush),
    State#{flush_timer := Ref}.

schedule_retry(State) ->
    Ref = erlang:send_after(?RETRY_DELAY_MS, self(), flush),
    State#{flush_timer := Ref}.

%%--------------------------------------------------------------------
%% Internal: overflow
%%--------------------------------------------------------------------

%% replayq:append never refuses items. After appending, check if the
%% queue has exceeded max_total_bytes. If so, pop the oldest items to
%% bring it back within limits and ack (discard) them immediately.
handle_overflow(Q, #{bridge_name := BridgeName, index := Index}) ->
    case replayq:overflow(Q) of
        Overflow when Overflow =< 0 ->
            Q;
        Overflow ->
            {Q1, QAckRef, Dropped} =
                replayq:pop(Q, #{bytes_limit => Overflow, count_limit => 999999999}),
            ok = replayq:ack(Q1, QAckRef),
            NumDropped = length(Dropped),
            maybe_log_discard(BridgeName, Index, NumDropped),
            Q1
    end.

maybe_log_discard(BridgeName, Index, NumDropped) ->
    Key = {?MODULE, discard_log, BridgeName, Index},
    Now = erlang:monotonic_time(millisecond),
    {LastTs, AccDropped} =
        case get(Key) of
            undefined -> {0, 0};
            V -> V
        end,
    NewAcc = AccDropped + NumDropped,
    case Now - LastTs > ?MIN_DISCARD_LOG_INTERVAL of
        true ->
            ?LOG_WARNING(#{
                msg => "mqtt_dq_buffer_overflow",
                bridge => BridgeName,
                index => Index,
                dropped => NewAcc
            }),
            put(Key, {Now, 0});
        false ->
            put(Key, {LastTs, NewAcc})
    end.

%%--------------------------------------------------------------------
%% Internal: helpers
%%--------------------------------------------------------------------

ack_enqueue(no_ack) -> ok;
ack_enqueue(Alias) -> Alias ! {Alias, ok}.

-spec render_topic(binary(), emqx_types:topic()) -> emqx_types:topic().
render_topic(<<"${topic}">>, OrigTopic) ->
    OrigTopic;
render_topic(Template, OrigTopic) ->
    binary:replace(Template, <<"${topic}">>, OrigTopic, [global]).

-spec resolve_qos(emqx_types:qos(), emqx_types:qos()) -> emqx_types:qos().
resolve_qos(_OrigQoS, ConfigQoS) ->
    %% Use the configured QoS for the bridge
    ConfigQoS.

-spec resolve_retain(boolean(), boolean()) -> boolean().
resolve_retain(_OrigRetain, ConfigRetain) ->
    %% Use the configured retain for the bridge
    ConfigRetain.

queue_dir(Base, Index) ->
    Dir = binary_to_list(iolist_to_binary([Base, "/", integer_to_binary(Index)])),
    Dir.

marshaller(Bin) when is_binary(Bin) -> binary_to_term(Bin);
marshaller(Term) -> term_to_binary(Term).

sizer(Item) -> erlang:external_size(Item).
