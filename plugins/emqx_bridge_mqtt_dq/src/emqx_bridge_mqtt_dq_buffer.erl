%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_buffer).

-behaviour(gen_server).

-include("emqx_bridge_mqtt_dq.hrl").

-export([start_link/2, enqueue/3, get_pid/2, sync_metrics/1, usage/1]).

-export([init/1, handle_cast/2, handle_info/2, handle_call/3, terminate/2]).

-define(ENQUEUE_BATCH_LIMIT, 100).
%% Max batches in flight from buffer to connector. Two is enough: while the
%% connector is publishing batch N, the buffer can already prepare batch N+1.
%% More would just increase memory pressure without improving throughput,
%% because the connector serializes dispatch within each batch anyway.
-define(MAX_INFLIGHT_BATCHES, 2).
-define(CONN_RETRY_DELAY_MS, 1000).
-define(FLUSH_RETRY_DELAY_MS, 1000).
-define(MIN_DISCARD_LOG_INTERVAL, 5000).
-define(REF(BRIDGE, INDEX), {n, l, {?MODULE, BRIDGE, INDEX}}).

-type queue_item() :: #{
    topic := emqx_types:topic(),
    payload := binary(),
    qos := emqx_types:qos(),
    retain := boolean(),
    timestamp := integer(),
    properties := emqx_types:properties()
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
    Pid ! {enqueue, Item, AckAlias},
    ok.

-spec get_pid(binary(), non_neg_integer()) -> pid().
get_pid(BridgeName, Index) ->
    case gproc:where(?REF(BridgeName, Index)) of
        Pid when is_pid(Pid) -> Pid;
        _ -> erlang:error(badarg)
    end.

-spec sync_metrics(pid()) -> ok.
sync_metrics(Pid) ->
    gen_server:call(Pid, sync_metrics).

-spec usage(pid()) -> #{buffered := non_neg_integer(), buffered_bytes := non_neg_integer()}.
usage(Pid) ->
    gen_server:call(Pid, usage).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init({BridgeConfig, Index}) ->
    process_flag(trap_exit, true),
    #{
        name := BridgeName,
        queue_base_dir := QueueBaseDir,
        seg_bytes := SegBytes,
        max_total_bytes := MaxTotalBytes,
        pool_size := PoolSize,
        max_inflight := MaxInflight
    } = BridgeConfig,
    QueueDir = queue_dir(QueueBaseDir, BridgeName, Index),
    ok = filelib:ensure_path(QueueDir),
    Q = replayq:open(#{
        dir => QueueDir,
        seg_bytes => SegBytes,
        max_total_bytes => MaxTotalBytes,
        marshaller => fun marshaller/1,
        sizer => fun sizer/1,
        mem_queue_module => replayq_mem_ets_exclusive
    }),
    true = gproc:reg(?REF(BridgeName, Index)),
    ConnIndex = Index rem PoolSize,
    State = #{
        bridge_name => BridgeName,
        index => Index,
        queue => Q,
        conn_index => ConnIndex,
        conn_pid => undefined,
        conn_mon => undefined,
        %% [{Ref, AckRef, Items}] — batches sent to connector, max MAX_INFLIGHT_BATCHES
        inflight => [],
        %% [{AckRef, Items}] — batches to resend, drained before queue; oldest first
        pending => [],
        %% Batch pop size = max_inflight: intentionally coupled so each batch
        %% exactly fills the connector's send window. Since replayq loads full
        %% segments into ETS, the pop is a memory read — no disk I/O benefit
        %% from larger batches. This keeps one config knob for the entire
        %% buffer → connector → emqtt pipeline.
        flush_batch_size => MaxInflight,
        flush_scheduled => false
    },
    %% Seed enqueue counter with messages already on disk so the
    %% enqueue = dequeue = publish + drop invariant holds after restart.
    Persisted = replayq:count(Q),
    case Persisted > 0 of
        true -> ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_enqueue(BridgeName, Persisted);
        false -> ok
    end,
    State1 = update_metrics(State),
    {ok, try_acquire_conn(State1)}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(sync_metrics, _From, State) ->
    {reply, ok, update_metrics(State)};
handle_call(usage, _From, #{queue := Q} = State) ->
    {reply, #{buffered => replayq:count(Q), buffered_bytes => replayq:bytes(Q)}, State};
handle_call(_Request, _From, State) ->
    {reply, {error, unknown_call}, State}.

handle_info({enqueue, Item, AckAlias}, #{queue := Q, bridge_name := BridgeName} = State) ->
    {Items, CallerAcks} = collect_enqueue([Item], [AckAlias], ?ENQUEUE_BATCH_LIMIT - 1),
    Q1 = replayq:append(Q, Items),
    Q2 = handle_overflow(Q1, State),
    ok = ack_enqueue_batch(CallerAcks),
    ?tp(mqtt_dq_buffer_enqueued, #{bridge => BridgeName, count => length(Items)}),
    State1 = update_metrics(State#{queue := Q2}),
    {noreply, maybe_flush(State1)};
handle_info({batch_ack, Ref, Result}, State) ->
    {noreply, handle_batch_ack(Ref, Result, State)};
handle_info(flush, State) ->
    {noreply, do_flush(State#{flush_scheduled := false})};
handle_info(retry_conn, State) ->
    {noreply, try_acquire_conn(State)};
handle_info(
    {'DOWN', Mon, process, Pid, _Reason},
    #{conn_mon := Mon, conn_pid := Pid} = State
) ->
    State1 = salvage_inflight(State),
    State2 = State1#{conn_pid := undefined, conn_mon := undefined},
    {noreply, schedule_retry_conn(State2)};
handle_info({'DOWN', _Mon, process, _Pid, _Reason}, State) ->
    {noreply, State};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, #{queue := Q, bridge_name := BridgeName, index := Index}) ->
    ok = emqx_bridge_mqtt_dq_metrics:set_buffered(BridgeName, Index, 0),
    ok = emqx_bridge_mqtt_dq_metrics:set_buffered_bytes(BridgeName, Index, 0),
    _ = replayq:close(Q),
    ok.

%%--------------------------------------------------------------------
%% Internal: connector affinity
%%--------------------------------------------------------------------

try_acquire_conn(#{bridge_name := BridgeName, conn_index := ConnIndex} = State) ->
    safe_demonitor(State),
    case find_connector(BridgeName, ConnIndex) of
        {ok, Pid} ->
            Mon = erlang:monitor(process, Pid),
            maybe_flush(State#{conn_pid := Pid, conn_mon := Mon});
        error ->
            schedule_retry_conn(State#{conn_pid := undefined, conn_mon := undefined})
    end.

find_connector(BridgeName, ConnIndex) ->
    try
        SupName = emqx_bridge_mqtt_dq_conn_sup:sup_pid(BridgeName),
        ChildId = {conn, BridgeName, ConnIndex},
        Children = supervisor:which_children(SupName),
        case lists:keyfind(ChildId, 1, Children) of
            {_, Pid, _, _} when is_pid(Pid) -> {ok, Pid};
            _ -> error
        end
    catch
        _:_ -> error
    end.

safe_demonitor(#{conn_mon := undefined}) -> ok;
safe_demonitor(#{conn_mon := Mon}) -> erlang:demonitor(Mon, [flush]).

schedule_retry_conn(State) ->
    erlang:send_after(?CONN_RETRY_DELAY_MS, self(), retry_conn),
    State.

%%--------------------------------------------------------------------
%% Internal: flush / inflight
%%--------------------------------------------------------------------

maybe_flush(#{flush_scheduled := true} = State) ->
    State;
maybe_flush(#{conn_pid := undefined} = State) ->
    State;
maybe_flush(#{inflight := Inflight} = State) when length(Inflight) >= ?MAX_INFLIGHT_BATCHES ->
    State;
maybe_flush(#{pending := [_ | _]} = State) ->
    self() ! flush,
    State#{flush_scheduled := true};
maybe_flush(#{queue := Q} = State) ->
    case replayq:is_empty(Q) of
        true ->
            State;
        false ->
            self() ! flush,
            State#{flush_scheduled := true}
    end.

do_flush(#{conn_pid := undefined} = State) ->
    State;
do_flush(#{inflight := Inflight} = State) when length(Inflight) >= ?MAX_INFLIGHT_BATCHES ->
    %% Inflight is typically just 2, so length/1 in guard is ok
    State;
do_flush(#{pending := [_ | _]} = State) ->
    send_pending(State);
do_flush(#{queue := Q} = State) ->
    case replayq:is_empty(Q) of
        true -> State;
        false -> send_from_queue(State)
    end.

send_pending(
    #{
        pending := [{AckRef, Items} | Rest],
        conn_pid := ConnPid,
        inflight := Inflight
    } = State
) ->
    Ref = make_ref(),
    ConnPid ! {publish_batch, Items, self(), Ref},
    State1 = update_metrics(State#{
        pending := Rest,
        inflight := [{Ref, AckRef, Items} | Inflight]
    }),
    maybe_flush(State1).

send_from_queue(
    #{queue := Q, conn_pid := ConnPid, inflight := Inflight, flush_batch_size := BatchSize} = State
) ->
    {Q1, AckRef, Items} = replayq:pop(Q, #{count_limit => BatchSize}),
    Ref = make_ref(),
    ConnPid ! {publish_batch, Items, self(), Ref},
    State1 = update_metrics(State#{
        queue := Q1,
        inflight := [{Ref, AckRef, Items} | Inflight]
    }),
    maybe_flush(State1).

handle_batch_ack(Ref, ok, #{inflight := Inflight, queue := Q} = State) ->
    case lists:keytake(Ref, 1, Inflight) of
        {value, {_, AckRef, Items}, Rest} ->
            ok = replayq:ack(Q, AckRef),
            #{bridge_name := BridgeName} = State,
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_dequeue(BridgeName, length(Items)),
            maybe_flush(update_metrics(State#{inflight := Rest}));
        false ->
            State
    end;
handle_batch_ack(Ref, {error, _NumOk}, #{inflight := Inflight} = State) ->
    case lists:keymember(Ref, 1, Inflight) of
        true ->
            %% Network error — salvage ALL inflight to pending in send order.
            %% The other in-flight batch shares the same broken connection,
            %% its ack (arriving later) will be a no-op since inflight is empty.
            schedule_flush_retry(salvage_inflight(State));
        false ->
            State
    end.

%% Connector DOWN — move all inflight back to pending head (preserving send order).
salvage_inflight(#{inflight := []} = State) ->
    State;
salvage_inflight(#{inflight := Inflight, pending := Pending} = State) ->
    %% inflight is newest-first; reverse to get send order
    Salvaged = [{AckRef, Items} || {_Ref, AckRef, Items} <- lists:reverse(Inflight)],
    update_metrics(State#{inflight := [], pending := Salvaged ++ Pending}).

schedule_flush_retry(State) ->
    erlang:send_after(?FLUSH_RETRY_DELAY_MS, self(), flush),
    State#{flush_scheduled := true}.

%%--------------------------------------------------------------------
%% Internal: overflow
%%--------------------------------------------------------------------

handle_overflow(Q, #{bridge_name := BridgeName, index := Index}) ->
    case replayq:overflow(Q) of
        Overflow when Overflow =< 0 ->
            Q;
        Overflow ->
            {Q1, QAckRef, Dropped} =
                replayq:pop(Q, #{bytes_limit => Overflow, count_limit => 999999999}),
            ok = replayq:ack(Q1, QAckRef),
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_dequeue(BridgeName, length(Dropped)),
            ok = emqx_bridge_mqtt_dq_metrics:incr_bridge_drop(BridgeName, length(Dropped)),
            maybe_log_discard(BridgeName, Index, length(Dropped)),
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
            ?LOG(warning, #{
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

collect_enqueue(Items, Acks, 0) ->
    {lists:reverse(Items), lists:reverse(Acks)};
collect_enqueue(Items, Acks, Remaining) ->
    receive
        {enqueue, Item, AckAlias} ->
            collect_enqueue([Item | Items], [AckAlias | Acks], Remaining - 1)
    after 0 ->
        {lists:reverse(Items), lists:reverse(Acks)}
    end.

ack_enqueue_batch(Acks) ->
    lists:foreach(fun ack_enqueue/1, Acks).

ack_enqueue(no_ack) -> ok;
ack_enqueue(Alias) -> Alias ! {Alias, ok}.

queue_dir(BaseDir, BridgeName, Index) ->
    filename:join([binary_to_list(BaseDir), binary_to_list(BridgeName), integer_to_list(Index)]).

marshaller(Bin) when is_binary(Bin) -> binary_to_term(Bin);
marshaller(Term) -> term_to_binary(Term).

sizer(Item) -> erlang:external_size(Item).

update_metrics(#{bridge_name := BridgeName, index := Index, queue := Q} = State) ->
    ok = emqx_bridge_mqtt_dq_metrics:set_buffered(BridgeName, Index, replayq:count(Q)),
    ok = emqx_bridge_mqtt_dq_metrics:set_buffered_bytes(BridgeName, Index, replayq:bytes(Q)),
    State.
