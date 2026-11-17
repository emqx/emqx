%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_part_reporter).

-behaviour(gen_server).

-export([start_link/1, shard_count/0, shard_name/1, part_accounted/3, forget_delivery/1]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(REPLICATION_RETRY_MS, 20).
-define(MAX_SOURCE_RETRY_MS, 5000).
-define(MISSING_RECHECK_MS, 1000).
-define(PENDING_SWEEP_MS, 5000).
-define(PENDING_SWEEP_BUDGET, 1000).

-record(state, {
    pending = #{},
    retry = #{},
    timer = undefined,
    sweep_ref = undefined,
    sweep_queue = undefined,
    sweep_queued = #{},
    source_misses = #{}
}).

start_link(Shard) ->
    gen_server:start_link({local, shard_name(Shard)}, ?MODULE, [], []).

shard_count() ->
    emqx_bcast_index_owner:shard_count().

shard_name(Shard) ->
    list_to_atom("emqx_bcast_part_reporter_" ++ integer_to_list(Shard)).

%% Called by the source index shard after it has durably written its per-part
%% accounting row. This is intentionally asynchronous: a slow or
%% cross-replica reporter must not block the source shard's ACK, claim, and
%% append mailbox.
part_accounted(SourceNode, SourceShard, Did) ->
    Reporter = reporter_target(Did),
    Request = {part_accounted, SourceNode, SourceShard, Did},
    gen_server:cast(Reporter, Request),
    {ok, Reporter}.

forget_delivery(Did) ->
    gen_server:cast(reporter_target(Did), {forget_delivery, Did}).

reporter_target(Did) ->
    DidShard = emqx_bcast_index_owner:shard_of_delivery(Did),
    {shard_name(erlang:phash2(Did, shard_count())), emqx_bcast_index_owner:shard_owner(DidShard)}.

init([]) ->
    {ok, schedule_pending_sweep(#state{sweep_queue = queue:new()})}.

handle_cast({part_accounted, SourceNode, SourceShard, Did}, State) ->
    State1 = note_account(SourceNode, SourceShard, Did, State),
    acknowledge_source(SourceNode, SourceShard, Did, self()),
    {noreply, arm_retry(Did, ?REPLICATION_RETRY_MS, State1)};
handle_cast({forget_delivery, Did}, State) ->
    {noreply, forget_pending(Did, State)};
handle_cast(_Msg, State) ->
    {noreply, State}.

handle_call(_Request, _From, State) ->
    {reply, {error, unsupported}, State}.

handle_info(check_pending, State) ->
    State1 = State#state{timer = undefined},
    {noreply, check_pending(State1)};
handle_info(sweep_pending, State) ->
    State1 = State#state{sweep_ref = undefined},
    {noreply, schedule_pending_sweep(sweep_pending(State1))};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

note_account(SourceNode, SourceShard, Did, State = #state{pending = Pending0}) ->
    IsNew = not maps:is_key(Did, Pending0),
    Sources0 = maps:get(Did, Pending0, #{}),
    Sources = maps:put({SourceNode, SourceShard}, true, Sources0),
    State1 = State#state{pending = maps:put(Did, Sources, Pending0)},
    State2 = State1#state{
        source_misses = maps:remove(Did, State1#state.source_misses)
    },
    case IsNew of
        true -> enqueue_pending_sweep(Did, State2);
        false -> State2
    end.

enqueue_pending_sweep(Did, State = #state{sweep_queued = Queued}) ->
    case maps:is_key(Did, Queued) of
        true ->
            State;
        false ->
            State#state{
                sweep_queue = queue:in(Did, State#state.sweep_queue),
                sweep_queued = maps:put(Did, true, Queued)
            }
    end.

check_pending(State = #state{retry = Retry}) ->
    Now = erlang:monotonic_time(millisecond),
    {Due, Later} = maps:fold(
        fun(Did, DueAt, {DueAcc, LaterAcc}) ->
            case DueAt =< Now of
                true -> {maps:put(Did, DueAt, DueAcc), LaterAcc};
                false -> {DueAcc, maps:put(Did, DueAt, LaterAcc)}
            end
        end,
        {#{}, #{}},
        Retry
    ),
    {Retry1, State1} = maps:fold(
        fun(Did, _DueAt, {RetryAcc, St}) ->
            case maps:find(Did, St#state.pending) of
                error ->
                    {RetryAcc, St};
                {ok, Sources} ->
                    case settle_delivery(Did, Sources, St) of
                        {done, St1} ->
                            {RetryAcc, drop_pending(Did, St1)};
                        {waiting, St1} ->
                            {RetryAcc, St1};
                        {retry, Delay, St1} ->
                            DueAt = erlang:monotonic_time(millisecond) + Delay,
                            {maps:put(Did, DueAt, RetryAcc), St1}
                    end
            end
        end,
        {Later, State},
        Due
    ),
    State2 = State1#state{retry = Retry1},
    rearm_check(State2).

settle_delivery(Did, Sources, State) ->
    case emqx_bcast_replica_read:delivery_target(Did) of
        unavailable ->
            {retry, ?MISSING_RECHECK_MS, State};
        gone ->
            confirm(Did, Sources),
            {done, State};
        {ok, Target} ->
            case accounted_ack_count(Did) of
                unavailable ->
                    {retry, ?REPLICATION_RETRY_MS, State};
                {ok, Completed, AccountedShards} ->
                    case missing_source_accounts(Sources, AccountedShards) of
                        true ->
                            retry_missing_source_accounts(Did, State);
                        false ->
                            settle_accounted_delivery(
                                Did,
                                Sources,
                                Target,
                                Completed,
                                clear_source_misses(Did, State)
                            )
                    end
            end
    end.

settle_accounted_delivery(Did, _Sources, Target, Completed, State) when Completed > Target ->
    ?SLOG(error, #{
        msg => "bcast_part_accounting_exceeds_target",
        delivery_id => Did,
        target => Target,
        completed => Completed
    }),
    {retry, ?MISSING_RECHECK_MS, State};
settle_accounted_delivery(Did, Sources, Target, Completed, State) ->
    Remaining = Target - Completed,
    case sync_counter(Did, Remaining) of
        error ->
            {retry, ?REPLICATION_RETRY_MS, State};
        ok when Remaining =:= 0 ->
            case complete_delivery(Did) of
                ok ->
                    confirm(Did, Sources),
                    {done, State};
                {error, Reason} ->
                    ?SLOG(warning, #{
                        msg => "bcast_part_completion_failed_retry",
                        delivery_id => Did,
                        reason => Reason
                    }),
                    {retry, ?MISSING_RECHECK_MS, State}
            end;
        ok ->
            {waiting, State}
    end.

missing_source_accounts(Sources, AccountedShards) ->
    lists:any(
        fun({{_SourceNode, SourceShard}, true}) ->
            not maps:is_key(SourceShard, AccountedShards)
        end,
        maps:to_list(Sources)
    ).

retry_missing_source_accounts(Did, State = #state{source_misses = Misses}) ->
    Count = maps:get(Did, Misses, 0) + 1,
    Shift = min(Count - 1, 8),
    Delay = min(?REPLICATION_RETRY_MS bsl Shift, ?MAX_SOURCE_RETRY_MS),
    {retry, Delay, State#state{source_misses = maps:put(Did, Count, Misses)}}.

clear_source_misses(Did, State = #state{source_misses = Misses}) ->
    State#state{source_misses = maps:remove(Did, Misses)}.

drop_pending(Did, State = #state{pending = Pending}) ->
    State1 = clear_source_misses(Did, State#state{pending = maps:remove(Did, Pending)}),
    clear_retry(Did, State1).

forget_pending(Did, State = #state{pending = Pending}) ->
    case maps:find(Did, Pending) of
        {ok, Sources} -> confirm(Did, Sources);
        error -> ok
    end,
    drop_pending(Did, State).

clear_retry(Did, State = #state{retry = Retry}) ->
    rearm_check(State#state{retry = maps:remove(Did, Retry)}).

arm_retry(Did, Delay, State = #state{retry = Retry}) ->
    DueAt = erlang:monotonic_time(millisecond) + Delay,
    rearm_check(State#state{retry = maps:put(Did, DueAt, Retry)}).

rearm_check(State = #state{timer = Timer, retry = Retry}) ->
    case Timer of
        undefined -> ok;
        _ -> erlang:cancel_timer(Timer)
    end,
    case maps:values(Retry) of
        [] ->
            State#state{timer = undefined};
        DueAts ->
            Delay = max(0, lists:min(DueAts) - erlang:monotonic_time(millisecond)),
            State#state{timer = erlang:send_after(Delay, self(), check_pending)}
    end.

accounted_ack_count(Did) ->
    try mnesia:dirty_read(?TAB_MSG_PART_ACCOUNTED, Did) of
        Accounts ->
            Completed = lists:sum([
                Acked
             || #bcast_msg_part_accounted{shard = Shard, acked = Acked} <- Accounts,
                is_integer(Shard)
            ]),
            AccountedShards = maps:from_keys(
                [
                    Shard
                 || #bcast_msg_part_accounted{shard = Shard} <- Accounts,
                    is_integer(Shard)
                ],
                true
            ),
            {ok, Completed, AccountedShards}
    catch
        _:_ -> unavailable
    end.

sync_counter(Did, Remaining) ->
    try
        case ets:member(?TAB_MSG_META, Did) of
            false ->
                error;
            true ->
                case mnesia:dirty_read(?TAB_MSG_META_CNT, Did) of
                    [#bcast_msg_meta_counter{counter = Remaining}] ->
                        ok;
                    _ ->
                        ok = mnesia:dirty_write(#bcast_msg_meta_counter{
                            delivery_id = Did,
                            counter = Remaining
                        }),
                        %% A concurrent delivery deletion can commit after the
                        %% metadata check above and before this write. Recheck
                        %% before reporting success; the delete transaction
                        %% removes the counter when it wins after this point.
                        case ets:member(?TAB_MSG_META, Did) of
                            true ->
                                ok;
                            false ->
                                _ = mnesia:dirty_delete(?TAB_MSG_META_CNT, Did),
                                error
                        end
                end
        end
    catch
        _:_ -> error
    end.

schedule_pending_sweep(State = #state{sweep_ref = undefined}) ->
    Ref = erlang:send_after(?PENDING_SWEEP_MS, self(), sweep_pending),
    State#state{sweep_ref = Ref};
schedule_pending_sweep(State) ->
    State.

sweep_pending(State) ->
    {Dids, Queue1} = sweep_candidates(State#state.sweep_queue, ?PENDING_SWEEP_BUDGET, []),
    State1 = lists:foldl(fun(Did, St) -> sweep_delivery(Did, St) end, State, Dids),
    SweepQueued1 = maps:without(Dids, State1#state.sweep_queued),
    State2 = State1#state{sweep_queued = SweepQueued1},
    Queue2 = lists:foldl(
        fun(Did, QueueAcc) ->
            case maps:is_key(Did, State2#state.pending) of
                true -> queue:in(Did, QueueAcc);
                false -> QueueAcc
            end
        end,
        Queue1,
        Dids
    ),
    SweepQueued2 = maps:from_keys(
        [Did || Did <- Dids, maps:is_key(Did, State2#state.pending)],
        true
    ),
    State2#state{
        sweep_queue = Queue2,
        sweep_queued = maps:merge(SweepQueued1, SweepQueued2)
    }.

sweep_candidates(Queue, 0, Acc) ->
    {Acc, Queue};
sweep_candidates(Queue, Budget, Acc) ->
    case queue:out(Queue) of
        {empty, Queue1} ->
            {Acc, Queue1};
        {{value, Did}, Queue1} ->
            sweep_candidates(Queue1, Budget - 1, [Did | Acc])
    end.

sweep_delivery(Did, State) ->
    case maps:is_key(Did, State#state.pending) of
        false ->
            State;
        true ->
            case ets:member(?TAB_MSG_REC, Did) of
                true ->
                    State;
                false ->
                    case emqx_bcast_replica_read:delivery_exists_cluster(Did) of
                        gone ->
                            Sources = maps:get(Did, State#state.pending),
                            confirm(Did, Sources),
                            drop_pending(Did, State);
                        _ ->
                            arm_retry(Did, ?MISSING_RECHECK_MS, State)
                    end
            end
    end.

acknowledge_source(SourceNode, SourceShard, Did, ReporterPid) ->
    case SourceNode =:= node() of
        true ->
            emqx_bcast_index_owner:part_report_received(SourceShard, Did, ReporterPid);
        false ->
            emqx_rpc:cast(
                SourceNode,
                emqx_bcast_index_owner,
                part_report_received,
                [SourceShard, Did, ReporterPid]
            )
    end.

complete_delivery(Did) ->
    try
        mnesia:transaction(
            fun() ->
                mnesia:delete({?TAB_MSG_ACKED, Did}),
                mnesia:delete({?TAB_MSG_PART_ACCOUNTED, Did}),
                mnesia:delete({?TAB_MSG_META, Did}),
                mnesia:delete({?TAB_MSG_META_CNT, Did}),
                mnesia:delete({?TAB_MSG_REC, Did}),
                ok
            end,
            20
        )
    of
        {atomic, ok} -> ok;
        {aborted, Reason} -> {error, Reason}
    catch
        Class:Reason:Stacktrace ->
            {error, {Class, Reason, Stacktrace}}
    end.

confirm(Did, Sources) ->
    lists:foreach(
        fun({{SourceNode, SourceShard}, true}) ->
            case SourceNode =:= node() of
                true ->
                    ok = emqx_bcast_index_owner:part_report_applied(SourceShard, Did);
                false ->
                    emqx_rpc:cast(
                        SourceNode,
                        emqx_bcast_index_owner,
                        part_report_applied,
                        [SourceShard, Did]
                    )
            end
        end,
        maps:to_list(Sources)
    ),
    ok.
