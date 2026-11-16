%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_part_reporter).

-behaviour(gen_server).

-export([start_link/1, shard_count/0, shard_name/1, part_accounted/3]).
-export([init/1, handle_call/3, handle_cast/2, handle_info/2, terminate/2, code_change/3]).

-include("emqx_bcast.hrl").
-include_lib("emqx/include/logger.hrl").

-define(CHECK_MS, 20).
-define(SYNC_TIMEOUT_MS, 5000).

-record(state, {
    pending = #{},
    retry = #{},
    timer = undefined
}).

start_link(Shard) ->
    gen_server:start_link({local, shard_name(Shard)}, ?MODULE, [], []).

shard_count() ->
    emqx_bcast_index_owner:shard_count().

shard_name(Shard) ->
    list_to_atom("emqx_bcast_part_reporter_" ++ integer_to_list(Shard)).

%% Called by the source index shard after it has durably written its
%% per-part accounting row. Route by delivery so every part of one delivery
%% is projected by one process, while the transaction is outside the ACK
%% index-shard process.
part_accounted(SourceNode, SourceShard, Did) ->
    DidShard = emqx_bcast_index_owner:shard_of_delivery(Did),
    TargetNode = emqx_bcast_index_owner:shard_owner(DidShard),
    ReporterShard = erlang:phash2(Did, shard_count()),
    Request = {part_accounted, SourceNode, SourceShard, Did},
    case TargetNode of
        Node when Node =:= node() ->
            gen_server:call(shard_name(ReporterShard), Request, ?SYNC_TIMEOUT_MS);
        _ ->
            gen_server:call(
                {shard_name(ReporterShard), TargetNode},
                Request,
                ?SYNC_TIMEOUT_MS
            )
    end.

init([]) ->
    {ok, #state{}}.

handle_call({part_accounted, SourceNode, SourceShard, Did}, _From, State) ->
    State1 = note_account(SourceNode, SourceShard, Did, State),
    {reply, {ok, self()}, check_delivery(Did, State1)};
handle_call(_Request, _From, State) ->
    {reply, {error, bad_request}, State}.

handle_cast(_Msg, State) ->
    {noreply, State}.

handle_info(check_pending, State) ->
    State1 = State#state{timer = undefined},
    {noreply, check_pending(State1)};
handle_info(_Info, State) ->
    {noreply, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

note_account(SourceNode, SourceShard, Did, State = #state{pending = Pending0}) ->
    Sources0 = maps:get(Did, Pending0, #{}),
    Sources = maps:put({SourceNode, SourceShard}, true, Sources0),
    State#state{pending = maps:put(Did, Sources, Pending0)}.

arm_check(State = #state{timer = undefined}) ->
    State#state{timer = erlang:send_after(?CHECK_MS, self(), check_pending)};
arm_check(State) ->
    State.

check_pending(State = #state{retry = Retry}) ->
    {Kept, State1} = maps:fold(
        fun(Did, true, {Acc, St}) ->
            Sources = maps:get(Did, St#state.pending),
            case settle_delivery(Did, Sources, St) of
                {done, St1} ->
                    {Acc, drop_pending(Did, St1)};
                {waiting, St1} ->
                    {Acc, St1};
                {retry, St1} ->
                    {maps:put(Did, true, Acc), St1}
            end
        end,
        {#{}, State},
        Retry
    ),
    State2 = State1#state{retry = Kept},
    case map_size(Kept) of
        0 -> State2;
        _ -> arm_check(State2)
    end.

check_delivery(Did, State = #state{pending = Pending}) ->
    case maps:find(Did, Pending) of
        error ->
            State;
        {ok, Sources} ->
            case settle_delivery(Did, Sources, State) of
                {done, State1} ->
                    drop_pending(Did, State1);
                {waiting, State1} ->
                    clear_retry(Did, State1);
                {retry, State1} ->
                    arm_retry(Did, State1)
            end
    end.

settle_delivery(Did, Sources, State) ->
    case delivery_target(Did) of
        unavailable ->
            {retry, State};
        gone ->
            confirm(Did, Sources),
            {done, State};
        {ok, Target} ->
            case accounted_ack_count(Did) of
                unavailable ->
                    {retry, State};
                {ok, Completed} ->
                    Remaining = max(0, Target - Completed),
                    case sync_counter(Did, Remaining) of
                        error ->
                            {retry, State};
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
                                    {retry, State}
                            end;
                        ok ->
                            {waiting, State}
                    end
            end
    end.

drop_pending(Did, State = #state{pending = Pending}) ->
    clear_retry(Did, State#state{pending = maps:remove(Did, Pending)}).

clear_retry(Did, State = #state{retry = Retry}) ->
    case maps:remove(Did, Retry) of
        Retry1 when map_size(Retry1) =:= 0 ->
            case State#state.timer of
                undefined ->
                    State#state{retry = Retry1};
                Timer ->
                    erlang:cancel_timer(Timer),
                    State#state{retry = Retry1, timer = undefined}
            end;
        Retry1 ->
            State#state{retry = Retry1}
    end.

arm_retry(Did, State = #state{retry = Retry}) ->
    arm_check(State#state{retry = maps:put(Did, true, Retry)}).

delivery_target(Did) ->
    try mnesia:dirty_read(?TAB_MSG_META, Did) of
        [#bcast_msg_meta{counter = Target}] ->
            {ok, Target};
        [] ->
            case mnesia:dirty_read(?TAB_MSG_REC, Did) of
                [#bcast_msg{target_ack_count = Target}] -> {ok, Target};
                [] -> gone
            end
    catch
        _:_ -> unavailable
    end.

accounted_ack_count(Did) ->
    try ets:lookup(?TAB_MSG_PART_ACCOUNTED, Did) of
        Accounts ->
            Completed = lists:sum([
                Acked
             || #bcast_msg_part_accounted{shard = Shard, acked = Acked} <- Accounts,
                is_integer(Shard)
            ]),
            {ok, Completed}
    catch
        _:_ -> unavailable
    end.

sync_counter(Did, Remaining) ->
    try
        case ets:member(?TAB_MSG_META, Did) of
            false ->
                _ = mnesia:dirty_delete({?TAB_MSG_META_CNT, Did}),
                ok;
            true ->
                case mnesia:dirty_read(?TAB_MSG_META_CNT, Did) of
                    [#bcast_msg_meta_counter{counter = Remaining}] ->
                        ok;
                    _ ->
                        ok = mnesia:dirty_write(#bcast_msg_meta_counter{
                            delivery_id = Did,
                            counter = Remaining
                        }),
                        ok
                end
        end
    catch
        _:_ -> error
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
