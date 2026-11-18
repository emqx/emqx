%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_mnesia_perf).

-compile(export_all).
-compile(nowarn_export_all).

-define(STATS, emqx_bcast_mnesia_perf_stats).
-define(OWNER, emqx_bcast_mnesia_perf_owner).

start() ->
    stop(),
    Parent = self(),
    Owner = spawn(fun() -> owner(Parent) end),
    receive
        {ready, Owner} -> ok
    after 5000 ->
        exit(Owner, kill),
        {error, owner_timeout}
    end.

stop() ->
    case whereis(?OWNER) of
        undefined ->
            empty_stats();
        Owner ->
            Ref = make_ref(),
            Owner ! {snapshot, self(), Ref},
            receive
                {stats, Ref, Stats} -> Stats
            after 30_000 ->
                exit(Owner, kill),
                empty_stats()
            end
    end.

report(Label, Stats) ->
    ct:pal(
        "MNESIA_PERF_SUMMARY ~s calls=~p in_bytes=~p out_bytes=~p txlog_writes=~p",
        [
            Label,
            maps:get(calls, Stats),
            maps:get(in_bytes, Stats),
            maps:get(out_bytes, Stats),
            maps:get(transaction_log_writes, Stats)
        ]
    ),
    ct:pal("MNESIA_PERF_HOT_PATHS ~s hot_paths=~0p", [
        Label, maps:get(hot_paths, Stats)
    ]),
    ct:pal("MNESIA_PERF ~s stats=~0p", [Label, Stats]).

owner(Parent) ->
    ?STATS = ets:new(?STATS, [
        named_table,
        public,
        set,
        {write_concurrency, true},
        {read_concurrency, true}
    ]),
    true = register(?OWNER, self()),
    LogWrites = mnesia:system_info(transaction_log_writes),
    enable_trace(),
    Parent ! {ready, self()},
    loop(#{stacks => #{}, log_writes => LogWrites}).

enable_trace() ->
    Pattern = [{'_', [], [{return_trace}]}],
    lists:foreach(
        fun(MFA) -> _ = erlang:trace_pattern(MFA, Pattern, [local]) end,
        traced_functions()
    ),
    HotPathPattern = [{'_', [], [true]}],
    lists:foreach(
        fun(MFA) -> _ = erlang:trace_pattern(MFA, HotPathPattern, [local]) end,
        traced_hot_paths()
    ),
    _ = erlang:trace(all, true, [call, {tracer, self()}]),
    ok.

disable_trace() ->
    _ = erlang:trace(all, false, [call]),
    lists:foreach(
        fun(MFA) -> _ = erlang:trace_pattern(MFA, false, [local]) end,
        traced_functions()
    ),
    lists:foreach(
        fun(MFA) -> _ = erlang:trace_pattern(MFA, false, [local]) end,
        traced_hot_paths()
    ),
    ok.

traced_functions() ->
    [
        {mnesia, dirty_read, 2},
        {mnesia, dirty_write, 1},
        {mnesia, dirty_delete, 1},
        {mnesia, dirty_delete_object, 1},
        {mnesia, dirty_update_counter, 3},
        {mnesia, dirty_index_read, 3},
        %% Plugin calls to dirty_match_object/1 are counted at the /2 call
        %% that the Mnesia /1 wrapper makes, avoiding a duplicate.
        {mnesia, dirty_match_object, 2},
        {mnesia, dirty_select, 2},
        {mnesia, read, 3},
        {mnesia, wread, 1},
        {mnesia, write, 1},
        {mnesia, delete, 1},
        {mnesia, delete_object, 1},
        {mnesia, transaction, 2},
        {mnesia, lock, 2}
    ].

%% These are the BatchPub/Ack bookkeeping functions that contain the Mnesia
%% operations under comparison. Missing functions in an older release are
%% harmless: trace_pattern/3 simply reports no module or function.
traced_hot_paths() ->
    [
        {emqx_bcast_index_owner, create_delivery, 6},
        {emqx_bcast_index_owner, ack_one_index, 2},
        {emqx_bcast_index_owner, report_part_accounted, 4},
        {emqx_bcast_index_owner, report_part_accounted, 5},
        {emqx_bcast_index_owner, report_part_done, 3},
        {emqx_bcast_index_owner, report_part_done, 4},
        {emqx_bcast_index_owner, delivery_row_gone, 1},
        {emqx_bcast_index_owner, flush_partial_ack_marker, 6},
        {emqx_bcast_index_owner, flush_partial_ack_markers, 1},
        {emqx_bcast_index_owner, prune_ended_parts, 1},
        {emqx_bcast_storage, promote_entry_tx, 1},
        {emqx_bcast_storage, get_device_deliveries, 1}
    ].

loop(State) ->
    receive
        {trace, Pid, call, {mnesia, Fun, Args}} ->
            loop(on_call(Pid, Fun, Args, State));
        {trace, _Pid, call, MFA} ->
            loop(on_hot_path(MFA, State));
        {trace, Pid, return_from, {mnesia, Fun, Arity}, Result} ->
            loop(on_return(Pid, Fun, Arity, Result, State));
        {trace, Pid, exception_from, {mnesia, Fun, Arity}, _Reason} ->
            loop(on_exception(Pid, Fun, Arity, State));
        {trace, _Pid, _Tag, _Info} ->
            loop(State);
        {snapshot, From, Ref} ->
            _ = erlang:trace(all, false, [call]),
            TraceRef = erlang:trace_delivered(all),
            loop(State#{snapshot => {From, Ref, TraceRef}});
        {trace_delivered, all, TraceRef} ->
            finish_snapshot(TraceRef, State)
    end.

trace_hot_path(MFA) ->
    Key = {hot_path, MFA, calls},
    _ = ets:update_counter(?STATS, Key, 1, {Key, 0}),
    ok.

on_hot_path(MFA, State) ->
    trace_hot_path(hot_path_mfa(MFA)),
    State.

hot_path_mfa({Module, Function, Args}) when is_list(Args) ->
    {Module, Function, length(Args)}.

finish_snapshot(TraceRef, State = #{snapshot := {From, Ref, TraceRef}}) ->
    disable_trace(),
    LogWrites = mnesia:system_info(transaction_log_writes),
    Stats = aggregate(maps:get(log_writes, State), LogWrites, ets:tab2list(?STATS)),
    From ! {stats, Ref, Stats},
    unregister(?OWNER),
    ok.

on_call(Pid, Fun, Args, State = #{stacks := Stacks}) ->
    on_call(Pid, classify(Fun, Args), Fun, Args, State, Stacks).

on_call(Pid, {transaction, transaction}, _Fun, _Args, State, Stacks) ->
    bump(transaction, transaction, calls, 1),
    State#{stacks => push_call(Pid, false, undefined, undefined, 0, Stacks)};
on_call(Pid, {lock, lock}, _Fun, _Args, State, Stacks) ->
    bump(lock, lock, calls, 1),
    State#{stacks => push_call(Pid, false, undefined, undefined, 0, Stacks)};
on_call(Pid, {Op, Tab}, _Fun, Args, State, Stacks) ->
    case is_bcast_table(Tab) of
        false ->
            State#{stacks => push_call(Pid, false, undefined, undefined, 0, Stacks)};
        true ->
            InBytes = erlang:external_size(Args),
            bump(Op, Tab, calls, 1),
            bump(Op, Tab, in_bytes, InBytes),
            State#{stacks => push_call(Pid, true, Op, Tab, InBytes, Stacks)}
    end.

push_call(Pid, Track, Op, Tab, InBytes, Stacks) ->
    Frame = {Track, Op, Tab, InBytes},
    maps:update_with(Pid, fun(Stack) -> [Frame | Stack] end, [Frame], Stacks).

on_return(Pid, _Fun, _Arity, Result, State = #{stacks := Stacks}) ->
    case pop_call(Pid, Stacks) of
        {[{true, Op, Tab, _InBytes} | _], Stacks1} ->
            bump(Op, Tab, out_bytes, erlang:external_size(Result)),
            State#{stacks => Stacks1};
        {[_ | _], Stacks1} ->
            State#{stacks => Stacks1};
        {[], Stacks1} ->
            State#{stacks => Stacks1}
    end.

on_exception(Pid, _Fun, _Arity, State = #{stacks := Stacks}) ->
    case pop_call(Pid, Stacks) of
        {[{true, Op, Tab, _InBytes} | _], Stacks1} ->
            bump(Op, Tab, errors, 1),
            State#{stacks => Stacks1};
        {[_ | _], Stacks1} ->
            State#{stacks => Stacks1};
        {[], Stacks1} ->
            State#{stacks => Stacks1}
    end.

pop_call(Pid, Stacks) ->
    case maps:get(Pid, Stacks, []) of
        [] ->
            {[], Stacks};
        [Frame | Rest] ->
            case Rest of
                [] -> {[Frame], maps:remove(Pid, Stacks)};
                _ -> {[Frame], maps:put(Pid, Rest, Stacks)}
            end
    end.

classify(dirty_read, [Tab, _Key]) ->
    {dirty_read, Tab};
classify(dirty_write, [Record]) ->
    {dirty_write, record_table(Record)};
classify(dirty_delete, [Key]) ->
    {dirty_delete, key_table(Key)};
classify(dirty_delete_object, [Record]) ->
    {dirty_delete_object, record_table(Record)};
classify(dirty_update_counter, [Tab, _Key, _Incr]) ->
    {dirty_update_counter, Tab};
classify(dirty_index_read, [Tab, _Key, _Pos]) ->
    {dirty_index_read, Tab};
classify(dirty_match_object, [Tab, _Pattern]) ->
    {dirty_match_object, Tab};
classify(dirty_select, [Tab, _MatchSpec]) ->
    {dirty_select, Tab};
classify(read, [Tab, _Key, _Lock]) ->
    {read, Tab};
classify(wread, [Key]) ->
    {wread, key_table(Key)};
classify(write, [Record]) ->
    {write, record_table(Record)};
classify(delete, [Key]) ->
    {delete, key_table(Key)};
classify(delete_object, [Record]) ->
    {delete_object, record_table(Record)};
classify(transaction, [_Fun, _Retries]) ->
    {transaction, transaction};
classify(lock, [_Lock, _LockKind]) ->
    {lock, lock};
classify(Fun, _Args) ->
    {Fun, undefined}.

bump(Op, Tab, Metric, Increment) ->
    Key = {Op, Tab, Metric},
    _ = ets:update_counter(?STATS, Key, Increment, {Key, 0}),
    ok.

record_table(Record) when is_tuple(Record), tuple_size(Record) > 0 ->
    element(1, Record);
record_table(_) ->
    undefined.

key_table({Tab, _Key}) when is_atom(Tab) ->
    Tab;
key_table({Tab, _Key, _Value}) when is_atom(Tab) ->
    Tab;
key_table(Tab) when is_atom(Tab) ->
    Tab;
key_table(_) ->
    undefined.

is_bcast_table(Tab) when is_atom(Tab) ->
    lists:prefix("bcast_", atom_to_list(Tab));
is_bcast_table(_) ->
    false.

aggregate(LogWrites0, LogWrites1, Rows) ->
    Empty = #{
        calls => 0,
        in_bytes => 0,
        out_bytes => 0,
        errors => 0,
        ops => #{},
        hot_paths => #{}
    },
    Totals = lists:foldl(
        fun
            ({{hot_path, MFA, calls}, Value}, Acc) ->
                add_hot_path(MFA, Value, Acc);
            ({{Op, Tab, Metric}, Value}, Acc) ->
                aggregate_row(Op, Tab, Metric, Value, Acc)
        end,
        Empty,
        Rows
    ),
    Totals#{transaction_log_writes => LogWrites1 - LogWrites0}.

add_hot_path(MFA, Value, Acc = #{hot_paths := HotPaths}) ->
    Acc#{hot_paths => maps:put(MFA, Value, HotPaths)}.

aggregate_row(Op, Tab, Metric, Value, Acc) ->
    Op0 = maps:get(Op, maps:get(ops, Acc), #{
        tables => #{}, calls => 0, in_bytes => 0, out_bytes => 0, errors => 0
    }),
    Tab0 = maps:get(
        Tab, maps:get(tables, Op0), #{calls => 0, in_bytes => 0, out_bytes => 0, errors => 0}
    ),
    Op1 = Op0#{
        Metric => maps:get(Metric, Op0, 0) + Value,
        tables => maps:put(Tab, add_metric(Tab0, Metric, Value), maps:get(tables, Op0))
    },
    Acc#{
        ops => maps:put(Op, Op1, maps:get(ops, Acc)),
        Metric => maps:get(Metric, Acc, 0) + Value
    }.

add_metric(Map, Metric, Value) ->
    Map#{Metric => maps:get(Metric, Map, 0) + Value}.

empty_stats() ->
    #{
        calls => 0,
        in_bytes => 0,
        out_bytes => 0,
        errors => 0,
        ops => #{},
        hot_paths => #{},
        transaction_log_writes => 0
    }.
