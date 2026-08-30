%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_pqueue_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-define(PQ, emqx_pqueue).

all() -> emqx_common_test_helpers:all(?MODULE).

%%--------------------------------------------------------------------
%% General
%%--------------------------------------------------------------------

t_is_empty(_) ->
    Q = ?PQ:new(),
    ?assertEqual(true, ?PQ:is_empty(Q)),
    ?assertEqual(false, ?PQ:is_empty(?PQ:in(a, Q))).

t_len(_) ->
    Q = ?PQ:new(),
    Q1 = ?PQ:in(a, Q),
    ?assertEqual(1, ?PQ:len(Q1)),
    Q2 = ?PQ:in(b, 1, default, Q1),
    ?assertEqual(2, ?PQ:len(Q2)),
    Q3 = ?PQ:in(c, 2, qos0, Q2),
    ?assertEqual(3, ?PQ:len(Q3)).

t_plen(_) ->
    Q = ?PQ:new(),
    Q1 = ?PQ:in(a, Q),
    ?assertEqual(1, ?PQ:plen(0, Q1)),
    Q2 = ?PQ:in(b, 1, default, Q1),
    Q3 = ?PQ:in(c, 1, default, Q2),
    ?assertEqual(2, ?PQ:plen(1, Q3)),
    ?assertEqual(1, ?PQ:plen(0, Q3)),
    {_, Q4} = ?PQ:out(Q3),
    {_, Q5} = ?PQ:out(Q4),
    {_, Q6} = ?PQ:out(Q5),
    ?assertEqual(0, ?PQ:plen(0, Q6)).

t_to_list(_) ->
    Q = ?PQ:new(),
    ?assertEqual([], ?PQ:to_list(Q)),
    Q1 = ?PQ:in(a, Q),
    ?assertEqual([{0, a}], ?PQ:to_list(Q1)),
    Q2 = ?PQ:in(b, 1, default, Q1),
    ?assertEqual([{1, b}, {0, a}], ?PQ:to_list(Q2)).

t_from_list(_) ->
    Q = ?PQ:from_list([{1, default, c}, {1, default, d}, {0, default, a}, {0, default, b}]),
    ?assertEqual(4, ?PQ:len(Q)),
    ?assertEqual([{1, c}, {1, d}, {0, a}, {0, b}], ?PQ:to_list(Q)).

t_in(_) ->
    Q = ?PQ:new(),
    Els = [a, b, {c, 1}, {d, 1}, {e, infinity}, {f, 2}],
    Q1 = lists:foldl(
        fun
            ({El, P}, Acc) ->
                ?PQ:in(El, P, default, Acc);
            (El, Acc) ->
                ?PQ:in(El, Acc)
        end,
        Q,
        Els
    ),
    ?assertEqual([{infinity, e}, {2, f}, {1, c}, {1, d}, {0, a}, {0, b}], ?PQ:to_list(Q1)),
    ?assertEqual(6, ?PQ:len(Q1)).

t_out_empty(_) ->
    ?assertMatch({empty, _}, ?PQ:out(?PQ:new())).

t_out(_) ->
    %% Two elements at same priority: FIFO
    {{value, a}, Q1} = ?PQ:out(?PQ:from_list([{0, default, a}, {0, qos0, b}])),
    {{value, b}, Q2} = ?PQ:out(Q1),
    ?assert(?PQ:is_empty(Q2)).

t_out_prio(_) ->
    %% Single element at non-zero priority:
    ?assertMatch({{value, a}, _}, ?PQ:out(?PQ:from_list([{1, default, a}]))),
    %% Higher priority dequeued first:
    Q1 = ?PQ:from_list([{0, default, a}, {0, default, b}, {1, default, c}]),
    {{value, c}, Q2} = ?PQ:out(Q1),
    {{value, a}, Q3} = ?PQ:out(Q2),
    ?assertEqual([{0, b}], ?PQ:to_list(Q3)).

t_out_2(_) ->
    %% out/2 on simple queue with priority 0
    ?assertMatch(
        {{value, a}, _},
        ?PQ:out(0, ?PQ:from_list([{0, default, a}, {0, default, b}]))
    ),
    %% out/2 on pqueue
    PQ = ?PQ:from_list([{1, default, a}, {0, default, b}]),
    {empty, _} = ?PQ:out(2, PQ),
    {{value, a}, _} = ?PQ:out(1, PQ),
    {{value, b}, _} = ?PQ:out(0, PQ).

t_out_2_cqueue(_) ->
    Q0 = ?PQ:from_list([{0, default, a}, {0, qos0, b}]),
    {{value, a}, Q1} = ?PQ:out(0, Q0),
    {{value, b}, Q2} = ?PQ:out(0, Q1),
    ?assert(?PQ:is_empty(Q2)),
    try ?PQ:out(1, Q0) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason -> ?assertEqual(badarg, Reason)
    end.

t_out_2_squeue(_) ->
    %% out/2 with wrong priority on simple queue should error
    SQ = ?PQ:new(),
    try ?PQ:out(1, SQ) of
        _ -> ct:fail(should_throw_error)
    catch
        error:Reason -> ?assertEqual(badarg, Reason)
    end.

t_out_p(_) ->
    {empty, _} = ?PQ:out_p(?PQ:new()),
    {{value, a, 1}, Q1} = ?PQ:out_p(?PQ:from_list([{1, default, a}, {0, default, b}])),
    ?assertEqual([{0, b}], ?PQ:to_list(Q1)).

t_filter(_) ->
    Q = ?PQ:from_list([
        {0, qos0, 1},
        {0, default, 3},
        {1, default, 2},
        {2, qos0, 4},
        {2, qos0, 10}
    ]),
    Even = ?PQ:filter(fun(V) -> V rem 2 =:= 0 end, Q),
    ?assertEqual([{2, 4}, {2, 10}, {1, 2}], ?PQ:to_list(Even)),
    ?assertEqual(3, ?PQ:len(Even)).

t_filter_empty(_) ->
    PQ = ?PQ:from_list([{1, default, a}, {2, qos0, b}]),
    Empty = ?PQ:filter(fun(_) -> false end, PQ),
    ?assert(?PQ:is_empty(Empty)),
    ?assertEqual(0, ?PQ:len(Empty)),
    ?assertEqual([], ?PQ:to_list(Empty)),
    {empty, _} = ?PQ:out_p(Empty).

t_filter_empty_cqueue(_) ->
    CQ = ?PQ:from_list([{0, default, a}, {0, qos0, b}]),
    Empty = ?PQ:filter(fun(_) -> false end, CQ),
    ?assert(?PQ:is_empty(Empty)),
    ?assertEqual(0, ?PQ:len(Empty)),
    ?assertEqual([], ?PQ:to_list(Empty)),
    {empty, _} = ?PQ:out_p(Empty).

t_highest(_) ->
    0 = ?PQ:highest(?PQ:new()),
    0 = ?PQ:highest(?PQ:from_list([{0, default, a}, {0, default, b}])),
    2 = ?PQ:highest(
        ?PQ:from_list([
            {0, default, a},
            {0, default, b},
            {1, qos0, c},
            {2, qos0, d},
            {2, default, e}
        ])
    ).

-doc "lowest/1 returns the lowest non-empty priority, the mirror of highest/1.".
t_lowest(_) ->
    0 = ?PQ:lowest(?PQ:new()),
    0 = ?PQ:lowest(?PQ:from_list([{0, default, a}, {0, default, b}])),
    0 = ?PQ:lowest(
        ?PQ:from_list([
            {0, default, a},
            {0, default, b},
            {1, qos0, c},
            {2, qos0, d},
            {2, default, e}
        ])
    ),
    %% Once the lowest lane empties out, it drops off the queue entirely.
    PQ0 = ?PQ:from_list([{1, default, a}, {2, default, b}]),
    ?assertEqual(1, ?PQ:lowest(PQ0)),
    {_, PQ1} = ?PQ:drop(1, PQ0),
    ?assertEqual(2, ?PQ:lowest(PQ1)),
    ?assertEqual(infinity, ?PQ:lowest(?PQ:from_list([{infinity, default, a}]))).

-doc "drop_lowest/1 is equivalent to drop(lowest(Q), Q).".
t_drop_lowest(_) ->
    {empty, _} = ?PQ:drop_lowest(?PQ:new()),
    %% Single-lane queue: same as an ordinary drop.
    {{value, a}, SQ1} = ?PQ:drop_lowest(?PQ:from_list([{0, default, a}, {0, default, b}])),
    ?assertEqual([{0, b}], ?PQ:to_list(SQ1)),
    %% Multi-lane queue: drops from the lowest priority, leaving higher
    %% priorities untouched, and removes the lane once it empties out.
    PQ0 = ?PQ:from_list([{2, default, a}, {1, default, b}, {1, default, c}]),
    {{value, b}, PQ1} = ?PQ:drop_lowest(PQ0),
    ?assertEqual([{2, a}, {1, c}], ?PQ:to_list(PQ1)),
    {{value, c}, PQ2} = ?PQ:drop_lowest(PQ1),
    ?assertEqual(2, ?PQ:highest(PQ2)),
    ?assertEqual(2, ?PQ:lowest(PQ2)),
    ?assertEqual([{2, a}], ?PQ:to_list(PQ2)).

t_shift_squeue(_) ->
    %% shift on simple queue is identity
    Q = ?PQ:from_list([{0, default, a}, {0, default, b}]),
    ?assertEqual(Q, ?PQ:shift(Q)).

t_shift(_) ->
    %% shift rotates priority groups
    PQ0 = ?PQ:from_list([{1, default, a}, {2, default, b}, {0, qos0, c}]),
    PQ1 = ?PQ:shift(PQ0),
    ?assertEqual(3, ?PQ:len(PQ1)),
    %% Highest priority changes after rotation
    ?assertEqual(2, ?PQ:highest(PQ0)),
    ?assertEqual(1, ?PQ:highest(PQ1)),
    ?assertEqual([{1, a}, {0, c}, {2, b}], ?PQ:to_list(PQ1)).

t_fold(_) ->
    Q = ?PQ:from_list([
        {1, default, a},
        {0, qos0, b},
        {2, default, c},
        {0, default, d}
    ]),
    ?assertEqual(
        [d, b, a, c],
        ?PQ:fold(fun(V, _P, Acc) -> [V | Acc] end, [], Q)
    ).

t_prop_queue_consistency(_) ->
    ?assert(proper:quickcheck(prop_pq_queue_consistency(), [{numtests, 200}])).

prop_pq_queue_consistency() ->
    %% TODO
    %% Hard to model `shift` precisely because the actual pqueue behavior is extremely
    %% weird, to the point of likely being broken.
    proper:forall(pq_operations_t(), fun(Operations) ->
        {_, _, ActualTrace, ModelTrace} = lists:foldl(
            fun pq_apply_step/2,
            {?PQ:new(), pq_model_new(), [], []},
            Operations
        ),
        proper:equals(lists:reverse(ActualTrace), lists:reverse(ModelTrace))
    end).

pq_apply_step(Op, {Q0, M0, ActualTrace, ModelTrace}) ->
    {ActualResult, Q} = pq_apply(Op, Q0),
    {ModelResult, M} = pq_model_apply(Op, M0),
    {Q, M, [ActualResult | ActualTrace], [ModelResult | ModelTrace]}.

pq_apply({in, P, Class, V}, Q) ->
    {ok, ?PQ:in(V, P, Class, Q)};
pq_apply(out, Q) ->
    ?PQ:out(Q);
pq_apply(out_p, Q) ->
    ?PQ:out_p(Q);
pq_apply({out, P}, Q) ->
    try
        ?PQ:out(P, Q)
    catch
        error:badarg ->
            {{error, badarg}, Q}
    end;
pq_apply({drop, P}, Q) ->
    try
        ?PQ:drop(P, Q)
    catch
        error:badarg ->
            {{error, badarg}, Q}
    end;
pq_apply({filter, Spec}, Q) ->
    {ok, ?PQ:filter(pq_filter_fun(Spec), Q)};
pq_apply(shift, Q) ->
    %% TODO: {ok, ?PQ:shift(Q)}.
    {ok, Q}.

pq_model_apply({in, P, Class, V}, Model) ->
    {ok, pq_model_in(P, Class, V, Model)};
pq_model_apply(out, Model) ->
    pq_model_out(Model);
pq_model_apply(out_p, Model) ->
    pq_model_out_p(Model);
pq_model_apply({out, P}, Model) ->
    try
        pq_model_assert_prio(P, Model),
        pq_model_out(P, Model)
    catch
        error:badarg ->
            {{error, badarg}, Model}
    end;
pq_model_apply({drop, P}, Model) ->
    try
        pq_model_assert_prio(P, Model),
        pq_model_drop(P, Model)
    catch
        error:badarg ->
            {{error, badarg}, Model}
    end;
pq_model_apply({filter, Spec}, Model) ->
    {ok, pq_model_filter(pq_filter_fun(Spec), Model)};
pq_model_apply(shift, Model) ->
    %% TODO: {ok, pq_model_rotate_cprio(Model)}.
    {ok, Model}.

pq_model_new() ->
    #{}.

pq_model_in(P, Class, V, Model) ->
    maps:update_with(P, fun(Items) -> Items ++ [{V, Class}] end, [{V, Class}], Model).

pq_model_out(Model) ->
    maybe
        [Prio | _] ?= pq_model_prios(Model),
        pq_model_out(Prio, Model)
    else
        _ ->
            {empty, Model}
    end.

pq_model_out_p(Model) ->
    maybe
        [Prio | _] ?= pq_model_prios(Model),
        {{value, V}, NModel} ?= pq_model_out(Prio, Model),
        {{value, V, Prio}, NModel}
    else
        _ ->
            {empty, Model}
    end.

pq_model_out(P, Model) ->
    pq_model_take(P, Model, fun pq_take_head/1).

pq_model_drop(P, Model) ->
    pq_model_take(P, Model, fun pq_take_drop/1).

pq_model_take(P, Model, TakeFun) ->
    case maps:find(P, Model) of
        error ->
            {empty, Model};
        {ok, Items0} ->
            case TakeFun(Items0) of
                {V, []} ->
                    NModel = maps:remove(P, Model);
                {V, Items} ->
                    NModel = maps:put(P, Items, Model)
            end,
            {{value, V}, NModel}
    end.

pq_take_head([{V, _Class} | Rest]) ->
    {V, Rest}.

pq_take_drop(Items) ->
    case lists:splitwith(fun({_V, Class}) -> Class =/= qos0 end, Items) of
        {_Before, []} ->
            pq_take_head(Items);
        {Before, [{V, qos0} | After]} ->
            {V, Before ++ After}
    end.

pq_model_filter(Pred, Model) ->
    maps:filtermap(
        fun(_, Items0) ->
            case [{V, Class} || {V, Class} <- Items0, Pred(V)] of
                [] -> false;
                Items -> {true, Items}
            end
        end,
        Model
    ).

pq_model_prios(Model) ->
    lists:reverse(lists:sort(maps:keys(Model))).

pq_model_assert_prio(0, _) ->
    true;
pq_model_assert_prio(_, Model) ->
    maps:without([0], Model) =/= #{} orelse error(badarg).

pq_operation_t() ->
    proper_types:frequency([
        {6, {in, pq_priority_t(), pq_class_t(), pq_value_t()}},
        {1, out},
        {1, out_p},
        {1, {out, pq_target_priority()}},
        {1, {drop, pq_target_priority()}},
        {1, {filter, proper_types:oneof([all, none, even, odd, positive])}},
        {1, shift}
    ]).

pq_operations_t() ->
    proper_types:list(pq_operation_t()).

pq_filter_fun(all) ->
    fun(_V) -> true end;
pq_filter_fun(none) ->
    fun(_V) -> false end;
pq_filter_fun(even) ->
    fun(V) -> V rem 2 =:= 0 end;
pq_filter_fun(odd) ->
    fun(V) -> V rem 2 =/= 0 end;
pq_filter_fun(positive) ->
    fun(V) -> V > 0 end.

pq_priority_t() ->
    proper_types:elements([0, 1, 2, infinity]).

pq_target_priority() ->
    proper_types:elements([0, 1, 2, 3, infinity]).

pq_class_t() ->
    proper_types:elements([default, qos0]).

pq_value_t() ->
    proper_types:range(-20, 20).

%%--------------------------------------------------------------------
%% cqueue
%%--------------------------------------------------------------------

t_cq_in_out(_) ->
    CQ0 = emqx_pqueue:cqueue_new(),
    CQ1 = cq_batch_insert(
        [
            {"a", default},
            {"b", default},
            {"c", qos0},
            {"d", qos0},
            {"e", default},
            {"f", qos0},
            {"g", default}
        ],
        CQ0
    ),
    ?assertEqual(
        ["a", "b", "c", "d", "e", "f", "g"],
        cq_drain(CQ1)
    ).

t_cq_drop(_) ->
    CQ0 = emqx_pqueue:cqueue_new(),
    CQ1 = cq_batch_insert(
        [
            {"a", default},
            {"b", default},
            {"c", qos0},
            {"d", qos0},
            {"e", default},
            {"f", qos0},
            {"g", default}
        ],
        CQ0
    ),
    {{value, "c"}, CQ2} = emqx_pqueue:cqueue_drop(CQ1),
    {{value, "d"}, CQ3} = emqx_pqueue:cqueue_drop(CQ2),
    ?assertEqual(
        ["a", "b", "e", "f", "g"],
        cq_drain(CQ3)
    ),
    {{value, "f"}, CQ4} = emqx_pqueue:cqueue_drop(CQ3),
    ?assertEqual(
        ["a", "b", "e", "g"],
        cq_drain(CQ4)
    ),
    {{value, "a"}, CQ5} = emqx_pqueue:cqueue_drop(CQ4),
    ?assertEqual(
        ["b", "e", "g"],
        cq_drain(CQ5)
    ).

t_cq_prop_queue(_) ->
    ?assert(proper:quickcheck(prop_cq_queue(), [{numtests, 200}])).

t_cq_prop_queue_consistency(_) ->
    ?assert(proper:quickcheck(prop_cq_queue_consistency(), [{numtests, 200}])).

t_cq_prop_drop(_) ->
    ?assert(proper:quickcheck(prop_cq_drop(), [{numtests, 200}])).

t_cq_edge_case1(_) ->
    Ops = [{0, qos0}, {1, default}, {2, qos0}, out, {3, qos0}, drop],
    Outcomes = [{value, 0}, {value, 2}],
    Leftovers = [1, 3],
    run_edge_case(Ops, Outcomes, Leftovers).

t_cq_edge_case2(_) ->
    Ops = [
        {0, qos0}, {1, default}, {2, default}, {3, qos0}, out, drop, drop, {4, qos0}, {5, default}
    ],
    Outcomes = [{value, 0}, {value, 3}, {value, 1}],
    Leftovers = [2, 4, 5],
    run_edge_case(Ops, Outcomes, Leftovers).

t_cq_edge_case3(_) ->
    Ops = [{0, qos0}, {1, default}, {2, qos0}],
    Leftovers = [0, 1, 2],
    run_edge_case(Ops, [], Leftovers).

prop_cq_queue() ->
    proper:forall(cq_entries(), fun(Entries) ->
        CQ = cq_batch_insert(Entries, emqx_pqueue:cqueue_new()),
        proper:equals(
            cq_drain(CQ),
            [V || {V, _Class} <- Entries]
        )
    end).

prop_cq_drop() ->
    proper:forall(cq_entries(), fun(Entries) ->
        CQ = cq_batch_insert(Entries, emqx_pqueue:cqueue_new()),
        proper:equals(
            cq_drop_drain(CQ),
            [V || {V, qos0} <- Entries] ++ [V || {V, default} <- Entries]
        )
    end).

prop_cq_queue_consistency() ->
    proper:forall(cq_operations(), fun(Operations) ->
        CQ0 = emqx_pqueue:cqueue_new(),
        {CQ, CQOutcomes} = lists:foldl(
            fun(Op, {CQ, Acc}) -> cq_apply(Op, CQ, Acc) end,
            {CQ0, []},
            Operations
        ),
        L0 = [],
        {L, ModelOutcomes} = lists:foldl(
            fun(Op, {L, Acc}) -> cq_model_apply(Op, L, Acc) end,
            {L0, []},
            Operations
        ),
        proper:conjunction([
            {outcomes, proper:equals(CQOutcomes, ModelOutcomes)},
            {leftover, proper:equals(cq_drain(CQ), [X || {X, _Class} <- L])}
        ])
    end).

cq_apply({X, Class}, CQ, Acc) ->
    NCQ = emqx_pqueue:cqueue_in(X, Class, CQ),
    {NCQ, Acc};
cq_apply(out, CQ, Acc) ->
    {Ret, NCQ} = emqx_pqueue:cqueue_out(CQ),
    {NCQ, [Ret | Acc]};
cq_apply(drop, CQ, Acc) ->
    {Ret, NCQ} = emqx_pqueue:cqueue_drop(CQ),
    {NCQ, [Ret | Acc]}.

cq_model_apply({X, Class}, L, Acc) ->
    {L ++ [{X, Class}], Acc};
cq_model_apply(out, L, Acc) ->
    case L of
        [{X, _Class} | NL] ->
            {NL, [{value, X} | Acc]};
        [] ->
            {L, [empty | Acc]}
    end;
cq_model_apply(drop, L, Acc) ->
    case lists:dropwhile(fun({_, Class}) -> Class =:= default end, L) of
        [] when L =:= [] ->
            {L, [empty | Acc]};
        [] ->
            [{X, _Class} | NL] = L,
            {NL, [{value, X} | Acc]};
        [{X, qos0} | Rest] = T ->
            NL = lists:sublist(L, length(L) - length(T)) ++ Rest,
            {NL, [{value, X} | Acc]}
    end.

cq_entries() ->
    proper_types:list(cq_entry()).

cq_entry() ->
    proper_types:tuple([
        proper_types:integer(),
        proper_types:elements([default, qos0])
    ]).

cq_operations() ->
    proper_types:list(
        proper_types:oneof([
            cq_entry(),
            cq_entry(),
            cq_entry(),
            out,
            drop
        ])
    ).

cq_batch_insert(Xs, CQ) ->
    lists:foldl(
        fun({X, Class}, Acc) -> emqx_pqueue:cqueue_in(X, Class, Acc) end,
        CQ,
        Xs
    ).

cq_drain(CQ) ->
    case emqx_pqueue:cqueue_out(CQ) of
        {{value, V}, NCQ} -> [V | cq_drain(NCQ)];
        {empty, _} -> []
    end.

cq_drop_drain(CQ) ->
    case emqx_pqueue:cqueue_drop(CQ) of
        {{value, V}, NCQ} -> [V | cq_drop_drain(NCQ)];
        {empty, _} -> []
    end.

run_edge_case(Ops, ExpectedOutcomes, ExpectedLeftovers) ->
    {CQ, Outcomes} = lists:foldl(
        fun(Op, {CQ, Acc}) ->
            cq_apply(Op, CQ, Acc)
        end,
        {emqx_pqueue:cqueue_new(), []},
        Ops
    ),
    ?assertEqual(ExpectedOutcomes, lists:reverse(Outcomes)),
    ?assertEqual(ExpectedLeftovers, cq_drain(CQ)).
