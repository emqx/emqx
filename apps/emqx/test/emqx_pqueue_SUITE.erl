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
