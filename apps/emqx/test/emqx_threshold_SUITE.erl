%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_threshold_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_no_triggers(_) ->
    St0 = emqx_threshold:init([]),
    St1 = emqx_threshold:run(42, 10000, St0),
    ?assert(not is_list(St1)),
    St2 = emqx_threshold:run(10, 1000, St1),
    ?assert(not is_list(St2)),
    ?assertEqual(St0, St2).

t_no_trigger_updates_current(_) ->
    St0 = emqx_threshold:init([{t1, count, 10}]),
    St1 = emqx_threshold:run(3, 1000, St0),
    ?assert(not is_list(St1)),
    St2 = emqx_threshold:run(6, 1000, St1),
    ?assert(not is_list(St2)).

t_account_trigger_accumulated(_) ->
    St0 = emqx_threshold:init([{t1, count, 10}]),
    St1 = emqx_threshold:account(3, 1000, St0),
    St2 = emqx_threshold:account(7, 1000, St1),
    [t1 | St3] = emqx_threshold:trigger(St2),
    ?assertEqual(St3, emqx_threshold:trigger(St3)).

t_trigger_resets_from_current(_) ->
    St0 = emqx_threshold:init([{t1, count, 10}]),
    St1 = emqx_threshold:run(7, 1000, St0),
    ?assert(not is_list(St1)),
    [t1 | St2] = emqx_threshold:run(7, 1000, St1),
    St3 = emqx_threshold:run(7, 1000, St2),
    ?assert(not is_list(St3)),
    [t1 | _St4] = emqx_threshold:run(3, 1000, St3).

t_multiple_triggers_per_metric(_) ->
    St0 = emqx_threshold:init([
        {c1, count, 10},
        {c2, count, 15},
        {c3, count, 20}
    ]),
    St1 = emqx_threshold:run(9, 1000, St0),
    [c1 | St2] = emqx_threshold:run(1, 1000, St1),
    [c2 | St3] = emqx_threshold:run(6, 1000, St2),
    [c3, c1 | _St4] = emqx_threshold:run(6, 1000, St3).

t_independent_metrics(_) ->
    St0 = emqx_threshold:init([
        {c1, count, 10},
        {b1, bytes, 512000}
    ]),
    St1 = emqx_threshold:run(5, 20000, St0),
    [b1 | St2] = emqx_threshold:run(3, 500000, St1),
    [c1 | _St] = emqx_threshold:run(2, 10000, St2).
