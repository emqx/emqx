%%--------------------------------------------------------------------
%% Copyright (c) 2018-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_pd_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-doc "Check cumulative counter increments and resets.".
t_update_counter(_) ->
    ?assertEqual(undefined, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(1, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(2, emqx_pd:inc_counter(bytes, 1)),
    ?assertEqual(3, emqx_pd:get_counter(bytes)),
    ?assertEqual(3, emqx_pd:reset_counter(bytes)),
    ?assertEqual(0, emqx_pd:get_counter(bytes)).

-doc "Keep pending deltas separate from ordinary counter increments.".
t_counter_delta(_) ->
    ?assertEqual(0, emqx_pd:take_counter_delta(bytes)),
    ?assertEqual(undefined, get(bytes)),
    ?assertEqual(undefined, emqx_pd:inc_counter(bytes, 10)),
    ?assertEqual(10, emqx_pd:inc_counter_with_delta(bytes, 2)),
    ?assertEqual({10, 2}, get(bytes)),
    ?assertEqual(12, emqx_pd:get_counter(bytes)),
    ?assertEqual(12, emqx_pd:inc_counter_with_delta(bytes, 3)),
    ?assertEqual(15, emqx_pd:inc_counter(bytes, 4)),
    ?assertEqual({14, 5}, get(bytes)),
    ?assertEqual(19, emqx_pd:get_counter(bytes)),
    ?assertEqual(5, emqx_pd:get_counter_delta(bytes)),
    ?assertEqual(5, emqx_pd:take_counter_delta(bytes)),
    ?assertEqual(19, get(bytes)),
    ?assertEqual(0, emqx_pd:take_counter_delta(bytes)),
    ?assertEqual(19, emqx_pd:inc_counter_with_delta(bytes, 1)),
    ?assertEqual({19, 1}, get(bytes)),
    ?assertEqual([{bytes, 20}], emqx_pd:get_counters([bytes])),
    ?assertEqual(20, emqx_pd:reset_counter(bytes)),
    ?assertEqual(0, emqx_pd:get_counter_delta(bytes)),
    ?assertEqual(0, get(bytes)).

-doc "Accumulate a delta when the dictionary has no counter yet.".
t_counter_delta_initial(_) ->
    ?assertEqual(undefined, emqx_pd:inc_counter_with_delta(bytes, 2)),
    ?assertEqual({0, 2}, get(bytes)),
    ?assertEqual(2, emqx_pd:inc_counter_with_delta(bytes, 3)),
    ?assertEqual(5, emqx_pd:take_counter_delta(bytes)),
    ?assertEqual(5, get(bytes)).
