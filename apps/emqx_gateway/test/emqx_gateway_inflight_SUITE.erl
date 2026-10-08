%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_gateway_inflight_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-doc "Check `contain/2` for a present and an absent key.".
t_contain(_) ->
    Inflight = emqx_gateway_inflight:insert(k, v, emqx_gateway_inflight:new()),
    ?assert(emqx_gateway_inflight:contain(k, Inflight)),
    ?assertNot(emqx_gateway_inflight:contain(badkey, Inflight)).

-doc "Check that `insert/3` keeps both entries and rejects a duplicate key.".
t_insert(_) ->
    Inflight = emqx_gateway_inflight:insert(
        {b, 2},
        2,
        emqx_gateway_inflight:insert(
            {a, 1}, 1, emqx_gateway_inflight:new()
        )
    ),
    ?assertEqual(2, emqx_gateway_inflight:size(Inflight)),
    ?assertEqual([{{a, 1}, 1}, {{b, 2}, 2}], emqx_gateway_inflight:to_list(Inflight)),
    ?assertError({key_exists, {a, 1}}, emqx_gateway_inflight:insert({a, 1}, 1, Inflight)).

-doc "Check that `update/3` replaces the value and rejects an absent key.".
t_update(_) ->
    Inflight = emqx_gateway_inflight:insert(k, v, emqx_gateway_inflight:new()),
    Inflight1 = emqx_gateway_inflight:update(k, v2, Inflight),
    ?assertEqual([{k, v2}], emqx_gateway_inflight:to_list(Inflight1)),
    ?assertError(function_clause, emqx_gateway_inflight:update(badkey, v, Inflight)).

-doc "Check that `delete/2` removes the entry and rejects an absent key.".
t_delete(_) ->
    Inflight = emqx_gateway_inflight:insert(k, v, emqx_gateway_inflight:new(2)),
    Inflight1 = emqx_gateway_inflight:delete(k, Inflight),
    ?assert(emqx_gateway_inflight:is_empty(Inflight1)),
    ?assertNot(emqx_gateway_inflight:contain(k, Inflight1)),
    ?assertError(function_clause, emqx_gateway_inflight:delete(k, Inflight1)).

-doc "Check `is_full/1` against the size limit, and that 0 means no limit.".
t_is_full(_) ->
    Unlimited = emqx_gateway_inflight:insert(k, v, emqx_gateway_inflight:new()),
    ?assertEqual(0, emqx_gateway_inflight:max_size(Unlimited)),
    ?assertNot(emqx_gateway_inflight:is_full(Unlimited)),
    Limited = emqx_gateway_inflight:insert(
        b,
        2,
        emqx_gateway_inflight:insert(
            a, 1, emqx_gateway_inflight:new(2)
        )
    ),
    ?assertEqual(2, emqx_gateway_inflight:max_size(Limited)),
    ?assert(emqx_gateway_inflight:is_full(Limited)),
    ?assertNot(emqx_gateway_inflight:is_full(emqx_gateway_inflight:delete(a, Limited))).

-doc "Check `is_empty/1` before and after the only entry is deleted.".
t_is_empty(_) ->
    Empty = emqx_gateway_inflight:new(2),
    ?assert(emqx_gateway_inflight:is_empty(Empty)),
    ?assertEqual([], emqx_gateway_inflight:to_list(Empty)),
    Inflight = emqx_gateway_inflight:insert(a, 1, Empty),
    ?assertNot(emqx_gateway_inflight:is_empty(Inflight)),
    ?assert(emqx_gateway_inflight:is_empty(emqx_gateway_inflight:delete(a, Inflight))).

-doc "Check that `to_list/1` orders entries by key for any key term.".
t_to_list(_) ->
    Keys = [10, {cmd, 3}, <<"k">>, atom, {cmd, 1}, 2],
    Inflight = lists:foldl(
        fun(Key, Acc) -> emqx_gateway_inflight:insert(Key, Key, Acc) end,
        emqx_gateway_inflight:new(100),
        Keys
    ),
    ?assertEqual(
        [{Key, Key} || Key <- lists:sort(Keys)],
        emqx_gateway_inflight:to_list(Inflight)
    ).
