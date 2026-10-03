%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_contain(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new()),
    ?assert(emqx_inflight:contain(1, Inflight)),
    ?assertNot(emqx_inflight:contain(99, Inflight)).

t_lookup(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new()),
    ?assertEqual({value, v}, emqx_inflight:lookup(1, Inflight)),
    ?assertEqual(none, emqx_inflight:lookup(99, Inflight)).

t_insert(_) ->
    Inflight = emqx_inflight:insert(
        2,
        2,
        emqx_inflight:insert(
            1, 1, emqx_inflight:new()
        )
    ),
    ?assertEqual(2, emqx_inflight:size(Inflight)),
    ?assertEqual({value, 1}, emqx_inflight:lookup(1, Inflight)),
    ?assertEqual({value, 2}, emqx_inflight:lookup(2, Inflight)),
    ?assertError({key_exists, 1}, emqx_inflight:insert(1, 1, Inflight)).

t_update(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new()),
    ?assertEqual(Inflight, emqx_inflight:update(1, v, Inflight)),
    ?assertError(function_clause, emqx_inflight:update(99, v, Inflight)).

t_resize(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new(2)),
    ?assertEqual(1, emqx_inflight:size(Inflight)),
    ?assertEqual(2, emqx_inflight:max_size(Inflight)),
    Inflight1 = emqx_inflight:resize(4, Inflight),
    ?assertEqual(4, emqx_inflight:max_size(Inflight1)),
    ?assertEqual(1, emqx_inflight:size(Inflight)).

t_delete(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new(2)),
    Inflight1 = emqx_inflight:delete(1, Inflight),
    ?assert(emqx_inflight:is_empty(Inflight1)),
    ?assertNot(emqx_inflight:contain(1, Inflight1)).

t_values(_) ->
    Inflight = emqx_inflight:insert(
        2,
        2,
        emqx_inflight:insert(
            1, 1, emqx_inflight:new()
        )
    ),
    ?assertEqual([1, 2], emqx_inflight:values(Inflight)),
    ?assertEqual([{1, 1}, {2, 2}], emqx_inflight:to_list(Inflight)).

t_fold(_) ->
    Inflight = maps:fold(
        fun emqx_inflight:insert/3,
        emqx_inflight:new(),
        #{1 => 1, 2 => 2, 3 => 42}
    ),
    ?assertEqual(
        emqx_inflight:fold(fun(_, V, S) -> S + V end, 0, Inflight),
        lists:foldl(fun({_, V}, S) -> S + V end, 0, emqx_inflight:to_list(Inflight))
    ).

t_is_full(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new()),
    ?assertNot(emqx_inflight:is_full(Inflight)),
    Inflight1 = emqx_inflight:insert(
        2,
        2,
        emqx_inflight:insert(
            1, 1, emqx_inflight:new(2)
        )
    ),
    ?assert(emqx_inflight:is_full(Inflight1)).

t_is_empty(_) ->
    Inflight = emqx_inflight:insert(1, 1, emqx_inflight:new(2)),
    ?assertNot(emqx_inflight:is_empty(Inflight)),
    Inflight1 = emqx_inflight:delete(1, Inflight),
    ?assert(emqx_inflight:is_empty(Inflight1)).

t_to_list(_) ->
    Inflight = lists:foldl(
        fun(Seq, InflightAcc) ->
            emqx_inflight:insert(Seq, integer_to_binary(Seq), InflightAcc)
        end,
        emqx_inflight:new(100),
        [1, 6, 2, 3, 10, 7, 9, 8, 4, 5]
    ),
    ExpList = [{Seq, integer_to_binary(Seq)} || Seq <- lists:seq(1, 10)],
    ?assertEqual(ExpList, emqx_inflight:to_list(Inflight)).

-doc """
Check that `reserve/1` skips a used packet id at the wrap point, the
case where the session used to crash with `{key_exists, Id}`.
""".
t_reserve_wraparound(_) ->
    Inflight = insert_ids([1, 2, 16#FFFF], emqx_inflight:new(50000)),
    ?assertEqual({ok, 3}, free_from(16#FFFF, Inflight)),
    ?assertEqual({ok, 3}, free_from(1, Inflight)),
    ?assertEqual({ok, 16#FFFE}, free_from(16#FFFE, Inflight)).

-doc """
Check that `reserve/1` returns `none` when all 65535 packet ids are in
use, and finds the only free id from any start point.
""".
t_reserve_full(_) ->
    Full = insert_ids(lists:seq(1, 16#FFFF), emqx_inflight:new(16#FFFF)),
    ?assert(emqx_inflight:is_full(Full)),
    ?assertEqual(none, free_from(1, Full)),
    ?assertEqual(none, free_from(16#FFFF, Full)),
    OneFree = emqx_inflight:delete(1000, Full),
    lists:foreach(
        fun(From) -> ?assertEqual({ok, 1000}, free_from(From, OneFree)) end,
        [1, 999, 1000, 1001, 16#FFFF]
    ).

-doc """
Check `reserve/1` when every other packet id is in use, which puts a
used id in every chunk of the index.
""".
t_reserve_alternating(_) ->
    Inflight = insert_ids(lists:seq(1, 16#FFFF, 2), emqx_inflight:new(0)),
    ?assertEqual(2048, map_size(index(Inflight))),
    ?assertEqual({ok, 2}, free_from(1, Inflight)),
    ?assertEqual({ok, 100}, free_from(99, Inflight)),
    ?assertEqual({ok, 100}, free_from(100, Inflight)),
    ?assertEqual({ok, 2}, free_from(16#FFFF, Inflight)).

-doc """
Check `reserve/1` when one long block of packet ids is in use, which
makes the scan walk over many full chunks.
""".
t_reserve_contiguous_block(_) ->
    Inflight = insert_ids(lists:seq(1, 65000), emqx_inflight:new(0)),
    ?assertEqual({ok, 65001}, free_from(1, Inflight)),
    ?assertEqual({ok, 65001}, free_from(32000, Inflight)),
    ?assertEqual({ok, 16#FFFF}, free_from(16#FFFF, Inflight)),
    Wrapped = emqx_inflight:insert(16#FFFF, v, Inflight),
    ?assertEqual({ok, 65001}, free_from(16#FFFF, Wrapped)),
    Hole = emqx_inflight:delete(30000, Inflight),
    ?assertEqual({ok, 30000}, free_from(1, Hole)).

-doc """
Check that the index removes a chunk when its last used id is deleted, and
adds it back when an id in it is used again.
""".
t_index_chunk_emptied_and_refilled(_) ->
    ChunkIds = lists:seq(32, 63),
    Filled = insert_ids(ChunkIds, emqx_inflight:new(0)),
    ?assertEqual(#{1 => 16#FFFFFFFF}, index(Filled)),
    ?assertEqual({ok, 64}, free_from(32, Filled)),
    Emptied = lists:foldl(fun emqx_inflight:delete/2, Filled, ChunkIds),
    ?assertEqual(#{}, index(Emptied)),
    ?assert(emqx_inflight:is_empty(Emptied)),
    ?assertEqual({ok, 32}, free_from(32, Emptied)),
    Refilled = emqx_inflight:insert(40, v, Emptied),
    ?assertEqual(#{1 => 1 bsl 8}, index(Refilled)),
    ?assertEqual({ok, 41}, free_from(40, Refilled)).

-doc """
Check that `reserve/1` returns the same id as a walk over `contain/2`,
for random sets of used packet ids of varying density.
""".
t_reserve_matches_walk(_) ->
    rand:seed(exsss, {17897, 1, 2}),
    lists:foreach(
        fun(Density) ->
            Ids = [Id || Id <- lists:seq(1, 16#FFFF), rand:uniform() < Density],
            Inflight = insert_ids(Ids, emqx_inflight:new(0)),
            lists:foreach(
                fun(_) ->
                    From = rand:uniform(16#FFFF),
                    ?assertEqual(
                        walk_free_id(From, Inflight, 16#FFFF),
                        free_from(From, Inflight)
                    )
                end,
                lists:seq(1, 100)
            )
        end,
        [0.0, 0.1, 0.5, 0.9, 0.99, 0.9999]
    ).

-doc "Check that `insert/3` rejects keys outside the packet id range.".
t_insert_rejects_non_packet_id(_) ->
    Inflight = emqx_inflight:insert(1, v, emqx_inflight:new(0)),
    lists:foreach(
        fun(Key) ->
            ?assertError(function_clause, emqx_inflight:insert(Key, v, emqx_inflight:new(0))),
            ?assertError(function_clause, emqx_inflight:insert(Key, v, Inflight))
        end,
        [0, 16#10000, -1, {1, 2}, <<"k">>, k]
    ).

-doc """
Check that `alloc/2` inserts the value under the first free packet id at or
after the counter and moves the counter past it, wrapping from 65535 to 1.
""".
t_alloc(_) ->
    I0 = emqx_inflight:set_next_id(16#FFFE, insert_ids([16#FFFF, 1], emqx_inflight:new(0))),
    {ok, 16#FFFE, I1} = emqx_inflight:alloc(a, I0),
    ?assertEqual(16#FFFF, emqx_inflight:next_id(I1)),
    {ok, 2, I2} = emqx_inflight:alloc(b, I1),
    ?assertEqual(3, emqx_inflight:next_id(I2)),
    ?assertEqual({value, a}, emqx_inflight:lookup(16#FFFE, I2)),
    ?assertEqual({value, b}, emqx_inflight:lookup(2, I2)),
    ?assertEqual(4, emqx_inflight:size(I2)),
    Full = insert_ids(lists:seq(1, 16#FFFF), emqx_inflight:new(0)),
    ?assertEqual(none, emqx_inflight:alloc(c, Full)).

-doc """
Check that `reserve/1` moves the counter without inserting, and that
`insert/3` and `delete/2` leave the counter unchanged.
""".
t_reserve_counter(_) ->
    I0 = emqx_inflight:new(0),
    ?assertEqual(1, emqx_inflight:next_id(I0)),
    {ok, 1, I1} = emqx_inflight:reserve(I0),
    ?assertEqual(2, emqx_inflight:next_id(I1)),
    ?assert(emqx_inflight:is_empty(I1)),
    I2 = emqx_inflight:insert(2, v, I1),
    ?assertEqual(2, emqx_inflight:next_id(I2)),
    {ok, 3, I3} = emqx_inflight:reserve(I2),
    ?assertEqual(4, emqx_inflight:next_id(emqx_inflight:delete(2, I3))),
    {ok, 16#FFFF, I4} = emqx_inflight:reserve(emqx_inflight:set_next_id(16#FFFF, I3)),
    ?assertEqual(1, emqx_inflight:next_id(I4)).

-doc """
Check that `new/1` returns a placeholder that holds only the size limit, that
the read functions work on it, and that the first insert builds the full
structure.
""".
t_new_is_lazy(_) ->
    I0 = emqx_inflight:new(32),
    ?assertEqual({inflight, 32}, I0),
    ?assertEqual(32, emqx_inflight:max_size(I0)),
    ?assertEqual(0, emqx_inflight:size(I0)),
    ?assert(emqx_inflight:is_empty(I0)),
    ?assertNot(emqx_inflight:is_full(I0)),
    ?assertNot(emqx_inflight:contain(1, I0)),
    ?assertEqual(none, emqx_inflight:lookup(1, I0)),
    ?assertEqual([], emqx_inflight:to_list(I0)),
    ?assertEqual([], emqx_inflight:to_list(fun erlang:'<'/2, I0)),
    ?assertEqual([], emqx_inflight:values(I0)),
    ?assertEqual(acc, emqx_inflight:fold(fun(_, _, _) -> hit end, acc, I0)),
    ?assertEqual(1, emqx_inflight:next_id(I0)),
    ?assertEqual({inflight, 64}, emqx_inflight:resize(64, I0)),
    ?assertEqual(I0, emqx_inflight:set_next_id(1, I0)),
    ?assertError(function_clause, emqx_inflight:delete(1, I0)),
    ?assertError(function_clause, emqx_inflight:update(1, v, I0)),
    I1 = emqx_inflight:insert(1, v, I0),
    ?assertMatch({inflight, 32, #{1 := v}, #{0 := 2}, 1}, I1),
    ?assertMatch({inflight, 32, #{}, #{}, 7}, emqx_inflight:set_next_id(7, I0)),
    {ok, 1, I2} = emqx_inflight:alloc(v, I0),
    ?assertMatch({inflight, 32, #{1 := v}, #{0 := 2}, 2}, I2),
    {ok, 1, I3} = emqx_inflight:reserve(I0),
    ?assertMatch({inflight, 32, #{}, #{}, 2}, I3).

insert_ids(Ids, Inflight) ->
    lists:foldl(fun(Id, Acc) -> emqx_inflight:insert(Id, v, Acc) end, Inflight, Ids).

%% Find a free packet id from `From`, through the public API.
free_from(From, Inflight) ->
    case emqx_inflight:reserve(emqx_inflight:set_next_id(From, Inflight)) of
        {ok, Id, _} -> {ok, Id};
        none -> none
    end.

%% Reads the packet id index of the opaque `#inflight{}` record.
index({inflight, _MaxSize, _Entries, Index, _NextId}) ->
    Index.

walk_free_id(_Id, _Inflight, 0) ->
    none;
walk_free_id(Id, Inflight, Left) ->
    case emqx_inflight:contain(Id, Inflight) of
        false -> {ok, Id};
        true -> walk_free_id(Id rem 16#FFFF + 1, Inflight, Left - 1)
    end.
