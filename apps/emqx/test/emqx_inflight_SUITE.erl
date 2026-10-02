%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

t_contain(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new()),
    ?assert(emqx_inflight:contain(k, Inflight)),
    ?assertNot(emqx_inflight:contain(badkey, Inflight)).

t_lookup(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new()),
    ?assertEqual({value, v}, emqx_inflight:lookup(k, Inflight)),
    ?assertEqual(none, emqx_inflight:lookup(badkey, Inflight)).

t_insert(_) ->
    Inflight = emqx_inflight:insert(
        b,
        2,
        emqx_inflight:insert(
            a, 1, emqx_inflight:new()
        )
    ),
    ?assertEqual(2, emqx_inflight:size(Inflight)),
    ?assertEqual({value, 1}, emqx_inflight:lookup(a, Inflight)),
    ?assertEqual({value, 2}, emqx_inflight:lookup(b, Inflight)),
    ?assertError({key_exists, a}, emqx_inflight:insert(a, 1, Inflight)).

t_update(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new()),
    ?assertEqual(Inflight, emqx_inflight:update(k, v, Inflight)),
    ?assertError(function_clause, emqx_inflight:update(badkey, v, Inflight)).

t_resize(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new(2)),
    ?assertEqual(1, emqx_inflight:size(Inflight)),
    ?assertEqual(2, emqx_inflight:max_size(Inflight)),
    Inflight1 = emqx_inflight:resize(4, Inflight),
    ?assertEqual(4, emqx_inflight:max_size(Inflight1)),
    ?assertEqual(1, emqx_inflight:size(Inflight)).

t_delete(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new(2)),
    Inflight1 = emqx_inflight:delete(k, Inflight),
    ?assert(emqx_inflight:is_empty(Inflight1)),
    ?assertNot(emqx_inflight:contain(k, Inflight1)).

t_values(_) ->
    Inflight = emqx_inflight:insert(
        b,
        2,
        emqx_inflight:insert(
            a, 1, emqx_inflight:new()
        )
    ),
    ?assertEqual([1, 2], emqx_inflight:values(Inflight)),
    ?assertEqual([{a, 1}, {b, 2}], emqx_inflight:to_list(Inflight)).

t_fold(_) ->
    Inflight = maps:fold(
        fun emqx_inflight:insert/3,
        emqx_inflight:new(),
        #{a => 1, b => 2, c => 42}
    ),
    ?assertEqual(
        emqx_inflight:fold(fun(_, V, S) -> S + V end, 0, Inflight),
        lists:foldl(fun({_, V}, S) -> S + V end, 0, emqx_inflight:to_list(Inflight))
    ).

t_is_full(_) ->
    Inflight = emqx_inflight:insert(k, v, emqx_inflight:new()),
    ?assertNot(emqx_inflight:is_full(Inflight)),
    Inflight1 = emqx_inflight:insert(
        b,
        2,
        emqx_inflight:insert(
            a, 1, emqx_inflight:new(2)
        )
    ),
    ?assert(emqx_inflight:is_full(Inflight1)).

t_is_empty(_) ->
    Inflight = emqx_inflight:insert(a, 1, emqx_inflight:new(2)),
    ?assertNot(emqx_inflight:is_empty(Inflight)),
    Inflight1 = emqx_inflight:delete(a, Inflight),
    ?assert(emqx_inflight:is_empty(Inflight1)).

t_window(_) ->
    ?assertEqual([], emqx_inflight:window(emqx_inflight:new(0))),
    Inflight = emqx_inflight:insert(
        b,
        2,
        emqx_inflight:insert(
            a, 1, emqx_inflight:new(2)
        )
    ),
    ?assertEqual([a, b], emqx_inflight:window(Inflight)).

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
Check that `next_free_id/2` skips a used packet id at the wrap point, the
case where the session used to crash with `{key_exists, Id}`.
""".
t_next_free_id_wraparound(_) ->
    Inflight = insert_ids([1, 2, 16#FFFF], emqx_inflight:new(50000)),
    ?assertEqual({ok, 3}, emqx_inflight:next_free_id(16#FFFF, Inflight)),
    ?assertEqual({ok, 3}, emqx_inflight:next_free_id(1, Inflight)),
    ?assertEqual({ok, 16#FFFE}, emqx_inflight:next_free_id(16#FFFE, Inflight)).

-doc """
Check that `next_free_id/2` returns `none` when all 65535 packet ids are in
use, and finds the only free id from any start point.
""".
t_next_free_id_full(_) ->
    Full = insert_ids(lists:seq(1, 16#FFFF), emqx_inflight:new(16#FFFF)),
    ?assert(emqx_inflight:is_full(Full)),
    ?assertEqual(none, emqx_inflight:next_free_id(1, Full)),
    ?assertEqual(none, emqx_inflight:next_free_id(16#FFFF, Full)),
    OneFree = emqx_inflight:delete(1000, Full),
    lists:foreach(
        fun(From) -> ?assertEqual({ok, 1000}, emqx_inflight:next_free_id(From, OneFree)) end,
        [1, 999, 1000, 1001, 16#FFFF]
    ).

-doc """
Check `next_free_id/2` when every other packet id is in use, which puts a
used id in every chunk of the index.
""".
t_next_free_id_alternating(_) ->
    Inflight = insert_ids(lists:seq(1, 16#FFFF, 2), emqx_inflight:new(0)),
    ?assertEqual(2048, map_size(index(Inflight))),
    ?assertEqual({ok, 2}, emqx_inflight:next_free_id(1, Inflight)),
    ?assertEqual({ok, 100}, emqx_inflight:next_free_id(99, Inflight)),
    ?assertEqual({ok, 100}, emqx_inflight:next_free_id(100, Inflight)),
    ?assertEqual({ok, 2}, emqx_inflight:next_free_id(16#FFFF, Inflight)).

-doc """
Check `next_free_id/2` when one long block of packet ids is in use, which
makes the scan walk over many full chunks.
""".
t_next_free_id_contiguous_block(_) ->
    Inflight = insert_ids(lists:seq(1, 65000), emqx_inflight:new(0)),
    ?assertEqual({ok, 65001}, emqx_inflight:next_free_id(1, Inflight)),
    ?assertEqual({ok, 65001}, emqx_inflight:next_free_id(32000, Inflight)),
    ?assertEqual({ok, 16#FFFF}, emqx_inflight:next_free_id(16#FFFF, Inflight)),
    Wrapped = emqx_inflight:insert(16#FFFF, v, Inflight),
    ?assertEqual({ok, 65001}, emqx_inflight:next_free_id(16#FFFF, Wrapped)),
    Hole = emqx_inflight:delete(30000, Inflight),
    ?assertEqual({ok, 30000}, emqx_inflight:next_free_id(1, Hole)).

-doc """
Check that the index removes a chunk when its last used id is deleted, and
adds it back when an id in it is used again.
""".
t_index_chunk_emptied_and_refilled(_) ->
    ChunkIds = lists:seq(32, 63),
    Filled = insert_ids(ChunkIds, emqx_inflight:new(0)),
    ?assertEqual(#{1 => 16#FFFFFFFF}, index(Filled)),
    ?assertEqual({ok, 64}, emqx_inflight:next_free_id(32, Filled)),
    Emptied = lists:foldl(fun emqx_inflight:delete/2, Filled, ChunkIds),
    ?assertEqual(#{}, index(Emptied)),
    ?assert(emqx_inflight:is_empty(Emptied)),
    ?assertEqual({ok, 32}, emqx_inflight:next_free_id(32, Emptied)),
    Refilled = emqx_inflight:insert(40, v, Emptied),
    ?assertEqual(#{1 => 1 bsl 8}, index(Refilled)),
    ?assertEqual({ok, 41}, emqx_inflight:next_free_id(40, Refilled)).

-doc """
Check that `next_free_id/2` returns the same id as a walk over `contain/2`,
for random sets of used packet ids of varying density.
""".
t_next_free_id_matches_walk(_) ->
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
                        emqx_inflight:next_free_id(From, Inflight)
                    )
                end,
                lists:seq(1, 100)
            )
        end,
        [0.0, 0.1, 0.5, 0.9, 0.99, 0.9999]
    ).

-doc "Check that keys outside the packet id range are kept out of the index.".
t_non_packet_id_keys_not_indexed(_) ->
    Inflight = insert_ids([0, 16#10000, -1, {1, 2}, <<"k">>], emqx_inflight:new(0)),
    ?assertEqual(5, emqx_inflight:size(Inflight)),
    ?assertEqual(#{}, index(Inflight)),
    ?assertEqual({ok, 1}, emqx_inflight:next_free_id(1, Inflight)),
    ?assert(
        emqx_inflight:is_empty(
            lists:foldl(fun emqx_inflight:delete/2, Inflight, [0, 16#10000, -1, {1, 2}, <<"k">>])
        )
    ).

insert_ids(Ids, Inflight) ->
    lists:foldl(fun(Id, Acc) -> emqx_inflight:insert(Id, v, Acc) end, Inflight, Ids).

index({inflight, _MaxSize, _Map, Index}) ->
    Index.

walk_free_id(_Id, _Inflight, 0) ->
    none;
walk_free_id(Id, Inflight, Left) ->
    case emqx_inflight:contain(Id, Inflight) of
        false -> {ok, Id};
        true -> walk_free_id(Id rem 16#FFFF + 1, Inflight, Left - 1)
    end.
