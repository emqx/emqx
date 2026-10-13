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
Check that `alloc/2` hands out the counter value and moves it on by one,
wrapping from 65535 to 1, without building the bitmap while no used id is
in the way.
""".
t_alloc_plain(_) ->
    I0 = emqx_inflight:set_next_id(16#FFFE, emqx_inflight:new(32)),
    {ok, 16#FFFE, I1} = emqx_inflight:alloc(a, I0),
    {ok, 16#FFFF, I2} = emqx_inflight:alloc(b, I1),
    {ok, 1, I3} = emqx_inflight:alloc(c, I2),
    ?assertEqual(2, emqx_inflight:next_id(I3)),
    ?assertEqual([{1, c}, {16#FFFE, a}, {16#FFFF, b}], emqx_inflight:to_list(I3)),
    ?assertEqual(lazy, bitmap(I3)).

-doc """
Check that `alloc/2` builds the bitmap when the counter lands on a used id,
skips to the first unused id, and keeps the bitmap in sync and present
while a used id is still ahead of the counter.
""".
t_alloc_clash_builds_bitmap(_) ->
    %% 65535 is an old unacked id; the counter has wrapped and caught up.
    I0 = emqx_inflight:insert(16#FFFF, old, emqx_inflight:new(32)),
    I1 = emqx_inflight:insert(1, a, emqx_inflight:set_next_id(16#FFFF, I0)),
    ?assertEqual(lazy, bitmap(I1)),
    {ok, 2, I2} = emqx_inflight:alloc(b, I1),
    ?assertEqual(3, emqx_inflight:next_id(I2)),
    %% Bitmap built from the keys {1, 65535} and updated with 2; 65535 is
    %% still ahead of the counter, so the bitmap stays.
    ?assertEqual(
        emqx_packet_id_bitmap:from_list([1, 2, 16#FFFF]),
        bitmap(I2)
    ),
    {ok, 3, I3} = emqx_inflight:alloc(c, I2),
    ?assertEqual(emqx_packet_id_bitmap:from_list([1, 2, 3, 16#FFFF]), bitmap(I3)),
    I4 = emqx_inflight:delete(1, I3),
    ?assertEqual(emqx_packet_id_bitmap:from_list([2, 3, 16#FFFF]), bitmap(I4)).

-doc """
Check that the bitmap is dropped as soon as the counter is past the largest
used id, both after a delete and after an allocation.
""".
t_bitmap_dropped_when_counter_is_ahead(_) ->
    I0 = emqx_inflight:insert(16#FFFF, old, emqx_inflight:new(32)),
    {ok, 1, I1} = emqx_inflight:alloc(a, emqx_inflight:set_next_id(16#FFFF, I0)),
    ?assertNotEqual(lazy, bitmap(I1)),
    %% Acking the old id leaves {1}, and the counter is at 2.
    I2 = emqx_inflight:delete(16#FFFF, I1),
    ?assertEqual(lazy, bitmap(I2)),
    ?assertEqual(2, emqx_inflight:next_id(I2)),
    %% The same through an allocation: counter at 3 lands on used id 3,
    %% takes 4, and is then past the largest used id.
    I3 = emqx_inflight:insert(3, c, emqx_inflight:set_next_id(3, emqx_inflight:new(32))),
    {ok, 4, I4} = emqx_inflight:alloc(d, I3),
    ?assertEqual(lazy, bitmap(I4)),
    ?assertEqual(5, emqx_inflight:next_id(I4)),
    %% Deleting the last entry also drops it.
    I5 = emqx_inflight:insert(7, x, emqx_inflight:set_next_id(5, emqx_inflight:new(32))),
    {ok, 6, I6} = emqx_inflight:alloc(y, emqx_inflight:insert(5, w, I5)),
    ?assertNotEqual(lazy, bitmap(I6)),
    I7 = emqx_inflight:delete(7, emqx_inflight:delete(5, emqx_inflight:delete(6, I6))),
    ?assert(emqx_inflight:is_empty(I7)),
    ?assertEqual(lazy, bitmap(I7)).

-doc """
Check that the bitmap is kept once built when the size limit is above
32767 or unlimited, even after the counter is past every used id.
""".
t_bitmap_kept_for_wide_window(_) ->
    lists:foreach(
        fun(MaxSize) ->
            I0 = emqx_inflight:insert(
                3, c, emqx_inflight:set_next_id(3, emqx_inflight:new(MaxSize))
            ),
            {ok, 4, I1} = emqx_inflight:alloc(d, I0),
            ?assertEqual(emqx_packet_id_bitmap:from_list([3, 4]), bitmap(I1)),
            I2 = emqx_inflight:delete(4, emqx_inflight:delete(3, I1)),
            ?assert(emqx_inflight:is_empty(I2)),
            ?assertEqual(emqx_packet_id_bitmap:new(), bitmap(I2)),
            {ok, 5, I3} = emqx_inflight:alloc(e, I2),
            ?assertEqual(emqx_packet_id_bitmap:from_list([5]), bitmap(I3))
        end,
        [32768, 50000, 16#FFFF, 0]
    ),
    %% At the limit itself the bitmap is still dropped.
    I4 = emqx_inflight:insert(3, c, emqx_inflight:set_next_id(3, emqx_inflight:new(32767))),
    {ok, 4, I5} = emqx_inflight:alloc(d, I4),
    ?assertEqual(lazy, bitmap(I5)).

-doc """
Check that `alloc/2` returns `none` when all 65535 packet ids are in use,
and allocates the only free id after one is released.
""".
t_alloc_full(_) ->
    Full = lists:foldl(
        fun(Id, Acc) -> emqx_inflight:insert(Id, v, Acc) end,
        emqx_inflight:new(16#FFFF),
        lists:seq(1, 16#FFFF)
    ),
    ?assert(emqx_inflight:is_full(Full)),
    ?assertEqual(none, emqx_inflight:alloc(v, Full)),
    OneFree = emqx_inflight:delete(1000, Full),
    {ok, 1000, _} = emqx_inflight:alloc(v, OneFree),
    {ok, 1000, _} = emqx_inflight:alloc(v, emqx_inflight:set_next_id(1001, OneFree)).

-doc """
Check that `new/1` returns a placeholder that holds only the size limit, that
the read functions work on it, and that the first write builds the record.
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
    ?assertMatch({inflight, 1, 32, lazy, _}, I1),
    ?assertEqual([{1, v}], emqx_inflight:to_list(I1)),
    ?assertMatch({inflight, 7, 32, lazy, _}, emqx_inflight:set_next_id(7, I0)),
    {ok, 1, I2} = emqx_inflight:alloc(v, I0),
    ?assertMatch({inflight, 2, 32, lazy, _}, I2).

%% Reads the bitmap field of the opaque `#inflight{}` record.
bitmap({inflight, _NextId, _MaxSize, Bitmap, _Entries}) ->
    Bitmap.
