%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_packet_id_bitmap_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() -> emqx_common_test_helpers:all(?MODULE).

-doc """
Check that `next_free/2` skips a used packet id at the wrap point, the
case where the session used to crash with `{key_exists, Id}`.
""".
t_wraparound(_) ->
    Bitmap = emqx_packet_id_bitmap:from_list([1, 2, 16#FFFF]),
    ?assertEqual({ok, 3}, emqx_packet_id_bitmap:next_free(16#FFFF, Bitmap)),
    ?assertEqual({ok, 3}, emqx_packet_id_bitmap:next_free(1, Bitmap)),
    ?assertEqual({ok, 16#FFFE}, emqx_packet_id_bitmap:next_free(16#FFFE, Bitmap)).

-doc """
Check that `next_free/2` returns `none` when all 65535 packet ids are used,
never returns 0, and finds the only free id from any start point.
""".
t_full(_) ->
    Full = emqx_packet_id_bitmap:from_list(lists:seq(1, 16#FFFF)),
    ?assertEqual(none, emqx_packet_id_bitmap:next_free(1, Full)),
    ?assertEqual(none, emqx_packet_id_bitmap:next_free(16#FFFF, Full)),
    OneFree = emqx_packet_id_bitmap:unset(1000, Full),
    lists:foreach(
        fun(From) ->
            ?assertEqual({ok, 1000}, emqx_packet_id_bitmap:next_free(From, OneFree))
        end,
        [1, 999, 1000, 1001, 16#FFFF]
    ).

-doc """
Check `next_free/2` when every other packet id is used, which puts a used
id in every chunk.
""".
t_alternating(_) ->
    Bitmap = emqx_packet_id_bitmap:from_list(lists:seq(1, 16#FFFF, 2)),
    ?assertEqual(2048, map_size(Bitmap)),
    ?assertEqual({ok, 2}, emqx_packet_id_bitmap:next_free(1, Bitmap)),
    ?assertEqual({ok, 100}, emqx_packet_id_bitmap:next_free(99, Bitmap)),
    ?assertEqual({ok, 100}, emqx_packet_id_bitmap:next_free(100, Bitmap)),
    ?assertEqual({ok, 2}, emqx_packet_id_bitmap:next_free(16#FFFF, Bitmap)).

-doc """
Check `next_free/2` when one long block of packet ids is used, which makes
the scan walk over many full chunks, including across the wrap.
""".
t_contiguous_block(_) ->
    Bitmap = emqx_packet_id_bitmap:from_list(lists:seq(1, 65000)),
    ?assertEqual({ok, 65001}, emqx_packet_id_bitmap:next_free(1, Bitmap)),
    ?assertEqual({ok, 65001}, emqx_packet_id_bitmap:next_free(32000, Bitmap)),
    ?assertEqual({ok, 16#FFFF}, emqx_packet_id_bitmap:next_free(16#FFFF, Bitmap)),
    Wrapped = emqx_packet_id_bitmap:set(16#FFFF, Bitmap),
    ?assertEqual({ok, 65001}, emqx_packet_id_bitmap:next_free(16#FFFF, Wrapped)),
    Hole = emqx_packet_id_bitmap:unset(30000, Bitmap),
    ?assertEqual({ok, 30000}, emqx_packet_id_bitmap:next_free(1, Hole)).

-doc """
Check that a chunk is removed when its last used id is unset, and added
back when an id in it is set again.
""".
t_chunk_emptied_and_refilled(_) ->
    ChunkIds = lists:seq(32, 63),
    Filled = emqx_packet_id_bitmap:from_list(ChunkIds),
    ?assertEqual(#{1 => 16#FFFFFFFF}, Filled),
    ?assertEqual({ok, 64}, emqx_packet_id_bitmap:next_free(32, Filled)),
    Emptied = lists:foldl(fun emqx_packet_id_bitmap:unset/2, Filled, ChunkIds),
    ?assertEqual(#{}, Emptied),
    ?assertEqual({ok, 32}, emqx_packet_id_bitmap:next_free(32, Emptied)),
    Refilled = emqx_packet_id_bitmap:set(40, Emptied),
    ?assertEqual(#{1 => 1 bsl 8}, Refilled),
    ?assertEqual({ok, 41}, emqx_packet_id_bitmap:next_free(40, Refilled)).

-doc """
Check that `next_free/2` returns the same id as a walk over the used set,
for random sets of used packet ids of varying density.
""".
t_matches_walk(_) ->
    rand:seed(exsss, {17897, 1, 2}),
    lists:foreach(
        fun(Density) ->
            Ids = [Id || Id <- lists:seq(1, 16#FFFF), rand:uniform() < Density],
            Used = sets:from_list(Ids, [{version, 2}]),
            Bitmap = emqx_packet_id_bitmap:from_list(Ids),
            lists:foreach(
                fun(_) ->
                    From = rand:uniform(16#FFFF),
                    ?assertEqual(
                        walk_free(From, Used, 16#FFFF),
                        emqx_packet_id_bitmap:next_free(From, Bitmap)
                    )
                end,
                lists:seq(1, 100)
            )
        end,
        [0.0, 0.1, 0.5, 0.9, 0.99, 0.9999]
    ).

walk_free(_Id, _Used, 0) ->
    none;
walk_free(Id, Used, Left) ->
    case sets:is_element(Id, Used) of
        false -> {ok, Id};
        true -> walk_free(Id rem 16#FFFF + 1, Used, Left - 1)
    end.
