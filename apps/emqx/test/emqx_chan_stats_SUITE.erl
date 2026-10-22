%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_chan_stats_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_channel.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(SOCK_KEYS, [recv_oct, recv_cnt, send_oct, send_cnt, send_pend]).
-define(SESSION_KEYS, [
    subscriptions_cnt,
    inflight_cnt,
    mqueue_len,
    total_payload_bytes,
    mqueue_dropped,
    awaiting_rel_cnt
]).

all() -> emqx_common_test_helpers:all(?MODULE).

-doc """
The array has one slot per key of `keys/0`, and every key has a slot: the
size constant in the module must follow the key list.
""".
t_array_size_follows_keys(_) ->
    Ref = emqx_chan_stats:new(connection),
    Keys = emqx_chan_stats:keys(),
    ?assertEqual(length(Keys), maps:get(size, atomics:info(Ref))),
    ?assertEqual(length(Keys), length(lists:usort(Keys))),
    lists:foreach(fun(Key) -> ?assertEqual({ok, 0}, emqx_chan_stats:lookup(Ref, Key)) end, Keys).

-doc """
A new array reports every key of its layout with the value 0, and `inc/2`
changes the value a reader of the array sees.
""".
t_new_and_inc(_) ->
    Ref = emqx_chan_stats:new(connection),
    ?assertEqual(Ref, emqx_chan_stats:ref()),
    ?assertEqual([{Key, 0} || Key <- emqx_chan_stats:keys()], emqx_chan_stats:to_list(Ref)),
    ok = emqx_chan_stats:inc(recv_pkt, 1),
    ok = emqx_chan_stats:inc(recv_pkt, 2),
    ok = emqx_chan_stats:inc('send_msg.dropped.expired', 5),
    ?assertEqual(3, emqx_chan_stats:get(recv_pkt)),
    ?assertEqual(3, emqx_chan_stats:get(Ref, recv_pkt)),
    ?assertEqual({ok, 3}, emqx_chan_stats:lookup(Ref, recv_pkt)),
    ?assertEqual(
        #{recv_pkt => 3, 'send_msg.dropped.expired' => 5, send_pkt => 0},
        maps:with(
            [recv_pkt, 'send_msg.dropped.expired', send_pkt],
            maps:from_list(emqx_chan_stats:to_list(Ref))
        )
    ),
    ?assertEqual(
        [{recv_pkt, 3}, {send_pkt, 0}],
        emqx_chan_stats:get_counters([recv_pkt, send_pkt])
    ).

-doc """
`put/1` writes the given values; the keys a reader lists are the layout's, in
the fixed order of `keys/0`: socket, session, channel.
""".
t_put(_) ->
    Ref = emqx_chan_stats:new(connection),
    ok = emqx_chan_stats:put([{mqueue_len, 7}, {recv_oct, 100}]),
    ?assertEqual(
        [{K, V} || {K, V} <- emqx_chan_stats:to_list(Ref), V =/= 0],
        [{recv_oct, 100}, {mqueue_len, 7}]
    ),
    ?assertEqual(?SOCK_KEYS ++ ?SESSION_KEYS ++ ?CHANNEL_METRICS, emqx_chan_stats:keys()),
    ?assertEqual(emqx_chan_stats:keys(), [K || {K, _} <- emqx_chan_stats:to_list(Ref)]),
    ?assertEqual({ok, 7}, emqx_chan_stats:lookup(Ref, mqueue_len)),
    ok = emqx_chan_stats:put([{mqueue_len, 0}]),
    ?assertEqual({ok, 0}, emqx_chan_stats:lookup(Ref, mqueue_len)),
    %% A producer passes its sources as lists of pairs, without appending them.
    ok = emqx_chan_stats:put([[{recv_cnt, 3}], [], [{mqueue_len, 2}, {inflight_cnt, 1}]]),
    ?assertEqual(
        [{recv_oct, 100}, {recv_cnt, 3}, {inflight_cnt, 1}, {mqueue_len, 2}],
        [{K, V} || {K, V} <- emqx_chan_stats:to_list(Ref), V =/= 0]
    ),
    ?assertEqual(error, emqx_chan_stats:lookup(Ref, not_a_stat)).

-doc """
The layout of an array is the set of keys its owner reports: a WebSocket
connection has no `send_pend`, an evicted session has no socket. The slot of
a key outside the layout holds -1, so `get/2` reads -1, and neither
`lookup/2` nor `to_list/1` reports the key.
""".
t_layouts(_) ->
    Ws = emqx_chan_stats:new(ws_connection),
    ?assertEqual(-1, emqx_chan_stats:get(Ws, send_pend)),
    ?assertEqual(
        emqx_chan_stats:keys() -- [send_pend], [K || {K, _} <- emqx_chan_stats:to_list(Ws)]
    ),
    ok = emqx_chan_stats:put([{recv_oct, 1}]),
    ?assertEqual(error, emqx_chan_stats:lookup(Ws, send_pend)),
    ?assertEqual({ok, 1}, emqx_chan_stats:lookup(Ws, recv_oct)),
    Evicted = emqx_chan_stats:new(evicted_session),
    ?assertEqual(
        [-1, -1, -1, -1, -1],
        [emqx_chan_stats:get(Evicted, K) || K <- ?SOCK_KEYS]
    ),
    ?assertEqual(
        ?SESSION_KEYS ++ ?CHANNEL_METRICS, [K || {K, _} <- emqx_chan_stats:to_list(Evicted)]
    ),
    ?assertEqual(error, emqx_chan_stats:lookup(Evicted, recv_oct)),
    ?assertError(function_clause, emqx_chan_stats:new(gateway)).

-doc "Every value of a stats array is an integer, and only the keys of the array have a slot.".
t_put_integers_only(_) ->
    _ = emqx_chan_stats:new(connection),
    ?assertError(badarg, emqx_chan_stats:put([{mqueue_len, infinity}])),
    ?assertError(badarg, emqx_chan_stats:put([{mqueue_len, undefined}])),
    %% A session attribute that does not change with traffic is not a stat.
    ?assertError(
        {unknown_chan_stats_key, subscriptions_max},
        emqx_chan_stats:put([{subscriptions_max, infinity}])
    ),
    ?assertError({unknown_chan_stats_key, inflight_max}, emqx_chan_stats:put([{inflight_max, 32}])),
    ?assertError({unknown_chan_stats_key, durable}, emqx_chan_stats:put([{durable, 1}])),
    %% The process stats are read from the process, not stored.
    ?assertError({unknown_chan_stats_key, memory}, emqx_chan_stats:put([{memory, 4096}])),
    ok = emqx_chan_stats:put([{send_pend, 7}]),
    ?assertEqual({ok, 7}, emqx_chan_stats:lookup(emqx_chan_stats:ref(), send_pend)).

-doc """
A process without a stats array, such as a gateway connection that runs a
memory session, counts nothing and raises when it reads.
""".
t_no_array(_) ->
    ?assertEqual(undefined, erlang:erase(emqx_chan_stats)),
    ?assertEqual(ok, emqx_chan_stats:inc(recv_pkt, 1)),
    ?assertError(no_chan_stats, emqx_chan_stats:ref()),
    ?assertError(no_chan_stats, emqx_chan_stats:get(recv_pkt)),
    ?assertError(no_chan_stats, emqx_chan_stats:put([{recv_oct, 1}])).
