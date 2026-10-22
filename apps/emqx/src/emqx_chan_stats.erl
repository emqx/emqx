%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_chan_stats).

-moduledoc """
Per-connection stats in an `atomics` array that the channel process owns.

The channel process creates the array with `new/1`, which also records the
reference in the process dictionary, so that the connection module, the
channel and the session code count without passing the reference around.
The `emqx_channel_info` table row holds the reference, and a reader on the
same node builds the stats proplist from it with `to_list/1`.

The packet and message counters (`?CHANNEL_METRICS`) are added in place with
`inc/2`, so a reader sees them as they happen. The socket and session gauges
are written by the owner with `put/1`, on its stats timer and when the
session changes. The layout given to `new/1` is the set of keys the owner
reports: a `connection` reports every key, a `ws_connection` has no
`send_pend`, and an `evicted_session` has no socket at all. The slots of the
other keys hold `-1`, which no counter or gauge can take, and readers leave
them out. The process stats (`mailbox_len`, `heap_size`, ...) are not in the
array: a reader on the same node takes them from the process with
`emqx_utils:proc_stats/1`.

Every slot holds a counter or a gauge. The session attributes that do not
change with traffic, such as `durable`, `subscriptions_max` or `inflight_max`,
are not stats: they live in the session entry of the info map of the row (see
`?CHAN_INFO_SESSION_KEYS` and `emqx_session:chan_info/1`).
""".

-include("emqx_channel.hrl").

%% Owner process
-export([
    new/1,
    ref/0,
    inc/2,
    get/1,
    put/1,
    get_counters/1
]).

%% Readers
-export([
    to_list/1,
    get/2,
    lookup/2
]).

-export([keys/0]).

-export_type([ref/0, key/0, layout/0, stats/0]).

-type ref() :: atomics:atomics_ref().
-type key() :: atom().
-type layout() :: connection | ws_connection | evicted_session.
%% Stats to write: pairs, or lists of pairs, so that a producer with several
%% sources passes them as they are, without appending the lists first.
-type stats() :: [{key(), integer()} | stats()].

-define(PD_KEY, emqx_chan_stats).
%% The value of a slot whose key is not in the layout of the array.
-define(ABSENT, -1).

-define(SOCK_KEYS, [
    recv_oct,
    recv_cnt,
    send_oct,
    send_cnt,
    send_pend
]).

-define(SESSION_KEYS, [
    subscriptions_cnt,
    inflight_cnt,
    mqueue_len,
    total_payload_bytes,
    mqueue_dropped,
    awaiting_rel_cnt
]).

%% The order is the order of `to_list/1`: socket, session, channel. The
%% socket and session stats keep the order their producers use, and
%% `?CHANNEL_METRICS` keeps its own, so the proplist of a reader lists the
%% keys in the order `emqx_connection:stats/1` does, before the process
%% stats that a reader appends.
-define(KEYS, (?SOCK_KEYS ++ ?SESSION_KEYS ++ ?CHANNEL_METRICS)).
%% The size of the array: `length(?KEYS)`. `emqx_chan_stats_SUITE` checks
%% that the two agree. A hot patch cannot add a key: the arrays of the
%% running channels keep their size across a code load, so a reader of the
%% new code would index past the end of an old array. A new key needs a
%% release upgrade.
-define(N_KEYS, 27).

%%--------------------------------------------------------------------
%% Owner process
%%--------------------------------------------------------------------

-doc """
Create the stats array of the calling process with the given layout and
record its reference in the process dictionary. The keys outside the layout
are marked absent.
""".
-spec new(layout()) -> ref().
new(Layout) ->
    Ref = atomics:new(?N_KEYS, []),
    lists:foreach(fun(Key) -> ok = atomics:put(Ref, slot(Key), ?ABSENT) end, absent_keys(Layout)),
    _ = erlang:put(?PD_KEY, Ref),
    Ref.

-doc "The stats array of the calling process. Raises when the process has none.".
-spec ref() -> ref().
ref() ->
    case erlang:get(?PD_KEY) of
        undefined -> error(no_chan_stats);
        Ref -> Ref
    end.

-doc """
Add `Inc` to the counter `Key` of the calling process. A process without a
stats array, such as a gateway connection or a QUIC data stream that runs
session code, has no per-connection stats to keep: the call does nothing.
""".
-spec inc(key(), integer()) -> ok.
inc(Key, Inc) ->
    case erlang:get(?PD_KEY) of
        undefined -> ok;
        Ref -> atomics:add(Ref, slot(Key), Inc)
    end.

-doc "The value of `Key` in the stats array of the calling process.".
-spec get(key()) -> integer().
get(Key) ->
    atomics:get(ref(), slot(Key)).

-doc """
Write the given stats to the stats array of the calling process: pairs, or
lists of pairs. Every value must be an integer.
""".
-spec put(stats()) -> ok.
put(Stats) ->
    put_stats(ref(), Stats).

put_stats(Ref, [{Key, Value} | Rest]) ->
    ok = atomics:put(Ref, slot(Key), Value),
    put_stats(Ref, Rest);
put_stats(Ref, [Stats | Rest]) when is_list(Stats) ->
    ok = put_stats(Ref, Stats),
    put_stats(Ref, Rest);
put_stats(_Ref, []) ->
    ok.

-doc "The given keys and their values in the stats array of the calling process.".
-spec get_counters([key()]) -> [{key(), integer()}].
get_counters(Keys) ->
    Ref = ref(),
    [{Key, atomics:get(Ref, slot(Key))} || Key <- Keys].

%%--------------------------------------------------------------------
%% Readers
%%--------------------------------------------------------------------

-doc "The keys of the layout of a stats array and their values, in the order of `keys/0`.".
-spec to_list(ref()) -> [{key(), integer()}].
to_list(Ref) ->
    to_list(?KEYS, 1, Ref).

to_list([], _Slot, _Ref) ->
    [];
to_list([Key | Keys], Slot, Ref) ->
    case atomics:get(Ref, Slot) of
        ?ABSENT -> to_list(Keys, Slot + 1, Ref);
        Value -> [{Key, Value} | to_list(Keys, Slot + 1, Ref)]
    end.

-doc "The value of `Key` in a stats array; `-1` when `Key` is not in its layout.".
-spec get(ref(), key()) -> integer().
get(Ref, Key) ->
    atomics:get(Ref, slot(Key)).

-doc "The value of `Key` in a stats array when `Key` is in its layout.".
-spec lookup(ref(), key()) -> {ok, integer()} | error.
lookup(Ref, Key) ->
    try atomics:get(Ref, slot(Key)) of
        ?ABSENT -> error;
        Value -> {ok, Value}
    catch
        error:{unknown_chan_stats_key, _} -> error
    end.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

-doc "Every key a stats array can hold, in the order of `to_list/1`.".
-spec keys() -> [key()].
keys() ->
    ?KEYS.

absent_keys(connection) -> [];
absent_keys(ws_connection) -> [send_pend];
absent_keys(evicted_session) -> ?SOCK_KEYS.

%% One clause per key of ?KEYS, in the same order, so that a literal key
%% folds to a constant at compile time where the function is inlined.
-compile({inline, [slot/1]}).
slot(recv_oct) -> 1;
slot(recv_cnt) -> 2;
slot(send_oct) -> 3;
slot(send_cnt) -> 4;
slot(send_pend) -> 5;
slot(subscriptions_cnt) -> 6;
slot(inflight_cnt) -> 7;
slot(mqueue_len) -> 8;
slot(total_payload_bytes) -> 9;
slot(mqueue_dropped) -> 10;
slot(awaiting_rel_cnt) -> 11;
slot(recv_pkt) -> 12;
slot(recv_msg) -> 13;
slot('recv_msg.qos0') -> 14;
slot('recv_msg.qos1') -> 15;
slot('recv_msg.qos2') -> 16;
slot('recv_msg.dropped') -> 17;
slot('recv_msg.dropped.await_pubrel_timeout') -> 18;
slot(send_pkt) -> 19;
slot(send_msg) -> 20;
slot('send_msg.qos0') -> 21;
slot('send_msg.qos1') -> 22;
slot('send_msg.qos2') -> 23;
slot('send_msg.dropped') -> 24;
slot('send_msg.dropped.expired') -> 25;
slot('send_msg.dropped.queue_full') -> 26;
slot('send_msg.dropped.too_large') -> 27;
slot(Key) -> error({unknown_chan_stats_key, Key}).
