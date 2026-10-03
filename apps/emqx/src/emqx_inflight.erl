%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight).

-moduledoc """
Outgoing inflight window of the MQTT session, keyed by packet id. Gateway
channels that retransmit frames under other keys use `emqx_gateway_inflight`.

Entries are stored in a map. Integer keys in the MQTT packet id range
(1..65535) are also recorded in a sparse bitmap index: a map from chunk
number to a 32-bit integer of used ids, `Id bsr 5 => Bits`. A chunk whose
bits become zero is removed, so an absent chunk means all its ids are free.

The inflight also owns the packet id counter. `alloc/2` and `reserve/1`
return the first unused packet id at or after the counter, wrapping from
65535 to 1, and move the counter past it. `insert/3` with an explicit key
does not move the counter.

`new/1` returns a placeholder that holds only the size limit. The record
with the entries, the index and the counter is built on the first insert,
`alloc/2`, `reserve/1` or `set_next_id/2`, so a client that never
receives a QoS 1 or 2 message never pays for it.
""".

-compile(inline).

%% APIs
-export([
    new/0,
    new/1,
    contain/2,
    lookup/2,
    insert/3,
    update/3,
    resize/2,
    delete/2,
    fold/3,
    values/1,
    to_list/1,
    to_list/2,
    size/1,
    max_size/1,
    is_full/1,
    is_empty/1,
    alloc/2,
    reserve/1,
    next_id/1,
    set_next_id/2
]).

-export_type([inflight/0]).

-define(MAX_ID, 16#FFFF).
-define(CHUNK_SHIFT, 5).
-define(OFFSET_MASK, 31).
-define(CHUNK_MASK, 16#FFFFFFFF).
-define(LAST_CHUNK, (?MAX_ID bsr ?CHUNK_SHIFT)).
-define(NUM_CHUNKS, (?LAST_CHUNK + 1)).

-type max_size() :: non_neg_integer().

-type packet_id() :: 1..?MAX_ID.

%% Chunk number => bits of used packet ids. Zero chunks are never stored.
-type index() :: #{non_neg_integer() => pos_integer()}.

-record(inflight, {
    %% 0 means no limit.
    max_size :: max_size(),
    entries = #{} :: #{packet_id() => term()},
    %% Packet id keys of `entries`.
    index = #{} :: index(),
    %% Where the next search for an unused packet id starts.
    next_id = 1 :: packet_id()
}).

%% Placeholder returned by `new/1`, before any entry or counter change.
-define(EMPTY(MaxSize), {inflight, MaxSize}).

-opaque inflight() :: #inflight{} | ?EMPTY(max_size()).

-define(IS_PACKET_ID(Key), (is_integer(Key) andalso Key >= 1 andalso Key =< ?MAX_ID)).

-spec new() -> inflight().
new() -> new(0).

-spec new(non_neg_integer()) -> inflight().
new(MaxSize) when MaxSize >= 0 ->
    ?EMPTY(MaxSize).

-spec contain(packet_id(), inflight()) -> boolean().
contain(_Key, ?EMPTY(_)) ->
    false;
contain(Key, #inflight{entries = Entries}) ->
    is_map_key(Key, Entries).

-spec lookup(packet_id(), inflight()) -> {value, term()} | none.
lookup(_Key, ?EMPTY(_)) ->
    none;
lookup(Key, #inflight{entries = Entries}) ->
    case Entries of
        #{Key := Val} -> {value, Val};
        #{} -> none
    end.

-spec insert(packet_id(), Val :: term(), inflight()) -> inflight().
insert(Key, Val, I = ?EMPTY(_)) when ?IS_PACKET_ID(Key) ->
    insert(Key, Val, materialize(I));
insert(Key, _Val, #inflight{entries = Entries}) when is_map_key(Key, Entries) ->
    erlang:error({key_exists, Key});
insert(Key, Val, I = #inflight{entries = Entries, index = Index}) when ?IS_PACKET_ID(Key) ->
    I#inflight{entries = Entries#{Key => Val}, index = mark(Key, Index)}.

-spec delete(packet_id(), inflight()) -> inflight().
delete(Key, I = #inflight{entries = Entries, index = Index}) when is_map_key(Key, Entries) ->
    I#inflight{entries = maps:remove(Key, Entries), index = unmark(Key, Index)}.

-spec update(packet_id(), Val :: term(), inflight()) -> inflight().
update(Key, Val, I = #inflight{entries = Entries}) when is_map_key(Key, Entries) ->
    I#inflight{entries = Entries#{Key := Val}}.

-spec fold(fun((packet_id(), Val :: term(), Acc) -> Acc), Acc, inflight()) -> Acc.
fold(_FoldFun, AccIn, ?EMPTY(_)) ->
    AccIn;
fold(FoldFun, AccIn, #inflight{entries = Entries}) ->
    maps:fold(FoldFun, AccIn, Entries).

-spec resize(integer(), inflight()) -> inflight().
resize(MaxSize, ?EMPTY(_)) ->
    ?EMPTY(MaxSize);
resize(MaxSize, I = #inflight{}) ->
    I#inflight{max_size = MaxSize}.

-spec is_full(inflight()) -> boolean().
is_full(?EMPTY(_)) ->
    false;
is_full(#inflight{max_size = 0}) ->
    false;
is_full(#inflight{max_size = MaxSize, entries = Entries}) ->
    MaxSize =< map_size(Entries).

-spec is_empty(inflight()) -> boolean().
is_empty(?EMPTY(_)) ->
    true;
is_empty(#inflight{entries = Entries}) ->
    map_size(Entries) =:= 0.

-doc "Return the values, ordered by key.".
-spec values(inflight()) -> list().
values(Inflight) ->
    [Val || {_Key, Val} <- to_list(Inflight)].

-doc "Return the entries, ordered by key.".
-spec to_list(inflight()) -> list({packet_id(), term()}).
to_list(?EMPTY(_)) ->
    [];
to_list(#inflight{entries = Entries}) ->
    lists:keysort(1, maps:to_list(Entries)).

-spec to_list(fun(), inflight()) -> list({packet_id(), term()}).
to_list(_SortFun, ?EMPTY(_)) ->
    [];
to_list(SortFun, #inflight{entries = Entries}) ->
    lists:sort(SortFun, maps:to_list(Entries)).

-spec size(inflight()) -> non_neg_integer().
size(?EMPTY(_)) ->
    0;
size(#inflight{entries = Entries}) ->
    map_size(Entries).

-spec max_size(inflight()) -> non_neg_integer().
max_size(?EMPTY(MaxSize)) ->
    MaxSize;
max_size(#inflight{max_size = MaxSize}) ->
    MaxSize.

-doc """
Insert `Val` under the first unused packet id at or after the counter, and
move the counter past that id. Return `none` when all 65535 packet ids are
in use.
""".
-spec alloc(Val :: term(), inflight()) -> {ok, packet_id(), inflight()} | none.
alloc(Val, I = ?EMPTY(_)) ->
    alloc(Val, materialize(I));
alloc(Val, I = #inflight{entries = Entries, index = Index, next_id = NextId}) ->
    case next_free_id(NextId, Index) of
        {ok, Id} ->
            {ok, Id, I#inflight{
                entries = Entries#{Id => Val},
                index = mark(Id, Index),
                next_id = next(Id)
            }};
        none ->
            none
    end.

-doc """
Return the first unused packet id at or after the counter, and move the
counter past it, without inserting an entry. Return `none` when all 65535
packet ids are in use.
""".
-spec reserve(inflight()) -> {ok, packet_id(), inflight()} | none.
reserve(I = ?EMPTY(_)) ->
    reserve(materialize(I));
reserve(I = #inflight{index = Index, next_id = NextId}) ->
    case next_free_id(NextId, Index) of
        {ok, Id} -> {ok, Id, I#inflight{next_id = next(Id)}};
        none -> none
    end.

-doc "Return the packet id counter: where the next search for an unused id starts.".
-spec next_id(inflight()) -> packet_id().
next_id(?EMPTY(_)) ->
    1;
next_id(#inflight{next_id = NextId}) ->
    NextId.

-spec set_next_id(packet_id(), inflight()) -> inflight().
set_next_id(1, I = ?EMPTY(_)) ->
    I;
set_next_id(NextId, I = ?EMPTY(_)) ->
    set_next_id(NextId, materialize(I));
set_next_id(NextId, I = #inflight{}) when ?IS_PACKET_ID(NextId) ->
    I#inflight{next_id = NextId}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

materialize(?EMPTY(MaxSize)) ->
    #inflight{max_size = MaxSize}.

next_free_id(From, Index) ->
    Chunk = From bsr ?CHUNK_SHIFT,
    FromMask = ?CHUNK_MASK bxor ((1 bsl (From band ?OFFSET_MASK)) - 1),
    scan(Chunk, FromMask, ?NUM_CHUNKS, Index).

%% Steps counts the chunks left to visit after this one. The start chunk is
%% visited twice: first from the `From` offset, last in full, so the ids
%% below `From` in that chunk are checked after the wrap.
scan(Chunk, Mask, Steps, Index) ->
    %% 1 bits mean 'free' or 'unused'.
    Free = (used_bits(Chunk, Index) bxor ?CHUNK_MASK) band Mask,
    case Free of
        0 when Steps =:= 0 ->
            none;
        0 ->
            scan(next_chunk(Chunk), ?CHUNK_MASK, Steps - 1, Index);
        _ ->
            Offset = offset(Free),
            {ok, (Chunk bsl ?CHUNK_SHIFT) + Offset}
    end.

%% Packet id 0 is not valid, so chunk 0 always reports it as used.
used_bits(0, Index) ->
    maps:get(0, Index, 0) bor 1;
used_bits(Chunk, Index) ->
    maps:get(Chunk, Index, 0).

next(?MAX_ID) -> 1;
next(Id) -> Id + 1.

next_chunk(?LAST_CHUNK) -> 0;
next_chunk(Chunk) -> Chunk + 1.

mark(Key, Index) ->
    Chunk = Key bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Key band ?OFFSET_MASK),
    Index#{Chunk => maps:get(Chunk, Index, 0) bor Bit}.

%% A chunk is removed when its last bit is cleared, so that the index only
%% holds chunks with at least one used id.
unmark(Key, Index) ->
    Chunk = Key bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Key band ?OFFSET_MASK),
    case maps:get(Chunk, Index) band (bnot Bit) of
        0 -> maps:remove(Chunk, Index);
        Bits -> Index#{Chunk := Bits}
    end.

%% Offset of the lowest 1 bit.
%% Replace with a count-trailing-zeros BIF once OTP has one:
%% https://github.com/erlang/otp/issues/11757
offset(Free) ->
    tzc(Free band -Free).

%% Trailing zero-bit count of a single-bit integer.
%% Benchmarks show the 32-clause function is faster than bsr algorithms
%% (binary search, and a shift loop is worse still).
tzc(16#1) -> 0;
tzc(16#2) -> 1;
tzc(16#4) -> 2;
tzc(16#8) -> 3;
tzc(16#10) -> 4;
tzc(16#20) -> 5;
tzc(16#40) -> 6;
tzc(16#80) -> 7;
tzc(16#100) -> 8;
tzc(16#200) -> 9;
tzc(16#400) -> 10;
tzc(16#800) -> 11;
tzc(16#1000) -> 12;
tzc(16#2000) -> 13;
tzc(16#4000) -> 14;
tzc(16#8000) -> 15;
tzc(16#10000) -> 16;
tzc(16#20000) -> 17;
tzc(16#40000) -> 18;
tzc(16#80000) -> 19;
tzc(16#100000) -> 20;
tzc(16#200000) -> 21;
tzc(16#400000) -> 22;
tzc(16#800000) -> 23;
tzc(16#1000000) -> 24;
tzc(16#2000000) -> 25;
tzc(16#4000000) -> 26;
tzc(16#8000000) -> 27;
tzc(16#10000000) -> 28;
tzc(16#20000000) -> 29;
tzc(16#40000000) -> 30;
tzc(16#80000000) -> 31.
