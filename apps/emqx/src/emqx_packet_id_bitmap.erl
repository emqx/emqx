%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_packet_id_bitmap).

-moduledoc """
Set of MQTT packet ids (1..65535) that answers "first unused id at or after
X", wrapping from 65535 to 1.

The set is a sparse bitmap: a map from chunk number to a 32-bit integer of
used ids, `Id bsr 5 => Bits`. A chunk whose bits become zero is removed, so
an absent chunk means all its ids are unused. Packet id 0 is not valid and
is never returned.
""".

-export([
    new/0,
    from_list/1,
    set/2,
    unset/2,
    next_free/2
]).

-export_type([t/0, packet_id/0]).

-define(MAX_ID, 16#FFFF).
-define(CHUNK_SHIFT, 5).
-define(OFFSET_MASK, 31).
-define(CHUNK_MASK, 16#FFFFFFFF).
-define(LAST_CHUNK, (?MAX_ID bsr ?CHUNK_SHIFT)).
-define(NUM_CHUNKS, (?LAST_CHUNK + 1)).

-type packet_id() :: 1..?MAX_ID.

%% Chunk number => bits of used packet ids. Zero chunks are never stored.
-opaque t() :: #{non_neg_integer() => pos_integer()}.

-spec new() -> t().
new() ->
    #{}.

-spec from_list([packet_id()]) -> t().
from_list(Ids) ->
    lists:foldl(fun set/2, new(), Ids).

-doc "Mark `Id` as used.".
-spec set(packet_id(), t()) -> t().
set(Id, Bitmap) ->
    Chunk = Id bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Id band ?OFFSET_MASK),
    Bitmap#{Chunk => maps:get(Chunk, Bitmap, 0) bor Bit}.

-doc """
Mark `Id` as unused. The chunk is removed when its last bit is cleared, so
that the bitmap only holds chunks with at least one used id.
""".
-spec unset(packet_id(), t()) -> t().
unset(Id, Bitmap) ->
    Chunk = Id bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Id band ?OFFSET_MASK),
    case maps:get(Chunk, Bitmap) band (bnot Bit) of
        0 -> maps:remove(Chunk, Bitmap);
        Bits -> Bitmap#{Chunk := Bits}
    end.

-doc """
Return the first unused packet id at or after `From`, wrapping from 65535
to 1. Return `none` when all 65535 packet ids are used.
""".
-spec next_free(packet_id(), t()) -> {ok, packet_id()} | none.
next_free(From, Bitmap) ->
    Chunk = From bsr ?CHUNK_SHIFT,
    FromMask = ?CHUNK_MASK bxor ((1 bsl (From band ?OFFSET_MASK)) - 1),
    scan(Chunk, FromMask, ?NUM_CHUNKS, Bitmap).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Steps counts the chunks left to visit after this one. The start chunk is
%% visited twice: first from the `From` offset, last in full, so the ids
%% below `From` in that chunk are checked after the wrap.
scan(Chunk, Mask, Steps, Bitmap) ->
    %% 1 bits mean 'free' or 'unused'.
    Free = (used_bits(Chunk, Bitmap) bxor ?CHUNK_MASK) band Mask,
    case Free of
        0 when Steps =:= 0 ->
            none;
        0 ->
            scan(next_chunk(Chunk), ?CHUNK_MASK, Steps - 1, Bitmap);
        _ ->
            Offset = offset(Free),
            {ok, (Chunk bsl ?CHUNK_SHIFT) + Offset}
    end.

%% Packet id 0 is not valid, so chunk 0 always reports it as used.
used_bits(0, Bitmap) ->
    maps:get(0, Bitmap, 0) bor 1;
used_bits(Chunk, Bitmap) ->
    maps:get(Chunk, Bitmap, 0).

next_chunk(?LAST_CHUNK) -> 0;
next_chunk(Chunk) -> Chunk + 1.

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
