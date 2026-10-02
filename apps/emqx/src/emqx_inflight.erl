%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight).

-moduledoc """
Inflight window keyed by packet id.

Entries are stored in a map. Integer keys in the MQTT packet id range
(1..65535) are also recorded in a sparse bitmap index: a map from chunk
number to a 32-bit integer of used ids, `Id bsr 5 => Bits`. A chunk whose
bits become zero is removed, so an absent chunk means all its ids are free.
`next_free_id/2` scans this index to find an unused packet id.

Keys outside the packet id range (gateways use other terms) are stored in
the map only.
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
    window/1,
    next_free_id/2
]).

-export_type([inflight/0]).

-define(MAX_ID, 16#FFFF).
-define(CHUNK_SHIFT, 5).
-define(OFFSET_MASK, 31).
-define(CHUNK_MASK, 16#FFFFFFFF).
-define(LAST_CHUNK, (?MAX_ID bsr ?CHUNK_SHIFT)).
-define(NUM_CHUNKS, (?LAST_CHUNK + 1)).

-type key() :: term().

-type max_size() :: pos_integer().

-type packet_id() :: 1..?MAX_ID.

%% Chunk number => bits of used packet ids. Zero chunks are never stored.
-type index() :: #{non_neg_integer() => pos_integer()}.

-opaque inflight() :: {inflight, max_size(), #{key() => term()}, index()}.

-define(IS_PACKET_ID(Key), (is_integer(Key) andalso Key >= 1 andalso Key =< ?MAX_ID)).

-define(INFLIGHT(Map), {inflight, _MaxSize, Map, _Index}).

-define(INFLIGHT(MaxSize, Map, Index), {inflight, MaxSize, (Map), (Index)}).

-spec new() -> inflight().
new() -> new(0).

-spec new(non_neg_integer()) -> inflight().
new(MaxSize) when MaxSize >= 0 ->
    ?INFLIGHT(MaxSize, #{}, #{}).

-spec contain(key(), inflight()) -> boolean().
contain(Key, ?INFLIGHT(Map)) ->
    is_map_key(Key, Map).

-spec lookup(key(), inflight()) -> {value, term()} | none.
lookup(Key, ?INFLIGHT(Map)) ->
    case Map of
        #{Key := Val} -> {value, Val};
        #{} -> none
    end.

-spec insert(key(), Val :: term(), inflight()) -> inflight().
insert(Key, _Val, ?INFLIGHT(Map)) when is_map_key(Key, Map) ->
    erlang:error({key_exists, Key});
insert(Key, Val, ?INFLIGHT(MaxSize, Map, Index)) ->
    ?INFLIGHT(MaxSize, Map#{Key => Val}, mark(Key, Index)).

-spec delete(key(), inflight()) -> inflight().
delete(Key, ?INFLIGHT(MaxSize, Map, Index)) when is_map_key(Key, Map) ->
    ?INFLIGHT(MaxSize, maps:remove(Key, Map), unmark(Key, Index)).

-spec update(key(), Val :: term(), inflight()) -> inflight().
update(Key, Val, ?INFLIGHT(MaxSize, Map, Index)) when is_map_key(Key, Map) ->
    ?INFLIGHT(MaxSize, Map#{Key := Val}, Index).

-spec fold(fun((key(), Val :: term(), Acc) -> Acc), Acc, inflight()) -> Acc.
fold(FoldFun, AccIn, ?INFLIGHT(Map)) ->
    maps:fold(FoldFun, AccIn, Map).

-spec resize(integer(), inflight()) -> inflight().
resize(MaxSize, ?INFLIGHT(_, Map, Index)) ->
    ?INFLIGHT(MaxSize, Map, Index).

-spec is_full(inflight()) -> boolean().
is_full(?INFLIGHT(0, _Map, _Index)) ->
    false;
is_full(?INFLIGHT(MaxSize, Map, _Index)) ->
    MaxSize =< map_size(Map).

-spec is_empty(inflight()) -> boolean().
is_empty(?INFLIGHT(Map)) ->
    map_size(Map) =:= 0.

-doc "Return the values, ordered by key.".
-spec values(inflight()) -> list().
values(Inflight) ->
    [Val || {_Key, Val} <- to_list(Inflight)].

-doc "Return the entries, ordered by key.".
-spec to_list(inflight()) -> list({key(), term()}).
to_list(?INFLIGHT(Map)) ->
    lists:sort(maps:to_list(Map)).

-spec to_list(fun(), inflight()) -> list({key(), term()}).
to_list(SortFun, ?INFLIGHT(Map)) ->
    lists:sort(SortFun, maps:to_list(Map)).

-doc "Return the smallest and the largest key, or `[]` when empty.".
-spec window(inflight()) -> list().
window(?INFLIGHT(Map)) when map_size(Map) =:= 0 ->
    [];
window(?INFLIGHT(Map)) ->
    Keys = maps:keys(Map),
    [lists:min(Keys), lists:max(Keys)].

-spec size(inflight()) -> non_neg_integer().
size(?INFLIGHT(Map)) ->
    map_size(Map).

-spec max_size(inflight()) -> non_neg_integer().
max_size(?INFLIGHT(MaxSize, _Map, _Index)) ->
    MaxSize.

-doc """
Return the first packet id at or after `From` that is not in the inflight,
wrapping from 65535 to 1. Return `none` when all 65535 packet ids are in use.
""".
-spec next_free_id(packet_id(), inflight()) -> {ok, packet_id()} | none.
next_free_id(From, ?INFLIGHT(_MaxSize, _Map, Index)) when ?IS_PACKET_ID(From) ->
    Chunk = From bsr ?CHUNK_SHIFT,
    FromMask = (?CHUNK_MASK bsl (From band ?OFFSET_MASK)) band ?CHUNK_MASK,
    scan(Chunk, FromMask, ?NUM_CHUNKS, Index).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% Steps counts the chunks left to visit after this one. The start chunk is
%% visited twice: first from the `From` offset, last in full, so the ids
%% below `From` in that chunk are checked after the wrap.
scan(Chunk, Mask, Steps, Index) ->
    Free = (used_bits(Chunk, Index) bxor ?CHUNK_MASK) band Mask,
    case Free of
        0 when Steps =:= 0 ->
            none;
        0 ->
            scan(next_chunk(Chunk), ?CHUNK_MASK, Steps - 1, Index);
        _ ->
            Lowest = Free band -Free,
            {ok, (Chunk bsl ?CHUNK_SHIFT) bor bit_offset(Lowest)}
    end.

%% Packet id 0 is not valid, so chunk 0 always reports it as used.
used_bits(0, Index) ->
    maps:get(0, Index, 0) bor 1;
used_bits(Chunk, Index) ->
    maps:get(Chunk, Index, 0).

next_chunk(?LAST_CHUNK) -> 0;
next_chunk(Chunk) -> Chunk + 1.

mark(Key, Index) when ?IS_PACKET_ID(Key) ->
    Chunk = Key bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Key band ?OFFSET_MASK),
    Index#{Chunk => maps:get(Chunk, Index, 0) bor Bit};
mark(_Key, Index) ->
    Index.

%% A chunk is removed when its last bit is cleared, so that the index only
%% holds chunks with at least one used id.
unmark(Key, Index) when ?IS_PACKET_ID(Key) ->
    Chunk = Key bsr ?CHUNK_SHIFT,
    Bit = 1 bsl (Key band ?OFFSET_MASK),
    case maps:get(Chunk, Index) band (bnot Bit) of
        0 -> maps:remove(Chunk, Index);
        Bits -> Index#{Chunk := Bits}
    end;
unmark(_Key, Index) ->
    Index.

%% Map a single-bit integer to its bit position.
bit_offset(16#1) -> 0;
bit_offset(16#2) -> 1;
bit_offset(16#4) -> 2;
bit_offset(16#8) -> 3;
bit_offset(16#10) -> 4;
bit_offset(16#20) -> 5;
bit_offset(16#40) -> 6;
bit_offset(16#80) -> 7;
bit_offset(16#100) -> 8;
bit_offset(16#200) -> 9;
bit_offset(16#400) -> 10;
bit_offset(16#800) -> 11;
bit_offset(16#1000) -> 12;
bit_offset(16#2000) -> 13;
bit_offset(16#4000) -> 14;
bit_offset(16#8000) -> 15;
bit_offset(16#10000) -> 16;
bit_offset(16#20000) -> 17;
bit_offset(16#40000) -> 18;
bit_offset(16#80000) -> 19;
bit_offset(16#100000) -> 20;
bit_offset(16#200000) -> 21;
bit_offset(16#400000) -> 22;
bit_offset(16#800000) -> 23;
bit_offset(16#1000000) -> 24;
bit_offset(16#2000000) -> 25;
bit_offset(16#4000000) -> 26;
bit_offset(16#8000000) -> 27;
bit_offset(16#10000000) -> 28;
bit_offset(16#20000000) -> 29;
bit_offset(16#40000000) -> 30;
bit_offset(16#80000000) -> 31.
