%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_inflight).

-moduledoc """
Outgoing inflight window of the MQTT session, keyed by packet id. Gateway
channels that retransmit frames under other keys use `emqx_gateway_inflight`.

Entries are kept in a `gb_trees` ordered by packet id. The inflight also
owns the packet id counter: `alloc/2` hands out the counter value and moves
it on by one, wrapping from 65535 to 1. `insert/3` with an explicit key does
not move the counter.

When the counter lands on a packet id that is still in use, an
`emqx_packet_id_bitmap` of the used ids is built from the tree and the
counter skips to the first unused id. The bitmap is kept in sync with the
tree and dropped as soon as the counter is past the largest used id, since
no further clash is possible until the counter wraps again. It is never
dropped when the size limit is above 32767 or unlimited: a window that wide
clashes often, and rebuilding it on every clash would cost more than keeping
it.

`new/1` returns a placeholder that holds only the size limit. The record is
built on the first insert, `alloc/2` or `set_next_id/2`, so a client that
never receives a QoS 1 or 2 message never pays for it.
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
    next_id/1,
    set_next_id/2
]).

-export_type([inflight/0]).

-define(MAX_ID, 16#FFFF).

%% Above this size limit the bitmap is kept once built.
-define(KEEP_BITMAP_ABOVE, 32767).

-type max_size() :: non_neg_integer().

-type packet_id() :: emqx_packet_id_bitmap:packet_id().

%% The bitmap is not built until the counter lands on a used id.
-define(NO_BITMAP, lazy).

-record(inflight, {
    %% Where the next search for an unused packet id starts.
    next_id = 1 :: packet_id(),
    %% 0 means no limit.
    max_size :: max_size(),
    %% Used packet ids, present only while the counter is behind a used id.
    bitmap = ?NO_BITMAP :: ?NO_BITMAP | emqx_packet_id_bitmap:t(),
    entries = gb_trees:empty() :: gb_trees:tree(packet_id(), term())
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
    gb_trees:is_defined(Key, Entries).

-spec lookup(packet_id(), inflight()) -> {value, term()} | none.
lookup(_Key, ?EMPTY(_)) ->
    none;
lookup(Key, #inflight{entries = Entries}) ->
    gb_trees:lookup(Key, Entries).

-spec insert(packet_id(), Val :: term(), inflight()) -> inflight().
insert(Key, Val, I = ?EMPTY(_)) when ?IS_PACKET_ID(Key) ->
    insert(Key, Val, materialize(I));
insert(Key, Val, I = #inflight{entries = Entries, bitmap = Bitmap}) when ?IS_PACKET_ID(Key) ->
    I#inflight{
        entries = gb_trees:insert(Key, Val, Entries),
        bitmap = bitmap_set(Key, Bitmap)
    }.

-spec delete(packet_id(), inflight()) -> inflight().
delete(Key, I = #inflight{entries = Entries, bitmap = Bitmap}) ->
    maybe_drop_bitmap(I#inflight{
        entries = gb_trees:delete(Key, Entries),
        bitmap = bitmap_unset(Key, Bitmap)
    }).

-spec update(packet_id(), Val :: term(), inflight()) -> inflight().
update(Key, Val, I = #inflight{entries = Entries}) ->
    I#inflight{entries = gb_trees:update(Key, Val, Entries)}.

-spec fold(fun((packet_id(), Val :: term(), Acc) -> Acc), Acc, inflight()) -> Acc.
fold(_FoldFun, AccIn, ?EMPTY(_)) ->
    AccIn;
fold(FoldFun, AccIn, #inflight{entries = Entries}) ->
    fold_iterator(FoldFun, AccIn, gb_trees:iterator(Entries)).

fold_iterator(FoldFun, Acc, It) ->
    case gb_trees:next(It) of
        {Key, Val, ItNext} ->
            fold_iterator(FoldFun, FoldFun(Key, Val, Acc), ItNext);
        none ->
            Acc
    end.

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
    MaxSize =< gb_trees:size(Entries).

-spec is_empty(inflight()) -> boolean().
is_empty(?EMPTY(_)) ->
    true;
is_empty(#inflight{entries = Entries}) ->
    gb_trees:is_empty(Entries).

-doc "Return the values, ordered by key.".
-spec values(inflight()) -> list().
values(?EMPTY(_)) ->
    [];
values(#inflight{entries = Entries}) ->
    gb_trees:values(Entries).

-doc "Return the entries, ordered by key.".
-spec to_list(inflight()) -> list({packet_id(), term()}).
to_list(?EMPTY(_)) ->
    [];
to_list(#inflight{entries = Entries}) ->
    gb_trees:to_list(Entries).

-spec to_list(fun(), inflight()) -> list({packet_id(), term()}).
to_list(_SortFun, ?EMPTY(_)) ->
    [];
to_list(SortFun, #inflight{entries = Entries}) ->
    lists:sort(SortFun, gb_trees:to_list(Entries)).

-spec size(inflight()) -> non_neg_integer().
size(?EMPTY(_)) ->
    0;
size(#inflight{entries = Entries}) ->
    gb_trees:size(Entries).

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
alloc(Val, I = #inflight{entries = Entries}) ->
    case take_next_id(I) of
        {ok, Id, Bitmap} ->
            I1 = I#inflight{
                entries = gb_trees:insert(Id, Val, Entries),
                bitmap = bitmap_set(Id, Bitmap),
                next_id = next(Id)
            },
            {ok, Id, maybe_drop_bitmap(I1)};
        none ->
            none
    end.

-doc "Return the packet id counter: where the next search for an unused id starts.".
-spec next_id(inflight()) -> packet_id().
next_id(?EMPTY(_)) ->
    1;
next_id(#inflight{next_id = NextId}) ->
    NextId.

-doc """
Set the packet id counter. The session uses it to restore the counter of a
session imported from another node after a takeover.
""".
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

%% Find the id to hand out, and the bitmap to continue with: `?NO_BITMAP`
%% while the counter is not on a used id, the used-id bitmap otherwise.
take_next_id(#inflight{entries = Entries, bitmap = ?NO_BITMAP, next_id = NextId}) ->
    case gb_trees:is_defined(NextId, Entries) of
        false ->
            {ok, NextId, ?NO_BITMAP};
        true ->
            Bitmap = emqx_packet_id_bitmap:from_list(gb_trees:keys(Entries)),
            take_next_id_from_bitmap(NextId, Bitmap)
    end;
take_next_id(#inflight{bitmap = Bitmap, next_id = NextId}) ->
    take_next_id_from_bitmap(NextId, Bitmap).

take_next_id_from_bitmap(NextId, Bitmap) ->
    case emqx_packet_id_bitmap:next_free(NextId, Bitmap) of
        {ok, Id} -> {ok, Id, Bitmap};
        none -> none
    end.

bitmap_set(_Id, ?NO_BITMAP) -> ?NO_BITMAP;
bitmap_set(Id, Bitmap) -> emqx_packet_id_bitmap:set(Id, Bitmap).

bitmap_unset(_Id, ?NO_BITMAP) -> ?NO_BITMAP;
bitmap_unset(Id, Bitmap) -> emqx_packet_id_bitmap:unset(Id, Bitmap).

%% Drop the bitmap once the counter is past every used id: the counter then
%% cannot land on a used id before it wraps. Keep it for a wide window.
maybe_drop_bitmap(I = #inflight{bitmap = ?NO_BITMAP}) ->
    I;
maybe_drop_bitmap(I = #inflight{max_size = MaxSize}) when
    MaxSize =:= 0; MaxSize > ?KEEP_BITMAP_ABOVE
->
    I;
maybe_drop_bitmap(I = #inflight{entries = Entries, next_id = NextId}) ->
    case gb_trees:is_empty(Entries) orelse NextId > element(1, gb_trees:largest(Entries)) of
        true -> I#inflight{bitmap = ?NO_BITMAP};
        false -> I
    end.

next(?MAX_ID) -> 1;
next(Id) -> Id + 1.
