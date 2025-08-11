%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_pmap).
-moduledoc """
This helper module implements a thin abstraction layer that allows
using DS to store state of a process as a collection of key-value
maps (and optionally other data).

A pmap (short for persistent map) is a persistent key-value map that
has a RAM cache. Pmaps can accumulate updates in RAM, that can be
later persisted and restored in a transaction. All keys and values in
the pmap are of the same type and binary encoding.

Each key-value pair of a pmap is kept in DS individually, reducing
write amplification from the updates.

Pmaps of different types can be grouped together into "collections".
Consistency is guaranteed at the level of an individual collection.

## Guards

A collection is protected from write conflicts using a guard
mechanism. A guard is a token stored together with the collection,
which is updated when a new process takes ownership of a collection.

Taking ownership of a collection is done using using two transactions.
The first one reads the data and the old guard. Then the owner process
can make changes to the cached data. The second transaction that
commits the changes must contain the following operations:

```
tx_assert_guard(Id, OldGuard),
tx_assert_guard(Id, ?ds_tx_serial)
```

to ensure that data hasn't changed between the two transactions and to
establish the new ownership. Then the owner must keep id of the second
transaction as a the new guard and call `tx_assert_guard(Id, Guard)`
in the subsequent transactions to make the collection hasn't been
hijacked by other processes.

Note that this mechanism only detects ownership change post-factum and
prevents data corruption, but doesn't _lock out_ other processes from
taking over the collection. Such locking is out of scope of this module.
""".

%% API:
-export([
    storage_opts/1,
    lts_threshold_cb/2,

    new_pmap/2,
    tx_restore/3,
    tx_commit/2,
    tx_destroy/2,
    dirty_read/4,

    get/2,
    put/3,
    del/3,
    fold/3,
    size/1,

    collection_get/3,
    collection_fold/4,
    collection_put/4,
    collection_del/3,
    collection_size/2,
    collection_check_sequence/1,

    tx_guard/1,
    tx_assert_guard/2,
    tx_write_guard/2,
    tx_delete_guard/1
]).

%% internal exports:
-export([
    pmap_topic/3
]).

-ifdef(CHECK_SEQNO).
-export([do_seqno/0]).
-endif.

-export_type([
    pmap/2,
    collection_id/0,
    guard/0,
    collection/0
]).

-compile({no_auto_import, [{size, 1}]}).

-include("emqx_ds.hrl").
-include("emqx_ds_pmap.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-opaque pmap(K, V) ::
    #pmap{
        name :: binary(),
        module :: module(),
        cache :: #{K => V},
        dirty :: #{K => dirty | {del, emqx_ds:topic()}}
    }.

-type collection_id() :: binary().

-type field() :: atom().

-type collection() :: #{
    id := collection_id(),
    ?collection_guard := guard() | undefined,
    ?collection_dirty := boolean(),
    field() => pmap(_Key, _Value),
    _ => _
}.

-doc """
Guard is used as token of ownership.

API consumers should keep the guard obtained while opening the
collection in the cache and use `tx_assert_guard` function to verify
that persistent representation of the collection didn't deviate from
the cached one.
""".
-type guard() :: binary().

-doc """
Unique identifier of the pmap that is used as part of a DS topic.
""".
-type pmap_name() :: binary().

-doc """
Serialized representation of the pmap key.
It's used as part of the DS topic.
""".
-type key_bin() :: binary().

-doc """
Serialized representation of the value.
""".
-type val_bin() :: binary().

%%================================================================================
%% Callbacks
%%================================================================================

-callback pmap_encode_key(pmap_name(), _Key, _Val) -> key_bin().

-callback pmap_decode_key(pmap_name(), key_bin()) -> _Key.

-callback pmap_encode_val(pmap_name(), _Key, _Val) -> val_bin().

-callback pmap_decode_val(pmap_name(), _Key, val_bin()) -> _Val.

%%================================================================================
%% API functions
%%================================================================================

storage_opts(UserOpts) ->
    {emqx_ds_storage_skipstream_lts_v2,
        maps:merge(
            #{
                timestamp_bytes => 0,
                lts_threshold_spec => {mf, ?MODULE, lts_threshold_cb}
            },
            UserOpts
        )}.

-doc """
Learned topic trie threshold callback.
""".
lts_threshold_cb(0, _Parent) ->
    %% Don't create a common stream for topics adjacent to the root:
    infinity;
lts_threshold_cb(_, ?top_guard) ->
    %% Always create a unified stream for session guards, since
    %% iteration over guards is used e.g. to enumerate sessions:
    0;
lts_threshold_cb(N, ?top_data) ->
    %% [<<"d">>, CollectionId, PmapName, PmapKey]
    %%    0          1         2        3
    case N of
        1 ->
            %% Create a unified stream for pmaps' roots. Even though
            %% we never iterate over pmaps that belong to different
            %% sessions, all LTS nodes are kept in RAM at all time. So
            %% if we don't unify the sessions' data, we'll end up with
            %% a number of objects per session, dead or alive, stuck
            %% in RAM.
            0;
        2 ->
            %% Don't unify the streams that belong to different pmaps:
            infinity;
        _ ->
            %% Unify stream for the pmap keys:
            0
    end;
lts_threshold_cb(_, _) ->
    infinity.

-spec new_pmap(module(), binary()) -> pmap(_K, _V).
new_pmap(CBM, Name) when is_atom(CBM), is_binary(Name) ->
    #pmap{name = Name, module = CBM}.

-doc """
Destroy all persistent data stored in a pmap.

This function should be called in a transaction context.
""".
-spec tx_destroy(collection_id(), pmap(_, _) | pmap_name()) -> ok.
tx_destroy(CollectionId, #pmap{name = Name}) ->
    tx_destroy(CollectionId, Name);
tx_destroy(CollectionId, Name) when is_binary(Name) ->
    emqx_ds:tx_del_topic(pmap_topic(Name, CollectionId, '+')).

-doc """
Dump changed data from the pmap to the durable storage.

This function should be called in a transaction context.
""".
-spec tx_commit(collection_id(), pmap(K, V)) -> pmap(K, V).
tx_commit(
    CollectionId, Pmap = #pmap{name = Name, module = CBM, dirty = Dirty, cache = Cache}
) when is_binary(CollectionId) ->
    maps:foreach(
        fun
            (_Key, {del, Topic}) ->
                emqx_ds:tx_del_topic(Topic);
            (Key, dirty) ->
                #{Key := Val} = Cache,
                tx_write_pmap_kv(CBM, Name, CollectionId, Key, Val)
        end,
        Dirty
    ),
    Pmap#pmap{
        dirty = #{}
    }.

-doc """
Read data from a durable storage to RAM, creating a new pmap.

This function should be called in a transaction context.
""".
-spec tx_restore(module(), pmap_name(), collection_id()) -> pmap(_, _).
tx_restore(CBM, Name, CollectionId) when is_atom(CBM), is_binary(Name), is_binary(CollectionId) ->
    Cache = emqx_ds:tx_fold_topic(
        fun(_Slab, _Stream, Payload, Acc) ->
            restore_fun(CBM, Payload, Acc)
        end,
        #{},
        pmap_topic(Name, CollectionId, '+')
    ),
    #pmap{
        name = Name,
        module = CBM,
        cache = Cache
    }.

-doc """
Read data from a durable storage to RAM, creating a regular map.

This function doesn't require a transaction context.
""".
-spec dirty_read(module(), pmap_name(), collection_id(), emqx_ds:fold_options()) ->
    map().
dirty_read(CBM, Name, CollectionId, FoldOptions) when
    is_atom(CBM), is_binary(CollectionId), is_map(FoldOptions)
->
    emqx_ds:fold_topic(
        fun(_Slab, _Stream, Payload, Acc) ->
            restore_fun(CBM, Payload, Acc)
        end,
        #{},
        pmap_topic(Name, CollectionId, '+'),
        FoldOptions
    ).

%% == Operations on the cache ==

-spec get(K, pmap(K, V)) -> V | undefined.
get(Key, #pmap{cache = Cache}) ->
    maps:get(Key, Cache, undefined).

-spec put(K, V, pmap(K, V)) -> pmap(K, V).
put(
    Key,
    Val,
    Pmap = #pmap{
        dirty = Dirty, cache = Cache
    }
) ->
    Pmap#pmap{
        cache = Cache#{Key => Val},
        dirty = Dirty#{Key => dirty}
    }.

-spec del(collection_id(), K, pmap(K, V)) -> pmap(K, V).
del(
    CollectionId,
    Key,
    Pmap = #pmap{
        name = Name,
        module = CBM,
        dirty = Dirty,
        cache = Cache0
    }
) ->
    case maps:take(Key, Cache0) of
        {Val, Cache} ->
            Topic = pmap_topic(Name, CollectionId, encode_key(CBM, Name, Key, Val)),
            Pmap#pmap{
                cache = Cache,
                dirty = Dirty#{Key => {del, Topic}}
            };
        error ->
            Pmap
    end.

-spec fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
fold(Fun, Acc, #pmap{cache = Cache}) ->
    maps:fold(Fun, Acc, Cache).

-spec size(pmap(_K, _V)) -> non_neg_integer().
size(#pmap{cache = Cache}) ->
    maps:size(Cache).

%% == Operations with the pmap collections ==

-spec collection_get(field(), _Key, collection()) -> _Val | undefined.
collection_get(Field, Key, Rec) ->
    collection_check_sequence(Rec),
    get(Key, maps:get(Field, Rec)).

-spec collection_fold(field(), fun((_K, _V, A) -> A), A, collection()) -> A.
collection_fold(Field, Fun, Acc, Rec) ->
    collection_check_sequence(Rec),
    fold(Fun, Acc, maps:get(Field, Rec)).

-spec collection_put(field(), _Key, _Val, collection()) -> collection().
collection_put(Field, Key, Val, Rec) ->
    collection_check_sequence(Rec),
    #{Field := Pmap} = Rec,
    Rec#{
        Field := put(Key, Val, Pmap),
        ?set_dirty
    }.

-spec collection_del(field(), _Key, collection()) -> collection().
collection_del(Field, Key, Rec) ->
    collection_check_sequence(Rec),
    #{id := SessionId, Field := PMap0} = Rec,
    PMap = del(SessionId, Key, PMap0),
    Rec#{
        Field := PMap,
        ?set_dirty
    }.

-spec collection_size(field(), collection()) -> non_neg_integer().
collection_size(Field, Rec) ->
    collection_check_sequence(Rec),
    size(maps:get(Field, Rec)).

-ifdef(CHECK_SEQNO).
do_seqno() ->
    case erlang:get(?MODULE) of
        undefined ->
            put(?MODULE, 0),
            0;
        N ->
            put(?MODULE, N + 1),
            N + 1
    end.

-spec collection_check_sequence(collection()) -> ok.
collection_check_sequence(A = #{'_' := N}) ->
    N = erlang:get(?MODULE),
    ok.
-else.
-spec collection_check_sequence(collection()) -> ok.
collection_check_sequence(_A) ->
    ok.
-endif.

%% == Operations with the guard ==

%% @doc Read the guard
-spec tx_guard(collection_id()) -> guard() | undefined.
tx_guard(CollectionId) ->
    case emqx_ds:tx_read(?guard_topic(CollectionId)) of
        [{_Topic, _TS, Guard}] ->
            Guard;
        [] ->
            undefined
    end.

-spec tx_assert_guard(collection_id(), guard() | undefined) -> ok.
tx_assert_guard(CollectionId, undefined) ->
    emqx_ds:tx_ttv_assert_absent(?guard_topic(CollectionId), 0);
tx_assert_guard(CollectionId, Guard) when is_binary(Guard) ->
    emqx_ds:tx_ttv_assert_present(?guard_topic(CollectionId), 0, Guard).

-spec tx_write_guard(collection_id(), guard() | ?ds_tx_serial) -> ok.
tx_write_guard(CollectionId, Guard) ->
    emqx_ds:tx_write({?guard_topic(CollectionId), 0, Guard}).

-spec tx_delete_guard(collection_id()) -> ok.
tx_delete_guard(CollectionId) ->
    emqx_ds:tx_del_topic(?guard_topic(CollectionId)).

%%================================================================================
%% Internal exports
%%================================================================================

-spec pmap_topic(pmap_name(), collection_id() | '+', key_bin() | '+') -> emqx_ds:topic().
pmap_topic(Name, CollectionId, Key) ->
    ?pmap_topic(Name, CollectionId, Key).

%%================================================================================
%% Internal functions
%%================================================================================

-spec encode_key(module(), pmap_name(), _Key, _Val) -> key_bin().
encode_key(CBM, Name, K, V) ->
    CBM:pmap_encode_key(Name, K, V).

-spec decode_key(module(), pmap_name(), key_bin()) -> _Key.
decode_key(CBM, Name, KeyBin) ->
    CBM:pmap_decode_key(Name, KeyBin).

-spec encode_val(module(), pmap_name(), _Key, _Val) -> val_bin().
encode_val(CBM, Name, K, V) ->
    CBM:pmap_encode_val(Name, K, V).

-spec decode_val(module(), pmap_name(), _Key, val_bin()) -> _Val.
decode_val(CBM, Name, K, ValBin) ->
    CBM:pmap_decode_val(Name, K, ValBin).

restore_fun(CBM, {?pmap_topic(Name, _CollectionId, KeyBin), _TS, ValBin}, Acc) ->
    Key = decode_key(CBM, Name, KeyBin),
    Val = decode_val(CBM, Name, Key, ValBin),
    Acc#{Key => Val}.

%% == Operations over PMap KV pairs ==

%% @doc Write a single key-value pair that belongs to a pmap:
-spec tx_write_pmap_kv(module(), pmap_name(), collection_id(), _, _) -> ok.
tx_write_pmap_kv(CBM, Name, CollectionId, Key, Val) ->
    emqx_ds:tx_write(
        {
            pmap_topic(Name, CollectionId, encode_key(CBM, Name, Key, Val)),
            0,
            encode_val(CBM, Name, Key, Val)
        }
    ).
