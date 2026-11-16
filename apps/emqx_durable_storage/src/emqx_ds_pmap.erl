%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

Pmaps come in two flavors: regular and diff-based. Diff based pmaps
try to minimize changes to the durable storage at the cost of higher
RAM usage. Their representation at rest is the same, so they are
interchangeable.

## Collections

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
tx_write_guard(Id, ?ds_tx_serial)
```

to ensure that data hasn't changed between the two transactions and to
establish the new ownership. Then the owner must keep id of the second
transaction as a the new guard and call `tx_assert_guard(Id, Guard)`
in the subsequent transactions to make the collection hasn't been
hijacked by other processes.

Note that this mechanism only detects ownership change post-factum and
prevents data corruption, but doesn't _lock out_ other processes from
taking over the collection. Such locking is out of scope of this module.

## PSet

Pmap can be created with `set` kind, making it more efficient for
storing set-like data.

Bits: |I|C|S|R|

R - RAM. State of the key in RAM (set 1 / unset 0).
S - Storage. Cached, simulated state of the key in DS.
C - Pending commit, as per last commit attempt (possibly in-flight)
I - Inflight flag. This bit is set when transaction starts, and reset when it finishes.
""".

%% API:
-export([
    storage_opts/1,
    lts_threshold_cb/2,

    new_pmap/2,
    new_pmap/3,
    tx_restore/3,
    tx_restore/4,
    tx_commit/2,
    tx_success/2,
    tx_destroy/2,
    dirty_read/4,

    get/2,
    put/3,
    del/3,
    fold/3,
    size/1,
    to_map/1,
    is_dirty/1,

    collection_get/3,
    collection_fold/4,
    collection_put/4,
    collection_del/3,
    collection_size/2,
    collection_check_sequence/1,
    tx_collection_commit/2,
    collection_release_dirty/3,

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
    tx_counter/0,
    pmap/2,
    collection_id/0,
    guard/0,
    collection/0
]).

-compile({no_auto_import, [{size, 1}]}).

-include("emqx_ds.hrl").
-include("emqx_ds_pmap.hrl").

-define(pset_ram, 2#0001).
-define(pset_stor, 2#0010).
-define(pset_comm, 2#0100).
-define(pset_infl, 2#1000).

-define(pset_live(STATE), ((STATE band ?pset_ram) > 0)).

%%================================================================================
%% Type declarations
%%================================================================================

-doc """
WARNING: set is experimental.
""".
-type kind() :: dirty | set.

-type tx_counter() :: integer().

-type pmap_dirty() ::
    #pmap_dirty{
        up_ctr :: tx_counter(),
        comm_ctr :: tx_counter(),
        was :: boolean(),
        live :: boolean()
    }.

-opaque pmap(K, V) ::
    #pmap{
        name :: binary(),
        module :: module(),
        cache :: #{K => V},
        dirty :: #{K => pmap_dirty()},
        tx_counter :: tx_counter()
    }
    | #pset{
        name :: binary(),
        module :: module(),
        data :: #{K => 0..2#1111},
        size :: non_neg_integer()
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

-callback pmap_encode_key(pmap_name(), _Key) -> key_bin().

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
new_pmap(CBM, Name) ->
    new_pmap(CBM, Name, dirty).

-spec new_pmap(module(), binary(), kind()) -> pmap(_K, _V).
new_pmap(CBM, Name, dirty) when is_atom(CBM), is_binary(Name) ->
    #pmap{name = Name, module = CBM};
new_pmap(CBM, Name, set) when is_atom(CBM), is_binary(Name) ->
    #pset{name = Name, module = CBM}.

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
Dump dirty (changed) data from the pmap to the durable storage.

This function should be called in a transaction context.

It does NOT clean the dirty flags for the keys:
that is done by `tx_success` function,
which should be called when the transaction succeeds.
""".
-spec tx_commit(collection_id(), pmap(K, V)) -> {tx_counter() | undefined, pmap(K, V)}.
tx_commit(
    CollectionId,
    #pmap{
        name = Name,
        tx_counter = TxCntr
    } = Pmap
) when is_binary(CollectionId) ->
    %% Claim the entire pmap data topic filter to avoid locking individual keys:
    LockedTF = pmap_topic(Name, CollectionId, '+'),
    emqx_ds:tx_lock(LockedTF, 0, infinity, w),
    emqx_ds:tx_lock(LockedTF, 0, infinity, d),
    %% Dump data:
    pmap_dump_dirty(CollectionId, Pmap),
    {TxCntr, Pmap#pmap{tx_counter = TxCntr + 1}};
tx_commit(
    CollectionId,
    #pset{
        name = Name,
        module = CBM,
        data = Data0
    } = Pmap
) ->
    %% Claim the entire pmap data topic filter to avoid locking individual keys:
    LockedTF = pmap_topic(Name, CollectionId, '+'),
    emqx_ds:tx_lock(LockedTF, 0, infinity, w),
    emqx_ds:tx_lock(LockedTF, 0, infinity, d),
    %% Dump dirty data while setting P & I flags:
    Data = maps:map(
        fun(Key, Val) ->
            case pset_dirty(Val) of
                false ->
                    Val;
                true ->
                    Topic = pmap_topic(Name, CollectionId, Key),
                    case ?pset_live(Val) of
                        true ->
                            tx_write_pmap_kv(CBM, Name, CollectionId, Key, true),
                            Val bor ?pset_infl bor ?pset_comm;
                        false ->
                            emqx_ds:tx_del_topic(Topic),
                            Val band (bnot ?pset_comm) bor ?pset_infl
                    end
            end
        end,
        Data0
    ),
    {undefined, Pmap#pset{
        data = Data
    }}.

-doc """
Commit all pmaps in the collection using `tx_commit`.

This function should be called in a transaction context.

It does NOT clean any dirty flags:
that is done by `collection_release_dirty` function,
which should be called when the transaction succeeds.
""".
-spec tx_collection_commit([field(), ...], collection()) ->
    {tx_counter(), collection()}.
tx_collection_commit([_ | _] = Fields, #{id := CollectionId} = Collection) ->
    lists:foldl(
        fun(Field, {CntrAcc, Acc}) ->
            #{Field := Pmap0} = Acc,
            {Cntr, Pmap} = tx_commit(CollectionId, Pmap0),
            CollCntr =
                case Cntr of
                    undefined ->
                        CntrAcc;
                    _ when CntrAcc =:= undefined ->
                        Cntr;
                    CntrAcc ->
                        %% Implicit assert
                        Cntr
                end,
            {CollCntr, Acc#{Field := Pmap}}
        end,
        {undefined, Collection},
        Fields
    ).

-doc """
Release dirty records committed by a transaction.
""".
-spec tx_success(tx_counter(), pmap(K, V)) -> pmap(K, V).
tx_success(Committed, Pmap = #pmap{dirty = Dirty}) ->
    Pmap#pmap{dirty = pmap_release_dirty(Committed, Dirty)};
tx_success(_, Pmap = #pset{data = Data0}) ->
    Data = maps:filtermap(
        fun(_, Val0) ->
            Val = pset_copy_comm(pset_clear_infl(Val0)),
            case Val of
                0 ->
                    %% Gone from both DB and RAM, delete the key:
                    false;
                _ ->
                    {true, Val}
            end
        end,
        Data0
    ),
    Pmap#pset{
        data = Data
    }.

-doc """
Release dirty records in all pmaps in the collection.
""".
-spec collection_release_dirty([field()], tx_counter(), collection()) -> collection().
collection_release_dirty(Fields, Committed, Collection0) ->
    {IsDirty, Collection} = lists:foldl(
        fun(Field, {IsDirtyAcc, Acc}) ->
            #{Field := Pmap0} = Acc,
            Pmap = tx_success(Committed, Pmap0),
            {
                IsDirtyAcc orelse is_dirty(Pmap),
                Acc#{Field := Pmap}
            }
        end,
        {false, Collection0},
        Fields
    ),
    Collection#{?collection_dirty := IsDirty}.

-spec tx_restore(module(), pmap_name(), collection_id()) -> pmap(_, _).
tx_restore(CBM, Name, CollectionId) ->
    tx_restore(CBM, Name, CollectionId, dirty).

-doc """
Read data from a durable storage to RAM, creating a new pmap.

This function should be called in a transaction context.
""".
-spec tx_restore(module(), pmap_name(), collection_id(), kind()) -> pmap(_, _).
tx_restore(CBM, Name, CollectionId, Kind) when
    is_atom(CBM), is_binary(Name), is_binary(CollectionId)
->
    PData = emqx_ds:tx_fold_topic(
        fun(_Slab, _Stream, Payload, Acc) ->
            restore_fun(CBM, Payload, Acc)
        end,
        #{},
        pmap_topic(Name, CollectionId, '+')
    ),
    case Kind of
        dirty ->
            #pmap{
                name = Name,
                module = CBM,
                cache = PData
            };
        set ->
            Data = maps:map(fun(_, _) -> ?pset_stor bor ?pset_ram bor ?pset_comm end, PData),
            #pset{
                name = Name,
                module = CBM,
                data = Data,
                size = maps:size(Data)
            }
    end.

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
    maps:get(Key, Cache, undefined);
get(Key, #pset{data = Data}) ->
    case Data of
        #{Key := Val} when ?pset_live(Val) ->
            true;
        #{} ->
            undefined
    end.

-spec put(K, V, pmap(K, V)) -> pmap(K, V).
put(
    Key,
    Val,
    #pmap{cache = Cache0} = Pmap
) ->
    Cache = Cache0#{Key => Val},
    Dirty = pmap_upd_dirty(Key, true, Pmap),
    Pmap#pmap{
        cache = Cache,
        dirty = Dirty
    };
put(
    Key,
    true,
    Pmap = #pset{
        data = Data,
        size = Size
    }
) ->
    case Data of
        #{Key := Val} ->
            Pmap#pset{
                data = Data#{Key := Val bor ?pset_ram},
                size =
                    case ?pset_live(Val) of
                        true -> Size;
                        false -> Size + 1
                    end
            };
        #{} ->
            Pmap#pset{
                data = Data#{Key => ?pset_ram},
                size = Size + 1
            }
    end.

-spec del(collection_id(), K, pmap(K, V)) -> pmap(K, V).
del(
    _CollectionId,
    Key,
    Pmap = #pmap{cache = Cache0}
) ->
    Cache = maps:remove(Key, Cache0),
    Dirty = pmap_upd_dirty(Key, false, Pmap),
    Pmap#pmap{
        cache = Cache,
        dirty = Dirty
    };
del(
    _CollectionId,
    Key,
    Pmap = #pset{
        data = Data,
        size = Size
    }
) ->
    case Data of
        #{Key := Val} ->
            Pmap#pset{
                data = Data#{Key => Val band (bnot ?pset_ram)},
                size =
                    case ?pset_live(Val) of
                        true -> Size - 1;
                        false -> Size
                    end
            };
        #{} ->
            Pmap
    end.

-spec fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
fold(Fun, Acc, #pmap{cache = Cache}) ->
    maps:fold(Fun, Acc, Cache);
fold(Fun, Acc0, #pset{data = Data}) ->
    maps:fold(
        fun(Key, Val, Acc) ->
            case ?pset_live(Val) of
                true -> Fun(Key, true, Acc);
                false -> Acc
            end
        end,
        Acc0,
        Data
    ).

-spec size(pmap(_K, _V)) -> non_neg_integer().
size(#pmap{cache = Cache}) ->
    maps:size(Cache);
size(#pset{size = Size}) ->
    Size.

-spec to_map(pmap(K, V)) -> #{K => V}.
to_map(#pmap{cache = Cache}) ->
    Cache;
to_map(#pset{data = Data}) ->
    #{Key => true || Key := Val <- Data, ?pset_live(Val)}.

-spec is_dirty(pmap(_K, _V)) -> boolean().
is_dirty(#pmap{dirty = D}) ->
    maps:size(D) > 0;
is_dirty(#pset{data = D}) ->
    try
        maps:foreach(
            fun(_, Val) ->
                pset_dirty(Val) andalso throw(dirty)
            end,
            D
        ),
        false
    catch
        dirty ->
            true
    end.

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

-spec encode_key(module(), pmap_name(), _Key) -> key_bin().
encode_key(CBM, Name, K) ->
    CBM:pmap_encode_key(Name, K).

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
            pmap_topic(Name, CollectionId, encode_key(CBM, Name, Key)),
            0,
            encode_val(CBM, Name, Key, Val)
        }
    ).

%% == PMap dirty handling ==

-spec pmap_upd_dirty(K, boolean(), pmap(K, _V)) -> map().
pmap_upd_dirty(Key, Live, #pmap{tx_counter = TxCntr, dirty = Dirty, cache = Cache}) ->
    case Dirty of
        #{Key := Entry0} ->
            case Entry0 of
                #pmap_dirty{comm_ctr = TxCntr, was = Was} ->
                    %% Previous update of the key happened after a
                    %% successful commit, so we can mutate the
                    %% entry:
                    case Live of
                        false when not Was ->
                            %% Optimization: [empty] <- put <- del = [empty]
                            maps:remove(Key, Dirty);
                        _ ->
                            Entry = Entry0#pmap_dirty{live = Live},
                            Dirty#{Key := Entry}
                    end;
                _ ->
                    %% If the key was updated before beginning of the
                    %% current pending checkpoint, then the actual
                    %% state of the DB is unknown (checkpoint could be
                    %% aborted). As such, unconditionally add dirty
                    %% entry:
                    Entry = Entry0#pmap_dirty{live = Live, up_ctr = TxCntr},
                    Dirty#{Key := Entry}
            end;
        #{} ->
            case maps:is_key(Key, Cache) orelse Live of
                true ->
                    Entry = #pmap_dirty{
                        up_ctr = TxCntr,
                        comm_ctr = TxCntr,
                        was = maps:is_key(Key, Cache),
                        live = Live
                    },
                    Dirty#{Key => Entry};
                false ->
                    %% Optimization: [empty] <- del = [empty]
                    Dirty
            end
    end.

-spec pmap_dump_dirty(collection_id(), pmap(_K, _V)) -> ok.
pmap_dump_dirty(CollectionId, #pmap{module = CBM, name = Name, dirty = Dirty, cache = Cache}) ->
    maps:foreach(
        fun(Key, #pmap_dirty{live = Live}) ->
            case Live of
                true ->
                    #{Key := Val} = Cache,
                    tx_write_pmap_kv(CBM, Name, CollectionId, Key, Val);
                false ->
                    Topic = pmap_topic(Name, CollectionId, encode_key(CBM, Name, Key)),
                    emqx_ds:tx_del_topic(Topic)
            end
        end,
        Dirty
    ).

-spec pmap_release_dirty(tx_counter(), map()) -> map().
pmap_release_dirty(Committed, Dirty) ->
    maps:filter(
        fun(_, #pmap_dirty{up_ctr = Ctr}) ->
            Ctr > Committed
        end,
        Dirty
    ).

%% == PSet accessors ==

-spec pset_dirty(0..2#1111) -> boolean().
pset_dirty(Val) ->
    pset_is_infl(Val) orelse pset_get_stor(Val) =/= pset_get_ram(Val).

pset_get_ram(V) ->
    V band ?pset_ram.

pset_get_stor(V) ->
    (V band ?pset_stor) bsr 1.

pset_get_comm(V) ->
    (V band ?pset_comm) bsr 2.

pset_is_infl(V) ->
    (V band ?pset_infl) > 0.

pset_clear_infl(V) ->
    V band (bnot ?pset_infl).

pset_copy_comm(V) ->
    case pset_get_comm(V) of
        1 -> V bor ?pset_stor;
        0 -> V band (bnot ?pset_stor)
    end.
