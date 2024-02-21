%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

%% @doc CRUD interface for the persistent session
%%
%% This module encapsulates the data related to the state of the
%% inflight messages for the persistent session based on DS.
%%
%% It is responsible for saving, caching, and restoring session state.
%% It is completely devoid of business logic. Not even the default
%% values should be set in this module.
-module(emqx_persistent_session_ds_state).

-export([create_tables/0]).

-export([open/1, create_new/1, delete/1, commit/1, format/1, print_session/1, list_sessions/0]).
-export([get_created_at/1, set_created_at/2]).
-export([get_last_alive_at/1, set_last_alive_at/2]).
-export([get_expiry_interval/1, set_expiry_interval/2]).
-export([new_id/1]).
-export([get_stream/2, put_stream/3, del_stream/2, fold_streams/3]).
-export([get_seqno/2, put_seqno/3]).
-export([get_rank/2, put_rank/3, del_rank/2, fold_ranks/3]).
-export([get_subscriptions/1, put_subscription/4, del_subscription/3]).

-export([make_session_iterator/0, session_iterator_next/2]).

-export_type([
    t/0, metadata/0, subscriptions/0, seqno_type/0, stream_key/0, rank_key/0, session_iterator/0
]).

-include("emqx_mqtt.hrl").
-include("emqx_persistent_session_ds.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type subscriptions() :: emqx_topic_gbt:t(_SubId, emqx_persistent_session_ds:subscription()).

-opaque session_iterator() :: emqx_persistent_session_ds:id() | '$end_of_table'.

%% Generic key-value wrapper that is used for exporting arbitrary
%% terms to mnesia:
-record(kv, {k, v}).

%% Persistent map.
%%
%% Pmap accumulates the updates in a term stored in the heap of a
%% process, so they can be committed all at once in a single
%% transaction.
%%
%% It should be possible to make frequent changes to the pmap without
%% stressing Mria.
%%
%% It's implemented as two maps: `cache', and `dirty'. `cache' stores
%% the data, and `dirty' contains information about dirty and deleted
%% keys. When `commit/1' is called, dirty keys are dumped to the
%% tables, and deleted keys are removed from the tables.
-record(pmap, {table, cache, dirty}).

-type pmap(K, V) ::
    #pmap{
        table :: atom(),
        cache :: #{K => V},
        dirty :: #{K => dirty | del}
    }.

-type metadata() ::
    #{
        ?created_at => emqx_persistent_session_ds:timestamp(),
        ?last_alive_at => emqx_persistent_session_ds:timestamp(),
        ?expiry_interval => non_neg_integer(),
        ?last_id => integer()
    }.

-type seqno_type() ::
    ?next(?QOS_1)
    | ?dup(?QOS_1)
    | ?committed(?QOS_1)
    | ?next(?QOS_2)
    | ?dup(?QOS_2)
    | ?rec
    | ?committed(?QOS_2).

-opaque t() :: #{
    id := emqx_persistent_session_ds:id(),
    dirty := boolean(),
    metadata := metadata(),
    subscriptions := subscriptions(),
    seqnos := pmap(seqno_type(), emqx_persistent_session_ds:seqno()),
    streams := pmap(emqx_ds:stream(), emqx_persistent_session_ds:stream_state()),
    ranks := pmap(term(), integer())
}.

-define(session_tab, emqx_ds_session_tab).
-define(subscription_tab, emqx_ds_session_subscriptions).
-define(stream_tab, emqx_ds_session_streams).
-define(seqno_tab, emqx_ds_session_seqnos).
-define(rank_tab, emqx_ds_session_ranks).
-define(pmap_tables, [?stream_tab, ?seqno_tab, ?rank_tab, ?subscription_tab]).

%% Enable this flag if you suspect some code breaks the sequence:
-ifndef(CHECK_SEQNO).
-define(set_dirty, dirty => true).
-define(unset_dirty, dirty => false).
-else.
-define(set_dirty, dirty => true, '_' => do_seqno()).
-define(unset_dirty, dirty => false, '_' => do_seqno()).
-endif.

%%================================================================================
%% API funcions
%%================================================================================

-spec create_tables() -> ok.
create_tables() ->
    ok = mria:create_table(
        ?session_tab,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, ordered_set},
            {storage, rocksdb_copies},
            {record_name, kv},
            {attributes, record_info(fields, kv)}
        ]
    ),
    [create_kv_pmap_table(Table) || Table <- ?pmap_tables],
    mria:wait_for_tables([?session_tab | ?pmap_tables]).

-spec open(emqx_persistent_session_ds:id()) -> {ok, t()} | undefined.
open(SessionId) ->
    ro_transaction(fun() ->
        case kv_restore(?session_tab, SessionId) of
            [Metadata] ->
                Rec = #{
                    id => SessionId,
                    metadata => Metadata,
                    subscriptions => read_subscriptions(SessionId),
                    streams => pmap_open(?stream_tab, SessionId),
                    seqnos => pmap_open(?seqno_tab, SessionId),
                    ranks => pmap_open(?rank_tab, SessionId),
                    ?unset_dirty
                },
                {ok, Rec};
            [] ->
                undefined
        end
    end).

-spec print_session(emqx_persistent_session_ds:id()) -> map() | undefined.
print_session(SessionId) ->
    case open(SessionId) of
        undefined ->
            undefined;
        {ok, Session} ->
            format(Session)
    end.

-spec format(t()) -> map().
format(#{
    metadata := Metadata,
    subscriptions := SubsGBT,
    streams := Streams,
    seqnos := Seqnos,
    ranks := Ranks
}) ->
    Subs = emqx_topic_gbt:fold(
        fun(Key, Sub, Acc) ->
            maps:put(emqx_topic_gbt:get_topic(Key), Sub, Acc)
        end,
        #{},
        SubsGBT
    ),
    #{
        metadata => Metadata,
        subscriptions => Subs,
        streams => pmap_format(Streams),
        seqnos => pmap_format(Seqnos),
        ranks => pmap_format(Ranks)
    }.

-spec list_sessions() -> [emqx_persistent_session_ds:id()].
list_sessions() ->
    mnesia:dirty_all_keys(?session_tab).

-spec delete(emqx_persistent_session_ds:id()) -> ok.
delete(Id) ->
    transaction(
        fun() ->
            [kv_pmap_delete(Table, Id) || Table <- ?pmap_tables],
            mnesia:delete(?session_tab, Id, write)
        end
    ).

-spec commit(t()) -> t().
commit(Rec = #{dirty := false}) ->
    Rec;
commit(
    Rec = #{
        id := SessionId,
        metadata := Metadata,
        streams := Streams,
        seqnos := SeqNos,
        ranks := Ranks
    }
) ->
    check_sequence(Rec),
    transaction(fun() ->
        kv_persist(?session_tab, SessionId, Metadata),
        Rec#{
            streams => pmap_commit(SessionId, Streams),
            seqnos => pmap_commit(SessionId, SeqNos),
            ranks => pmap_commit(SessionId, Ranks),
            ?unset_dirty
        }
    end).

-spec create_new(emqx_persistent_session_ds:id()) -> t().
create_new(SessionId) ->
    transaction(fun() ->
        delete(SessionId),
        #{
            id => SessionId,
            metadata => #{},
            subscriptions => emqx_topic_gbt:new(),
            streams => pmap_open(?stream_tab, SessionId),
            seqnos => pmap_open(?seqno_tab, SessionId),
            ranks => pmap_open(?rank_tab, SessionId),
            ?set_dirty
        }
    end).

%%

-spec get_created_at(t()) -> emqx_persistent_session_ds:timestamp() | undefined.
get_created_at(Rec) ->
    get_meta(?created_at, Rec).

-spec set_created_at(emqx_persistent_session_ds:timestamp(), t()) -> t().
set_created_at(Val, Rec) ->
    set_meta(?created_at, Val, Rec).

-spec get_last_alive_at(t()) -> emqx_persistent_session_ds:timestamp() | undefined.
get_last_alive_at(Rec) ->
    get_meta(?last_alive_at, Rec).

-spec set_last_alive_at(emqx_persistent_session_ds:timestamp(), t()) -> t().
set_last_alive_at(Val, Rec) ->
    set_meta(?last_alive_at, Val, Rec).

-spec get_expiry_interval(t()) -> non_neg_integer() | undefined.
get_expiry_interval(Rec) ->
    get_meta(?expiry_interval, Rec).

-spec set_expiry_interval(non_neg_integer(), t()) -> t().
set_expiry_interval(Val, Rec) ->
    set_meta(?expiry_interval, Val, Rec).

-spec new_id(t()) -> {emqx_persistent_session_ds:subscription_id(), t()}.
new_id(Rec) ->
    LastId =
        case get_meta(?last_id, Rec) of
            undefined -> 0;
            N when is_integer(N) -> N
        end,
    {LastId, set_meta(?last_id, LastId + 1, Rec)}.

%%

-spec get_subscriptions(t()) -> subscriptions().
get_subscriptions(#{subscriptions := Subs}) ->
    Subs.

-spec put_subscription(
    emqx_persistent_session_ds:topic_filter(),
    _SubId,
    emqx_persistent_session_ds:subscription(),
    t()
) -> t().
put_subscription(TopicFilter, SubId, Subscription, Rec = #{id := Id, subscriptions := Subs0}) ->
    %% Note: currently changes to the subscriptions are persisted immediately.
    Key = {TopicFilter, SubId},
    transaction(fun() -> kv_pmap_persist(?subscription_tab, Id, Key, Subscription) end),
    Subs = emqx_topic_gbt:insert(TopicFilter, SubId, Subscription, Subs0),
    Rec#{subscriptions => Subs}.

-spec del_subscription(emqx_persistent_session_ds:topic_filter(), _SubId, t()) -> t().
del_subscription(TopicFilter, SubId, Rec = #{id := Id, subscriptions := Subs0}) ->
    %% Note: currently the subscriptions are persisted immediately.
    Key = {TopicFilter, SubId},
    transaction(fun() -> kv_pmap_delete(?subscription_tab, Id, Key) end),
    Subs = emqx_topic_gbt:delete(TopicFilter, SubId, Subs0),
    Rec#{subscriptions => Subs}.

%%

-type stream_key() :: {emqx_persistent_session_ds:subscription_id(), _StreamId}.

-spec get_stream(stream_key(), t()) ->
    emqx_persistent_session_ds:stream_state() | undefined.
get_stream(Key, Rec) ->
    gen_get(streams, Key, Rec).

-spec put_stream(stream_key(), emqx_persistent_session_ds:stream_state(), t()) -> t().
put_stream(Key, Val, Rec) ->
    gen_put(streams, Key, Val, Rec).

-spec del_stream(stream_key(), t()) -> t().
del_stream(Key, Rec) ->
    gen_del(streams, Key, Rec).

-spec fold_streams(fun(), Acc, t()) -> Acc.
fold_streams(Fun, Acc, Rec) ->
    gen_fold(streams, Fun, Acc, Rec).

%%

-spec get_seqno(seqno_type(), t()) -> emqx_persistent_session_ds:seqno() | undefined.
get_seqno(Key, Rec) ->
    gen_get(seqnos, Key, Rec).

-spec put_seqno(seqno_type(), emqx_persistent_session_ds:seqno(), t()) -> t().
put_seqno(Key, Val, Rec) ->
    gen_put(seqnos, Key, Val, Rec).

%%

-type rank_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:rank_x()}.

-spec get_rank(rank_key(), t()) -> integer() | undefined.
get_rank(Key, Rec) ->
    gen_get(ranks, Key, Rec).

-spec put_rank(rank_key(), integer(), t()) -> t().
put_rank(Key, Val, Rec) ->
    gen_put(ranks, Key, Val, Rec).

-spec del_rank(rank_key(), t()) -> t().
del_rank(Key, Rec) ->
    gen_del(ranks, Key, Rec).

-spec fold_ranks(fun(), Acc, t()) -> Acc.
fold_ranks(Fun, Acc, Rec) ->
    gen_fold(ranks, Fun, Acc, Rec).

-spec make_session_iterator() -> session_iterator().
make_session_iterator() ->
    case mnesia:dirty_first(?session_tab) of
        '$end_of_table' ->
            '$end_of_table';
        Key ->
            Key
    end.

-spec session_iterator_next(session_iterator(), pos_integer()) ->
    {[{emqx_persistent_session_ds:id(), metadata()}], session_iterator()}.
session_iterator_next(Cursor, 0) ->
    {[], Cursor};
session_iterator_next('$end_of_table', _N) ->
    {[], '$end_of_table'};
session_iterator_next(Cursor0, N) ->
    ThisVal = [
        {Cursor0, Metadata}
     || #kv{v = Metadata} <- mnesia:dirty_read(?session_tab, Cursor0)
    ],
    {NextVals, Cursor} = session_iterator_next(mnesia:dirty_next(?session_tab, Cursor0), N - 1),
    {ThisVal ++ NextVals, Cursor}.

%%================================================================================
%% Internal functions
%%================================================================================

%% All mnesia reads and writes are passed through this function.
%% Backward compatiblity issues can be handled here.
encoder(encode, _Table, Term) ->
    Term;
encoder(decode, _Table, Term) ->
    Term.

%%

get_meta(K, #{metadata := Meta}) ->
    maps:get(K, Meta, undefined).

set_meta(K, V, Rec = #{metadata := Meta}) ->
    check_sequence(Rec#{metadata => maps:put(K, V, Meta), ?set_dirty}).

%%

gen_get(Field, Key, Rec) ->
    check_sequence(Rec),
    pmap_get(Key, maps:get(Field, Rec)).

gen_fold(Field, Fun, Acc, Rec) ->
    check_sequence(Rec),
    pmap_fold(Fun, Acc, maps:get(Field, Rec)).

gen_put(Field, Key, Val, Rec) ->
    check_sequence(Rec),
    maps:update_with(
        Field,
        fun(PMap) -> pmap_put(Key, Val, PMap) end,
        Rec#{?set_dirty}
    ).

gen_del(Field, Key, Rec) ->
    check_sequence(Rec),
    maps:update_with(
        Field,
        fun(PMap) -> pmap_del(Key, PMap) end,
        Rec#{?set_dirty}
    ).

%%

read_subscriptions(SessionId) ->
    Records = kv_pmap_restore(?subscription_tab, SessionId),
    lists:foldl(
        fun({{TopicFilter, SubId}, Subscription}, Acc) ->
            emqx_topic_gbt:insert(TopicFilter, SubId, Subscription, Acc)
        end,
        emqx_topic_gbt:new(),
        Records
    ).

%%

%% @doc Open a PMAP and fill the clean area with the data from DB.
%% This functtion should be ran in a transaction.
-spec pmap_open(atom(), emqx_persistent_session_ds:id()) -> pmap(_K, _V).
pmap_open(Table, SessionId) ->
    Clean = maps:from_list(kv_pmap_restore(Table, SessionId)),
    #pmap{
        table = Table,
        cache = Clean,
        dirty = #{}
    }.

-spec pmap_get(K, pmap(K, V)) -> V | undefined.
pmap_get(K, #pmap{cache = Cache}) ->
    maps:get(K, Cache, undefined).

-spec pmap_put(K, V, pmap(K, V)) -> pmap(K, V).
pmap_put(K, V, Pmap = #pmap{dirty = Dirty, cache = Cache}) ->
    Pmap#pmap{
        cache = maps:put(K, V, Cache),
        dirty = Dirty#{K => dirty}
    }.

-spec pmap_del(K, pmap(K, V)) -> pmap(K, V).
pmap_del(
    Key,
    Pmap = #pmap{dirty = Dirty, cache = Cache}
) ->
    Pmap#pmap{
        cache = maps:remove(Key, Cache),
        dirty = Dirty#{Key => del}
    }.

-spec pmap_fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
pmap_fold(Fun, Acc, #pmap{cache = Cache}) ->
    maps:fold(Fun, Acc, Cache).

-spec pmap_commit(emqx_persistent_session_ds:id(), pmap(K, V)) -> pmap(K, V).
pmap_commit(
    SessionId, Pmap = #pmap{table = Tab, dirty = Dirty, cache = Cache}
) ->
    maps:foreach(
        fun
            (K, del) ->
                kv_pmap_delete(Tab, SessionId, K);
            (K, dirty) ->
                V = maps:get(K, Cache),
                kv_pmap_persist(Tab, SessionId, K, V)
        end,
        Dirty
    ),
    Pmap#pmap{
        dirty = #{}
    }.

-spec pmap_format(pmap(_K, _V)) -> map().
pmap_format(#pmap{cache = Cache}) ->
    Cache.

%% Functions dealing with set tables:

kv_persist(Tab, SessionId, Val0) ->
    Val = encoder(encode, Tab, Val0),
    mnesia:write(Tab, #kv{k = SessionId, v = Val}, write).

kv_restore(Tab, SessionId) ->
    [encoder(decode, Tab, V) || #kv{v = V} <- mnesia:read(Tab, SessionId)].

%% Functions dealing with bags:

%% @doc Create a mnesia table for the PMAP:
-spec create_kv_pmap_table(atom()) -> ok.
create_kv_pmap_table(Table) ->
    mria:create_table(Table, [
        {type, ordered_set},
        {rlog_shard, ?DS_MRIA_SHARD},
        {storage, rocksdb_copies},
        {record_name, kv},
        {attributes, record_info(fields, kv)}
    ]).

kv_pmap_persist(Tab, SessionId, Key, Val0) ->
    %% Write data to mnesia:
    Val = encoder(encode, Tab, Val0),
    mnesia:write(Tab, #kv{k = {SessionId, Key}, v = Val}, write).

kv_pmap_restore(Table, SessionId) ->
    MS = [{#kv{k = {SessionId, '$1'}, v = '$2'}, [], [{{'$1', '$2'}}]}],
    Objs = mnesia:select(Table, MS, read),
    [{K, encoder(decode, Table, V)} || {K, V} <- Objs].

kv_pmap_delete(Table, SessionId) ->
    MS = [{#kv{k = {SessionId, '$1'}, _ = '_'}, [], ['$1']}],
    Keys = mnesia:select(Table, MS, read),
    [mnesia:delete(Table, {SessionId, K}, write) || K <- Keys],
    ok.

kv_pmap_delete(Table, SessionId, Key) ->
    %% Note: this match spec uses a fixed primary key, so it doesn't
    %% require a table scan, and the transaction doesn't grab the
    %% whole table lock:
    mnesia:delete(Table, {SessionId, Key}, write).

%%

transaction(Fun) ->
    mria:async_dirty(?DS_MRIA_SHARD, Fun).

ro_transaction(Fun) ->
    mria:async_dirty(?DS_MRIA_SHARD, Fun).

%% transaction(Fun) ->
%%     case mnesia:is_transaction() of
%%         true ->
%%             Fun();
%%         false ->
%%             {atomic, Res} = mria:transaction(?DS_MRIA_SHARD, Fun),
%%             Res
%%     end.

%% ro_transaction(Fun) ->
%%     {atomic, Res} = mria:ro_transaction(?DS_MRIA_SHARD, Fun),
%%     Res.

-compile({inline, check_sequence/1}).

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

check_sequence(A = #{'_' := N}) ->
    N = erlang:get(?MODULE),
    A.
-else.
check_sequence(A) ->
    A.
-endif.
