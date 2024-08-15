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
%%
%% Session process MUST NOT use `cold_*' functions! They are reserved
%% for use in the management APIs.
-module(emqx_persistent_session_ds_state).

-feature(maybe_expr, enable).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-export([open_db/0]).

-export([
    open/1, create_new/1, delete/1, commit/1, commit/2, format/1, print_session/1, list_sessions/0
]).
-export([get_created_at/1, set_created_at/2]).
-export([get_last_alive_at/1, set_last_alive_at/2]).
-export([get_expiry_interval/1, set_expiry_interval/2]).
-export([get_clientinfo/1, set_clientinfo/2]).
-export([get_will_message/1, set_will_message/2, clear_will_message/1, clear_will_message_now/1]).
-export([set_offline_info/2]).
-export([get_peername/1, set_peername/2]).
-export([get_protocol/1, set_protocol/2]).
-export([new_id/1]).
-export([get_stream/2, put_stream/3, del_stream/2, fold_streams/3, iter_streams/2, n_streams/1]).
-export([get_seqno/2, put_seqno/3]).
-export([get_rank/2, put_rank/3, del_rank/2, fold_ranks/3]).
-export([
    get_subscription_state/2,
    cold_get_subscription_state/2,
    fold_subscription_states/3,
    put_subscription_state/3,
    del_subscription_state/2
]).
-export([
    get_subscription/2,
    cold_get_subscription/2,
    fold_subscriptions/3,
    n_subscriptions/1,
    total_subscription_count/0,
    put_subscription/3,
    del_subscription/2
]).
-export([
    get_awaiting_rel/2,
    put_awaiting_rel/3,
    del_awaiting_rel/2,
    fold_awaiting_rel/3,
    n_awaiting_rel/1
]).

-export([iter_next/1]).

-export([make_session_iterator/0, session_iterator_next/2]).

-export_type([
    t/0,
    metadata/0,
    iter/2,
    seqno_type/0,
    stream_key/0,
    rank_key/0,
    session_iterator/0,
    protocol/0
]).

-include("emqx_mqtt.hrl").
-include("session_internals.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/qlc.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-ifdef(TEST).
-export([to_domain_msg/4, from_domain_msg/1]).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(DB, ?DURABLE_SESSION_STATE).
-define(TS, 0).

-type message() :: emqx_types:message().

-opaque iter(K, V) :: #{
    it := gb_trees:iter(internal_key(K), V), inv_key_mapping := #{internal_key(K) => K}
}.

-opaque session_iterator() :: #{its := [emqx_ds:iterator()]} | '$end_of_table'.

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
%%
%% `key_mapping' maps external keys to unique integers, which are internally used in the
%% topic level structure to avoid costly encodings of arbitrary key terms.
-record(pmap, {domain, key_mapping = #{}, cache, dirty}).

-type internal_key(_K) ::
    %% `?stream_domain'
    binary()
    %% other domains
    | integer().

-type pmap(K, V) ::
    #pmap{
        domain :: atom(),
        key_mapping :: #{K => internal_key(K)},
        cache :: #{internal_key(K) => V} | gb_trees:tree(internal_key(K), V),
        dirty :: #{internal_key(K) => dirty | del}
    }.

-type protocol() :: {binary(), emqx_types:proto_ver()}.

-type metadata() ::
    #{
        ?created_at => emqx_persistent_session_ds:timestamp(),
        ?last_alive_at => emqx_persistent_session_ds:timestamp(),
        ?expiry_interval => non_neg_integer(),
        ?last_id => integer(),
        ?peername => emqx_types:peername(),
        ?protocol => protocol()
    }.

-type seqno_type() ::
    ?next(?QOS_1)
    | ?dup(?QOS_1)
    | ?committed(?QOS_1)
    | ?next(?QOS_2)
    | ?dup(?QOS_2)
    | ?rec
    | ?committed(?QOS_2).

-define(id, id).
-define(dirty, dirty).
-define(metadata, metadata).
-define(subscriptions, subscriptions).
-define(subscription_states, subscription_states).
-define(seqnos, seqnos).
-define(streams, streams).
-define(ranks, ranks).
-define(awaiting_rel, awaiting_rel).

-opaque t() :: #{
    ?id := emqx_persistent_session_ds:id(),
    ?dirty := boolean(),
    ?metadata := metadata(),
    ?subscriptions := pmap(
        emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_subs:subscription()
    ),
    ?subscription_states := pmap(
        emqx_persistent_session_ds_subs:subscription_state_id(),
        emqx_persistent_session_ds_subs:subscription_state()
    ),
    ?seqnos := pmap(seqno_type(), emqx_persistent_session_ds:seqno()),
    ?streams := pmap(emqx_ds:stream(), emqx_persistent_session_ds:stream_state()),
    ?ranks := pmap(term(), integer()),
    ?awaiting_rel := pmap(emqx_types:packet_id(), _Timestamp :: integer())
}.

-define(session_topic_ns, <<"session">>).
-define(metadata_domain, metadata).
-define(metadata_domain_bin, <<"metadata">>).
-define(subscription_domain, subscription).
-define(subscription_domain_bin, <<"subscription">>).
-define(subscription_state_domain, subscription_state).
-define(subscription_state_domain_bin, <<"subscription_state">>).
-define(stream_domain, stream).
-define(stream_domain_bin, <<"stream">>).
-define(rank_domain, rank).
-define(rank_domain_bin, <<"rank">>).
-define(seqno_domain, seqno).
-define(seqno_domain_bin, <<"seqno">>).
-define(awaiting_rel_domain, awaiting_rel).
-define(awaiting_rel_domain_bin, <<"awaiting_rel">>).
-type domain() ::
    ?metadata_domain
    | ?subscription_domain
    | ?subscription_state_domain
    | ?stream_domain
    | ?rank_domain
    | ?seqno_domain
    | ?awaiting_rel_domain.

-type sub_id() :: nil().
-type srs() :: #srs{}.
-type data() ::
    #{
        domain := ?metadata_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := any(),
        val := map()
    }
    | #{
        domain := ?subscription_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := {emqx_types:topic(), sub_id()},
        val := emqx_persistent_session_ds:subscription()
    }
    | #{
        domain := ?subscription_state_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := emqx_persistent_session_ds_subs:subscription_state_id(),
        val := emqx_persistent_session_ds_subs:subscription_state()
    }
    | #{
        domain := ?stream_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := {non_neg_integer(), emqx_ds:stream()},
        val := srs()
    }
    | #{
        domain := ?rank_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := rank_key(),
        val := non_neg_integer()
    }
    | #{
        domain := ?seqno_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := seqno_type(),
        val := non_neg_integer()
    }
    | #{
        domain := ?awaiting_rel_domain,
        session_id := emqx_persistent_session_ds:id(),
        key := emqx_types:packet_id(),
        val := _Timestamp :: integer()
    }.

-define(pmaps, [
    {?subscriptions, ?subscription_domain},
    {?subscription_states, ?subscription_state_domain},
    {?streams, ?stream_domain},
    {?seqnos, ?seqno_domain},
    {?ranks, ?rank_domain},
    {?awaiting_rel, ?awaiting_rel_domain}
]).

%% Enable this flag if you suspect some code breaks the sequence:
-ifndef(CHECK_SEQNO).
-define(set_dirty, dirty => true).
-define(unset_dirty, dirty => false).
-else.
-define(set_dirty, dirty => true, '_' => do_seqno()).
-define(unset_dirty, dirty => false, '_' => do_seqno()).
-endif.

%%================================================================================
%% API functions
%%================================================================================

-spec open_db() -> ok.
open_db() ->
    emqx_ds:open_db(?DB, #{backend => builtin_mnesia}).

-spec open(emqx_persistent_session_ds:id()) -> {ok, t()} | undefined.
open(SessionId) ->
    case session_restore(SessionId) of
        #{
            ?metadata_domain := [#{val := #{metadata := Metadata, key_mappings := KeyMappings}}],
            ?subscription_domain := Subs,
            ?subscription_state_domain := SubStates,
            ?stream_domain := Streams,
            ?rank_domain := Ranks,
            ?seqno_domain := Seqnos,
            ?awaiting_rel_domain := AwaitingRels
        } ->
            PmapOpen = fun(Domain, Data) ->
                KeyMapping = maps:get(Domain, KeyMappings, #{}),
                pmap_open(Domain, Data, KeyMapping)
            end,
            Rec = #{
                ?id => SessionId,
                ?metadata => Metadata,
                ?subscriptions => PmapOpen(?subscription_domain, Subs),
                ?subscription_states => PmapOpen(?subscription_state_domain, SubStates),
                ?streams => PmapOpen(?stream_domain, Streams),
                ?seqnos => PmapOpen(?seqno_domain, Seqnos),
                ?ranks => PmapOpen(?rank_domain, Ranks),
                ?awaiting_rel => PmapOpen(?awaiting_rel_domain, AwaitingRels),
                ?unset_dirty
            },
            {ok, Rec};
        _ ->
            undefined
    end.

-spec print_session(emqx_persistent_session_ds:id()) -> map() | undefined.
print_session(SessionId) ->
    case open(SessionId) of
        undefined ->
            undefined;
        {ok, Session} ->
            format(Session)
    end.

-spec format(t()) -> map().
format(Rec) ->
    update_pmaps(
        fun(Pmap, _Domain) ->
            pmap_format(Pmap)
        end,
        maps:without([id, dirty], Rec)
    ).

-spec list_sessions() -> [emqx_persistent_session_ds:id()].
list_sessions() ->
    lists:map(
        fun(#{session_id := Id}) -> Id end,
        read_iterate('+', ?metadata_domain_bin, ?metadata_domain_bin)
    ).

-spec delete(emqx_persistent_session_ds:id()) -> ok.
delete(Id) ->
    delete_iterate(Id, _Domain = '+', _Key = '+').

-spec commit(t()) -> t().
commit(Rec) ->
    commit(Rec, _Opts = #{}).

-spec commit(t(), #{ensure_new => boolean()}) -> t().
commit(Rec = #{dirty := false}, _Opts) ->
    Rec;
commit(
    Rec0 = #{
        ?id := SessionId,
        ?metadata := Metadata,
        ?subscriptions := Subs0,
        ?subscription_states := SubStates0,
        ?streams := Streams0,
        ?seqnos := SeqNos0,
        ?ranks := Ranks0,
        ?awaiting_rel := AwaitingRels0
    },
    Opts
) ->
    check_sequence(Rec0),
    {SubsOps, Subs} = pmap_commit(SessionId, Subs0),
    {SubStatesOps, SubStates} = pmap_commit(SessionId, SubStates0),
    {StreamsOps, Streams} = pmap_commit(SessionId, Streams0),
    {SeqNosOps, SeqNos} = pmap_commit(SessionId, SeqNos0),
    {RanksOps, Ranks} = pmap_commit(SessionId, Ranks0),
    {AwaitingRelsOps, AwaitingRels} = pmap_commit(SessionId, AwaitingRels0),
    Rec = Rec0#{
        ?subscriptions := Subs,
        ?subscription_states := SubStates,
        ?streams := Streams,
        ?seqnos := SeqNos,
        ?ranks := Ranks,
        ?awaiting_rel := AwaitingRels,
        ?unset_dirty
    },
    MetadataVal = #{metadata => Metadata, key_mappings => key_mappings(Rec)},
    MetadataOp = to_domain_msg(?metadata_domain, SessionId, _Key = undefined, MetadataVal),
    Res = store_batch(
        SessionId,
        lists:flatten([
            MetadataOp,
            SubsOps,
            SubStatesOps,
            StreamsOps,
            SeqNosOps,
            RanksOps,
            AwaitingRelsOps
        ]),
        Opts
    ),
    case Res of
        {error, unrecoverable, {precondition_failed, _Msg}} ->
            %% Race: the session already exists.
            throw(session_already_exists);
        ok ->
            Rec
    end.

key_mappings(Rec) ->
    lists:foldl(
        fun({Field, Domain}, Acc) ->
            #pmap{key_mapping = KM} = maps:get(Field, Rec),
            Acc#{Domain => KM}
        end,
        #{},
        ?pmaps
    ).

store_batch(SessionId, Batch) ->
    store_batch(SessionId, Batch, _Opts = #{}).

store_batch(SessionId, Batch0, Opts) ->
    EnsureNew = maps:get(ensure_new, Opts, false),
    Batch =
        case EnsureNew of
            true ->
                #dsbatch{
                    operations = Batch0,
                    preconditions = [
                        {unless_exists, matcher(SessionId, ?metadata_domain, ?metadata_domain_bin)}
                    ]
                };
            false ->
                Batch0
        end,
    emqx_ds:store_batch(?DB, Batch, #{sync => true}).

-spec create_new(emqx_persistent_session_ds:id()) -> t().
create_new(SessionId) ->
    delete(SessionId),
    #{
        ?id => SessionId,
        ?metadata => #{},
        ?subscriptions => pmap_open(?subscription_domain, [], #{}),
        ?subscription_states => pmap_open(?subscription_state_domain, [], #{}),
        ?streams => pmap_open(?stream_domain, [], #{}),
        ?seqnos => pmap_open(?seqno_domain, [], #{}),
        ?ranks => pmap_open(?rank_domain, [], #{}),
        ?awaiting_rel => pmap_open(?awaiting_rel_domain, [], #{}),
        ?set_dirty
    }.

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

-spec get_peername(t()) -> emqx_types:peername() | undefined.
get_peername(Rec) ->
    get_meta(?peername, Rec).

-spec set_peername(emqx_types:peername(), t()) -> t().
set_peername(Val, Rec) ->
    set_meta(?peername, Val, Rec).

-spec get_protocol(t()) -> protocol() | undefined.
get_protocol(Rec) ->
    get_meta(?protocol, Rec).

-spec set_protocol(protocol(), t()) -> t().
set_protocol(Val, Rec) ->
    set_meta(?protocol, Val, Rec).

-spec get_clientinfo(t()) -> emqx_maybe:t(emqx_types:clientinfo()).
get_clientinfo(Rec) ->
    get_meta(?clientinfo, Rec).

-spec set_clientinfo(emqx_types:clientinfo(), t()) -> t().
set_clientinfo(Val, Rec) ->
    set_meta(?clientinfo, Val, Rec).

-spec get_will_message(t()) -> emqx_maybe:t(message()).
get_will_message(Rec) ->
    get_meta(?will_message, Rec).

-spec set_will_message(emqx_maybe:t(message()), t()) -> t().
set_will_message(Val, Rec) ->
    set_meta(?will_message, Val, Rec).

-spec clear_will_message_now(emqx_persistent_session_ds:id()) -> ok.
clear_will_message_now(SessionId) when is_binary(SessionId) ->
    case session_restore(SessionId) of
        #{?metadata_domain := [#{val := Metadata0}]} ->
            Metadata = Metadata0#{?will_message => undefined},
            MetadataMsg = to_domain_msg(?metadata_domain, SessionId, _Key = undefined, Metadata),
            ok = store_batch(SessionId, [MetadataMsg]),
            ok;
        _ ->
            ok
    end.

-spec clear_will_message(t()) -> t().
clear_will_message(Rec) ->
    set_will_message(undefined, Rec).

-spec set_offline_info(_Info :: map(), t()) -> t().
set_offline_info(Info, Rec) ->
    set_meta(?offline_info, Info, Rec).

-spec new_id(t()) -> {emqx_persistent_session_ds:subscription_id(), t()}.
new_id(Rec) ->
    LastId =
        case get_meta(?last_id, Rec) of
            undefined -> 0;
            N when is_integer(N) -> N
        end,
    {LastId, set_meta(?last_id, LastId + 1, Rec)}.

%%

-spec get_subscription(emqx_persistent_session_ds:topic_filter(), t()) ->
    emqx_persistent_session_ds_subs:subscription() | undefined.
get_subscription(TopicFilter, Rec) ->
    gen_get(?subscriptions, TopicFilter, Rec).

-spec cold_get_subscription(
    emqx_persistent_session_ds:id(), emqx_types:topic() | emqx_types:share()
) ->
    [emqx_persistent_session_ds_subs:subscription()].
cold_get_subscription(SessionId, Topic) ->
    maybe
        [#{val := #{key_mappings := #{?subscription_domain := KeyMapping}}}] ?=
            read_iterate(SessionId, ?metadata_domain_bin, ?metadata_domain_bin),
        {ok, IntKey} ?= maps:find(Topic, KeyMapping),
        Data = read_iterate(
            SessionId,
            ?subscription_domain_bin,
            key_encode(?subscription_domain, IntKey)
        ),
        lists:map(fun(#{val := V}) -> V end, Data)
    else
        _ -> []
    end.

-spec fold_subscriptions(fun(), Acc, t()) -> Acc.
fold_subscriptions(Fun, Acc, Rec) ->
    gen_fold(?subscriptions, Fun, Acc, Rec).

-spec n_subscriptions(t()) -> non_neg_integer().
n_subscriptions(Rec) ->
    gen_size(?subscriptions, Rec).

-spec total_subscription_count() -> non_neg_integer().
total_subscription_count() ->
    Fun = fun(Data, Acc) -> length(Data) + Acc end,
    read_fold(Fun, 0, '+', ?subscription_domain_bin, '+').

-spec put_subscription(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_subs:subscription(),
    t()
) -> t().
put_subscription(TopicFilter, Subscription, Rec) ->
    gen_put(?subscriptions, TopicFilter, Subscription, Rec).

-spec del_subscription(emqx_persistent_session_ds:topic_filter(), t()) -> t().
del_subscription(TopicFilter, Rec) ->
    gen_del(?subscriptions, TopicFilter, Rec).

%%

-spec get_subscription_state(emqx_persistent_session_ds_subs:subscription_state_id(), t()) ->
    emqx_persistent_session_ds_subs:subscription_state() | undefined.
get_subscription_state(SStateId, Rec) ->
    gen_get(?subscription_states, SStateId, Rec).

-spec cold_get_subscription_state(
    emqx_persistent_session_ds:id(), emqx_persistent_session_ds_subs:subscription_state_id()
) ->
    [emqx_persistent_session_ds_subs:subscription_state()].
cold_get_subscription_state(SessionId, SStateId) ->
    maybe
        [#{val := #{key_mappings := #{?subscription_state_domain := KeyMapping}}}] ?=
            read_iterate(SessionId, ?metadata_domain_bin, ?metadata_domain_bin),
        {ok, IntKey} ?= maps:find(SStateId, KeyMapping),
        Data = read_iterate(
            SessionId,
            ?subscription_state_domain_bin,
            key_encode(?subscription_state_domain, IntKey)
        ),
        lists:map(fun(#{val := V}) -> V end, Data)
    else
        _ -> []
    end.

-spec fold_subscription_states(fun(), Acc, t()) -> Acc.
fold_subscription_states(Fun, Acc, Rec) ->
    gen_fold(?subscription_states, Fun, Acc, Rec).

-spec put_subscription_state(
    emqx_persistent_session_ds_subs:subscription_state_id(),
    emqx_persistent_session_ds_subs:subscription_state(),
    t()
) -> t().
put_subscription_state(SStateId, SState, Rec) ->
    gen_put(?subscription_states, SStateId, SState, Rec).

-spec del_subscription_state(emqx_persistent_session_ds_subs:subscription_state_id(), t()) -> t().
del_subscription_state(SStateId, Rec) ->
    gen_del(?subscription_states, SStateId, Rec).

%%

-type stream_key() :: {emqx_persistent_session_ds:subscription_id(), _StreamId}.

-spec get_stream(stream_key(), t()) ->
    emqx_persistent_session_ds:stream_state() | undefined.
get_stream(Key, Rec) ->
    gen_get(?streams, Key, Rec).

-spec put_stream(stream_key(), emqx_persistent_session_ds:stream_state(), t()) -> t().
put_stream(Key, Val, Rec) ->
    gen_put(?streams, Key, Val, Rec).

-spec del_stream(stream_key(), t()) -> t().
del_stream(Key, Rec) ->
    gen_del(?streams, Key, Rec).

-spec fold_streams(fun(), Acc, t()) -> Acc.
fold_streams(Fun, Acc, Rec) ->
    gen_fold(?streams, Fun, Acc, Rec).

-spec iter_streams(_StartAfter :: stream_key() | beginning, t()) ->
    iter(stream_key(), emqx_persistent_session_ds:stream_state()).
iter_streams(After, Rec) ->
    gen_iter_after(?streams, After, Rec).

-spec n_streams(t()) -> non_neg_integer().
n_streams(Rec) ->
    gen_size(?streams, Rec).

%%

-spec get_seqno(seqno_type(), t()) -> emqx_persistent_session_ds:seqno() | undefined.
get_seqno(Key, Rec) ->
    gen_get(?seqnos, Key, Rec).

-spec put_seqno(seqno_type(), emqx_persistent_session_ds:seqno(), t()) -> t().
put_seqno(Key, Val, Rec) ->
    gen_put(?seqnos, Key, Val, Rec).

%%

-type rank_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:rank_x()}.

-spec get_rank(rank_key(), t()) -> integer() | undefined.
get_rank(Key, Rec) ->
    gen_get(?ranks, Key, Rec).

-spec put_rank(rank_key(), integer(), t()) -> t().
put_rank(Key, Val, Rec) ->
    gen_put(?ranks, Key, Val, Rec).

-spec del_rank(rank_key(), t()) -> t().
del_rank(Key, Rec) ->
    gen_del(?ranks, Key, Rec).

-spec fold_ranks(fun(), Acc, t()) -> Acc.
fold_ranks(Fun, Acc, Rec) ->
    gen_fold(?ranks, Fun, Acc, Rec).

%%

-spec get_awaiting_rel(emqx_types:packet_id(), t()) -> integer() | undefined.
get_awaiting_rel(Key, Rec) ->
    gen_get(?awaiting_rel, Key, Rec).

-spec put_awaiting_rel(emqx_types:packet_id(), _Timestamp :: integer(), t()) -> t().
put_awaiting_rel(Key, Val, Rec) ->
    gen_put(?awaiting_rel, Key, Val, Rec).

-spec del_awaiting_rel(emqx_types:packet_id(), t()) -> t().
del_awaiting_rel(Key, Rec) ->
    gen_del(?awaiting_rel, Key, Rec).

-spec fold_awaiting_rel(fun(), Acc, t()) -> Acc.
fold_awaiting_rel(Fun, Acc, Rec) ->
    gen_fold(?awaiting_rel, Fun, Acc, Rec).

-spec n_awaiting_rel(t()) -> non_neg_integer().
n_awaiting_rel(Rec) ->
    gen_size(?awaiting_rel, Rec).

%%

-spec iter_next(iter(K, V)) -> {K, V, iter(K, V)} | none.
iter_next(#{it := InnerIt0, inv_key_mapping := InvKeyMapping} = It0) ->
    case gen_iter_next(InnerIt0) of
        none ->
            none;
        {IntKey, Value, InnerIt} ->
            Key = maps:get(IntKey, InvKeyMapping),
            {Key, Value, It0#{it := InnerIt}}
    end.

%%

-spec make_session_iterator() -> session_iterator().
make_session_iterator() ->
    %% NB. This hides the existence of streams.  Users will need to start iteration
    %% again to see new streams, if any.
    %% NB. This is not assumed to be stored permanently anywhere.
    Time = ?TS,
    TopicFilter = [
        ?metadata_domain_bin,
        '+',
        ?metadata_domain_bin
    ],
    Iterators = lists:map(
        fun({_Rank, Stream}) ->
            {ok, Iterator} = emqx_ds:make_iterator(?DB, Stream, TopicFilter, Time),
            Iterator
        end,
        emqx_ds:get_streams(?DB, TopicFilter, Time)
    ),
    #{its => Iterators}.

-spec session_iterator_next(session_iterator(), pos_integer()) ->
    {[{emqx_persistent_session_ds:id(), metadata()}], session_iterator() | '$end_of_table'}.
session_iterator_next(Cursor, N) ->
    session_iterator_next(Cursor, N, []).

%% Note: ordering is not respected here.
session_iterator_next(#{its := [It | Rest]} = Cursor, 0, Acc) ->
    %% Peek the next item to detect end of table.
    case emqx_ds:next(?DB, It, 1) of
        {ok, end_of_stream} ->
            session_iterator_next(Cursor#{its := Rest}, 0, Acc);
        {ok, _NewIt, []} ->
            session_iterator_next(Cursor#{its := Rest}, 0, Acc);
        {ok, _NewIt, _Batch} ->
            {Acc, Cursor}
    end;
session_iterator_next(_Cursor, 0, Acc) ->
    {Acc, '$end_of_table'};
session_iterator_next('$end_of_table', _N, Acc) ->
    {Acc, '$end_of_table'};
session_iterator_next(#{its := []}, _N, Acc) ->
    {Acc, '$end_of_table'};
session_iterator_next(#{its := [It | Rest]} = Cursor0, N, Acc) ->
    case emqx_ds:next(?DB, It, N) of
        {ok, end_of_stream} ->
            session_iterator_next(Cursor0#{its := Rest}, N, Acc);
        {ok, _NewIt, []} ->
            session_iterator_next(Cursor0#{its := Rest}, N, Acc);
        {ok, NewIt, Batch} ->
            NumBatch = length(Batch),
            SessionIds = lists:map(
                fun({_DSKey, Msg}) ->
                    #{session_id := Id} = Data = from_domain_msg(Msg),
                    Val = unwrap_value(Data),
                    {Id, Val}
                end,
                Batch
            ),
            session_iterator_next(
                Cursor0#{its := [NewIt | Rest]}, N - NumBatch, SessionIds ++ Acc
            )
    end.

unwrap_value(#{domain := ?metadata_domain, val := #{metadata := Metadata}}) ->
    Metadata;
unwrap_value(#{val := Val}) ->
    Val.

%%================================================================================
%% Internal functions
%%================================================================================

%% All mnesia reads and writes are passed through this function.
%% Backward compatiblity issues can be handled here.
val_encode(_Domain, Term) ->
    term_to_binary(Term).

val_decode(_Domain, Bin) ->
    binary_to_term(Bin).

-spec key_encode(domain(), term()) -> binary().
key_encode(?metadata_domain, _Key) ->
    ?metadata_domain_bin;
key_encode(?stream_domain, Key) ->
    %% The generated binary might still contain `$/', which would be confused with an
    %% extra topic level.
    binary:encode_hex(Key, uppercase);
key_encode(_Domain, Key) ->
    integer_to_binary(Key).

-spec key_decode(domain(), binary()) -> term().
key_decode(?metadata_domain, Bin) ->
    Bin;
key_decode(?stream_domain, Bin) ->
    binary:decode_hex(Bin);
key_decode(_Domain, Bin) ->
    binary_to_integer(Bin).

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

gen_size(Field, Rec) ->
    check_sequence(Rec),
    pmap_size(maps:get(Field, Rec)).

gen_iter_after(Field, After, Rec) ->
    check_sequence(Rec),
    pmap_iter_after(After, maps:get(Field, Rec)).

gen_iter_next(It) ->
    %% NOTE: Currently, gbt iterators is the only type of iterators.
    gbt_iter_next(It).

-spec update_pmaps(fun((pmap(_K, _V) | undefined, atom()) -> term()), map()) -> map().
update_pmaps(Fun, Map) ->
    lists:foldl(
        fun({MapKey, Domain}, Acc) ->
            OldVal = maps:get(MapKey, Map, undefined),
            Val = Fun(OldVal, Domain),
            maps:put(MapKey, Val, Acc)
        end,
        Map,
        ?pmaps
    ).

%%

%% @doc Open a PMAP and fill the clean area with the data from DB.
%% This functtion should be ran in a transaction.
invert_key_mapping(KeyMapping) ->
    maps:fold(fun(K, IntK, AccIn) -> AccIn#{IntK => K} end, #{}, KeyMapping).

-spec pmap_open(domain(), [data()], #{K => internal_key(K)}) -> pmap(K, _V).
pmap_open(Domain, Data0, KeyMapping0) ->
    InvKeyMapping = invert_key_mapping(KeyMapping0),
    {Data, KeyMapping} =
        lists:mapfoldl(
            fun(#{key := IntK, val := V}, AccIn) ->
                %% Drop keys that are no longer used.
                K = maps:get(IntK, InvKeyMapping),
                Acc = AccIn#{K => IntK},
                {{IntK, V}, Acc}
            end,
            #{},
            Data0
        ),
    Clean = cache_from_list(Domain, Data),
    #pmap{
        domain = Domain,
        key_mapping = KeyMapping,
        cache = Clean,
        dirty = #{}
    }.

-spec pmap_get(K, pmap(K, V)) -> V | undefined.
pmap_get(K, #pmap{domain = Domain, key_mapping = KeyMapping, cache = Cache}) when
    is_map_key(K, KeyMapping)
->
    IntK = maps:get(K, KeyMapping),
    cache_get(Domain, IntK, Cache);
pmap_get(_K, _Pmap) ->
    undefined.

-spec pmap_put(K, V, pmap(K, V)) -> pmap(K, V).
pmap_put(
    K, V, Pmap = #pmap{domain = Domain, key_mapping = KeyMapping0, dirty = Dirty, cache = Cache}
) ->
    {IntK, KeyMapping} = get_or_gen_internal_key(K, KeyMapping0, Domain, Cache),
    Pmap#pmap{
        key_mapping = KeyMapping,
        cache = cache_put(Domain, IntK, V, Cache),
        dirty = Dirty#{IntK => dirty}
    }.

get_or_gen_internal_key(K, KeyMapping, _Domain, _Cache) when
    is_map_key(K, KeyMapping)
->
    IntK = maps:get(K, KeyMapping),
    {IntK, KeyMapping};
get_or_gen_internal_key(K, KeyMapping0, Domain, Cache) ->
    IntK = gen_internal_key(Domain, K),
    case cache_has_key(Domain, IntK, Cache) of
        true ->
            %% collision (node restarted?); just try again
            get_or_gen_internal_key(K, KeyMapping0, Domain, Cache);
        false ->
            KeyMapping = KeyMapping0#{K => IntK},
            {IntK, KeyMapping}
    end.

gen_internal_key(?stream_domain, {Rank, _Stream}) ->
    LSB = erlang:unique_integer(),
    <<Rank:64, LSB:64>>;
gen_internal_key(_Domain, _K) ->
    erlang:unique_integer().

-spec pmap_del(K, pmap(K, V)) -> pmap(K, V).
pmap_del(
    Key,
    Pmap = #pmap{domain = Domain, key_mapping = KeyMapping, dirty = Dirty, cache = Cache}
) when is_map_key(Key, KeyMapping) ->
    IntK = maps:get(Key, KeyMapping),
    Pmap#pmap{
        cache = cache_remove(Domain, IntK, Cache),
        dirty = Dirty#{IntK => del}
    };
pmap_del(_Key, Pmap) ->
    Pmap.

-spec pmap_fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
pmap_fold(Fun, Acc, #pmap{domain = Domain, key_mapping = KeyMapping, cache = Cache}) ->
    cache_fold(Domain, Fun, Acc, KeyMapping, Cache).

-spec pmap_commit(emqx_persistent_session_ds:id(), pmap(K, V)) ->
    {[emqx_ds:operation()], pmap(K, V)}.
pmap_commit(
    SessionId, Pmap = #pmap{domain = Domain, dirty = Dirty, cache = Cache}
) ->
    Out =
        maps:fold(
            fun
                (K, del, Acc) ->
                    [{delete, matcher(SessionId, Domain, K)} | Acc];
                (K, dirty, Acc) ->
                    V = cache_get(Domain, K, Cache),
                    Msg = to_domain_msg(Domain, SessionId, K, V),
                    [Msg | Acc]
            end,
            [],
            Dirty
        ),
    {Out, Pmap#pmap{
        dirty = #{}
    }}.

matcher(SessionId, Domain, Key) ->
    #message_matcher{
        from = SessionId,
        topic = to_topic(Domain, SessionId, key_encode(Domain, Key)),
        payload = '_',
        timestamp = ?TS
    }.

-spec pmap_format(pmap(_K, _V)) -> map().
pmap_format(#pmap{domain = Domain, key_mapping = KeyMapping, cache = Cache}) ->
    InvKeyMapping = invert_key_mapping(KeyMapping),
    cache_format(Domain, InvKeyMapping, Cache).

-spec pmap_size(pmap(_K, _V)) -> non_neg_integer().
pmap_size(#pmap{domain = Domain, cache = Cache}) ->
    cache_size(Domain, Cache).

pmap_iter_after(beginning, #pmap{domain = Domain, key_mapping = KeyMapping, cache = Cache}) ->
    %% NOTE: Only valid for gbt-backed PMAPs.
    gbt = cache_data_type(Domain),
    It = gb_trees:iterator(Cache),
    InvKeyMapping = invert_key_mapping(KeyMapping),
    #{it => It, inv_key_mapping => InvKeyMapping};
pmap_iter_after(AfterExt, #pmap{domain = Domain, key_mapping = KeyMapping, cache = Cache}) ->
    %% NOTE: Only valid for gbt-backed PMAPs.
    gbt = cache_data_type(Domain),
    AfterInt = maps:get(AfterExt, KeyMapping, undefined),
    It = gbt_iter_after(AfterInt, Cache),
    InvKeyMapping = invert_key_mapping(KeyMapping),
    #{it => It, inv_key_mapping => InvKeyMapping}.

%%

cache_data_type(?stream_domain) -> gbt;
cache_data_type(_Domain) -> map.

cache_from_list(?stream_domain, L) ->
    gbt_from_list(L);
cache_from_list(_Domain, L) ->
    maps:from_list(L).

cache_get(?stream_domain, K, Cache) ->
    gbt_get(K, Cache, undefined);
cache_get(_Domain, K, Cache) ->
    maps:get(K, Cache, undefined).

cache_put(?stream_domain, K, V, Cache) ->
    gbt_put(K, V, Cache);
cache_put(_Domain, K, V, Cache) ->
    maps:put(K, V, Cache).

cache_remove(?stream_domain, K, Cache) ->
    gbt_remove(K, Cache);
cache_remove(_Domain, K, Cache) ->
    maps:remove(K, Cache).

cache_fold(?stream_domain, Fun, Acc, KeyMapping, Cache) ->
    gbt_fold(Fun, Acc, KeyMapping, Cache);
cache_fold(_Domain, FunIn, Acc, KeyMapping, Cache) ->
    InvKeyMapping = invert_key_mapping(KeyMapping),
    Fun = fun(IntK, V, AccIn) ->
        K = maps:get(IntK, InvKeyMapping),
        FunIn(K, V, AccIn)
    end,
    maps:fold(Fun, Acc, Cache).

cache_has_key(?stream_domain, Key, Cache) ->
    gb_trees:is_defined(Key, Cache);
cache_has_key(_Domain, Key, Cache) ->
    is_map_key(Key, Cache).

cache_format(?stream_domain, InvKeyMapping, Cache) ->
    lists:map(
        fun({IntK, V}) ->
            K = maps:get(IntK, InvKeyMapping),
            {K, V}
        end,
        gbt_format(Cache)
    );
cache_format(_Domain, InvKeyMapping, Cache) ->
    maps:fold(
        fun(IntK, V, Acc) ->
            K = maps:get(IntK, InvKeyMapping),
            Acc#{K => V}
        end,
        #{},
        Cache
    ).

cache_size(?stream_domain, Cache) ->
    gbt_size(Cache);
cache_size(_Domain, Cache) ->
    maps:size(Cache).

%% PMAP Cache implementation backed by `gb_trees'.
%% Supports iteration starting from specific key.

gbt_from_list(L) ->
    lists:foldl(
        fun({K, V}, Acc) -> gb_trees:insert(K, V, Acc) end,
        gb_trees:empty(),
        L
    ).

gbt_get(K, Cache, undefined) ->
    case gb_trees:lookup(K, Cache) of
        none -> undefined;
        {_, V} -> V
    end.

gbt_put(K, V, Cache) ->
    gb_trees:enter(K, V, Cache).

gbt_remove(K, Cache) ->
    gb_trees:delete_any(K, Cache).

gbt_format(Cache) ->
    gb_trees:to_list(Cache).

gbt_fold(Fun, Acc, KeyMapping, Cache) ->
    InvKeyMapping = invert_key_mapping(KeyMapping),
    It = gb_trees:iterator(Cache),
    gbt_fold_iter(Fun, Acc, InvKeyMapping, It).

gbt_fold_iter(Fun, Acc, InvKeyMapping, It0) ->
    case gb_trees:next(It0) of
        {IntK, V, It} ->
            K = maps:get(IntK, InvKeyMapping),
            gbt_fold_iter(Fun, Fun(K, V, Acc), InvKeyMapping, It);
        _ ->
            Acc
    end.

gbt_size(Cache) ->
    gb_trees:size(Cache).

gbt_iter_after(After, Cache) ->
    It0 = gb_trees:iterator_from(After, Cache),
    case gb_trees:next(It0) of
        {After, _, It} ->
            It;
        _ ->
            It0
    end.

gbt_iter_next(It) ->
    gb_trees:next(It).

session_restore(SessionId) ->
    Empty = maps:from_keys(
        [
            ?metadata_domain,
            ?subscription_domain,
            ?subscription_state_domain,
            ?stream_domain,
            ?rank_domain,
            ?seqno_domain,
            ?awaiting_rel_domain
        ],
        []
    ),
    Data = maps:groups_from_list(
        fun(#{domain := Domain}) ->
            Domain
        end,
        read_iterate(SessionId, _Domain = '+', _Key = '+')
    ),
    maps:merge(Empty, Data).

%%

to_domain_msg(Domain, SessionId, IntKey, Val) ->
    #message{
        %% unused; empty binary to satisfy dialyzer
        id = <<>>,
        timestamp = ?TS,
        from = SessionId,
        topic = to_topic(Domain, SessionId, key_encode(Domain, IntKey)),
        payload = val_encode(Domain, Val)
    }.

from_domain_msg(#message{topic = Topic, payload = Bin}) ->
    #{
        domain := Domain,
        session_id := _SessionId,
        key := _Key
    } = Data = domain_topic_decode(Topic),
    Data#{val => val_decode(Domain, Bin)}.

to_topic(Domain, SessionId0, BinKey) when is_binary(BinKey) ->
    SessionId = emqx_http_lib:uri_encode(SessionId0),
    emqx_topic:join([
        atom_to_binary(Domain),
        SessionId,
        BinKey
    ]).

domain_topic_decode(Topic) ->
    [DomainBin, SessionId, Bin] = emqx_topic:tokens(Topic),
    case DomainBin of
        _ when
            DomainBin =:= ?metadata_domain_bin;
            DomainBin =:= ?subscription_domain_bin;
            DomainBin =:= ?subscription_state_domain_bin;
            DomainBin =:= ?stream_domain_bin;
            DomainBin =:= ?rank_domain_bin;
            DomainBin =:= ?seqno_domain_bin;
            DomainBin =:= ?awaiting_rel_domain_bin
        ->
            Domain = binary_to_existing_atom(DomainBin),
            #{
                domain => Domain,
                session_id => emqx_http_lib:uri_decode(SessionId),
                key => key_decode(Domain, Bin)
            }
    end.

-spec read_iterate(emqx_persistent_session_ds:id() | '#' | '+', Word, Word) ->
    [data()]
when
    Word :: binary() | '+' | '#' | ''.
read_iterate(SessionId0, DomainFilter, KeyFilter) ->
    Fun = fun(Data, Acc) -> Data ++ Acc end,
    Acc = [],
    read_fold(Fun, Acc, SessionId0, DomainFilter, KeyFilter).

read_fold(Fun, Acc, SessionId0, DomainFilter, KeyFilter) ->
    Time = ?TS,
    SessionId =
        case is_binary(SessionId0) of
            true -> emqx_http_lib:uri_encode(SessionId0);
            false -> SessionId0
        end,
    TopicFilter = [DomainFilter, SessionId, KeyFilter],
    Iterators = lists:map(
        fun({_Rank, Stream}) ->
            {ok, Iterator} = emqx_ds:make_iterator(?DB, Stream, TopicFilter, Time),
            Iterator
        end,
        emqx_ds:get_streams(?DB, TopicFilter, Time)
    ),
    do_read_fold(Fun, Iterators, Acc).

%% Note: no ordering.
do_read_fold(_Fun, [], Acc) ->
    Acc;
do_read_fold(Fun, [Iterator | Rest], Acc) ->
    %% TODO: config?
    BatchSize = 100,
    case emqx_ds:next(?DB, Iterator, BatchSize) of
        {ok, end_of_stream} ->
            do_read_fold(Fun, Rest, Acc);
        {ok, _NewIterator, []} ->
            do_read_fold(Fun, Rest, Acc);
        {ok, NewIterator, Msgs} ->
            Data = lists:map(
                fun({_DSKey, Msg}) -> from_domain_msg(Msg) end,
                Msgs
            ),
            do_read_fold(Fun, [NewIterator | Rest], Fun(Data, Acc))
    end.

delete_iterate(SessionId, DomainFilter, KeyFilter) ->
    Time = 0,
    TopicFilter = [DomainFilter, emqx_http_lib:uri_encode(SessionId), KeyFilter],
    Iterators = lists:map(
        fun(Stream) ->
            {ok, Iterator} = emqx_ds:make_delete_iterator(?DB, Stream, TopicFilter, Time),
            Iterator
        end,
        emqx_ds:get_delete_streams(?DB, TopicFilter, Time)
    ),
    Selector = fun(_) -> true end,
    do_delete_iterate(Iterators, Selector).

do_delete_iterate([], _Selector) ->
    ok;
do_delete_iterate([Iterator | Rest], Selector) ->
    %% TODO: config?
    BatchSize = 100,
    case emqx_ds:delete_next(?DB, Iterator, Selector, BatchSize) of
        {ok, end_of_stream} ->
            do_delete_iterate(Rest, Selector);
        {ok, _NewIterator, 0} ->
            do_delete_iterate(Rest, Selector);
        {ok, NewIterator, _NDeleted} ->
            do_delete_iterate([NewIterator | Rest], Selector)
    end.

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
