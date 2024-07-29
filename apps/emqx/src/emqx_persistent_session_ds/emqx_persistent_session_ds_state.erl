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

-ifdef(STORE_STATE_IN_DS).
-export([open_db/1]).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
-export([create_tables/0]).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-export([open/1, create_new/1, delete/1, commit/1, format/1, print_session/1, list_sessions/0]).
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
-ifdef(STORE_STATE_IN_DS).
-export([to_domain_msg/4, from_domain_msg/1]).
%% END ifdef(STORE_STATE_IN_DS).
-endif.
%% END ifdef(TEST).
-endif.

%%================================================================================
%% Type declarations
%%================================================================================

-define(DB, ?DURABLE_SESSION_STATE).

-type message() :: emqx_types:message().

-opaque iter(K, V) :: gb_trees:iter(K, V).

-opaque session_iterator() :: emqx_persistent_session_ds:id() | '$end_of_table'.

-ifndef(STORE_STATE_IN_DS).
%% Generic key-value wrapper that is used for exporting arbitrary
%% terms to mnesia:
-record(kv, {k, v}).
-endif.

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
        cache :: #{K => V} | gb_trees:tree(K, V),
        dirty :: #{K => dirty | del}
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

-ifdef(STORE_STATE_IN_DS).
-define(session_topic_ns, <<"session">>).
-define(metadata_domain, metadata).
-define(metadata_domain_bin, <<"metadata">>).
-define(subscription_domain, subscription).
-define(subscription_domain_bin, <<"subscription">>).
-define(subscription_state_domain, subscription_state).
-define(subscription_state_domain_bin, <<"subscription_state">>).
-define(stream_domain, stream).
-define(rank_domain, rank).
-define(seqno_domain, seqno).
-define(awaiting_rel_domain, awaiting_rel).
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
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
-define(session_tab, emqx_ds_session_tab).
-define(subscription_tab, emqx_ds_session_subscriptions).
-define(subscription_states_tab, emqx_ds_session_subscription_states).
-define(stream_tab, emqx_ds_session_streams).
-define(seqno_tab, emqx_ds_session_seqnos).
-define(rank_tab, emqx_ds_session_ranks).
-define(awaiting_rel_tab, emqx_ds_session_awaiting_rel).

-define(pmaps, [
    {?subscriptions, ?subscription_tab},
    {?subscription_states, ?subscription_states_tab},
    {?streams, ?stream_tab},
    {?seqnos, ?seqno_tab},
    {?ranks, ?rank_tab},
    {?awaiting_rel, ?awaiting_rel_tab}
]).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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

-ifdef(STORE_STATE_IN_DS).
-spec open_db(emqx_ds:create_db_opts()) -> ok.
open_db(Config) ->
    emqx_ds:open_db(?DB, Config).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
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
    {_, PmapTables} = lists:unzip(?pmaps),
    [create_kv_pmap_table(Table) || Table <- PmapTables],
    mria:wait_for_tables([?session_tab | PmapTables]).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec open(emqx_persistent_session_ds:id()) -> {ok, t()} | undefined.
-ifdef(STORE_STATE_IN_DS).
open(SessionId) ->
    case session_restore(SessionId) of
        #{
            ?metadata_domain := [#{val := Metadata}],
            ?subscription_domain := Subs,
            ?subscription_state_domain := SubStates,
            ?stream_domain := Streams,
            ?rank_domain := Ranks,
            ?seqno_domain := Seqnos,
            ?awaiting_rel_domain := AwaitingRels
        } ->
            Rec = #{
                ?id => SessionId,
                ?metadata => Metadata,
                ?subscriptions => pmap_open(?subscription_domain, Subs),
                ?subscription_states => pmap_open(?subscription_state_domain, SubStates),
                ?streams => pmap_open(?stream_domain, Streams),
                ?seqnos => pmap_open(?seqno_domain, Seqnos),
                ?ranks => pmap_open(?rank_domain, Ranks),
                ?awaiting_rel => pmap_open(?awaiting_rel_domain, AwaitingRels),
                ?unset_dirty
            },
            {ok, Rec};
        _ ->
            undefined
    end.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
open(SessionId) ->
    ro_transaction(fun() ->
        case kv_restore(?session_tab, SessionId) of
            [Metadata] ->
                Rec = update_pmaps(
                    fun(_Pmap, Table) ->
                        pmap_open(Table, SessionId)
                    end,
                    #{
                        ?id => SessionId,
                        ?metadata => Metadata,
                        ?unset_dirty
                    }
                ),
                {ok, Rec};
            [] ->
                undefined
        end
    end).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
        fun(Pmap, _Table) ->
            pmap_format(Pmap)
        end,
        maps:without([id, dirty], Rec)
    ).

-spec list_sessions() -> [emqx_persistent_session_ds:id()].
-ifdef(STORE_STATE_IN_DS).
list_sessions() ->
    lists:map(
        fun(#{session_id := Id}) -> Id end,
        read_iterate('+', [?metadata_domain_bin, ?metadata_domain_bin])
    ).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
list_sessions() ->
    mnesia:dirty_all_keys(?session_tab).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec delete(emqx_persistent_session_ds:id()) -> ok.
-ifdef(STORE_STATE_IN_DS).
delete(Id) ->
    delete_iterate(Id, ['#']).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
delete(Id) ->
    transaction(
        fun() ->
            [kv_pmap_delete(Table, Id) || {_, Table} <- ?pmaps],
            mnesia:delete(?session_tab, Id, write)
        end
    ).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec commit(t()) -> t().
-ifdef(STORE_STATE_IN_DS).
commit(Rec = #{dirty := false}) ->
    Rec;
commit(
    Rec = #{
        ?id := SessionId,
        ?metadata := Metadata,
        ?subscriptions := Subs0,
        ?subscription_states := SubStates0,
        ?streams := Streams0,
        ?seqnos := SeqNos0,
        ?ranks := Ranks0,
        ?awaiting_rel := AwaitingRels0
    }
) ->
    check_sequence(Rec),
    MetadataMsg = to_domain_msg(?metadata_domain, SessionId, _Key = undefined, Metadata),
    {{SubsMsgs, SubsDel}, Subs} = pmap_commit(SessionId, Subs0),
    {{SubStatesMsgs, SubStatesDel}, SubStates} = pmap_commit(SessionId, SubStates0),
    {{StreamsMsgs, StreamsDel}, Streams} = pmap_commit(SessionId, Streams0),
    {{SeqNosMsgs, SeqNosDel}, SeqNos} = pmap_commit(SessionId, SeqNos0),
    {{RanksMsgs, RanksDel}, Ranks} = pmap_commit(SessionId, Ranks0),
    {{AwaitingRelsMsgs, AwaitingRelsDel}, AwaitingRels} = pmap_commit(SessionId, AwaitingRels0),
    delete_specific_keys(SessionId, ?subscription_domain, SubsDel),
    delete_specific_keys(SessionId, ?subscription_state_domain, SubStatesDel),
    delete_specific_keys(SessionId, ?stream_domain, StreamsDel),
    delete_specific_keys(SessionId, ?seqno_domain, SeqNosDel),
    delete_specific_keys(SessionId, ?rank_domain, RanksDel),
    delete_specific_keys(SessionId, ?awaiting_rel_domain, AwaitingRelsDel),
    ok = store_batch(
        [MetadataMsg] ++
            SubsMsgs ++
            SubStatesMsgs ++
            StreamsMsgs ++
            SeqNosMsgs ++ RanksMsgs ++ AwaitingRelsMsgs
    ),
    Rec#{
        ?subscriptions := Subs,
        ?subscription_states := SubStates,
        ?streams := Streams,
        ?seqnos := SeqNos,
        ?ranks := Ranks,
        ?awaiting_rel := AwaitingRels,
        ?unset_dirty
    }.

store_batch(Batch0) ->
    Batch = [{emqx_message:timestamp(Msg, microsecond), Msg} || Msg <- Batch0],
    emqx_ds:store_batch(?DB, Batch, #{atomic => true}).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
commit(Rec = #{dirty := false}) ->
    Rec;
commit(
    Rec = #{
        ?id := SessionId,
        ?metadata := Metadata
    }
) ->
    check_sequence(Rec),
    transaction(fun() ->
        kv_persist(?session_tab, SessionId, Metadata),
        update_pmaps(
            fun(Pmap, _Table) ->
                pmap_commit(SessionId, Pmap)
            end,
            Rec#{?unset_dirty}
        )
    end).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec create_new(emqx_persistent_session_ds:id()) -> t().
-ifdef(STORE_STATE_IN_DS).
create_new(SessionId) ->
    delete(SessionId),
    #{
        ?id => SessionId,
        ?metadata => #{},
        ?subscriptions => pmap_open(?subscription_domain, []),
        ?subscription_states => pmap_open(?subscription_state_domain, []),
        ?streams => pmap_open(?stream_domain, []),
        ?seqnos => pmap_open(?seqno_domain, []),
        ?ranks => pmap_open(?rank_domain, []),
        ?awaiting_rel => pmap_open(?awaiting_rel_domain, []),
        ?set_dirty
    }.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
create_new(SessionId) ->
    transaction(fun() ->
        delete(SessionId),
        update_pmaps(
            fun(_Pmap, Table) ->
                pmap_open(Table, SessionId)
            end,
            #{
                ?id => SessionId,
                ?metadata => #{},
                ?set_dirty
            }
        )
    end).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
-ifdef(STORE_STATE_IN_DS).
clear_will_message_now(SessionId) when is_binary(SessionId) ->
    case session_restore(SessionId) of
        #{?metadata_domain := [#{val := Metadata0}]} ->
            Metadata = Metadata0#{?will_message => undefined},
            MetadataMsg = to_domain_msg(?metadata_domain, SessionId, _Key = undefined, Metadata),
            ok = store_batch([MetadataMsg]),
            ok;
        _ ->
            ok
    end.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
clear_will_message_now(SessionId) when is_binary(SessionId) ->
    transaction(fun() ->
        case kv_restore(?session_tab, SessionId) of
            [Metadata0] ->
                Metadata = Metadata0#{?will_message => undefined},
                kv_persist(?session_tab, SessionId, Metadata),
                ok;
            [] ->
                ok
        end
    end).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
-ifdef(STORE_STATE_IN_DS).
cold_get_subscription(SessionId, Topic) ->
    Data = read_iterate(SessionId, [
        ?subscription_domain_bin, key_encode(?subscription_domain, Topic)
    ]),
    lists:map(fun(#{val := V}) -> V end, Data).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
cold_get_subscription(SessionId, Topic) ->
    kv_pmap_read(?subscription_tab, SessionId, Topic).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec fold_subscriptions(fun(), Acc, t()) -> Acc.
fold_subscriptions(Fun, Acc, Rec) ->
    gen_fold(?subscriptions, Fun, Acc, Rec).

-spec n_subscriptions(t()) -> non_neg_integer().
n_subscriptions(Rec) ->
    gen_size(?subscriptions, Rec).

-spec total_subscription_count() -> non_neg_integer().
-ifdef(STORE_STATE_IN_DS).
total_subscription_count() ->
    Fun = fun(Data, Acc) -> length(Data) + Acc end,
    read_fold(Fun, 0, '+', [?subscription_domain_bin, '+']).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
total_subscription_count() ->
    mria:async_dirty(?DS_MRIA_SHARD, fun() ->
        mnesia:foldl(fun(#kv{}, Acc) -> Acc + 1 end, 0, ?subscription_tab)
    end).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
-ifdef(STORE_STATE_IN_DS).
cold_get_subscription_state(SessionId, SStateId) ->
    Data = read_iterate(SessionId, [
        ?subscription_state_domain_bin, key_encode(?subscription_state_domain, SStateId)
    ]),
    lists:map(fun(#{val := V}) -> V end, Data).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
cold_get_subscription_state(SessionId, SStateId) ->
    kv_pmap_read(?subscription_states_tab, SessionId, SStateId).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
    %% NOTE
    %% No special handling for `beginning', as it always compares less
    %% than any `stream_key()'.
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
iter_next(It0) ->
    gen_iter_next(It0).

%%

-spec make_session_iterator() -> session_iterator().
-ifdef(STORE_STATE_IN_DS).
make_session_iterator() ->
    %% NB. This hides the existence of streams.  Users will need to start iteration
    %% again to see new streams, if any.
    %% NB. This is not assumed to be stored permanently anywhere.
    Time = 0,
    TopicFilter = [
        <<"session">>,
        '+',
        ?metadata_domain_bin,
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
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
make_session_iterator() ->
    mnesia:dirty_first(?session_tab).
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec session_iterator_next(session_iterator(), pos_integer()) ->
    {[{emqx_persistent_session_ds:id(), metadata()}], session_iterator() | '$end_of_table'}.
-ifdef(STORE_STATE_IN_DS).
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
                    #{session_id := Id, val := Val} = from_domain_msg(Msg),
                    {Id, Val}
                end,
                Batch
            ),
            session_iterator_next(
                Cursor0#{its := [NewIt | Rest]}, N - NumBatch, SessionIds ++ Acc
            )
    end.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
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
%% END ifdef(STORE_STATE_IN_DS).
-endif.

%%================================================================================
%% Internal functions
%%================================================================================

%% All mnesia reads and writes are passed through this function.
%% Backward compatiblity issues can be handled here.
-ifdef(STORE_STATE_IN_DS).
val_encode(_Domain, Term) ->
    term_to_binary(Term).

val_decode(_Domain, Bin) ->
    binary_to_term(Bin).

-spec key_encode(domain(), term()) -> binary().
key_encode(?metadata_domain, _Key) ->
    ?metadata_domain_bin;
key_encode(?subscription_domain, TopicFilterAndSubId) ->
    term_to_topic_level(TopicFilterAndSubId);
key_encode(?subscription_state_domain, SubStateId) ->
    integer_to_binary(SubStateId);
key_encode(?stream_domain, StreamKey) ->
    term_to_topic_level(StreamKey);
key_encode(?rank_domain, RankKey) ->
    term_to_topic_level(RankKey);
key_encode(?seqno_domain, SeqnoType) ->
    integer_to_binary(SeqnoType);
key_encode(?awaiting_rel_domain, PacketId) ->
    integer_to_binary(PacketId).

-spec key_decode(domain(), binary()) -> term().
key_decode(?metadata_domain, Bin) ->
    Bin;
key_decode(?subscription_domain, Bin) ->
    topic_level_to_term(Bin);
key_decode(?subscription_state_domain, Bin) ->
    binary_to_integer(Bin);
key_decode(?stream_domain, Bin) ->
    topic_level_to_term(Bin);
key_decode(?rank_domain, Bin) ->
    topic_level_to_term(Bin);
key_decode(?seqno_domain, Bin) ->
    binary_to_integer(Bin);
key_decode(?awaiting_rel_domain, Bin) ->
    binary_to_integer(Bin).

term_to_topic_level(Term) ->
    base64:encode(term_to_binary(Term), #{mode => urlsafe, padding => false}).

topic_level_to_term(Bin) ->
    binary_to_term(base64:decode(Bin, #{mode => urlsafe, padding => false})).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
encoder(encode, _Table, Term) ->
    Term;
encoder(decode, _Table, Term) ->
    Term.
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
        fun({MapKey, Table}, Acc) ->
            OldVal = maps:get(MapKey, Map, undefined),
            Val = Fun(OldVal, Table),
            maps:put(MapKey, Val, Acc)
        end,
        Map,
        ?pmaps
    ).

%%

%% @doc Open a PMAP and fill the clean area with the data from DB.
%% This functtion should be ran in a transaction.
-ifdef(STORE_STATE_IN_DS).
-spec pmap_open(domain(), [data()]) -> pmap(_K, _V).
pmap_open(Domain, Data0) ->
    Data = lists:map(fun(#{key := K, val := V}) -> {K, V} end, Data0),
    Clean = cache_from_list(Domain, Data),
    #pmap{
        table = Domain,
        cache = Clean,
        dirty = #{}
    }.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
-spec pmap_open(atom(), emqx_persistent_session_ds:id()) -> pmap(_K, _V).
pmap_open(Table, SessionId) ->
    Clean = cache_from_list(Table, kv_pmap_restore(Table, SessionId)),
    #pmap{
        table = Table,
        cache = Clean,
        dirty = #{}
    }.
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec pmap_get(K, pmap(K, V)) -> V | undefined.
pmap_get(K, #pmap{table = Table, cache = Cache}) ->
    cache_get(Table, K, Cache).

-spec pmap_put(K, V, pmap(K, V)) -> pmap(K, V).
pmap_put(K, V, Pmap = #pmap{table = Table, dirty = Dirty, cache = Cache}) ->
    Pmap#pmap{
        cache = cache_put(Table, K, V, Cache),
        dirty = Dirty#{K => dirty}
    }.

-spec pmap_del(K, pmap(K, V)) -> pmap(K, V).
pmap_del(
    Key,
    Pmap = #pmap{table = Table, dirty = Dirty, cache = Cache}
) ->
    Pmap#pmap{
        cache = cache_remove(Table, Key, Cache),
        dirty = Dirty#{Key => del}
    }.

-spec pmap_fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
pmap_fold(Fun, Acc, #pmap{table = Table, cache = Cache}) ->
    cache_fold(Table, Fun, Acc, Cache).

-ifdef(STORE_STATE_IN_DS).
-spec pmap_commit(emqx_persistent_session_ds:id(), pmap(K, V)) ->
    {{[emqx_types:message()], [term()]}, pmap(K, V)}.
pmap_commit(
    SessionId, Pmap = #pmap{table = Domain, dirty = Dirty, cache = Cache}
) ->
    Out =
        maps:fold(
            fun
                (K, del, {AccPersist, AccDel}) ->
                    {AccPersist, [K | AccDel]};
                (K, dirty, {AccPersist, AccDel}) ->
                    V = cache_get(Domain, K, Cache),
                    Msg = to_domain_msg(Domain, SessionId, K, V),
                    {[Msg | AccPersist], AccDel}
            end,
            {[], []},
            Dirty
        ),
    {Out, Pmap#pmap{
        dirty = #{}
    }}.
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.
-spec pmap_commit(emqx_persistent_session_ds:id(), pmap(K, V)) -> pmap(K, V).
pmap_commit(
    SessionId, Pmap = #pmap{table = Tab, dirty = Dirty, cache = Cache}
) ->
    maps:foreach(
        fun
            (K, del) ->
                kv_pmap_delete(Tab, SessionId, K);
            (K, dirty) ->
                V = cache_get(Tab, K, Cache),
                kv_pmap_persist(Tab, SessionId, K, V)
        end,
        Dirty
    ),
    Pmap#pmap{
        dirty = #{}
    }.
%% END ifdef(STORE_STATE_IN_DS).
-endif.

-spec pmap_format(pmap(_K, _V)) -> map().
pmap_format(#pmap{table = Table, cache = Cache}) ->
    cache_format(Table, Cache).

-spec pmap_size(pmap(_K, _V)) -> non_neg_integer().
pmap_size(#pmap{table = Table, cache = Cache}) ->
    cache_size(Table, Cache).

pmap_iter_after(After, #pmap{table = Table, cache = Cache}) ->
    %% NOTE: Only valid for gbt-backed PMAPs.
    gbt = cache_data_type(Table),
    gbt_iter_after(After, Cache).

%%

-ifdef(STORE_STATE_IN_DS).
-define(stream_tab, ?stream_domain).
-endif.

cache_data_type(?stream_tab) -> gbt;
cache_data_type(_Table) -> map.

cache_from_list(?stream_tab, L) ->
    gbt_from_list(L);
cache_from_list(_Table, L) ->
    maps:from_list(L).

cache_get(?stream_tab, K, Cache) ->
    gbt_get(K, Cache, undefined);
cache_get(_Table, K, Cache) ->
    maps:get(K, Cache, undefined).

cache_put(?stream_tab, K, V, Cache) ->
    gbt_put(K, V, Cache);
cache_put(_Table, K, V, Cache) ->
    maps:put(K, V, Cache).

cache_remove(?stream_tab, K, Cache) ->
    gbt_remove(K, Cache);
cache_remove(_Table, K, Cache) ->
    maps:remove(K, Cache).

cache_fold(?stream_tab, Fun, Acc, Cache) ->
    gbt_fold(Fun, Acc, Cache);
cache_fold(_Table, Fun, Acc, Cache) ->
    maps:fold(Fun, Acc, Cache).

cache_format(?stream_tab, Cache) ->
    gbt_format(Cache);
cache_format(_Table, Cache) ->
    Cache.

cache_size(?stream_tab, Cache) ->
    gbt_size(Cache);
cache_size(_Table, Cache) ->
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

gbt_fold(Fun, Acc, Cache) ->
    It = gb_trees:iterator(Cache),
    gbt_fold_iter(Fun, Acc, It).

gbt_fold_iter(Fun, Acc, It0) ->
    case gb_trees:next(It0) of
        {K, V, It} ->
            gbt_fold_iter(Fun, Fun(K, V, Acc), It);
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

-ifdef(STORE_STATE_IN_DS).
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
        read_iterate(SessionId, ['#'])
    ),
    maps:merge(Empty, Data).
%% ELSE ifdef(STORE_STATE_IN_DS).
-else.

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

kv_pmap_read(Table, SessionId, Key) ->
    lists:map(
        fun(#kv{v = Val}) ->
            encoder(decode, Table, Val)
        end,
        mnesia:dirty_read(Table, {SessionId, Key})
    ).

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

%% END ifdef(STORE_STATE_IN_DS).
-endif.

%%

-ifdef(STORE_STATE_IN_DS).
to_domain_msg(Domain, SessionId, Key, Val) ->
    #message{
        %% unused; empty binary to satisfy dialyzer
        id = <<>>,
        timestamp = 0,
        from = SessionId,
        topic = to_topic(Domain, SessionId, key_encode(Domain, Key)),
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
        ?session_topic_ns,
        SessionId,
        atom_to_binary(Domain),
        BinKey
    ]).

domain_topic_decode(Topic) ->
    [<<"session">>, SessionId | Rest] = emqx_topic:tokens(Topic),
    case parse_domain(Rest) of
        [Domain, Bin] when
            Domain =:= ?metadata_domain;
            Domain =:= ?subscription_domain;
            Domain =:= ?subscription_state_domain;
            Domain =:= ?stream_domain;
            Domain =:= ?rank_domain;
            Domain =:= ?seqno_domain;
            Domain =:= ?awaiting_rel_domain
        ->
            #{
                domain => Domain,
                session_id => emqx_http_lib:uri_decode(SessionId),
                key => key_decode(Domain, Bin)
            }
    end.

parse_domain([DomainBin | Rest]) ->
    [binary_to_existing_atom(DomainBin) | Rest].

-spec read_iterate(emqx_persistent_session_ds:id() | '#' | '+', emqx_ds:topic_filter()) ->
    [data()].
read_iterate(SessionId0, TopicFilterWords0) ->
    Fun = fun(Data, Acc) -> Data ++ Acc end,
    Acc = [],
    read_fold(Fun, Acc, SessionId0, TopicFilterWords0).

read_fold(Fun, Acc, SessionId0, TopicFilterWords0) ->
    Time = 0,
    SessionId =
        case is_binary(SessionId0) of
            true -> emqx_http_lib:uri_encode(SessionId0);
            false -> SessionId0
        end,
    TopicFilter = [?session_topic_ns, SessionId | TopicFilterWords0],
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

delete_specific_keys(SessionId, Domain, Keys) when is_list(Keys) ->
    lists:foreach(
        fun(Key) ->
            delete_specific_key(SessionId, Domain, Key)
        end,
        Keys
    ).

delete_specific_key(SessionId, Domain, Key) ->
    KeyBin = key_encode(Domain, Key),
    delete_iterate(SessionId, [atom_to_binary(Domain), KeyBin]).

delete_iterate(SessionId, TopicFilterWords0) ->
    Time = 0,
    TopicFilter = [?session_topic_ns, emqx_http_lib:uri_encode(SessionId) | TopicFilterWords0],
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
%% END ifdef(STORE_STATE_IN_DS).
-endif.

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
