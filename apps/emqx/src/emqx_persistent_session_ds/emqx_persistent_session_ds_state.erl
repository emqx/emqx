%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
%%
-module(emqx_persistent_session_ds_state).

-feature(maybe_expr, enable).
-compile(inline).

-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("session_internals.hrl").

-export([open_db/0]).

-export([
    open/1,
    create_new/1,
    delete/1,
    commit/2,
    on_commit_reply/2,
    format/1,
    print_session/1,
    print_channel/1,
    list_sessions/0
]).
-export([is_dirty/1, checkpoint_ref/1]).
-export([get_created_at/1, set_created_at/2]).
-export([get_last_alive_at/1, set_last_alive_at/2]).
-export([get_expiry_interval/1, set_expiry_interval/2]).
-export([set_offline_info/2, get_offline_info/1]).
-export([get_peername/1, set_peername/2]).
-export([get_protocol/1, set_protocol/2]).
-export([new_id/1]).
-export([get_stream/2, put_stream/3, del_stream/2, fold_streams/3, fold_streams/4, n_streams/1]).
-export([get_seqno/2, put_seqno/3]).
-export([get_rank/2, put_rank/3, del_rank/2, fold_ranks/3, fold_ranks/4]).
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

%% Iterating storage:
-export([make_session_iterator/0, session_iterator_next/2]).
-export([make_subscription_iterator/0, subscription_iterator_next/2]).

-export_type([
    t/0,
    lifetime/0,
    metadata/0,
    seqno_type/0,
    rank_key/0,
    session_iterator/0,
    subscription_iterator/0,
    protocol/0,
    commit_opts/0,
    guard/0
]).

-include("emqx_mqtt.hrl").
-include("session_internals.hrl").
-include("pmap.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-type session_iterator() :: emqx_persistent_session_ds_state_v2:session_iterator().
-type subscription_iterator() :: emqx_persistent_session_ds_state_v2:subscription_iterator().

-define(DB, ?DURABLE_SESSION_STATE).

-type protocol() :: {binary(), emqx_types:proto_ver()}.

-type metadata() ::
    #{
        ?created_at => emqx_persistent_session_ds:timestamp(),
        ?last_alive_at => emqx_persistent_session_ds:timestamp(),
        ?expiry_interval => non_neg_integer(),
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

-type guard() :: emqx_ds_pmap:guard().

%% TODO: move this type to a different module and make it opaque,
%% otherwise someone WILL absolutely use it in the session business
%% code and bypass API.
-type t() :: #{
    ?id := emqx_persistent_session_ds:id(),
    ?collection_guard := guard() | undefined,
    ?collection_dirty := boolean(),
    %% Reference for the ongoing async commit.
    ?checkpoint_ref := reference() | undefined,
    ?metadata := emqx_ds_pmap:pmap(atom(), term()),
    ?subscriptions := emqx_ds_pmap:pmap(
        emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_subs:subscription()
    ),
    ?subscription_states := emqx_ds_pmap:pmap(
        emqx_persistent_session_ds_subs:subscription_state_id(),
        emqx_persistent_session_ds_subs:subscription_state()
    ),
    ?seqnos := emqx_ds_pmap:pmap(seqno_type(), emqx_persistent_session_ds:seqno()),
    %% Fixme: key is actualy `{StreamId :: non_neg_integer(), emqx_ds:stream()}', from
    %% stream scheduler module.
    ?streams := emqx_ds_pmap:pmap(emqx_ds:stream(), emqx_persistent_session_ds:stream_state()),
    ?ranks := emqx_ds_pmap:pmap(term(), integer()),
    ?awaiting_rel := emqx_ds_pmap:pmap(emqx_types:packet_id(), _Timestamp :: integer())
}.

-type lifetime() ::
    %% Session wasn't present before:
    new
    %% Channel is terminating normally:
    | terminate
    %% Client is taking over an existing session:
    | takeover
    %% Checkpoint:
    | up.

-type commit_opts() :: #{lifetime := lifetime(), sync := boolean()}.

%%================================================================================
%% API functions
%%================================================================================

-spec open_db() -> ok.
open_db() ->
    Config = emqx_ds_schema:db_config_sessions(),
    Storage = emqx_ds_pmap:storage_opts(
        #{lts_threshold_spec => {mf, emqx_persistent_session_ds_state_v2, lts_threshold_cb}}
    ),
    emqx_ds:open_db(?DB, Config#{
        atomic_batches => true,
        append_only => false,
        store_ttv => true,
        storage => Storage
    }).

-spec open(emqx_persistent_session_ds:id()) -> {ok, t()} | emqx_ds:error(_) | undefined.
open(SessionId) ->
    ?tp_span(
        psds_open,
        #{id => SessionId},
        emqx_persistent_session_ds_state_v2:open(generation(), SessionId, true)
    ).

-spec print_session(emqx_persistent_session_ds:id()) -> map() | undefined.
print_session(SessionId) ->
    case emqx_persistent_session_ds_state_v2:open(generation(), SessionId, false) of
        undefined ->
            undefined;
        {ok, Session} ->
            format(Session)
    end.

-spec print_channel(emqx_persistent_session_ds:id()) ->
    [{{emqx_persistent_session_ds:id(), undefined}, Info, Stats}]
when
    Info :: map(),
    Stats :: [{atom(), _}].
print_channel(SessionId) ->
    Session = print_session(SessionId),
    get_offline_info(Session).

-spec format(t()) -> map().
format(Rec = #{?id := Id}) ->
    Pmaps = maps:fold(
        fun(MapKey, #pmap{cache = Val}, Acc) ->
            Acc#{MapKey => Val}
        end,
        #{?id => Id},
        maps:without([?id, ?collection_dirty, ?collection_guard, ?checkpoint_ref, ?last_id], Rec)
    ),
    maps:merge(Pmaps, maps:with([?collection_dirty, ?collection_guard, ?checkpoint_ref], Rec)).

-spec list_sessions() -> [emqx_persistent_session_ds:id()].
list_sessions() ->
    emqx_persistent_session_ds_state_v2:list_sessions(generation()).

-spec delete(emqx_persistent_session_ds:id() | t()) -> ok.
delete(Id) when is_binary(Id) ->
    case emqx_persistent_session_ds_state_v2:open(generation(), Id, true) of
        {ok, Rec} ->
            delete(Rec);
        undefined ->
            ok
    end;
delete(Rec) when is_map(Rec) ->
    emqx_persistent_session_ds_state_v2:delete(generation(), Rec).

-spec commit(t(), commit_opts()) ->
    t().
commit(Rec, Opts = #{lifetime := _, sync := _}) ->
    emqx_ds_pmap:collection_check_sequence(Rec),
    emqx_persistent_session_ds_state_v2:commit(generation(), Rec, Opts).

-spec on_commit_reply(term(), t()) -> {ok, t()} | ignore | {error, _}.
on_commit_reply(
    ?ds_tx_commit_reply(Ref, Reply),
    S = #{?id := ClientId, ?checkpoint_ref := Ref}
) ->
    case emqx_ds:tx_commit_outcome(?DB, Ref, Reply) of
        {ok, _CommitSerial} ->
            {ok, S#{?checkpoint_ref := undefined}};
        ?err_rec(Reason) ->
            ?tp(
                warning,
                ?sessds_commit_failure,
                #{
                    recoverable => true,
                    reason => Reason,
                    client => ClientId
                }
            ),
            {ok, S#{?checkpoint_ref := undefined, ?set_dirty}};
        ?err_unrec(Reason) ->
            ?tp(
                error,
                ?sessds_commit_failure,
                #{
                    recoverable => false,
                    reason => Reason,
                    client => ClientId
                }
            ),
            {error, Reason}
    end;
on_commit_reply(_, _) ->
    ignore.

-spec create_new(emqx_persistent_session_ds:id()) -> t().
create_new(SessionId) ->
    #{
        ?id => SessionId,
        ?collection_guard => undefined,
        ?collection_dirty => true,
        ?checkpoint_ref => undefined,
        ?metadata => emqx_persistent_session_ds_state_v2:new_pmap(?metadata),
        ?subscriptions => emqx_persistent_session_ds_state_v2:new_pmap(?subscriptions),
        ?subscription_states => emqx_persistent_session_ds_state_v2:new_pmap(?subscription_states),
        ?seqnos => emqx_persistent_session_ds_state_v2:new_pmap(?seqnos),
        ?streams => emqx_persistent_session_ds_state_v2:new_pmap(?streams),
        ?ranks => emqx_persistent_session_ds_state_v2:new_pmap(?ranks),
        ?awaiting_rel => emqx_persistent_session_ds_state_v2:new_pmap(?awaiting_rel)
    }.

-spec is_dirty(t()) -> boolean().
is_dirty(#{?collection_dirty := Dirty}) ->
    Dirty.

-spec checkpoint_ref(t()) -> undefined | reference().
checkpoint_ref(#{?checkpoint_ref := Dirty}) ->
    Dirty.

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

-spec set_offline_info(_Info :: map(), t()) -> t().
set_offline_info(Info, #{?id := SessionId} = Rec) ->
    emqx_persistent_session_ds_state_v2:set_offline_info(generation(), SessionId, Info),
    Rec.

-spec get_offline_info(FormattedState) ->
    [{{emqx_persistent_session_ds:id(), undefined}, Info, Stats}]
when
    FormattedState :: map(),
    Info :: map(),
    Stats :: [{atom(), _}].
get_offline_info(Rec) ->
    emqx_persistent_session_ds_state_v2:get_offline_info(generation(), Rec).

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
    emqx_ds_pmap:collection_get(?subscriptions, TopicFilter, Rec).

-spec cold_get_subscription(
    emqx_persistent_session_ds:id(), emqx_types:topic() | emqx_types:share()
) ->
    [emqx_persistent_session_ds_subs:subscription()].
cold_get_subscription(SessionId, Topic) ->
    AllSubs = emqx_persistent_session_ds_state_v2:pmap_dirty_read(
        generation(), ?subscriptions, SessionId
    ),
    case AllSubs of
        #{Topic := Val} ->
            [Val];
        #{} ->
            []
    end.

-spec fold_subscriptions(fun(), Acc, t()) -> Acc.
fold_subscriptions(Fun, Acc, Rec) ->
    emqx_ds_pmap:collection_fold(?subscriptions, Fun, Acc, Rec).

-spec n_subscriptions(t()) -> non_neg_integer().
n_subscriptions(Rec) ->
    emqx_ds_pmap:collection_size(?subscriptions, Rec).

-spec total_subscription_count() -> non_neg_integer().
total_subscription_count() ->
    emqx_persistent_session_ds_state_v2:total_subscription_count(generation()).

-spec put_subscription(
    emqx_persistent_session_ds:topic_filter(),
    emqx_persistent_session_ds_subs:subscription(),
    t()
) -> t().
put_subscription(TopicFilter, Subscription, Rec) ->
    emqx_ds_pmap:collection_put(?subscriptions, TopicFilter, Subscription, Rec).

-spec del_subscription(emqx_persistent_session_ds:topic_filter(), t()) -> t().
del_subscription(TopicFilter, Rec) ->
    emqx_ds_pmap:collection_del(?subscriptions, TopicFilter, Rec).

%%

-spec get_subscription_state(emqx_persistent_session_ds_subs:subscription_state_id(), t()) ->
    emqx_persistent_session_ds_subs:subscription_state() | undefined.
get_subscription_state(SStateId, Rec) ->
    emqx_ds_pmap:collection_get(?subscription_states, SStateId, Rec).

-spec cold_get_subscription_state(
    emqx_persistent_session_ds:id(), emqx_persistent_session_ds_subs:subscription_state_id()
) ->
    [emqx_persistent_session_ds_subs:subscription_state()].
cold_get_subscription_state(SessionId, SStateId) ->
    AllSubStates = emqx_persistent_session_ds_state_v2:pmap_dirty_read(
        generation(), ?subscription_states, SessionId
    ),
    case AllSubStates of
        #{SStateId := Val} ->
            [Val];
        #{} ->
            []
    end.

-spec fold_subscription_states(fun(), Acc, t()) -> Acc.
fold_subscription_states(Fun, Acc, Rec) ->
    emqx_ds_pmap:collection_fold(?subscription_states, Fun, Acc, Rec).

-spec put_subscription_state(
    emqx_persistent_session_ds_subs:subscription_state_id(),
    emqx_persistent_session_ds_subs:subscription_state(),
    t()
) -> t().
put_subscription_state(SStateId, SState, Rec) ->
    emqx_ds_pmap:collection_put(?subscription_states, SStateId, SState, Rec).

-spec del_subscription_state(emqx_persistent_session_ds_subs:subscription_state_id(), t()) -> t().
del_subscription_state(SStateId, Rec) ->
    emqx_ds_pmap:collection_del(?subscription_states, SStateId, Rec).

%%

-spec get_stream(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) ->
    emqx_persistent_session_ds:stream_state() | undefined.
get_stream(Key, Rec) ->
    emqx_ds_pmap:collection_get(?streams, Key, Rec).

-spec put_stream(
    emqx_persistent_session_ds_stream_scheduler:stream_key(),
    emqx_persistent_session_ds:stream_state(),
    t()
) -> t().
put_stream(Key, Val, Rec) ->
    emqx_ds_pmap:collection_put(?streams, Key, Val, Rec).

-spec del_stream(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) -> t().
del_stream(Key, Rec) ->
    emqx_ds_pmap:collection_del(?streams, Key, Rec).

-spec fold_streams(
    fun(
        (
            emqx_persistent_session_ds_stream_scheduler:stream_key(),
            emqx_persistent_session_ds:stream_state(),
            Acc
        ) -> Acc
    ),
    Acc,
    t()
) -> Acc.
fold_streams(Fun, Acc, Rec) ->
    emqx_ds_pmap:collection_fold(?streams, Fun, Acc, Rec).

%% @doc Fold streams for a specific subscription id:
-spec fold_streams(
    emqx_persistent_session_ds:subscription_id(),
    fun((_StreamId, emqx_persistent_session_ds:stream_state(), Acc) -> Acc),
    Acc,
    t()
) -> Acc.
fold_streams(SubId, Fun, Acc0, Rec) ->
    %% TODO: find some way to avoid full scan. Is it worth storing
    %% data as map of maps?
    emqx_ds_pmap:collection_fold(
        ?streams,
        fun({SID, StreamId}, Val, Acc) ->
            case SID of
                SubId ->
                    Fun(StreamId, Val, Acc);
                _ ->
                    Acc
            end
        end,
        Acc0,
        Rec
    ).

-spec n_streams(t()) -> non_neg_integer().
n_streams(Rec) ->
    emqx_ds_pmap:collection_size(?streams, Rec).

%%

-spec get_seqno(seqno_type(), t()) -> emqx_persistent_session_ds:seqno() | undefined.
get_seqno(Key, Rec) ->
    emqx_ds_pmap:collection_get(?seqnos, Key, Rec).

-spec put_seqno(seqno_type(), emqx_persistent_session_ds:seqno(), t()) -> t().
put_seqno(Key, Val, Rec) ->
    emqx_ds_pmap:collection_put(?seqnos, Key, Val, Rec).

%%

-type rank_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:shard()}.

-spec get_rank(rank_key(), t()) -> integer() | undefined.
get_rank(Key, Rec) ->
    emqx_ds_pmap:collection_get(?ranks, Key, Rec).

-spec put_rank(rank_key(), integer(), t()) -> t().
put_rank(Key, Val, Rec) ->
    emqx_ds_pmap:collection_put(?ranks, Key, Val, Rec).

-spec del_rank(rank_key(), t()) -> t().
del_rank(Key, Rec) ->
    emqx_ds_pmap:collection_del(?ranks, Key, Rec).

-spec fold_ranks(fun(), Acc, t()) -> Acc.
fold_ranks(Fun, Acc, Rec) ->
    emqx_ds_pmap:collection_fold(?ranks, Fun, Acc, Rec).

%% @doc Fold ranks for a specific subscription ID
-spec fold_ranks(
    emqx_persistent_session_ds:subscription_id(),
    fun((emqx_ds:shard(), emqx_ds:generation(), Acc) -> Acc),
    Acc,
    t()
) -> Acc.
fold_ranks(SubId, Fun, Acc0, Rec) ->
    %% TODO: find some way to avoid full scan.
    emqx_ds_pmap:collection_fold(
        ?ranks,
        fun({SID, RankX}, Val, Acc) ->
            case SID of
                SubId ->
                    Fun(RankX, Val, Acc);
                _ ->
                    Acc
            end
        end,
        Acc0,
        Rec
    ).

%%

-spec get_awaiting_rel(emqx_types:packet_id(), t()) -> integer() | undefined.
get_awaiting_rel(Key, Rec) ->
    emqx_ds_pmap:collection_get(?awaiting_rel, Key, Rec).

-spec put_awaiting_rel(emqx_types:packet_id(), _Timestamp :: integer(), t()) -> t().
put_awaiting_rel(Key, Val, Rec) ->
    emqx_ds_pmap:collection_put(?awaiting_rel, Key, Val, Rec).

-spec del_awaiting_rel(emqx_types:packet_id(), t()) -> t().
del_awaiting_rel(Key, Rec) ->
    emqx_ds_pmap:collection_del(?awaiting_rel, Key, Rec).

-spec fold_awaiting_rel(fun(), Acc, t()) -> Acc.
fold_awaiting_rel(Fun, Acc, Rec) ->
    emqx_ds_pmap:collection_fold(?awaiting_rel, Fun, Acc, Rec).

-spec n_awaiting_rel(t()) -> non_neg_integer().
n_awaiting_rel(Rec) ->
    emqx_ds_pmap:collection_size(?awaiting_rel, Rec).

%%

-spec make_session_iterator() -> session_iterator() | '$end_of_table'.
make_session_iterator() ->
    emqx_persistent_session_ds_state_v2:make_session_iterator(generation()).

-spec session_iterator_next(session_iterator(), pos_integer()) ->
    {[emqx_persistent_session_ds:id()], session_iterator() | '$end_of_table'}.
session_iterator_next(It0, N) ->
    emqx_persistent_session_ds_state_v2:session_iterator_next(generation(), It0, N).

-spec make_subscription_iterator() -> subscription_iterator() | '$end_of_table'.
make_subscription_iterator() ->
    emqx_persistent_session_ds_state_v2:make_subscription_iterator(generation()).

-spec subscription_iterator_next(subscription_iterator(), pos_integer()) ->
    {
        [{emqx_persistent_session_ds:id(), emqx_persistent_session_ds:topic_filter()}],
        subscription_iterator() | '$end_of_table'
    }.
subscription_iterator_next(It0, N) ->
    emqx_persistent_session_ds_state_v2:subscription_iterator_next(generation(), It0, N).

%%================================================================================
%% Internal functions
%%================================================================================

%%

get_meta(Key, Rec) ->
    emqx_ds_pmap:collection_get(?metadata, Key, Rec).

set_meta(Key, Val, Rec) ->
    emqx_ds_pmap:collection_put(?metadata, Key, Val, Rec).

%%

generation() ->
    1.
