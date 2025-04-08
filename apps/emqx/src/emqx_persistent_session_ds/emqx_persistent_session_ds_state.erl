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

-export([open_db/1]).

-export([
    open/1, create_new/1, delete/1, commit/1, commit/2, format/1, print_session/1, list_sessions/0
]).
-export([is_dirty/1]).
-export([get_created_at/1, set_created_at/2]).
-export([get_last_alive_at/1, set_last_alive_at/2]).
-export([get_node_epoch_id/1, set_node_epoch_id/2]).
-export([get_expiry_interval/1, set_expiry_interval/2]).
-export([get_clientinfo/1, set_clientinfo/2]).
-export([get_will_message/1, set_will_message/2, clear_will_message/1, clear_will_message_now/1]).
-export([set_offline_info/2]).
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
%% -export([make_session_iterator/0, session_iterator_next/2]).
%% -export([make_subscription_iterator/0, subscription_iterator_next/2]).

-export_type([
    t/0,
    metadata/0,
    seqno_type/0,
    rank_key/0,
    session_iterator/0,
    protocol/0,
    commit_opts/0
]).

-include("emqx_mqtt.hrl").
-include("session_internals.hrl").
-include("pmap.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/qlc.hrl").

%%================================================================================
%% Type declarations
%%================================================================================

-opaque session_iterator() :: term().

-define(DB, ?DURABLE_SESSION_STATE).

-define(SESSION_REOPEN_RETRY_TIMEOUT, 1_000).
-define(READ_RETRY_TIMEOUT, 100).
-define(MAX_READ_ATTEMPTS, 5).

-type pmap(K, V) ::
    #pmap{
        cache :: #{K => V},
        dirty :: #{K => dirty | {del, emqx_ds:topic()}}
    }.

-type protocol() :: {binary(), emqx_types:proto_ver()}.

-type metadata() ::
    #{
        ?created_at => emqx_persistent_session_ds:timestamp(),
        ?last_alive_at => emqx_persistent_session_ds:timestamp(),
        ?node_epoch_id => emqx_persistent_session_ds_node_heartbeat_worker:epoch_id() | undefined,
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

-type guard() :: binary().

%% TODO: move this type to a different module and make it opaque,
%% otherwise someone WILL absolutely use it in the session business
%% code and bypass API.
-type t() :: #{
    ?id := emqx_persistent_session_ds:id(),
    ?guard := guard(),
    ?dirty := boolean(),
    ?metadata := pmap(atom(), term()),
    ?subscriptions := pmap(
        emqx_persistent_session_ds:topic_filter(), emqx_persistent_session_ds_subs:subscription()
    ),
    ?subscription_states := pmap(
        emqx_persistent_session_ds_subs:subscription_state_id(),
        emqx_persistent_session_ds_subs:subscription_state()
    ),
    ?seqnos := pmap(seqno_type(), emqx_persistent_session_ds:seqno()),
    %% Fixme: key is actualy `{StreamId :: non_neg_integer(), emqx_ds:stream()}', from
    %% stream scheduler module.
    ?streams := pmap(emqx_ds:stream(), emqx_persistent_session_ds:stream_state()),
    ?ranks := pmap(term(), integer()),
    ?awaiting_rel := pmap(emqx_types:packet_id(), _Timestamp :: integer())
}.

%% -if(?EMQX_RELEASE_EDITION == ee).
%% -define(DEFAULT_BACKEND, builtin_raft).
%% -else.
-define(DEFAULT_BACKEND, builtin_local).
%% -endif.

-type lifetime() :: new | terminate | takeover | up.

-type commit_opts() :: #{lifetime := lifetime()}.

%%================================================================================
%% API functions
%%================================================================================

-spec open_db(emqx_ds:create_db_opts()) -> ok.
open_db(Config) ->
    Storage =
        {emqx_ds_storage_skipstream_lts, #{
            master_hash_bits => 128,
            lts_threshold_spec => {mf, emqx_persistent_session_ds_state_v2, lts_threshold_cb}
        }},
    emqx_ds:open_db(?DB, Config#{
        backend => ?DEFAULT_BACKEND,
        atomic_batches => true,
        append_only => false,
        store_blobs => true,
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

-spec format(t()) -> map().
format(Rec = #{?id := Id}) ->
    maps:fold(
        fun(MapKey, #pmap{cache = Val}, Acc) ->
            Acc#{MapKey => Val}
        end,
        #{?id => Id},
        maps:without([?id, ?dirty, ?guard, ?last_id], Rec)
    ).

-spec list_sessions() -> [emqx_persistent_session_ds:id()].
list_sessions() ->
    %% FIXME:
    [].
%% lists:map(
%%     fun(#{session_id := Id}) -> Id end,
%%     read_iterate('+', [?metadata_domain_bin, ?metadata_domain_bin])
%% ).

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

-spec commit(t()) -> t().
commit(Rec) ->
    commit(Rec, #{lifetime => up}).

-spec commit(t(), commit_opts()) -> t().
commit(Rec, Opts = #{lifetime := _}) ->
    check_sequence(Rec),
    ?tp(psds_commit, Rec),
    emqx_persistent_session_ds_state_v2:commit(generation(), Rec, Opts).

-spec create_new(emqx_persistent_session_ds:id()) -> t().
create_new(SessionId) ->
    delete(SessionId),
    #{
        ?id => SessionId,
        ?guard => undefined,
        ?dirty => true,
        ?last_id => 1,
        ?metadata => #pmap{name = ?metadata},
        ?subscriptions => #pmap{name = ?subscriptions},
        ?subscription_states => #pmap{name = ?subscription_states},
        ?seqnos => #pmap{name = ?seqnos},
        ?streams => #pmap{name = ?streams},
        ?ranks => #pmap{name = ?ranks},
        ?awaiting_rel => #pmap{name = ?awaiting_rel}
    }.

-spec is_dirty(t()) -> boolean().
is_dirty(#{?dirty := Dirty}) ->
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

-spec get_node_epoch_id(t()) ->
    emqx_persistent_session_ds_node_heartbeat_worker:epoch_id() | undefined.
get_node_epoch_id(Rec) ->
    get_meta(?node_epoch_id, Rec).

-spec set_node_epoch_id(
    emqx_persistent_session_ds_node_heartbeat_worker:epoch_id() | undefined, t()
) -> t().
set_node_epoch_id(Val, Rec) ->
    set_meta(?node_epoch_id, Val, Rec).

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

-spec get_will_message(t()) -> emqx_maybe:t(emqx_types:message()).
get_will_message(Rec) ->
    get_meta(?will_message, Rec).

-spec set_will_message(emqx_maybe:t(emqx_types:message()), t()) -> t().
set_will_message(Val, Rec) ->
    set_meta(?will_message, Val, Rec).

-spec clear_will_message_now(emqx_persistent_session_ds:id()) -> ok.
clear_will_message_now(SessionId) when is_binary(SessionId) ->
    do_clear_will_message_now(SessionId, _AttemptsRemaining = 5).

do_clear_will_message_now(_SessionId, _AttemptsRemaining) ->
    ok.
%% case session_restore(SessionId) of
%%     #{?metadata_domain := [#{val := #{?metadata := Metadata0} = OldVal}]} ->
%%         ExtK = IntK = ?metadata_domain_bin,
%%         PreviousPayload = val_encode(?metadata_domain, ExtK, OldVal),
%%         Metadata = Metadata0#{?will_message => undefined},
%%         NewVal = OldVal#{?metadata := Metadata},
%%         MetadataMsg = to_domain_msg(?metadata_domain, SessionId, IntK, ExtK, NewVal),
%%         Preconditions = [
%%             {if_exists, exact_matcher(SessionId, ?metadata_domain, IntK, PreviousPayload)}
%%         ],
%%         case store_batch([MetadataMsg], Preconditions) of
%%             ok ->
%%                 ok;
%%             {error, recoverable, _Reason} when AttemptsRemaining > 0 ->
%%                 %% Todo: smaller timeout?
%%                 timer:sleep(?SESSION_REOPEN_RETRY_TIMEOUT),
%%                 do_clear_will_message_now(SessionId, AttemptsRemaining - 1);
%%             {error, unrecoverable, {precondition_failed, not_found}} ->
%%                 %% Impossible?  Session metadata disappeared while this function,
%%                 %% called by the Session GC worker, was running.  Nothing to clear
%%                 %% anymore...
%%                 ok;
%%             {error, unrecoverable, {precondition_failed, _Message}} when
%%                 AttemptsRemaining > 0
%%             ->
%%                 %% Metadata updated, which means the session has come back alive and
%%                 %% taken over ownership of the session state..  Since this function is
%%                 %% called from the Session GC Worker, we should give up trying to
%%                 %% clear the will message.
%%                 ok;
%%             {error, _Class, _Reason} = Error ->
%%                 Error
%%         end
%% end.

-spec clear_will_message(t()) -> t().
clear_will_message(Rec) ->
    set_will_message(undefined, Rec).

-spec set_offline_info(_Info :: map(), t()) -> t().
set_offline_info(Info, #{?id := SessionId} = Rec) ->
    emqx_persistent_session_ds_state_v2:set_offline_info(generation(), SessionId, Info),
    Rec.

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
cold_get_subscription(_SessionId, _Topic) ->
    [].
%% AllData = read_iterate(
%%     SessionId,
%%     [?subscription_domain_bin, '+']
%% ),
%% Data = [D || #{ext_key := ExtK} = D <- AllData, ExtK =:= Topic],
%% lists:map(fun(#{val := V}) -> V end, Data).

-spec fold_subscriptions(fun(), Acc, t()) -> Acc.
fold_subscriptions(Fun, Acc, Rec) ->
    gen_fold(?subscriptions, Fun, Acc, Rec).

-spec n_subscriptions(t()) -> non_neg_integer().
n_subscriptions(Rec) ->
    gen_size(?subscriptions, Rec).

-spec total_subscription_count() -> non_neg_integer().
total_subscription_count() ->
    %% Fun = fun(Data, Acc) -> length(Data) + Acc end,
    %% read_fold(Fun, 0, '+', [?subscription_domain_bin, '+']).
    0.

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
cold_get_subscription_state(_SessionId, _SStateId) ->
    %% AllData = read_iterate(
    %%     SessionId,
    %%     [?subscription_state_domain_bin, '+']
    %% ),
    %% Data = [D || #{ext_key := ExtK} = D <- AllData, ExtK =:= SStateId],
    %% lists:map(fun(#{val := V}) -> V end, Data).
    [].

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

-spec get_stream(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) ->
    emqx_persistent_session_ds:stream_state() | undefined.
get_stream(Key, Rec) ->
    gen_get(?streams, Key, Rec).

-spec put_stream(
    emqx_persistent_session_ds_stream_scheduler:stream_key(),
    emqx_persistent_session_ds:stream_state(),
    t()
) -> t().
put_stream(Key, Val, Rec) ->
    gen_put(?streams, Key, Val, Rec).

-spec del_stream(emqx_persistent_session_ds_stream_scheduler:stream_key(), t()) -> t().
del_stream(Key, Rec) ->
    gen_del(?streams, Key, Rec).

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
    gen_fold(?streams, Fun, Acc, Rec).

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
    gen_fold(
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
    gen_size(?streams, Rec).

%%

-spec get_seqno(seqno_type(), t()) -> emqx_persistent_session_ds:seqno() | undefined.
get_seqno(Key, Rec) ->
    gen_get(?seqnos, Key, Rec).

-spec put_seqno(seqno_type(), emqx_persistent_session_ds:seqno(), t()) -> t().
put_seqno(Key, Val, Rec) ->
    gen_put(?seqnos, Key, Val, Rec).

%%

-type rank_key() :: {emqx_persistent_session_ds:subscription_id(), emqx_ds:shard()}.

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

%% @doc Fold ranks for a specific subscription ID
-spec fold_ranks(
    emqx_persistent_session_ds:subscription_id(),
    fun((emqx_ds:shard(), emqx_ds:generation(), Acc) -> Acc),
    Acc,
    t()
) -> Acc.
fold_ranks(SubId, Fun, Acc0, Rec) ->
    %% TODO: find some way to avoid full scan.
    gen_fold(
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

%% -spec make_session_iterator() -> session_iterator().
%% make_session_iterator() ->
%%     emqx_persistent_session_ds_state_v2:session_iterator().

%% make_subscription_iterator() ->
%%     emqx_persistent_session_ds_state_v2:subscription_iterator().

%% -spec session_iterator_next(session_iterator(), pos_integer()) ->
%%     {[{emqx_persistent_session_ds:id(), metadata()}], session_iterator() | '$end_of_table'}.
%% session_iterator_next(It0, N) ->
%%     case emqx_utils_stream:consume(N, It0) of
%%         {Data, It} ->
%%             {Data, It};
%%         Data when is_list(Data) ->
%%             {Data, '$end_of_table'}
%%     end.

%% subscription_iterator_next(It0, N) ->
%%     case emqx_utils_stream:consume(N, It0) of
%%         {Data, It} ->
%%             {Data, It};
%%         Data when is_list(Data) ->
%%             {Data, '$end_of_table'}
%%     end.

%%================================================================================
%% Internal functions
%%================================================================================

%%

get_meta(Key, Rec) ->
    gen_get(?metadata, Key, Rec).

set_meta(Key, Val, Rec) ->
    gen_put(?metadata, Key, Val, Rec).

%%

gen_get(Field, Key, Rec) ->
    check_sequence(Rec),
    pmap_get(Key, maps:get(Field, Rec)).

gen_fold(Field, Fun, Acc, Rec) ->
    check_sequence(Rec),
    pmap_fold(Fun, Acc, maps:get(Field, Rec)).

gen_put(Field, Key, Val, Rec) ->
    check_sequence(Rec),
    #{Field := Pmap} = Rec,
    Rec#{
        Field := pmap_put(Key, Val, Pmap),
        ?set_dirty
    }.

gen_del(Field, Key, Rec) ->
    check_sequence(Rec),
    #{?id := SessionId, Field := PMap0} = Rec,
    PMap = pmap_del(SessionId, Key, PMap0),
    Rec#{Field := PMap}.

gen_size(Field, Rec) ->
    check_sequence(Rec),
    pmap_size(maps:get(Field, Rec)).

%% PMaps

-spec pmap_get(K, pmap(K, V)) -> V | undefined.
pmap_get(Key, #pmap{cache = Cache}) ->
    maps:get(Key, Cache, undefined).

pmap_put(
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

-spec pmap_del(emqx_persistent_session_ds:id(), K, pmap(K, V)) -> pmap(K, V).
pmap_del(
    SessionId,
    Key,
    Pmap = #pmap{
        name = Name,
        dirty = Dirty,
        cache = Cache0
    }
) ->
    case maps:take(Key, Cache0) of
        {Val, Cache} ->
            Topic = emqx_persistent_session_ds_state_v2:pmap_topic(Name, SessionId, Key, Val),
            Pmap#pmap{
                cache = Cache,
                dirty = Dirty#{Key => {del, Topic}}
            };
        error ->
            Pmap
    end.

-spec pmap_fold(fun((K, V, A) -> A), A, pmap(K, V)) -> A.
pmap_fold(Fun, Acc, #pmap{cache = Cache}) ->
    maps:fold(Fun, Acc, Cache).

-spec pmap_size(pmap(_K, _V)) -> non_neg_integer().
pmap_size(#pmap{cache = Cache}) ->
    maps:size(Cache).

%%

generation() ->
    1.

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
