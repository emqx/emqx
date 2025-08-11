%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements second version of durable encoding of
%% the session state.
%%
%% Data layout in DS DB. Note: /foo denotes individual stream, |bar
%% means wildcard stream:
%%
%% g|<sessid>                       -- Session guard
%%
%% d|<sessid>/meta|createdat        -- PMaps
%%                |last_alive_at
%%                |...
%%           /subscriptions|1
%%                         |12
%%                         |...
%%           /...|...
%%
%% o|<sessid>                       -- Offline data, aka misc. stuff
%%                                     for the dashboard that we don't
%%                                     need in the state
%%
-module(emqx_persistent_session_ds_state_v2).

-feature(maybe_expr, enable).

-behaviour(emqx_ds_pmap).

%% API:
-export([
    open/2,
    delete/2,
    commit/3,

    list_sessions/1,

    set_offline_info/3,
    get_offline_info/2,

    lts_threshold_cb/2,

    new_pmap/1,
    pmap_dirty_read/3,

    total_subscription_count/1,
    make_session_iterator/1,
    session_iterator_next/3,
    make_subscription_iterator/1,
    subscription_iterator_next/3
]).

%% Behavior callbacks:
-export([pmap_encode_key/3, pmap_decode_key/2, pmap_encode_val/3, pmap_decode_val/3]).

-export_type([session_iterator/0, subscription_iterator/0]).

%% FIXME: rebar idiocy
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("stdlib/include/assert.hrl").
-include("../src/emqx_tracepoints.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("pmap.hrl").
-include("session_internals.hrl").
-include("../../gen_src/DurableSession.hrl").

%% TODO: https://github.com/erlang/otp/issues/9841
-dialyzer(
    {nowarn_function, [
        pmap_encode_key/3,
        pmap_decode_key/2,
        ser_stream_key/1,
        deser_stream_key/1,
        ser_srs/1,
        deser_srs/1,
        ser_sub/1,
        deser_sub/1,
        ser_sub_state/1,
        deser_sub_state/1
    ]}
).

%%================================================================================
%% Type declarations
%%================================================================================

-define(DB, ?DURABLE_SESSION_STATE).

%% Topic roots:
-define(top_offline_info, <<"o">>).
%% Pmap topics:
-define(top_metadata, <<"mta">>).
-define(top_subscriptions, <<"sub">>).
-define(top_subscription_states, <<"sst">>).
-define(top_seqnos, <<"sn">>).
-define(top_streams, <<"st">>).
-define(top_ranks, <<"rnk">>).
-define(top_awaiting_rel, <<"arl">>).

-type session_iterator() :: emqx_ds:multi_iterator().
-type subscription_iterator() :: emqx_ds:multi_iterator().

%%================================================================================
%% API functions
%%================================================================================

%% @doc Open a session
%% TODO: combine opening with updating metadata to make it a one transaction
-spec open(emqx_ds:generation(), emqx_types:clientid()) ->
    {ok, emqx_persistent_session_ds_state:t()} | undefined.
open(Generation, ClientId) ->
    Opts = #{
        db => ?DB,
        generation => Generation,
        shard => {auto, ClientId},
        timeout => trans_timeout(),
        retries => 10,
        retry_interval => 100
    },
    Ret = emqx_ds:trans(
        Opts,
        fun() -> open_tx(ClientId) end
    ),
    case Ret of
        {atomic, _TXSerial, Result} ->
            Result;
        {nop, Result} ->
            Result
    end.

-spec commit(
    emqx_ds:generation(),
    emqx_persistent_session_ds_state:t(),
    emqx_persistent_session_ds_state:commit_opts()
) ->
    emqx_persistent_session_ds_state:t().
commit(Generation, Rec0 = #{?id := ClientId, ?checkpoint_ref := Ref}, Opts) when
    is_reference(Ref)
->
    %% Attempt to commit while previous async checkpoint is still in
    %% progress.
    case Opts of
        #{sync := false} ->
            %% This is another async checkpoint. Just ignore it.
            Rec0;
        #{sync := true} ->
            %% Wait for the checkpoint to conclude, then commit again:
            receive
                ?ds_tx_commit_reply(Ref, Reply) ->
                    Rec =
                        case emqx_ds:tx_commit_outcome(?DB, Ref, Reply) of
                            {ok, _} ->
                                Rec0;
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
                                Rec0#{?set_dirty}
                        end,
                    commit(Generation, Rec#{?checkpoint_ref := undefined}, Opts)
            end
    end;
commit(_Generation, Rec = #{?collection_dirty := false}, #{lifetime := L}) when
    L =/= takeover, L =/= new
->
    %% There's nothing to checkpoint.
    Rec;
commit(
    Generation,
    Rec0 = #{
        ?id := ClientId,
        ?collection_guard := Guard0,
        ?metadata := Metadata,
        ?subscriptions := Subs,
        ?subscription_states := SubStates,
        ?streams := Streams,
        ?seqnos := SeqNos,
        ?ranks := Ranks,
        ?awaiting_rel := AwaitingRels
    },
    #{lifetime := Lifetime, sync := Sync}
) ->
    NewGuard = Lifetime =:= takeover orelse Lifetime =:= new,
    Opts = #{
        db => ?DB,
        shard => {auto, ClientId},
        generation => Generation,
        sync => Sync,
        timeout => trans_timeout(),
        retries => 5,
        retry_interval => 1000
    },
    Result =
        emqx_ds:trans(
            Opts,
            fun() ->
                %% Generate a new guard if needed:
                NewGuard andalso
                    emqx_ds_pmap:tx_write_guard(ClientId, ?ds_tx_serial),
                case Lifetime of
                    new ->
                        %% Drop the old session state:
                        tx_del_session_data(ClientId);
                    _ ->
                        %% Ensure continuity of the session:
                        emqx_ds_pmap:tx_assert_guard(ClientId, Guard0)
                end,
                Rec0#{
                    ?metadata := emqx_ds_pmap:tx_commit(ClientId, Metadata),
                    ?subscriptions := emqx_ds_pmap:tx_commit(ClientId, Subs),
                    ?subscription_states := emqx_ds_pmap:tx_commit(ClientId, SubStates),
                    ?streams := emqx_ds_pmap:tx_commit(ClientId, Streams),
                    ?seqnos := emqx_ds_pmap:tx_commit(ClientId, SeqNos),
                    ?ranks := emqx_ds_pmap:tx_commit(ClientId, Ranks),
                    ?awaiting_rel := emqx_ds_pmap:tx_commit(ClientId, AwaitingRels),
                    ?unset_dirty
                }
            end
        ),
    case Result of
        {atomic, TXSerial, Rec} when NewGuard ->
            %% This is a new incarnation of the client. Update the
            %% guard:
            Rec#{?collection_guard := TXSerial};
        {atomic, _TXSerial, Rec} ->
            Rec;
        {nop, Rec} ->
            Rec;
        {async, Ref, Rec} ->
            Rec#{?checkpoint_ref := Ref};
        {error, unrecoverable, {precondition_failed, Conflict}} when
            Lifetime =:= terminate
        ->
            %% Don't interrupt graceful channel shut down even when
            %% the guard is invalidated:
            ?tp(warning, ?sessds_takeover_conflict, #{
                id => ClientId, conflict => Conflict, channel => self()
            }),
            Rec0;
        {error, Class, Reason} ->
            error(
                {failed_to_commit_session, #{
                    id => ClientId,
                    lifetime => Lifetime,
                    Class => Reason
                }}
            )
    end.

-spec pmap_dirty_read(emqx_ds:generation(), atom(), emqx_persistent_session_ds:id()) -> map().
pmap_dirty_read(Generation, Name, ClientId) ->
    Opts = #{db => ?DB, generation => Generation, shard => emqx_ds:shard_of(?DB, ClientId)},
    emqx_ds_pmap:dirty_read(?MODULE, pmap_name(Name), ClientId, Opts).

-spec list_sessions(emqx_ds:generation()) -> [emqx_persistent_session_ds:id()].
list_sessions(Gen) ->
    {L, _Errors} = emqx_ds:fold_topic(
        fun(_Slab, _Stream, {Topic, _TS, _Guard}, Acc) ->
            ?guard_topic(Id) = Topic,
            [Id | Acc]
        end,
        [],
        ?guard_topic('+'),
        #{db => ?DB, generation => Gen, errors => report}
    ),
    L.

-spec set_offline_info(
    emqx_ds:generation(),
    emqx_persistent_session_ds:id(),
    term()
) ->
    ok.
set_offline_info(Generation, ClientId, Data) ->
    Opts = #{
        db => ?DB, shard => {auto, ClientId}, generation => Generation, timeout => trans_timeout()
    },
    _ = emqx_ds:trans(
        Opts,
        fun() ->
            emqx_ds:tx_write({
                [?top_offline_info, ClientId],
                0,
                emqx_persistent_session_ds_channel_info:encode(Data)
            })
        end
    ),
    ok.

get_offline_info(_Generation, undefined) ->
    [];
get_offline_info(Generation, S = #{?id := ClientId}) ->
    Opts = #{
        db => ?DB,
        shard => emqx_ds:shard_of(?DB, ClientId),
        generation => Generation,
        errors => ignore
    },
    Result = emqx_ds:dirty_read(
        Opts,
        [?top_offline_info, ClientId]
    ),
    case Result of
        [{_, _, Bin}] ->
            [emqx_persistent_session_ds_channel_info:enrich(Bin, S)];
        [] ->
            []
    end.

-spec delete(emqx_ds:generation(), emqx_persistent_session_ds_state:t()) -> ok.
delete(
    Generation,
    #{?collection_guard := Guard, ?id := ClientId}
) ->
    Opts = #{
        db => ?DB, shard => {auto, ClientId}, generation => Generation, timeout => trans_timeout()
    },
    {atomic, _, _} =
        emqx_ds:trans(
            Opts,
            fun() ->
                emqx_ds_pmap:tx_assert_guard(ClientId, Guard),
                tx_del_session_data(ClientId),
                emqx_ds_pmap:tx_delete_guard(ClientId)
            end
        ),
    ok.

tx_del_session_data(ClientId) ->
    emqx_ds_pmap:tx_destroy(ClientId, ?top_metadata),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_subscriptions),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_subscription_states),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_seqnos),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_streams),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_ranks),
    emqx_ds_pmap:tx_destroy(ClientId, ?top_awaiting_rel),
    emqx_ds:tx_del_topic([?top_offline_info, ClientId]).

%% @doc LTS trie wildcard threshold function
lts_threshold_cb(_N, ?top_offline_info) ->
    %% Create unified stream for all offline infos:
    0;
lts_threshold_cb(Level, Parent) ->
    emqx_ds_pmap:lts_threshold_cb(Level, Parent).

-spec make_session_iterator(emqx_ds:generation()) -> session_iterator() | '$end_of_table'.
make_session_iterator(Generation) ->
    emqx_ds:make_multi_iterator(#{db => ?DB, generation => Generation}, ?guard_topic('+')).

-spec session_iterator_next(emqx_ds:generation(), session_iterator(), pos_integer()) ->
    {[emqx_persistent_session_ds:id()], session_iterator() | '$end_of_table'}.
session_iterator_next(Generation, It0, N) ->
    {Batch, It} = emqx_ds:multi_iterator_next(
        #{db => ?DB, generation => Generation}, ?guard_topic('+'), It0, N
    ),
    Results = lists:map(
        fun({?guard_topic(SessId), _, _Guard}) ->
            SessId
        end,
        Batch
    ),
    {Results, It}.

-spec make_subscription_iterator(emqx_ds:generation()) ->
    subscription_iterator() | '$end_of_table'.
make_subscription_iterator(Generation) ->
    emqx_ds:make_multi_iterator(
        #{db => ?DB, generation => Generation},
        emqx_ds_pmap:pmap_topic(?top_subscriptions, '+', '+')
    ).

-spec subscription_iterator_next(emqx_ds:generation(), subscription_iterator(), pos_integer()) ->
    {
        [{emqx_persistent_session_ds:id(), emqx_persistent_session_ds:topic_filter()}],
        subscription_iterator() | '$end_of_table'
    }.
subscription_iterator_next(Generation, It0, N) ->
    {Batch, It} = emqx_ds:multi_iterator_next(
        #{db => ?DB, generation => Generation},
        emqx_ds_pmap:pmap_topic(?top_subscriptions, '+', '+'),
        It0,
        N
    ),
    Results = lists:map(
        fun({?pmap_topic(_, ClientId, Sub), _, _SubInfo}) ->
            {ClientId, pmap_decode_key(?top_subscriptions, Sub)}
        end,
        Batch
    ),
    {Results, It}.

total_subscription_count(Generation) ->
    emqx_ds:wait_db(?DB, all, infinity),
    emqx_ds:fold_topic(
        fun(_, _, _, Acc) -> Acc + 1 end,
        0,
        emqx_ds_pmap:pmap_topic(?top_subscriptions, '+', '+'),
        #{db => ?DB, generation => Generation, errors => ignore}
    ).

%%================================================================================
%% Internal functions
%%================================================================================

open_tx(ClientId) ->
    case emqx_ds_pmap:tx_guard(ClientId) of
        undefined ->
            undefined;
        Guard ->
            Ret = #{
                ?id => ClientId,
                ?collection_guard => Guard,
                ?collection_dirty => false,
                ?checkpoint_ref => undefined,
                ?metadata => emqx_ds_pmap:tx_restore(?MODULE, ?top_metadata, ClientId),
                ?subscriptions => emqx_ds_pmap:tx_restore(?MODULE, ?top_subscriptions, ClientId),
                ?subscription_states => emqx_ds_pmap:tx_restore(
                    ?MODULE, ?top_subscription_states, ClientId
                ),
                ?seqnos => emqx_ds_pmap:tx_restore(?MODULE, ?top_seqnos, ClientId),
                ?streams => emqx_ds_pmap:tx_restore(?MODULE, ?top_streams, ClientId),
                ?ranks => emqx_ds_pmap:tx_restore(?MODULE, ?top_ranks, ClientId),
                ?awaiting_rel => emqx_ds_pmap:tx_restore(?MODULE, ?top_awaiting_rel, ClientId)
            },
            {ok, Ret}
    end.

-spec new_pmap(atom()) -> emqx_ds_pmap:pmap(_, _).
new_pmap(Pmap) ->
    emqx_ds_pmap:new_pmap(?MODULE, pmap_name(Pmap)).

pmap_encode_key(?top_metadata, MetaKey, _Val) ->
    atom_to_binary(MetaKey, latin1);
pmap_encode_key(?top_streams, Key, _Val) ->
    ser_stream_key(Key);
pmap_encode_key(?top_subscriptions, Topic, _Val) ->
    'DurableSession':encode('TopicFilter', wrap_topic(Topic));
pmap_encode_key(?top_seqnos, Track, _Val) ->
    ?assert(Track < 255),
    <<Track:8>>;
pmap_encode_key(_, Key, _Val) ->
    term_to_binary(Key).

pmap_decode_key(?top_metadata, Key) ->
    binary_to_atom(Key, latin1);
pmap_decode_key(?top_streams, Bin) ->
    deser_stream_key(Bin);
pmap_decode_key(?top_subscriptions, Key) ->
    unwrap_topic('DurableSession':decode('TopicFilter', Key));
pmap_decode_key(?top_seqnos, Bin) ->
    <<Track:8>> = Bin,
    Track;
pmap_decode_key(_, Key) ->
    binary_to_term(Key).

%% Payload (de)serialization:

pmap_encode_val(?top_streams, _Key, SRS) ->
    ser_srs(SRS);
pmap_encode_val(?top_subscriptions, _Key, Sub) ->
    ser_sub(Sub);
pmap_encode_val(?top_subscription_states, _Key, SState) ->
    ser_sub_state(SState);
pmap_encode_val(_Name, _Key, Val) ->
    term_to_binary(Val).

pmap_decode_val(?top_streams, _Key, Bin) ->
    deser_srs(Bin);
pmap_decode_val(?top_subscriptions, _Key, Bin) ->
    deser_sub(Bin);
pmap_decode_val(?top_subscription_states, _Key, Bin) ->
    deser_sub_state(Bin);
pmap_decode_val(_, _, Bin) ->
    binary_to_term(Bin).

%% Topic

wrap_topic(T) ->
    case T of
        #share{group = Group, topic = Topic} ->
            ok;
        Topic when is_binary(Topic) ->
            Group = asn1_NOVALUE
    end,
    #'TopicFilter'{
        topic = Topic, group = Group
    }.

unwrap_topic(Rec) ->
    #'TopicFilter'{topic = Topic, group = Group} = Rec,
    case Group of
        asn1_NOVALUE ->
            Topic;
        _ ->
            #share{topic = Topic, group = Group}
    end.

%% Stream

ser_stream_key({SubId, Stream}) ->
    {ok, StreamBin} = emqx_ds:stream_to_binary(?PERSISTENT_MESSAGE_DB, Stream),
    Rec = #'StreamKey'{subId = SubId, stream = StreamBin},
    'DurableSession':encode('StreamKey', Rec).

deser_stream_key(Bin) ->
    #'StreamKey'{subId = SubId, stream = StreamBin} =
        'DurableSession':decode('StreamKey', Bin),
    {ok, Stream} = emqx_ds:binary_to_stream(?PERSISTENT_MESSAGE_DB, StreamBin),
    {SubId, Stream}.

ser_srs(#srs{
    rank_x = Shard,
    rank_y = Generation,
    it_begin = ItBegin,
    it_end = ItEnd,
    batch_size = BS,
    first_seqno_qos1 = FSN1,
    first_seqno_qos2 = FSN2,
    last_seqno_qos1 = LSN1,
    last_seqno_qos2 = LSN2,
    unsubscribed = Unsub,
    sub_state_id = SSid
}) ->
    {ok, ItBeginB} = emqx_ds:iterator_to_binary(?PERSISTENT_MESSAGE_DB, ItBegin),
    {ok, ItEndB} = emqx_ds:iterator_to_binary(?PERSISTENT_MESSAGE_DB, ItEnd),
    Rec = #'SRS'{
        shard = Shard,
        generation = Generation,
        itBegin = ItBeginB,
        itEnd = ItEndB,
        batchSize = BS,
        firstSeqNoQoS1 = FSN1,
        firstSeqNoQoS2 = FSN2,
        lastSeqNoQoS1 = LSN1,
        lastSeqNoQoS2 = LSN2,
        unsubscribed = Unsub,
        subscriptionState = SSid
    },
    'DurableSession':encode('SRS', Rec).

deser_srs(Bin) ->
    #'SRS'{
        shard = Shard,
        generation = Generation,
        itBegin = ItBeginB,
        itEnd = ItEndB,
        batchSize = BS,
        firstSeqNoQoS1 = FSN1,
        firstSeqNoQoS2 = FSN2,
        lastSeqNoQoS1 = LSN1,
        lastSeqNoQoS2 = LSN2,
        unsubscribed = Unsub,
        subscriptionState = SSid
    } = 'DurableSession':decode('SRS', Bin),
    {ok, ItBegin} = emqx_ds:binary_to_iterator(?PERSISTENT_MESSAGE_DB, ItBeginB),
    case ItEndB of
        asn1_NOVALUE ->
            ItEnd = undefined;
        _ ->
            {ok, ItEnd} = emqx_ds:binary_to_iterator(?PERSISTENT_MESSAGE_DB, ItEndB)
    end,
    #srs{
        rank_x = Shard,
        rank_y = Generation,
        it_begin = ItBegin,
        it_end = ItEnd,
        batch_size = BS,
        first_seqno_qos1 = FSN1,
        first_seqno_qos2 = FSN2,
        last_seqno_qos1 = LSN1,
        last_seqno_qos2 = LSN2,
        unsubscribed = Unsub,
        sub_state_id = SSid
    }.

%% Subscription

ser_sub(#{id := Id, current_state := CS, start_time := T}) ->
    Rec = #'Subscription'{id = Id, currentState = CS, startTime = T},
    'DurableSession':encode('Subscription', Rec).

deser_sub(Bin) ->
    #'Subscription'{id = Id, currentState = CS, startTime = T} =
        'DurableSession':decode('Subscription', Bin),
    #{id => Id, current_state => CS, start_time => T}.

%% Subscription state

ser_sub_state(
    #{parent_subscription := PSub, upgrade_qos := UQ, subopts := SubOpts, mode := Mode} = SState
) ->
    Misc = wrap_subopts(SubOpts),
    Share =
        case SState of
            #{share_topic_filter := T} ->
                wrap_topic(T);
            _ ->
                asn1_NOVALUE
        end,
    Rec = #'SubState'{
        parentSub = PSub,
        upgradeQos = UQ,
        supersededBy = maps:get(superseded_by, SState, asn1_NOVALUE),
        shareTopicFilter = Share,
        miscSubopts = Misc,
        durable = Mode =:= durable
    },
    'DurableSession':encode('SubState', Rec).

deser_sub_state(Bin) ->
    #'SubState'{
        parentSub = PSub,
        upgradeQos = UQ,
        supersededBy = SupBy,
        shareTopicFilter = Share,
        miscSubopts = Misc,
        durable = Durable
    } = 'DurableSession':decode('SubState', Bin),
    SubOpts = unwrap_subopts(Misc),
    M1 = #{
        parent_subscription => PSub,
        upgrade_qos => UQ,
        subopts => SubOpts,
        mode =>
            case Durable of
                true -> durable;
                false -> direct
            end
    },
    M2 =
        case SupBy of
            asn1_NOVALUE ->
                M1;
            _ ->
                M1#{superseded_by => SupBy}
        end,
    case Share of
        asn1_NOVALUE ->
            M2;
        _ ->
            M2#{share_topic_filter => unwrap_topic(Share)}
    end.

wrap_subopts(SubOpts) ->
    maps:fold(
        fun(K, V, Acc) ->
            [#'Misc'{key = atom_to_binary(K), val = term_to_binary(V)} | Acc]
        end,
        [],
        SubOpts
    ).

unwrap_subopts(Misc) ->
    maps:from_list(
        lists:map(
            fun(#'Misc'{key = K, val = V}) ->
                {binary_to_atom(K), binary_to_term(V)}
            end,
            Misc
        )
    ).

trans_timeout() ->
    emqx_config:get([durable_sessions, commit_timeout]).

pmap_name(Pmap) ->
    case Pmap of
        ?metadata -> ?top_metadata;
        ?subscriptions -> ?top_subscriptions;
        ?subscription_states -> ?top_subscription_states;
        ?seqnos -> ?top_seqnos;
        ?streams -> ?top_streams;
        ?ranks -> ?top_ranks;
        ?awaiting_rel -> ?top_awaiting_rel
    end.
