%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_ds).

-behaviour(emqx_session).

-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_persistent_session_ds.hrl").

%% Session API
-export([
    create/3,
    open/3,
    destroy/1
]).

-export([
    info/2,
    stats/1
]).

-export([
    subscribe/3,
    unsubscribe/2,
    get_subscription/2
]).

-export([
    publish/3,
    puback/3,
    pubrec/2,
    pubrel/2,
    pubcomp/3
]).

-export([
    deliver/3,
    replay/3,
    handle_timeout/3,
    disconnect/2,
    terminate/2
]).

%% session table operations
-export([create_tables/0]).

%% internal export used by session GC process
-export([destroy_session/1]).

%% Remove me later (satisfy checks for an unused BPAPI)
-export([
    do_open_iterator/3,
    do_ensure_iterator_closed/1,
    do_ensure_all_iterators_closed/1
]).

-export([print_session/1]).

-ifdef(TEST).
-export([
    session_open/2,
    list_all_sessions/0,
    list_all_subscriptions/0,
    list_all_streams/0,
    list_all_pubranges/0
]).
-endif.

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type topic_filter() :: emqx_types:topic().
-type topic_filter_words() :: emqx_ds:topic_filter().
-type subscription_id() :: {id(), topic_filter()}.
-type subscription() :: #{
    start_time := emqx_ds:time(),
    props := map(),
    extra := map()
}.

-define(TIMER_PULL, timer_pull).
-define(TIMER_GET_STREAMS, timer_get_streams).
-define(TIMER_BUMP_LAST_ALIVE_AT, timer_bump_last_alive_at).
-type timer() :: ?TIMER_PULL | ?TIMER_GET_STREAMS | ?TIMER_BUMP_LAST_ALIVE_AT.

-type subscriptions() :: emqx_topic_gbt:t(nil(), subscription()).

-type session() :: #{
    %% Client ID
    id := id(),
    %% When the session was created
    created_at := timestamp(),
    %% When the client was last considered alive
    last_alive_at := timestamp(),
    %% Client’s Subscriptions.
    subscriptions := subscriptions(),
    %% Inflight messages
    inflight := emqx_persistent_message_ds_replayer:inflight(),
    %% Receive maximum
    receive_maximum := pos_integer(),
    %% Connection Info
    conninfo := emqx_types:conninfo(),
    %% Timers
    timer() => reference(),
    %%
    props := map()
}.

-type timestamp() :: emqx_utils_calendar:epoch_millisecond().
-type millisecond() :: non_neg_integer().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().

-define(STATS_KEYS, [
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max,
    next_pkt_id
]).

-define(IS_EXPIRED(NOW_MS, LAST_ALIVE_AT, EI),
    (is_number(LAST_ALIVE_AT) andalso
        is_number(EI) andalso
        (NOW_MS >= LAST_ALIVE_AT + EI))
).

-export_type([id/0]).

%%

-spec create(clientinfo(), conninfo(), emqx_session:conf()) ->
    session().
create(#{clientid := ClientID}, ConnInfo, Conf) ->
    Session = session_ensure_new(ClientID, ConnInfo),
    apply_conf(ConnInfo, Conf, ensure_timers(Session)).

-spec open(clientinfo(), conninfo(), emqx_session:conf()) ->
    {_IsPresent :: true, session(), []} | false.
open(#{clientid := ClientID} = _ClientInfo, ConnInfo, Conf) ->
    %% NOTE
    %% The fact that we need to concern about discarding all live channels here
    %% is essentially a consequence of the in-memory session design, where we
    %% have disconnected channels holding onto session state. Ideally, we should
    %% somehow isolate those idling not-yet-expired sessions into a separate process
    %% space, and move this call back into `emqx_cm` where it belongs.
    ok = emqx_cm:discard_session(ClientID),
    case session_open(ClientID, ConnInfo) of
        Session0 = #{} ->
            Session = apply_conf(ConnInfo, Conf, Session0),
            {true, ensure_timers(Session), []};
        false ->
            false
    end.

apply_conf(ConnInfo, Conf, Session) ->
    Session#{
        receive_maximum => receive_maximum(ConnInfo),
        props => Conf
    }.

-spec destroy(session() | clientinfo()) -> ok.
destroy(#{id := ClientID}) ->
    destroy_session(ClientID);
destroy(#{clientid := ClientID}) ->
    destroy_session(ClientID).

destroy_session(ClientID) ->
    session_drop(ClientID).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, #{id := ClientID}) ->
    ClientID;
info(clientid, #{id := ClientID}) ->
    ClientID;
info(created_at, #{created_at := CreatedAt}) ->
    CreatedAt;
info(is_persistent, #{}) ->
    true;
info(subscriptions, #{subscriptions := Subs}) ->
    subs_to_map(Subs);
info(subscriptions_cnt, #{subscriptions := Subs}) ->
    subs_size(Subs);
info(subscriptions_max, #{props := Conf}) ->
    maps:get(max_subscriptions, Conf);
info(upgrade_qos, #{props := Conf}) ->
    maps:get(upgrade_qos, Conf);
% info(inflight, #sessmem{inflight = Inflight}) ->
%     Inflight;
info(inflight_cnt, #{inflight := Inflight}) ->
    emqx_persistent_message_ds_replayer:n_inflight(Inflight);
info(inflight_max, #{receive_maximum := ReceiveMaximum}) ->
    ReceiveMaximum;
info(retry_interval, #{props := Conf}) ->
    maps:get(retry_interval, Conf);
% info(mqueue, #sessmem{mqueue = MQueue}) ->
%     MQueue;
% info(mqueue_len, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:len(MQueue);
% info(mqueue_max, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:max_len(MQueue);
% info(mqueue_dropped, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:dropped(MQueue);
info(next_pkt_id, #{inflight := Inflight}) ->
    {PacketId, _} = emqx_persistent_message_ds_replayer:next_packet_id(Inflight),
    PacketId;
% info(awaiting_rel, #sessmem{awaiting_rel = AwaitingRel}) ->
%     AwaitingRel;
% info(awaiting_rel_cnt, #sessmem{awaiting_rel = AwaitingRel}) ->
%     maps:size(AwaitingRel);
info(awaiting_rel_max, #{props := Conf}) ->
    maps:get(max_awaiting_rel, Conf);
info(await_rel_timeout, #{props := Conf}) ->
    maps:get(await_rel_timeout, Conf).

-spec stats(session()) -> emqx_types:stats().
stats(Session) ->
    info(?STATS_KEYS, Session).

%% Debug/troubleshooting
-spec print_session(emqx_types:client_id()) -> map() | undefined.
print_session(ClientId) ->
    catch ro_transaction(
        fun() ->
            case mnesia:read(?SESSION_TAB, ClientId) of
                [Session] ->
                    #{
                        session => Session,
                        streams => mnesia:read(?SESSION_STREAM_TAB, ClientId),
                        pubranges => session_read_pubranges(ClientId),
                        offsets => session_read_offsets(ClientId),
                        subscriptions => session_read_subscriptions(ClientId)
                    };
                [] ->
                    undefined
            end
        end
    ).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE / UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec subscribe(topic_filter(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, subscriptions := Subs}
) ->
    case subs_lookup(TopicFilter, Subs) of
        Subscription = #{} ->
            NSubscription = update_subscription(TopicFilter, Subscription, SubOpts, ID),
            NSubs = subs_insert(TopicFilter, NSubscription, Subs),
            {ok, Session#{subscriptions := NSubs}};
        undefined ->
            % TODO: max_subscriptions
            Subscription = add_subscription(TopicFilter, SubOpts, ID),
            NSubs = subs_insert(TopicFilter, Subscription, Subs),
            {ok, Session#{subscriptions := NSubs}}
    end.

-spec unsubscribe(topic_filter(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    TopicFilter,
    Session = #{id := ID, subscriptions := Subs}
) ->
    case subs_lookup(TopicFilter, Subs) of
        _Subscription = #{props := SubOpts} ->
            ok = del_subscription(TopicFilter, ID),
            NSubs = subs_delete(TopicFilter, Subs),
            {ok, Session#{subscriptions := NSubs}, SubOpts};
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED}
    end.

-spec get_subscription(topic_filter(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(TopicFilter, #{subscriptions := Subs}) ->
    case subs_lookup(TopicFilter, Subs) of
        _Subscription = #{props := SubOpts} ->
            SubOpts;
        undefined ->
            undefined
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec publish(emqx_types:packet_id(), emqx_types:message(), session()) ->
    {ok, emqx_types:publish_result(), session()}
    | {error, emqx_types:reason_code()}.
publish(_PacketId, Msg, Session) ->
    %% TODO: QoS2
    Result = emqx_broker:publish(Msg),
    {ok, Result, Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-spec puback(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
puback(_ClientInfo, PacketId, Session = #{id := Id, inflight := Inflight0}) ->
    case emqx_persistent_message_ds_replayer:commit_offset(Id, ack, PacketId, Inflight0) of
        {true, Inflight} ->
            %% TODO: we pass a bogus message into the hook:
            Msg = emqx_message:make(Id, <<>>, <<>>),
            {ok, Msg, [], pull_now(Session#{inflight => Inflight})};
        {false, _} ->
            %% Invalid Packet Id
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec pubrec(emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()}
    | {error, emqx_types:reason_code()}.
pubrec(PacketId, Session = #{id := Id, inflight := Inflight0}) ->
    case emqx_persistent_message_ds_replayer:commit_offset(Id, rec, PacketId, Inflight0) of
        {true, Inflight} ->
            %% TODO: we pass a bogus message into the hook:
            Msg = emqx_message:make(Id, <<>>, <<>>),
            {ok, Msg, pull_now(Session#{inflight => Inflight})};
        {false, _} ->
            %% Invalid Packet Id
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec pubrel(emqx_types:packet_id(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
pubrel(_PacketId, Session = #{}) ->
    % TODO: stub
    {ok, Session}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec pubcomp(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
pubcomp(_ClientInfo, PacketId, Session = #{id := Id, inflight := Inflight0}) ->
    case emqx_persistent_message_ds_replayer:commit_offset(Id, comp, PacketId, Inflight0) of
        {true, Inflight} ->
            %% TODO
            Msg = emqx_message:make(Id, <<>>, <<>>),
            {ok, Msg, [], Session#{inflight => Inflight}};
        {false, _} ->
            %% Invalid Packet Id
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------

-spec deliver(clientinfo(), [emqx_types:deliver()], session()) ->
    {ok, replies(), session()}.
deliver(_ClientInfo, _Delivers, Session) ->
    %% TODO: system messages end up here.
    {ok, [], Session}.

-spec handle_timeout(clientinfo(), _Timeout, session()) ->
    {ok, replies(), session()} | {ok, replies(), timeout(), session()}.
handle_timeout(
    ClientInfo,
    ?TIMER_PULL,
    Session0 = #{
        id := Id,
        inflight := Inflight0,
        subscriptions := Subs,
        props := Conf,
        receive_maximum := ReceiveMaximum
    }
) ->
    MaxBatchSize = emqx_config:get([session_persistence, max_batch_size]),
    BatchSize = min(ReceiveMaximum, MaxBatchSize),
    UpgradeQoS = maps:get(upgrade_qos, Conf),
    PreprocFun = make_preproc_fun(ClientInfo, Subs, UpgradeQoS),
    {Publishes, Inflight} = emqx_persistent_message_ds_replayer:poll(
        PreprocFun,
        Id,
        Inflight0,
        BatchSize
    ),
    IdlePollInterval = emqx_config:get([session_persistence, idle_poll_interval]),
    Timeout =
        case Publishes of
            [] ->
                IdlePollInterval;
            [_ | _] ->
                0
        end,
    Session = emqx_session:ensure_timer(?TIMER_PULL, Timeout, Session0#{inflight := Inflight}),
    {ok, Publishes, Session};
handle_timeout(_ClientInfo, ?TIMER_GET_STREAMS, Session) ->
    renew_streams(Session),
    Interval = emqx_config:get([session_persistence, renew_streams_interval]),
    {ok, [], emqx_session:ensure_timer(?TIMER_GET_STREAMS, Interval, Session)};
handle_timeout(_ClientInfo, ?TIMER_BUMP_LAST_ALIVE_AT, Session0) ->
    %% Note: we take a pessimistic approach here and assume that the client will be alive
    %% until the next bump timeout.  With this, we avoid garbage collecting this session
    %% too early in case the session/connection/node crashes earlier without having time
    %% to commit the time.
    BumpInterval = emqx_config:get([session_persistence, last_alive_update_interval]),
    EstimatedLastAliveAt = now_ms() + BumpInterval,
    Session = session_set_last_alive_at_trans(Session0, EstimatedLastAliveAt),
    {ok, [], emqx_session:ensure_timer(?TIMER_BUMP_LAST_ALIVE_AT, BumpInterval, Session)};
handle_timeout(_ClientInfo, expire_awaiting_rel, Session) ->
    %% TODO: stub
    {ok, [], Session}.

-spec replay(clientinfo(), [], session()) ->
    {ok, replies(), session()}.
replay(
    ClientInfo,
    [],
    Session = #{inflight := Inflight0, subscriptions := Subs, props := Conf}
) ->
    UpgradeQoS = maps:get(upgrade_qos, Conf),
    PreprocFun = make_preproc_fun(ClientInfo, Subs, UpgradeQoS),
    {Replies, Inflight} = emqx_persistent_message_ds_replayer:replay(PreprocFun, Inflight0),
    {ok, Replies, Session#{inflight := Inflight}}.

%%--------------------------------------------------------------------

-spec disconnect(session(), emqx_types:conninfo()) -> {shutdown, session()}.
disconnect(Session0, ConnInfo) ->
    Session = session_set_last_alive_at_trans(Session0, ConnInfo, now_ms()),
    {shutdown, Session}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(_Reason, _Session = #{}) ->
    ok.

%%--------------------------------------------------------------------

make_preproc_fun(ClientInfo, Subs, UpgradeQoS) ->
    fun(Message = #message{topic = Topic}) ->
        emqx_utils:flattermap(
            fun(Match) ->
                #{props := SubOpts} = subs_get_match(Match, Subs),
                emqx_session:enrich_message(ClientInfo, Message, SubOpts, UpgradeQoS)
            end,
            subs_matches(Topic, Subs)
        )
    end.

%%--------------------------------------------------------------------

-spec add_subscription(topic_filter(), emqx_types:subopts(), id()) ->
    subscription().
add_subscription(TopicFilter, SubOpts, DSSessionID) ->
    %% N.B.: we chose to update the router before adding the subscription to the
    %% session/iterator table.  The reasoning for this is as follows:
    %%
    %% Messages matching this topic filter should start to be persisted as soon as
    %% possible to avoid missing messages.  If this is the first such persistent
    %% session subscription, it's important to do so early on.
    %%
    %% This could, in turn, lead to some inconsistency: if such a route gets
    %% created but the session/iterator data fails to be updated accordingly, we
    %% have a dangling route.  To remove such dangling routes, we may have a
    %% periodic GC process that removes routes that do not have a matching
    %% persistent subscription.  Also, route operations use dirty mnesia
    %% operations, which inherently have room for inconsistencies.
    %%
    %% In practice, we use the iterator reference table as a source of truth,
    %% since it is guarded by a transaction context: we consider a subscription
    %% operation to be successful if it ended up changing this table.  Both router
    %% and iterator information can be reconstructed from this table, if needed.
    ok = emqx_persistent_session_ds_router:do_add_route(TopicFilter, DSSessionID),
    {ok, DSSubExt, IsNew} = session_add_subscription(
        DSSessionID, TopicFilter, SubOpts
    ),
    ?tp(persistent_session_ds_subscription_added, #{sub => DSSubExt, is_new => IsNew}),
    %% we'll list streams and open iterators when implementing message replay.
    DSSubExt.

-spec update_subscription(topic_filter(), subscription(), emqx_types:subopts(), id()) ->
    subscription().
update_subscription(TopicFilter, DSSubExt, SubOpts, DSSessionID) ->
    {ok, NDSSubExt, false} = session_add_subscription(
        DSSessionID, TopicFilter, SubOpts
    ),
    ok = ?tp(persistent_session_ds_iterator_updated, #{sub => DSSubExt}),
    NDSSubExt.

-spec del_subscription(topic_filter(), id()) ->
    ok.
del_subscription(TopicFilter, DSSessionId) ->
    %% TODO: transaction?
    ?tp_span(
        persistent_session_ds_subscription_delete,
        #{session_id => DSSessionId},
        ok = session_del_subscription(DSSessionId, TopicFilter)
    ),
    ?tp_span(
        persistent_session_ds_subscription_route_delete,
        #{session_id => DSSessionId},
        ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, DSSessionId)
    ).

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

create_tables() ->
    ok = mria:create_table(
        ?SESSION_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, session},
            {attributes, record_info(fields, session)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_SUBSCRIPTIONS_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, ordered_set},
            {storage, storage()},
            {record_name, ds_sub},
            {attributes, record_info(fields, ds_sub)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_STREAM_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, bag},
            {storage, storage()},
            {record_name, ds_stream},
            {attributes, record_info(fields, ds_stream)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_PUBRANGE_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, ordered_set},
            {storage, storage()},
            {record_name, ds_pubrange},
            {attributes, record_info(fields, ds_pubrange)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_COMMITTED_OFFSET_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, ds_committed_offset},
            {attributes, record_info(fields, ds_committed_offset)}
        ]
    ),
    ok = mria:create_table(
        ?SESSION_SEQUENCE_TAB,
        [
            {rlog_shard, ?DS_MRIA_SHARD},
            {type, set},
            {storage, storage()},
            {record_name, ds_sequence},
            {attributes, record_info(fields, ds_sequence)}
        ]
    ),
    ok = mria:wait_for_tables([
        ?SESSION_TAB,
        ?SESSION_SUBSCRIPTIONS_TAB,
        ?SESSION_STREAM_TAB,
        ?SESSION_PUBRANGE_TAB,
        ?SESSION_COMMITTED_OFFSET_TAB,
        ?SESSION_SEQUENCE_TAB
    ]),
    ok.

-dialyzer({nowarn_function, storage/0}).
storage() ->
    %% FIXME: This is a temporary workaround to avoid crashes when starting on Windows
    case mria:rocksdb_backend_available() of
        true ->
            rocksdb_copies;
        _ ->
            disc_copies
    end.

%% @doc Called when a client connects. This function looks up a
%% session or returns `false` if previous one couldn't be found.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec session_open(id(), emqx_types:conninfo()) ->
    session() | false.
session_open(SessionId, NewConnInfo) ->
    NowMS = now_ms(),
    transaction(fun() ->
        case mnesia:read(?SESSION_TAB, SessionId, write) of
            [Record0 = #session{last_alive_at = LastAliveAt, conninfo = ConnInfo}] ->
                EI = expiry_interval(ConnInfo),
                case ?IS_EXPIRED(NowMS, LastAliveAt, EI) of
                    true ->
                        session_drop(SessionId),
                        false;
                    false ->
                        %% new connection being established
                        Record1 = Record0#session{conninfo = NewConnInfo},
                        Record = session_set_last_alive_at(Record1, NowMS),
                        Session = export_session(Record),
                        DSSubs = session_read_subscriptions(SessionId),
                        Subscriptions = export_subscriptions(DSSubs),
                        Inflight = emqx_persistent_message_ds_replayer:open(SessionId),
                        Session#{
                            conninfo => NewConnInfo,
                            inflight => Inflight,
                            subscriptions => Subscriptions
                        }
                end;
            _ ->
                false
        end
    end).

-spec session_ensure_new(id(), emqx_types:conninfo()) ->
    session().
session_ensure_new(SessionId, ConnInfo) ->
    transaction(fun() ->
        Session = export_session(session_create(SessionId, ConnInfo)),
        Session#{
            subscriptions => subs_new(),
            inflight => emqx_persistent_message_ds_replayer:new()
        }
    end).

session_create(SessionId, ConnInfo) ->
    Session = #session{
        id = SessionId,
        created_at = now_ms(),
        last_alive_at = now_ms(),
        conninfo = ConnInfo
    },
    ok = mnesia:write(?SESSION_TAB, Session, write),
    Session.

session_set_last_alive_at_trans(Session, LastAliveAt) ->
    #{conninfo := ConnInfo} = Session,
    session_set_last_alive_at_trans(Session, ConnInfo, LastAliveAt).

session_set_last_alive_at_trans(Session, NewConnInfo, LastAliveAt) ->
    #{id := SessionId} = Session,
    transaction(fun() ->
        case mnesia:read(?SESSION_TAB, SessionId, write) of
            [#session{} = SessionRecord0] ->
                SessionRecord = SessionRecord0#session{conninfo = NewConnInfo},
                _ = session_set_last_alive_at(SessionRecord, LastAliveAt),
                ok;
            _ ->
                %% log and crash?
                ok
        end
    end),
    Session#{conninfo := NewConnInfo, last_alive_at := LastAliveAt}.

session_set_last_alive_at(SessionRecord0, LastAliveAt) ->
    SessionRecord = SessionRecord0#session{last_alive_at = LastAliveAt},
    ok = mnesia:write(?SESSION_TAB, SessionRecord, write),
    SessionRecord.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id()) -> ok.
session_drop(DSSessionId) ->
    transaction(fun() ->
        ok = session_drop_records(DSSessionId),
        ok = mnesia:delete(?SESSION_TAB, DSSessionId, write)
    end).

-spec session_drop_records(id()) -> ok.
session_drop_records(DSSessionId) ->
    ok = session_drop_subscriptions(DSSessionId),
    ok = session_drop_pubranges(DSSessionId),
    ok = session_drop_offsets(DSSessionId),
    ok = session_drop_streams(DSSessionId).

-spec session_drop_subscriptions(id()) -> ok.
session_drop_subscriptions(DSSessionId) ->
    Subscriptions = session_read_subscriptions(DSSessionId, write),
    lists:foreach(
        fun(#ds_sub{id = DSSubId} = DSSub) ->
            TopicFilter = subscription_id_to_topic_filter(DSSubId),
            ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, DSSessionId),
            ok = session_del_subscription(DSSub)
        end,
        Subscriptions
    ).

%% @doc Called when a client subscribes to a topic. Idempotent.
-spec session_add_subscription(id(), topic_filter(), _Props :: map()) ->
    {ok, subscription(), _IsNew :: boolean()}.
session_add_subscription(DSSessionId, TopicFilter, Props) ->
    DSSubId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        case mnesia:read(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write) of
            [] ->
                DSSub = session_insert_subscription(DSSessionId, TopicFilter, Props),
                DSSubExt = export_subscription(DSSub),
                ?tp(
                    ds_session_subscription_added,
                    #{sub => DSSubExt, session_id => DSSessionId}
                ),
                {ok, DSSubExt, _IsNew = true};
            [#ds_sub{} = DSSub] ->
                NDSSub = session_update_subscription(DSSub, Props),
                NDSSubExt = export_subscription(NDSSub),
                ?tp(
                    ds_session_subscription_present,
                    #{sub => NDSSubExt, session_id => DSSessionId}
                ),
                {ok, NDSSubExt, _IsNew = false}
        end
    end).

-spec session_insert_subscription(id(), topic_filter(), map()) -> ds_sub().
session_insert_subscription(DSSessionId, TopicFilter, Props) ->
    {DSSubId, StartMS} = new_subscription_id(DSSessionId, TopicFilter),
    DSSub = #ds_sub{
        id = DSSubId,
        start_time = StartMS,
        props = Props,
        extra = #{}
    },
    ok = mnesia:write(?SESSION_SUBSCRIPTIONS_TAB, DSSub, write),
    DSSub.

-spec session_update_subscription(ds_sub(), map()) -> ds_sub().
session_update_subscription(DSSub, Props) ->
    NDSSub = DSSub#ds_sub{props = Props},
    ok = mnesia:write(?SESSION_SUBSCRIPTIONS_TAB, NDSSub, write),
    NDSSub.

session_del_subscription(DSSessionId, TopicFilter) ->
    DSSubId = {DSSessionId, TopicFilter},
    transaction(fun() ->
        mnesia:delete(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write)
    end).

session_del_subscription(#ds_sub{id = DSSubId}) ->
    mnesia:delete(?SESSION_SUBSCRIPTIONS_TAB, DSSubId, write).

session_read_subscriptions(DSSessionID) ->
    session_read_subscriptions(DSSessionID, read).

session_read_subscriptions(DSSessionId, LockKind) ->
    MS = ets:fun2ms(
        fun(Sub = #ds_sub{id = {Sess, _}}) when Sess =:= DSSessionId ->
            Sub
        end
    ),
    mnesia:select(?SESSION_SUBSCRIPTIONS_TAB, MS, LockKind).

session_read_pubranges(DSSessionID) ->
    session_read_pubranges(DSSessionID, read).

session_read_pubranges(DSSessionId, LockKind) ->
    MS = ets:fun2ms(
        fun(#ds_pubrange{id = {Sess, First}}) when Sess =:= DSSessionId ->
            {DSSessionId, First}
        end
    ),
    mnesia:select(?SESSION_PUBRANGE_TAB, MS, LockKind).

session_read_offsets(DSSessionID) ->
    session_read_offsets(DSSessionID, read).

session_read_offsets(DSSessionId, LockKind) ->
    MS = ets:fun2ms(
        fun(#ds_committed_offset{id = {Sess, Type}}) when Sess =:= DSSessionId ->
            {DSSessionId, Type}
        end
    ),
    mnesia:select(?SESSION_COMMITTED_OFFSET_TAB, MS, LockKind).

-spec new_subscription_id(id(), topic_filter()) -> {subscription_id(), integer()}.
new_subscription_id(DSSessionId, TopicFilter) ->
    %% Note: here we use _milliseconds_ to match with the timestamp
    %% field of `#message' record.
    NowMS = now_ms(),
    DSSubId = {DSSessionId, TopicFilter},
    {DSSubId, NowMS}.

-spec subscription_id_to_topic_filter(subscription_id()) -> topic_filter().
subscription_id_to_topic_filter({_DSSessionId, TopicFilter}) ->
    TopicFilter.

now_ms() ->
    erlang:system_time(millisecond).

%%--------------------------------------------------------------------
%% RPC targets (v1)
%%--------------------------------------------------------------------

%% RPC target.
-spec do_open_iterator(emqx_types:words(), emqx_ds:time(), emqx_ds:iterator_id()) ->
    {ok, emqx_ds_storage_layer:iterator()} | {error, _Reason}.
do_open_iterator(_TopicFilter, _StartMS, _IteratorID) ->
    {error, not_implemented}.

%% RPC target.
-spec do_ensure_iterator_closed(emqx_ds:iterator_id()) -> ok.
do_ensure_iterator_closed(_IteratorID) ->
    ok.

%% RPC target.
-spec do_ensure_all_iterators_closed(id()) -> ok.
do_ensure_all_iterators_closed(_DSSessionID) ->
    ok.

%%--------------------------------------------------------------------
%% Reading batches
%%--------------------------------------------------------------------

-spec renew_streams(session()) -> ok.
renew_streams(#{id := SessionId, subscriptions := Subscriptions}) ->
    transaction(fun() ->
        ExistingStreams = mnesia:read(?SESSION_STREAM_TAB, SessionId, write),
        subs_fold(
            fun(TopicFilter, #{start_time := StartTime}, Streams) ->
                TopicFilterWords = emqx_topic:words(TopicFilter),
                renew_topic_streams(SessionId, TopicFilterWords, StartTime, Streams)
            end,
            ExistingStreams,
            Subscriptions
        )
    end),
    ok.

-spec renew_topic_streams(id(), topic_filter_words(), emqx_ds:time(), _Acc :: [ds_stream()]) -> ok.
renew_topic_streams(DSSessionId, TopicFilter, StartTime, ExistingStreams) ->
    TopicStreams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime),
    lists:foldl(
        fun({Rank, Stream}, Streams) ->
            case lists:keymember(Stream, #ds_stream.stream, Streams) of
                true ->
                    Streams;
                false ->
                    StreamRef = length(Streams) + 1,
                    DSStream = session_store_stream(
                        DSSessionId,
                        StreamRef,
                        Stream,
                        Rank,
                        TopicFilter,
                        StartTime
                    ),
                    [DSStream | Streams]
            end
        end,
        ExistingStreams,
        TopicStreams
    ).

session_store_stream(DSSessionId, StreamRef, Stream, Rank, TopicFilter, StartTime) ->
    {ok, ItBegin} = emqx_ds:make_iterator(
        ?PERSISTENT_MESSAGE_DB,
        Stream,
        TopicFilter,
        StartTime
    ),
    DSStream = #ds_stream{
        session = DSSessionId,
        ref = StreamRef,
        stream = Stream,
        rank = Rank,
        beginning = ItBegin
    },
    mnesia:write(?SESSION_STREAM_TAB, DSStream, write),
    DSStream.

%% must be called inside a transaction
-spec session_drop_streams(id()) -> ok.
session_drop_streams(DSSessionId) ->
    mnesia:delete(?SESSION_STREAM_TAB, DSSessionId, write).

%% must be called inside a transaction
-spec session_drop_pubranges(id()) -> ok.
session_drop_pubranges(DSSessionId) ->
    RangeIds = session_read_pubranges(DSSessionId, write),
    lists:foreach(
        fun(RangeId) ->
            mnesia:delete(?SESSION_PUBRANGE_TAB, RangeId, write)
        end,
        RangeIds
    ).

%% must be called inside a transaction
-spec session_drop_offsets(id()) -> ok.
session_drop_offsets(DSSessionId) ->
    OffsetIds = session_read_offsets(DSSessionId, write),
    lists:foreach(
        fun(OffsetId) ->
            mnesia:delete(?SESSION_COMMITTED_OFFSET_TAB, OffsetId, write)
        end,
        OffsetIds
    ).

%%--------------------------------------------------------------------------------

subs_new() ->
    emqx_topic_gbt:new().

subs_lookup(TopicFilter, Subs) ->
    emqx_topic_gbt:lookup(TopicFilter, [], Subs, undefined).

subs_insert(TopicFilter, Subscription, Subs) ->
    emqx_topic_gbt:insert(TopicFilter, [], Subscription, Subs).

subs_delete(TopicFilter, Subs) ->
    emqx_topic_gbt:delete(TopicFilter, [], Subs).

subs_matches(Topic, Subs) ->
    emqx_topic_gbt:matches(Topic, Subs, []).

subs_get_match(M, Subs) ->
    emqx_topic_gbt:get_record(M, Subs).

subs_size(Subs) ->
    emqx_topic_gbt:size(Subs).

subs_to_map(Subs) ->
    subs_fold(
        fun(TopicFilter, #{props := Props}, Acc) -> Acc#{TopicFilter => Props} end,
        #{},
        Subs
    ).

subs_fold(Fun, AccIn, Subs) ->
    emqx_topic_gbt:fold(
        fun(Key, Sub, Acc) -> Fun(emqx_topic_gbt:get_topic(Key), Sub, Acc) end,
        AccIn,
        Subs
    ).

%%--------------------------------------------------------------------------------

transaction(Fun) ->
    case mnesia:is_transaction() of
        true ->
            Fun();
        false ->
            {atomic, Res} = mria:transaction(?DS_MRIA_SHARD, Fun),
            Res
    end.

ro_transaction(Fun) ->
    {atomic, Res} = mria:ro_transaction(?DS_MRIA_SHARD, Fun),
    Res.

%%--------------------------------------------------------------------------------

export_subscriptions(DSSubs) ->
    lists:foldl(
        fun(DSSub = #ds_sub{id = {_DSSessionId, TopicFilter}}, Acc) ->
            subs_insert(TopicFilter, export_subscription(DSSub), Acc)
        end,
        subs_new(),
        DSSubs
    ).

export_session(#session{} = Record) ->
    export_record(Record, #session.id, [id, created_at, last_alive_at, conninfo, props], #{}).

export_subscription(#ds_sub{} = Record) ->
    export_record(Record, #ds_sub.start_time, [start_time, props, extra], #{}).

export_record(Record, I, [Field | Rest], Acc) ->
    export_record(Record, I + 1, Rest, Acc#{Field => element(I, Record)});
export_record(_, _, [], Acc) ->
    Acc.

%% TODO: find a more reliable way to perform actions that have side
%% effects. Add `CBM:init' callback to the session behavior?
-spec ensure_timers(session()) -> session().
ensure_timers(Session0) ->
    Session1 = emqx_session:ensure_timer(?TIMER_PULL, 100, Session0),
    Session2 = emqx_session:ensure_timer(?TIMER_GET_STREAMS, 100, Session1),
    emqx_session:ensure_timer(?TIMER_BUMP_LAST_ALIVE_AT, 100, Session2).

-spec pull_now(session()) -> session().
pull_now(Session) ->
    emqx_session:reset_timer(?TIMER_PULL, 0, Session).

-spec receive_maximum(conninfo()) -> pos_integer().
receive_maximum(ConnInfo) ->
    %% Note: the default value should be always set by the channel
    %% with respect to the zone configuration, but the type spec
    %% indicates that it's optional.
    maps:get(receive_maximum, ConnInfo, 65_535).

-spec expiry_interval(conninfo()) -> millisecond().
expiry_interval(ConnInfo) ->
    maps:get(expiry_interval, ConnInfo, 0).

-ifdef(TEST).
list_all_sessions() ->
    DSSessionIds = mnesia:dirty_all_keys(?SESSION_TAB),
    ConnInfo = #{},
    Sessions = lists:filtermap(
        fun(SessionID) ->
            Sess = session_open(SessionID, ConnInfo),
            case Sess of
                false ->
                    false;
                _ ->
                    {true, {SessionID, Sess}}
            end
        end,
        DSSessionIds
    ),
    maps:from_list(Sessions).

list_all_subscriptions() ->
    DSSubIds = mnesia:dirty_all_keys(?SESSION_SUBSCRIPTIONS_TAB),
    Subscriptions = lists:map(
        fun(DSSubId) ->
            [DSSub] = mnesia:dirty_read(?SESSION_SUBSCRIPTIONS_TAB, DSSubId),
            {DSSubId, export_subscription(DSSub)}
        end,
        DSSubIds
    ),
    maps:from_list(Subscriptions).

list_all_streams() ->
    DSStreamIds = mnesia:dirty_all_keys(?SESSION_STREAM_TAB),
    DSStreams = lists:map(
        fun(DSStreamId) ->
            Records = mnesia:dirty_read(?SESSION_STREAM_TAB, DSStreamId),
            ExtDSStreams =
                lists:map(
                    fun(Record) ->
                        export_record(
                            Record,
                            #ds_stream.session,
                            [session, topic_filter, stream, rank],
                            #{}
                        )
                    end,
                    Records
                ),
            {DSStreamId, ExtDSStreams}
        end,
        DSStreamIds
    ),
    maps:from_list(DSStreams).

list_all_pubranges() ->
    DSPubranges = mnesia:dirty_match_object(?SESSION_PUBRANGE_TAB, #ds_pubrange{_ = '_'}),
    lists:foldl(
        fun(Record = #ds_pubrange{id = {SessionId, First}}, Acc) ->
            Range = export_record(
                Record, #ds_pubrange.until, [until, stream, type, iterator], #{first => First}
            ),
            maps:put(SessionId, maps:get(SessionId, Acc, []) ++ [Range], Acc)
        end,
        #{},
        DSPubranges
    ).

%% ifdef(TEST)
-endif.
