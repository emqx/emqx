%%--------------------------------------------------------------------
%% Copyright (c) 2021-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_persistent_session_ds.hrl").

-ifdef(TEST).
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

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
    list_all_sessions/0
]).
-endif.

-export_type([
    id/0,
    seqno/0,
    timestamp/0,
    topic_filter/0,
    subscription/0,
    session/0,
    stream_state/0
]).

-type seqno() :: non_neg_integer().

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type topic_filter() :: emqx_types:topic().

-type subscription() :: #{
    start_time := emqx_ds:time(),
    props := map(),
    extra := map()
}.

%%%%% Session sequence numbers:
-define(next(QOS), {0, QOS}).
%% Note: we consider the sequence number _committed_ once the full
%% packet MQTT flow is completed for the sequence number. That is,
%% when we receive PUBACK for the QoS1 message, or PUBCOMP, or PUBREC
%% with Reason code > 0x80 for QoS2 message.
-define(committed(QOS), {1, QOS}).
%% For QoS2 messages we also need to store the sequence number of the
%% last PUBREL message:
-define(pubrec, 2).

-define(TIMER_PULL, timer_pull).
-define(TIMER_GET_STREAMS, timer_get_streams).
-define(TIMER_BUMP_LAST_ALIVE_AT, timer_bump_last_alive_at).
-type timer() :: ?TIMER_PULL | ?TIMER_GET_STREAMS | ?TIMER_BUMP_LAST_ALIVE_AT.

-type session() :: #{
    %% Client ID
    id := id(),
    %% Configuration:
    props := map(),
    %% Persistent state:
    s := emqx_persistent_session_ds_state:t(),
    %% Buffer:
    inflight := emqx_persistent_session_ds_inflight:t(),
    %% Timers:
    timer() => reference()
}.

-type stream_state() :: #ifs{}.

-type timestamp() :: emqx_utils_calendar:epoch_millisecond().
-type millisecond() :: non_neg_integer().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().

-define(STATS_KEYS, [
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max
]).

%%

-spec create(clientinfo(), conninfo(), emqx_session:conf()) ->
    session().
create(#{clientid := ClientID}, ConnInfo, Conf) ->
    ensure_timers(session_ensure_new(ClientID, ConnInfo, Conf)).

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
            Session = Session0#{props => Conf},
            {true, ensure_timers(Session), []};
        false ->
            false
    end.

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
info(created_at, #{s := S}) ->
    emqx_persistent_session_ds_state:get_created_at(S);
info(is_persistent, #{}) ->
    true;
info(subscriptions, #{s := S}) ->
    subs_to_map(S);
info(subscriptions_cnt, #{s := S}) ->
    emqx_topic_gbt:size(emqx_persistent_session_ds_state:get_subscriptions(S));
info(subscriptions_max, #{props := Conf}) ->
    maps:get(max_subscriptions, Conf);
info(upgrade_qos, #{props := Conf}) ->
    maps:get(upgrade_qos, Conf);
info(inflight, #{inflight := Inflight}) ->
    Inflight;
info(inflight_cnt, #{inflight := Inflight}) ->
    emqx_persistent_session_ds_inflight:n_inflight(Inflight);
info(inflight_max, #{inflight := Inflight}) ->
    emqx_persistent_session_ds_inflight:receive_maximum(Inflight);
info(retry_interval, #{props := Conf}) ->
    maps:get(retry_interval, Conf);
% info(mqueue, #sessmem{mqueue = MQueue}) ->
%     MQueue;
info(mqueue_len, #{inflight := Inflight}) ->
    emqx_persistent_session_ds_inflight:n_buffered(Inflight);
% info(mqueue_max, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:max_len(MQueue);
info(mqueue_dropped, _Session) ->
    0;
%% info(next_pkt_id, #{s := S}) ->
%%     {PacketId, _} = emqx_persistent_message_ds_replayer:next_packet_id(S),
%%     PacketId;
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
-spec print_session(emqx_types:clientid()) -> map() | undefined.
print_session(ClientId) ->
    emqx_persistent_session_ds_state:print_session(ClientId).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE / UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec subscribe(topic_filter(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    TopicFilter,
    SubOpts,
    Session = #{id := ID, s := S0}
) ->
    case subs_lookup(TopicFilter, S0) of
        undefined ->
            %% N.B.: we chose to update the router before adding the
            %% subscription to the session/iterator table. The
            %% reasoning for this is as follows:
            %%
            %% Messages matching this topic filter should start to be
            %% persisted as soon as possible to avoid missing
            %% messages. If this is the first such persistent session
            %% subscription, it's important to do so early on.
            %%
            %% This could, in turn, lead to some inconsistency: if
            %% such a route gets created but the session/iterator data
            %% fails to be updated accordingly, we have a dangling
            %% route. To remove such dangling routes, we may have a
            %% periodic GC process that removes routes that do not
            %% have a matching persistent subscription. Also, route
            %% operations use dirty mnesia operations, which
            %% inherently have room for inconsistencies.
            %%
            %% In practice, we use the iterator reference table as a
            %% source of truth, since it is guarded by a transaction
            %% context: we consider a subscription operation to be
            %% successful if it ended up changing this table. Both
            %% router and iterator information can be reconstructed
            %% from this table, if needed.
            ok = emqx_persistent_session_ds_router:do_add_route(TopicFilter, ID),
            Subscription = #{
                start_time => now_ms(),
                props => SubOpts
            },
            IsNew = true;
        Subscription0 = #{} ->
            Subscription = Subscription0#{props => SubOpts},
            IsNew = false
    end,
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, [], Subscription, S0),
    ?tp(persistent_session_ds_subscription_added, #{
        topic_filter => TopicFilter, sub => Subscription, is_new => IsNew
    }),
    {ok, Session#{s => S}}.

-spec unsubscribe(topic_filter(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    TopicFilter,
    Session = #{id := ID, s := S0}
) ->
    %% TODO: drop streams and messages from the buffer
    case subs_lookup(TopicFilter, S0) of
        #{props := SubOpts} ->
            S = emqx_persistent_session_ds_state:del_subscription(TopicFilter, [], S0),
            ?tp_span(
                persistent_session_ds_subscription_route_delete,
                #{session_id => ID},
                ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, ID)
            ),
            {ok, Session#{s => S}, SubOpts};
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED}
    end.

-spec get_subscription(topic_filter(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(TopicFilter, #{s := S}) ->
    case subs_lookup(TopicFilter, S) of
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
puback(_ClientInfo, PacketId, Session0) ->
    case commit_seqno(puback, PacketId, Session0) of
        {ok, Msg, Session} ->
            {ok, Msg, [], inc_send_quota(Session)};
        Error ->
            Error
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec pubrec(emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()}
    | {error, emqx_types:reason_code()}.
pubrec(PacketId, Session0) ->
    case commit_seqno(pubrec, PacketId, Session0) of
        {ok, Msg, Session} ->
            {ok, Msg, Session};
        Error = {error, _} ->
            Error
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
pubcomp(_ClientInfo, PacketId, Session0) ->
    case commit_seqno(pubcomp, PacketId, Session0) of
        {ok, Msg, Session} ->
            {ok, Msg, [], inc_send_quota(Session)};
        Error = {error, _} ->
            Error
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
    Session0
) ->
    {Publishes, Session1} = drain_buffer(fill_buffer(Session0, ClientInfo)),
    Timeout =
        case Publishes of
            [] ->
                emqx_config:get([session_persistence, idle_poll_interval]);
            [_ | _] ->
                0
        end,
    Session = emqx_session:ensure_timer(?TIMER_PULL, Timeout, Session1),
    {ok, Publishes, Session};
handle_timeout(_ClientInfo, ?TIMER_GET_STREAMS, Session0 = #{s := S0}) ->
    S = renew_streams(S0),
    Interval = emqx_config:get([session_persistence, renew_streams_interval]),
    Session = emqx_session:ensure_timer(
        ?TIMER_GET_STREAMS,
        Interval,
        Session0#{s => S}
    ),
    {ok, [], Session};
handle_timeout(_ClientInfo, ?TIMER_BUMP_LAST_ALIVE_AT, Session0 = #{s := S0}) ->
    S = emqx_persistent_session_ds_state:commit(bump_last_alive(S0)),
    Session = emqx_session:ensure_timer(
        ?TIMER_BUMP_LAST_ALIVE_AT,
        bump_interval(),
        Session0#{s => S}
    ),
    {ok, [], Session};
handle_timeout(_ClientInfo, expire_awaiting_rel, Session) ->
    %% TODO: stub
    {ok, [], Session}.

bump_last_alive(S0) ->
    %% Note: we take a pessimistic approach here and assume that the client will be alive
    %% until the next bump timeout.  With this, we avoid garbage collecting this session
    %% too early in case the session/connection/node crashes earlier without having time
    %% to commit the time.
    EstimatedLastAliveAt = now_ms() + bump_interval(),
    emqx_persistent_session_ds_state:set_last_alive_at(EstimatedLastAliveAt, S0).

-spec replay(clientinfo(), [], session()) ->
    {ok, replies(), session()}.
replay(ClientInfo, [], Session0) ->
    Streams = find_replay_streams(Session0),
    Session = lists:foldl(
        fun({StreamKey, Stream}, SessionAcc) ->
            replay_batch(StreamKey, Stream, SessionAcc, ClientInfo)
        end,
        Session0,
        Streams
    ),
    %% Note: we filled the buffer with the historical messages, and
    %% from now on we'll rely on the normal inflight/flow control
    %% mechanisms to replay them:
    {ok, [], pull_now(Session)}.
%%--------------------------------------------------------------------

-spec disconnect(session(), emqx_types:conninfo()) -> {shutdown, session()}.
disconnect(Session = #{s := S0}, _ConnInfo) ->
    S1 = emqx_persistent_session_ds_state:set_last_alive_at(now_ms(), S0),
    S = emqx_persistent_session_ds_state:commit(S1),
    {shutdown, Session#{s => S}}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(_Reason, _Session = #{s := S}) ->
    emqx_persistent_session_ds_state:commit(S),
    ok.

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

create_tables() ->
    emqx_persistent_session_ds_state:create_tables().

-define(IS_EXPIRED(NOW_MS, LAST_ALIVE_AT, EI),
    (is_number(LAST_ALIVE_AT) andalso
        is_number(EI) andalso
        (NOW_MS >= LAST_ALIVE_AT + EI))
).

%% @doc Called when a client connects. This function looks up a
%% session or returns `false` if previous one couldn't be found.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec session_open(id(), emqx_types:conninfo()) ->
    session() | false.
session_open(SessionId, NewConnInfo) ->
    NowMS = now_ms(),
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            EI = expiry_interval(emqx_persistent_session_ds_state:get_conninfo(S0)),
            LastAliveAt = emqx_persistent_session_ds_state:get_last_alive_at(S0),
            case ?IS_EXPIRED(NowMS, LastAliveAt, EI) of
                true ->
                    emqx_persistent_session_ds_state:delete(SessionId),
                    false;
                false ->
                    %% New connection being established
                    S1 = emqx_persistent_session_ds_state:set_conninfo(NewConnInfo, S0),
                    S2 = emqx_persistent_session_ds_state:set_last_alive_at(NowMS, S1),
                    S = emqx_persistent_session_ds_state:commit(S2),
                    Inflight = emqx_persistent_session_ds_inflight:new(
                        receive_maximum(NewConnInfo)
                    ),
                    #{
                        id => SessionId,
                        s => S,
                        inflight => Inflight,
                        props => #{}
                    }
            end;
        undefined ->
            false
    end.

-spec session_ensure_new(id(), emqx_types:conninfo(), emqx_session:conf()) ->
    session().
session_ensure_new(Id, ConnInfo, Conf) ->
    Now = now_ms(),
    S0 = emqx_persistent_session_ds_state:create_new(Id),
    S1 = emqx_persistent_session_ds_state:set_conninfo(ConnInfo, S0),
    S2 = bump_last_alive(S1),
    S3 = emqx_persistent_session_ds_state:set_created_at(Now, S2),
    S4 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_1), 0, S3),
    S5 = emqx_persistent_session_ds_state:put_seqno(?committed(?QOS_1), 0, S4),
    S6 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_2), 0, S5),
    S7 = emqx_persistent_session_ds_state:put_seqno(?committed(?QOS_2), 0, S6),
    S8 = emqx_persistent_session_ds_state:put_seqno(?pubrec, 0, S7),
    S = emqx_persistent_session_ds_state:commit(S8),
    #{
        id => Id,
        props => Conf,
        s => S,
        inflight => emqx_persistent_session_ds_inflight:new(receive_maximum(ConnInfo))
    }.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id()) -> ok.
session_drop(ID) ->
    emqx_persistent_session_ds_state:delete(ID).

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
%% Buffer filling
%%--------------------------------------------------------------------

fill_buffer(Session = #{s := S}, ClientInfo) ->
    fill_buffer(shuffle(find_new_streams(S)), Session, ClientInfo).

-spec shuffle([A]) -> [A].
shuffle(L0) ->
    L1 = lists:map(
        fun(A) ->
            %% maybe topic/stream prioritization could be introduced here?
            {rand:uniform(), A}
        end,
        L0
    ),
    L2 = lists:sort(L1),
    {_, L} = lists:unzip(L2),
    L.

fill_buffer([], Session, _ClientInfo) ->
    Session;
fill_buffer(
    [{StreamKey, Stream0 = #ifs{it_end = It0}} | Streams],
    Session0 = #{s := S0, inflight := Inflight0},
    ClientInfo
) ->
    BatchSize = emqx_config:get([session_persistence, max_batch_size]),
    MaxBufferSize = BatchSize * 2,
    case emqx_persistent_session_ds_inflight:n_buffered(Inflight0) < MaxBufferSize of
        true ->
            case emqx_ds:next(?PERSISTENT_MESSAGE_DB, It0, BatchSize) of
                {ok, It, []} ->
                    S = emqx_persistent_session_ds_state:put_stream(
                        StreamKey, Stream0#ifs{it_end = It}, S0
                    ),
                    fill_buffer(Streams, Session0#{s := S}, ClientInfo);
                {ok, It, Messages} ->
                    Session = new_batch(StreamKey, Stream0, It, Messages, Session0, ClientInfo),
                    fill_buffer(Streams, Session, ClientInfo);
                {ok, end_of_stream} ->
                    S = emqx_persistent_session_ds_state:put_stream(
                        StreamKey, Stream0#ifs{it_end = end_of_stream}, S0
                    ),
                    fill_buffer(Streams, Session0#{s := S}, ClientInfo)
            end;
        false ->
            Session0
    end.

new_batch(
    StreamKey, Stream0, Iterator, [{BatchBeginMsgKey, _} | _] = Messages0, Session0, ClientInfo
) ->
    #{inflight := Inflight0, s := S0} = Session0,
    FirstSeqnoQos1 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_1), S0),
    FirstSeqnoQos2 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_2), S0),
    NBefore = emqx_persistent_session_ds_inflight:n_buffered(Inflight0),
    {LastSeqnoQos1, LastSeqnoQos2, Session} = do_process_batch(
        false, FirstSeqnoQos1, FirstSeqnoQos2, Messages0, Session0, ClientInfo
    ),
    NAfter = emqx_persistent_session_ds_inflight:n_buffered(maps:get(inflight, Session)),
    Stream = Stream0#ifs{
        batch_size = NAfter - NBefore,
        batch_begin_key = BatchBeginMsgKey,
        first_seqno_qos1 = FirstSeqnoQos1,
        first_seqno_qos2 = FirstSeqnoQos2,
        last_seqno_qos1 = LastSeqnoQos1,
        last_seqno_qos2 = LastSeqnoQos2,
        it_end = Iterator
    },
    S1 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_1), LastSeqnoQos1, S0),
    S2 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_2), LastSeqnoQos2, S1),
    S = emqx_persistent_session_ds_state:put_stream(StreamKey, Stream, S2),
    Session#{s => S}.

replay_batch(_StreamKey, Stream, Session0, ClientInfo) ->
    #ifs{
        batch_begin_key = BatchBeginMsgKey,
        batch_size = BatchSize,
        first_seqno_qos1 = FirstSeqnoQos1,
        first_seqno_qos2 = FirstSeqnoQos2,
        it_end = ItEnd
    } = Stream,
    {ok, ItBegin} = emqx_ds:update_iterator(?PERSISTENT_MESSAGE_DB, ItEnd, BatchBeginMsgKey),
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, BatchSize) of
        {ok, _ItEnd, Messages} ->
            {_LastSeqnoQo1, _LastSeqnoQos2, Session} = do_process_batch(
                true, FirstSeqnoQos1, FirstSeqnoQos2, Messages, Session0, ClientInfo
            ),
            %% TODO: check consistency of the sequence numbers
            Session
    end.

do_process_batch(_IsReplay, LastSeqnoQos1, LastSeqnoQos2, [], Session, _ClientInfo) ->
    {LastSeqnoQos1, LastSeqnoQos2, Session};
do_process_batch(IsReplay, FirstSeqnoQos1, FirstSeqnoQos2, [KV | Messages], Session, ClientInfo) ->
    #{s := S, props := #{upgrade_qos := UpgradeQoS}, inflight := Inflight0} = Session,
    {_DsMsgKey, Msg0 = #message{topic = Topic}} = KV,
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    Msgs = [
        Msg
     || SubMatch <- emqx_topic_gbt:matches(Topic, Subs, []),
        Msg <- begin
            #{props := SubOpts} = emqx_topic_gbt:get_record(SubMatch, Subs),
            emqx_session:enrich_message(ClientInfo, Msg0, SubOpts, UpgradeQoS)
        end
    ],
    CommittedQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommittedQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    {Inflight, LastSeqnoQos1, LastSeqnoQos2} = lists:foldl(
        fun(Msg = #message{qos = Qos}, {Inflight1, SeqnoQos10, SeqnoQos20}) ->
            case Qos of
                ?QOS_0 ->
                    SeqnoQos1 = SeqnoQos10,
                    SeqnoQos2 = SeqnoQos20,
                    PacketId = undefined;
                ?QOS_1 ->
                    SeqnoQos1 = inc_seqno(?QOS_1, SeqnoQos10),
                    SeqnoQos2 = SeqnoQos20,
                    PacketId = seqno_to_packet_id(?QOS_1, SeqnoQos1);
                ?QOS_2 ->
                    SeqnoQos1 = SeqnoQos10,
                    SeqnoQos2 = inc_seqno(?QOS_2, SeqnoQos20),
                    PacketId = seqno_to_packet_id(?QOS_2, SeqnoQos2)
            end,
            %% ?SLOG(debug, #{
            %%     msg => "out packet",
            %%     qos => Qos,
            %%     packet_id => PacketId,
            %%     enriched => emqx_message:to_map(Msg),
            %%     original => emqx_message:to_map(Msg0),
            %%     upgrade_qos => UpgradeQoS
            %% }),

            %% Handle various situations where we want to ignore the packet:
            Inflight2 =
                case IsReplay of
                    true when Qos =:= ?QOS_0 ->
                        Inflight1;
                    true when Qos =:= ?QOS_1, SeqnoQos1 < CommittedQos1 ->
                        Inflight1;
                    true when Qos =:= ?QOS_2, SeqnoQos2 < CommittedQos2 ->
                        Inflight1;
                    _ ->
                        emqx_persistent_session_ds_inflight:push({PacketId, Msg}, Inflight1)
                end,
            {
                Inflight2,
                SeqnoQos1,
                SeqnoQos2
            }
        end,
        {Inflight0, FirstSeqnoQos1, FirstSeqnoQos2},
        Msgs
    ),
    do_process_batch(
        IsReplay, LastSeqnoQos1, LastSeqnoQos2, Messages, Session#{inflight => Inflight}, ClientInfo
    ).

%%--------------------------------------------------------------------
%% Buffer drain
%%--------------------------------------------------------------------

drain_buffer(Session = #{inflight := Inflight0}) ->
    {Messages, Inflight} = emqx_persistent_session_ds_inflight:pop(Inflight0),
    {Messages, Session#{inflight => Inflight}}.

%%--------------------------------------------------------------------
%% Stream renew
%%--------------------------------------------------------------------

%% erlfmt-ignore
-define(fully_replayed(STREAM, COMMITTEDQOS1, COMMITTEDQOS2),
    ((STREAM#ifs.last_seqno_qos1 =< COMMITTEDQOS1 orelse STREAM#ifs.last_seqno_qos1 =:= undefined) andalso
     (STREAM#ifs.last_seqno_qos2 =< COMMITTEDQOS2 orelse STREAM#ifs.last_seqno_qos2 =:= undefined))).

%% erlfmt-ignore
-define(last_replayed(STREAM, NEXTQOS1, NEXTQOS2),
    ((STREAM#ifs.last_seqno_qos1 == NEXTQOS1 orelse STREAM#ifs.last_seqno_qos1 =:= undefined) andalso
     (STREAM#ifs.last_seqno_qos2 == NEXTQOS2 orelse STREAM#ifs.last_seqno_qos2 =:= undefined))).

-spec find_replay_streams(session()) ->
    [{emqx_persistent_session_ds_state:stream_key(), stream_state()}].
find_replay_streams(#{s := S}) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    Streams = emqx_persistent_session_ds_state:fold_streams(
        fun(Key, Stream, Acc) ->
            case Stream of
                #ifs{
                    first_seqno_qos1 = F1,
                    first_seqno_qos2 = F2,
                    last_seqno_qos1 = L1,
                    last_seqno_qos2 = L2
                } when F1 >= CommQos1, L1 =< CommQos1, F2 >= CommQos2, L2 =< CommQos2 ->
                    [{Key, Stream} | Acc];
                _ ->
                    Acc
            end
        end,
        [],
        S
    ),
    lists:sort(
        fun(
            #ifs{first_seqno_qos1 = A1, first_seqno_qos2 = A2},
            #ifs{first_seqno_qos1 = B1, first_seqno_qos2 = B2}
        ) ->
            case A1 =:= A2 of
                true -> B1 =< B2;
                false -> A1 < A2
            end
        end,
        Streams
    ).

-spec find_new_streams(emqx_persistent_session_ds_state:t()) ->
    [{emqx_persistent_session_ds_state:stream_key(), stream_state()}].
find_new_streams(S) ->
    %% FIXME: this function is currently very sensitive to the
    %% consistency of the packet IDs on both broker and client side.
    %%
    %% If the client fails to properly ack packets due to a bug, or a
    %% network issue, or if the state of streams and seqno tables ever
    %% become de-synced, then this function will return an empty list,
    %% and the replay cannot progress.
    %%
    %% In other words, this function is not robust, and we should find
    %% some way to get the replays un-stuck at the cost of potentially
    %% losing messages during replay (or just kill the stuck channel
    %% after timeout?)
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    emqx_persistent_session_ds_state:fold_streams(
        fun
            (Key, Stream, Acc) when ?fully_replayed(Stream, CommQos1, CommQos2) ->
                %% This stream has been full acked by the client. It
                %% means we can get more messages from it:
                [{Key, Stream} | Acc];
            (_Key, _Stream, Acc) ->
                Acc
        end,
        [],
        S
    ).

-spec renew_streams(emqx_persistent_session_ds_state:t()) -> emqx_persistent_session_ds_state:t().
renew_streams(S0) ->
    CommQos1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S0),
    CommQos2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S0),
    subs_fold(
        fun(TopicFilterBin, _Subscription = #{start_time := StartTime}, S1) ->
            SubId = [],
            TopicFilter = emqx_topic:words(TopicFilterBin),
            TopicStreams = emqx_ds:get_streams(?PERSISTENT_MESSAGE_DB, TopicFilter, StartTime),
            TopicStreamGroups = maps:groups_from_list(fun({{X, _}, _}) -> X end, TopicStreams),
            %% Iterate over groups of streams with the same rank X,
            %% finding the first eligible stream to replay:
            maps:fold(
                fun(RankX, Streams, S2) ->
                    Key = {RankX, SubId},
                    case emqx_persistent_session_ds_state:get_stream(Key, S2) of
                        undefined ->
                            MinRankY = emqx_persistent_session_ds_state:get_rank(RankX, S2),
                            start_stream_replay(
                                TopicFilter, StartTime, Key, MinRankY, Streams, S2
                            );
                        Stream = #ifs{it_end = end_of_stream, rank_y = MinRankY} when
                            ?fully_replayed(Stream, CommQos1, CommQos2)
                        ->
                            %% We have fully replayed the stream with
                            %% the given rank X, and the client acked
                            %% all messages:
                            S3 = emqx_persistent_session_ds_state:del_stream(Key, S2),
                            S4 = emqx_persistent_session_ds_state:put_rank(RankX, MinRankY, S3),
                            start_stream_replay(TopicFilter, StartTime, Key, MinRankY, Streams, S4);
                        #ifs{} ->
                            %% Stream replay is currently in progress, leave it as is:
                            S2
                    end
                end,
                S1,
                TopicStreamGroups
            )
        end,
        S0,
        S0
    ).

start_stream_replay(TopicFilter, StartTime, Key, MinRankY, Streams, S0) ->
    case find_first_stream(MinRankY, Streams) of
        {RankY, Stream} ->
            {ok, Iterator} = emqx_ds:make_iterator(
                ?PERSISTENT_MESSAGE_DB, Stream, TopicFilter, StartTime
            ),
            NewStreamState = #ifs{
                rank_y = RankY,
                it_end = Iterator
            },
            emqx_persistent_session_ds_state:put_stream(Key, NewStreamState, S0);
        undefined ->
            S0
    end.

%% @doc Find the first stream with rank Y greater than the one given as the first argument.
-spec find_first_stream(emqx_ds:rank_y() | undefined, [
    {emqx_ds:stream_rank(), emqx_ds:ds_specific_stream()}
]) ->
    {emqx_ds:rank_y(), emqx_ds:ds_specific_stream()} | undefined.
find_first_stream(MinRankY, Streams) ->
    lists:foldl(
        fun
            ({{_RankX, RankY}, Stream}, Acc) when RankY > MinRankY; MinRankY =:= undefined ->
                case Acc of
                    {AccY, _} when AccY < RankY ->
                        Acc;
                    _ ->
                        {RankY, Stream}
                end;
            (_, Acc) ->
                Acc
        end,
        undefined,
        Streams
    ).

%%--------------------------------------------------------------------------------

subs_lookup(TopicFilter, S) ->
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    emqx_topic_gbt:lookup(TopicFilter, [], Subs, undefined).

subs_to_map(S) ->
    subs_fold(
        fun(TopicFilter, #{props := Props}, Acc) -> Acc#{TopicFilter => Props} end,
        #{},
        S
    ).

subs_fold(Fun, AccIn, S) ->
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    emqx_topic_gbt:fold(
        fun(Key, Sub, Acc) -> Fun(emqx_topic_gbt:get_topic(Key), Sub, Acc) end,
        AccIn,
        Subs
    ).

%%--------------------------------------------------------------------------------

%% TODO: find a more reliable way to perform actions that have side
%% effects. Add `CBM:init' callback to the session behavior?
-spec ensure_timers(session()) -> session().
ensure_timers(Session0) ->
    Session1 = emqx_session:ensure_timer(?TIMER_PULL, 100, Session0),
    Session2 = emqx_session:ensure_timer(?TIMER_GET_STREAMS, 100, Session1),
    emqx_session:ensure_timer(?TIMER_BUMP_LAST_ALIVE_AT, 100, Session2).

-spec inc_send_quota(session()) -> session().
inc_send_quota(Session = #{inflight := Inflight0}) ->
    {_NInflight, Inflight} = emqx_persistent_session_ds_inflight:inc_send_quota(Inflight0),
    pull_now(Session#{inflight => Inflight}).

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

bump_interval() ->
    emqx_config:get([session_persistence, last_alive_update_interval]).

%%--------------------------------------------------------------------
%% SeqNo tracking
%% --------------------------------------------------------------------

-spec commit_seqno(puback | pubrec | pubcomp, emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()} | {error, _}.
commit_seqno(Track, PacketId, Session = #{id := SessionId, s := S}) ->
    SeqNo = packet_id_to_seqno(PacketId, S),
    case Track of
        puback ->
            Old = ?committed(?QOS_1),
            Next = ?next(?QOS_1);
        pubrec ->
            Old = ?pubrec,
            Next = ?next(?QOS_2);
        pubcomp ->
            Old = ?committed(?QOS_2),
            Next = ?next(?QOS_2)
    end,
    NextSeqNo = emqx_persistent_session_ds_state:get_seqno(Next, S),
    PrevSeqNo = emqx_persistent_session_ds_state:get_seqno(Old, S),
    case PrevSeqNo =< SeqNo andalso SeqNo =< NextSeqNo of
        true ->
            %% TODO: we pass a bogus message into the hook:
            Msg = emqx_message:make(SessionId, <<>>, <<>>),
            {ok, Msg, Session#{s => emqx_persistent_session_ds_state:put_seqno(Old, SeqNo, S)}};
        false ->
            ?SLOG(warning, #{
                msg => "out-of-order_commit",
                track => Track,
                packet_id => PacketId,
                commit_seqno => SeqNo,
                prev => PrevSeqNo,
                next => NextSeqNo
            }),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Functions for dealing with the sequence number and packet ID
%% generation
%% --------------------------------------------------------------------

%% Epoch size = `16#10000 div 2' since we generate different sets of
%% packet IDs for QoS1 and QoS2:
-define(EPOCH_SIZE, 16#8000).

%% Reconstruct session counter by adding most significant bits from
%% the current counter to the packet id:
-spec packet_id_to_seqno(emqx_types:packet_id(), emqx_persistent_session_ds_state:t()) ->
    seqno().
packet_id_to_seqno(PacketId, S) ->
    NextSeqNo = emqx_persistent_session_ds_state:get_seqno(?next(packet_id_to_qos(PacketId)), S),
    Epoch = NextSeqNo bsr 15,
    SeqNo = (Epoch bsl 15) + (PacketId bsr 1),
    case SeqNo =< NextSeqNo of
        true ->
            SeqNo;
        false ->
            SeqNo - ?EPOCH_SIZE
    end.

-spec inc_seqno(?QOS_1 | ?QOS_2, seqno()) -> emqx_types:packet_id().
inc_seqno(Qos, SeqNo) ->
    NextSeqno = SeqNo + 1,
    case seqno_to_packet_id(Qos, NextSeqno) of
        0 ->
            %% We skip sequence numbers that lead to PacketId = 0 to
            %% simplify math. Note: it leads to occasional gaps in the
            %% sequence numbers.
            NextSeqno + 1;
        _ ->
            NextSeqno
    end.

%% Note: we use the least significant bit to store the QoS. Even
%% packet IDs are QoS1, odd packet IDs are QoS2.
seqno_to_packet_id(?QOS_1, SeqNo) ->
    (SeqNo bsl 1) band 16#ffff;
seqno_to_packet_id(?QOS_2, SeqNo) ->
    ((SeqNo bsl 1) band 16#ffff) bor 1.

packet_id_to_qos(PacketId) ->
    case PacketId band 1 of
        0 -> ?QOS_1;
        1 -> ?QOS_2
    end.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-ifdef(TEST).

%% Warning: the below functions may return out-of-date results because
%% the sessions commit data to mria asynchronously.

list_all_sessions() ->
    maps:from_list(
        [
            {Id, emqx_persistent_session_ds_state:print_session(Id)}
         || Id <- emqx_persistent_session_ds_state:list_sessions()
        ]
    ).

%%%% Proper generators:

%% Generate a sequence number that smaller than the given `NextSeqNo'
%% number by at most `?EPOCH_SIZE':
seqno_gen(NextSeqNo) ->
    WindowSize = ?EPOCH_SIZE - 1,
    Min = max(0, NextSeqNo - WindowSize),
    Max = max(0, NextSeqNo - 1),
    range(Min, Max).

%% Generate a sequence number:
next_seqno_gen() ->
    ?LET(
        {Epoch, Offset},
        {non_neg_integer(), non_neg_integer()},
        Epoch bsl 15 + Offset
    ).

%%%% Property-based tests:

%% erlfmt-ignore
packet_id_to_seqno_prop() ->
    ?FORALL(
        {Qos, NextSeqNo}, {oneof([?QOS_1, ?QOS_2]), next_seqno_gen()},
        ?FORALL(
            ExpectedSeqNo, seqno_gen(NextSeqNo),
            begin
                PacketId = seqno_to_packet_id(Qos, ExpectedSeqNo),
                SeqNo = packet_id_to_seqno(PacketId, NextSeqNo),
                ?WHENFAIL(
                    begin
                        io:format(user, " *** PacketID = ~p~n", [PacketId]),
                        io:format(user, " *** SeqNo = ~p -> ~p~n", [ExpectedSeqNo, SeqNo]),
                        io:format(user, " *** NextSeqNo = ~p~n", [NextSeqNo])
                    end,
                    PacketId < 16#10000 andalso SeqNo =:= ExpectedSeqNo
                )
            end)).

inc_seqno_prop() ->
    ?FORALL(
        {Qos, SeqNo},
        {oneof([?QOS_1, ?QOS_2]), next_seqno_gen()},
        begin
            NewSeqNo = inc_seqno(Qos, SeqNo),
            PacketId = seqno_to_packet_id(Qos, NewSeqNo),
            ?WHENFAIL(
                begin
                    io:format(user, " *** SeqNo = ~p -> ~p~n", [SeqNo, NewSeqNo]),
                    io:format(user, " *** PacketId = ~p~n", [PacketId])
                end,
                PacketId > 0 andalso PacketId < 16#10000
            )
        end
    ).

seqno_proper_test_() ->
    Props = [packet_id_to_seqno_prop(), inc_seqno_prop()],
    Opts = [{numtests, 10000}, {to_file, user}],
    {timeout, 30,
        {setup,
            fun() ->
                meck:new(emqx_persistent_session_ds_state, [no_history]),
                ok = meck:expect(emqx_persistent_session_ds_state, get_seqno, fun(_Track, Seqno) ->
                    Seqno
                end)
            end,
            fun(_) ->
                meck:unload(emqx_persistent_session_ds_state)
            end,
            [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}}.

-endif.
