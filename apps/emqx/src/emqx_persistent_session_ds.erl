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
-include_lib("snabbkaffe/include/trace.hrl").
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

%% Managment APIs:
-export([
    list_client_subscriptions/1
]).

%% session table operations
-export([create_tables/0, sync/1]).

%% internal export used by session GC process
-export([destroy_session/1]).

%% Remove me later (satisfy checks for an unused BPAPI)
-export([
    do_open_iterator/3,
    do_ensure_iterator_closed/1,
    do_ensure_all_iterators_closed/1
]).

-export([print_session/1, seqno_diff/4]).

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
    subscription_id/0,
    subscription/0,
    session/0,
    stream_state/0
]).

-type seqno() :: non_neg_integer().

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type topic_filter() :: emqx_types:topic().

-type subscription_id() :: integer().

-type subscription() :: #{
    id := subscription_id(),
    start_time := emqx_ds:time(),
    props := map(),
    extra := map()
}.

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

-record(req_sync, {
    from :: pid(),
    ref :: reference()
}).

-type stream_state() :: #srs{}.

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
    mqueue_len,
    mqueue_dropped
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
    session_drop(ClientID, destroy).

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
    emqx_persistent_session_ds_inflight:n_buffered(all, Inflight);
% info(mqueue_max, #sessmem{mqueue = MQueue}) ->
%     emqx_mqueue:max_len(MQueue);
info(mqueue_dropped, _Session) ->
    0;
%% info(next_pkt_id, #{s := S}) ->
%%     {PacketId, _} = emqx_persistent_message_ds_replayer:next_packet_id(S),
%%     PacketId;
% info(awaiting_rel, #sessmem{awaiting_rel = AwaitingRel}) ->
%     AwaitingRel;
%% info(awaiting_rel_cnt, #{s := S}) ->
%%     seqno_diff(?QOS_2, ?rec, ?committed(?QOS_2), S);
info(awaiting_rel_max, #{props := Conf}) ->
    maps:get(max_awaiting_rel, Conf);
info(await_rel_timeout, #{props := Conf}) ->
    maps:get(await_rel_timeout, Conf).

-spec stats(session()) -> emqx_types:stats().
stats(Session) ->
    info(?STATS_KEYS, Session).

%% Used by management API
-spec print_session(emqx_types:clientid()) -> map() | undefined.
print_session(ClientId) ->
    case try_get_live_session(ClientId) of
        {Pid, SessionState} ->
            maps:update_with(
                s, fun emqx_persistent_session_ds_state:format/1, SessionState#{
                    '_alive' => {true, Pid}
                }
            );
        not_found ->
            case emqx_persistent_session_ds_state:print_session(ClientId) of
                undefined ->
                    undefined;
                S ->
                    #{s => S, '_alive' => false}
            end;
        not_persistent ->
            undefined
    end.

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
            %% TODO: max subscriptions

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
            {SubId, S1} = emqx_persistent_session_ds_state:new_id(S0),
            Subscription = #{
                start_time => now_ms(),
                props => SubOpts,
                id => SubId
            },
            IsNew = true;
        Subscription0 = #{} ->
            Subscription = Subscription0#{props => SubOpts},
            IsNew = false,
            S1 = S0
    end,
    S = emqx_persistent_session_ds_state:put_subscription(TopicFilter, [], Subscription, S1),
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
    case subs_lookup(TopicFilter, S0) of
        undefined ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED};
        Subscription = #{props := SubOpts} ->
            S = do_unsubscribe(ID, TopicFilter, Subscription, S0),
            {ok, Session#{s => S}, SubOpts}
    end.

-spec do_unsubscribe(id(), topic_filter(), subscription(), emqx_persistent_session_ds_state:t()) ->
    emqx_persistent_session_ds_state:t().
do_unsubscribe(SessionId, TopicFilter, #{id := SubId}, S0) ->
    S1 = emqx_persistent_session_ds_state:del_subscription(TopicFilter, [], S0),
    ?tp(persistent_session_ds_subscription_delete, #{
        session_id => SessionId, topic_filter => TopicFilter
    }),
    S = emqx_persistent_session_ds_stream_scheduler:del_subscription(SubId, S1),
    ?tp_span(
        persistent_session_ds_subscription_route_delete,
        #{session_id => SessionId, topic_filter => TopicFilter},
        ok = emqx_persistent_session_ds_router:do_delete_route(TopicFilter, SessionId)
    ),
    S.

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
    case update_seqno(puback, PacketId, Session0) of
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
    case update_seqno(pubrec, PacketId, Session0) of
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
    case update_seqno(pubcomp, PacketId, Session0) of
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
    {Publishes, Session1} = drain_buffer(fetch_new_messages(Session0, ClientInfo)),
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
    S = emqx_persistent_session_ds_stream_scheduler:renew_streams(S0),
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
handle_timeout(_ClientInfo, #req_sync{from = From, ref = Ref}, Session = #{s := S0}) ->
    S = emqx_persistent_session_ds_state:commit(S0),
    From ! Ref,
    {ok, [], Session#{s => S}};
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
replay(ClientInfo, [], Session0 = #{s := S0}) ->
    Streams = emqx_persistent_session_ds_stream_scheduler:find_replay_streams(S0),
    Session = lists:foldl(
        fun({_StreamKey, Stream}, SessionAcc) ->
            replay_batch(Stream, SessionAcc, ClientInfo)
        end,
        Session0,
        Streams
    ),
    %% Note: we filled the buffer with the historical messages, and
    %% from now on we'll rely on the normal inflight/flow control
    %% mechanisms to replay them:
    {ok, [], pull_now(Session)}.

-spec replay_batch(stream_state(), session(), clientinfo()) -> session().
replay_batch(Srs0, Session, ClientInfo) ->
    #srs{batch_size = BatchSize} = Srs0,
    %% TODO: retry on errors:
    {Srs, Inflight} = enqueue_batch(true, BatchSize, Srs0, Session, ClientInfo),
    %% Assert:
    Srs =:= Srs0 orelse
        ?tp(warning, emqx_persistent_session_ds_replay_inconsistency, #{
            expected => Srs0,
            got => Srs
        }),
    Session#{inflight => Inflight}.

%%--------------------------------------------------------------------

-spec disconnect(session(), emqx_types:conninfo()) -> {shutdown, session()}.
disconnect(Session = #{s := S0}, ConnInfo) ->
    S1 = emqx_persistent_session_ds_state:set_last_alive_at(now_ms(), S0),
    S2 =
        case ConnInfo of
            #{expiry_interval := EI} when is_number(EI) ->
                emqx_persistent_session_ds_state:set_expiry_interval(EI, S1);
            _ ->
                S1
        end,
    S = emqx_persistent_session_ds_state:commit(S2),
    {shutdown, Session#{s => S}}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(_Reason, _Session = #{id := Id, s := S}) ->
    _ = emqx_persistent_session_ds_state:commit(S),
    ?tp(debug, persistent_session_ds_terminate, #{id => Id}),
    ok.

%%--------------------------------------------------------------------
%% Management APIs (dashboard)
%%--------------------------------------------------------------------

-spec list_client_subscriptions(emqx_types:clientid()) ->
    {node() | undefined, [{emqx_types:topic() | emqx_types:share(), emqx_types:subopts()}]}
    | {error, not_found}.
list_client_subscriptions(ClientId) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            %% TODO: this is not the most optimal implementation, since it
            %% should be possible to avoid reading extra data (streams, etc.)
            case print_session(ClientId) of
                Sess = #{s := #{subscriptions := Subs}} ->
                    Node =
                        case Sess of
                            #{'_alive' := {true, Pid}} ->
                                node(Pid);
                            _ ->
                                undefined
                        end,
                    SubList =
                        maps:fold(
                            fun(Topic, #{props := SubProps}, Acc) ->
                                Elem = {Topic, SubProps},
                                [Elem | Acc]
                            end,
                            [],
                            Subs
                        ),
                    {Node, SubList};
                undefined ->
                    {error, not_found}
            end;
        false ->
            {error, not_found}
    end.

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

create_tables() ->
    emqx_persistent_session_ds_state:create_tables().

%% @doc Force syncing of the transient state to persistent storage
sync(ClientId) ->
    case emqx_cm:lookup_channels(ClientId) of
        [Pid] ->
            Ref = monitor(process, Pid),
            Pid ! {emqx_session, #req_sync{from = self(), ref = Ref}},
            receive
                {'DOWN', Ref, process, _Pid, Reason} ->
                    {error, Reason};
                Ref ->
                    demonitor(Ref, [flush]),
                    ok
            end;
        [] ->
            {error, noproc}
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
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            EI = emqx_persistent_session_ds_state:get_expiry_interval(S0),
            LastAliveAt = emqx_persistent_session_ds_state:get_last_alive_at(S0),
            case NowMS >= LastAliveAt + EI of
                true ->
                    session_drop(SessionId, expired),
                    false;
                false ->
                    ?tp(open_session, #{ei => EI, now => NowMS, laa => LastAliveAt}),
                    %% New connection being established
                    S1 = emqx_persistent_session_ds_state:set_expiry_interval(EI, S0),
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
    ?tp(debug, persistent_session_ds_ensure_new, #{id => Id}),
    Now = now_ms(),
    S0 = emqx_persistent_session_ds_state:create_new(Id),
    S1 = emqx_persistent_session_ds_state:set_expiry_interval(expiry_interval(ConnInfo), S0),
    S2 = bump_last_alive(S1),
    S3 = emqx_persistent_session_ds_state:set_created_at(Now, S2),
    S4 = lists:foldl(
        fun(Track, Acc) ->
            emqx_persistent_session_ds_state:put_seqno(Track, 0, Acc)
        end,
        S3,
        [
            ?next(?QOS_1),
            ?dup(?QOS_1),
            ?committed(?QOS_1),
            ?next(?QOS_2),
            ?dup(?QOS_2),
            ?rec,
            ?committed(?QOS_2)
        ]
    ),
    S = emqx_persistent_session_ds_state:commit(S4),
    #{
        id => Id,
        props => Conf,
        s => S,
        inflight => emqx_persistent_session_ds_inflight:new(receive_maximum(ConnInfo))
    }.

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id(), _Reason) -> ok.
session_drop(ID, Reason) ->
    case emqx_persistent_session_ds_state:open(ID) of
        {ok, S0} ->
            ?tp(debug, drop_persistent_session, #{client_id => ID, reason => Reason}),
            _S = subs_fold(
                fun(TopicFilter, Subscription, S) ->
                    do_unsubscribe(ID, TopicFilter, Subscription, S)
                end,
                S0,
                S0
            ),
            emqx_persistent_session_ds_state:delete(ID);
        undefined ->
            ok
    end.

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
%% Normal replay:
%%--------------------------------------------------------------------

fetch_new_messages(Session = #{s := S}, ClientInfo) ->
    Streams = emqx_persistent_session_ds_stream_scheduler:find_new_streams(S),
    fetch_new_messages(Streams, Session, ClientInfo).

fetch_new_messages([], Session, _ClientInfo) ->
    Session;
fetch_new_messages([I | Streams], Session0 = #{inflight := Inflight}, ClientInfo) ->
    BatchSize = emqx_config:get([session_persistence, max_batch_size]),
    case emqx_persistent_session_ds_inflight:n_buffered(all, Inflight) >= BatchSize of
        true ->
            %% Buffer is full:
            Session0;
        false ->
            Session = new_batch(I, BatchSize, Session0, ClientInfo),
            fetch_new_messages(Streams, Session, ClientInfo)
    end.

new_batch({StreamKey, Srs0}, BatchSize, Session = #{s := S0}, ClientInfo) ->
    SN1 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_1), S0),
    SN2 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_2), S0),
    Srs1 = Srs0#srs{
        first_seqno_qos1 = SN1,
        first_seqno_qos2 = SN2,
        batch_size = 0,
        last_seqno_qos1 = SN1,
        last_seqno_qos2 = SN2
    },
    {Srs, Inflight} = enqueue_batch(false, BatchSize, Srs1, Session, ClientInfo),
    S1 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_1), Srs#srs.last_seqno_qos1, S0),
    S2 = emqx_persistent_session_ds_state:put_seqno(?next(?QOS_2), Srs#srs.last_seqno_qos2, S1),
    S = emqx_persistent_session_ds_state:put_stream(StreamKey, Srs, S2),
    Session#{s => S, inflight => Inflight}.

enqueue_batch(IsReplay, BatchSize, Srs0, Session = #{inflight := Inflight0}, ClientInfo) ->
    #srs{
        it_begin = ItBegin0,
        it_end = ItEnd0,
        first_seqno_qos1 = FirstSeqnoQos1,
        first_seqno_qos2 = FirstSeqnoQos2
    } = Srs0,
    ItBegin =
        case IsReplay of
            true -> ItBegin0;
            false -> ItEnd0
        end,
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, BatchSize) of
        {ok, ItEnd, Messages} ->
            {Inflight, LastSeqnoQos1, LastSeqnoQos2} = process_batch(
                IsReplay, Session, ClientInfo, FirstSeqnoQos1, FirstSeqnoQos2, Messages, Inflight0
            ),
            Srs = Srs0#srs{
                it_begin = ItBegin,
                it_end = ItEnd,
                %% TODO: it should be possible to avoid calling
                %% length here by diffing size of inflight before
                %% and after inserting messages:
                batch_size = length(Messages),
                last_seqno_qos1 = LastSeqnoQos1,
                last_seqno_qos2 = LastSeqnoQos2
            },
            {Srs, Inflight};
        {ok, end_of_stream} ->
            %% No new messages; just update the end iterator:
            {Srs0#srs{it_begin = ItBegin, it_end = end_of_stream, batch_size = 0}, Inflight0};
        {error, _} when not IsReplay ->
            ?SLOG(info, #{msg => "failed_to_fetch_batch", iterator => ItBegin}),
            {Srs0, Inflight0}
    end.

%% key_of_iter(#{3 := #{3 := #{5 := K}}}) ->
%%     K.

process_batch(_IsReplay, _Session, _ClientInfo, LastSeqNoQos1, LastSeqNoQos2, [], Inflight) ->
    {Inflight, LastSeqNoQos1, LastSeqNoQos2};
process_batch(
    IsReplay, Session, ClientInfo, FirstSeqNoQos1, FirstSeqNoQos2, [KV | Messages], Inflight0
) ->
    #{s := S, props := #{upgrade_qos := UpgradeQoS}} = Session,
    {_DsMsgKey, Msg0 = #message{topic = Topic}} = KV,
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    Dup1 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_1), S),
    Dup2 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_2), S),
    Rec = emqx_persistent_session_ds_state:get_seqno(?rec, S),
    Subs = emqx_persistent_session_ds_state:get_subscriptions(S),
    Msgs = [
        Msg
     || SubMatch <- emqx_topic_gbt:matches(Topic, Subs, []),
        Msg <- begin
            #{props := SubOpts} = emqx_topic_gbt:get_record(SubMatch, Subs),
            emqx_session:enrich_message(ClientInfo, Msg0, SubOpts, UpgradeQoS)
        end
    ],
    {Inflight, LastSeqNoQos1, LastSeqNoQos2} = lists:foldl(
        fun(Msg = #message{qos = Qos}, {Acc, SeqNoQos10, SeqNoQos20}) ->
            case Qos of
                ?QOS_0 ->
                    SeqNoQos1 = SeqNoQos10,
                    SeqNoQos2 = SeqNoQos20;
                ?QOS_1 ->
                    SeqNoQos1 = inc_seqno(?QOS_1, SeqNoQos10),
                    SeqNoQos2 = SeqNoQos20;
                ?QOS_2 ->
                    SeqNoQos1 = SeqNoQos10,
                    SeqNoQos2 = inc_seqno(?QOS_2, SeqNoQos20)
            end,
            {
                case Qos of
                    ?QOS_0 when IsReplay ->
                        %% We ignore QoS 0 messages during replay:
                        Acc;
                    ?QOS_0 ->
                        emqx_persistent_session_ds_inflight:push({undefined, Msg}, Acc);
                    ?QOS_1 when SeqNoQos1 =< Comm1 ->
                        %% QoS1 message has been acked by the client, ignore:
                        Acc;
                    ?QOS_1 when SeqNoQos1 =< Dup1 ->
                        %% QoS1 message has been sent but not
                        %% acked. Retransmit:
                        Msg1 = emqx_message:set_flag(dup, true, Msg),
                        emqx_persistent_session_ds_inflight:push({SeqNoQos1, Msg1}, Acc);
                    ?QOS_1 ->
                        emqx_persistent_session_ds_inflight:push({SeqNoQos1, Msg}, Acc);
                    ?QOS_2 when SeqNoQos2 =< Comm2 ->
                        %% QoS2 message has been PUBCOMP'ed by the client, ignore:
                        Acc;
                    ?QOS_2 when SeqNoQos2 =< Rec ->
                        %% QoS2 message has been PUBREC'ed by the client, resend PUBREL:
                        emqx_persistent_session_ds_inflight:push({pubrel, SeqNoQos2}, Acc);
                    ?QOS_2 when SeqNoQos2 =< Dup2 ->
                        %% QoS2 message has been sent, but we haven't received PUBREC.
                        %%
                        %% TODO: According to the MQTT standard 4.3.3:
                        %% DUP flag is never set for QoS2 messages? We
                        %% do so for mem sessions, though.
                        Msg1 = emqx_message:set_flag(dup, true, Msg),
                        emqx_persistent_session_ds_inflight:push({SeqNoQos2, Msg1}, Acc);
                    ?QOS_2 ->
                        emqx_persistent_session_ds_inflight:push({SeqNoQos2, Msg}, Acc)
                end,
                SeqNoQos1,
                SeqNoQos2
            }
        end,
        {Inflight0, FirstSeqNoQos1, FirstSeqNoQos2},
        Msgs
    ),
    process_batch(
        IsReplay, Session, ClientInfo, LastSeqNoQos1, LastSeqNoQos2, Messages, Inflight
    ).

%%--------------------------------------------------------------------
%% Buffer drain
%%--------------------------------------------------------------------

drain_buffer(Session = #{inflight := Inflight0, s := S0}) ->
    {Publishes, Inflight, S} = do_drain_buffer(Inflight0, S0, []),
    {Publishes, Session#{inflight => Inflight, s := S}}.

do_drain_buffer(Inflight0, S0, Acc) ->
    case emqx_persistent_session_ds_inflight:pop(Inflight0) of
        undefined ->
            {lists:reverse(Acc), Inflight0, S0};
        {{pubrel, SeqNo}, Inflight} ->
            Publish = {pubrel, seqno_to_packet_id(?QOS_2, SeqNo)},
            do_drain_buffer(Inflight, S0, [Publish | Acc]);
        {{SeqNo, Msg}, Inflight} ->
            case Msg#message.qos of
                ?QOS_0 ->
                    do_drain_buffer(Inflight, S0, [{undefined, Msg} | Acc]);
                Qos ->
                    S = emqx_persistent_session_ds_state:put_seqno(?dup(Qos), SeqNo, S0),
                    Publish = {seqno_to_packet_id(Qos, SeqNo), Msg},
                    do_drain_buffer(Inflight, S, [Publish | Acc])
            end
    end.

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
    Inflight = emqx_persistent_session_ds_inflight:inc_send_quota(Inflight0),
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

-spec try_get_live_session(emqx_types:clientid()) ->
    {pid(), session()} | not_found | not_persistent.
try_get_live_session(ClientId) ->
    case emqx_cm:lookup_channels(local, ClientId) of
        [Pid] ->
            try
                #{channel := ChanState} = emqx_connection:get_state(Pid),
                case emqx_channel:info(impl, ChanState) of
                    ?MODULE ->
                        {Pid, emqx_channel:info(session_state, ChanState)};
                    _ ->
                        not_persistent
                end
            catch
                _:_ ->
                    not_found
            end;
        _ ->
            not_found
    end.

%%--------------------------------------------------------------------
%% SeqNo tracking
%% --------------------------------------------------------------------

-spec update_seqno(puback | pubrec | pubcomp, emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()} | {error, _}.
update_seqno(Track, PacketId, Session = #{id := SessionId, s := S}) ->
    SeqNo = packet_id_to_seqno(PacketId, S),
    case Track of
        puback ->
            QoS = ?QOS_1,
            SeqNoKey = ?committed(?QOS_1);
        pubrec ->
            QoS = ?QOS_2,
            SeqNoKey = ?rec;
        pubcomp ->
            QoS = ?QOS_2,
            SeqNoKey = ?committed(?QOS_2)
    end,
    Current = emqx_persistent_session_ds_state:get_seqno(SeqNoKey, S),
    case inc_seqno(QoS, Current) of
        SeqNo ->
            %% TODO: we pass a bogus message into the hook:
            Msg = emqx_message:make(SessionId, <<>>, <<>>),
            {ok, Msg, Session#{s => emqx_persistent_session_ds_state:put_seqno(SeqNoKey, SeqNo, S)}};
        Expected ->
            ?SLOG(warning, #{
                msg => "out-of-order_commit",
                track => Track,
                packet_id => PacketId,
                seqno => SeqNo,
                expected => Expected
            }),
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Functions for dealing with the sequence number and packet ID
%% generation
%% --------------------------------------------------------------------

-define(EPOCH_BITS, 15).
-define(PACKET_ID_MASK, 2#111_1111_1111_1111).

%% Epoch size = `16#10000 div 2' since we generate different sets of
%% packet IDs for QoS1 and QoS2:
-define(EPOCH_SIZE, 16#8000).

%% Reconstruct session counter by adding most significant bits from
%% the current counter to the packet id:
-spec packet_id_to_seqno(emqx_types:packet_id(), emqx_persistent_session_ds_state:t()) ->
    seqno().
packet_id_to_seqno(PacketId, S) ->
    NextSeqNo = emqx_persistent_session_ds_state:get_seqno(?next(packet_id_to_qos(PacketId)), S),
    Epoch = NextSeqNo bsr ?EPOCH_BITS,
    SeqNo = (Epoch bsl ?EPOCH_BITS) + (PacketId band ?PACKET_ID_MASK),
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

%% Note: we use the most significant bit to store the QoS.
seqno_to_packet_id(?QOS_1, SeqNo) ->
    SeqNo band ?PACKET_ID_MASK;
seqno_to_packet_id(?QOS_2, SeqNo) ->
    SeqNo band ?PACKET_ID_MASK bor ?EPOCH_SIZE.

packet_id_to_qos(PacketId) ->
    PacketId bsr ?EPOCH_BITS + 1.

seqno_diff(Qos, A, B, S) ->
    seqno_diff(
        Qos,
        emqx_persistent_session_ds_state:get_seqno(A, S),
        emqx_persistent_session_ds_state:get_seqno(B, S)
    ).

%% Dialyzer complains about the second clause, since it's currently
%% unused, shut it up:
-dialyzer({nowarn_function, seqno_diff/3}).
seqno_diff(?QOS_1, A, B) ->
    %% For QoS1 messages we skip a seqno every time the epoch changes,
    %% we need to substract that from the diff:
    EpochA = A bsr ?EPOCH_BITS,
    EpochB = B bsr ?EPOCH_BITS,
    A - B - (EpochA - EpochB);
seqno_diff(?QOS_2, A, B) ->
    A - B.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

-ifdef(TEST).

%% Warning: the below functions may return out-of-date results because
%% the sessions commit data to mria asynchronously.

list_all_sessions() ->
    maps:from_list(
        [
            {Id, print_session(Id)}
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
        {non_neg_integer(), range(0, ?EPOCH_SIZE)},
        Epoch bsl ?EPOCH_BITS + Offset
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
                    io:format(user, " *** QoS = ~p~n", [Qos]),
                    io:format(user, " *** SeqNo = ~p -> ~p~n", [SeqNo, NewSeqNo]),
                    io:format(user, " *** PacketId = ~p~n", [PacketId])
                end,
                PacketId > 0 andalso PacketId < 16#10000
            )
        end
    ).

seqno_diff_prop() ->
    ?FORALL(
        {Qos, SeqNo, N},
        {oneof([?QOS_1, ?QOS_2]), next_seqno_gen(), range(0, 100)},
        ?IMPLIES(
            seqno_to_packet_id(Qos, SeqNo) > 0,
            begin
                NewSeqNo = apply_n_times(N, fun(A) -> inc_seqno(Qos, A) end, SeqNo),
                Diff = seqno_diff(Qos, NewSeqNo, SeqNo),
                ?WHENFAIL(
                    begin
                        io:format(user, " *** QoS = ~p~n", [Qos]),
                        io:format(user, " *** SeqNo = ~p -> ~p~n", [SeqNo, NewSeqNo]),
                        io:format(user, " *** N : ~p == ~p~n", [N, Diff])
                    end,
                    N =:= Diff
                )
            end
        )
    ).

seqno_proper_test_() ->
    Props = [packet_id_to_seqno_prop(), inc_seqno_prop(), seqno_diff_prop()],
    Opts = [{numtests, 1000}, {to_file, user}],
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

apply_n_times(0, _Fun, A) ->
    A;
apply_n_times(N, Fun, A) when N > 0 ->
    apply_n_times(N - 1, Fun, Fun(A)).

-endif.
