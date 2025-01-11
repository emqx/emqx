%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc This module implements an MQTT session that can survive
%% restart of EMQX node by backing up its state on disk. It consumes
%% messages from a shared durable storage. This is in contrast to the
%% regular "mem" sessions that store all recieved messages in their
%% own memory queues.
%%
%% The main challenge of durable session is to replay sent, but
%% unacked, messages in case of the client reconnect. This
%% implementation approaches this problem by storing iterators, batch
%% sizes and sequence numbers of MQTT packets for the consumed
%% messages as an array of "stream replay state" records (`#srs'), in
%% such a way, that messages and their corresponging packet IDs can be
%% reconstructing by "replaying" the stored SRSes.

-module(emqx_persistent_session_ds).

-behaviour(emqx_session).

-include("emqx.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").
-include_lib("stdlib/include/ms_transform.hrl").

-include("emqx_mqtt.hrl").

-include("emqx_session.hrl").
-include("emqx_persistent_session_ds/session_internals.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").

-ifdef(TEST).
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("proper/include/proper.hrl").
-include_lib("eunit/include/eunit.hrl").
-endif.

%% Session API
-export([
    create/4,
    open/4,
    destroy/1,
    kick_offline_session/1
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
    handle_info/3,
    disconnect/2,
    terminate/2
]).

%% Will message handling
-export([
    clear_will_message/1,
    publish_will_message_now/2
]).
%% Managment APIs:
-export([
    list_client_subscriptions/1,
    get_client_subscription/2
]).

%% session table operations
-export([sync/1]).
-ifndef(STORE_STATE_IN_DS).
-export([create_tables/0]).
%% END ifndef(STORE_STATE_IN_DS).
-endif.

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
    open_session_state/4,
    list_all_sessions/0,
    state_invariants/2,
    trace_specs/0
]).
-endif.

-export_type([
    id/0,
    seqno/0,
    timestamp/0,
    topic_filter/0,
    share_topic_filter/0,
    subscription_id/0,
    subscription/0,
    session/0,
    stream_state/0
]).

-type seqno() :: non_neg_integer().

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type share_topic_filter() :: #share{}.
-type topic_filter() :: emqx_types:topic() | share_topic_filter().

%% Subscription and subscription states:
%%
%% Persistent sessions cannot simply update or delete subscriptions,
%% since subscription parameters must be exactly the same during
%% replay.
%%
%% To solve this problem, we store subscriptions in a twofold manner:
%%
%% - `subscription' is an object that holds up-to-date information
%% about the client's subscription and a reference to the latest
%% subscription state id
%%
%% - `subscription_state' is an immutable object that holds
%% information about the subcription parameters at a certain point of
%% time
%%
%% New subscription states are created whenever the client subscribes
%% to a topics, or updates an existing subscription.
%%
%% Stream replay states contain references to the subscription states.
%%
%% Outdated subscription states are discarded when they are not
%% referenced by either subscription or stream replay state objects.

-type subscription_id() :: integer().

%% This type is a result of merging
%% `emqx_persistent_session_ds_subs:subscription()' with its current
%% state.
-type subscription() :: #{
    id := subscription_id(),
    start_time := emqx_ds:time(),
    current_state := emqx_persistent_session_ds_subs:subscription_state_id(),
    subopts := map()
}.

-type shared_sub_state() :: term().

-define(TIMER_DRAIN_INFLIGHT, timer_drain_inflight).
-define(TIMER_COMMIT, timer_commit).
-define(TIMER_RETRY_REPLAY, timer_retry_replay).

-type timer() ::
    ?TIMER_DRAIN_INFLIGHT
    | ?TIMER_COMMIT
    | ?TIMER_RETRY_REPLAY.

-type timer_state() :: reference() | undefined.

-record(ds_suback, {stream_key, seqno}).

%% TODO: Needs configuration?
-define(TIMEOUT_RETRY_REPLAY, 1000).

-type session() :: #{
    %% Client ID
    id := id(),
    %% Configuration:
    props := map(),
    %% Persistent state:
    s := emqx_persistent_session_ds_state:t(),
    %% Shared subscription state:
    shared_sub_s := shared_sub_state(),
    %% Buffer accumulates poll replies received from the DS, that
    %% could not be immediately added to the inflight due to stream
    %% blocking. Buffered messages have not been seen by the client.
    %% Therefore, they (and their iterators) are entirely ephemeral
    %% and are lost when the client disconnets. Session restarts with
    %% an empty buffer.
    buffer := emqx_persistent_session_ds_buffer:t(),
    %% Inflight represents a sequence of outgoing messages with
    %% assigned packet IDs. Some of these messages MAY have been seen
    %% by the client. Therefore, inflight is a persistent structure.
    %% During startup session recreates the state of `inflight'.
    %%
    %% Once messages from a stream are added to the inflight, this
    %% stream becomes "blocked", and no further messages from that
    %% stream can be added to the inflight until the client acks the
    %% previous messages.
    inflight := emqx_persistent_session_ds_inflight:t(),
    stream_scheduler_s := emqx_persistent_session_ds_stream_scheduler:t(),
    %% In-progress replay:
    %% List of stream replay states to be added to the inflight buffer.
    replay := [{_StreamKey, stream_state()}, ...] | undefined,
    %% Timers:
    ?TIMER_DRAIN_INFLIGHT := timer_state(),
    ?TIMER_COMMIT := timer_state(),
    ?TIMER_RETRY_REPLAY := timer_state()
}.

-define(IS_REPLAY_ONGOING(REPLAY), is_list(REPLAY)).

-record(req_sync, {
    from :: pid(),
    ref :: reference()
}).

-type stream_state() :: emqx_persistent_session_ds_stream_scheduler:srs().

-type message() :: emqx_types:message().
-type timestamp() :: emqx_utils_calendar:epoch_millisecond().
-type millisecond() :: non_neg_integer().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().

-define(STATS_KEYS, [
    durable,
    subscriptions_cnt,
    subscriptions_max,
    inflight_cnt,
    inflight_max,
    mqueue_len,
    mqueue_dropped,
    seqno_q1_comm,
    seqno_q1_dup,
    seqno_q1_next,
    seqno_q2_comm,
    seqno_q2_dup,
    seqno_q2_rec,
    seqno_q2_next,
    n_streams,
    awaiting_rel_cnt,
    awaiting_rel_max
]).

%%

-spec create(clientinfo(), conninfo(), emqx_maybe:t(message()), emqx_session:conf()) ->
    session().
create(#{clientid := ClientID} = ClientInfo, ConnInfo, MaybeWillMsg, Conf) ->
    State = ensure_new_session_state(ClientID, ClientInfo, ConnInfo, MaybeWillMsg),
    create_session(true, ClientID, State, ConnInfo, Conf).

-spec open(clientinfo(), conninfo(), emqx_maybe:t(message()), emqx_session:conf()) ->
    {_IsPresent :: true, session(), []} | false.
open(#{clientid := ClientID} = ClientInfo, ConnInfo, MaybeWillMsg, Conf) ->
    %% NOTE
    %% The fact that we need to concern about discarding all live channels here
    %% is essentially a consequence of the in-memory session design, where we
    %% have disconnected channels holding onto session state. Ideally, we should
    %% somehow isolate those idling not-yet-expired sessions into a separate process
    %% space, and move this call back into `emqx_cm` where it belongs.
    ok = emqx_cm:takeover_kick(ClientID),
    case open_session_state(ClientID, ClientInfo, ConnInfo, MaybeWillMsg) of
        false ->
            false;
        State ->
            Session = create_session(false, ClientID, State, ConnInfo, Conf),
            {true, do_expire(ClientInfo, Session), []}
    end.

-spec destroy(session() | clientinfo()) -> ok.
destroy(#{id := ClientID}) ->
    destroy_session(ClientID);
destroy(#{clientid := ClientID}) ->
    destroy_session(ClientID).

destroy_session(ClientID) ->
    session_drop(ClientID, destroy).

-spec kick_offline_session(emqx_types:clientid()) -> ok.
kick_offline_session(ClientID) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            session_drop(ClientID, kicked);
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, #{id := ClientID}) ->
    ClientID;
info(clientid, #{id := ClientID}) ->
    ClientID;
info(durable, _) ->
    true;
info(created_at, #{s := S}) ->
    emqx_persistent_session_ds_state:get_created_at(S);
info(is_persistent, #{}) ->
    true;
info(subscriptions, #{s := S, shared_sub_s := SharedSubS}) ->
    maps:merge(
        emqx_persistent_session_ds_subs:to_map(S),
        emqx_persistent_session_ds_shared_subs:to_map(S, SharedSubS)
    );
info(subscriptions_cnt, #{s := S}) ->
    emqx_persistent_session_ds_state:n_subscriptions(S);
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
info(mqueue_len, #{inflight := Inflight}) ->
    emqx_persistent_session_ds_inflight:n_buffered(all, Inflight);
info(mqueue_dropped, _Session) ->
    0;
info(awaiting_rel, #{s := S}) ->
    emqx_persistent_session_ds_state:fold_awaiting_rel(fun maps:put/3, #{}, S);
info(awaiting_rel_max, #{props := Conf}) ->
    maps:get(max_awaiting_rel, Conf);
info(awaiting_rel_cnt, #{s := S}) ->
    emqx_persistent_session_ds_state:n_awaiting_rel(S);
info(await_rel_timeout, #{props := Conf}) ->
    maps:get(await_rel_timeout, Conf);
info(seqno_q1_comm, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S);
info(seqno_q1_dup, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_1), S);
info(seqno_q1_next, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?next(?QOS_1), S);
info(seqno_q2_comm, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S);
info(seqno_q2_dup, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_2), S);
info(seqno_q2_rec, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?rec, S);
info(seqno_q2_next, #{s := S}) ->
    emqx_persistent_session_ds_state:get_seqno(?next(?QOS_2), S);
info(n_streams, #{s := S}) ->
    emqx_persistent_session_ds_state:n_streams(S);
info({MsgsQ, _PagerParams}, _Session) when MsgsQ =:= mqueue_msgs; MsgsQ =:= inflight_msgs ->
    {error, not_implemented}.

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

%% Suppress warnings about clauses handling unimplemented results
%% of `emqx_persistent_session_ds_shared_subs:on_subscribe/3`
-dialyzer({nowarn_function, subscribe/3}).
-spec subscribe(topic_filter(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    #share{} = TopicFilter,
    SubOpts,
    Session0
) ->
    case emqx_persistent_session_ds_shared_subs:on_subscribe(TopicFilter, SubOpts, Session0) of
        {ok, S, SharedSubS} ->
            Session = Session0#{s := S, shared_sub_s := SharedSubS},
            {ok, commit(Session)};
        Error = {error, _} ->
            Error
    end;
subscribe(
    TopicFilter,
    SubOpts,
    Session0 = #{stream_scheduler_s := SchedS0}
) ->
    case emqx_persistent_session_ds_subs:on_subscribe(TopicFilter, SubOpts, Session0) of
        {ok, S1, Subscription} ->
            {_NewSLSIds, S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_subscribe(
                TopicFilter, Subscription, S1, SchedS0
            ),
            Session = Session0#{s := S, stream_scheduler_s := SchedS},
            {ok, commit(Session)};
        Error = {error, _} ->
            Error
    end.

%% Suppress warnings about clauses handling unimplemented results
%% of `emqx_persistent_session_ds_shared_subs:on_unsubscribe/4`
-dialyzer({nowarn_function, unsubscribe/2}).
-spec unsubscribe(topic_filter(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    #share{} = TopicFilter,
    Session0 = #{
        id := SessionId,
        s := S0,
        shared_sub_s := SharedSubS0,
        stream_scheduler_s := SchedS0,
        buffer := Buf0
    }
) ->
    case
        emqx_persistent_session_ds_shared_subs:on_unsubscribe(
            SessionId, TopicFilter, S0, SchedS0, SharedSubS0
        )
    of
        {ok, S, SchedS, SharedSubS, SubOpts = #{id := SubId}} ->
            Buf = emqx_persistent_session_ds_buffer:clean_by_subid(SubId, Buf0),
            Session = Session0#{
                s := S, shared_sub_s := SharedSubS, stream_scheduler_s := SchedS, buffer := Buf
            },
            {ok, commit(Session), SubOpts};
        Error = {error, _} ->
            Error
    end;
unsubscribe(
    TopicFilter,
    Session0 = #{id := SessionId, s := S0, stream_scheduler_s := SchedS0}
) ->
    case emqx_persistent_session_ds_subs:on_unsubscribe(SessionId, TopicFilter, S0) of
        {ok, S1, #{id := SubId, subopts := SubOpts}} ->
            {S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
                TopicFilter, SubId, S1, SchedS0
            ),
            Session = Session0#{s := S, stream_scheduler_s := SchedS},
            {ok, commit(clear_buffer(SubId, Session)), SubOpts};
        Error = {error, _} ->
            Error
    end.

-spec get_subscription(topic_filter(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(#share{}, _) ->
    %% TODO: shared subscriptions are not supported yet:
    undefined;
get_subscription(TopicFilter, #{s := S}) ->
    case emqx_persistent_session_ds_subs:lookup(TopicFilter, S) of
        #{subopts := SubOpts} ->
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
publish(
    PacketId,
    Msg = #message{qos = ?QOS_2, timestamp = Ts},
    Session = #{s := S0}
) ->
    case is_awaiting_full(Session) of
        false ->
            case emqx_persistent_session_ds_state:get_awaiting_rel(PacketId, S0) of
                undefined ->
                    Results = emqx_broker:publish(Msg),
                    S = emqx_persistent_session_ds_state:put_awaiting_rel(PacketId, Ts, S0),
                    {ok, Results, ensure_state_commit_timer(Session#{s := S})};
                _Ts ->
                    {error, ?RC_PACKET_IDENTIFIER_IN_USE}
            end;
        true ->
            {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED}
    end;
publish(_PacketId, Msg, Session) ->
    Result = emqx_broker:publish(Msg),
    {ok, Result, Session}.

is_awaiting_full(#{s := S, props := Props}) ->
    emqx_persistent_session_ds_state:n_awaiting_rel(S) >=
        maps:get(max_awaiting_rel, Props, infinity).

-spec expire(emqx_types:clientinfo(), session()) ->
    {ok, [], timeout(), session()} | {ok, [], session()}.
expire(ClientInfo, Session0 = #{props := Props}) ->
    Session = #{s := S} = ensure_state_commit_timer(do_expire(ClientInfo, Session0)),
    case emqx_persistent_session_ds_state:n_awaiting_rel(S) of
        0 ->
            {ok, [], Session};
        _ ->
            AwaitRelTimeout = maps:get(await_rel_timeout, Props),
            {ok, [], AwaitRelTimeout, Session}
    end.

do_expire(ClientInfo, Session = #{s := S0, props := Props}) ->
    %% 1. Find expired packet IDs:
    Now = erlang:system_time(millisecond),
    AwaitRelTimeout = maps:get(await_rel_timeout, Props),
    ExpiredPacketIds =
        emqx_persistent_session_ds_state:fold_awaiting_rel(
            fun(PacketId, Ts, Acc) ->
                Age = Now - Ts,
                case Age > AwaitRelTimeout of
                    true ->
                        [PacketId | Acc];
                    false ->
                        Acc
                end
            end,
            [],
            S0
        ),
    %% 2. Perform side effects:
    _ = emqx_session_events:handle_event(ClientInfo, {expired_rel, length(ExpiredPacketIds)}),
    %% 3. Update state:
    S = lists:foldl(
        fun emqx_persistent_session_ds_state:del_awaiting_rel/2,
        S0,
        ExpiredPacketIds
    ),
    Session#{s := S}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-spec puback(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
puback(ClientInfo, PacketId, Session0) ->
    case update_seqno(puback, PacketId, Session0, ClientInfo) of
        {ok, Msg, Session} ->
            {ok, Msg, [], ensure_delivery_timer(Session)};
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
    case update_seqno(pubrec, PacketId, Session0, undefined) of
        {ok, Msg, Session} ->
            {ok, Msg, ensure_state_commit_timer(Session)};
        Error = {error, _} ->
            Error
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec pubrel(emqx_types:packet_id(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
pubrel(PacketId, Session = #{s := S0}) ->
    case emqx_persistent_session_ds_state:get_awaiting_rel(PacketId, S0) of
        undefined ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND};
        _TS ->
            S = emqx_persistent_session_ds_state:del_awaiting_rel(PacketId, S0),
            {ok, ensure_state_commit_timer(Session#{s := S})}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec pubcomp(clientinfo(), emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
pubcomp(ClientInfo, PacketId, Session0) ->
    case update_seqno(pubcomp, PacketId, Session0, ClientInfo) of
        {ok, Msg, Session} ->
            {ok, Msg, [], ensure_delivery_timer(Session)};
        Error = {error, _} ->
            Error
    end.

%%--------------------------------------------------------------------
%% Delivers
%%--------------------------------------------------------------------

-spec deliver(clientinfo(), [emqx_types:deliver()], session()) ->
    {ok, replies(), session()}.
deliver(ClientInfo, Delivers, Session0) ->
    %% Durable sessions still have to handle some transient messages.
    %% For example, retainer sends messages to the session directly.
    Session = lists:foldl(
        fun(Msg, Acc) -> enqueue_transient(ClientInfo, Msg, Acc) end, Session0, Delivers
    ),
    {ok, [], ensure_delivery_timer(Session)}.

%%--------------------------------------------------------------------
%% Timeouts
%%--------------------------------------------------------------------

-spec handle_timeout(clientinfo(), _Timeout, session()) ->
    {ok, replies(), session()} | {ok, replies(), timeout(), session()}.
handle_timeout(_ClientInfo, ?TIMER_DRAIN_INFLIGHT, Session0) ->
    Session1 = Session0#{?TIMER_DRAIN_INFLIGHT := undefined},
    {Publishes, Session} = drain_inflight(Session1),
    {ok, Publishes, Session};
handle_timeout(ClientInfo, ?TIMER_RETRY_REPLAY, Session0) ->
    Session = replay_streams(Session0, ClientInfo),
    {ok, [], ensure_state_commit_timer(Session)};
handle_timeout(_ClientInfo, ?TIMER_COMMIT, Session) ->
    {ok, [], commit(Session)};
handle_timeout(ClientInfo, expire_awaiting_rel, Session) ->
    expire(ClientInfo, Session);
handle_timeout(_ClientInfo, Timeout, Session) ->
    ?tp(warning, ?sessds_unknown_timeout, #{timeout => Timeout}),
    {ok, [], Session}.

%%--------------------------------------------------------------------
%% Generic messages
%%--------------------------------------------------------------------

-spec handle_info(term(), session(), clientinfo()) -> session().
handle_info(
    ?shared_sub_message = Msg,
    Session0 = #{s := S0, shared_sub_s := SharedSubS0, stream_scheduler_s := SchedS0},
    _ClientInfo
) ->
    {_NeedPush, S, SchedS, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_info(
        S0, SchedS0, SharedSubS0, Msg
    ),
    Session = Session0#{
        s := S, shared_sub_s := SharedSubS, stream_scheduler_s := SchedS
    },
    ensure_delivery_timer(Session);
handle_info(AsyncReply = #poll_reply{}, Session, ClientInfo) ->
    handle_ds_reply(AsyncReply, Session, ClientInfo);
handle_info(
    #new_stream_event{subref = Ref},
    Session = #{s := S0, stream_scheduler_s := SchedS0},
    _ClientInfo
) ->
    %% Handle DS notification about new streams:
    {_NewStreams, S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_new_stream_event(
        Ref, S0, SchedS0
    ),
    ensure_delivery_timer(Session#{s := S, stream_scheduler_s := SchedS});
handle_info(#req_sync{from = From, ref = Ref}, Session0, _ClientInfo) ->
    Session = commit(Session0),
    From ! Ref,
    Session;
handle_info(Msg, Session, _ClientInfo) ->
    ?tp(warning, ?sessds_unknown_message, #{message => Msg}),
    Session.

%%--------------------------------------------------------------------
%% Shared subscription outgoing messages
%%--------------------------------------------------------------------

shared_sub_opts(SessionId) ->
    #{session_id => SessionId}.

%%--------------------------------------------------------------------
%% Replay of old messages during session restart
%%--------------------------------------------------------------------

-spec replay(clientinfo(), [], session()) ->
    {ok, replies(), session()}.
replay(ClientInfo, [], Session0 = #{s := S0}) ->
    Streams = emqx_persistent_session_ds_stream_scheduler:find_replay_streams(S0),
    Session = replay_streams(Session0#{replay := Streams}, ClientInfo),
    {ok, [], Session}.

replay_streams(Session0 = #{replay := [{StreamKey, SRS0} | Rest]}, ClientInfo) ->
    case replay_batch(StreamKey, SRS0, Session0, ClientInfo) of
        {ok, _SRS, Session} ->
            replay_streams(Session#{replay := Rest}, ClientInfo);
        {error, recoverable, Reason} ->
            RetryTimeout = ?TIMEOUT_RETRY_REPLAY,
            ?SLOG(debug, #{
                msg => "failed_to_fetch_replay_batch",
                stream => StreamKey,
                reason => Reason,
                class => recoverable,
                retry_in_ms => RetryTimeout
            }),
            set_timer(?TIMER_RETRY_REPLAY, RetryTimeout, Session0);
        {error, unrecoverable, Reason} ->
            Session1 = skip_replay_batch(StreamKey, SRS0, Session0, ClientInfo, Reason),
            replay_streams(Session1#{replay := Rest}, ClientInfo)
    end;
replay_streams(Session = #{replay := []}, _ClientInfo) ->
    %% Note: we filled the buffer with the historical messages, and
    %% from now on we'll rely on the normal inflight/flow control
    %% mechanisms to replay them:
    ensure_delivery_timer(Session#{replay := undefined}).

%% Handle `{error, unrecoverable, _}' returned by `next'.
%% Most likely they mean that the generation containing the messages
%% has been removed while the client was offline.
-spec skip_replay_batch(_StreamKey, stream_state(), session(), clientinfo(), _Reason) -> session().
skip_replay_batch(StreamKey, SRS0, Session = #{s := S0}, ClientInfo, Reason) ->
    ?tp(warning, ?sessds_replay_unrecoverable_error, #{
        reason => Reason,
        srs => SRS0
    }),
    GenEvents = fun
        F(QoS, SeqNo, LastSeqNo) when SeqNo < LastSeqNo ->
            FakeMsg = #message{
                id = <<>>,
                qos = QoS,
                payload = <<>>,
                topic = <<>>,
                timestamp = 0
            },
            _ = emqx_session_events:handle_event(ClientInfo, {expired, FakeMsg}),
            F(QoS, inc_seqno(QoS, SeqNo), LastSeqNo);
        F(_, _, _) ->
            ok
    end,
    %% Treat messages as expired:
    GenEvents(?QOS_1, SRS0#srs.first_seqno_qos1, SRS0#srs.last_seqno_qos1),
    GenEvents(?QOS_2, SRS0#srs.first_seqno_qos2, SRS0#srs.last_seqno_qos2),
    SRS = SRS0#srs{it_end = end_of_stream, batch_size = 0},
    %% That's it for the iterator. Mark SRS as reached the
    %% `end_of_stream', and let stream scheduler do the rest:
    S = emqx_persistent_session_ds_state:put_stream(StreamKey, SRS, S0),
    Session#{s := S}.

-spec replay_batch(
    emqx_persistent_session_ds_stream_scheduler:stream_key(),
    stream_state(),
    session(),
    clientinfo()
) ->
    {ok, stream_state(), session()} | emqx_ds:error(_).
replay_batch(StreamKey, SRS, Session0 = #{s := S0, inflight := Inflight0}, ClientInfo) ->
    #srs{
        it_begin = ItBegin,
        batch_size = BatchSize,
        sub_state_id = SSID,
        first_seqno_qos1 = FirstSeqnoQos1,
        first_seqno_qos2 = FirstSeqnoQos2
    } = SRS,
    %% TODO: end_of_stream
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, BatchSize) of
        {ok, ItEnd, Batch} ->
            SubState = emqx_persistent_session_ds_state:get_subscription_state(SSID, S0),
            {Inflight, LastSeqnoQos1, LastSeqnoQos2} =
                process_batch(
                    true,
                    S0,
                    SubState,
                    ClientInfo,
                    FirstSeqnoQos1,
                    FirstSeqnoQos2,
                    Batch,
                    Inflight0
                ),
            check_replay_consistency(
                StreamKey, SRS, ItEnd, Batch, LastSeqnoQos1, LastSeqnoQos2, Session0
            ),
            Session = on_enqueue(true, StreamKey, SRS, Session0#{inflight := Inflight}),
            {ok, SRS, Session};
        Error = {error, _, _} ->
            Error
    end.

check_replay_consistency(StreamKey, SRS, _ItEnd, Batch, LastSeqnoQos1, LastSeqnoQos2, Session) ->
    #srs{last_seqno_qos1 = EQ1, last_seqno_qos2 = EQ2, batch_size = EBS} = SRS,
    %% TODO: Also check the end iterator? This is currently impossible
    %% because iterator returned by `scan_stream' (a call used by
    %% beamformer) and the end iterator of `next' (a call used during
    %% replay) may point at different keys. Namely, `scan_stream'
    %% returns key at the tip of the combined batch, and `next'
    %% returns key at the tip of the normal batch. This is (believed
    %% to be) harmless.
    case length(Batch) of
        EBS when
            EQ1 =:= LastSeqnoQos1,
            EQ2 =:= LastSeqnoQos2
        ->
            ok;
        BatchSize ->
            ?tp(
                warning,
                ?sessds_replay_inconsistency,
                #{
                    stream => StreamKey,
                    srs => SRS,
                    batch_size => BatchSize,
                    q1 => LastSeqnoQos1,
                    q2 => LastSeqnoQos2,
                    sess => Session
                }
            )
    end.

on_enqueue(
    IsReplay,
    StreamKey,
    SRS,
    Session = #{s := S0, stream_scheduler_s := SchedS0, shared_sub_s := SharedSubS0}
) ->
    {ReadyStreams, S1, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_enqueue(
        IsReplay, StreamKey, SRS, S0, SchedS0
    ),
    {S, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_streams_replay(
        S1, SharedSubS0, ReadyStreams
    ),
    Session#{s := S, stream_scheduler_s := SchedS, shared_sub_s := SharedSubS}.

%%--------------------------------------------------------------------

-spec disconnect(session(), emqx_types:conninfo()) -> {shutdown, session()}.
disconnect(Session = #{id := Id, s := S0, shared_sub_s := SharedSubS0}, ConnInfo) ->
    S1 = maybe_set_offline_info(S0, Id),
    S2 = emqx_persistent_session_ds_state:set_last_alive_at(now_ms(), S1),
    S3 =
        case ConnInfo of
            #{expiry_interval := EI} when is_number(EI) ->
                emqx_persistent_session_ds_state:set_expiry_interval(EI, S2);
            _ ->
                S2
        end,
    {S, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_disconnect(S3, SharedSubS0),
    {shutdown, commit(Session#{s := S, shared_sub_s := SharedSubS})}.

-spec terminate(Reason :: term(), session()) -> ok.
terminate(Reason, Session = #{s := S0, id := Id}) ->
    _ = maybe_set_will_message_timer(Session),
    S = finalize_last_alive_at(S0),
    _ = commit(Session#{s := S}, #{terminate => true}),
    ?tp(debug, ?sessds_terminate, #{id => Id, reason => Reason}),
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
                Sess = #{s := #{subscriptions := Subs, subscription_states := SStates}} ->
                    Node =
                        case Sess of
                            #{'_alive' := {true, Pid}} ->
                                node(Pid);
                            _ ->
                                undefined
                        end,
                    SubList =
                        maps:fold(
                            fun(Topic, #{current_state := CS}, Acc) ->
                                #{subopts := SubOpts} = maps:get(CS, SStates),
                                Elem = {Topic, SubOpts#{durable => true}},
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

-spec get_client_subscription(emqx_types:clientid(), topic_filter() | share_topic_filter()) ->
    subscription() | undefined.
get_client_subscription(ClientId, #share{} = ShareTopicFilter) ->
    emqx_persistent_session_ds_shared_subs:cold_get_subscription(ClientId, ShareTopicFilter);
get_client_subscription(ClientId, TopicFilter) ->
    emqx_persistent_session_ds_subs:cold_get_subscription(ClientId, TopicFilter).

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

-ifndef(STORE_STATE_IN_DS).
create_tables() ->
    emqx_persistent_session_ds_state:create_tables().
%% END ifndef(STORE_STATE_IN_DS).
-endif.

%% @doc Force commit of the transient state to persistent storage
sync(ClientId) ->
    case emqx_cm:lookup_channels(ClientId) of
        [Pid] ->
            Ref = monitor(process, Pid),
            Pid ! #req_sync{from = self(), ref = Ref},
            receive
                {'DOWN', Ref, process, _Pid, Reason} ->
                    {error, Reason};
                Ref ->
                    demonitor(Ref, [flush]),
                    ok
            after 5000 ->
                {error, timeout}
            end;
        [] ->
            {error, noproc}
    end.

%% @doc Called when a client connects. This function looks up a
%% session or returns `false` if previous one couldn't be found.
%%
%% Note: session API doesn't handle session takeovers, it's the job of
%% the broker.
-spec open_session_state(
    id(), emqx_types:clientinfo(), emqx_types:conninfo(), emqx_maybe:t(message())
) ->
    emqx_persistent_session_ds_state:t() | false.
open_session_state(
    SessionId,
    ClientInfo,
    NewConnInfo = #{proto_name := ProtoName, proto_ver := ProtoVer},
    MaybeWillMsg
) ->
    NowMS = now_ms(),
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            EI = emqx_persistent_session_ds_state:get_expiry_interval(S0),
            LastAliveAt = get_last_alive_at(S0),
            case NowMS >= LastAliveAt + EI of
                true ->
                    session_drop(SessionId, expired),
                    false;
                false ->
                    ?tp(?sessds_open_session, #{ei => EI, now => NowMS, laa => LastAliveAt}),
                    %% New connection being established; update the
                    %% existing data:
                    S1 = emqx_persistent_session_ds_state:set_expiry_interval(EI, S0),
                    S2 = init_last_alive_at(NowMS, S1),
                    S3 = emqx_persistent_session_ds_state:set_peername(
                        maps:get(peername, NewConnInfo), S2
                    ),
                    S4 = emqx_persistent_session_ds_state:set_will_message(MaybeWillMsg, S3),
                    S = set_clientinfo(ClientInfo, S4),
                    emqx_persistent_session_ds_state:set_protocol({ProtoName, ProtoVer}, S)
            end;
        undefined ->
            false
    end.

-spec ensure_new_session_state(
    id(),
    emqx_types:clientinfo(),
    emqx_types:conninfo(),
    emqx_maybe:t(message())
) ->
    emqx_persistent_session_ds_state:t().
ensure_new_session_state(
    Id, ClientInfo, ConnInfo = #{proto_name := ProtoName, proto_ver := ProtoVer}, MaybeWillMsg
) ->
    ?tp(debug, ?sessds_ensure_new, #{id => Id}),
    Now = now_ms(),
    S0 = emqx_persistent_session_ds_state:create_new(Id),
    S1 = emqx_persistent_session_ds_state:set_expiry_interval(expiry_interval(ConnInfo), S0),
    S2 = init_last_alive_at(S1),
    S3 = emqx_persistent_session_ds_state:set_created_at(Now, S2),
    S4 = lists:foldl(
        fun(Track, SAcc) ->
            put_seqno(Track, 0, SAcc)
        end,
        S3,
        [
            %% QoS1:
            ?next(?QOS_1),
            ?dup(?QOS_1),
            ?committed(?QOS_1),
            %% QoS2:
            ?next(?QOS_2),
            ?dup(?QOS_2),
            ?rec,
            ?committed(?QOS_2)
        ]
    ),
    S5 = emqx_persistent_session_ds_state:set_will_message(MaybeWillMsg, S4),
    S = set_clientinfo(ClientInfo, S5),
    emqx_persistent_session_ds_state:set_protocol({ProtoName, ProtoVer}, S).

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id(), _Reason) -> ok.
session_drop(SessionId, Reason) ->
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            ?tp(debug, ?sessds_drop, #{client_id => SessionId, reason => Reason}),
            ok = emqx_persistent_session_ds_subs:on_session_drop(SessionId, S0),
            ok = emqx_persistent_session_ds_state:delete(SessionId);
        undefined ->
            ok
    end.

now_ms() ->
    erlang:system_time(millisecond).

set_clientinfo(ClientInfo0, S) ->
    %% Remove unnecessary fields from the clientinfo:
    ClientInfo = maps:without([cn, dn, auth_result], ClientInfo0),
    emqx_persistent_session_ds_state:set_clientinfo(ClientInfo, S).

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

handle_ds_reply(
    Reply,
    Session0 = #{
        s := S, buffer := B0, stream_scheduler_s := SchedS0, inflight := Inflight0
    },
    ClientInfo
) ->
    ?tp(debug, ?sessds_poll_reply, #{reply => Reply}),
    #poll_reply{userdata = StreamKey} = Reply,
    case emqx_persistent_session_ds_stream_scheduler:verify_reply(Reply, SchedS0) of
        {true, SchedS} ->
            SRS0 = emqx_persistent_session_ds_state:get_stream(StreamKey, S),
            case emqx_persistent_session_ds_stream_scheduler:is_fully_acked(SRS0, S) of
                true ->
                    %% This stream is not blocked. Add messages
                    %% directly to the inflight and set drain timer.
                    %%
                    %% Note: this may overwrite SRS records that
                    %% correspond to batches that only contain QoS0
                    %% messages. We ignore QoS0 messages during
                    %% replay, so it's not deemed a problem.
                    {SRS1, SubState} = pre_enqueue_new(SRS0, S),
                    {SRS, Inflight} = enqueue_batch(
                        StreamKey, S, Reply, SubState, SRS1, ClientInfo, Inflight0
                    ),
                    post_enqueue_new(
                        StreamKey,
                        SRS,
                        ensure_delivery_timer(Session0#{
                            inflight := Inflight, stream_scheduler_s := SchedS
                        })
                    );
                false ->
                    %% This stream is blocked, add batch to the buffer
                    %% instead:
                    B = emqx_persistent_session_ds_buffer:push_batch(StreamKey, Reply, B0),
                    Session0#{buffer := B, stream_scheduler_s := SchedS}
            end;
        {false, SchedS} ->
            Session0#{stream_scheduler_s := SchedS}
    end.

inc_next(Track, S) ->
    Old = emqx_persistent_session_ds_state:get_seqno(Track, S),
    case Track of
        ?next(?QOS_1) ->
            put_seqno(Track, inc_seqno(?QOS_1, Old), S);
        ?next(?QOS_2) ->
            put_seqno(Track, inc_seqno(?QOS_2, Old), S)
    end.

put_next(Track, Val, S) ->
    %% put_seqno(Track, Val, S).
    %% FIXME: find the root cause for a possible seqno order violation
    Old = emqx_persistent_session_ds_state:get_seqno(Track, S),
    case true of
        _ when Val >= Old ->
            put_seqno(Track, Val, S);
        _ ->
            inc_next(Track, S)
    end.

%% @doc Remove buffered data for all streams that belong to a given
%% subscription.
clear_buffer(SubId, Session = #{buffer := Buf}) ->
    Session#{buffer := emqx_persistent_session_ds_buffer:clean_by_subid(SubId, Buf)}.

%%--------------------------------------------------------------------
%% Generic functions for fetching messages (during replay or normal
%% operation):
%% --------------------------------------------------------------------

%% Enrich messages according to the subscription options and assign
%% sequence number to each message, later to be used for packet ID
%% generation:
%%
%% TODO: ClientInfo is only used by enrich_message to get
%% client ID. While this should not lead to replay
%% inconsistency, this API is not the best.
process_batch(
    _IsReplay, _S, _SubState, _ClientInfo, LastSeqNoQos1, LastSeqNoQos2, [], Inflight
) ->
    {Inflight, LastSeqNoQos1, LastSeqNoQos2};
process_batch(
    IsReplay,
    S,
    SubState,
    ClientInfo,
    FirstSeqNoQos1,
    FirstSeqNoQos2,
    [KV | Messages],
    Inflight0
) ->
    #{upgrade_qos := UpgradeQoS, subopts := SubOpts} = SubState,
    {_DsMsgKey, Msg0} = KV,
    Comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
    Comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
    Dup1 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_1), S),
    Dup2 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_2), S),
    Rec = emqx_persistent_session_ds_state:get_seqno(?rec, S),
    Msgs = emqx_session:enrich_message(ClientInfo, Msg0, SubOpts, UpgradeQoS),
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
        IsReplay, S, SubState, ClientInfo, LastSeqNoQos1, LastSeqNoQos2, Messages, Inflight
    ).

%%--------------------------------------------------------------------
%% Transient messages
%%--------------------------------------------------------------------

enqueue_transient(
    _ClientInfo, Msg = #message{qos = Qos}, Session = #{inflight := Inflight0, s := S0}
) ->
    %% TODO: Such messages won't be retransmitted, should the session
    %% reconnect before transient messages are acked.
    %%
    %% Proper solution could look like this: session publishes
    %% transient messages to a separate DS DB that serves as a queue,
    %% then subscribes to a special system topic that contains the
    %% queued messages. Since streams in this DB are exclusive to the
    %% session, messages from the queue can be dropped as soon as they
    %% are acked.
    case Qos of
        ?QOS_0 ->
            S = S0,
            Inflight = emqx_persistent_session_ds_inflight:push({undefined, Msg}, Inflight0);
        QoS when QoS =:= ?QOS_1; QoS =:= ?QOS_2 ->
            SeqNo = inc_seqno(
                QoS, emqx_persistent_session_ds_state:get_seqno(?next(QoS), S0)
            ),
            S = put_seqno(?next(QoS), SeqNo, S0),
            Inflight = emqx_persistent_session_ds_inflight:push({SeqNo, Msg}, Inflight0)
    end,
    Session#{
        inflight := Inflight,
        s := S
    }.

%%--------------------------------------------------------------------
%% Inflight fill and drain
%%--------------------------------------------------------------------

drain_inflight(Session0 = #{inflight := Inflight0, s := S0, stream_scheduler_s := SchedS}) ->
    ?tp(?sessds_drain_inflight, #{}),
    {Publishes, Inflight, S} = do_drain_inflight(Inflight0, S0, [], SchedS),
    Session = ensure_state_commit_timer(
        cancel_delivery_timer(Session0#{inflight := Inflight, s := S})
    ),
    {Publishes, Session}.

do_drain_inflight(Inflight0, S0, Acc, SchedS) ->
    case emqx_persistent_session_ds_inflight:pop(Inflight0) of
        undefined ->
            {lists:reverse(Acc), Inflight0, S0};
        {{other, #ds_suback{stream_key = Key, seqno = SeqNo}}, Inflight} ->
            emqx_persistent_session_ds_stream_scheduler:suback(Key, SeqNo, SchedS),
            do_drain_inflight(Inflight, S0, Acc, SchedS);
        {{pubrel, SeqNo}, Inflight} ->
            Publish = {pubrel, seqno_to_packet_id(?QOS_2, SeqNo)},
            do_drain_inflight(Inflight, S0, [Publish | Acc], SchedS);
        {{SeqNo, Msg}, Inflight} ->
            case Msg#message.qos of
                ?QOS_0 ->
                    do_drain_inflight(Inflight, S0, [{undefined, Msg} | Acc], SchedS);
                Qos ->
                    S = put_seqno(?dup(Qos), SeqNo, S0),
                    Publish = {seqno_to_packet_id(Qos, SeqNo), Msg},
                    do_drain_inflight(Inflight, S, [Publish | Acc], SchedS)
            end
    end.

%% @doc Move batches from a specified stream's buffer to the inflight.
%% This function is called when the client unblock the stream by
%% acking a message.
-spec drain_buffer_of_stream(
    emqx_persistent_session_ds_stream_scheduler:stream_key(),
    session(),
    clientinfo()
) ->
    session().
drain_buffer_of_stream(
    StreamKey,
    Session0 = #{s := S, buffer := Buf, inflight := Inflight},
    ClientInfo
) ->
    SRS0 = emqx_persistent_session_ds_state:get_stream(StreamKey, S),
    ?tp(debug, "sessds_drain_buffer_of_stream", #{stream => StreamKey, srs => SRS0}),
    {SRS, SubState} = pre_enqueue_new(SRS0, S),
    do_drain_buffer_of_stream(StreamKey, SRS, SubState, Session0, ClientInfo, Buf, Inflight).

do_drain_buffer_of_stream(
    StreamKey,
    SRS0,
    SubState,
    Session0 = #{s := S0},
    ClientInfo,
    Buf0,
    Inflight0
) ->
    case emqx_persistent_session_ds_buffer:pop_batch(StreamKey, Buf0) of
        {[#poll_reply{payload = {ok, _, _}} = DSReply], Buf} ->
            {SRS, Inflight} = enqueue_batch(
                StreamKey, S0, DSReply, SubState, SRS0, ClientInfo, Inflight0
            ),
            do_drain_buffer_of_stream(
                StreamKey, SRS, SubState, Session0, ClientInfo, Buf, Inflight
            );
        {[], Buf} ->
            %% No more buffered messages:
            post_enqueue_new(StreamKey, SRS0, Session0#{buffer := Buf, inflight := Inflight0});
        {[#poll_reply{payload = Other}], Buf} ->
            %% Reached the end of the stream:
            case Other of
                {ok, end_of_stream} ->
                    ok;
                {error, unrecoverable, Err} ->
                    ?tp(warning, ?sessds_replay_unrecoverable_error, #{
                        reason => Err,
                        srs => SRS0
                    })
            end,
            SRS = SRS0#srs{it_end = end_of_stream},
            post_enqueue_new(StreamKey, SRS, Session0#{buffer := Buf, inflight := Inflight0})
    end.

%%--------------------------------------------------------------------------------

-spec create_session(
    boolean(),
    emqx_types:clientid(),
    emqx_persistent_session_ds_state:t(),
    emqx_types:conninfo(),
    emqx_session:conf()
) -> session().
create_session(IsNew, ClientID, S0, ConnInfo, Conf) ->
    {S1, SchedS} = emqx_persistent_session_ds_stream_scheduler:init(S0),
    Buffer = emqx_persistent_session_ds_buffer:new(),
    Inflight = emqx_persistent_session_ds_inflight:new(receive_maximum(ConnInfo)),
    %% Create or init shared subscription state:
    case IsNew of
        false ->
            {ok, S2, SharedSubS} = emqx_persistent_session_ds_shared_subs:open(
                S1, shared_sub_opts(ClientID)
            );
        true ->
            S2 = S1,
            SharedSubS = emqx_persistent_session_ds_shared_subs:new(shared_sub_opts(ClientID))
    end,
    S = emqx_persistent_session_ds_state:commit(S2, #{ensure_new => IsNew}),
    #{
        id => ClientID,
        s => S,
        shared_sub_s => SharedSubS,
        buffer => Buffer,
        inflight => Inflight,
        props => Conf,
        stream_scheduler_s => SchedS,
        replay => undefined,
        ?TIMER_DRAIN_INFLIGHT => undefined,
        ?TIMER_COMMIT => undefined,
        ?TIMER_RETRY_REPLAY => undefined
    }.

%% This function triggers sending packets stored in the inflight to
%% the client (provided there is something to send and the number of
%% in-flight packets is less than `Recieve-Maximum'). Normally, this
%% function is called triggered when:
%%
%% - New messages (durable or transient) are enqueued
%%
%% - When the client releases a packet ID (via PUBACK or PUBCOMP)
-compile({inline, ensure_delivery_timer/1}).
-spec ensure_delivery_timer(session()) -> session().
ensure_delivery_timer(Session) ->
    ensure_state_commit_timer(ensure_timer(?TIMER_DRAIN_INFLIGHT, 0, Session)).

-compile({inline, cancel_delivery_timer/1}).
-spec cancel_delivery_timer(session()) -> session().
cancel_delivery_timer(#{?TIMER_DRAIN_INFLIGHT := TRef} = Session) ->
    emqx_utils:cancel_timer(TRef),
    Session#{?TIMER_DRAIN_INFLIGHT := undefined}.

-spec receive_maximum(conninfo()) -> pos_integer().
receive_maximum(ConnInfo) ->
    %% Note: the default value should be always set by the channel
    %% with respect to the zone configuration, but the type spec
    %% indicates that it's optional.
    maps:get(receive_maximum, ConnInfo, 65_535).

-spec expiry_interval(conninfo()) -> millisecond().
expiry_interval(ConnInfo) ->
    maps:get(expiry_interval, ConnInfo, 0).

%% Note: we don't allow overriding `heartbeat_interval' per
%% zone, since the GC process is responsible for all sessions
%% regardless of the zone.
bump_interval() ->
    emqx_config:get([durable_sessions, heartbeat_interval]).

commit_interval() ->
    bump_interval().

%% get_config(#{zone := Zone}, Key) ->
%%     emqx_config:get_zone_conf(Zone, [durable_sessions | Key]).

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

-spec maybe_set_offline_info(emqx_persistent_session_ds_state:t(), emqx_types:clientid()) ->
    emqx_persistent_session_ds_state:t().
maybe_set_offline_info(S, Id) ->
    case emqx_cm:lookup_client({clientid, Id}) of
        [{_Key, ChannelInfo, Stats}] ->
            emqx_persistent_session_ds_state:set_offline_info(
                #{
                    chan_info => ChannelInfo,
                    stats => Stats,
                    disconnected_at => erlang:system_time(millisecond),
                    last_connected_to => node()
                },
                S
            );
        _ ->
            S
    end.

%%--------------------------------------------------------------------
%% SeqNo tracking
%% --------------------------------------------------------------------

-compile({inline, put_seqno/3}).
put_seqno(Key, Val, S) ->
    ?tp_ignore_side_effects_in_prod(sessds_state_put_seqno, #{track => Key, seqno => Val}),
    emqx_persistent_session_ds_state:put_seqno(Key, Val, S).

-spec update_seqno
    (puback | pubcomp, emqx_types:packet_id(), session(), clientinfo()) ->
        {ok, emqx_types:message(), session()} | {error, _};
    (pubrec, emqx_types:packet_id(), session(), undefined) ->
        {ok, emqx_types:message(), session()} | {error, _}.
update_seqno(
    Track,
    PacketId,
    Session0 = #{
        id := SessionId,
        s := S0,
        inflight := Inflight0,
        shared_sub_s := SharedSubS0,
        stream_scheduler_s := SchedS0
    },
    ClientInfo
) ->
    %% TODO: we pass a bogus message into the hook:
    Msg = emqx_message:make(SessionId, <<>>, <<>>),
    SeqNo = packet_id_to_seqno(PacketId, S0),
    case Track of
        puback ->
            QoS = ?QOS_1,
            SeqNoKey = ?committed(?QOS_1),
            Result = emqx_persistent_session_ds_inflight:puback(SeqNo, Inflight0);
        pubcomp ->
            QoS = ?QOS_2,
            SeqNoKey = ?committed(?QOS_2),
            Result = emqx_persistent_session_ds_inflight:pubcomp(SeqNo, Inflight0);
        pubrec ->
            QoS = ?QOS_2,
            SeqNoKey = ?rec,
            Result = emqx_persistent_session_ds_inflight:pubrec(SeqNo, Inflight0)
    end,
    case Result of
        {ok, Inflight} when Track =:= pubrec ->
            S = put_seqno(SeqNoKey, SeqNo, S0),
            Session = Session0#{inflight := Inflight, s := S},
            {ok, Msg, Session};
        {ok, Inflight} ->
            {ReadyStreams, S1, SchedS} =
                emqx_persistent_session_ds_stream_scheduler:on_seqno_release(
                    QoS, SeqNo, S0, SchedS0
                ),
            S2 = put_seqno(SeqNoKey, SeqNo, S1),
            {S, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_streams_replay(
                S2, SharedSubS0, ReadyStreams
            ),
            case ReadyStreams of
                [] ->
                    Session = Session0;
                _ ->
                    Session1 =
                        lists:foldl(
                            fun(StreamKey, SessionAcc) ->
                                drain_buffer_of_stream(StreamKey, SessionAcc, ClientInfo)
                            end,
                            Session0,
                            ReadyStreams
                        ),
                    Session = ensure_delivery_timer(Session1)
            end,
            {ok, Msg, Session#{
                s := put_seqno(SeqNoKey, SeqNo, S),
                inflight := Inflight,
                stream_scheduler_s := SchedS,
                shared_sub_s := SharedSubS
            }};
        {error, undefined = _Expected} ->
            {error, ?RC_PROTOCOL_ERROR};
        {error, Expected} ->
            ?tp(
                warning,
                ?sessds_out_of_order_commit,
                #{
                    track => Track,
                    packet_id => PacketId,
                    packet_seqno => SeqNo,
                    expected => Expected,
                    track_seqno => emqx_persistent_session_ds_state:get_seqno(SeqNoKey, S0)
                }
            ),
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
%% Will message handling
%%--------------------------------------------------------------------

-spec clear_will_message(session()) -> session().
clear_will_message(#{s := S0} = Session) ->
    S = emqx_persistent_session_ds_state:clear_will_message(S0),
    Session#{s := S}.

-spec publish_will_message_now(session(), message()) -> session().
publish_will_message_now(#{} = Session, WillMsg = #message{}) ->
    _ = emqx_broker:publish(WillMsg),
    clear_will_message(Session).

maybe_set_will_message_timer(#{id := SessionId, s := S}) ->
    case emqx_persistent_session_ds_state:get_will_message(S) of
        #message{} = WillMsg ->
            WillDelayInterval = emqx_channel:will_delay_interval(WillMsg),
            WillDelayInterval > 0 andalso
                emqx_persistent_session_ds_gc_worker:check_session_after(
                    SessionId,
                    timer:seconds(WillDelayInterval)
                ),
            ok;
        _ ->
            ok
    end.

%% @doc Prepare stream state for new messages. Make current end
%% iterator into the begin iterator, update seqnos and subscription
%% state id.
pre_enqueue_new(SRS0 = #srs{it_end = NewItBegin}, S) ->
    SNQ1 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_1), S),
    SNQ2 = emqx_persistent_session_ds_state:get_seqno(?next(?QOS_2), S),
    SRS = SRS0#srs{
        it_begin = NewItBegin,
        it_end = NewItBegin,
        batch_size = 0,
        first_seqno_qos1 = SNQ1,
        last_seqno_qos1 = SNQ1,
        first_seqno_qos2 = SNQ2,
        last_seqno_qos2 = SNQ2
    },
    maybe_update_sub_state_id(SRS, S).

%% @doc Enqueue a batch of messages to the inflight, and _extend_ the
%% corresponding SRS: update end iterator and the last seqnos.

enqueue_batch(
    StreamKey,
    S,
    #poll_reply{seqno = SeqNo, payload = {ok, It, Batch}, size = Size},
    SubState,
    SRS0 = #srs{batch_size = BatchSize, last_seqno_qos1 = SNQ1, last_seqno_qos2 = SNQ2},
    ClientInfo,
    Inflight0
) ->
    %% Enqueue messages:
    {Inflight1, LastSeqnoQos1, LastSeqnoQos2} =
        process_batch(
            false,
            S,
            SubState,
            ClientInfo,
            SNQ1,
            SNQ2,
            Batch,
            Inflight0
        ),
    %% Enqueue the DS suback that will be acked once the message is
    %% sent to the wire:
    Inflight = emqx_persistent_session_ds_inflight:push(
        {other, #ds_suback{stream_key = StreamKey, seqno = SeqNo}},
        Inflight1
    ),
    SRS = SRS0#srs{
        it_end = It,
        batch_size = BatchSize + Size,
        last_seqno_qos1 = LastSeqnoQos1,
        last_seqno_qos2 = LastSeqnoQos2
    },
    {SRS, Inflight}.

post_enqueue_new(
    StreamKey, SRS = #srs{last_seqno_qos1 = SNQ1, last_seqno_qos2 = SNQ2}, Session0 = #{s := S0}
) ->
    S1 = put_next(?next(?QOS_1), SNQ1, S0),
    S2 = put_next(?next(?QOS_2), SNQ2, S1),
    S = emqx_persistent_session_ds_state:put_stream(StreamKey, SRS, S2),
    Session = Session0#{s := S},
    on_enqueue(false, StreamKey, SRS, Session).

%% If needed, refresh reference to the subscription state in the SRS
%% and return the updated records:
-spec maybe_update_sub_state_id(SRS, emqx_persistent_session_ds_state:t()) ->
    {SRS, emqx_persistent_session_ds_subs:subscription_state()} | undefined
when
    SRS :: emqx_persistent_session_ds_stream_scheduler:srs().
maybe_update_sub_state_id(SRS = #srs{sub_state_id = SSID0}, S) ->
    case emqx_persistent_session_ds_state:get_subscription_state(SSID0, S) of
        #{superseded_by := _, parent_subscription := ParentSub} ->
            {_, #{current_state := SSID}} = emqx_persistent_session_ds_subs:find_by_subid(
                ParentSub, S
            ),
            ?tp(?sessds_update_srs_ssid, #{old => SSID0, new => SSID, srs => SRS}),
            maybe_update_sub_state_id(SRS#srs{sub_state_id = SSID}, S);
        #{} = SubState ->
            {SRS, SubState}
    end.

-compile({inline, ensure_timer/3}).
-spec ensure_timer(timer(), non_neg_integer(), session()) -> session().
ensure_timer(Timer, Time, Session) ->
    case Session of
        #{Timer := undefined} ->
            set_timer(Timer, Time, Session);
        #{Timer := TRef} when is_reference(TRef) ->
            Session
    end.

-compile({inline, set_timer/3}).
-spec set_timer(timer(), non_neg_integer(), session()) -> session().
set_timer(Timer, Time, Session) ->
    TRef = emqx_utils:start_timer(Time, {emqx_session, Timer}),
    Session#{Timer := TRef}.

%%--------------------------------------------------------------------
%% Session commit, maintenance and batch jobs
%%--------------------------------------------------------------------

commit(Session) ->
    commit(Session, _Opts = #{}).

commit(Session = #{s := S0}, Opts) ->
    ?tp(debug, ?sessds_commit, #{}),
    S1 = emqx_persistent_session_ds_subs:gc(emqx_persistent_session_ds_stream_scheduler:gc(S0)),
    S = emqx_persistent_session_ds_state:commit(S1, Opts),
    cancel_state_commit_timer(Session#{s := S}).

-spec ensure_state_commit_timer(session()) -> session().
ensure_state_commit_timer(#{s := S, ?TIMER_COMMIT := undefined} = Session) ->
    case emqx_persistent_session_ds_state:is_dirty(S) of
        true ->
            set_timer(?TIMER_COMMIT, commit_interval(), Session);
        false ->
            Session
    end;
ensure_state_commit_timer(Session) ->
    Session.

-spec cancel_state_commit_timer(session()) -> session().
cancel_state_commit_timer(#{?TIMER_COMMIT := TRef} = Session) ->
    emqx_utils:cancel_timer(TRef),
    Session#{?TIMER_COMMIT := undefined}.

%%--------------------------------------------------------------------
%% Management of the heartbeat
%%--------------------------------------------------------------------

init_last_alive_at(S) ->
    init_last_alive_at(now_ms(), S).

init_last_alive_at(NowMs, S0) ->
    NodeEpochId = emqx_persistent_session_ds_node_heartbeat_worker:get_node_epoch_id(),
    S1 = emqx_persistent_session_ds_state:set_node_epoch_id(NodeEpochId, S0),
    emqx_persistent_session_ds_state:set_last_alive_at(NowMs + bump_interval(), S1).

finalize_last_alive_at(S0) ->
    S = emqx_persistent_session_ds_state:set_last_alive_at(now_ms(), S0),
    emqx_persistent_session_ds_state:set_node_epoch_id(undefined, S).

%% NOTE
%% Here we ignore the case when:
%% * the session is terminated abnormally, without running terminate callback,
%% e.g. when the conection was brutally killed;
%% * but its node and persistent session subsystem remained alive.
%%
%% In this case, the session's lifitime is prolonged till the node termination.
get_last_alive_at(S) ->
    LastAliveAt = emqx_persistent_session_ds_state:get_last_alive_at(S),
    NodeEpochId = emqx_persistent_session_ds_state:get_node_epoch_id(S),
    emqx_persistent_session_ds_gc_worker:session_last_alive_at(LastAliveAt, NodeEpochId).

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

%% WARNING: this function must be called inside `?check_trace' to
%% avoid losing invariant violations: some checks may be wrapped in
%% `?defer_assert()' macro,
state_invariants(undefined, Sess) ->
    %% According to the model, the session should not exist. Verify
    %% that it is the case:
    case Sess of
        undefined ->
            true;
        _ ->
            ?tp(
                error,
                "sessds_test_unexpected_session_presence",
                #{sess => Sess}
            ),
            false
    end;
state_invariants(ModelState, #{'_alive' := {true, _}} = Sess) ->
    runtime_state_invariants(ModelState, Sess);
state_invariants(ModelState, #{'_alive' := false} = Sess) ->
    offline_state_invariants(ModelState, Sess).

trace_specs() ->
    [
        {"No warnings", fun tprop_no_warnings/1},
        {"Sequence numbers are strictly increasing", fun tprop_seqnos/1}
    ].

tprop_no_warnings(Trace) ->
    ?assertMatch(
        [],
        ?of_kind(
            [
                ?sessds_replay_inconsistency,
                ?sessds_out_of_order_commit,
                ?sessds_unknown_message,
                ?sessds_unknown_timeout
            ],
            Trace
        )
    ).

%% Sequence numbers for all tracks should be increasing:
tprop_seqnos(Trace) ->
    L = ?projection([track, seqno], ?of_kind(sessds_state_put_seqno, Trace)),
    %% Group seqno operations by track:
    M = maps:groups_from_list(fun({Track, _}) -> Track end, L),
    %% Validate seqnos for each track:
    maps:foreach(
        fun(_Track, Vals) ->
            ?defer_assert(snabbkaffe:increasing(Vals))
        end,
        M
    ),
    true.

%% @doc Check invariantss for a living session
runtime_state_invariants(ModelState, Session) ->
    emqx_persistent_session_ds_stream_scheduler:runtime_state_invariants(ModelState, Session) and
        emqx_persistent_session_ds_subs:state_invariants(ModelState, Session).

%% @doc Check invariants for a saved session state
offline_state_invariants(ModelState, Session) ->
    emqx_persistent_session_ds_stream_scheduler:offline_state_invariants(ModelState, Session) and
        emqx_persistent_session_ds_subs:state_invariants(ModelState, Session).

-endif.
