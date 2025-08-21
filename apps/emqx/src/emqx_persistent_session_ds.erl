%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% @doc This module implements an MQTT session that can survive the
%% restart of an EMQX node by backing up its state on disk. It
%% consumes messages from a shared durable storage, in contrast to
%% regular "mem" sessions, which store all received messages in their
%% own memory queues.
%%
%% The main challenge with durable sessions is to replay sent but
%% unacked messages when the client reconnects. This implementation
%% addresses this issue by storing iterators, batch sizes, and
%% sequence numbers of MQTT packets for consumed messages as an array
%% of "stream replay state" records (`#srs'). In this way, messages
%% and their corresponding packet IDs can be reconstructed by
%% "replaying" the stored SRSes.
-module(emqx_persistent_session_ds).

-compile(inline).
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
    terminate/3
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
-export([create_tables/0]).

%% internal export used by session GC process
-export([destroy_session/1]).

-export([print_session/1, seqno_diff/4]).

-ifdef(TEST).
-export([
    open_session_state/2,
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
    subscription_id/0,
    subscription/0,
    session/0,
    stream_state/0
]).

-type seqno() :: non_neg_integer().

%% Currently, this is the clientid.  We avoid `emqx_types:clientid()' because that can be
%% an atom, in theory (?).
-type id() :: binary().
-type topic_filter() :: emqx_types:topic() | emqx_types:share().

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
    mode => emqx_persistent_session_ds_subs:subscription_mode(),
    current_state := emqx_persistent_session_ds_subs:subscription_state_id(),
    subopts := map()
}.

-type shared_sub_state() :: term().

-define(TIMER_DRAIN_INFLIGHT, timer_drain_inflight).
-define(TIMER_COMMIT, timer_commit).
-define(TIMER_RETRY_REPLAY, timer_retry_replay).

-type timer() ::
    ?TIMER_COMMIT
    | ?TIMER_RETRY_REPLAY
    | ?TIMER_SCHEDULER_RETRY.

-type timer_state() :: reference() | undefined.

-record(ds_suback, {sub_ref, seqno}).

%% TODO: Needs configuration?
-define(TIMEOUT_RETRY_REPLAY, 1000).

-type session() :: #{
    %% Client ID
    id := id(),
    %% Configuration:
    props := #{upgrade_qos := boolean(), max_subscriptions := _, _ => _},
    %% Persistent state:
    s := emqx_persistent_session_ds_state:t(),
    %% Shared subscription state:
    shared_sub_s := shared_sub_state(),
    %% Buffer accumulates batches received from the DS, that could not
    %% be immediately added to the inflight due to stream blocking.
    %% Buffered messages have not been seen by the client. Therefore,
    %% they (and their iterators) are entirely ephemeral and are lost
    %% when the client disconnets. Session restarts with an empty
    %% buffer.
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
    %% List of stream replay states to be added to the inflight.
    replay := [{_StreamKey, stream_state()}, ...] | undefined,
    %% Will message (optional)
    will_msg := emqx_types:message() | undefined,
    %% Flags:
    ?TIMER_DRAIN_INFLIGHT := boolean(),
    %% Timers:
    ?TIMER_RETRY_REPLAY := timer_state(),
    ?TIMER_COMMIT := timer_state()
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
    State = ensure_new_session_state(ClientID, ConnInfo),
    create_session(new, ClientID, State, ClientInfo, ConnInfo, MaybeWillMsg, Conf).

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
    case open_session_state(ClientID, ConnInfo) of
        false ->
            false;
        State ->
            Session = create_session(
                takeover, ClientID, State, ClientInfo, ConnInfo, MaybeWillMsg, Conf
            ),
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
print_session(ClientID) ->
    case try_get_live_session(ClientID) of
        {Pid, SessionState} ->
            maps:update_with(
                s, fun emqx_persistent_session_ds_state:format/1, SessionState#{
                    '_alive' => {true, Pid}
                }
            );
        not_found ->
            case emqx_persistent_session_ds_state:print_session(ClientID) of
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
    #share{} = TopicFilter,
    SubOpts,
    Session0
) ->
    case emqx_persistent_session_ds_shared_subs:on_subscribe(TopicFilter, SubOpts, Session0) of
        {ok, S, SharedSubS} ->
            Session = Session0#{s := S, shared_sub_s := SharedSubS},
            {ok, async_checkpoint(Session)};
        Error = {error, _} ->
            Error
    end;
subscribe(
    TopicFilter,
    SubOpts,
    Session0 = #{stream_scheduler_s := SchedS0}
) ->
    case emqx_persistent_session_ds_subs:on_subscribe(TopicFilter, SubOpts, Session0) of
        {Ok, durable, S1, Subscription} when Ok == ok; Ok == mode_changed ->
            {_NewSLSIds, S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_subscribe(
                TopicFilter, Subscription, S1, SchedS0
            ),
            Session = Session0#{s := S, stream_scheduler_s := SchedS},
            %% If the subscription mode was changed, flush any matching transient messages
            %% coming directly from the broker:
            Ok == mode_changed andalso flush_transient(TopicFilter),
            {ok, async_checkpoint(Session)};
        {ok, direct, S, _Subscription} ->
            Session = Session0#{s := S},
            {ok, async_checkpoint(Session)};
        Error = {error, _} ->
            Error
    end.

-spec unsubscribe(topic_filter(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    #share{} = TopicFilter,
    Session0 = #{
        id := SessionId,
        s := S0,
        shared_sub_s := SharedSubS0,
        stream_scheduler_s := SchedS0
    }
) ->
    case
        emqx_persistent_session_ds_shared_subs:on_unsubscribe(
            SessionId, TopicFilter, S0, SchedS0, SharedSubS0
        )
    of
        {ok, S, SchedS, SharedSubS, SubOpts = #{id := SubId}} ->
            Session = Session0#{
                s := S, shared_sub_s := SharedSubS, stream_scheduler_s := SchedS
            },
            {ok, async_checkpoint(clear_buffer(SubId, Session)), SubOpts};
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
            {ok, async_checkpoint(clear_buffer(SubId, Session)), SubOpts};
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
    Msg = #message{qos = ?QOS_2, timestamp = TS},
    Session = #{s := S0}
) ->
    case is_awaiting_full(Session) of
        false ->
            case emqx_persistent_session_ds_state:get_awaiting_rel(PacketId, S0) of
                undefined ->
                    Results = emqx_broker:publish(Msg),
                    S = emqx_persistent_session_ds_state:put_awaiting_rel(PacketId, TS, S0),
                    {ok, Results, ensure_state_commit_timer(Session#{s := S})};
                _TS ->
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
            fun(PacketId, TS, Acc) ->
                Age = Now - TS,
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
            {ok, Msg, [], Session};
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
            {ok, Msg, [], Session};
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
    {ok, [], schedule_delivery(Session)}.

%%--------------------------------------------------------------------
%% Timeouts
%%--------------------------------------------------------------------

-spec handle_timeout(clientinfo(), _Timeout, session()) ->
    {ok, replies(), session()} | {ok, replies(), timeout(), session()}.
handle_timeout(_ClientInfo, ?TIMER_DRAIN_INFLIGHT, Session0) ->
    %% This particular piece of code is responsible for sending
    %% messages queued up in the inflight to the client:
    {Publishes, Session} = drain_inflight(Session0),
    {ok, Publishes, Session};
handle_timeout(ClientInfo, ?TIMER_RETRY_REPLAY, Session0) ->
    Session = replay_streams(Session0, ClientInfo),
    {ok, [], ensure_state_commit_timer(Session)};
handle_timeout(_ClientInfo, ?TIMER_COMMIT, Session) ->
    {ok, [], async_checkpoint(Session)};
handle_timeout(ClientInfo, expire_awaiting_rel, Session) ->
    expire(ClientInfo, Session);
handle_timeout(
    _ClientInfo, ?TIMER_SCHEDULER_RETRY, Session = #{s := S0, stream_scheduler_s := SchedS0}
) ->
    {_NewStreams, S, SchedS} = emqx_persistent_session_ds_stream_scheduler:handle_retry(
        S0, SchedS0
    ),
    {ok, [], Session#{stream_scheduler_s := SchedS, s := S}};
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
    schedule_delivery(Session);
handle_info(AsyncReply = #ds_sub_reply{}, Session, ClientInfo) ->
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
    Session#{s := S, stream_scheduler_s := SchedS};
handle_info(#req_sync{from = From, ref = Ref}, Session0, _ClientInfo) ->
    Session = commit(Session0, #{lifetime => up, sync => true}),
    From ! Ref,
    Session;
handle_info(
    Info,
    Session = #{s := S0, stream_scheduler_s := SchedS0, buffer := Buf0},
    _ClientInfo
) ->
    %% Handle all the other messages.
    %%
    %% Is it a state commit reply?
    case emqx_persistent_session_ds_state:on_commit_reply(Info, S0) of
        {ok, S} ->
            %% Yes, session checkpoint just completed.
            ensure_state_commit_timer(Session#{s := S});
        {error, _Reason} ->
            %% Yes, session checkpoint just failed.
            exit(commit_failure);
        ignore ->
            %% No. Is it a DS subscription crash then?
            case emqx_persistent_session_ds_stream_scheduler:handle_down(Info, S0, SchedS0) of
                {drop_buffer, StreamKey, SchedS} ->
                    %% Yes, drop the buffer:
                    Buf = emqx_persistent_session_ds_buffer:drop_stream(StreamKey, Buf0),
                    Session#{
                        stream_scheduler_s := SchedS,
                        buffer := Buf
                    };
                ignore ->
                    %% No, unknown message:
                    ?tp(warning, ?sessds_unknown_message, #{message => Info}),
                    Session
            end
    end.

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
replay(ClientInfo, [], Session0) ->
    #{id := SessionId, s := S0, stream_scheduler_s := SchedS0} = Session0,
    {S1, Events} = emqx_persistent_session_ds_subs:on_session_replay(SessionId, S0),
    {S2, SchedS1} = lists:foldl(
        fun({mode_changed, direct, TopicFilter, #{id := SubId}}, {SAcc, SchedSAcc}) ->
            %% Subscription mode was changed, notify the stream scheduler:
            emqx_persistent_session_ds_stream_scheduler:on_unsubscribe(
                TopicFilter,
                SubId,
                SAcc,
                SchedSAcc
            )
        end,
        {S1, SchedS0},
        Events
    ),
    {S, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_session_replay(S2, SchedS1),
    Streams = emqx_persistent_session_ds_stream_scheduler:find_replay_streams(S),
    Session1 = Session0#{
        s := S,
        stream_scheduler_s := SchedS,
        replay := Streams
    },
    case Events of
        [] -> Session2 = Session1;
        _ -> Session2 = async_checkpoint(Session1)
    end,
    Session = replay_streams(Session2, ClientInfo),
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
replay_streams(Session = #{replay := []}, ClientInfo) ->
    %% Note: we filled the buffer with the historical messages, and
    %% from now on we'll rely on the normal inflight/flow control
    %% mechanisms to replay them:
    on_replay_complete(Session#{replay := undefined}, ClientInfo).

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
        first_seqno_qos1 = FirstSeqNoQos1,
        first_seqno_qos2 = FirstSeqNoQos2
    } = SRS,
    %% TODO: should we handle end_of_stream here?
    case emqx_ds:next(?PERSISTENT_MESSAGE_DB, ItBegin, BatchSize) of
        {ok, ItEnd, Batch} ->
            SubState = emqx_persistent_session_ds_state:get_subscription_state(SSID, S0),
            {Inflight, LastSeqNoQos1, LastSeqNoQos2} =
                process_batch(
                    true,
                    S0,
                    SubState,
                    ClientInfo,
                    FirstSeqNoQos1,
                    FirstSeqNoQos2,
                    Batch,
                    Inflight0
                ),
            check_replay_consistency(
                StreamKey, SRS, ItEnd, Batch, LastSeqNoQos1, LastSeqNoQos2, Session0
            ),
            Session = on_enqueue(true, StreamKey, SRS, Session0#{inflight := Inflight}),
            {ok, SRS, Session};
        Error = {error, _, _} ->
            Error
    end.

check_replay_consistency(StreamKey, SRS, _ItEnd, Batch, LastSeqNoQos1, LastSeqNoQos2, Session) ->
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
            EQ1 =:= LastSeqNoQos1,
            EQ2 =:= LastSeqNoQos2
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
                    q1 => LastSeqNoQos1,
                    q2 => LastSeqNoQos2,
                    sess => Session
                }
            )
    end.

on_replay_complete(Session, ClientInfo) ->
    schedule_delivery(drain_buffer(Session, ClientInfo)).

on_enqueue(
    IsReplay,
    StreamKey,
    SRS,
    Session = #{s := S0, stream_scheduler_s := SchedS0, shared_sub_s := SharedSubS0}
) ->
    {UnblockedStreams, S1, SchedS} = emqx_persistent_session_ds_stream_scheduler:on_enqueue(
        IsReplay, StreamKey, SRS, S0, SchedS0
    ),
    {S, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_streams_replay(
        S1, SharedSubS0, UnblockedStreams
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
    {shutdown, async_checkpoint(Session#{s := S, shared_sub_s := SharedSubS})}.

-spec terminate(emqx_types:clientinfo(), Reason :: term(), session()) -> ok.
terminate(ClientInfo, Reason, Session = #{s := S, id := Id, will_msg := MaybeWillMsg}) ->
    _ = commit(Session#{s := S}, #{lifetime => terminate, sync => true}),
    SessExpiryInterval = emqx_persistent_session_ds_state:get_expiry_interval(S),
    ok = emqx_persistent_session_ds_gc_timer:on_disconnect(Id, SessExpiryInterval),
    ok = emqx_durable_will:on_disconnect(Id, ClientInfo, SessExpiryInterval, MaybeWillMsg),
    ?tp(debug, ?sessds_terminate, #{id => Id, reason => Reason}),
    ok.

%%--------------------------------------------------------------------
%% Management APIs (dashboard)
%%--------------------------------------------------------------------

-spec list_client_subscriptions(emqx_types:clientid()) ->
    {node() | undefined, [{emqx_types:topic() | emqx_types:share(), emqx_types:subopts()}]}
    | {error, not_found}.
list_client_subscriptions(ClientID) ->
    case emqx_persistent_message:is_persistence_enabled() of
        true ->
            %% TODO: this is not the most optimal implementation, since it
            %% should be possible to avoid reading extra data (streams, etc.)
            case print_session(ClientID) of
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

-spec get_client_subscription(emqx_types:clientid(), topic_filter()) ->
    subscription() | undefined.
get_client_subscription(ClientID, #share{} = ShareTopicFilter) ->
    emqx_persistent_session_ds_shared_subs:cold_get_subscription(ClientID, ShareTopicFilter);
get_client_subscription(ClientID, TopicFilter) ->
    emqx_persistent_session_ds_subs:cold_get_subscription(ClientID, TopicFilter).

%%--------------------------------------------------------------------
%% Session tables operations
%%--------------------------------------------------------------------

create_tables() ->
    emqx_persistent_session_ds_state:open_db().

%% @doc Force commit of the transient state to persistent storage
sync(ClientID) ->
    case emqx_cm:lookup_channels(ClientID) of
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
-spec open_session_state(id(), emqx_types:conninfo()) ->
    emqx_persistent_session_ds_state:t() | false.
open_session_state(
    SessionId,
    NewConnInfo = #{proto_name := ProtoName, proto_ver := ProtoVer}
) ->
    NowMs = now_ms(),
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            EI = emqx_persistent_session_ds_state:get_expiry_interval(S0),
            ?tp(?sessds_open_session, #{ei => EI, now => NowMs}),
            %% New connection being established; update the
            %% existing data:
            S1 = emqx_persistent_session_ds_state:set_expiry_interval(EI, S0),
            S = emqx_persistent_session_ds_state:set_peername(
                maps:get(peername, NewConnInfo), S1
            ),
            emqx_persistent_session_ds_state:set_protocol({ProtoName, ProtoVer}, S);
        undefined ->
            false
    end.

-spec ensure_new_session_state(id(), emqx_types:conninfo()) ->
    emqx_persistent_session_ds_state:t().
ensure_new_session_state(
    Id, ConnInfo = #{proto_name := ProtoName, proto_ver := ProtoVer}
) ->
    ?tp(debug, ?sessds_ensure_new, #{id => Id}),
    Now = now_ms(),
    S0 = emqx_persistent_session_ds_state:create_new(Id),
    S1 = emqx_persistent_session_ds_state:set_expiry_interval(expiry_interval(ConnInfo), S0),
    S2 = emqx_persistent_session_ds_state:set_created_at(Now, S1),
    S = lists:foldl(
        fun(Track, SAcc) ->
            put_seqno(Track, 0, SAcc)
        end,
        S2,
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
    emqx_persistent_session_ds_state:set_protocol({ProtoName, ProtoVer}, S).

%% @doc Called when a client reconnects with `clean session=true' or
%% during session GC
-spec session_drop(id(), _Reason) -> ok.
session_drop(SessionId, Reason) ->
    case emqx_persistent_session_ds_state:open(SessionId) of
        {ok, S0} ->
            ?tp(debug, ?sessds_drop, #{client_id => SessionId, reason => Reason}),
            ok = emqx_persistent_session_ds_subs:on_session_drop(SessionId, S0),
            ok = emqx_persistent_session_ds_state:delete(SessionId),
            emqx_persistent_session_ds_gc_timer:delete(SessionId);
        undefined ->
            ok
    end.

now_ms() ->
    erlang:system_time(millisecond).

%%--------------------------------------------------------------------
%% Normal replay:
%%--------------------------------------------------------------------

handle_ds_reply(
    Reply,
    Session0 = #{
        s := S,
        buffer := Buf0,
        stream_scheduler_s := SchedS0,
        inflight := Inflight0,
        replay := Replay
    },
    ClientInfo
) ->
    case emqx_persistent_session_ds_stream_scheduler:verify_reply(Reply, S, SchedS0) of
        {true, StreamKey, SchedS} ->
            SRS0 = emqx_persistent_session_ds_state:get_stream(StreamKey, S),
            case
                emqx_persistent_session_ds_stream_scheduler:is_fully_acked(SRS0, S) and
                    not ?IS_REPLAY_ONGOING(Replay)
            of
                true ->
                    ?tp(?sessds_poll_reply, #{reply => Reply, blocked => false}),
                    %% This stream is not blocked. Add messages
                    %% directly to the inflight and set drain timer.
                    %%
                    %% Note: this may overwrite SRS records of batches
                    %% that only contain QoS0 messages. We ignore QoS0
                    %% messages during replay, so it's not deemed a
                    %% problem.
                    {SRS1, SubState} = pre_enqueue_new(SRS0, S),
                    {SRS, Inflight} = enqueue_batch(
                        S, Reply, SubState, SRS1, ClientInfo, Inflight0
                    ),
                    Session = Session0#{inflight := Inflight, stream_scheduler_s := SchedS},
                    post_enqueue_new(
                        StreamKey,
                        SRS,
                        schedule_delivery(Session)
                    );
                false ->
                    ?tp(?sessds_poll_reply, #{reply => Reply, blocked => true}),
                    %% This stream is blocked, add batch to the buffer
                    %% instead:
                    Buf = emqx_persistent_session_ds_buffer:push_batch(StreamKey, Reply, Buf0),
                    Session0#{buffer := Buf, stream_scheduler_s := SchedS}
            end;
        {false, _, SchedS} ->
            %% Unexpected reply, no action needed:
            Session0#{stream_scheduler_s := SchedS};
        {drop_buffer, StreamKey, SchedS} ->
            %% Scheduler detected inconsistency and requested to drop
            %% the stream buffer:
            Buf = emqx_persistent_session_ds_buffer:drop_stream(StreamKey, Buf0),
            Session0#{
                stream_scheduler_s := SchedS,
                buffer := Buf
            }
    end.

%% @doc Remove buffered data for all streams that belong to a given
%% subscription.
clear_buffer(SubId, Session = #{buffer := Buf}) ->
    Session#{buffer := emqx_persistent_session_ds_buffer:clean_by_subid(SubId, Buf)}.

%%--------------------------------------------------------------------
%% Generic functions for fetching messages (during replay or normal
%% operation):
%% --------------------------------------------------------------------

-record(ctx, {
    is_replay :: boolean(),
    clientinfo :: emqx_types:clientinfo(),
    substate :: emqx_persistent_session_ds_subs:subscription_state(),
    comm1 :: emqx_persistent_session_ds:seqno(),
    comm2 :: emqx_persistent_session_ds:seqno(),
    dup1 :: emqx_persistent_session_ds:seqno(),
    dup2 :: emqx_persistent_session_ds:seqno(),
    rec :: emqx_persistent_session_ds:seqno()
}).

%% Enrich messages according to the subscription options and assign
%% sequence number to each message, later to be used for packet ID
%% generation:
process_batch(IsReplay, S, SubState, ClientInfo, FirstSeqNoQos1, FirstSeqNoQos2, Batch, Inflight) ->
    Ctx = #ctx{
        is_replay = IsReplay,
        clientinfo = ClientInfo,
        substate = SubState,
        comm1 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_1), S),
        comm2 = emqx_persistent_session_ds_state:get_seqno(?committed(?QOS_2), S),
        dup1 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_1), S),
        dup2 = emqx_persistent_session_ds_state:get_seqno(?dup(?QOS_2), S),
        rec = emqx_persistent_session_ds_state:get_seqno(?rec, S)
    },
    do_process_batch(Ctx, FirstSeqNoQos1, FirstSeqNoQos2, Batch, Inflight).

do_process_batch(_Ctx, LastSeqNoQos1, LastSeqNoQos2, [], Inflight) ->
    {Inflight, LastSeqNoQos1, LastSeqNoQos2};
do_process_batch(Ctx, FirstSeqNoQos1, FirstSeqNoQos2, [Msg0 | Batch], Inflight0) ->
    #ctx{clientinfo = ClientInfo, substate = SubState} = Ctx,
    #{upgrade_qos := UpgradeQoS, subopts := SubOpts} = SubState,
    case emqx_session:enrich_message(ClientInfo, Msg0, SubOpts, UpgradeQoS) of
        [Msg = #message{qos = QoS}] ->
            case QoS of
                ?QOS_0 ->
                    SeqNoQos1 = FirstSeqNoQos1,
                    SeqNoQos2 = FirstSeqNoQos2;
                ?QOS_1 ->
                    SeqNoQos1 = inc_seqno(?QOS_1, FirstSeqNoQos1),
                    SeqNoQos2 = FirstSeqNoQos2;
                ?QOS_2 ->
                    SeqNoQos1 = FirstSeqNoQos1,
                    SeqNoQos2 = inc_seqno(?QOS_2, FirstSeqNoQos2)
            end,
            Inflight =
                case QoS of
                    ?QOS_0 when Ctx#ctx.is_replay ->
                        %% We ignore QoS 0 messages during replay:
                        Inflight0;
                    ?QOS_0 ->
                        emqx_persistent_session_ds_inflight:push({undefined, Msg}, Inflight0);
                    ?QOS_1 when SeqNoQos1 =< Ctx#ctx.comm1 ->
                        %% QoS1 message has been acked by the client, ignore:
                        Inflight0;
                    ?QOS_1 when SeqNoQos1 =< Ctx#ctx.dup1 ->
                        %% QoS1 message has been sent but not
                        %% acked. Retransmit:
                        Msg1 = emqx_message:set_flag(dup, true, Msg),
                        emqx_persistent_session_ds_inflight:push({SeqNoQos1, Msg1}, Inflight0);
                    ?QOS_1 ->
                        emqx_persistent_session_ds_inflight:push({SeqNoQos1, Msg}, Inflight0);
                    ?QOS_2 when SeqNoQos2 =< Ctx#ctx.comm2 ->
                        %% QoS2 message has been PUBCOMP'ed by the client, ignore:
                        Inflight0;
                    ?QOS_2 when SeqNoQos2 =< Ctx#ctx.rec ->
                        %% QoS2 message has been PUBREC'ed by the client, resend PUBREL:
                        emqx_persistent_session_ds_inflight:push({pubrel, SeqNoQos2}, Inflight0);
                    ?QOS_2 when SeqNoQos2 =< Ctx#ctx.dup2 ->
                        %% QoS2 message has been sent, but we haven't received PUBREC.
                        %%
                        %% TODO: According to the MQTT standard 4.3.3:
                        %% DUP flag is never set for QoS2 messages? We
                        %% do so for mem sessions, though.
                        Msg1 = emqx_message:set_flag(dup, true, Msg),
                        emqx_persistent_session_ds_inflight:push({SeqNoQos2, Msg1}, Inflight0);
                    ?QOS_2 ->
                        emqx_persistent_session_ds_inflight:push({SeqNoQos2, Msg}, Inflight0)
                end,
            do_process_batch(Ctx, SeqNoQos1, SeqNoQos2, Batch, Inflight);
        [] ->
            do_process_batch(Ctx, FirstSeqNoQos1, FirstSeqNoQos2, Batch, Inflight0)
    end.

%%--------------------------------------------------------------------
%% Transient messages
%%--------------------------------------------------------------------

enqueue_transient(
    _ClientInfo, Msg = #message{qos = QoS}, Session = #{inflight := Inflight0, s := S0}
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
    case QoS of
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

%% @doc Flushes any transient non-retained messages that were delivered
%% to the given topic filter.
flush_transient(TopicFilter) ->
    receive
        {deliver, TopicFilter, #message{headers = #{retained := false}}} ->
            flush_transient(TopicFilter)
    after 0 -> ok
    end.

%%--------------------------------------------------------------------
%% Inflight management
%%--------------------------------------------------------------------

drain_inflight(Session0 = #{inflight := Inflight0, s := S0, stream_scheduler_s := SchedS}) ->
    ?tp(?sessds_drain_inflight, #{}),
    {Publishes, Inflight, DSSubacks, S} = do_drain_inflight(Inflight0, S0, [], #{}),
    maps:foreach(
        fun(SubRef, SeqNo) ->
            emqx_persistent_session_ds_stream_scheduler:suback(SubRef, SeqNo, SchedS)
        end,
        DSSubacks
    ),
    Session = ensure_state_commit_timer(
        Session0#{inflight := Inflight, s := S, ?TIMER_DRAIN_INFLIGHT := false}
    ),
    {Publishes, Session}.

do_drain_inflight(Inflight0, S0, Acc, DSSubacks0) ->
    case emqx_persistent_session_ds_inflight:pop(Inflight0) of
        undefined ->
            {lists:reverse(Acc), Inflight0, DSSubacks0, S0};
        {{other, #ds_suback{sub_ref = SubRef, seqno = SeqNo}}, Inflight} ->
            %% Accumulate the DS subacks:
            DSSubacks = DSSubacks0#{SubRef => SeqNo},
            do_drain_inflight(Inflight, S0, Acc, DSSubacks);
        {{pubrel, SeqNo}, Inflight} ->
            Publish = {pubrel, seqno_to_packet_id(?QOS_2, SeqNo)},
            do_drain_inflight(Inflight, S0, [Publish | Acc], DSSubacks0);
        {{SeqNo, Msg}, Inflight} ->
            case Msg#message.qos of
                ?QOS_0 ->
                    do_drain_inflight(Inflight, S0, [{undefined, Msg} | Acc], DSSubacks0);
                QoS ->
                    S = update_dup(QoS, SeqNo, S0),
                    Publish = {seqno_to_packet_id(QoS, SeqNo), Msg},
                    do_drain_inflight(Inflight, S, [Publish | Acc], DSSubacks0)
            end
    end.

%%--------------------------------------------------------------------
%% Buffer management
%%--------------------------------------------------------------------

%% @doc Move buffered messages from all unblocked streams to the
%% inflight:
drain_buffer(Session = #{buffer := Buf}, ClientInfo) ->
    drain_buffer(Session, ClientInfo, emqx_persistent_session_ds_buffer:iterator(Buf)).

drain_buffer(Session0, ClientInfo, BufferIterator) ->
    case emqx_persistent_session_ds_buffer:next(BufferIterator) of
        none ->
            Session0;
        {StreamKey, NextBufferIterator} ->
            Session = drain_buffer_of_stream(StreamKey, Session0, ClientInfo),
            drain_buffer(Session, ClientInfo, NextBufferIterator)
    end.

%% @doc Move batches from a specified stream's buffer to the inflight.
%% This function is called when the client unblocks the stream by
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
    case emqx_persistent_session_ds_stream_scheduler:is_active_unblocked(SRS0, S) of
        true ->
            {SRS, SubState} = pre_enqueue_new(SRS0, S),
            do_drain_buffer_of_stream(
                StreamKey, SRS, SubState, Session0, ClientInfo, Buf, Inflight
            );
        false ->
            Session0
    end.

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
        {[DSReply], Buf} ->
            ?tp("sessds_drain_buffer_of_stream", #{stream => StreamKey, reply => DSReply}),
            {SRS, Inflight} = enqueue_batch(
                S0, DSReply, SubState, SRS0, ClientInfo, Inflight0
            ),
            do_drain_buffer_of_stream(
                StreamKey, SRS, SubState, Session0, ClientInfo, Buf, Inflight
            );
        {[], Buf} ->
            %% No more buffered messages:
            post_enqueue_new(StreamKey, SRS0, Session0#{buffer := Buf, inflight := Inflight0})
    end.

%%--------------------------------------------------------------------------------

-spec create_session(
    emqx_persistent_session_ds_state:lifetime(),
    emqx_types:clientid(),
    emqx_persistent_session_ds_state:t(),
    emqx_types:clientinfo(),
    emqx_types:conninfo(),
    emqx_maybe:t(message()),
    emqx_session:conf()
) -> session().
create_session(Lifetime, ClientID, S0, ClientInfo, ConnInfo, MaybeWillMsg, Conf) ->
    SchedS = emqx_persistent_session_ds_stream_scheduler:new(),
    Buffer = emqx_persistent_session_ds_buffer:new(),
    Inflight = emqx_persistent_session_ds_inflight:new(receive_maximum(ConnInfo)),
    %% Create or init shared subscription state:
    case Lifetime of
        new ->
            S1 = S0,
            SharedSubS = emqx_persistent_session_ds_shared_subs:new(shared_sub_opts(ClientID));
        _ ->
            {ok, S1, SharedSubS} = emqx_persistent_session_ds_shared_subs:open(
                S0, shared_sub_opts(ClientID)
            )
    end,
    SessExpiryInterval = emqx_persistent_session_ds_state:get_expiry_interval(S1),
    ok = emqx_persistent_session_ds_gc_timer:on_connect(ClientID, SessExpiryInterval),
    ok = emqx_durable_will:on_connect(ClientID, ClientInfo, SessExpiryInterval, MaybeWillMsg),
    S = emqx_persistent_session_ds_state:commit(S1, #{lifetime => Lifetime, sync => true}),
    #{
        id => ClientID,
        s => S,
        shared_sub_s => SharedSubS,
        buffer => Buffer,
        inflight => Inflight,
        props => Conf,
        stream_scheduler_s => SchedS,
        replay => undefined,
        will_msg => MaybeWillMsg,
        ?TIMER_DRAIN_INFLIGHT => false,
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
-spec schedule_delivery(session()) -> session().
schedule_delivery(Session = #{?TIMER_DRAIN_INFLIGHT := false}) ->
    self() ! {timeout, make_ref(), {emqx_session, ?TIMER_DRAIN_INFLIGHT}},
    Session#{?TIMER_DRAIN_INFLIGHT := true};
schedule_delivery(Session = #{?TIMER_DRAIN_INFLIGHT := true}) ->
    Session.

-spec receive_maximum(conninfo()) -> pos_integer().
receive_maximum(ConnInfo) ->
    %% Note: the default value should be always set by the channel
    %% with respect to the zone configuration, but the type spec
    %% indicates that it's optional.
    maps:get(receive_maximum, ConnInfo, 65_535).

-spec expiry_interval(conninfo()) -> millisecond().
expiry_interval(ConnInfo) ->
    maps:get(expiry_interval, ConnInfo, 0).

commit_interval() ->
    emqx_config:get([durable_sessions, checkpoint_interval]).

%% get_config(#{zone := Zone}, Key) ->
%%     emqx_config:get_zone_conf(Zone, [durable_sessions | Key]).

-spec try_get_live_session(emqx_types:clientid()) ->
    {pid(), session()} | not_found | not_persistent.
try_get_live_session(ClientID) ->
    case emqx_cm:lookup_channels(local, ClientID) of
        [Pid] ->
            try
                ConnMod = emqx_cm:do_get_chann_conn_mod(ClientID, Pid),
                ConnState = sys:get_state(Pid),
                case apply(ConnMod, info, [{channel, impl}, ConnState]) of
                    ?MODULE ->
                        {Pid, apply(ConnMod, info, [{channel, session_state}, ConnState])};
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

%% @doc Safely update DUP track for the QoS
update_dup(QoS, New, S) ->
    Key = ?dup(QoS),
    Prev = emqx_persistent_session_ds_state:get_seqno(Key, S),
    case New > Prev of
        true ->
            put_seqno(Key, New, S);
        false ->
            %% We may encounter old packets during replay, but we
            %% don't want to decrease DUP track:
            S
    end.

put_seqno(Key, Val, S) ->
    ?tp_ignore_side_effects_in_prod(?sessds_put_seqno, #{
        track => Key,
        seqno => Val,
        prev_val => emqx_persistent_session_ds_state:get_seqno(Key, S)
    }),
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
    case Track of
        puback ->
            QoS = ?QOS_1,
            SeqNoKey = ?committed(?QOS_1);
        pubcomp ->
            QoS = ?QOS_2,
            SeqNoKey = ?committed(?QOS_2);
        pubrec ->
            QoS = ?QOS_2,
            SeqNoKey = ?rec
    end,
    TrackSeqNo = emqx_persistent_session_ds_state:get_seqno(SeqNoKey, S0),
    PacketSeqNo = packet_id_to_seqno(PacketId, TrackSeqNo),
    Result =
        case Track of
            puback -> emqx_persistent_session_ds_inflight:puback(PacketSeqNo, Inflight0);
            pubcomp -> emqx_persistent_session_ds_inflight:pubcomp(PacketSeqNo, Inflight0);
            pubrec -> emqx_persistent_session_ds_inflight:pubrec(PacketSeqNo, Inflight0)
        end,
    case Result of
        {ok, Inflight} when Track =:= pubrec ->
            S = put_seqno(SeqNoKey, PacketSeqNo, S0),
            Session = Session0#{inflight := Inflight, s := S},
            {ok, Msg, schedule_delivery(Session)};
        {ok, Inflight1} ->
            {UnblockedStreams, S1, SchedS} =
                emqx_persistent_session_ds_stream_scheduler:on_seqno_release(
                    QoS, PacketSeqNo, S0, SchedS0
                ),
            S2 = put_seqno(SeqNoKey, PacketSeqNo, S1),
            {S, SharedSubS} = emqx_persistent_session_ds_shared_subs:on_streams_replay(
                S2, SharedSubS0, UnblockedStreams
            ),
            Session1 = Session0#{
                inflight := Inflight1,
                s := S,
                stream_scheduler_s := SchedS,
                shared_sub_s := SharedSubS
            },
            %% Dump stream messages that have been stored in the
            %% buffer while the stream was blocked into the inflight:
            Session =
                lists:foldl(
                    fun(StreamKey, SessionAcc) ->
                        ?tp(?sessds_unblock_stream, #{key => StreamKey}),
                        drain_buffer_of_stream(StreamKey, SessionAcc, ClientInfo)
                    end,
                    Session1,
                    UnblockedStreams
                ),
            {ok, Msg, schedule_delivery(Session)};
        {error, undefined = _Expected} ->
            {error, ?RC_PROTOCOL_ERROR};
        {error, Expected} ->
            ?tp(
                warning,
                ?sessds_out_of_order_commit,
                #{
                    track => Track,
                    packet_id => PacketId,
                    packet_seqno => PacketSeqNo,
                    expected => Expected,
                    track_seqno => TrackSeqNo
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
-spec packet_id_to_seqno(emqx_types:packet_id(), seqno()) ->
    seqno().
packet_id_to_seqno(PacketId, CommittedSeqNo) ->
    Epoch = CommittedSeqNo bsr ?EPOCH_BITS,
    SeqNo = (Epoch bsl ?EPOCH_BITS) + (PacketId band ?PACKET_ID_MASK),
    case SeqNo < CommittedSeqNo of
        true ->
            SeqNo + ?EPOCH_SIZE;
        false ->
            SeqNo
    end.

-spec inc_seqno(?QOS_1 | ?QOS_2, seqno()) -> emqx_types:packet_id().
inc_seqno(QoS, SeqNo) ->
    NextSeqNo = SeqNo + 1,
    case seqno_to_packet_id(QoS, NextSeqNo) of
        0 ->
            %% We skip sequence numbers that lead to PacketId = 0 to
            %% simplify math. Note: it leads to occasional gaps in the
            %% sequence numbers.
            NextSeqNo + 1;
        _ ->
            NextSeqNo
    end.

%% Note: we use the most significant bit to store the QoS.
seqno_to_packet_id(?QOS_1, SeqNo) ->
    SeqNo band ?PACKET_ID_MASK;
seqno_to_packet_id(?QOS_2, SeqNo) ->
    SeqNo band ?PACKET_ID_MASK bor ?EPOCH_SIZE.

seqno_diff(QoS, A, B, S) ->
    seqno_diff(
        QoS,
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
clear_will_message(#{id := Id} = Session) ->
    emqx_durable_will:clear(Id),
    Session#{will_msg := undefined}.

-spec publish_will_message_now(session(), message()) -> session().
publish_will_message_now(#{} = Session, _WillMsg = #message{}) ->
    %% We always rely on `emqx_durable_will' module to send will
    %% messages. Hence we ignore request from the channel to avoid
    %% duplication of will messages.
    Session.

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
%% corresponding SRS: update end iterator, batch size and the last
%% seqnos, but do not change the begin iterator and first seqnos.
enqueue_batch(
    S,
    #ds_sub_reply{ref = SubRef, seqno = SeqNo, payload = {ok, It, Batch}, size = Size},
    SubState,
    SRS0 = #srs{batch_size = BatchSize, last_seqno_qos1 = SNQ1, last_seqno_qos2 = SNQ2},
    ClientInfo,
    Inflight0
) ->
    %% Enqueue messages:
    {Inflight1, LastSeqNoQos1, LastSeqNoQos2} =
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
    %% Enqueue the DS suback that will be executed once the message is
    %% sent to the wire. This way we tie pushback towards the DS to
    %% the client's speed:
    Inflight = emqx_persistent_session_ds_inflight:push(
        {other, #ds_suback{sub_ref = SubRef, seqno = SeqNo}},
        Inflight1
    ),
    SRS = SRS0#srs{
        it_end = It,
        batch_size = BatchSize + Size,
        last_seqno_qos1 = LastSeqNoQos1,
        last_seqno_qos2 = LastSeqNoQos2
    },
    {SRS, Inflight};
enqueue_batch(
    _S,
    #ds_sub_reply{ref = SubRef, seqno = SeqNo, payload = Other},
    _SubState,
    SRS0,
    _ClientInfo,
    Inflight0
) ->
    %% Assert:
    case Other of
        {ok, end_of_stream} ->
            ok;
        {error, unrecoverable, Err} ->
            ?tp(warning, ?sessds_replay_unrecoverable_error, #{
                reason => Err,
                srs => SRS0
            })
    end,
    %% Enqueue the DS suback:
    Inflight = emqx_persistent_session_ds_inflight:push(
        {other, #ds_suback{sub_ref = SubRef, seqno = SeqNo}},
        Inflight0
    ),
    SRS = SRS0#srs{
        it_end = end_of_stream
    },
    {SRS, Inflight}.

post_enqueue_new(
    StreamKey,
    SRS = #srs{last_seqno_qos1 = SNQ1, last_seqno_qos2 = SNQ2},
    Session0 = #{s := S0}
) ->
    S1 = put_seqno(?next(?QOS_1), SNQ1, S0),
    S2 = put_seqno(?next(?QOS_2), SNQ2, S1),
    S = emqx_persistent_session_ds_state:put_stream(StreamKey, SRS, S2),
    Session = Session0#{s := S},
    on_enqueue(false, StreamKey, SRS, Session).

%% If needed, refresh reference to the subscription state in the SRS
%% and return the updated records:
-spec maybe_update_sub_state_id(SRS, emqx_persistent_session_ds_state:t()) ->
    {SRS, emqx_persistent_session_ds_subs:subscription_state()}
when
    SRS :: emqx_persistent_session_ds_stream_scheduler:srs().
maybe_update_sub_state_id(SRS = #srs{sub_state_id = SSID0}, S) ->
    case emqx_persistent_session_ds_state:get_subscription_state(SSID0, S) of
        #{superseded_by := _, parent_subscription := ParentSub} ->
            case emqx_persistent_session_ds_subs:find_by_subid(ParentSub, S) of
                {_, #{current_state := SSID}} ->
                    ?tp(?sessds_update_srs_ssid, #{old => SSID0, new => SSID, srs => SRS}),
                    maybe_update_sub_state_id(SRS#srs{sub_state_id = SSID}, S);
                undefined ->
                    undefined
            end;
        #{} = SubState ->
            {SRS, SubState};
        undefined ->
            error({unknown_subscription, SRS})
    end.

-spec ensure_timer(timer(), non_neg_integer(), session()) -> session().
ensure_timer(Timer, Time, Session) ->
    case Session of
        #{Timer := undefined} ->
            set_timer(Timer, Time, Session);
        #{Timer := TRef} when is_reference(TRef) ->
            Session
    end.

-spec set_timer(timer(), non_neg_integer(), session()) -> session().
set_timer(Timer, Time, Session) ->
    TRef = emqx_utils:start_timer(Time, {emqx_session, Timer}),
    Session#{Timer := TRef}.

%%--------------------------------------------------------------------
%% Session commit, maintenance and batch jobs
%%--------------------------------------------------------------------

async_checkpoint(Session) ->
    commit(Session, #{lifetime => up, sync => false}).

commit(Session0 = #{s := S0}, Opts) ->
    ?tp(?sessds_commit, #{s => S0, opts => Opts}),
    S1 = emqx_persistent_session_ds_subs:gc(emqx_persistent_session_ds_stream_scheduler:gc(S0)),
    S = emqx_persistent_session_ds_state:commit(S1, Opts),
    Session = Session0#{s := S},
    cancel_state_commit_timer(Session).

-spec ensure_state_commit_timer(session()) -> session().
ensure_state_commit_timer(#{s := S} = Session) ->
    Dirty = emqx_persistent_session_ds_state:is_dirty(S),
    case emqx_persistent_session_ds_state:checkpoint_ref(S) of
        undefined when Dirty ->
            ensure_timer(?TIMER_COMMIT, commit_interval(), Session);
        _ ->
            Session
    end.

-spec cancel_state_commit_timer(session()) -> session().
cancel_state_commit_timer(#{?TIMER_COMMIT := TRef} = Session) ->
    emqx_utils:cancel_timer(TRef),
    Session#{?TIMER_COMMIT := undefined}.

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

%% Generate a sequence number:
seqno_gen() ->
    ?LET(
        {Epoch, Offset},
        {non_neg_integer(), range(0, ?EPOCH_SIZE)},
        (Epoch bsl ?EPOCH_BITS) + Offset
    ).

%%%% Property-based tests:

%% erlfmt-ignore
packet_id_to_seqno_prop() ->
    ?FORALL(
        {QoS, CommittedSeqNo}, {oneof([?QOS_1, ?QOS_2]), seqno_gen()},
        ?FORALL(
            ExpectedSeqNo, range(CommittedSeqNo, CommittedSeqNo + ?EPOCH_SIZE),
            begin
                PacketId = seqno_to_packet_id(QoS, ExpectedSeqNo),
                SeqNo = packet_id_to_seqno(PacketId, CommittedSeqNo),
                ?WHENFAIL(
                    begin
                        io:format(user, "~p~n", [?FUNCTION_NAME]),
                        io:format(user, " *** PacketID = ~p~n", [PacketId]),
                        io:format(user, " *** SeqNo = ~p -> ~p~n", [ExpectedSeqNo, SeqNo]),
                        io:format(user, " *** CommittedSeqNo = ~p~n", [CommittedSeqNo])
                    end,
                    PacketId < 16#10000 andalso SeqNo =:= ExpectedSeqNo
                )
            end)).

inc_seqno_prop() ->
    ?FORALL(
        {QoS, SeqNo},
        {oneof([?QOS_1, ?QOS_2]), seqno_gen()},
        begin
            NewSeqNo = inc_seqno(QoS, SeqNo),
            PacketId = seqno_to_packet_id(QoS, NewSeqNo),
            ?WHENFAIL(
                begin
                    io:format(user, "~p~n", [?FUNCTION_NAME]),
                    io:format(user, " *** QoS = ~p~n", [QoS]),
                    io:format(user, " *** SeqNo = ~p -> ~p~n", [SeqNo, NewSeqNo]),
                    io:format(user, " *** PacketId = ~p~n", [PacketId])
                end,
                PacketId > 0 andalso PacketId < 16#10000
            )
        end
    ).

seqno_diff_prop() ->
    ?FORALL(
        {QoS, SeqNo, N},
        {oneof([?QOS_1, ?QOS_2]), seqno_gen(), range(0, 100)},
        ?IMPLIES(
            seqno_to_packet_id(QoS, SeqNo) > 0,
            begin
                NewSeqNo = apply_n_times(N, fun(A) -> inc_seqno(QoS, A) end, SeqNo),
                Diff = seqno_diff(QoS, NewSeqNo, SeqNo),
                ?WHENFAIL(
                    begin
                        io:format(user, "~p~n", [?FUNCTION_NAME]),
                        io:format(user, " *** QoS = ~p~n", [QoS]),
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
    {timeout, 30, [?_assert(proper:quickcheck(Prop, Opts)) || Prop <- Props]}.

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
    %% Group seqno operations by track:
    M = maps:groups_from_list(
        fun(#{track := Track, ?snk_meta := #{clientid := Client}}) -> {Client, Track} end,
        fun(#{seqno := SeqNo}) -> SeqNo end,
        ?of_kind(?sessds_put_seqno, Trace)
    ),
    %% Validate seqnos for each track:
    maps:foreach(
        fun({Client, Track}, Vals) ->
            ?defer_assert(
                ?assert(
                    snabbkaffe:increasing(Vals),
                    #{
                        client => Client,
                        track => Track,
                        msg => "Session's sequence numbers should be updated monotonically"
                    }
                )
            )
        end,
        M
    ),
    %% Bypass elvis idiocy:
    apply(ct, pal, ["~p: Verified sequence numbers in ~p tracks.", [?FUNCTION_NAME, maps:size(M)]]),
    true.

%% @doc Check invariantss for a living session
runtime_state_invariants(ModelState, Session) ->
    emqx_persistent_session_ds_stream_scheduler:runtime_state_invariants(ModelState, Session) and
        emqx_persistent_session_ds_subs:runtime_state_invariants(ModelState, Session).

%% @doc Check invariants for a saved session state
offline_state_invariants(ModelState, Session) ->
    emqx_persistent_session_ds_stream_scheduler:offline_state_invariants(ModelState, Session) and
        emqx_persistent_session_ds_subs:offline_state_invariants(ModelState, Session).

-endif.
