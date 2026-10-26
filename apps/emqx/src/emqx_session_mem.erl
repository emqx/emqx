%%--------------------------------------------------------------------
%% Copyright (c) 2017-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% @doc
%% A stateful interaction between a Client and a Server. Some Sessions
%% last only as long as the Network Connection, others can span multiple
%% consecutive Network Connections between a Client and a Server.
%%
%% The Session State in the Server consists of:
%%
%% The existence of a Session, even if the rest of the Session State is empty.
%%
%% The Clients subscriptions, including any Subscription Identifiers.
%%
%% QoS 1 and QoS 2 messages which have been sent to the Client, but have not
%% been completely acknowledged.
%%
%% QoS 1 and QoS 2 messages pending transmission to the Client and OPTIONALLY
%% QoS 0 messages pending transmission to the Client.
%%
%% QoS 2 messages which have been received from the Client, but have not been
%% completely acknowledged.The Will Message and the Will Delay Interval
%%
%% If the Session is currently not connected, the time at which the Session
%% will end and Session State will be discarded.
%% @end
%%--------------------------------------------------------------------

%% MQTT Session implementation
%% State is stored in-memory in the process heap.
-module(emqx_session_mem).

-behaviour(emqx_session).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_session.hrl").
-include("emqx_session_mem.hrl").
-include("logger.hrl").
-include_lib("snabbkaffe/include/trace.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([
    create/3,
    open/3,
    destroy/1
]).

-export([
    info/2,
    stats/1,
    chan_info/1,
    obtain_next_pkt_id/1
]).

-export([
    subscribe/4,
    unsubscribe/3,
    get_subscription/2
]).

-export([
    publish/3,
    puback/4,
    pubrec/2,
    pubrel/2,
    pubcomp/4
]).

-export([
    deliver/4,
    replay/3,
    handle_timeout/4,
    handle_info/3,
    handle_signal/3,
    refresh_quota/1,
    disconnect/2,
    terminate/3
]).

-export([
    retry/2,
    expire/2
]).

%% Part of takeover sequence
-export([
    takeover/1,
    resume/2,
    export/1,
    import/2,
    import/3,
    enqueue/3,
    dequeue/3,
    replay/2,
    dedup/2
]).
-export([save_subopts/1]).

%% Eviction agent channel
-export([
    replay_enqueue/4
]).

%% Will message handling
-export([
    clear_will_message/1,
    publish_will_message_now/2
]).

%% Export for CT
-export([set_field/3]).

-type session_id() :: emqx_guid:guid().

-export_type([
    session_id/0,
    session/0,
    subscriptions/0,
    awaiting_rel/0,
    exported/0
]).

-type subscriptions() :: #{emqx_types:topic() => emqx_types:subopts()}.
-type awaiting_rel() :: #{emqx_types:packet_id() => unix_timestamp_ms()}.

-type inflight_data_phase() :: wait_ack | wait_comp.

%% This record tracks QoS2 phases and retries.
-record(inflight_data, {
    phase :: inflight_data_phase(),
    message :: emqx_types:message(),
    timestamp :: unix_timestamp_ms()
}).

-type session() :: #session{}.
-type replayctx() :: emqx_cm_takeover:channelref().

-type exported() :: #{
    id := session_id(),
    is_persistent := boolean(),
    subscriptions := #{emqx_types:topic() => emqx_types:subopts()},
    inflight := [persistent_inflight_entry()],
    mqueue := [emqx_types:message()],
    next_pkt_id := emqx_types:packet_id(),
    awaiting_rel := #{emqx_types:packet_id() => unix_timestamp_ms()},
    created_at := pos_integer()
}.

-type persistent_inflight_entry() :: #{
    packet_id := emqx_types:packet_id(),
    phase := inflight_data_phase(),
    message := emqx_types:message(),
    timestamp := unix_timestamp_ms()
}.

-type unix_timestamp_ms() :: non_neg_integer().
-type inflight_position() :: {integer(), emqx_types:packet_id()} | none.

-type message() :: emqx_types:message().
-type clientinfo() :: emqx_types:clientinfo().
-type conninfo() :: emqx_session:conninfo().
-type replies() :: emqx_session:replies().
-type effect() :: emqx_session:effect().
-type effects() :: emqx_session:effects().

%% Gauges only: the channel process stores them in its stats array (see
%% `emqx_chan_stats`). The attributes that do not change with traffic are
%% cached in the channel info table instead, see `chan_info/1`.
-define(STATS_KEYS, [
    subscriptions_cnt,
    inflight_cnt,
    mqueue_len,
    total_payload_bytes,
    mqueue_dropped,
    awaiting_rel_cnt
]).

-define(DEQUEUE_RETRY_TIMER, retry_dequeue).
-define(DELIVER_RETRY_TIMER, retry_delivery).

-define(RESCHEDULE_DEQUEUE_TIMER_EFFECT, {set_timer, ?DEQUEUE_RETRY_TIMER, 1}).
-define(RETRY_DELIVERY_EFFECT(Timeout), {set_timer, ?DELIVER_RETRY_TIMER, Timeout}).

%%--------------------------------------------------------------------
%% Init a Session
%%--------------------------------------------------------------------

-spec create(clientinfo(), conninfo(), emqx_maybe:t(message())) ->
    session().
create(
    #{zone := Zone} = ClientInfo,
    #{expiry_interval := EI, receive_maximum := ReceiveMax},
    _MaybeWillMsg
) ->
    Limiter = create_limiter(ClientInfo),
    #session{
        id = emqx_guid:gen(),
        created_at = erlang:system_time(millisecond),
        is_persistent = EI > 0,
        subscriptions = #{},
        inflight = emqx_inflight:new(ReceiveMax),
        mqueue = empty_mqueue(Zone),
        quota = Limiter,
        next_pkt_id = 1,
        awaiting_rel = #{},
        conf = emqx_connection_conf:session_conf(Zone)
    }.

create_limiter(#{listener := ListenerId} = ClientInfo) ->
    %% A hook callback may install a limiter container here (e.g.
    %% multi-tenant limiters). Otherwise the compact placeholder stays
    %% until refresh_limiter/1 finds a finite delivery limit for the
    %% listener.
    %%
    %% The hook must run here, not at materialization: materialization
    %% is gated by emqx_limiter:session_limits_configured/1, which only
    %% sees listener-level limits. A hook-installed container (e.g. a
    %% tenant with delivery limits under a listener with none) must not
    %% depend on that gate.
    refresh_limiter(emqx_hooks:'session.limiter_adjustment'(ClientInfo, {lazy, ListenerId}));
create_limiter(#{}) ->
    %% No listener: no quota is instantiated.
    %% This can happen during session handover from eviction agent running
    %% on older EMQX release. Subsequent handover to a client channel should
    %% recreate a limiter.
    false.

%% Build the delivery limiter container when the session has the placeholder
%% and the listener has a finite delivery limit. A placeholder delivers
%% without a limit check.
refresh_limiter({lazy, ListenerId} = Limiter) ->
    case emqx_limiter:session_limits_configured(ListenerId) of
        false -> Limiter;
        true -> emqx_limiter:create_session_client_container(ListenerId)
    end;
refresh_limiter(Limiter) ->
    Limiter.

-doc """
Build the delivery limiter container when a finite delivery limit is now
configured for the listener. The channel calls this on each stats timer, so a
limit configured at runtime applies after at most one stats interval.
""".
-spec refresh_quota(session()) -> session().
refresh_quota(#session{quota = Limiter} = Session) ->
    Session#session{quota = refresh_limiter(Limiter)}.

empty_mqueue(Zone) ->
    %% emqx_mqueue treats 0 as no limit.
    MaxLen =
        case get_mqtt_conf(Zone, max_mqueue_len, 1000) of
            N when is_integer(N), N > 0 -> N;
            _ -> 0
        end,
    {empty, MaxLen}.

%% Returns the queue itself, or builds one from the zone config for an
%% `{empty, MaxLen}` placeholder.
build_mqueue(#{zone := Zone}, {empty, MaxLen}) ->
    emqx_mqueue:init(get_mqueue_conf(Zone, MaxLen));
build_mqueue(_ClientInfo, Q) ->
    Q.

get_mqueue_conf(Zone, MaxLen) ->
    #{
        max_len => MaxLen,
        store_qos0 => get_mqtt_conf(Zone, mqueue_store_qos0),
        priorities => get_mqtt_conf(Zone, mqueue_priorities),
        default_priority => get_mqtt_conf(Zone, mqueue_default_priority)
    }.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).

get_mqtt_conf(Zone, Key, Default) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key], Default).

-spec destroy(session() | clientinfo()) -> ok.
destroy(_Session) ->
    %% NOTE
    %% This is a stub. This session impl has no backing store, thus always `ok`.
    ok.

%%--------------------------------------------------------------------
%% Open a (possibly existing) Session
%%--------------------------------------------------------------------

-spec open(clientinfo(), conninfo(), emqx_maybe:t(message())) ->
    {_IsPresent :: true, session(), replayctx()} | _IsPresent :: false.
open(ClientInfo = #{clientid := ClientId}, ConnInfo, _MaybeWillMsg) ->
    case emqx_cm:takeover_session_begin(ClientId) of
        {ok, ChannelRef, ExportedSession} ->
            Session0 = import(ClientInfo, ConnInfo, ExportedSession),
            Session = resume(ClientInfo, Session0),
            {true, Session, ChannelRef};
        none ->
            false
    end.

resize_inflight(#{receive_maximum := ReceiveMax}, Session = #session{inflight = Inflight}) ->
    Session#session{
        inflight = emqx_inflight:resize(ReceiveMax, Inflight)
    }.

apply_conf(ClientInfo = #{zone := Zone}, Session = #session{}) ->
    Session#session{
        quota = create_limiter(ClientInfo),
        conf = emqx_connection_conf:session_conf(Zone)
    }.

filter_remote_session(Session = #session{mqueue = {empty, _}}) ->
    Session;
filter_remote_session(Session = #session{mqueue = Q}) ->
    Q1 = emqx_mqueue:filter(fun emqx_session:should_keep/1, Q),
    Session#session{mqueue = Q1}.

-doc """
Export state in the current session format.
Cross-version encoding belongs in `emqx_cm_takeover`, where the peer takeover protocol is known.
""".
-spec export(session()) -> exported().
export(#session{
    id = Id,
    is_persistent = IsPersistent,
    subscriptions = Subscriptions,
    inflight = Inflight,
    mqueue = MQueue,
    next_pkt_id = NextPacketId,
    awaiting_rel = AwaitingRel,
    created_at = CreatedAt
}) ->
    #{
        id => Id,
        is_persistent => IsPersistent,
        subscriptions => Subscriptions,
        inflight => export_inflight(Inflight),
        mqueue => export_mqueue(MQueue),
        next_pkt_id => NextPacketId,
        awaiting_rel => AwaitingRel,
        created_at => CreatedAt
    }.

export_mqueue({empty, _}) ->
    [];
export_mqueue(MQueue) ->
    emqx_mqueue:export(MQueue).

export_inflight(Inflight) ->
    [export_inflight_entry(Entry) || Entry <- emqx_inflight:to_list(Inflight)].

export_inflight_entry({PacketId, #message{} = Message}) ->
    #{
        packet_id => PacketId,
        phase => wait_ack,
        message => Message,
        timestamp => inflight_timestamp(Message)
    };
export_inflight_entry(
    {PacketId, #inflight_data{phase = Phase, message = Message, timestamp = Timestamp}}
) ->
    #{
        packet_id => PacketId,
        phase => Phase,
        message => Message,
        timestamp => Timestamp
    }.

-doc """
Import state in the current session format.
Cross-version decoding belongs in `emqx_cm_takeover`, where the peer takeover protocol is known.
""".
-spec import(clientinfo(), exported()) -> session().
import(ClientInfo, #{
    id := Id,
    is_persistent := IsPersistent,
    subscriptions := Subscriptions,
    inflight := Inflight,
    mqueue := Messages,
    next_pkt_id := NextPacketId,
    awaiting_rel := AwaitingRel,
    created_at := CreatedAt
}) ->
    #session{
        id = Id,
        is_persistent = IsPersistent,
        subscriptions = Subscriptions,
        conf = #session_conf{},
        inflight = import_inflight(Inflight),
        mqueue = import_mqueue(ClientInfo, Messages),
        quota = false,
        next_pkt_id = NextPacketId,
        awaiting_rel = AwaitingRel,
        created_at = CreatedAt
    }.

-doc """
Import current-format state with new connection limits and session configuration.
Cross-version decoding belongs in `emqx_cm_takeover`, where the peer takeover protocol is known.
""".
-spec import(clientinfo(), conninfo(), exported()) -> session().
import(ClientInfo, ConnInfo, Exported) ->
    Session0 = import(ClientInfo, Exported),
    Session1 = resize_inflight(ConnInfo, Session0),
    Session2 = apply_conf(ClientInfo, Session1),
    filter_remote_session(Session2).

import_mqueue(ClientInfo = #{zone := Zone}, Messages) ->
    enqueue_messages(ClientInfo, Messages, empty_mqueue(Zone)).

import_inflight(Inflight) ->
    lists:foldl(fun import_inflight_entry/2, emqx_inflight:new(0), Inflight).

import_inflight_entry(
    #{
        packet_id := PacketId,
        phase := Phase,
        message := Msg,
        timestamp := Timestamp
    },
    Inflight
) ->
    %% Imported messages may retain unused metadata.
    Entry =
        case {Phase, Msg#message.qos, inflight_timestamp(Msg)} of
            {wait_ack, ?QOS_1, Timestamp} ->
                Msg;
            _ ->
                #inflight_data{phase = Phase, message = Msg, timestamp = Timestamp}
        end,
    emqx_inflight:insert(PacketId, Entry, Inflight).

%%--------------------------------------------------------------------
%% Info, Stats
%%--------------------------------------------------------------------

%% @doc Get infos of the session.
info(Keys, Session) when is_list(Keys) ->
    [{Key, info(Key, Session)} || Key <- Keys];
info(id, #session{id = Id}) ->
    Id;
info(created_at, #session{created_at = CreatedAt}) ->
    CreatedAt;
info(is_persistent, #session{is_persistent = IsPersistent}) ->
    IsPersistent;
info(durable, _) ->
    false;
info(subscriptions, #session{subscriptions = Subs}) ->
    Subs;
info(subscriptions_cnt, #session{subscriptions = Subs}) ->
    maps:size(Subs);
info(subscriptions_max, #session{conf = #session_conf{max_subscriptions = MaxSubs}}) ->
    MaxSubs;
info(upgrade_qos, #session{conf = #session_conf{upgrade_qos = UpgradeQoS}}) ->
    UpgradeQoS;
info(inflight, #session{inflight = Inflight}) ->
    Inflight;
info(inflight_cnt, #session{inflight = Inflight}) ->
    emqx_inflight:size(Inflight);
info(inflight_max, #session{inflight = Inflight}) ->
    emqx_inflight:max_size(Inflight);
info({inflight_msgs, PagerParams}, #session{inflight = Inflight}) ->
    inflight_query(Inflight, PagerParams);
info(retry_interval, #session{conf = #session_conf{retry_interval = Interval}}) ->
    Interval;
info(mqueue, #session{mqueue = {empty, MaxLen}}) ->
    %% Only for inspecting the queue: the other options are not stored.
    emqx_mqueue:init(#{max_len => MaxLen, store_qos0 => false});
info(mqueue, #session{mqueue = MQueue}) ->
    MQueue;
info(mqueue_len, #session{mqueue = {empty, _}}) ->
    0;
info(mqueue_len, #session{mqueue = MQueue}) ->
    emqx_mqueue:len(MQueue);
info(mqueue_max, #session{mqueue = {empty, MaxLen}}) ->
    MaxLen;
info(mqueue_max, #session{mqueue = MQueue}) ->
    emqx_mqueue:max_len(MQueue);
info(total_payload_bytes, #session{inflight = Inflight, mqueue = {empty, _}}) ->
    inflight_payload_bytes(Inflight);
info(total_payload_bytes, #session{inflight = Inflight, mqueue = MQueue}) ->
    inflight_payload_bytes(Inflight) + emqx_mqueue:payload_bytes(MQueue);
info(mqueue_dropped, #session{mqueue = {empty, _}}) ->
    0;
info(mqueue_dropped, #session{mqueue = MQueue}) ->
    emqx_mqueue:dropped(MQueue);
info({mqueue_msgs, #{limit := _} = PagerParams}, #session{mqueue = {empty, _}}) ->
    %% What `emqx_mqueue:query/2' returns for an empty queue, including its
    %% crash on pager params without a limit.
    {[], #{position => maps:get(position, PagerParams, none), start => none}};
info({mqueue_msgs, PagerParams}, #session{mqueue = MQueue}) ->
    emqx_mqueue:query(MQueue, PagerParams);
info(next_pkt_id, #session{next_pkt_id = PacketId}) ->
    PacketId;
info(awaiting_rel, #session{awaiting_rel = AwaitingRel}) ->
    AwaitingRel;
info(awaiting_rel_cnt, #session{awaiting_rel = AwaitingRel}) ->
    maps:size(AwaitingRel);
info(awaiting_rel_max, #session{conf = #session_conf{max_awaiting_rel = Max}}) ->
    Max;
info(await_rel_timeout, #session{conf = #session_conf{await_rel_timeout = Timeout}}) ->
    Timeout.

%% @doc Get stats of the session.
-spec stats(session()) -> emqx_types:stats().
stats(Session) -> info(?STATS_KEYS, Session).

-doc "The attributes of a memory session that the channel info table caches.".
chan_info(Session) ->
    maps:from_list([{impl, ?MODULE} | info(?CHAN_INFO_SESSION_KEYS ++ [mqueue_max], Session)]).

%%--------------------------------------------------------------------
%% Client -> Broker: SUBSCRIBE / UNSUBSCRIBE
%%--------------------------------------------------------------------

-spec subscribe(emqx_types:clientinfo(), emqx_types:topic(), emqx_types:subopts(), session()) ->
    {ok, session()} | {error, emqx_types:reason_code()}.
subscribe(
    _ClientInfo = #{clientid := ClientId},
    TopicFilter,
    SubOpts,
    Session = #session{
        subscriptions = Subs,
        conf = #session_conf{max_subscriptions = MaxSubs}
    }
) ->
    IsNew = not maps:is_key(TopicFilter, Subs),
    Monitor = maybe_monitor(),
    case IsNew andalso map_size(Subs) >= MaxSubs of
        false ->
            ok = emqx_broker:subscribe(TopicFilter, ClientId, SubOpts, Monitor),
            Session1 = Session#session{subscriptions = maps:put(TopicFilter, SubOpts, Subs)},
            {ok, Session1};
        true ->
            {error, ?RC_QUOTA_EXCEEDED}
    end.

%% Returns 'no_monitor' if self() is monitored by emqx_cm.
%% Otherwise `monitor`.
%%
%% When 'no_monitor', it means there is no need for broker to monitor self() process,
%% because emqx_cm:clean_down/1 calls emqx_broker_helper:clean_down/1
%% when the process is DOWN
maybe_monitor() ->
    case emqx_cm:is_monitored(self()) of
        true ->
            %% gen_tcp/ssl/socket connections
            no_monitor;
        false ->
            %% quic stream
            monitor
    end.

-spec unsubscribe(emqx_types:clientinfo(), emqx_types:topic(), session()) ->
    {ok, session(), emqx_types:subopts()} | {error, emqx_types:reason_code()}.
unsubscribe(
    _ClientInfo,
    TopicFilter,
    Session = #session{subscriptions = Subs}
) ->
    case Subs of
        #{TopicFilter := SubOpts} ->
            ok = emqx_broker:unsubscribe(TopicFilter),
            {ok, Session#session{subscriptions = maps:remove(TopicFilter, Subs)}, SubOpts};
        #{} ->
            {error, ?RC_NO_SUBSCRIPTION_EXISTED}
    end.

-spec get_subscription(emqx_types:topic() | emqx_types:share(), session()) ->
    emqx_types:subopts() | undefined.
get_subscription(Topic, #session{subscriptions = Subs}) ->
    maps:get(Topic, Subs, undefined).

%%--------------------------------------------------------------------
%% Client -> Broker: PUBLISH
%%--------------------------------------------------------------------

-spec publish(emqx_types:packet_id(), emqx_types:message(), session()) ->
    {ok, emqx_types:publish_result(), session()}
    | {error, emqx_types:reason_code()}.
publish(
    PacketId,
    Msg = #message{qos = ?QOS_2, timestamp = Ts},
    Session = #session{
        awaiting_rel = AwaitingRel,
        conf = Conf
    }
) ->
    Dup = emqx_message:get_flag(dup, Msg),
    case maps:is_key(PacketId, AwaitingRel) of
        true when Dup ->
            %% Retransmitted QoS2 PUBLISH: do not publish to subscribers again.
            {ok, dup_publish_result(Msg), Session};
        true ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        false when map_size(AwaitingRel) >= Conf#session_conf.max_awaiting_rel ->
            {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED};
        false ->
            Results = emqx_broker:publish(Msg),
            AwaitingRel1 = maps:put(PacketId, Ts, AwaitingRel),
            {ok, Results, Session#session{awaiting_rel = AwaitingRel1}}
    end;
%% Publish QoS0/1 directly
publish(_PacketId, Msg, Session) ->
    {ok, emqx_broker:publish(Msg), [], Session}.

dup_publish_result(#message{topic = Topic}) ->
    [{node(), Topic, ok}].

%%--------------------------------------------------------------------
%% Client -> Broker: PUBACK
%%--------------------------------------------------------------------

-compile({inline, [ack_inflight/4]}).

-spec puback(clientinfo(), emqx_types:packet_id(), [emqx_session:connflag()], session()) ->
    {ok | effects(), emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
puback(ClientInfo, PacketId, Flags, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, #message{qos = ?QOS_1} = Msg} ->
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            ack_inflight(ClientInfo, Flags, Msg, Session#session{inflight = Inflight1});
        {value, #inflight_data{phase = wait_ack, message = #message{qos = ?QOS_1} = Msg}} ->
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            ack_inflight(ClientInfo, Flags, Msg, Session#session{inflight = Inflight1});
        {value, #inflight_data{phase = wait_ack, message = _DifferentQoSMsg}} ->
            {error, ?RC_PROTOCOL_ERROR};
        {value, _} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

ack_inflight(ClientInfo, Flags, Msg, Session) ->
    {OkEffects, Replies, Session1} = dequeue(ClientInfo, Flags, Session),
    {OkEffects, Msg, Replies, Session1}.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREC
%%--------------------------------------------------------------------

-spec pubrec(emqx_types:packet_id(), session()) ->
    {ok, emqx_types:message(), session()}
    | {error, emqx_types:reason_code()}.
pubrec(PacketId, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, #inflight_data{phase = wait_ack, message = #message{qos = ?QOS_2} = Msg} = Data} ->
            Update = Data#inflight_data{phase = wait_comp},
            Inflight1 = emqx_inflight:update(PacketId, Update, Inflight),
            {ok, Msg, Session#session{inflight = Inflight1}};
        {value, #inflight_data{phase = wait_ack, message = _DifferentQoSMsg}} ->
            {error, ?RC_PROTOCOL_ERROR};
        {value, #message{}} ->
            {error, ?RC_PROTOCOL_ERROR};
        {value, _} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBREL
%%--------------------------------------------------------------------

-spec pubrel(emqx_types:packet_id(), session()) ->
    {ok, session()}
    | {error, emqx_types:reason_code()}.
pubrel(PacketId, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:take(PacketId, AwaitingRel) of
        {_Ts, AwaitingRel1} ->
            NSession = Session#session{awaiting_rel = AwaitingRel1},
            {ok, NSession};
        error ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Client -> Broker: PUBCOMP
%%--------------------------------------------------------------------

-spec pubcomp(clientinfo(), emqx_types:packet_id(), [emqx_session:connflag()], session()) ->
    {ok | effects(), emqx_types:message(), replies(), session()}
    | {error, emqx_types:reason_code()}.
pubcomp(ClientInfo, PacketId, Flags, Session = #session{inflight = Inflight}) ->
    case emqx_inflight:lookup(PacketId, Inflight) of
        {value, #inflight_data{phase = wait_comp, message = #message{qos = ?QOS_2} = Msg}} ->
            Inflight1 = emqx_inflight:delete(PacketId, Inflight),
            ack_inflight(ClientInfo, Flags, Msg, Session#session{inflight = Inflight1});
        {value, #inflight_data{message = #message{qos = QoS}}} when QoS =/= ?QOS_2 ->
            {error, ?RC_PROTOCOL_ERROR};
        {value, #message{}} ->
            {error, ?RC_PROTOCOL_ERROR};
        {value, _Other} ->
            {error, ?RC_PACKET_IDENTIFIER_IN_USE};
        none ->
            {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND}
    end.

%%--------------------------------------------------------------------
%% Dequeue Msgs
%%--------------------------------------------------------------------

-compile(
    {inline, [
        dequeue_next/2,
        finish_dequeue/6,
        reply/7,
        boolean_to_int/1
    ]}
).

dequeue(_ClientInfo, _Flags, Session = #session{mqueue = {empty, _}}) ->
    {ok, [], Session};
dequeue(
    ClientInfo,
    Flags,
    Session = #session{inflight = Inflight, mqueue = Q, quota = L, next_pkt_id = PktId}
) ->
    case emqx_mqueue:is_empty(Q) of
        true ->
            {ok, [], Session};
        false ->
            %% NOTE
            %% `Budget` Bounds outgoing messages, excluding expired and over-limit
            %% messages.
            Zone = maps:get(zone, ClientInfo),
            Allowance = batch_n(Inflight),
            Budget = max(emqx_inflight:max_size(Inflight), ?DEFAULT_BATCH_N),
            Congested = lists:member(congested, Flags),
            dequeue(
                ClientInfo,
                Zone,
                Congested,
                Session,
                Allowance,
                Budget,
                [],
                Q,
                Inflight,
                L,
                PktId
            )
    end.

-doc """
Current dequeue policy depends on congestion status:
 * Total budget: no more than `max(MaxInflight, ?DEFAULT_BATCH_N)` outgoing messages
   in a single batch.
 * Inflight allowance: number of free inflight slots, or `?DEFAULT_BATCH_N` when
   inflight is unlimited.
 * Expired and over-limit messages consume neither budget nor inflight allowance.
 * If uncongested, most common case:
   - QoS1/2 messages consume empty inflight slots.
   - QoS0 messages get into the batch.
   - Once inflight is full, extract QoS0 from the queue while total budget allows,
     bypassing QoS1/2. Note that if topic priorities are enabled and mqueue is a
     priority queue, extraction stops if currently drained priority lane has no more
     QoS0 messages.
   - Batch contains messages up to total budget.
 * If congested: every message (including QoS0) consumes inflight allowance (number
   of free slots) until the allowance is exhausted.
   - Batch contains messages up to inflight allowance.
""".
dequeue(_ClientInfo, _, _Congested = true, S, 0, _Budget, Acc, Q, Inflight, Limiter, PktId) ->
    %% Connection congested + batch consumed inflight allowance, time to stop.
    %% Delivery retry is deliberately not scheduled.
    reply(ok, S, Acc, Q, Inflight, Limiter, PktId);
dequeue(_ClientInfo, _, _Congested, S, _Allowance, _Budget = 0, Acc, Q, Inflight, Limiter, PktId) ->
    %% Batch consumed whole budget, time to stop.
    finish_dequeue(S, Acc, Q, Inflight, Limiter, PktId);
dequeue(ClientInfo, Zone, Congested, S, Allowance, Budget, Acc0, Q0, Inflight0, L0, PktId) ->
    maybe
        {{value, Msg}, Q} ?= dequeue_next(Allowance, Q0),
        Expired = emqx_message:is_expired(Msg, Zone),
        case Expired orelse try_consume_delivery_rate_limit(Msg, L0) of
            true = _Expired ->
                emqx_session_events:handle_event(ClientInfo, {expired, Msg}),
                dequeue(
                    ClientInfo,
                    Zone,
                    Congested,
                    S,
                    Allowance,
                    Budget,
                    Acc0,
                    Q,
                    Inflight0,
                    L0,
                    PktId
                );
            {true, Limiter} ->
                case Msg#message.qos of
                    ?QOS_0 ->
                        Publish = {undefined, maybe_ack(Msg)},
                        Inflight = Inflight0,
                        %% NOTE: QoS0 consumes inflight allowance under congestion.
                        Left = Allowance - boolean_to_int(Congested),
                        NextPktId = PktId;
                    _Qos12 ->
                        Publish = {PktId, maybe_ack(Msg)},
                        Inflight = insert_inflight(PktId, Msg, Inflight0),
                        Left = Allowance - 1,
                        NextPktId = next_pkt_id(PktId)
                end,
                Acc = [Publish | Acc0],
                dequeue(
                    ClientInfo,
                    Zone,
                    Congested,
                    S,
                    Left,
                    Budget - 1,
                    Acc,
                    Q,
                    Inflight,
                    Limiter,
                    NextPktId
                );
            {false, Limiter, Reason} ->
                case Msg#message.qos of
                    ?QOS_0 ->
                        %% NOTE
                        %% Limiter policy is "drop and continue": a sequence of over-limit
                        %% QoS0 messages may be lost. Mirrors `deliver/9` policy.
                        _ = maybe_ack(Msg),
                        _ = emqx_session_events:handle_event(
                            ClientInfo, {dropped, Msg, rate_limited}
                        ),
                        dequeue(
                            ClientInfo,
                            Zone,
                            Congested,
                            S,
                            Allowance,
                            Budget,
                            Acc0,
                            Q,
                            Inflight0,
                            Limiter,
                            PktId
                        );
                    _Qos12 ->
                        %% Stop at over-limit QoS1/2, retaining it and the remaining queue.
                        Retry = retry_dequeue_effect(Reason, Q0),
                        reply(Retry, S, Acc0, Q0, Inflight0, Limiter, PktId)
                end
        end
    else
        _EmptyOrBlocked when Congested ->
            reply(ok, S, Acc0, Q0, Inflight0, L0, PktId);
        _EmptyOrBlocked ->
            Effect = delivery_effect(S, Inflight0),
            reply(Effect, S, Acc0, Q0, Inflight0, L0, PktId)
    end.

boolean_to_int(true) -> 1;
boolean_to_int(_) -> 0.

dequeue_next(Allowance, Q) when Allowance > 0 ->
    emqx_mqueue:out(Q);
dequeue_next(_, Q) ->
    emqx_mqueue:out_qos0(Q).

finish_dequeue(S0, Acc, Q, Inflight, Limiter, PktId) ->
    Effect =
        case emqx_mqueue:is_empty(Q) of
            true -> delivery_effect(S0, Inflight);
            false -> ?RESCHEDULE_DEQUEUE_TIMER_EFFECT
        end,
    reply(Effect, S0, Acc, Q, Inflight, Limiter, PktId).

reply(OkEffect, S0, Acc, Q, Inflight, Limiter, PktId) ->
    Session = S0#session{
        mqueue = Q,
        inflight = Inflight,
        quota = Limiter,
        next_pkt_id = PktId
    },
    Replies = lists:reverse(Acc),
    {OkEffect, Replies, Session}.

retry_dequeue_effect(_Reason, {empty, _}) ->
    ok;
retry_dequeue_effect(Reason, Q) ->
    case emqx_mqueue:is_empty(Q) of
        true -> ok;
        false -> retry_dequeue_timer_effect(Reason)
    end.

%%--------------------------------------------------------------------
%% Broker -> Client: Deliver
%%--------------------------------------------------------------------

-compile(
    {inline, [
        insert_inflight/3,
        delivery_effect/2
    ]}
).

-spec deliver(clientinfo(), [emqx_types:deliver()], [emqx_session:connflag()], session()) ->
    {ok | effects(), replies(), session()}.
deliver(
    ClientInfo,
    Msgs,
    Flags,
    Session = #session{mqueue = Q, quota = L, inflight = Inflight, next_pkt_id = PktId}
) ->
    Congested = lists:member(congested, Flags),
    deliver(ClientInfo, Congested, Session, Msgs, [], Q, Inflight, L, PktId).

deliver(_ClientInfo, _Congested, S0, [], Acc, Q, Inflight, Limiter, PktId) ->
    OkEffect = delivery_effect(S0, Inflight),
    reply(OkEffect, S0, Acc, Q, Inflight, Limiter, PktId);
deliver(
    ClientInfo,
    Congested,
    S,
    [Msg = #message{qos = ?QOS_0} | More],
    Acc0,
    Q0,
    Inflight,
    L0,
    PktId
) ->
    %% Ensure `maybe_ack` is called here, even if rate limit is hit.
    Publish = {undefined, maybe_ack(Msg)},
    QoS0Queued =
        case Q0 of
            {empty, _} -> false;
            _ -> emqx_mqueue:num_qos0(Q0) > 0
        end,
    case Congested orelse QoS0Queued of
        true ->
            %% Buffer without consuming delivery quota. Charge only when sent.
            Q = enqueue_msg(ClientInfo, Msg, Q0),
            deliver(ClientInfo, Congested, S, More, Acc0, Q, Inflight, L0, PktId);
        false ->
            case try_consume_delivery_rate_limit(Msg, L0) of
                {true, Limiter} ->
                    Acc = [Publish | Acc0],
                    deliver(ClientInfo, Congested, S, More, Acc, Q0, Inflight, Limiter, PktId);
                {false, Limiter, _Reason} ->
                    %% NOTE
                    %% Limiter policy is "drop and continue": a sequence of over-limit
                    %% QoS0 messages may be lost. Check each message: a smaller one may
                    %% still fit the remaining quota.
                    case nack_by_hook(Msg) of
                        true ->
                            ok;
                        false ->
                            _ = emqx_session_events:handle_event(
                                ClientInfo, {dropped, Msg, rate_limited}
                            )
                    end,
                    deliver(ClientInfo, Congested, S, More, Acc0, Q0, Inflight, Limiter, PktId)
            end
    end;
deliver(
    ClientInfo,
    Congested,
    S,
    [Msg = #message{qos = _Qos12} | More],
    Acc0,
    Q0,
    Inflight0,
    L0,
    PktId
) ->
    maybe
        %% NOTE
        %% Earlier QoS1/2 may still await a limiter retry.
        false ?=
            case Q0 of
                {empty, _} -> false;
                _ -> emqx_mqueue:num_qos12(Q0) > 0
            end,
        false ?= emqx_inflight:is_full(Inflight0),
        case try_consume_delivery_rate_limit(Msg, L0) of
            {true, Limiter} ->
                %% Note that we publish message without shared ack header
                %% But add to inflight with ack headers
                %% This ack header is required for redispatch-on-terminate feature to work
                Publish = {PktId, maybe_ack(Msg)},
                Inflight = insert_inflight(PktId, Msg, Inflight0),
                Acc = [Publish | Acc0],
                NextPktId = next_pkt_id(PktId),
                deliver(ClientInfo, Congested, S, More, Acc, Q0, Inflight, Limiter, NextPktId);
            {false, Limiter, Reason} ->
                %% NOTE
                %% Stopping at over-limit QoS1/2 and postpone the remaining batch,
                %% including QoS0. Delivery retries are not scheduled deliberately:
                %% dequeue will schedule it once it pushes out complete under-limit
                %% batch.
                Q = lists:foldl(
                    fun(PendingMsg, QAcc) ->
                        enqueue_unless_nacked(ClientInfo, PendingMsg, QAcc)
                    end,
                    Q0,
                    [Msg | More]
                ),
                Effect = retry_dequeue_effect(Reason, Q),
                reply(Effect, S, Acc0, Q, Inflight0, Limiter, PktId)
        end
    else
        _Blocked = true ->
            Q1 =
                case maybe_nack(Msg) of
                    true -> Q0;
                    false -> enqueue_msg(ClientInfo, Msg, Q0)
                end,
            deliver(ClientInfo, Congested, S, More, Acc0, Q1, Inflight0, L0, PktId)
    end.

delivery_effect(#session{conf = #session_conf{retry_interval = Interval}}, Inflight) when
    Interval =/= infinity
->
    case emqx_inflight:is_empty(Inflight) of
        false ->
            ?RETRY_DELIVERY_EFFECT(Interval);
        true ->
            ok
    end;
delivery_effect(_Session, _Inflight) ->
    ok.

insert_inflight(PktId, Msg, Inflight) ->
    Now = erlang:system_time(microsecond),
    MarkedMsg = mark_begin_deliver(Msg, Now),
    Data =
        case Msg#message.qos of
            ?QOS_1 ->
                MarkedMsg;
            ?QOS_2 ->
                #inflight_data{
                    phase = wait_ack,
                    message = MarkedMsg,
                    timestamp = Now div 1000
                }
        end,
    emqx_inflight:insert(PktId, Data, Inflight).

inflight_payload_bytes(Inflight) ->
    emqx_inflight:fold(
        fun(_PacketId, Data, Acc) ->
            Acc + emqx_message:payload_size(inflight_message(Data))
        end,
        0,
        Inflight
    ).

-spec enqueue(clientinfo(), [emqx_types:message()], session()) ->
    session().
enqueue(ClientInfo, Msgs, Session = #session{mqueue = Q}) when is_list(Msgs) ->
    Session#session{mqueue = enqueue_messages(ClientInfo, Msgs, Q)}.

enqueue_messages(ClientInfo, Msgs, Q) when is_list(Msgs) ->
    lists:foldl(
        fun(Msg, Q0) -> enqueue_msg(ClientInfo, Msg, Q0) end,
        Q,
        Msgs
    ).

enqueue_msg(ClientInfo, Msg, Q0) ->
    Q = build_mqueue(ClientInfo, Q0),
    case emqx_mqueue:in(Msg, Q) of
        {undefined, NQ} ->
            NQ;
        {Dropped, NQ} ->
            emqx_session_events:handle_event(
                ClientInfo,
                {dropped, Dropped, #{
                    reason => queue_full,
                    logctx => #{queue => emqx_mqueue:info(NQ)}
                }}
            ),
            NQ;
        false ->
            emqx_session_events:handle_event(
                ClientInfo,
                {dropped, Msg, #{
                    reason => qos0_msg,
                    logctx => #{queue => emqx_mqueue:info(Q)}
                }}
            ),
            Q0
    end.

maybe_ack(Msg) ->
    emqx_shared_sub:maybe_ack(Msg).

maybe_nack(Msg) ->
    maybe
        false ?= emqx_shared_sub:maybe_nack_dropped(Msg),
        nack_by_hook(Msg)
    end.

%% Sources with their own buffer, for example ExtSub, take back the messages that the
%% delivery rate limit refuses. Other messages go to mqueue.
%% TODO: decide whether shared subscription messages refused by the rate limit go to
%% another group member, as they do when inflight is full.
enqueue_unless_nacked(ClientInfo, Msg, Q) ->
    case nack_by_hook(Msg) of
        true -> Q;
        false -> enqueue_msg(ClientInfo, Msg, Q)
    end.

nack_by_hook(Msg) ->
    emqx_hooks:'message.nack'(Msg, false).

mark_begin_deliver(Msg, Now) ->
    emqx_message:set_header(deliver_begin_at_us, Now, Msg).

%%--------------------------------------------------------------------
%% Timeouts
%%--------------------------------------------------------------------

%% @doc Handle timeout events
-spec handle_timeout(
    clientinfo(), emqx_session:common_timer_name(), [emqx_session:connflag()], session()
) ->
    {ok | effects(), replies(), session()}
    | {ok, replies(), timeout(), session()}.
handle_timeout(ClientInfo, expire_awaiting_rel, _Flags, Session) ->
    expire(ClientInfo, Session);
handle_timeout(ClientInfo, ?DELIVER_RETRY_TIMER, _Flags, Session) ->
    retry(ClientInfo, Session);
handle_timeout(ClientInfo, ?DEQUEUE_RETRY_TIMER, Flags, Session) ->
    dequeue(ClientInfo, Flags, Session).

%%--------------------------------------------------------------------
%% Geneic messages
%%--------------------------------------------------------------------

-spec handle_info(clientinfo(), term(), session()) -> {ok, session()}.
handle_info(ClientInfo, zone_changed, Session) ->
    {ok, apply_conf(ClientInfo, Session)};
handle_info(_ClientInfo, _Msg, Session) ->
    {ok, Session}.

-spec handle_signal(clientinfo(), term(), session()) ->
    {ok, session()} | {ok | effects(), replies(), session()}.
handle_signal(ClientInfo, {connection, decongested, _Info}, Session) ->
    %% Restart delivery retries even if dequeue has no queued messages to process.
    {Effects, Publishes, NSession = #session{inflight = NInflight}} =
        dequeue(ClientInfo, [], Session),
    RetryEffect = delivery_effect(NSession, NInflight),
    {combine_effects(RetryEffect, Effects), Publishes, NSession};
handle_signal(_ClientInfo, _Signal, Session) ->
    {ok, Session}.

%%--------------------------------------------------------------------
%% Retry Delivery
%%--------------------------------------------------------------------

-spec retry(clientinfo(), session()) ->
    {ok | effects(), replies(), session()}.
retry(
    ClientInfo,
    Session = #session{inflight = Inflight, conf = #session_conf{retry_interval = Interval}}
) ->
    case emqx_inflight:is_empty(Inflight) orelse Interval =:= infinity of
        true ->
            {ok, [], Session};
        false ->
            Now = erlang:system_time(millisecond),
            retry_delivery(
                ClientInfo,
                emqx_inflight:to_list(fun sort_fun/2, Inflight),
                [],
                Now,
                Interval,
                Inflight,
                Session
            )
    end.

retry_delivery(_ClientInfo, [], Acc, _Now, Interval, Inflight, Session) ->
    Effect =
        case emqx_inflight:is_empty(Inflight) of
            false ->
                ?RETRY_DELIVERY_EFFECT(Interval);
            _ ->
                ok
        end,
    {Effect, lists:reverse(Acc), Session#session{inflight = Inflight}};
retry_delivery(
    ClientInfo,
    [{PacketId, Data} | More],
    Acc,
    Now,
    Interval,
    Inflight,
    Session
) ->
    Age = age(Now, inflight_timestamp(Data)),
    case Age >= Interval of
        true ->
            {Acc1, Inflight1} = do_retry_delivery(ClientInfo, PacketId, Data, Now, Acc, Inflight),
            retry_delivery(ClientInfo, More, Acc1, Now, Interval, Inflight1, Session);
        false ->
            Effect = ?RETRY_DELIVERY_EFFECT(Interval - max(0, Age)),
            {Effect, lists:reverse(Acc), Session#session{inflight = Inflight}}
    end.

do_retry_delivery(
    ClientInfo,
    PacketId,
    #inflight_data{phase = wait_ack, message = Msg},
    Now,
    Acc,
    Inflight
) ->
    do_retry_delivery(ClientInfo, PacketId, Msg, Now, Acc, Inflight);
do_retry_delivery(
    ClientInfo = #{zone := Zone},
    PacketId,
    #message{} = Msg,
    Now,
    Acc,
    Inflight
) ->
    case emqx_message:is_expired(Msg, Zone) of
        true ->
            _ = emqx_session_events:handle_event(ClientInfo, {expired, Msg}),
            {Acc, emqx_inflight:delete(PacketId, Inflight)};
        false ->
            Msg1 = emqx_message:set_flag(dup, true, Msg),
            Update = #inflight_data{phase = wait_ack, message = Msg1, timestamp = Now},
            Inflight1 = emqx_inflight:update(PacketId, Update, Inflight),
            {[{PacketId, Msg1} | Acc], Inflight1}
    end;
do_retry_delivery(_ClientInfo, PacketId, Data, Now, Acc, Inflight) ->
    Update = Data#inflight_data{timestamp = Now},
    Inflight1 = emqx_inflight:update(PacketId, Update, Inflight),
    {[{pubrel, PacketId} | Acc], Inflight1}.

%%--------------------------------------------------------------------
%% Expire Awaiting Rel
%%--------------------------------------------------------------------

-spec expire(clientinfo(), session()) ->
    {ok, replies(), session()} | {ok, replies(), timeout(), session()}.
expire(ClientInfo, Session = #session{awaiting_rel = AwaitingRel}) ->
    case maps:size(AwaitingRel) of
        0 ->
            {ok, [], Session};
        _ ->
            Now = erlang:system_time(millisecond),
            expire_awaiting_rel(ClientInfo, Now, Session)
    end.

expire_awaiting_rel(
    ClientInfo,
    Now,
    Session = #session{
        awaiting_rel = AwaitingRel,
        conf = #session_conf{await_rel_timeout = Timeout}
    }
) ->
    NotExpired = fun(_PacketId, Ts) -> age(Now, Ts) < Timeout end,
    AwaitingRel1 = maps:filter(NotExpired, AwaitingRel),
    ExpiredCnt = maps:size(AwaitingRel) - maps:size(AwaitingRel1),
    _ = emqx_session_events:handle_event(ClientInfo, {expired_rel, ExpiredCnt}),
    Session1 = Session#session{awaiting_rel = AwaitingRel1},
    case maps:size(AwaitingRel1) of
        0 -> {ok, [], Session1};
        _ -> {ok, [], Timeout, Session1}
    end.

%%--------------------------------------------------------------------
%% Takeover, Resume and Replay
%%--------------------------------------------------------------------

-spec takeover(session()) ->
    ok.
takeover(#session{subscriptions = Subs}) ->
    lists:foreach(fun emqx_broker:unsubscribe/1, maps:keys(Subs)).

-spec save_subopts(session()) -> session().
save_subopts(#session{subscriptions = Subs0} = Session0) ->
    Subs = maps:map(
        fun(TopicFilter, SubOpts0) ->
            emqx_hooks:'session.save_subopts'(#{topic_filter => TopicFilter}, SubOpts0)
        end,
        Subs0
    ),
    Session0#session{subscriptions = Subs}.

-doc """
Restores the session's broker subscriptions in the calling channel process.
This lets the new channel receive broker deliveries while takeover is still
in progress. Completing the handover and restarting message delivery are the
responsibility of replay.
""".
-spec resume(emqx_types:clientinfo(), session()) ->
    session().
resume(_ClientInfo = #{clientid := ClientId}, Session = #session{subscriptions = Subs}) ->
    Monitor = maybe_monitor(),
    ok = maps:foreach(
        fun(TopicFilter, SubOpts) ->
            ok = emqx_broker:subscribe(TopicFilter, ClientId, SubOpts, Monitor)
        end,
        Subs
    ),
    Session.

-doc """
Completes delivery handover from the old channel to the new channel.
Reconciles pending deliveries received by either channel during the subscription
overlap, removing duplicates. Continues the session's unfinished and queued
deliveries before processing those pending deliveries.
""".
-spec replay(emqx_types:clientinfo(), replayctx(), session()) ->
    {ok | effects(), replies(), session()}.
replay(ClientInfo, TakeoverState, Session) ->
    replay(ClientInfo, emqx_utils:drain_deliver(), TakeoverState, Session).

replay(ClientInfo, DeliversLocal, TakeoverState, Session) ->
    case emqx_cm:takeover_session_end(TakeoverState) of
        {ok, PendingsRemote0} ->
            %% NOTE
            %% Here, `Pendings` is a list messages that were pending delivery in the remote
            %% session, see `emqx_channel:handle_call({takeover, 'end'}, Channel)`. When we
            %% have a set of remote deliveries and a set of local deliveries, some publishes
            %% might actually be in both sets, because there's a tiny amount of time when
            %% both remote and local sessions were subscribed to the same set of topics
            %% simultaneously (i.e. after local session calls `resume/2` but before remote
            %% session calls `takeover/1`.
            %% We basically need to:
            %% 1. Combine and deduplicate remote and local pending messages, so that no message
            %%    is delivered twice.
            %% 2. Replay deliveries of the inflight messages, this time to the new channel.
            %% 3. Deliver the combined pending messages, following the same logic as `deliver/3`.
            PendingsLocal = emqx_session:enrich_delivers(ClientInfo, DeliversLocal, Session),
            PendingsRemote = filter_remote_pendings(ClientInfo, Session, PendingsRemote0),
            PendingsAll = dedup(PendingsRemote, PendingsLocal),
            {OkReply, PubsResendQueued, Session1} = replay(ClientInfo, Session),
            {OkDeliver, PubsPending, Session2} = deliver(ClientInfo, PendingsAll, [], Session1),
            Replies = append(PubsResendQueued, PubsPending),
            {combine_effects(OkReply, OkDeliver), Replies, Session2};
        {error, _} ->
            % TODO log error?
            replay(ClientInfo, Session)
    end.

-spec replay_enqueue(emqx_types:clientinfo(), [emqx_types:deliver()], replayctx(), session()) ->
    session().
replay_enqueue(ClientInfo, DeliversLocal, TakeoverState, Session) ->
    case emqx_cm:takeover_session_end(TakeoverState) of
        {ok, PendingsRemote0} ->
            PendingsLocal = emqx_session:enrich_delivers(ClientInfo, DeliversLocal, Session),
            PendingsRemote = filter_remote_pendings(ClientInfo, Session, PendingsRemote0),
            PendingsAll = dedup(PendingsRemote, PendingsLocal),
            enqueue(ClientInfo, PendingsAll, Session);
        {error, _} ->
            % TODO log error?
            PendingsLocal = emqx_session:enrich_delivers(ClientInfo, DeliversLocal, Session),
            enqueue(ClientInfo, PendingsLocal, Session)
    end.

filter_remote_pendings(ClientInfo, Session, Pendings) ->
    Pendings1 = emqx_session:enrich_delivers(ClientInfo, Pendings, Session),
    lists:filter(fun emqx_session:should_keep/1, Pendings1).

-doc """
Restarts delivery of messages already held by the session when a connection is ready
again. Retransmits inflight PUBLISH and PUBREL messages, then resumes draining the
message queue within dequeue limits.
Assumes an uncongested connection.
Also used when an MQTT-SN client wakes up.
Takeover coordination and pending deliveries in channel mailboxes are the
responsibility of the caller.
""".
-spec replay(emqx_types:clientinfo(), session()) ->
    {ok | effects(), replies(), session()}.
replay(ClientInfo, Session) ->
    %% Inflight messages are resent verbatim without an expiry check, unlike the
    %% queued messages dropped on expiry by dequeue/3. This asymmetry is intentional:
    %% per [MQTT-3.3.2-5] an expired message is only deleted if onward delivery has
    %% not started; an inflight message was already sent (awaiting PUBACK/PUBREC/
    %% PUBCOMP), so its delivery must be completed regardless of Message-Expiry-Interval.
    PubsResend = lists:map(
        fun
            ({PacketId, #inflight_data{phase = wait_comp}}) ->
                {pubrel, PacketId};
            ({PacketId, Data}) ->
                {PacketId, emqx_message:set_flag(dup, true, inflight_message(Data))}
        end,
        emqx_inflight:to_list(Session#session.inflight)
    ),
    %% Replay runs on an uncongested connection.
    {OkEffect, More, Session1} = dequeue(ClientInfo, [], Session),
    %% TODO: Arm delivery retries for replayed inflight messages as well.
    {OkEffect, append(PubsResend, More), Session1}.

-spec dedup([emqx_types:message()], [emqx_types:message()]) ->
    [emqx_types:message()].
dedup(Pendings, PendingsLocal) ->
    PendingsLocal1 = lists:filter(
        fun(Msg) -> not lists:keymember(Msg#message.id, #message.id, Pendings) end,
        PendingsLocal
    ),
    append(Pendings, PendingsLocal1).

append(L1, []) -> L1;
append(L1, L2) -> L1 ++ L2.

-spec combine_effects(ok | effect(), ok | effects()) -> ok | effects().
combine_effects(ok, OkEffect) -> OkEffect;
combine_effects(OkEffect, ok) -> OkEffect;
combine_effects(Effect, Effect) -> Effect;
combine_effects(E1, E2) -> [E1, E2].

%%--------------------------------------------------------------------

-spec disconnect(session(), emqx_types:conninfo()) -> {idle, effects(), session()}.
disconnect(Session = #session{}, _ConnInfo) ->
    Effects = [
        {reset_timer, ?DELIVER_RETRY_TIMER, infinity},
        {reset_timer, ?DEQUEUE_RETRY_TIMER, infinity}
    ],
    {idle, Effects, Session}.

-spec terminate(emqx_types:clientinfo(), _Reason, session()) -> ok.
terminate(_ClienInfo, Reason, Session) ->
    maybe_redispatch_shared_messages(Reason, Session),
    ok.

maybe_redispatch_shared_messages(takenover, _Session) ->
    ok;
maybe_redispatch_shared_messages(kicked, _Session) ->
    ok;
maybe_redispatch_shared_messages(_Reason, Session) ->
    redispatch_shared_messages(Session).

redispatch_shared_messages(#session{inflight = Inflight, mqueue = Q}) ->
    AllInflights = emqx_inflight:to_list(fun sort_fun/2, Inflight),
    F = fun({_PacketId, Data}) ->
        case inflight_message(Data) of
            #message{qos = ?QOS_1} = Msg ->
                %% For QoS 2, here is what the spec says:
                %% If the Client's Session terminates before the Client reconnects,
                %% the Server MUST NOT send the Application Message to any other
                %% subscribed Client [MQTT-4.8.2-5].
                {true, Msg};
            _ ->
                false
        end
    end,
    InflightList = lists:filtermap(F, AllInflights),
    %% Redispatched messages enter another queue as new entries.
    emqx_shared_sub:redispatch(InflightList ++ redispatch_queue(Q)).

redispatch_queue({empty, _}) ->
    [];
redispatch_queue(Q) ->
    emqx_mqueue:to_list(Q).

%%--------------------------------------------------------------------
%% Next Packet Id
%%--------------------------------------------------------------------

obtain_next_pkt_id(Session = #session{next_pkt_id = PktId}) ->
    {PktId, Session#session{next_pkt_id = next_pkt_id(PktId)}}.

next_pkt_id(?MAX_PACKET_ID) ->
    1;
next_pkt_id(Id) ->
    Id + 1.

%%--------------------------------------------------------------------
%% Will message handling
%%--------------------------------------------------------------------

-spec clear_will_message(session()) -> session().
clear_will_message(#session{} = Session) ->
    Session.

-spec publish_will_message_now(session(), message()) -> session().
publish_will_message_now(#session{} = Session, #message{} = WillMsg) ->
    _ = emqx_broker:publish(WillMsg),
    Session.

%%--------------------------------------------------------------------
%% Delivery rate limiting
%%--------------------------------------------------------------------

try_consume_delivery_rate_limit(_Msg, Limiter = false) ->
    {true, Limiter};
try_consume_delivery_rate_limit(_Msg, {lazy, _ListenerId} = Limiter) ->
    {true, Limiter};
try_consume_delivery_rate_limit(Msg, Limiter0) ->
    Ret = emqx_limiter_client_container:try_consume(Limiter0, [
        {delivery_bytes, emqx_message:estimate_size(Msg)},
        {delivery_messages, 1}
    ]),
    case Ret of
        {true, _} ->
            Ret;
        {false, _, Reason} ->
            ?tp("mem_delivery_rate_limit", #{reason => Reason}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => cannot_deliver_from_topic_due_to_quota_exceeded,
                    reason => Reason
                },
                #{tag => "QUOTA", topic => emqx_message:topic(Msg)}
            ),
            Ret
    end.

retry_dequeue_timer_effect({failed_to_consume_from_limiter, LimiterId}) ->
    try emqx_limiter_registry:get_limiter_options(LimiterId) of
        #{interval := Interval, capacity := Capacity} ->
            %% Finite capacity
            Time = max(1, Interval div Capacity),
            {set_timer, ?DEQUEUE_RETRY_TIMER, Time};
        _ ->
            %% Infinite capacity; should be impossible at this point.
            ok
    catch
        error:{limiter_not_found, _} ->
            %% Should be impossible, or limiter has changed return type...
            ?tp("session_mem_limiter_not_found", #{limiter_id => LimiterId}),
            ok
    end;
retry_dequeue_timer_effect(_Reason) ->
    %% Some other limiter error reason (should be impossible?)
    ok.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [sort_fun/2, inflight_message/1, inflight_timestamp/1, batch_n/1, age/2]}).

inflight_message(#message{} = Msg) ->
    Msg;
inflight_message(#inflight_data{message = Msg}) ->
    Msg.

inflight_timestamp(#message{headers = #{deliver_begin_at_us := Begin}}) ->
    Begin div 1000;
inflight_timestamp(#inflight_data{timestamp = Ts}) ->
    Ts.

sort_fun({_, A}, {_, B}) ->
    inflight_timestamp(A) =< inflight_timestamp(B).

inflight_insert_time(#message{headers = #{deliver_begin_at_us := Begin}}) ->
    erlang:convert_time_unit(Begin, microsecond, nanosecond).

-spec inflight_query(emqx_inflight:inflight(), #{
    position => inflight_position(),
    limit := pos_integer()
}) ->
    {
        [emqx_types:message()],
        #{position := inflight_position(), start := inflight_position()}
    }.
inflight_query(Inflight, #{limit := Limit} = PagerParams) ->
    InflightL = lists:keysort(1, [
        {{inflight_insert_time(Msg), PacketId}, Msg}
     || {PacketId, Data} <- emqx_inflight:to_list(Inflight), Msg <- [inflight_message(Data)]
    ]),
    StartPos =
        case InflightL of
            [{FirstPos, _} | _] -> FirstPos;
            [] -> none
        end,
    Position = maps:get(position, PagerParams, none),
    Page = sublist_from_pos(InflightL, Position, Limit),
    NextPos =
        case Page of
            [_ | _] = L ->
                {LastPos, _} = lists:last(L),
                LastPos;
            [] ->
                Position
        end,
    InflightMsgs = [with_inflight_query_position(Msg, Pos) || {Pos, Msg} <- Page],
    {InflightMsgs, #{start => StartPos, position => NextPos}}.

sublist_from_pos(InflightList, none = _Position, Limit) ->
    lists:sublist(InflightList, Limit);
sublist_from_pos(InflightList, Position, Limit) ->
    Inflight = lists:dropwhile(
        fun({MsgPosition, _}) ->
            MsgPosition =< Position
        end,
        InflightList
    ),
    lists:sublist(Inflight, Limit).

with_inflight_query_position(#message{extra = Extra} = Msg, Cursor) when is_map(Extra) ->
    Msg#message{extra = Extra#{inflight_cursor => Cursor}};
with_inflight_query_position(Msg, Cursor) ->
    %% Before 5.4.0, the unused extra field defaulted to an empty list.
    Msg#message{extra = #{inflight_cursor => Cursor}}.

batch_n(Inflight) ->
    case emqx_inflight:max_size(Inflight) of
        0 ->
            ?DEFAULT_BATCH_N;
        Sz ->
            %% NOTE
            %% A connection inheriting existing session may advertise Receive-Maximum
            %% lower than the number of outstanding publishes.
            max(0, Sz - emqx_inflight:size(Inflight))
    end.

age(Now, Ts) -> Now - Ts.

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, Session = #session{conf = Conf}) ->
    case lists:member(Name, record_info(fields, session_conf)) of
        true ->
            Pos = emqx_utils:index_of(Name, record_info(fields, session_conf)),
            Session#session{conf = setelement(Pos + 1, Conf, Value)};
        false ->
            Pos = emqx_utils:index_of(Name, record_info(fields, session)),
            setelement(Pos + 1, Session, Value)
    end.
