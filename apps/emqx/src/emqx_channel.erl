%%--------------------------------------------------------------------
%% Copyright (c) 2019-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% MQTT Channel
-module(emqx_channel).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([
    info/1,
    info/2,
    get_mqtt_conf/2,
    get_mqtt_conf/3,
    set_conn_state/2,
    stats/1,
    caps/1
]).

-export([
    init/2,
    handle_in/2,
    handle_deliver/2,
    handle_out/3,
    handle_timeout/3,
    handle_call/2,
    handle_info/2,
    terminate/2
]).

%% Export for emqx_sn
-export([
    do_deliver/2,
    ensure_keepalive/2,
    clear_keepalive/1
]).

%% Exports for CT
-export([set_field/3]).

-import(
    emqx_misc,
    [
        run_fold/3,
        pipeline/3,
        maybe_apply/2
    ]
).

-export_type([channel/0, opts/0]).

-record(channel, {
    %% MQTT ConnInfo
    conninfo :: emqx_types:conninfo(),
    %% MQTT ClientInfo
    clientinfo :: emqx_types:clientinfo(),
    %% MQTT Session
    session :: maybe(emqx_session:session()),
    %% Keepalive
    keepalive :: maybe(emqx_keepalive:keepalive()),
    %% MQTT Will Msg
    will_msg :: maybe(emqx_types:message()),
    %% MQTT Topic Aliases
    topic_aliases :: emqx_types:topic_aliases(),
    %% MQTT Topic Alias Maximum
    alias_maximum :: maybe(map()),
    %% Authentication Data Cache
    auth_cache :: maybe(map()),
    %% Quota checkers
    quota :: maybe(emqx_limiter_container:limiter()),
    %% Timers
    timers :: #{atom() => disabled | maybe(reference())},
    %% Conn State
    conn_state :: conn_state(),
    %% Takeover
    takeover :: boolean(),
    %% Resume
    resuming :: boolean(),
    %% Pending delivers when takeovering
    pendings :: list()
}).

-type channel() :: #channel{}.

-type opts() :: #{
    zone := atom(),
    listener := {Type :: atom(), Name :: atom()},
    atom() => term()
}.

-type conn_state() :: idle | connecting | connected | reauthenticating | disconnected.

-type reply() ::
    {outgoing, emqx_types:packet()}
    | {outgoing, [emqx_types:packet()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: emqx_types:packet() | reply() | [reply()].

-define(IS_MQTT_V5, #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}).

-define(TIMER_TABLE, #{
    alive_timer => keepalive,
    retry_timer => retry_delivery,
    await_timer => expire_awaiting_rel,
    expire_timer => expire_session,
    will_timer => will_message,
    quota_timer => expire_quota_limit
}).

-define(CHANNEL_METRICS, [
    recv_pkt,
    recv_msg,
    'recv_msg.qos0',
    'recv_msg.qos1',
    'recv_msg.qos2',
    'recv_msg.dropped',
    'recv_msg.dropped.await_pubrel_timeout',
    send_pkt,
    send_msg,
    'send_msg.qos0',
    'send_msg.qos1',
    'send_msg.qos2',
    'send_msg.dropped',
    'send_msg.dropped.expired',
    'send_msg.dropped.queue_full',
    'send_msg.dropped.too_large'
]).

-define(INFO_KEYS, [
    conninfo,
    conn_state,
    clientinfo,
    session,
    will_msg
]).

-define(LIMITER_ROUTING, message_routing).

-dialyzer({no_match, [shutdown/4, ensure_timer/2, interval/2]}).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(socktype, #channel{conninfo = ConnInfo}) ->
    maps:get(socktype, ConnInfo, undefined);
info(peername, #channel{conninfo = ConnInfo}) ->
    maps:get(peername, ConnInfo, undefined);
info(sockname, #channel{conninfo = ConnInfo}) ->
    maps:get(sockname, ConnInfo, undefined);
info(proto_name, #channel{conninfo = ConnInfo}) ->
    maps:get(proto_name, ConnInfo, undefined);
info(proto_ver, #channel{conninfo = ConnInfo}) ->
    maps:get(proto_ver, ConnInfo, undefined);
info(connected_at, #channel{conninfo = ConnInfo}) ->
    maps:get(connected_at, ConnInfo, undefined);
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(zone, #channel{clientinfo = ClientInfo}) ->
    maps:get(zone, ClientInfo);
info(listener, #channel{clientinfo = ClientInfo}) ->
    maps:get(listener, ClientInfo);
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(username, #channel{clientinfo = ClientInfo}) ->
    maps:get(username, ClientInfo, undefined);
info(session, #channel{session = Session}) ->
    maybe_apply(fun emqx_session:info/1, Session);
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = Keepalive}) ->
    maybe_apply(fun emqx_keepalive:info/1, Keepalive);
info(will_msg, #channel{will_msg = undefined}) ->
    undefined;
info(will_msg, #channel{will_msg = WillMsg}) ->
    emqx_message:to_map(WillMsg);
info(topic_aliases, #channel{topic_aliases = Aliases}) ->
    Aliases;
info(alias_maximum, #channel{alias_maximum = Limits}) ->
    Limits;
info(timers, #channel{timers = Timers}) ->
    Timers.

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

set_session(Session, Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    %% Assume that this is also an updated session. Allow side effect.
    Session1 = emqx_persistent_session:persist(ClientInfo, ConnInfo, Session),
    Channel#channel{session = Session1}.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{session = Session}) ->
    lists:append(emqx_session:stats(Session), emqx_pd:get_counters(?CHANNEL_METRICS)).

-spec caps(channel()) -> emqx_types:caps().
caps(#channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec init(emqx_types:conninfo(), opts()) -> channel().
init(
    ConnInfo = #{
        peername := {PeerHost, _Port},
        sockname := {_Host, SockPort}
    },
    #{
        zone := Zone,
        limiter := LimiterCfg,
        listener := {Type, Listener}
    } = Opts
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Protocol = maps:get(protocol, ConnInfo, mqtt),
    MountPoint =
        case emqx_config:get_listener_conf(Type, Listener, [mountpoint]) of
            <<>> -> undefined;
            MP -> MP
        end,
    ClientInfo = set_peercert_infos(
        Peercert,
        #{
            zone => Zone,
            listener => emqx_listeners:listener_id(Type, Listener),
            protocol => Protocol,
            peerhost => PeerHost,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            mountpoint => MountPoint,
            is_bridge => false,
            is_superuser => false,
            enable_authn => maps:get(enable_authn, Opts, true)
        },
        Zone
    ),
    {NClientInfo, NConnInfo} = take_ws_cookie(ClientInfo, ConnInfo),
    #channel{
        conninfo = NConnInfo,
        clientinfo = NClientInfo,
        topic_aliases = #{
            inbound => #{},
            outbound => #{}
        },
        auth_cache = #{},
        quota = emqx_limiter_container:get_limiter_by_names([?LIMITER_ROUTING], LimiterCfg),
        timers = #{},
        conn_state = idle,
        takeover = false,
        resuming = false,
        pendings = []
    }.

set_peercert_infos(NoSSL, ClientInfo, _) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo#{username => undefined};
set_peercert_infos(Peercert, ClientInfo, Zone) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    PeercetAs = fun(Key) ->
        case get_mqtt_conf(Zone, Key) of
            cn -> CN;
            dn -> DN;
            crt -> Peercert;
            pem when is_binary(Peercert) -> base64:encode(Peercert);
            md5 when is_binary(Peercert) -> emqx_passwd:hash_data(md5, Peercert);
            _ -> undefined
        end
    end,
    Username = PeercetAs(peer_cert_as_username),
    ClientId = PeercetAs(peer_cert_as_clientid),
    ClientInfo#{username => Username, clientid => ClientId, dn => DN, cn => CN}.

take_ws_cookie(ClientInfo, ConnInfo) ->
    case maps:take(ws_cookie, ConnInfo) of
        {WsCookie, NConnInfo} ->
            {ClientInfo#{ws_cookie => WsCookie}, NConnInfo};
        _ ->
            {ClientInfo, ConnInfo}
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(emqx_types:packet(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(?CONNECT_PACKET(), Channel = #channel{conn_state = ConnState}) when
    ConnState =:= connected orelse ConnState =:= reauthenticating
->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
handle_in(?CONNECT_PACKET(), Channel = #channel{conn_state = connecting}) ->
    handle_out(connack, ?RC_PROTOCOL_ERROR, Channel);
handle_in(?CONNECT_PACKET(ConnPkt) = Packet, Channel) ->
    case
        pipeline(
            [
                fun overload_protection/2,
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun check_connect/2,
                fun enrich_client/2,
                fun set_log_meta/2,
                fun check_banned/2
            ],
            ConnPkt,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, NConnPkt, NChannel = #channel{clientinfo = ClientInfo}} ->
            ?TRACE("MQTT", "mqtt_packet_received", #{packet => Packet}),
            NChannel1 = NChannel#channel{
                will_msg = emqx_packet:will_msg(NConnPkt),
                alias_maximum = init_alias_maximum(NConnPkt, ClientInfo)
            },
            case authenticate(?CONNECT_PACKET(NConnPkt), NChannel1) of
                {ok, Properties, NChannel2} ->
                    process_connect(Properties, ensure_connected(NChannel2));
                {continue, Properties, NChannel2} ->
                    handle_out(auth, {?RC_CONTINUE_AUTHENTICATION, Properties}, NChannel2);
                {error, ReasonCode} ->
                    handle_out(connack, ReasonCode, NChannel1)
            end;
        {error, ReasonCode, NChannel} ->
            handle_out(connack, ReasonCode, NChannel)
    end;
handle_in(
    Packet = ?AUTH_PACKET(ReasonCode, _Properties),
    Channel = #channel{conn_state = ConnState}
) ->
    try
        case {ReasonCode, ConnState} of
            {?RC_CONTINUE_AUTHENTICATION, connecting} -> ok;
            {?RC_CONTINUE_AUTHENTICATION, reauthenticating} -> ok;
            {?RC_RE_AUTHENTICATE, connected} -> ok;
            _ -> error(protocol_error)
        end,
        case authenticate(Packet, Channel) of
            {ok, NProperties, NChannel} ->
                case ConnState of
                    connecting ->
                        process_connect(NProperties, ensure_connected(NChannel));
                    _ ->
                        handle_out(
                            auth,
                            {?RC_SUCCESS, NProperties},
                            NChannel#channel{conn_state = connected}
                        )
                end;
            {continue, NProperties, NChannel} ->
                handle_out(
                    auth,
                    {?RC_CONTINUE_AUTHENTICATION, NProperties},
                    NChannel#channel{conn_state = reauthenticating}
                );
            {error, NReasonCode} ->
                case ConnState of
                    connecting ->
                        handle_out(connack, NReasonCode, Channel);
                    _ ->
                        handle_out(disconnect, NReasonCode, Channel)
                end
        end
    catch
        _Class:_Reason ->
            case ConnState of
                connecting ->
                    handle_out(connack, ?RC_PROTOCOL_ERROR, Channel);
                _ ->
                    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel)
            end
    end;
handle_in(?PACKET(_), Channel = #channel{conn_state = ConnState}) when
    ConnState =/= connected andalso ConnState =/= reauthenticating
->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
handle_in(Packet = ?PUBLISH_PACKET(_QoS), Channel) ->
    case emqx_packet:check(Packet) of
        ok -> process_publish(Packet, Channel);
        {error, ReasonCode} -> handle_out(disconnect, ReasonCode, Channel)
    end;
handle_in(
    ?PUBACK_PACKET(PacketId, _ReasonCode, Properties),
    Channel =
        #channel{clientinfo = ClientInfo, session = Session}
) ->
    case emqx_session:puback(ClientInfo, PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(ClientInfo, Msg, Properties),
            {ok, set_session(NSession, Channel)};
        {ok, Msg, Publishes, NSession} ->
            ok = after_message_acked(ClientInfo, Msg, Properties),
            handle_out(publish, Publishes, set_session(NSession, Channel));
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{msg => "puback_packetId_inuse", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.puback.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{msg => "puback_packetId_not_found", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.puback.missed'),
            {ok, Channel}
    end;
handle_in(
    ?PUBREC_PACKET(PacketId, _ReasonCode, Properties),
    Channel =
        #channel{clientinfo = ClientInfo, session = Session}
) ->
    case emqx_session:pubrec(ClientInfo, PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(ClientInfo, Msg, Properties),
            NChannel = set_session(NSession, Channel),
            handle_out(pubrel, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{msg => "pubrec_packetId_inuse", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.pubrec.inuse'),
            handle_out(pubrel, {PacketId, RC}, Channel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{msg => "pubrec_packetId_not_found", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            handle_out(pubrel, {PacketId, RC}, Channel)
    end;
handle_in(
    ?PUBREL_PACKET(PacketId, _ReasonCode),
    Channel = #channel{
        clientinfo = ClientInfo,
        session = Session
    }
) ->
    case emqx_session:pubrel(ClientInfo, PacketId, Session) of
        {ok, NSession} ->
            NChannel = set_session(NSession, Channel),
            handle_out(pubcomp, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{msg => "pubrec_packetId_not_found", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            handle_out(pubcomp, {PacketId, RC}, Channel)
    end;
handle_in(
    ?PUBCOMP_PACKET(PacketId, _ReasonCode),
    Channel = #channel{
        clientinfo = ClientInfo, session = Session
    }
) ->
    case emqx_session:pubcomp(ClientInfo, PacketId, Session) of
        {ok, NSession} ->
            {ok, set_session(NSession, Channel)};
        {ok, Publishes, NSession} ->
            handle_out(publish, Publishes, set_session(NSession, Channel));
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.pubcomp.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{msg => "pubcomp_packetId_not_found", packetId => PacketId}),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {ok, Channel}
    end;
handle_in(
    Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
    Channel = #channel{clientinfo = ClientInfo}
) ->
    case emqx_packet:check(Packet) of
        ok ->
            TopicFilters0 = parse_topic_filters(TopicFilters),
            TopicFilters1 = put_subid_in_subopts(Properties, TopicFilters0),
            TupleTopicFilters0 = check_sub_authzs(TopicFilters1, Channel),
            HasAuthzDeny = lists:any(
                fun({_TopicFilter, ReasonCode}) ->
                    ReasonCode =:= ?RC_NOT_AUTHORIZED
                end,
                TupleTopicFilters0
            ),
            DenyAction = emqx:get_config([authorization, deny_action], ignore),
            case DenyAction =:= disconnect andalso HasAuthzDeny of
                true ->
                    handle_out(disconnect, ?RC_NOT_AUTHORIZED, Channel);
                false ->
                    TopicFilters2 = [TopicFilter || {TopicFilter, 0} <- TupleTopicFilters0],
                    TopicFilters3 = run_hooks(
                        'client.subscribe',
                        [ClientInfo, Properties],
                        TopicFilters2
                    ),
                    {TupleTopicFilters1, NChannel} = process_subscribe(
                        TopicFilters3,
                        Properties,
                        Channel
                    ),
                    TupleTopicFilters2 =
                        lists:foldl(
                            fun
                                ({{Topic, Opts = #{deny_subscription := true}}, _QoS}, Acc) ->
                                    Key = {Topic, maps:without([deny_subscription], Opts)},
                                    lists:keyreplace(Key, 1, Acc, {Key, ?RC_UNSPECIFIED_ERROR});
                                (Tuple = {Key, _Value}, Acc) ->
                                    lists:keyreplace(Key, 1, Acc, Tuple)
                            end,
                            TupleTopicFilters0,
                            TupleTopicFilters1
                        ),
                    ReasonCodes2 = [
                        ReasonCode
                     || {_TopicFilter, ReasonCode} <- TupleTopicFilters2
                    ],
                    handle_out(suback, {PacketId, ReasonCodes2}, NChannel)
            end;
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;
handle_in(
    Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
    Channel = #channel{clientinfo = ClientInfo}
) ->
    case emqx_packet:check(Packet) of
        ok ->
            TopicFilters1 = run_hooks(
                'client.unsubscribe',
                [ClientInfo, Properties],
                parse_topic_filters(TopicFilters)
            ),
            {ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Properties, Channel),
            handle_out(unsuback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;
handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};
handle_in(
    ?DISCONNECT_PACKET(ReasonCode, Properties),
    Channel = #channel{conninfo = ConnInfo}
) ->
    NConnInfo = ConnInfo#{disconn_props => Properties},
    NChannel = maybe_clean_will_msg(ReasonCode, Channel#channel{conninfo = NConnInfo}),
    process_disconnect(ReasonCode, Properties, NChannel);
handle_in(?AUTH_PACKET(), Channel) ->
    handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);
handle_in({frame_error, frame_too_large}, Channel = #channel{conn_state = connecting}) ->
    shutdown(frame_too_large, ?CONNACK_PACKET(?RC_PACKET_TOO_LARGE), Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, ?CONNACK_PACKET(?RC_MALFORMED_PACKET), Channel);
handle_in({frame_error, frame_too_large}, Channel = #channel{conn_state = ConnState}) when
    ConnState =:= connected orelse ConnState =:= reauthenticating
->
    handle_out(disconnect, {?RC_PACKET_TOO_LARGE, frame_too_large}, Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = ConnState}) when
    ConnState =:= connected orelse ConnState =:= reauthenticating
->
    handle_out(disconnect, {?RC_MALFORMED_PACKET, Reason}, Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = disconnected}) ->
    ?SLOG(error, #{msg => "malformed_mqtt_message", reason => Reason}),
    {ok, Channel};
handle_in(Packet, Channel) ->
    ?SLOG(error, #{msg => "disconnecting_due_to_unexpected_message", packet => Packet}),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel).

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(
    AckProps,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    #{clean_start := CleanStart} = ConnInfo,
    case emqx_cm:open_session(CleanStart, ClientInfo, ConnInfo) of
        {ok, #{session := Session, present := false}} ->
            NChannel = Channel#channel{session = Session},
            handle_out(connack, {?RC_SUCCESS, sp(false), AckProps}, NChannel);
        {ok, #{session := Session, present := true, pendings := Pendings}} ->
            Pendings1 = lists:usort(lists:append(Pendings, emqx_misc:drain_deliver())),
            NChannel = Channel#channel{
                session = Session,
                resuming = true,
                pendings = Pendings1
            },
            handle_out(connack, {?RC_SUCCESS, sp(true), AckProps}, NChannel);
        {error, client_id_unavailable} ->
            handle_out(connack, ?RC_CLIENT_IDENTIFIER_NOT_VALID, Channel);
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_open_session", reason => Reason}),
            handle_out(connack, ?RC_UNSPECIFIED_ERROR, Channel)
    end.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

process_publish(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId), Channel) ->
    case
        pipeline(
            [
                fun check_quota_exceeded/2,
                fun process_alias/2,
                fun check_pub_alias/2,
                fun check_pub_authz/2,
                fun check_pub_caps/2
            ],
            Packet,
            Channel
        )
    of
        {ok, NPacket, NChannel} ->
            Msg = packet_to_message(NPacket, NChannel),
            do_publish(PacketId, Msg, NChannel);
        {error, Rc = ?RC_NOT_AUTHORIZED, NChannel} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_publish_to_topic",
                    reason => emqx_reason_codes:name(Rc)
                },
                #{topic => Topic}
            ),
            case emqx:get_config([authorization, deny_action], ignore) of
                ignore ->
                    case QoS of
                        ?QOS_0 -> {ok, NChannel};
                        ?QOS_1 -> handle_out(puback, {PacketId, Rc}, NChannel);
                        ?QOS_2 -> handle_out(pubrec, {PacketId, Rc}, NChannel)
                    end;
                disconnect ->
                    handle_out(disconnect, Rc, NChannel)
            end;
        {error, Rc = ?RC_QUOTA_EXCEEDED, NChannel} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_publish_to_topic",
                    reason => emqx_reason_codes:name(Rc)
                },
                #{topic => Topic}
            ),
            case QoS of
                ?QOS_0 ->
                    ok = emqx_metrics:inc('packets.publish.dropped'),
                    {ok, NChannel};
                ?QOS_1 ->
                    handle_out(puback, {PacketId, Rc}, NChannel);
                ?QOS_2 ->
                    handle_out(pubrec, {PacketId, Rc}, NChannel)
            end;
        {error, Rc, NChannel} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_publish_to_topic",
                    topic => Topic,
                    reason => emqx_reason_codes:name(Rc)
                },
                #{topic => Topic}
            ),
            handle_out(disconnect, Rc, NChannel)
    end.

packet_to_message(Packet, #channel{
    conninfo = #{proto_ver := ProtoVer},
    clientinfo = #{
        protocol := Protocol,
        clientid := ClientId,
        username := Username,
        peerhost := PeerHost,
        mountpoint := MountPoint
    }
}) ->
    emqx_mountpoint:mount(
        MountPoint,
        emqx_packet:to_message(
            Packet,
            ClientId,
            #{
                proto_ver => ProtoVer,
                protocol => Protocol,
                username => Username,
                peerhost => PeerHost
            }
        )
    ).

do_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    Result = emqx_broker:publish(Msg),
    NChannel = ensure_quota(Result, Channel),
    {ok, NChannel};
do_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    PubRes = emqx_broker:publish(Msg),
    RC = puback_reason_code(PubRes),
    NChannel = ensure_quota(PubRes, Channel),
    handle_out(puback, {PacketId, RC}, NChannel);
do_publish(
    PacketId,
    Msg = #message{qos = ?QOS_2},
    Channel = #channel{clientinfo = ClientInfo, session = Session}
) ->
    case emqx_session:publish(ClientInfo, PacketId, Msg, Session) of
        {ok, PubRes, NSession} ->
            RC = puback_reason_code(PubRes),
            NChannel0 = set_session(NSession, Channel),
            NChannel1 = ensure_timer(await_timer, NChannel0),
            NChannel2 = ensure_quota(PubRes, NChannel1),
            handle_out(pubrec, {PacketId, RC}, NChannel2);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.publish.inuse'),
            handle_out(pubrec, {PacketId, RC}, Channel);
        {error, RC = ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ?SLOG(
                warning,
                #{
                    msg => "dropped_qos2_packet",
                    reason => emqx_reason_codes:name(RC),
                    packet_id => PacketId
                },
                #{topic => Msg#message.topic}
            ),
            ok = emqx_metrics:inc('packets.publish.dropped'),
            handle_out(disconnect, RC, Channel)
    end.

ensure_quota(_, Channel = #channel{quota = undefined}) ->
    Channel;
ensure_quota(PubRes, Channel = #channel{quota = Limiter}) ->
    Cnt = lists:foldl(
        fun
            ({_, _, ok}, N) -> N + 1;
            ({_, _, {ok, I}}, N) -> N + I;
            (_, N) -> N
        end,
        1,
        PubRes
    ),
    case emqx_limiter_container:check(Cnt, ?LIMITER_ROUTING, Limiter) of
        {ok, NLimiter} ->
            Channel#channel{quota = NLimiter};
        {_, Intv, NLimiter} ->
            ensure_timer(quota_timer, Intv, Channel#channel{quota = NLimiter})
    end.

-compile({inline, [puback_reason_code/1]}).
puback_reason_code([]) -> ?RC_NO_MATCHING_SUBSCRIBERS;
puback_reason_code([_ | _]) -> ?RC_SUCCESS.

-compile({inline, [after_message_acked/3]}).
after_message_acked(ClientInfo, Msg, PubAckProps) ->
    ok = emqx_metrics:inc('messages.acked'),
    emqx_hooks:run('message.acked', [
        ClientInfo,
        emqx_message:set_header(puback_props, PubAckProps, Msg)
    ]).

%%--------------------------------------------------------------------
%% Process Subscribe
%%--------------------------------------------------------------------

-compile({inline, [process_subscribe/3]}).
process_subscribe(TopicFilters, SubProps, Channel) ->
    process_subscribe(TopicFilters, SubProps, Channel, []).

process_subscribe([], _SubProps, Channel, Acc) ->
    {lists:reverse(Acc), Channel};
process_subscribe([Topic = {TopicFilter, SubOpts} | More], SubProps, Channel, Acc) ->
    case check_sub_caps(TopicFilter, SubOpts, Channel) of
        ok ->
            {ReasonCode, NChannel} = do_subscribe(
                TopicFilter,
                SubOpts#{sub_props => SubProps},
                Channel
            ),
            process_subscribe(More, SubProps, NChannel, [{Topic, ReasonCode} | Acc]);
        {error, ReasonCode} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_subscribe_topic_filter",
                    reason => emqx_reason_codes:name(ReasonCode)
                },
                #{topic => TopicFilter}
            ),
            process_subscribe(More, SubProps, Channel, [{Topic, ReasonCode} | Acc])
    end.

do_subscribe(
    TopicFilter,
    SubOpts = #{qos := QoS},
    Channel =
        #channel{
            clientinfo = ClientInfo = #{mountpoint := MountPoint},
            session = Session
        }
) ->
    NTopicFilter = emqx_mountpoint:mount(MountPoint, TopicFilter),
    NSubOpts = enrich_subopts(maps:merge(?DEFAULT_SUBOPTS, SubOpts), Channel),
    case emqx_session:subscribe(ClientInfo, NTopicFilter, NSubOpts, Session) of
        {ok, NSession} ->
            {QoS, set_session(NSession, Channel)};
        {error, RC} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_subscribe_topic_filter",
                    reason => emqx_reason_codes:text(RC)
                },
                #{topic => NTopicFilter}
            ),
            {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Process Unsubscribe
%%--------------------------------------------------------------------

-compile({inline, [process_unsubscribe/3]}).
process_unsubscribe(TopicFilters, UnSubProps, Channel) ->
    process_unsubscribe(TopicFilters, UnSubProps, Channel, []).

process_unsubscribe([], _UnSubProps, Channel, Acc) ->
    {lists:reverse(Acc), Channel};
process_unsubscribe([{TopicFilter, SubOpts} | More], UnSubProps, Channel, Acc) ->
    {RC, NChannel} = do_unsubscribe(TopicFilter, SubOpts#{unsub_props => UnSubProps}, Channel),
    process_unsubscribe(More, UnSubProps, NChannel, [RC | Acc]).

do_unsubscribe(
    TopicFilter,
    SubOpts,
    Channel =
        #channel{
            clientinfo = ClientInfo = #{mountpoint := MountPoint},
            session = Session
        }
) ->
    TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
    case emqx_session:unsubscribe(ClientInfo, TopicFilter1, SubOpts, Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, set_session(NSession, Channel)};
        {error, RC} ->
            {RC, Channel}
    end.
%%--------------------------------------------------------------------
%% Process Disconnect
%%--------------------------------------------------------------------

%% MQTT-v5.0: 3.14.4 DISCONNECT Actions
maybe_clean_will_msg(?RC_SUCCESS, Channel) ->
    Channel#channel{will_msg = undefined};
maybe_clean_will_msg(_ReasonCode, Channel) ->
    Channel.

%% MQTT-v5.0: 3.14.2.2.2 Session Expiry Interval
process_disconnect(
    _ReasonCode,
    #{'Session-Expiry-Interval' := Interval},
    Channel = #channel{conninfo = #{expiry_interval := 0}}
) when
    Interval > 0
->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
process_disconnect(ReasonCode, Properties, Channel) ->
    NChannel = maybe_update_expiry_interval(Properties, Channel),
    {ok, {close, disconnect_reason(ReasonCode)}, NChannel}.

maybe_update_expiry_interval(
    #{'Session-Expiry-Interval' := Interval},
    Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo}
) ->
    EI = timer:seconds(Interval),
    OldEI = maps:get(expiry_interval, ConnInfo, 0),
    case OldEI =:= EI of
        true ->
            Channel;
        false ->
            NChannel = Channel#channel{conninfo = ConnInfo#{expiry_interval => EI}},
            ClientID = maps:get(clientid, ClientInfo, undefined),
            %% Check if the client turns off persistence (turning it on is disallowed)
            case EI =:= 0 andalso OldEI > 0 of
                true ->
                    S = emqx_persistent_session:discard(ClientID, NChannel#channel.session),
                    set_session(S, NChannel);
                false ->
                    NChannel
            end
    end;
maybe_update_expiry_interval(_Properties, Channel) ->
    Channel.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()} | {ok, replies(), channel()}.

handle_deliver(
    Delivers,
    Channel = #channel{
        takeover = true,
        pendings = Pendings,
        session = Session,
        clientinfo = #{clientid := ClientId} = ClientInfo
    }
) ->
    %% NOTE: Order is important here. While the takeover is in
    %% progress, the session cannot enqueue messages, since it already
    %% passed on the queue to the new connection in the session state.
    NPendings = lists:append(
        Pendings,
        emqx_session:ignore_local(ClientInfo, maybe_nack(Delivers), ClientId, Session)
    ),
    {ok, Channel#channel{pendings = NPendings}};
handle_deliver(
    Delivers,
    Channel = #channel{
        conn_state = disconnected,
        takeover = false,
        session = Session,
        clientinfo = #{clientid := ClientId} = ClientInfo
    }
) ->
    Delivers1 = maybe_nack(Delivers),
    Delivers2 = emqx_session:ignore_local(ClientInfo, Delivers1, ClientId, Session),
    NSession = emqx_session:enqueue(ClientInfo, Delivers2, Session),
    NChannel = set_session(NSession, Channel),
    %% We consider queued/dropped messages as delivered since they are now in the session state.
    maybe_mark_as_delivered(Session, Delivers),
    {ok, NChannel};
handle_deliver(
    Delivers,
    Channel = #channel{
        session = Session,
        takeover = false,
        clientinfo = #{clientid := ClientId} = ClientInfo
    }
) ->
    case
        emqx_session:deliver(
            ClientInfo,
            emqx_session:ignore_local(ClientInfo, Delivers, ClientId, Session),
            Session
        )
    of
        {ok, Publishes, NSession} ->
            NChannel = set_session(NSession, Channel),
            maybe_mark_as_delivered(NSession, Delivers),
            handle_out(publish, Publishes, ensure_timer(retry_timer, NChannel));
        {ok, NSession} ->
            {ok, set_session(NSession, Channel)}
    end.

%% Nack delivers from shared subscription
maybe_nack(Delivers) ->
    lists:filter(fun not_nacked/1, Delivers).

not_nacked({deliver, _Topic, Msg}) ->
    not (emqx_shared_sub:is_ack_required(Msg) andalso
        (ok == emqx_shared_sub:nack_no_connection(Msg))).

maybe_mark_as_delivered(Session, Delivers) ->
    case emqx_session:info(is_persistent, Session) of
        false ->
            skip;
        true ->
            SessionID = emqx_session:info(id, Session),
            emqx_persistent_session:mark_as_delivered(SessionID, Delivers)
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_out(connack, {?RC_SUCCESS, SP, Props}, Channel = #channel{conninfo = ConnInfo}) ->
    AckProps = run_fold(
        [
            fun enrich_connack_caps/2,
            fun enrich_server_keepalive/2,
            fun enrich_response_information/2,
            fun enrich_assigned_clientid/2
        ],
        Props,
        Channel
    ),
    NAckProps = run_hooks(
        'client.connack',
        [ConnInfo, emqx_reason_codes:name(?RC_SUCCESS)],
        AckProps
    ),

    return_connack(
        ?CONNACK_PACKET(?RC_SUCCESS, SP, NAckProps),
        ensure_keepalive(NAckProps, Channel)
    );
handle_out(connack, ReasonCode, Channel = #channel{conninfo = ConnInfo}) ->
    Reason = emqx_reason_codes:name(ReasonCode),
    AckProps = run_hooks('client.connack', [ConnInfo, Reason], emqx_mqtt_props:new()),
    AckPacket = ?CONNACK_PACKET(
        case maps:get(proto_ver, ConnInfo) of
            ?MQTT_PROTO_V5 -> ReasonCode;
            _ -> emqx_reason_codes:compat(connack, ReasonCode)
        end,
        sp(false),
        AckProps
    ),
    shutdown(Reason, AckPacket, Channel);
%% Optimize?
handle_out(publish, [], Channel) ->
    {ok, Channel};
handle_out(publish, Publishes, Channel) ->
    {Packets, NChannel} = do_deliver(Publishes, Channel),
    {ok, {outgoing, Packets}, NChannel};
handle_out(puback, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), Channel};
handle_out(pubrec, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), Channel};
handle_out(pubrel, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), Channel};
handle_out(pubcomp, {PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), Channel};
handle_out(suback, {PacketId, ReasonCodes}, Channel = ?IS_MQTT_V5) ->
    return_sub_unsub_ack(?SUBACK_PACKET(PacketId, ReasonCodes), Channel);
handle_out(suback, {PacketId, ReasonCodes}, Channel) ->
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    return_sub_unsub_ack(?SUBACK_PACKET(PacketId, ReasonCodes1), Channel);
handle_out(unsuback, {PacketId, ReasonCodes}, Channel = ?IS_MQTT_V5) ->
    return_sub_unsub_ack(?UNSUBACK_PACKET(PacketId, ReasonCodes), Channel);
handle_out(unsuback, {PacketId, _ReasonCodes}, Channel) ->
    return_sub_unsub_ack(?UNSUBACK_PACKET(PacketId), Channel);
handle_out(disconnect, ReasonCode, Channel) when is_integer(ReasonCode) ->
    ReasonName = disconnect_reason(ReasonCode),
    handle_out(disconnect, {ReasonCode, ReasonName}, Channel);
handle_out(disconnect, {ReasonCode, ReasonName}, Channel = ?IS_MQTT_V5) ->
    Packet = ?DISCONNECT_PACKET(ReasonCode),
    {ok, [{outgoing, Packet}, {close, ReasonName}], Channel};
handle_out(disconnect, {_ReasonCode, ReasonName}, Channel) ->
    {ok, {close, ReasonName}, Channel};
handle_out(auth, {ReasonCode, Properties}, Channel) ->
    {ok, ?AUTH_PACKET(ReasonCode, Properties), Channel};
handle_out(Type, Data, Channel) ->
    ?SLOG(error, #{msg => "unexpected_outgoing", type => Type, data => Data}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Return ConnAck
%%--------------------------------------------------------------------

return_connack(AckPacket, Channel) ->
    Replies = [{event, connected}, {connack, AckPacket}],
    case maybe_resume_session(Channel) of
        ignore ->
            {ok, Replies, Channel};
        {ok, Publishes, NSession} ->
            NChannel0 = Channel#channel{
                resuming = false,
                pendings = []
            },
            NChannel1 = set_session(NSession, NChannel0),
            {Packets, NChannel2} = do_deliver(Publishes, NChannel1),
            Outgoing = [{outgoing, Packets} || length(Packets) > 0],
            {ok, Replies ++ Outgoing, NChannel2}
    end.

%%--------------------------------------------------------------------
%% Deliver publish: broker -> client
%%--------------------------------------------------------------------

%% return list(emqx_types:packet())
do_deliver({pubrel, PacketId}, Channel) ->
    {[?PUBREL_PACKET(PacketId, ?RC_SUCCESS)], Channel};
do_deliver(
    {PacketId, Msg},
    Channel = #channel{
        clientinfo =
            ClientInfo =
                #{mountpoint := MountPoint}
    }
) ->
    ok = emqx_metrics:inc('messages.delivered'),
    Msg1 = emqx_hooks:run_fold(
        'message.delivered',
        [ClientInfo],
        emqx_message:update_expiry(Msg)
    ),
    Msg2 = emqx_mountpoint:unmount(MountPoint, Msg1),
    Packet = emqx_message:to_packet(PacketId, Msg2),
    {NPacket, NChannel} = packing_alias(Packet, Channel),
    {[NPacket], NChannel};
do_deliver([Publish], Channel) ->
    do_deliver(Publish, Channel);
do_deliver(Publishes, Channel) when is_list(Publishes) ->
    {Packets, NChannel} =
        lists:foldl(
            fun(Publish, {Acc, Chann}) ->
                {Packets, NChann} = do_deliver(Publish, Chann),
                {Packets ++ Acc, NChann}
            end,
            {[], Channel},
            Publishes
        ),
    {lists:reverse(Packets), NChannel}.

%%--------------------------------------------------------------------
%% Handle out suback
%%--------------------------------------------------------------------

return_sub_unsub_ack(Packet, Channel) ->
    {ok, [{outgoing, Packet}, {event, updated}], Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), emqx_types:packet(), channel()}.
handle_call(
    kick,
    Channel = #channel{
        conn_state = ConnState,
        will_msg = WillMsg,
        conninfo = #{proto_ver := ProtoVer}
    }
) ->
    (WillMsg =/= undefined) andalso publish_will_msg(WillMsg),
    Channel1 =
        case ConnState of
            connected -> ensure_disconnected(kicked, Channel);
            _ -> Channel
        end,
    case ProtoVer == ?MQTT_PROTO_V5 andalso ConnState == connected of
        true ->
            shutdown(
                kicked,
                ok,
                ?DISCONNECT_PACKET(?RC_ADMINISTRATIVE_ACTION),
                Channel1
            );
        _ ->
            shutdown(kicked, ok, Channel1)
    end;
handle_call(discard, Channel) ->
    disconnect_and_shutdown(discarded, ok, Channel);
%% Session Takeover
handle_call({takeover, 'begin'}, Channel = #channel{session = Session}) ->
    reply(Session, Channel#channel{takeover = true});
handle_call(
    {takeover, 'end'},
    Channel = #channel{
        session = Session,
        pendings = Pendings
    }
) ->
    ok = emqx_session:takeover(Session),
    %% TODO: Should not drain deliver here (side effect)
    Delivers = emqx_misc:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    disconnect_and_shutdown(takenover, AllPendings, Channel);
handle_call(list_authz_cache, Channel) ->
    {reply, emqx_authz_cache:list_authz_cache(), Channel};
handle_call({quota, Bucket}, #channel{quota = Quota} = Channel) ->
    Quota2 = emqx_limiter_container:update_by_name(message_routing, Bucket, Quota),
    reply(ok, Channel#channel{quota = Quota2});
handle_call(
    {keepalive, Interval},
    Channel = #channel{
        keepalive = KeepAlive,
        conninfo = ConnInfo
    }
) ->
    ClientId = info(clientid, Channel),
    NKeepalive = emqx_keepalive:set(interval, Interval * 1000, KeepAlive),
    NConnInfo = maps:put(keepalive, Interval, ConnInfo),
    NChannel = Channel#channel{keepalive = NKeepalive, conninfo = NConnInfo},
    SockInfo = maps:get(sockinfo, emqx_cm:get_chan_info(ClientId), #{}),
    ChanInfo1 = info(NChannel),
    emqx_cm:set_chan_info(ClientId, ChanInfo1#{sockinfo => SockInfo}),
    reply(ok, reset_timer(alive_timer, NChannel));
handle_call(Req, Channel) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.

handle_info({subscribe, TopicFilters}, Channel) ->
    {_, NChannel} = lists:foldl(
        fun({TopicFilter, SubOpts}, {_, ChannelAcc}) ->
            do_subscribe(TopicFilter, SubOpts, ChannelAcc)
        end,
        {[], Channel},
        parse_topic_filters(TopicFilters)
    ),
    {ok, NChannel};
handle_info({unsubscribe, TopicFilters}, Channel) ->
    {_RC, NChannel} = process_unsubscribe(TopicFilters, #{}, Channel),
    {ok, NChannel};
handle_info({sock_closed, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);
handle_info({sock_closed, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel =
        #channel{
            conn_state = ConnState,
            clientinfo = ClientInfo = #{zone := Zone}
        }
) when
    ConnState =:= connected orelse ConnState =:= reauthenticating
->
    emqx_config:get_zone_conf(Zone, [flapping_detect, enable]) andalso
        emqx_flapping:detect(ClientInfo),
    Channel1 = ensure_disconnected(Reason, maybe_publish_will_msg(Channel)),
    case maybe_shutdown(Reason, Channel1) of
        {ok, Channel2} -> {ok, {event, disconnected}, Channel2};
        Shutdown -> Shutdown
    end;
handle_info({sock_closed, Reason}, Channel = #channel{conn_state = disconnected}) ->
    ?SLOG(error, #{msg => "unexpected_sock_close", reason => Reason}),
    {ok, Channel};
handle_info(clean_authz_cache, Channel) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    {ok, Channel};
handle_info(die_if_test = Info, Channel) ->
    die_if_test_compiled(),
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {ok, Channel};
handle_info(Info, Channel) ->
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {ok, Channel}.

-ifdef(TEST).

-spec die_if_test_compiled() -> no_return().
die_if_test_compiled() ->
    exit(normal).

-else.

die_if_test_compiled() ->
    ok.

-endif.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_timeout(
    _TRef,
    {keepalive, _StatVal},
    Channel = #channel{keepalive = undefined}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, _StatVal},
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, StatVal},
    Channel = #channel{keepalive = Keepalive}
) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, ?RC_KEEP_ALIVE_TIMEOUT, Channel)
    end;
handle_timeout(
    _TRef,
    retry_delivery,
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    retry_delivery,
    Channel = #channel{session = Session, clientinfo = ClientInfo}
) ->
    case emqx_session:retry(ClientInfo, Session) of
        {ok, NSession} ->
            {ok, clean_timer(retry_timer, set_session(NSession, Channel))};
        {ok, Publishes, Timeout, NSession} ->
            NChannel = set_session(NSession, Channel),
            handle_out(publish, Publishes, reset_timer(retry_timer, Timeout, NChannel))
    end;
handle_timeout(
    _TRef,
    expire_awaiting_rel,
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    expire_awaiting_rel,
    Channel = #channel{session = Session, clientinfo = ClientInfo}
) ->
    case emqx_session:expire(ClientInfo, awaiting_rel, Session) of
        {ok, NSession} ->
            {ok, clean_timer(await_timer, set_session(NSession, Channel))};
        {ok, Timeout, NSession} ->
            {ok, reset_timer(await_timer, Timeout, set_session(NSession, Channel))}
    end;
handle_timeout(_TRef, expire_session, Channel) ->
    shutdown(expired, Channel);
handle_timeout(_TRef, will_message, Channel = #channel{will_msg = WillMsg}) ->
    (WillMsg =/= undefined) andalso publish_will_msg(WillMsg),
    {ok, clean_timer(will_timer, Channel#channel{will_msg = undefined})};
handle_timeout(
    _TRef,
    expire_quota_limit,
    #channel{quota = Quota} = Channel
) ->
    case emqx_limiter_container:retry(?LIMITER_ROUTING, Quota) of
        {_, Intv, Quota2} ->
            Channel2 = ensure_timer(quota_timer, Intv, Channel#channel{quota = Quota2}),
            {ok, Channel2};
        {_, Quota2} ->
            {ok, clean_timer(quota_timer, Channel#channel{quota = Quota2})}
    end;
handle_timeout(_TRef, Msg, Channel) ->
    ?SLOG(error, #{msg => "unexpected_timeout", timeout_msg => Msg}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Ensure timers
%%--------------------------------------------------------------------

ensure_timer([Name], Channel) ->
    ensure_timer(Name, Channel);
ensure_timer([Name | Rest], Channel) ->
    ensure_timer(Rest, ensure_timer(Name, Channel));
ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    Msg = maps:get(Name, ?TIMER_TABLE),
    TRef = emqx_misc:start_timer(Time, Msg),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

reset_timer(Name, Time, Channel) ->
    ensure_timer(Name, Time, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(alive_timer, #channel{keepalive = KeepAlive}) ->
    emqx_keepalive:info(interval, KeepAlive);
interval(retry_timer, #channel{session = Session}) ->
    emqx_session:info(retry_interval, Session);
interval(await_timer, #channel{session = Session}) ->
    emqx_session:info(await_rel_timeout, Session);
interval(expire_timer, #channel{conninfo = ConnInfo}) ->
    maps:get(expiry_interval, ConnInfo);
interval(will_timer, #channel{will_msg = WillMsg}) ->
    timer:seconds(will_delay_interval(WillMsg)).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

-spec terminate(any(), channel()) -> ok.
terminate(_, #channel{conn_state = idle}) ->
    ok;
terminate(normal, Channel) ->
    run_terminate_hook(normal, Channel);
terminate({shutdown, kicked}, Channel) ->
    run_terminate_hook(kicked, Channel);
terminate({shutdown, Reason}, Channel) when
    Reason =:= discarded;
    Reason =:= takenover
->
    run_terminate_hook(Reason, Channel);
terminate(Reason, Channel = #channel{will_msg = WillMsg}) ->
    (WillMsg =/= undefined) andalso publish_will_msg(WillMsg),
    (Reason =:= expired) andalso persist_if_session(Channel),
    run_terminate_hook(Reason, Channel).

persist_if_session(#channel{session = Session} = Channel) ->
    case emqx_session:is_session(Session) of
        true ->
            _ = emqx_persistent_session:persist(
                Channel#channel.clientinfo,
                Channel#channel.conninfo,
                Channel#channel.session
            ),
            ok;
        false ->
            ok
    end.

run_terminate_hook(_Reason, #channel{session = undefined}) ->
    ok;
run_terminate_hook(Reason, #channel{clientinfo = ClientInfo, session = Session}) ->
    emqx_session:terminate(ClientInfo, Reason, Session).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------
overload_protection(_, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_olp:backoff(Zone),
    ok.

%%--------------------------------------------------------------------
%% Enrich MQTT Connect Info

enrich_conninfo(
    ConnPkt = #mqtt_packet_connect{
        proto_name = ProtoName,
        proto_ver = ProtoVer,
        clean_start = CleanStart,
        keepalive = Keepalive,
        properties = ConnProps,
        clientid = ClientId,
        username = Username
    },
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = #{zone := Zone}
    }
) ->
    ExpiryInterval = expiry_interval(Zone, ConnPkt),
    NConnInfo = ConnInfo#{
        proto_name => ProtoName,
        proto_ver => ProtoVer,
        clean_start => CleanStart,
        keepalive => Keepalive,
        clientid => ClientId,
        username => Username,
        conn_props => ConnProps,
        expiry_interval => ExpiryInterval,
        receive_maximum => receive_maximum(Zone, ConnProps)
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

%% If the Session Expiry Interval is absent the value 0 is used.
expiry_interval(_, #mqtt_packet_connect{
    proto_ver = ?MQTT_PROTO_V5,
    properties = ConnProps
}) ->
    timer:seconds(emqx_mqtt_props:get('Session-Expiry-Interval', ConnProps, 0));
expiry_interval(Zone, #mqtt_packet_connect{clean_start = false}) ->
    get_mqtt_conf(Zone, session_expiry_interval);
expiry_interval(_, #mqtt_packet_connect{clean_start = true}) ->
    0.

receive_maximum(Zone, ConnProps) ->
    MaxInflightConfig =
        case get_mqtt_conf(Zone, max_inflight) of
            0 -> ?RECEIVE_MAXIMUM_LIMIT;
            N -> N
        end,
    %% Received might be zero which should be a protocol error
    %% we do not validate MQTT properties here
    %% it is to be caught later
    Received = emqx_mqtt_props:get('Receive-Maximum', ConnProps, MaxInflightConfig),
    erlang:min(Received, MaxInflightConfig).

%%--------------------------------------------------------------------
%% Run Connect Hooks

run_conn_hooks(ConnPkt, Channel = #channel{conninfo = ConnInfo}) ->
    ConnProps = emqx_packet:info(properties, ConnPkt),
    case run_hooks('client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        NConnProps -> {ok, emqx_packet:set_props(NConnProps, ConnPkt), Channel}
    end.

%%--------------------------------------------------------------------
%% Check Connect Packet

check_connect(ConnPkt, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_packet:check(ConnPkt, emqx_mqtt_caps:get_caps(Zone)).

%%--------------------------------------------------------------------
%% Enrich Client Info

enrich_client(ConnPkt, Channel = #channel{clientinfo = ClientInfo}) ->
    Pipe = pipeline(
        [
            fun set_username/2,
            fun set_bridge_mode/2,
            fun maybe_username_as_clientid/2,
            fun maybe_assign_clientid/2,
            fun fix_mountpoint/2
        ],
        ConnPkt,
        ClientInfo
    ),
    case Pipe of
        {ok, NConnPkt, NClientInfo} ->
            {ok, NConnPkt, Channel#channel{clientinfo = NClientInfo}};
        {error, ReasonCode, NClientInfo} ->
            {error, ReasonCode, Channel#channel{clientinfo = NClientInfo}}
    end.

set_username(
    #mqtt_packet_connect{username = Username},
    ClientInfo = #{username := undefined}
) ->
    {ok, ClientInfo#{username => Username}};
set_username(_ConnPkt, ClientInfo) ->
    {ok, ClientInfo}.

set_bridge_mode(#mqtt_packet_connect{is_bridge = true}, ClientInfo) ->
    {ok, ClientInfo#{is_bridge => true}};
set_bridge_mode(_ConnPkt, _ClientInfo) ->
    ok.

maybe_username_as_clientid(_ConnPkt, ClientInfo = #{username := undefined}) ->
    {ok, ClientInfo};
maybe_username_as_clientid(
    _ConnPkt,
    ClientInfo = #{
        zone := Zone,
        username := Username
    }
) ->
    case get_mqtt_conf(Zone, use_username_as_clientid) of
        true when Username =/= <<>> -> {ok, ClientInfo#{clientid => Username}};
        true -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID, ClientInfo};
        false -> ok
    end.

maybe_assign_clientid(_ConnPkt, ClientInfo = #{clientid := ClientId}) when
    ClientId /= undefined
->
    {ok, ClientInfo};
maybe_assign_clientid(#mqtt_packet_connect{clientid = <<>>}, ClientInfo) ->
    %% Generate a rand clientId
    {ok, ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())}};
maybe_assign_clientid(#mqtt_packet_connect{clientid = ClientId}, ClientInfo) ->
    {ok, ClientInfo#{clientid => ClientId}}.

fix_mountpoint(_ConnPkt, #{mountpoint := undefined}) ->
    ok;
fix_mountpoint(_ConnPkt, ClientInfo = #{mountpoint := MountPoint}) ->
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := MountPoint1}}.

%%--------------------------------------------------------------------
%% Set log metadata

set_log_meta(_ConnPkt, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId).

%%--------------------------------------------------------------------
%% Check banned

check_banned(_ConnPkt, #channel{clientinfo = ClientInfo}) ->
    case emqx_banned:check(ClientInfo) of
        true -> {error, ?RC_BANNED};
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Authenticate

authenticate(
    ?CONNECT_PACKET(
        #mqtt_packet_connect{
            proto_ver = ?MQTT_PROTO_V5,
            properties = #{'Authentication-Method' := AuthMethod} = Properties
        }
    ),
    #channel{
        clientinfo = ClientInfo,
        auth_cache = AuthCache
    } = Channel
) ->
    AuthData = emqx_mqtt_props:get('Authentication-Data', Properties, undefined),
    do_authenticate(
        ClientInfo#{
            auth_method => AuthMethod,
            auth_data => AuthData,
            auth_cache => AuthCache
        },
        Channel
    );
authenticate(
    ?CONNECT_PACKET(#mqtt_packet_connect{password = Password}),
    #channel{clientinfo = ClientInfo} = Channel
) ->
    do_authenticate(ClientInfo#{password => Password}, Channel);
authenticate(
    ?AUTH_PACKET(_, #{'Authentication-Method' := AuthMethod} = Properties),
    #channel{
        clientinfo = ClientInfo,
        conninfo = #{conn_props := ConnProps},
        auth_cache = AuthCache
    } = Channel
) ->
    case emqx_mqtt_props:get('Authentication-Method', ConnProps, undefined) of
        AuthMethod ->
            AuthData = emqx_mqtt_props:get('Authentication-Data', Properties, undefined),
            do_authenticate(
                ClientInfo#{
                    auth_method => AuthMethod,
                    auth_data => AuthData,
                    auth_cache => AuthCache
                },
                Channel
            );
        _ ->
            {error, ?RC_BAD_AUTHENTICATION_METHOD}
    end.

do_authenticate(
    #{auth_method := AuthMethod} = Credential,
    #channel{clientinfo = ClientInfo} = Channel
) ->
    Properties = #{'Authentication-Method' => AuthMethod},
    case emqx_access_control:authenticate(Credential) of
        {ok, AuthResult} ->
            {ok, Properties, Channel#channel{
                clientinfo = merge_auth_result(ClientInfo, AuthResult),
                auth_cache = #{}
            }};
        {ok, AuthResult, AuthData} ->
            {ok, Properties#{'Authentication-Data' => AuthData}, Channel#channel{
                clientinfo = merge_auth_result(ClientInfo, AuthResult),
                auth_cache = #{}
            }};
        {continue, AuthCache} ->
            {continue, Properties, Channel#channel{auth_cache = AuthCache}};
        {continue, AuthData, AuthCache} ->
            {continue, Properties#{'Authentication-Data' => AuthData}, Channel#channel{
                auth_cache = AuthCache
            }};
        {error, Reason} ->
            {error, emqx_reason_codes:connack_error(Reason)}
    end;
do_authenticate(Credential, #channel{clientinfo = ClientInfo} = Channel) ->
    case emqx_access_control:authenticate(Credential) of
        {ok, AuthResult} ->
            {ok, #{}, Channel#channel{clientinfo = merge_auth_result(ClientInfo, AuthResult)}};
        {error, Reason} ->
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

merge_auth_result(ClientInfo, AuthResult) when is_map(ClientInfo) andalso is_map(AuthResult) ->
    IsSuperuser = maps:get(is_superuser, AuthResult, false),
    maps:merge(ClientInfo, AuthResult#{is_superuser => IsSuperuser}).

%%--------------------------------------------------------------------
%% Process Topic Alias

process_alias(
    Packet = #mqtt_packet{
        variable =
            #mqtt_packet_publish{
                topic_name = <<>>,
                properties = #{'Topic-Alias' := AliasId}
            } = Publish
    },
    Channel = ?IS_MQTT_V5 = #channel{topic_aliases = TopicAliases}
) ->
    case find_alias(inbound, AliasId, TopicAliases) of
        {ok, Topic} ->
            NPublish = Publish#mqtt_packet_publish{topic_name = Topic},
            {ok, Packet#mqtt_packet{variable = NPublish}, Channel};
        error ->
            {error, ?RC_PROTOCOL_ERROR}
    end;
process_alias(
    #mqtt_packet{
        variable = #mqtt_packet_publish{
            topic_name = Topic,
            properties = #{'Topic-Alias' := AliasId}
        }
    },
    Channel = ?IS_MQTT_V5 = #channel{topic_aliases = TopicAliases}
) ->
    NTopicAliases = save_alias(inbound, AliasId, Topic, TopicAliases),
    {ok, Channel#channel{topic_aliases = NTopicAliases}};
process_alias(_Packet, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Packing Topic Alias

packing_alias(
    Packet = #mqtt_packet{
        variable =
            #mqtt_packet_publish{
                topic_name = Topic,
                properties = Prop
            } = Publish
    },
    Channel =
        ?IS_MQTT_V5 = #channel{
            topic_aliases = TopicAliases,
            alias_maximum = Limits
        }
) ->
    case find_alias(outbound, Topic, TopicAliases) of
        {ok, AliasId} ->
            NPublish = Publish#mqtt_packet_publish{
                topic_name = <<>>,
                properties = maps:merge(Prop, #{'Topic-Alias' => AliasId})
            },
            {Packet#mqtt_packet{variable = NPublish}, Channel};
        error ->
            #{outbound := Aliases} = TopicAliases,
            AliasId = maps:size(Aliases) + 1,
            case
                (Limits =:= undefined) orelse
                    (AliasId =< maps:get(outbound, Limits, 0))
            of
                true ->
                    NTopicAliases = save_alias(outbound, AliasId, Topic, TopicAliases),
                    NChannel = Channel#channel{topic_aliases = NTopicAliases},
                    NPublish = Publish#mqtt_packet_publish{
                        topic_name = Topic,
                        properties = maps:merge(Prop, #{'Topic-Alias' => AliasId})
                    },
                    {Packet#mqtt_packet{variable = NPublish}, NChannel};
                false ->
                    {Packet, Channel}
            end
    end;
packing_alias(Packet, Channel) ->
    {Packet, Channel}.

%%--------------------------------------------------------------------
%% Check quota state

check_quota_exceeded(_, #channel{timers = Timers}) ->
    case maps:get(quota_timer, Timers, undefined) of
        undefined -> ok;
        _ -> {error, ?RC_QUOTA_EXCEEDED}
    end.

%%--------------------------------------------------------------------
%% Check Pub Alias

check_pub_alias(
    #mqtt_packet{
        variable = #mqtt_packet_publish{
            properties = #{'Topic-Alias' := AliasId}
        }
    },
    #channel{alias_maximum = Limits}
) ->
    case
        (Limits =:= undefined) orelse
            (AliasId =< maps:get(inbound, Limits, ?MAX_TOPIC_AlIAS))
    of
        true -> ok;
        false -> {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
check_pub_alias(_Packet, _Channel) ->
    ok.

%%--------------------------------------------------------------------
%% Check Pub Authorization

check_pub_authz(
    #mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
    #channel{clientinfo = ClientInfo}
) ->
    case emqx_access_control:authorize(ClientInfo, publish, Topic) of
        allow -> ok;
        deny -> {error, ?RC_NOT_AUTHORIZED}
    end.

%%--------------------------------------------------------------------
%% Check Pub Caps

check_pub_caps(
    #mqtt_packet{
        header = #mqtt_packet_header{
            qos = QoS,
            retain = Retain
        },
        variable = #mqtt_packet_publish{topic_name = Topic}
    },
    #channel{clientinfo = #{zone := Zone}}
) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain, topic => Topic}).

%%--------------------------------------------------------------------
%% Check Sub Authorization

check_sub_authzs(TopicFilters, Channel) ->
    check_sub_authzs(TopicFilters, Channel, []).

check_sub_authzs(
    [TopicFilter = {Topic, _} | More],
    Channel = #channel{clientinfo = ClientInfo},
    Acc
) ->
    case emqx_access_control:authorize(ClientInfo, subscribe, Topic) of
        allow ->
            check_sub_authzs(More, Channel, [{TopicFilter, 0} | Acc]);
        deny ->
            check_sub_authzs(More, Channel, [{TopicFilter, ?RC_NOT_AUTHORIZED} | Acc])
    end;
check_sub_authzs([], _Channel, Acc) ->
    lists:reverse(Acc).

%%--------------------------------------------------------------------
%% Check Sub Caps

check_sub_caps(TopicFilter, SubOpts, #channel{clientinfo = ClientInfo}) ->
    emqx_mqtt_caps:check_sub(ClientInfo, TopicFilter, SubOpts).

%%--------------------------------------------------------------------
%% Enrich SubId

put_subid_in_subopts(#{'Subscription-Identifier' := SubId}, TopicFilters) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
put_subid_in_subopts(_Properties, TopicFilters) ->
    TopicFilters.

%%--------------------------------------------------------------------
%% Enrich SubOpts

enrich_subopts(SubOpts, _Channel = ?IS_MQTT_V5) ->
    SubOpts;
enrich_subopts(SubOpts, #channel{clientinfo = #{zone := Zone, is_bridge := IsBridge}}) ->
    NL = flag(get_mqtt_conf(Zone, ignore_loop_deliver)),
    SubOpts#{rap => flag(IsBridge), nl => NL}.

%%--------------------------------------------------------------------
%% Enrich ConnAck Caps

enrich_connack_caps(
    AckProps,
    ?IS_MQTT_V5 = #channel{
        clientinfo = #{
            zone := Zone
        }
    }
) ->
    #{
        max_packet_size := MaxPktSize,
        max_qos_allowed := MaxQoS,
        retain_available := Retain,
        max_topic_alias := MaxAlias,
        shared_subscription := Shared,
        wildcard_subscription := Wildcard
    } = emqx_mqtt_caps:get_caps(Zone),
    NAckProps = AckProps#{
        'Retain-Available' => flag(Retain),
        'Maximum-Packet-Size' => MaxPktSize,
        'Topic-Alias-Maximum' => MaxAlias,
        'Wildcard-Subscription-Available' => flag(Wildcard),
        'Subscription-Identifier-Available' => 1,
        'Shared-Subscription-Available' => flag(Shared)
    },
    %% MQTT 5.0 - 3.2.2.3.4:
    %% It is a Protocol Error to include Maximum QoS more than once,
    %% or to have a value other than 0 or 1. If the Maximum QoS is absent,
    %% the Client uses a Maximum QoS of 2.
    case MaxQoS =:= 2 of
        true -> NAckProps;
        _ -> NAckProps#{'Maximum-QoS' => MaxQoS}
    end;
enrich_connack_caps(AckProps, _Channel) ->
    AckProps.

%%--------------------------------------------------------------------
%% Enrich server keepalive

enrich_server_keepalive(AckProps, ?IS_MQTT_V5 = #channel{clientinfo = #{zone := Zone}}) ->
    case get_mqtt_conf(Zone, server_keepalive) of
        disabled -> AckProps;
        Keepalive -> AckProps#{'Server-Keep-Alive' => Keepalive}
    end;
enrich_server_keepalive(AckProps, _Channel) ->
    AckProps.

%%--------------------------------------------------------------------
%% Enrich response information

enrich_response_information(AckProps, #channel{
    conninfo = #{conn_props := ConnProps},
    clientinfo = #{zone := Zone}
}) ->
    case emqx_mqtt_props:get('Request-Response-Information', ConnProps, 0) of
        0 ->
            AckProps;
        1 ->
            AckProps#{
                'Response-Information' =>
                    case get_mqtt_conf(Zone, response_information, "") of
                        "" -> undefined;
                        RspInfo -> RspInfo
                    end
            }
    end.

%%--------------------------------------------------------------------
%% Enrich Assigned ClientId

enrich_assigned_clientid(AckProps, #channel{
    conninfo = ConnInfo,
    clientinfo = #{clientid := ClientId}
}) ->
    case maps:get(clientid, ConnInfo) of
        %% Original ClientId is null.
        <<>> ->
            AckProps#{'Assigned-Client-Identifier' => ClientId};
        _Origin ->
            AckProps
    end.

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }.

%%--------------------------------------------------------------------
%% Init Alias Maximum

init_alias_maximum(
    #mqtt_packet_connect{
        proto_ver = ?MQTT_PROTO_V5,
        properties = Properties
    },
    #{zone := Zone} = _ClientInfo
) ->
    #{
        outbound => emqx_mqtt_props:get('Topic-Alias-Maximum', Properties, 0),
        inbound => maps:get(max_topic_alias, emqx_mqtt_caps:get_caps(Zone))
    };
init_alias_maximum(_ConnPkt, _ClientInfo) ->
    undefined.

%%--------------------------------------------------------------------
%% Ensure Keepalive

%% MQTT 5
ensure_keepalive(#{'Server-Keep-Alive' := Interval}, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(Interval, Channel#channel{conninfo = ConnInfo#{keepalive => Interval}});
%% MQTT 3,4
ensure_keepalive(_AckProps, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_keepalive_timer(0, Channel) ->
    Channel;
ensure_keepalive_timer(disabled, Channel) ->
    Channel;
ensure_keepalive_timer(Interval, Channel = #channel{clientinfo = #{zone := Zone}}) ->
    Backoff = get_mqtt_conf(Zone, keepalive_backoff),
    RecvOct = emqx_pd:get_counter(incoming_bytes),
    Keepalive = emqx_keepalive:init(RecvOct, round(timer:seconds(Interval) * Backoff)),
    ensure_timer(alive_timer, Channel#channel{keepalive = Keepalive}).

clear_keepalive(Channel = #channel{timers = Timers}) ->
    case maps:get(alive_timer, Timers, undefined) of
        undefined ->
            Channel;
        TRef ->
            emqx_misc:cancel_timer(TRef),
            Channel#channel{timers = maps:without([alive_timer], Timers)}
    end.
%%--------------------------------------------------------------------
%% Maybe Resume Session

maybe_resume_session(#channel{resuming = false}) ->
    ignore;
maybe_resume_session(#channel{
    session = Session,
    resuming = true,
    pendings = Pendings,
    clientinfo = #{clientid := ClientId} = ClientInfo
}) ->
    {ok, Publishes, Session1} = emqx_session:replay(ClientInfo, Session),
    %% We consider queued/dropped messages as delivered since they are now in the session state.
    emqx_persistent_session:mark_as_delivered(ClientId, Pendings),
    case emqx_session:deliver(ClientInfo, Pendings, Session1) of
        {ok, Session2} ->
            {ok, Publishes, Session2};
        {ok, More, Session2} ->
            {ok, lists:append(Publishes, More), Session2}
    end.

%%--------------------------------------------------------------------
%% Maybe Shutdown the Channel

maybe_shutdown(Reason, Channel = #channel{conninfo = ConnInfo}) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?UINT_MAX ->
            {ok, Channel};
        I when I > 0 ->
            {ok, ensure_timer(expire_timer, I, Channel)};
        _ ->
            shutdown(Reason, Channel)
    end.

%%--------------------------------------------------------------------
%% Parse Topic Filters

-compile({inline, [parse_topic_filters/1]}).
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    ChanPid = self(),
    emqx_cm:mark_channel_disconnected(ChanPid),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

%%--------------------------------------------------------------------
%% Maybe Publish will msg

maybe_publish_will_msg(Channel = #channel{will_msg = undefined}) ->
    Channel;
maybe_publish_will_msg(Channel = #channel{will_msg = WillMsg}) ->
    case will_delay_interval(WillMsg) of
        0 ->
            ok = publish_will_msg(WillMsg),
            Channel#channel{will_msg = undefined};
        I ->
            ensure_timer(will_timer, timer:seconds(I), Channel)
    end.

will_delay_interval(WillMsg) ->
    maps:get(
        'Will-Delay-Interval',
        emqx_message:get_header(properties, WillMsg, #{}),
        0
    ).

publish_will_msg(Msg) ->
    _ = emqx_broker:publish(Msg),
    ok.

%%--------------------------------------------------------------------
%% Disconnect Reason

disconnect_reason(?RC_SUCCESS) -> normal;
disconnect_reason(ReasonCode) -> emqx_reason_codes:name(ReasonCode).

reason_code(takenover) -> ?RC_SESSION_TAKEN_OVER;
reason_code(discarded) -> ?RC_SESSION_TAKEN_OVER.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [run_hooks/2, run_hooks/3]}).
run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name),
    emqx_hooks:run_fold(Name, Args, Acc).

-compile({inline, [find_alias/3, save_alias/4]}).

find_alias(_, _, undefined) -> error;
find_alias(inbound, AliasId, _TopicAliases = #{inbound := Aliases}) -> maps:find(AliasId, Aliases);
find_alias(outbound, Topic, _TopicAliases = #{outbound := Aliases}) -> maps:find(Topic, Aliases).

save_alias(_, _, _, undefined) ->
    false;
save_alias(inbound, AliasId, Topic, TopicAliases = #{inbound := Aliases}) ->
    NAliases = maps:put(AliasId, Topic, Aliases),
    TopicAliases#{inbound => NAliases};
save_alias(outbound, AliasId, Topic, TopicAliases = #{outbound := Aliases}) ->
    NAliases = maps:put(Topic, AliasId, Aliases),
    TopicAliases#{outbound => NAliases}.

-compile({inline, [reply/2, shutdown/2, shutdown/3, sp/1, flag/1]}).

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

shutdown(success, Channel) ->
    shutdown(normal, Channel);
shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(success, Reply, Channel) ->
    shutdown(normal, Reply, Channel);
shutdown(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

shutdown(success, Reply, Packet, Channel) ->
    shutdown(normal, Reply, Packet, Channel);
shutdown(Reason, Reply, Packet, Channel) ->
    {shutdown, Reason, Reply, Packet, Channel}.

disconnect_and_shutdown(
    Reason,
    Reply,
    Channel =
        ?IS_MQTT_V5 =
        #channel{conn_state = ConnState}
) when
    ConnState =:= connected orelse ConnState =:= reauthenticating
->
    shutdown(Reason, Reply, ?DISCONNECT_PACKET(reason_code(Reason)), Channel);
disconnect_and_shutdown(Reason, Reply, Channel) ->
    shutdown(Reason, Reply, Channel).

sp(true) -> 1;
sp(false) -> 0.

flag(true) -> 1;
flag(false) -> 0.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).

get_mqtt_conf(Zone, Key, Default) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key], Default).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, Channel) ->
    Pos = emqx_misc:index_of(Name, record_info(fields, channel)),
    setelement(Pos + 1, Channel, Value).
