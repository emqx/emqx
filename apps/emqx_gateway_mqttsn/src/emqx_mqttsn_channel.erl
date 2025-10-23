%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mqttsn_channel).

-behaviour(emqx_gateway_channel).

-include("emqx_mqttsn.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/types.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_access_control.hrl").
-include_lib("emqx/include/logger.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% API
-export([
    info/1,
    info/2,
    stats/1
]).

-export([
    init/2,
    handle_in/2,
    handle_frame_error/2,
    handle_out/3,
    handle_deliver/2,
    handle_timeout/3,
    terminate/2,
    set_conn_state/2
]).

-export([
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

-record(channel, {
    %% Context
    ctx :: emqx_gateway_ctx:context(),
    %% Gateway Id
    gateway_id :: integer(),
    %% Enable negative_qos
    enable_negative_qos :: boolean(),
    %% MQTT-SN Connection Info
    conninfo :: emqx_types:conninfo(),
    %% MQTT-SN Client Info
    clientinfo :: emqx_types:clientinfo(),
    %% Session
    session :: emqx_mqttsn_session:session() | undefined,
    %% Keepalive
    keepalive :: emqx_keepalive:keepalive() | undefined,
    %% Will Msg
    will_msg :: emqx_types:message() | undefined,
    %% ClientInfo override specs
    clientinfo_override :: map(),
    %% Connection State
    conn_state :: conn_state(),
    %% Inflight register message queue
    register_inflight :: option(term()),
    %% Topics list for awaiting to register to client
    register_awaiting_queue :: list(),
    %% Duration for asleep
    asleep_timer_duration :: integer() | undefined,
    %% Timer
    timers :: #{atom() => disable | undefined | reference()},
    %%% Takeover
    takeover :: boolean(),
    %% Resume
    resuming :: boolean(),
    %% Pending delivers when takeovering
    pendings :: list()
}).

-type channel() :: #channel{}.

-type conn_state() ::
    idle
    | connecting
    | connected
    | asleep
    | awake
    | disconnected.

-type reply() ::
    {outgoing, mqtt_sn_message()}
    | {outgoing, [mqtt_sn_message()]}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(DEFAULT_OVERRIDE, #{
    clientid => <<"${ConnInfo.clientid}">>
    %, username => <<"${ConnInfo.clientid}">>
    %, password => <<"${Packet.headers.passcode}">>
}).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

-define(NEG_QOS_CLIENT_ID, <<"NegQoS-Client">>).

-define(REGISTER_INFLIGHT(TopicId, TopicName), #channel{register_inflight = {TopicId, _, TopicName}}).

-define(MAX_RETRY_TIMES, 3).

% 5s
-define(REGISTER_TIMEOUT, 5000).
%% 2h
-define(DEFAULT_SESSION_EXPIRY, 7200000).

-define(RAND_CLIENTID_BYTES, 16).

%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

%% @doc Init protocol
init(
    ConnInfo = #{
        peername := {PeerHost, _} = PeerName,
        sockname := {_, SockPort}
    },
    Option
) ->
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Mountpoint = maps:get(mountpoint, Option, undefined),
    GwId = maps:get(gateway_id, Option),
    EnableNegQoS = maps:get(enable_qos3, Option, true),
    ListenerId =
        case maps:get(listener, Option, undefined) of
            undefined -> undefined;
            {GwName, Type, LisName} -> emqx_gateway_utils:listener_id(GwName, Type, LisName)
        end,
    EnableAuthn = maps:get(enable_authn, Option, true),
    ClientInfo = set_peercert_infos(
        Peercert,
        #{
            zone => default,
            listener => ListenerId,
            protocol => 'mqtt-sn',
            peerhost => PeerHost,
            peername => PeerName,
            sockport => SockPort,
            clientid => undefined,
            username => undefined,
            is_bridge => false,
            is_superuser => false,
            enable_authn => EnableAuthn,
            mountpoint => Mountpoint
        }
    ),

    Ctx = maps:get(ctx, Option),
    Override = maps:merge(
        ?DEFAULT_OVERRIDE,
        maps:get(clientinfo_override, Option, #{})
    ),
    #channel{
        ctx = Ctx,
        gateway_id = GwId,
        enable_negative_qos = EnableNegQoS,
        conninfo = ConnInfo,
        clientinfo = ClientInfo,
        clientinfo_override = Override,
        conn_state = idle,
        register_awaiting_queue = [],
        timers = #{},
        takeover = false,
        resuming = false,
        pendings = []
    }.

set_peercert_infos(NoSSL, ClientInfo) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo;
set_peercert_infos(Peercert, ClientInfo) ->
    {DN, CN} = {esockd_peercert:subject(Peercert), esockd_peercert:common_name(Peercert)},
    ClientInfo#{dn => DN, cn => CN}.

-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom(), channel()) -> term().
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
info(session, #channel{session = Session}) ->
    emqx_utils:maybe_apply(fun emqx_mqttsn_session:info/1, Session);
info(will_msg, #channel{will_msg = WillMsg}) ->
    WillMsg;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(ctx, #channel{ctx = Ctx}) ->
    Ctx.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{session = undefined}) ->
    [];
stats(#channel{session = Session}) ->
    emqx_mqttsn_session:stats(Session).

set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

enrich_conninfo(
    ?SN_CONNECT_MSG(Flags, _ProtoId, Duration, ClientId),
    Channel = #channel{conninfo = ConnInfo}
) ->
    CleanStart = Flags#mqtt_sn_flags.clean_start,
    NConnInfo = ConnInfo#{
        clientid => ClientId,
        proto_name => <<"MQTT-SN">>,
        proto_ver => <<"1.2">>,
        clean_start => CleanStart,
        keepalive => Duration,
        expiry_interval => expiry_interval(Flags)
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

expiry_interval(#mqtt_sn_flags{clean_start = false}) ->
    %% TODO: make it configurable
    ?DEFAULT_SESSION_EXPIRY;
expiry_interval(#mqtt_sn_flags{clean_start = true}) ->
    0.

run_conn_hooks(
    Packet,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo
    }
) ->
    %% XXX: Assign headers of Packet to ConnProps
    ConnProps = #{},
    case run_hooks(Ctx, 'client.connect', [ConnInfo], ConnProps) of
        Error = {error, _Reason} -> Error;
        _NConnProps -> {ok, Packet, Channel}
    end.

enrich_clientinfo(
    Packet,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo0,
        clientinfo_override = Override
    }
) ->
    ClientInfo = write_clientinfo(
        feedvar(Override, Packet, ConnInfo, ClientInfo0),
        ClientInfo0
    ),
    {ok, NPacket, NClientInfo} = emqx_utils:pipeline(
        [
            fun maybe_assign_clientid/2,
            %% FIXME: CALL After authentication successfully
            fun fix_mountpoint/2
        ],
        Packet,
        ClientInfo
    ),
    {ok, NPacket, Channel#channel{clientinfo = NClientInfo}}.

feedvar(Override, Packet, ConnInfo, ClientInfo) ->
    Envs = #{
        'ConnInfo' => ConnInfo,
        'ClientInfo' => ClientInfo,
        'Packet' => connect_packet_to_map(Packet)
    },
    maps:map(
        fun(_K, V) ->
            Tokens = emqx_placeholder:preproc_tmpl(V),
            emqx_placeholder:proc_tmpl(Tokens, Envs)
        end,
        Override
    ).

connect_packet_to_map(#mqtt_sn_message{}) ->
    %% XXX: Empty now
    #{}.

write_clientinfo(Override, ClientInfo) ->
    Override1 = maps:with([username, password, clientid], Override),
    maps:merge(ClientInfo, Override1).

maybe_assign_clientid(_Packet, ClientInfo = #{clientid := ClientId}) when
    ClientId == undefined;
    ClientId == <<>>
->
    {ok, ClientInfo#{clientid => emqx_utils:rand_id(?RAND_CLIENTID_BYTES)}};
maybe_assign_clientid(_Packet, ClientInfo) ->
    {ok, ClientInfo}.

fix_mountpoint(_Packet, #{mountpoint := undefined}) ->
    ok;
fix_mountpoint(_Packet, ClientInfo = #{mountpoint := Mountpoint}) ->
    %% TODO: Enrich the variable replacement????
    %%       i.e: ${ClientInfo.auth_result.productKey}
    Mountpoint1 = emqx_mountpoint:replvar(Mountpoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := Mountpoint1}}.

set_log_meta(_Packet, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId),
    ok.

maybe_require_will_msg(?SN_CONNECT_MSG(Flags, _, _, _), Channel) ->
    #mqtt_sn_flags{will = Will} = Flags,
    case Will of
        true ->
            {error, need_will_msg, Channel};
        _ ->
            ok
    end.

auth_connect(
    _Packet,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    #{
        clientid := ClientId,
        username := Username
    } = ClientInfo,
    case emqx_gateway_ctx:authenticate(Ctx, ClientInfo) of
        {ok, NClientInfo} ->
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?SLOG(warning, #{
                msg => "client_login_failed",
                clientid => ClientId,
                username => Username,
                reason => Reason
            }),
            {error, name_to_returncode(Reason)}
    end.

ensure_connected(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks(Ctx, 'client.connected', [ClientInfo, NConnInfo]),
    schedule_connection_expire(Channel#channel{
        conninfo = NConnInfo,
        conn_state = connected
    }).

schedule_connection_expire(Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}) ->
    case emqx_gateway_ctx:connection_expire_interval(Ctx, ClientInfo) of
        undefined ->
            Channel;
        Interval ->
            ensure_timer(connection_expire, Interval, Channel)
    end.

process_connect(
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo = #{clean_start := CleanStart},
        clientinfo = ClientInfo,
        will_msg = MaybeWillMsg
    }
) ->
    SessFun = fun(ClientInfoT, _) -> emqx_mqttsn_session:init(ClientInfoT, MaybeWillMsg) end,
    case
        emqx_gateway_ctx:open_session(
            Ctx,
            CleanStart,
            ClientInfo,
            ConnInfo,
            SessFun,
            emqx_mqttsn_session
        )
    of
        {ok, #{
            session := Session,
            present := false
        }} ->
            handle_out(
                connack,
                ?SN_RC_ACCEPTED,
                Channel#channel{session = Session}
            );
        {ok, #{session := Session, present := true, pendings := Pendings}} ->
            Pendings1 = lists:usort(lists:append(Pendings, emqx_utils:drain_deliver())),
            NChannel = Channel#channel{
                session = Session,
                resuming = true,
                pendings = Pendings1
            },
            handle_out(connack, ?SN_RC_ACCEPTED, NChannel);
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "failed_to_open_session",
                reason => Reason
            }),
            handle_out(connack, ?SN_RC2_FAILED_SESSION, Channel)
    end.

%%--------------------------------------------------------------------
%% Enrich Keepalive

ensure_keepalive(Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_keepalive_timer(0, Channel) ->
    Channel;
ensure_keepalive_timer(Interval, Channel) ->
    Keepalive = emqx_keepalive:init(Interval),
    ensure_timer(keepalive, Channel#channel{keepalive = Keepalive}).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(mqtt_sn_message(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

%% SEARCHGW, GWINFO
handle_in(
    ?SN_SEARCHGW_MSG(_Radius),
    Channel = #channel{gateway_id = GwId}
) ->
    {ok, {outgoing, ?SN_GWINFO_MSG(GwId, <<>>)}, Channel};
handle_in(?SN_ADVERTISE_MSG(_GwId, _Radius), Channel) ->
    % ignore
    shutdown(normal, Channel);
%% Ack DISCONNECT even if it is not connected
handle_in(
    ?SN_DISCONNECT_MSG(_Duration),
    Channel = #channel{conn_state = idle}
) ->
    handle_out(disconnect, normal, Channel);
handle_in(
    Publish =
        ?SN_PUBLISH_MSG(
            #mqtt_sn_flags{
                qos = ?QOS_NEG1,
                topic_id_type = TopicIdType
            },
            TopicId,
            MsgId,
            Data
        ),
    Channel = #channel{conn_state = idle}
) ->
    case check_negative_qos_enable(Publish, Channel) of
        ok ->
            TopicName =
                case TopicIdType of
                    ?SN_SHORT_TOPIC ->
                        TopicId;
                    ?SN_PREDEFINED_TOPIC ->
                        Registry = emqx_mqttsn_registry:init(),
                        emqx_mqttsn_registry:lookup_topic(TopicId, Registry);
                    _ ->
                        undefined
                end,
            case TopicName =/= undefined of
                true ->
                    Msg = emqx_message:make(
                        ?NEG_QOS_CLIENT_ID,
                        ?QOS_0,
                        TopicName,
                        Data
                    ),
                    ?SLOG(debug, #{
                        msg => "receive_qo3_message_in_idle_mode",
                        topic => TopicName,
                        data => Data
                    }),
                    _ = emqx_broker:publish(Msg),
                    ok;
                false ->
                    ok
            end,
            shutdown(normal, Channel);
        {error, RC} ->
            ?tp(info, ignore_negative_qos, #{
                topic_id => TopicId,
                msg_id => MsgId,
                return_code => RC
            }),
            PubAck = ?SN_PUBACK_MSG(TopicId, MsgId, RC),
            shutdown(normal, PubAck, Channel)
    end;
handle_in(
    Pkt = #mqtt_sn_message{type = Type},
    Channel = #channel{conn_state = idle}
) when
    Type /= ?SN_CONNECT
->
    ?SLOG(warning, #{
        msg => "receive_unknown_packet_in_idle_state",
        packet => Pkt
    }),
    shutdown(normal, Channel);
handle_in(
    ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId),
    Channel = #channel{conn_state = connecting}
) ->
    ?SLOG(warning, #{msg => "receive_connect_packet_in_connecting_state"}),
    {ok, Channel};
handle_in(
    ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId),
    Channel = #channel{conn_state = connected}
) ->
    {error, unexpected_connect, Channel};
handle_in(
    ?SN_WILLTOPIC_EMPTY_MSG,
    Channel = #channel{conn_state = connecting}
) ->
    %% 6.3:
    %% Note that if a client wants to delete only its Will data at
    %% connection setup, it could send a CONNECT message with
    %% 'CleanSession=false' and 'Will=true',
    %% and sends an empty WILLTOPIC message to the GW when prompted to do so
    case auth_connect(fake_packet, Channel#channel{will_msg = undefined}) of
        {ok, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, ReasonCode} ->
            handle_out(connack, ReasonCode, Channel)
    end;
handle_in(
    ?SN_WILLTOPIC_MSG(Flags, Topic),
    Channel = #channel{
        conn_state = connecting,
        clientinfo = #{clientid := ClientId}
    }
) ->
    #mqtt_sn_flags{qos = QoS, retain = Retain} = Flags,
    WillMsg0 = emqx_message:make(ClientId, QoS, Topic, <<>>),
    WillMsg = emqx_message:set_flag(retain, Retain, WillMsg0),
    NChannel = Channel#channel{will_msg = WillMsg},
    {ok, {outgoing, ?SN_WILLMSGREQ_MSG()}, NChannel};
handle_in(
    ?SN_WILLMSG_MSG(Payload),
    Channel = #channel{
        conn_state = connecting,
        will_msg = WillMsg
    }
) ->
    NWillMsg = WillMsg#message{payload = Payload},
    case auth_connect(fake_packet, Channel#channel{will_msg = NWillMsg}) of
        {ok, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, ReasonCode} ->
            handle_out(connack, ReasonCode, Channel)
    end;
%% TODO: takeover ???
handle_in(
    ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, ClientId),
    Channel = #channel{
        clientinfo = #{clientid := ClientId},
        conn_state = ConnState
    }
) when
    ConnState == asleep;
    ConnState == awake
->
    %% From the asleep or awake state a client can return either to the
    %% active state by sending a CONNECT message [6.14]
    ?SLOG(info, #{
        msg => "goto_connected_state",
        previous_state => ConnState,
        clientid => ClientId
    }),
    handle_out(
        connack,
        ?SN_RC_ACCEPTED,
        Channel#channel{conn_state = connected}
    );
%% new connection
handle_in(
    Packet = ?SN_CONNECT_MSG(_Flags, _ProtoId, _Duration, _ClientId),
    Channel = #channel{conn_state = idle}
) ->
    case
        emqx_utils:pipeline(
            [
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun enrich_clientinfo/2,
                fun set_log_meta/2,
                %% TODO: How to implement the banned in the gateway instance?
                %, fun check_banned/2
                fun maybe_require_will_msg/2,
                fun auth_connect/2
            ],
            Packet,
            Channel#channel{conn_state = connecting}
        )
    of
        {ok, _NPacket, NChannel} ->
            process_connect(ensure_connected(NChannel));
        {error, need_will_msg, NChannel} ->
            {ok, {outgoing, ?SN_WILLTOPICREQ_MSG()}, NChannel};
        {error, ReasonCode, NChannel} ->
            handle_out(connack, ReasonCode, NChannel)
    end;
handle_in(
    ?SN_REGISTER_MSG(_TopicId, MsgId, TopicName),
    Channel = #channel{session = Session}
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:reg(TopicName, Registry) of
        {ok, TopicId, NRegistry} ->
            ?SLOG(debug, #{
                msg => "registered_topic_name",
                topic_name => TopicName,
                topic_id => TopicId
            }),
            AckPacket = ?SN_REGACK_MSG(TopicId, MsgId, ?SN_RC_ACCEPTED),
            NSession = emqx_mqttsn_session:set_registry(NRegistry, Session),
            {ok, {outgoing, AckPacket}, Channel#channel{session = NSession}};
        {error, too_large} ->
            ?SLOG(error, #{
                msg => "register_topic_failed",
                topic_name => TopicName,
                reason => topic_id_fulled
            }),
            AckPacket = ?SN_REGACK_MSG(
                ?SN_INVALID_TOPIC_ID,
                MsgId,
                ?SN_RC_NOT_SUPPORTED
            ),
            {ok, {outgoing, AckPacket}, Channel};
        {error, wildcard_topic} ->
            ?SLOG(error, #{
                msg => "register_topic_failed",
                topic_name => TopicName,
                reason => not_support_wildcard_topic
            }),
            AckPacket = ?SN_REGACK_MSG(
                ?SN_INVALID_TOPIC_ID,
                MsgId,
                ?SN_RC_NOT_SUPPORTED
            ),
            {ok, {outgoing, AckPacket}, Channel}
    end;
handle_in(
    ?SN_REGACK_MSG(TopicId, _MsgId, ?SN_RC_ACCEPTED),
    Channel = ?REGISTER_INFLIGHT(TopicId, TopicName)
) ->
    ?SLOG(debug, #{
        msg => "register_topic_name_to_client_succesfully",
        topic_id => TopicId,
        topic_name => TopicName
    }),
    NChannel = cancel_timer(
        retry_register,
        Channel#channel{register_inflight = undefined}
    ),
    send_next_register_or_replay_publish(TopicName, NChannel);
handle_in(
    ?SN_REGACK_MSG(TopicId, _MsgId, Reason),
    Channel = ?REGISTER_INFLIGHT(TopicId, TopicName)
) ->
    case Reason of
        ?SN_RC_CONGESTION ->
            %% TODO: a or b?
            %% a. waiting for next register timer
            %% b. re-new the re-transmit timer
            {ok, Channel};
        _ ->
            %% skip this topic-name register, if the reason is
            %% ?SN_RC_NOT_SUPPORTED, ?SN_RC_INVALID_TOPIC_ID, etc.
            ?SLOG(warning, #{
                msg => "skipp_register_topic_name_to_client",
                topic_id => TopicId,
                topic_name => TopicName
            }),
            NChannel = cancel_timer(
                retry_register,
                Channel#channel{register_inflight = undefined}
            ),
            send_next_register_or_replay_publish(TopicName, NChannel)
    end;
handle_in(
    ?SN_REGACK_MSG(TopicId, MsgId, Reason),
    Channel = #channel{register_inflight = Inflight}
) ->
    ?SLOG(error, #{
        msg => "unexpected_regack_msg",
        acked_msg_id => MsgId,
        acked_topic_id => TopicId,
        acked_reason => Reason,
        current_inflight => Inflight
    }),
    {ok, Channel};
handle_in(PubPkt = ?SN_PUBLISH_MSG(_Flags, TopicId0, MsgId, _Data), Channel) ->
    TopicId =
        case is_integer(TopicId0) of
            true ->
                TopicId0;
            _ ->
                <<Id:16>> = TopicId0,
                Id
        end,
    case
        emqx_utils:pipeline(
            [
                fun check_negative_qos_enable/2,
                fun preproc_pub_pkt/2,
                fun convert_topic_id_to_name/2,
                fun check_pub_authz/2,
                fun convert_pub_to_msg/2
            ],
            PubPkt,
            Channel
        )
    of
        {ok, Msg, NChannel} ->
            do_publish(TopicId, MsgId, Msg, NChannel);
        {error, ReturnCode, NChannel} ->
            ?tp(info, publish_msg_rejected, #{
                topic_id => TopicId,
                msg_id => MsgId,
                return_code => ReturnCode
            }),
            handle_out(puback, {TopicId, MsgId, ReturnCode}, NChannel)
    end;
handle_in(
    ?SN_PUBACK_MSG(TopicId, MsgId, ReturnCode),
    Channel = #channel{
        ctx = Ctx,
        session = Session,
        clientinfo = ClientInfo
    }
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case ReturnCode of
        ?SN_RC_ACCEPTED ->
            case emqx_mqttsn_session:puback(ClientInfo, MsgId, Session) of
                {ok, Msg, NSession} ->
                    ok = after_message_acked(ClientInfo, Msg, Channel),
                    {Replies, NChannel} = goto_asleep_if_buffered_msgs_sent(
                        Channel#channel{session = NSession}
                    ),
                    {ok, Replies, NChannel};
                {ok, Msg, Publishes, NSession} ->
                    ok = after_message_acked(ClientInfo, Msg, Channel),
                    handle_out(
                        publish,
                        Publishes,
                        Channel#channel{session = NSession}
                    );
                {error, ?RC_PROTOCOL_ERROR} ->
                    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
                {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
                    ?SLOG(warning, #{
                        msg => "commit_puback_failed",
                        msg_id => MsgId,
                        reason => msg_id_inused
                    }),
                    ok = metrics_inc(Ctx, 'packets.puback.inuse'),
                    {ok, Channel};
                {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
                    ?SLOG(warning, #{
                        msg => "commit_puback_failed",
                        msg_id => MsgId,
                        reason => not_found
                    }),
                    ok = metrics_inc(Ctx, 'packets.puback.missed'),
                    {ok, Channel}
            end;
        ?SN_RC_INVALID_TOPIC_ID ->
            case emqx_mqttsn_registry:lookup_topic(TopicId, Registry) of
                undefined ->
                    {ok, Channel};
                TopicName ->
                    %% notice that this TopicName maybe normal or predefined,
                    %% involving the predefined topic name in register to
                    %% enhance the gateway's robustness even inconsistent
                    %% with MQTT-SN channels
                    handle_out(register, {TopicId, TopicName}, Channel)
            end;
        _ ->
            ?SLOG(error, #{
                msg => "cannt_handle_PUBACK",
                return_code => ReturnCode
            }),
            {ok, Channel}
    end;
handle_in(
    ?SN_PUBREC_MSG(?SN_PUBREC, MsgId),
    Channel = #channel{
        ctx = Ctx,
        session = Session,
        clientinfo = ClientInfo
    }
) ->
    case emqx_mqttsn_session:pubrec(ClientInfo, MsgId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(ClientInfo, Msg, Channel),
            NChannel = Channel#channel{session = NSession},
            handle_out(pubrel, MsgId, NChannel);
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{
                msg => "commit_PUBREC_failed",
                msg_id => MsgId,
                reason => msg_id_inused
            }),
            ok = metrics_inc(Ctx, 'packets.pubrec.inuse'),
            handle_out(pubrel, MsgId, Channel);
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{
                msg => "commit_PUBREC_failed",
                msg_id => MsgId,
                reason => not_found
            }),
            ok = metrics_inc(Ctx, 'packets.pubrec.missed'),
            handle_out(pubrel, MsgId, Channel)
    end;
handle_in(
    ?SN_PUBREC_MSG(?SN_PUBREL, MsgId),
    Channel = #channel{ctx = Ctx, session = Session, clientinfo = ClientInfo}
) ->
    case emqx_mqttsn_session:pubrel(ClientInfo, MsgId, Session) of
        {ok, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(pubcomp, MsgId, NChannel);
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{
                msg => "commit_PUBREL_failed",
                msg_id => MsgId,
                reason => not_found
            }),
            ok = metrics_inc(Ctx, 'packets.pubrel.missed'),
            handle_out(pubcomp, MsgId, Channel)
    end;
handle_in(
    ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId),
    Channel = #channel{ctx = Ctx, session = Session, clientinfo = ClientInfo}
) ->
    case emqx_mqttsn_session:pubcomp(ClientInfo, MsgId, Session) of
        {ok, NSession} ->
            {Replies, NChannel} = goto_asleep_if_buffered_msgs_sent(
                Channel#channel{session = NSession}
            ),
            {ok, Replies, NChannel};
        {ok, Publishes, NSession} ->
            handle_out(
                publish,
                Publishes,
                Channel#channel{session = NSession}
            );
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{
                msg => "commit_PUBCOMP_failed",
                msg_id => MsgId,
                reason => msg_id_inused
            }),
            ok = metrics_inc(Ctx, 'packets.pubcomp.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?SLOG(warning, #{
                msg => "commit_PUBCOMP_failed",
                msg_id => MsgId,
                reason => not_found
            }),
            ok = metrics_inc(Ctx, 'packets.pubcomp.missed'),
            {ok, Channel}
    end;
handle_in(SubPkt = ?SN_SUBSCRIBE_MSG(_, MsgId, _), Channel) ->
    case
        emqx_utils:pipeline(
            [
                fun preproc_subs_type/2,
                fun check_subscribe_authz/2,
                fun run_client_subs_hook/2,
                fun do_subscribe/2
            ],
            SubPkt,
            Channel
        )
    of
        {ok, {TopicId, _TopicName, SubOpts}, NChannel} ->
            GrantedQoS = maps:get(qos, SubOpts),
            SubAck = ?SN_SUBACK_MSG(
                #mqtt_sn_flags{qos = GrantedQoS},
                TopicId,
                MsgId,
                ?SN_RC_ACCEPTED
            ),
            {ok, outgoing_and_update(SubAck), NChannel};
        {error, ReturnCode, NChannel} ->
            SubAck = ?SN_SUBACK_MSG(
                #mqtt_sn_flags{},
                ?SN_INVALID_TOPIC_ID,
                MsgId,
                ReturnCode
            ),
            {ok, {outgoing, SubAck}, NChannel}
    end;
handle_in(
    UnsubPkt = ?SN_UNSUBSCRIBE_MSG(_, MsgId, TopicIdOrName),
    Channel
) ->
    case
        emqx_utils:pipeline(
            [
                fun preproc_unsub_type/2,
                fun run_client_unsub_hook/2,
                fun do_unsubscribe/2
            ],
            UnsubPkt,
            Channel
        )
    of
        {ok, _TopicName, NChannel} ->
            UnsubAck = ?SN_UNSUBACK_MSG(MsgId),
            {ok, outgoing_and_update(UnsubAck), NChannel};
        {error, Reason, NChannel} ->
            ?SLOG(warning, #{
                msg => "unsubscribe_failed",
                topic => TopicIdOrName,
                reason => Reason
            }),
            %% XXX: Even if it fails, the reply is successful.
            UnsubAck = ?SN_UNSUBACK_MSG(MsgId),
            {ok, {outgoing, UnsubAck}, NChannel}
    end;
handle_in(?SN_PINGREQ_MSG(ClientId), Channel) when
    ClientId == undefined;
    ClientId == <<>>
->
    {ok, {outgoing, ?SN_PINGRESP_MSG()}, Channel};
handle_in(
    ?SN_PINGREQ_MSG(ReqClientId),
    Channel = #channel{clientinfo = #{clientid := ClientId}}
) when
    ReqClientId =/= ClientId
->
    ?SLOG(warning, #{
        msg => "awake_pingreq_clientid_not_match",
        clientid => ClientId,
        request_clientid => ReqClientId
    }),
    %% FIXME: takeover_and_awake..
    {ok, Channel};
handle_in(
    ?SN_PINGREQ_MSG(ClientId),
    Channel = #channel{conn_state = ConnState}
) when
    ConnState == idle; ConnState == asleep; ConnState == awake
->
    awake(ClientId, Channel);
handle_in(
    ?SN_PINGREQ_MSG(ClientId),
    Channel = #channel{
        conn_state = connected,
        clientinfo = #{clientid := ClientId}
    }
) ->
    {ok, {outgoing, ?SN_PINGRESP_MSG()}, Channel};
handle_in(?SN_DISCONNECT_MSG(_Duration = undefined), Channel) ->
    handle_out(disconnect, normal, Channel);
handle_in(
    ?SN_DISCONNECT_MSG(Duration),
    Channel = #channel{conn_state = ConnState}
) when
    ConnState == connected; ConnState == asleep
->
    %% A DISCONNECT message with a Duration field is sent by a client
    %% when it wants to go to the “asleep” state. The receipt of this
    %% message is also acknowledged by the gateway by means of a
    %% DISCONNECT message (without a duration field) [5.4.21]
    %%
    AckPkt = ?SN_DISCONNECT_MSG(undefined),
    {ok, [{outgoing, AckPkt}, {event, asleep}], asleep(Duration, Channel)};
handle_in(
    ?SN_WILLTOPICUPD_MSG(Flags, Topic),
    Channel = #channel{
        will_msg = WillMsg,
        clientinfo = #{clientid := ClientId}
    }
) ->
    NWillMsg =
        case Topic of
            undefined -> undefined;
            _ -> update_will_topic(WillMsg, Flags, Topic, ClientId)
        end,
    AckPkt = ?SN_WILLTOPICRESP_MSG(?SN_RC_ACCEPTED),
    {ok, {outgoing, AckPkt}, Channel#channel{will_msg = NWillMsg}};
handle_in(
    ?SN_WILLMSGUPD_MSG(Payload),
    Channel = #channel{will_msg = WillMsg}
) ->
    AckPkt = ?SN_WILLMSGRESP_MSG(?SN_RC_ACCEPTED),
    NWillMsg = update_will_msg(WillMsg, Payload),
    {ok, {outgoing, AckPkt}, Channel#channel{will_msg = NWillMsg}}.

handle_frame_error(Reason, Channel) ->
    shutdown(Reason, Channel).

after_message_acked(ClientInfo, Msg, #channel{ctx = Ctx}) ->
    ok = metrics_inc(Ctx, 'messages.acked'),
    run_hooks_without_metrics(
        Ctx,
        'message.acked',
        [ClientInfo, emqx_message:set_header(puback_props, #{}, Msg)]
    ).

outgoing_and_update(Pkt) ->
    [{outgoing, Pkt}, {event, update}].

send_next_register_or_replay_publish(
    _TopicName,
    Channel = #channel{register_awaiting_queue = []}
) ->
    {Outgoing, NChannel} = resume_or_replay_messages(Channel),
    {ok, Outgoing, NChannel};
send_next_register_or_replay_publish(
    _TopicName,
    Channel = #channel{register_awaiting_queue = RAQueue}
) ->
    [RegisterReq | NRAQueue] = RAQueue,
    handle_out(
        register,
        RegisterReq,
        Channel#channel{register_awaiting_queue = NRAQueue}
    ).

%%--------------------------------------------------------------------
%% Handle Publish

check_negative_qos_enable(
    ?SN_PUBLISH_MSG(Flags, _TopicId, _MsgId, _Data),
    #channel{enable_negative_qos = EnableNegQoS}
) ->
    #mqtt_sn_flags{qos = QoS} = Flags,
    case EnableNegQoS =:= false andalso QoS =:= ?QOS_NEG1 of
        true ->
            {error, ?SN_RC_NOT_SUPPORTED};
        false ->
            ok
    end.

preproc_pub_pkt(
    ?SN_PUBLISH_MSG(Flags, Topic0, _MsgId, Data),
    Channel
) ->
    #mqtt_sn_flags{topic_id_type = TopicIdType} = Flags,
    case TopicIdType of
        ?SN_NORMAL_TOPIC ->
            <<TopicId:16>> = Topic0,
            TopicIndicator = {id, TopicId},
            {ok, {TopicIndicator, Flags, Data}, Channel};
        ?SN_PREDEFINED_TOPIC ->
            TopicIndicator = {id, Topic0},
            {ok, {TopicIndicator, Flags, Data}, Channel};
        ?SN_SHORT_TOPIC ->
            case emqx_topic:wildcard(Topic0) of
                true ->
                    {error, ?SN_RC_NOT_SUPPORTED};
                false ->
                    TopicIndicator = {name, Topic0},
                    {ok, {TopicIndicator, Flags, Data}, Channel}
            end
    end.

convert_topic_id_to_name({{name, TopicName}, Flags, Data}, Channel) ->
    {ok, {TopicName, Flags, Data}, Channel};
convert_topic_id_to_name(
    {{id, TopicId}, Flags, Data},
    Channel = #channel{session = Session}
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:lookup_topic(TopicId, Registry) of
        undefined ->
            {error, ?SN_RC_INVALID_TOPIC_ID};
        TopicName ->
            {ok, {TopicName, Flags, Data}, Channel}
    end.

check_pub_authz(
    {TopicName, #mqtt_sn_flags{qos = QoS, retain = Retain}, _Data},
    #channel{ctx = Ctx, clientinfo = ClientInfo}
) ->
    Action = ?AUTHZ_PUBLISH(QoS, Retain),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, TopicName) of
        allow -> ok;
        deny -> {error, ?SN_RC2_NOT_AUTHORIZE}
    end.

convert_pub_to_msg(
    {TopicName, Flags, Data},
    Channel = #channel{clientinfo = #{clientid := ClientId, mountpoint := Mountpoint}}
) ->
    #mqtt_sn_flags{qos = QoS, dup = Dup, retain = Retain} = Flags,
    NewQoS = get_corrected_qos(QoS),
    NTopicName = emqx_mountpoint:mount(Mountpoint, TopicName),
    Message = put_message_headers(
        emqx_message:make(
            ClientId,
            NewQoS,
            NTopicName,
            Data,
            #{dup => Dup, retain => Retain},
            #{}
        ),
        Channel
    ),
    {ok, Message, Channel}.

put_message_headers(Msg, #channel{
    conninfo = #{proto_ver := ProtoVer},
    clientinfo = #{
        protocol := Protocol,
        username := Username,
        peerhost := PeerHost
    }
}) ->
    emqx_message:set_headers(
        #{
            proto_ver => ProtoVer,
            protocol => Protocol,
            username => Username,
            peerhost => PeerHost
        },
        Msg
    ).

get_corrected_qos(?QOS_NEG1) -> ?QOS_0;
get_corrected_qos(QoS) -> QoS.

do_publish(_TopicId, _MsgId, Msg = #message{qos = ?QOS_0}, Channel) ->
    _ = emqx_broker:publish(Msg),
    {ok, Channel};
do_publish(TopicId, MsgId, Msg = #message{qos = ?QOS_1}, Channel) ->
    _ = emqx_broker:publish(Msg),
    handle_out(puback, {TopicId, MsgId, ?SN_RC_ACCEPTED}, Channel);
do_publish(
    TopicId,
    MsgId,
    Msg = #message{qos = ?QOS_2},
    Channel = #channel{ctx = Ctx, session = Session, clientinfo = ClientInfo}
) ->
    case emqx_mqttsn_session:publish(ClientInfo, MsgId, Msg, Session) of
        {ok, _PubRes, NSession} ->
            NChannel1 = ensure_timer(
                expire_awaiting_rel,
                Channel#channel{session = NSession}
            ),
            handle_out(pubrec, MsgId, NChannel1);
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = metrics_inc(Ctx, 'packets.publish.inuse'),
            %% XXX: Use PUBACK to reply a PUBLISH Error Code
            handle_out(
                puback,
                {TopicId, MsgId, ?SN_RC_NOT_SUPPORTED},
                Channel
            );
        {error, ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ok = metrics_inc(Ctx, 'messages.dropped.receive_maximum'),
            handle_out(puback, {TopicId, MsgId, ?SN_RC_CONGESTION}, Channel)
    end.

%%--------------------------------------------------------------------
%% Handle Susbscribe

preproc_subs_type(
    ?SN_SUBSCRIBE_MSG_TYPE(
        ?SN_NORMAL_TOPIC,
        TopicName,
        QoS
    ),
    Channel = #channel{session = Session}
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    %% If the gateway is able accept the subscription,
    %% it assigns a topic id to the received topic name
    %% and returns it within a SUBACK message
    case emqx_mqttsn_registry:reg(TopicName, Registry) of
        {error, too_large} ->
            {error, ?SN_RC2_EXCEED_LIMITATION};
        {error, wildcard_topic} ->
            %% If the client subscribes to a topic name which contains a
            %% wildcard character, the returning SUBACK message will contain
            %% the topic id value 0x0000. The GW will the use the registration
            %% procedure to inform the client about the to-be-used topic id
            %% value when it has the first PUBLISH message with a matching
            %% topic name to be sent to the client, see also Section 6.10.
            {ok, {?SN_INVALID_TOPIC_ID, TopicName, QoS}, Channel};
        {ok, TopicId, NRegistry} ->
            NSession = emqx_mqttsn_session:set_registry(NRegistry, Session),
            {ok, {TopicId, TopicName, QoS}, Channel#channel{session = NSession}}
    end;
preproc_subs_type(
    ?SN_SUBSCRIBE_MSG_TYPE(
        ?SN_PREDEFINED_TOPIC,
        TopicId,
        QoS
    ),
    Channel = #channel{session = Session}
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:lookup_topic(TopicId, Registry) of
        undefined ->
            {error, ?SN_RC_INVALID_TOPIC_ID};
        TopicName ->
            {ok, {TopicId, TopicName, QoS}, Channel}
    end;
preproc_subs_type(
    ?SN_SUBSCRIBE_MSG_TYPE(
        ?SN_SHORT_TOPIC,
        TopicId,
        QoS
    ),
    Channel
) ->
    TopicName =
        case is_binary(TopicId) of
            true -> TopicId;
            false -> <<TopicId:16>>
        end,
    %% XXX: ?SN_INVALID_TOPIC_ID ???
    {ok, {?SN_INVALID_TOPIC_ID, TopicName, QoS}, Channel};
preproc_subs_type(
    ?SN_SUBSCRIBE_MSG_TYPE(_Reserved, _TopicId, _QoS),
    _Channel
) ->
    {error, ?SN_RC_NOT_SUPPORTED}.

check_subscribe_authz(
    {_TopicId, TopicName, QoS},
    Channel = #channel{ctx = Ctx, clientinfo = ClientInfo}
) ->
    Action = ?AUTHZ_SUBSCRIBE(QoS),
    case emqx_gateway_ctx:authorize(Ctx, ClientInfo, Action, TopicName) of
        allow ->
            {ok, Channel};
        _ ->
            {error, ?SN_RC2_NOT_AUTHORIZE}
    end.

run_client_subs_hook(
    {TopicId, TopicName, QoS},
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    {TopicName1, SubOpts0} = emqx_topic:parse(TopicName),
    TopicFilters = [{TopicName1, SubOpts0#{qos => QoS}}],
    case
        run_hooks(
            Ctx,
            'client.subscribe',
            [ClientInfo, #{}],
            TopicFilters
        )
    of
        [] ->
            ?SLOG(warning, #{
                msg => "skip_to_subscribe",
                topic_name => TopicName,
                reason => "'client.subscribe' filtered it"
            }),
            {error, ?SN_RC2_EXCEED_LIMITATION};
        [{NTopicName, NSubOpts} | _] ->
            {ok, {TopicId, NTopicName, NSubOpts}, Channel}
    end.

do_subscribe(
    {TopicId, TopicName, SubOpts},
    Channel = #channel{
        session = Session,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint}
    }
) ->
    NTopicName = emqx_mountpoint:mount(Mountpoint, TopicName),
    NSubOpts = maps:merge(emqx_gateway_utils:default_subopts(), SubOpts),
    case emqx_mqttsn_session:subscribe(ClientInfo, NTopicName, NSubOpts, Session) of
        {ok, NSession} ->
            {ok, {TopicId, NTopicName, NSubOpts}, Channel#channel{session = NSession}};
        {error, ?RC_QUOTA_EXCEEDED} ->
            ?SLOG(warning, #{
                msg => "cannt_subscribe_due_to_quota_exceeded",
                topic_name => TopicName,
                reason => emqx_reason_codes:text(?RC_QUOTA_EXCEEDED)
            }),
            {error, ?SN_RC2_EXCEED_LIMITATION}
    end.

%%--------------------------------------------------------------------
%% Handle Unsubscribe

preproc_unsub_type(
    ?SN_UNSUBSCRIBE_MSG_TYPE(
        ?SN_NORMAL_TOPIC,
        TopicName
    ),
    Channel
) ->
    {ok, TopicName, Channel};
preproc_unsub_type(
    ?SN_UNSUBSCRIBE_MSG_TYPE(
        ?SN_PREDEFINED_TOPIC,
        TopicId
    ),
    Channel = #channel{session = Session}
) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:lookup_topic(TopicId, Registry) of
        undefined ->
            {error, not_found};
        TopicName ->
            {ok, TopicName, Channel}
    end;
preproc_unsub_type(
    ?SN_UNSUBSCRIBE_MSG_TYPE(
        ?SN_SHORT_TOPIC,
        TopicId
    ),
    Channel
) ->
    TopicName =
        case is_binary(TopicId) of
            true -> TopicId;
            false -> <<TopicId:16>>
        end,
    {ok, TopicName, Channel}.

run_client_unsub_hook(
    TopicName,
    Channel = #channel{
        ctx = Ctx,
        clientinfo = ClientInfo
    }
) ->
    TopicFilters = [emqx_topic:parse(TopicName)],
    case
        run_hooks(
            Ctx,
            'client.unsubscribe',
            [ClientInfo, #{}],
            TopicFilters
        )
    of
        [] ->
            {ok, [], Channel};
        NTopicFilters ->
            {ok, NTopicFilters, Channel}
    end.

do_unsubscribe(
    TopicFilters,
    Channel = #channel{
        session = Session,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint}
    }
) ->
    NChannel =
        lists:foldl(
            fun({TopicName, SubOpts}, ChannAcc) ->
                NTopicName = emqx_mountpoint:mount(Mountpoint, TopicName),
                NSubOpts = maps:merge(
                    emqx_gateway_utils:default_subopts(),
                    SubOpts
                ),
                case
                    emqx_mqttsn_session:unsubscribe(
                        ClientInfo,
                        NTopicName,
                        NSubOpts,
                        Session
                    )
                of
                    {ok, NSession} ->
                        ChannAcc#channel{session = NSession};
                    {error, ?RC_NO_SUBSCRIPTION_EXISTED} ->
                        ChannAcc
                end
            end,
            Channel,
            TopicFilters
        ),
    {ok, TopicFilters, NChannel}.

%%--------------------------------------------------------------------
%% Awake & Asleep

awake(ClientId, Channel = #channel{conn_state = idle}) ->
    ?SLOG(warning, #{
        msg => "awake_pingreq_in_idle_state",
        clientid => ClientId
    }),
    %% TODO: takeover and awake?
    %% 1. Query emqx_cm_registry to get the session state?
    %% 2. Takeover it and goto awake state
    {ok, {outgoing, ?SN_PINGRESP_MSG()}, Channel};
awake(
    ClientId,
    Channel = #channel{
        conn_state = ConnState,
        session = Session,
        clientinfo = ClientInfo = #{clientid := ClientId}
    }
) when
    ConnState == asleep; ConnState == awake
->
    ?SLOG(info, #{
        msg => "goto_awake_state",
        clientid => ClientId,
        previous_state => ConnState
    }),
    {ok, Publishes, NSession} = emqx_mqttsn_session:replay(ClientInfo, Session),
    Channel1 = cancel_timer(expire_asleep, Channel),
    {Replies0, NChannel0} = outgoing_deliver_and_register(
        do_deliver(
            Publishes,
            Channel1#channel{
                conn_state = awake, session = NSession
            }
        )
    ),
    Replies1 = [{event, awake} | Replies0],

    {Replies2, NChannel} = goto_asleep_if_buffered_msgs_sent(NChannel0),
    {ok, Replies1 ++ Replies2, NChannel}.

goto_asleep_if_buffered_msgs_sent(
    Channel = #channel{
        conn_state = awake,
        session = Session,
        asleep_timer_duration = Duration
    }
) ->
    case
        emqx_mqueue:is_empty(emqx_mqttsn_session:info(mqueue, Session)) andalso
            emqx_inflight:is_empty(emqx_mqttsn_session:info(inflight, Session))
    of
        true ->
            ?SLOG(info, #{
                msg => "goto_asleep_state",
                reason => buffered_messages_sent,
                duration => Duration
            }),
            Replies = [
                {outgoing, ?SN_PINGRESP_MSG()},
                {event, asleep}
            ],
            {Replies, ensure_asleep_timer(Channel#channel{conn_state = asleep})};
        false ->
            {[], Channel}
    end;
goto_asleep_if_buffered_msgs_sent(Channel) ->
    {[], Channel}.

asleep(Duration, Channel = #channel{conn_state = asleep}) ->
    %% 6.14: The client can also modify its sleep duration
    %% by sending a DISCONNECT message with a new value of
    %% the sleep duration
    %%
    %% XXX: Do we need to limit the maximum of Duration?
    ?SLOG(debug, #{
        msg => "update_asleep_timer",
        new_duration => Duration
    }),
    ensure_asleep_timer(Duration, cancel_timer(expire_asleep, Channel));
asleep(Duration, Channel = #channel{conn_state = connected}) ->
    ?SLOG(info, #{
        msg => "goto_asleep_state",
        duration => Duration
    }),
    ensure_asleep_timer(Duration, Channel#channel{conn_state = asleep}).

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.

handle_out(
    connack,
    ?SN_RC_ACCEPTED,
    Channel = #channel{ctx = Ctx, conninfo = ConnInfo}
) ->
    _ = run_hooks(
        Ctx,
        'client.connack',
        [ConnInfo, returncode_name(?SN_RC_ACCEPTED)],
        #{}
    ),
    return_connack(
        ?SN_CONNACK_MSG(?SN_RC_ACCEPTED),
        ensure_keepalive(Channel)
    );
handle_out(
    connack,
    ReasonCode,
    Channel = #channel{ctx = Ctx, conninfo = ConnInfo}
) ->
    Reason = returncode_name(ReasonCode),
    _ = run_hooks(Ctx, 'client.connack', [ConnInfo, Reason], #{}),
    AckPacket = ?SN_CONNACK_MSG(ReasonCode),
    shutdown(Reason, AckPacket, Channel);
handle_out(publish, Publishes, Channel) ->
    {Replies1, NChannel} = outgoing_deliver_and_register(
        do_deliver(Publishes, Channel)
    ),
    {Replies2, NChannel2} = goto_asleep_if_buffered_msgs_sent(NChannel),
    {ok, Replies1 ++ Replies2, NChannel2};
handle_out(puback, {TopicId, MsgId, RC}, Channel) ->
    {ok, {outgoing, ?SN_PUBACK_MSG(TopicId, MsgId, RC)}, Channel};
handle_out(pubrec, MsgId, Channel) ->
    {ok, {outgoing, ?SN_PUBREC_MSG(?SN_PUBREC, MsgId)}, Channel};
handle_out(pubrel, MsgId, Channel) ->
    {ok, {outgoing, ?SN_PUBREC_MSG(?SN_PUBREL, MsgId)}, Channel};
handle_out(pubcomp, MsgId, Channel) ->
    {ok, {outgoing, ?SN_PUBREC_MSG(?SN_PUBCOMP, MsgId)}, Channel};
handle_out(
    register,
    {TopicId, TopicName},
    Channel = #channel{
        session = Session,
        register_inflight = undefined
    }
) ->
    {MsgId, NSession} = emqx_mqttsn_session:obtain_next_pkt_id(Session),
    Outgoing = {outgoing, ?SN_REGISTER_MSG(TopicId, MsgId, TopicName)},
    NChannel = Channel#channel{
        session = NSession,
        register_inflight = {TopicId, MsgId, TopicName}
    },
    {ok, Outgoing, ensure_register_timer(NChannel)};
handle_out(
    register,
    {TopicId, TopicName},
    Channel = #channel{
        register_inflight = Inflight,
        register_awaiting_queue = RAQueue
    }
) ->
    case enqueue_register_request({TopicId, TopicName}, Inflight, RAQueue) of
        ignore ->
            ?SLOG(debug, #{
                msg => "ingore_register_request_to_client",
                register_request =>
                    #{
                        topic_id => TopicId,
                        topic_name => TopicName
                    }
            }),
            {ok, Channel};
        NRAQueue ->
            ?SLOG(debug, #{
                msg => "put_register_msg_into_awaiting_queue",
                register_request =>
                    #{
                        topic_id => TopicId,
                        topic_name => TopicName
                    },
                register_awaiting_queue_size => length(NRAQueue)
            }),
            {ok, Channel#channel{register_awaiting_queue = NRAQueue}}
    end;
handle_out(disconnect, RC, Channel) ->
    DisPkt = ?SN_DISCONNECT_MSG(undefined),
    Reason =
        case is_atom(RC) of
            true -> RC;
            false -> returncode_name(RC)
        end,
    {ok, [{outgoing, DisPkt}, {close, Reason}], Channel}.

enqueue_register_request({_, TopicName}, {_, _, TopicName}, _RAQueue) ->
    ignore;
enqueue_register_request({TopicId, TopicName}, _, RAQueue) ->
    HasQueued = lists:any(fun({_, T}) -> T == TopicName end, RAQueue),
    case HasQueued of
        true -> ignore;
        false -> RAQueue ++ [{TopicId, TopicName}]
    end.

%%--------------------------------------------------------------------
%% Return ConnAck
%%--------------------------------------------------------------------

return_connack(AckPacket, Channel) ->
    Replies1 = [{event, connected}, {outgoing, AckPacket}],
    {Replies2, NChannel} = maybe_resume_session(Channel),
    {ok, Replies1 ++ Replies2, NChannel}.

%%--------------------------------------------------------------------
%% Maybe Resume Session

maybe_resume_session(Channel = #channel{resuming = false}) ->
    {[], Channel};
maybe_resume_session(
    Channel = #channel{
        session = Session,
        resuming = true
    }
) ->
    Subs = emqx_mqttsn_session:info(subscriptions, Session),
    case subs_resume() andalso map_size(Subs) =/= 0 of
        true ->
            TopicNames = lists:filter(fun(T) -> not emqx_topic:wildcard(T) end, maps:keys(Subs)),
            Registers = lists:map(fun(T) -> {register, T} end, TopicNames),
            {Registers, Channel};
        false ->
            resume_or_replay_messages(Channel)
    end.

resume_or_replay_messages(
    Channel = #channel{
        resuming = Resuming,
        pendings = Pendings,
        session = Session,
        clientinfo = ClientInfo
    }
) ->
    {NPendings, NChannel} =
        case Resuming of
            true ->
                {Pendings, Channel#channel{resuming = false, pendings = []}};
            false ->
                {[], Channel}
        end,
    {ok, Publishes, Session1} = emqx_mqttsn_session:replay(ClientInfo, Session),
    {NPublishes, NSession} =
        case emqx_mqttsn_session:deliver(ClientInfo, NPendings, Session1) of
            {ok, Session2} ->
                {Publishes, Session2};
            {ok, More, Session2} ->
                {lists:append(Publishes, More), Session2}
        end,
    outgoing_deliver_and_register(
        do_deliver(NPublishes, NChannel#channel{session = NSession})
    ).

subs_resume() ->
    emqx:get_config([gateway, mqttsn, subs_resume]).

%%--------------------------------------------------------------------
%% Deliver publish: broker -> client
%%--------------------------------------------------------------------

do_deliver({pubrel, MsgId}, Channel) ->
    {[?SN_PUBREC_MSG(?SN_PUBREL, MsgId)], Channel};
do_deliver(
    {MsgId, Msg},
    Channel = #channel{
        ctx = Ctx,
        clientinfo =
            ClientInfo =
                #{mountpoint := Mountpoint}
    }
) ->
    metrics_inc(Ctx, 'messages.delivered'),
    Msg1 = run_hooks_without_metrics(
        Ctx,
        'message.delivered',
        [ClientInfo],
        emqx_message:update_expiry(Msg)
    ),
    Msg2 = emqx_mountpoint:unmount(Mountpoint, Msg1),
    Packet = message_to_packet(MsgId, Msg2, Channel),
    {[Packet], Channel};
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

outgoing_deliver_and_register({Packets, Channel}) ->
    {NPackets, NRegisters} =
        lists:foldl(
            fun(P, {Acc0, Acc1}) ->
                case P of
                    {register, _} ->
                        {Acc0, [P | Acc1]};
                    _ ->
                        {[P | Acc0], Acc1}
                end
            end,
            {[], []},
            Packets
        ),
    {[{outgoing, lists:reverse(NPackets)}] ++ lists:reverse(NRegisters), Channel}.

message_to_packet(
    MsgId,
    Message,
    #channel{session = Session}
) ->
    QoS = emqx_message:qos(Message),
    Topic = emqx_message:topic(Message),
    Payload = emqx_message:payload(Message),
    NMsgId =
        case QoS of
            ?QOS_0 -> 0;
            _ -> MsgId
        end,
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:lookup_topic_id(Topic, Registry) of
        {predef, PredefTopicId} ->
            Flags = #mqtt_sn_flags{qos = QoS, topic_id_type = ?SN_PREDEFINED_TOPIC},
            ?SN_PUBLISH_MSG(Flags, PredefTopicId, NMsgId, Payload);
        TopicId when is_integer(TopicId) ->
            Flags = #mqtt_sn_flags{qos = QoS, topic_id_type = ?SN_NORMAL_TOPIC},
            ?SN_PUBLISH_MSG(Flags, TopicId, NMsgId, Payload);
        undefined when byte_size(Topic) =:= 2 ->
            Flags = #mqtt_sn_flags{qos = QoS, topic_id_type = ?SN_SHORT_TOPIC},
            ?SN_PUBLISH_MSG(Flags, Topic, NMsgId, Payload);
        undefined ->
            {register, Topic}
    end.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), From :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {reply, Reply :: term(), replies(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), emqx_types:packet(), channel()}.
handle_call({subscribe, Topic, SubOpts}, _From, Channel) ->
    case do_subscribe({?SN_INVALID_TOPIC_ID, Topic, SubOpts}, Channel) of
        {ok, {_, NTopicName, NSubOpts}, NChannel} ->
            reply_and_update({ok, {NTopicName, NSubOpts}}, NChannel);
        {error, ?SN_RC2_EXCEED_LIMITATION} ->
            reply({error, exceed_limitation}, Channel)
    end;
handle_call({unsubscribe, Topic}, _From, Channel) ->
    TopicFilters = [emqx_topic:parse(Topic)],
    {ok, _, NChannel} = do_unsubscribe(TopicFilters, Channel),
    reply_and_update(ok, NChannel);
handle_call(subscriptions, _From, Channel = #channel{session = Session}) ->
    reply({ok, maps:to_list(emqx_mqttsn_session:info(subscriptions, Session))}, Channel);
handle_call(kick, _From, Channel) ->
    NChannel = ensure_disconnected(kicked, Channel),
    shutdown_and_reply(kicked, ok, NChannel);
handle_call(discard, _From, Channel) ->
    shutdown_and_reply(discarded, ok, Channel);
handle_call({takeover, 'begin'}, _From, Channel = #channel{session = Session}) ->
    %% In MQTT-SN the meaning of a “clean session” is extended to the Will
    %% feature, i.e. not only the subscriptions are persistent, but also the
    %% Will topic and the Will message. [6.3]
    %%
    %% FIXME: We need to reply WillMsg and Session
    reply(Session, Channel#channel{takeover = true});
handle_call(
    {takeover, 'end'},
    _From,
    Channel = #channel{
        session = Session,
        pendings = Pendings
    }
) ->
    ok = emqx_mqttsn_session:takeover(Session),
    %% TODO: Should not drain deliver here (side effect)
    Delivers = emqx_utils:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    shutdown_and_reply(takenover, AllPendings, Channel);
%handle_call(list_authz_cache, _From, Channel) ->
%    {reply, emqx_authz_cache:list_authz_cache(), Channel};

%% XXX: No Quota Now
% handle_call({quota, Policy}, _From, Channel) ->
%     Zone = info(zone, Channel),
%     Quota = emqx_limiter:init(Zone, Policy),
%     reply(ok, Channel#channel{quota = Quota});

handle_call(Req, _From, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_call",
        call => Req
    }),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Cast
%%--------------------------------------------------------------------

-spec handle_cast(Req :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.
handle_cast(_Req, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}.

handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = idle}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = connecting}
) ->
    shutdown(Reason, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{
        conn_state = connected,
        clientinfo = _ClientInfo
    }
) ->
    %% XXX: Flapping detect ???
    %% How to get the flapping detect policy ???
    %emqx_zone:enable_flapping_detect(Zone)
    %    andalso emqx_flapping:detect(ClientInfo),
    NChannel = ensure_disconnected(Reason, mabye_publish_will_msg(Channel)),
    case maybe_shutdown(Reason, NChannel) of
        {ok, NChannel1} -> {ok, {event, disconnected}, NChannel1};
        Shutdown -> Shutdown
    end;
handle_info(
    {sock_closed, Reason},
    Channel = #channel{conn_state = disconnected}
) ->
    ?SLOG(error, #{
        msg => "unexpected_sock_closed",
        reason => Reason
    }),
    {ok, Channel};
handle_info(clean_authz_cache, Channel) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    {ok, Channel};
handle_info({subscribe, _}, Channel) ->
    {ok, Channel};
handle_info({register, TopicName}, Channel = #channel{session = Session}) ->
    Registry = emqx_mqttsn_session:registry(Session),
    case emqx_mqttsn_registry:reg(TopicName, Registry) of
        {error, Reason} ->
            ?SLOG(error, #{
                msg => "register_topic_failed",
                topic_name => TopicName,
                reason => Reason
            }),
            {ok, Channel};
        {ok, TopicId, NRegistry} ->
            NSession = emqx_mqttsn_session:set_registry(NRegistry, Session),
            handle_out(register, {TopicId, TopicName}, Channel#channel{session = NSession})
    end;
handle_info(Info, Channel) ->
    ?SLOG(error, #{
        msg => "unexpected_info",
        info => Info
    }),
    {ok, Channel}.

maybe_shutdown(Reason, Channel = #channel{conninfo = ConnInfo}) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?UINT_MAX ->
            {ok, Channel};
        I when I > 0 ->
            {ok, ensure_timer(expire_session, I, Channel)};
        _ ->
            shutdown(Reason, Channel)
    end.

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(
    Reason,
    Channel = #channel{
        ctx = Ctx,
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    ok = run_hooks(
        Ctx,
        'client.disconnected',
        [ClientInfo, Reason, NConnInfo]
    ),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

mabye_publish_will_msg(Channel = #channel{will_msg = undefined}) ->
    Channel;
mabye_publish_will_msg(Channel = #channel{will_msg = WillMsg}) ->
    ok = publish_will_msg(put_message_headers(WillMsg, Channel)),
    Channel#channel{will_msg = undefined}.

publish_will_msg(Msg) ->
    _ = emqx_broker:publish(Msg),
    ok.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}.
handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        conn_state = ConnState,
        session = Session,
        clientinfo = ClientInfo = #{clientid := ClientId}
    }
) when
    ConnState =:= disconnected;
    ConnState =:= asleep
->
    NSession = emqx_mqttsn_session:enqueue(
        ClientInfo,
        ignore_local(maybe_nack(Delivers), ClientId, Session, Ctx),
        Session
    ),
    {ok, Channel#channel{session = NSession}};
%% There are two scenarios need to cache delivering messages:
%%  1. it is being takeover by other channel
%%  2. it is being resume registered topic-names
handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        takeover = Takeover,
        pendings = Pendings,
        session = Session,
        resuming = Resuming,
        clientinfo = #{clientid := ClientId}
    }
) when
    Takeover == true; Resuming == true
->
    NPendings = lists:append(
        Pendings,
        ignore_local(maybe_nack(Delivers), ClientId, Session, Ctx)
    ),
    {ok, Channel#channel{pendings = NPendings}};
handle_deliver(
    Delivers,
    Channel = #channel{
        ctx = Ctx,
        session = Session,
        clientinfo = ClientInfo = #{clientid := ClientId}
    }
) ->
    case
        emqx_mqttsn_session:deliver(
            ClientInfo,
            ignore_local(Delivers, ClientId, Session, Ctx),
            Session
        )
    of
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(
                publish,
                Publishes,
                ensure_timer(retry_delivery, NChannel)
            );
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end.

ignore_local(Delivers, Subscriber, Session, Ctx) ->
    Subs = emqx_mqttsn_session:info(subscriptions, Session),
    lists:filter(
        fun({deliver, Topic, #message{from = Publisher}}) ->
            case maps:find(Topic, Subs) of
                {ok, #{nl := 1}} when Subscriber =:= Publisher ->
                    ok = metrics_inc(Ctx, 'delivery.dropped'),
                    ok = metrics_inc(Ctx, 'delivery.dropped.no_local'),
                    false;
                _ ->
                    true
            end
        end,
        Delivers
    ).

%% Nack delivers from shared subscription
maybe_nack(Delivers) ->
    lists:filter(fun not_nacked/1, Delivers).

not_nacked({deliver, _Topic, Msg}) ->
    not (emqx_shared_sub:is_ack_required(Msg) andalso
        (ok == emqx_shared_sub:nack_no_connection(Msg))).

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
    Channel = #channel{conn_state = ConnState}
) when
    ConnState =:= disconnected;
    ConnState =:= asleep
->
    {ok, Channel};
handle_timeout(
    _TRef,
    {keepalive, StatVal},
    Channel = #channel{keepalive = Keepalive}
) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(keepalive, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, ?SN_RC2_KEEPALIVE_TIMEOUT, Channel)
    end;
handle_timeout(
    _TRef,
    retry_delivery,
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    retry_delivery = TimerName,
    Channel = #channel{conn_state = asleep}
) ->
    {ok, reset_timer(TimerName, Channel)};
handle_timeout(
    _TRef,
    expire_awaiting_rel,
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    expire_awaiting_rel = TimerName,
    Channel = #channel{conn_state = asleep}
) ->
    {ok, reset_timer(TimerName, Channel)};
handle_timeout(
    _TRef,
    TimerName,
    Channel = #channel{session = Session, clientinfo = ClientInfo}
) when TimerName == retry_delivery; TimerName == expire_awaiting_rel ->
    case emqx_mqttsn_session:handle_timeout(ClientInfo, TimerName, Session) of
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(publish, Publishes, clean_timer(TimerName, NChannel));
        {ok, Publishes, Timeout, NSession} ->
            NChannel = Channel#channel{session = NSession},
            %% XXX: These replay messages should awaiting register acked?
            handle_out(publish, Publishes, reset_timer(TimerName, Timeout, NChannel))
    end;
handle_timeout(
    _TRef,
    {retry_register, RetryTimes},
    Channel = #channel{register_inflight = {TopicId, MsgId, TopicName}}
) ->
    case RetryTimes < ?MAX_RETRY_TIMES of
        true ->
            Outgoing = {outgoing, ?SN_REGISTER_MSG(TopicId, MsgId, TopicName)},
            {ok, Outgoing, ensure_register_timer(RetryTimes + 1, Channel)};
        false ->
            ?SLOG(error, #{
                msg => "register_topic_reached_max_retry_times",
                register_request =>
                    #{
                        topic_id => TopicId,
                        msg_id => MsgId,
                        topic_name => TopicName
                    }
            }),
            handle_out(disconnect, ?SN_RC2_REACHED_MAX_RETRY, Channel)
    end;
handle_timeout(_TRef, expire_session, Channel) ->
    shutdown(expired, Channel);
handle_timeout(_TRef, expire_asleep, Channel) ->
    shutdown(asleep_timeout, Channel);
handle_timeout(_TRef, connection_expire, Channel) ->
    NChannel = clean_timer(connection_expire, Channel),
    handle_out(disconnect, expired, NChannel);
handle_timeout(_TRef, Msg, Channel) ->
    %% NOTE
    %% We do not expect `emqx_mqttsn_session` to set up any custom timers (i.e with
    %% `emqx_session:ensure_timer/3`), because `emqx_session_mem` doesn't use any.
    ?SLOG(error, #{
        msg => "unexpected_timeout",
        timeout_msg => Msg
    }),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(_Reason, _Channel) ->
    ok.

reply(Reply, Channel) ->
    {reply, Reply, Channel}.

reply_and_update(Reply, Channel) ->
    {reply, Reply, [{event, updated}], Channel}.

shutdown(Reason, Channel) ->
    {shutdown, Reason, Channel}.

shutdown(Reason, AckFrame, Channel) ->
    {shutdown, Reason, AckFrame, Channel}.

shutdown_and_reply(Reason, Reply, Channel) ->
    {shutdown, Reason, Reply, Channel}.

%%--------------------------------------------------------------------
%% Will

update_will_topic(
    undefined,
    #mqtt_sn_flags{qos = QoS, retain = Retain},
    Topic,
    ClientId
) ->
    WillMsg0 = emqx_message:make(ClientId, QoS, Topic, <<>>),
    emqx_message:set_flag(retain, Retain, WillMsg0);
update_will_topic(
    Will,
    #mqtt_sn_flags{qos = QoS, retain = Retain},
    Topic,
    _ClientId
) ->
    emqx_message:set_flag(
        retain,
        Retain,
        Will#message{qos = QoS, topic = Topic}
    ).

update_will_msg(Will, Payload) ->
    Will#message{payload = Payload}.

%%--------------------------------------------------------------------
%% Timer

ensure_asleep_timer(Channel = #channel{asleep_timer_duration = Duration}) when
    is_integer(Duration)
->
    ensure_asleep_timer(Duration, Channel).

ensure_asleep_timer(Durtion, Channel) ->
    ensure_timer(
        expire_asleep,
        timer:seconds(Durtion),
        Channel#channel{asleep_timer_duration = Durtion}
    ).

ensure_register_timer(Channel) ->
    ensure_register_timer(0, Channel).

ensure_register_timer(RetryTimes, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(?REGISTER_TIMEOUT, {retry_register, RetryTimes}),
    Channel#channel{timers = Timers#{retry_register => TRef}}.

cancel_timer(Name, Channel = #channel{timers = Timers}) ->
    case maps:get(Name, Timers, undefined) of
        undefined ->
            Channel;
        TRef ->
            emqx_utils:cancel_timer(TRef),
            Channel#channel{timers = maps:without([Name], Timers)}
    end.

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso is_integer(Time) andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(Time, Name),
    Channel#channel{timers = Timers#{Name => TRef}}.

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

reset_timer(Name, Time, Channel) ->
    ensure_timer(Name, Time, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    Channel#channel{timers = maps:remove(Name, Timers)}.

interval(keepalive, #channel{keepalive = Keepalive}) ->
    emqx_keepalive:info(check_interval, Keepalive);
interval(retry_delivery, #channel{session = Session}) ->
    emqx_mqttsn_session:info(retry_interval, Session);
interval(expire_awaiting_rel, #channel{session = Session}) ->
    emqx_mqttsn_session:info(await_rel_timeout, Session).
%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

run_hooks(Ctx, Name, Args) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run(Name, Args).

run_hooks(Ctx, Name, Args, Acc) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name),
    emqx_hooks:run_fold(Name, Args, Acc).

run_hooks_without_metrics(_Ctx, Name, Args) ->
    emqx_hooks:run(Name, Args).

run_hooks_without_metrics(_Ctx, Name, Args, Acc) ->
    emqx_hooks:run_fold(Name, Args, Acc).

metrics_inc(Ctx, Name) ->
    emqx_gateway_ctx:metrics_inc(Ctx, Name).

returncode_name(?SN_RC_ACCEPTED) -> accepted;
returncode_name(?SN_RC_CONGESTION) -> rejected_congestion;
returncode_name(?SN_RC_INVALID_TOPIC_ID) -> rejected_invaild_topic_id;
returncode_name(?SN_RC_NOT_SUPPORTED) -> rejected_not_supported;
returncode_name(?SN_RC2_NOT_AUTHORIZE) -> rejected_not_authorize;
returncode_name(?SN_RC2_FAILED_SESSION) -> rejected_failed_open_session;
returncode_name(?SN_RC2_KEEPALIVE_TIMEOUT) -> rejected_keepalive_timeout;
returncode_name(?SN_RC2_EXCEED_LIMITATION) -> rejected_exceed_limitation;
returncode_name(?SN_RC2_REACHED_MAX_RETRY) -> reached_max_retry_times;
returncode_name(_) -> accepted.

name_to_returncode(not_authorized) -> ?SN_RC2_NOT_AUTHORIZE.
