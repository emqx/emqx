%%--------------------------------------------------------------------
%% Copyright (c) 2019-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% MQTT Channel
-module(emqx_channel).

-include("emqx.hrl").
-include("emqx_channel.hrl").
-include("emqx_session.hrl").
-include("emqx_mqtt.hrl").
-include("emqx_access_control.hrl").
-include("logger.hrl").
-include("types.hrl").
-include("emqx_external_trace.hrl").
-include("emqx_config.hrl").

-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    handle_cast/2,
    handle_info/2,
    handle_signal/2,
    terminate/2
]).

%% Export for emqx_sn
-export([
    do_deliver/2,
    ensure_keepalive/2,
    clear_keepalive/1
]).

%% Export for emqx_channel implementations
-export([
    maybe_nack/1
]).

%% Export for DS session GC worker and session implementations
-export([
    will_delay_interval/1,
    prepare_will_message_for_publishing/2
]).

%% Exports for manual inspection/debugging only
-export([inspect/1]).

%% Exports for tests
-ifdef(TEST).
-export([
    dummy/0,
    set_field/3,
    set_log_meta/2
]).
-endif.

-export_type([channel/0, opts/0, conn_state/0, reply/0, replies/0]).

-record(takeover, {
    %% Pending delivers when takeovering
    pendings :: [emqx_types:deliver()]
}).

-record(resumption, {
    %% Replay context
    replayctx
}).

-record(channel, {
    %% MQTT ConnInfo
    conninfo :: emqx_types:conninfo(),
    %% MQTT ClientInfo
    clientinfo :: emqx_types:clientinfo(),
    %% MQTT Session
    session :: option(emqx_session:t()),
    %% KeepAlive
    keepalive :: option(emqx_keepalive:keepalive()),
    %% MQTT Will Msg
    will_msg :: option(emqx_types:message()),
    %% MQTT Topic Aliases
    topic_aliases :: emqx_types:topic_aliases(),
    %% MQTT Topic Alias Maximum
    alias_maximum :: option(map()),
    %% Authentication Data Cache
    auth_cache :: option(map()),
    %% Quota checkers
    quota :: option(emqx_limiter_client_container:t()),
    %% Timers
    timers :: #{
        %% Common timers
        atom() => disabled | option(reference()),
        %% Other timers (e.g.: requested by sessions)
        any() => option(reference())
    },
    %% Conn State
    conn_state :: conn_state(),
    %% Conn Flags
    conn_flags :: ordsets:ordset(atom()),
    %% Takeover
    takeover :: undefined | #takeover{} | #resumption{}
}).

-type subscribe_filter() ::
    {emqx_types:topic() | emqx_types:share(), emqx_types:subopts()}.
-type subscribe_topic_filters() ::
    [subscribe_filter() | {subscribe_filter(), emqx_types:reason_code()}].
-record(subscribe_operation, {
    properties = #{} :: emqx_types:properties(),
    topic_filters = [] :: subscribe_topic_filters()
}).
-type subscribe_operation() :: #subscribe_operation{}.

-opaque channel() :: #channel{}.

-type opts() :: #{
    zone := atom(),
    listener := {Type :: atom(), Name :: atom()},
    atom() => term()
}.

%%  init
-type conn_state() ::
    idle
    %% mqtt connect recved but not acked
    | connecting
    %% mqtt connect acked
    | connected
    %% mqtt connected but reauthenticating
    | reauthenticating
    %% keepalive timeout or connection terminated
    | disconnected.

-type reply() ::
    {outgoing, emqx_types:packet()}
    | {outgoing, [emqx_types:packet()]}
    | {connack, emqx_types:packet()}
    | {event, conn_state() | updated}
    | {close, Reason :: atom()}.

-type replies() :: reply() | [reply()].

-define(IS_MQTT_V5, #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}).
-define(IS_CONNECTED_OR_REAUTHENTICATING(ConnState),
    ((ConnState == connected) orelse (ConnState == reauthenticating))
).

%% Timers implemented by sessions
-define(IS_COMMON_SESSION_TIMER(N),
    ((N == retry_delivery) orelse (N == expire_awaiting_rel))
).
%% Timers implemented by sessions that need to be handled only when the client is connected
-define(IS_COMMON_SESSION_ONLINE_TIMER(N),
    (N == retry_delivery)
).

-define(chan_terminating, chan_terminating).
-define(normal, normal).
-define(RAND_CLIENTID_BYTES, 16).

-dialyzer({no_match, [shutdown/4, ensure_timer/2, interval/2]}).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec info(channel()) -> emqx_types:infos().
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec info(list(atom()) | atom() | tuple(), channel()) -> term().
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
info(is_superuser, #channel{clientinfo = ClientInfo}) ->
    maps:get(is_superuser, ClientInfo, undefined);
info(expire_at, #channel{clientinfo = ClientInfo}) ->
    maps:get(expire_at, ClientInfo, undefined);
info(zone, #channel{clientinfo = ClientInfo}) ->
    maps:get(zone, ClientInfo);
info(listener, #channel{clientinfo = ClientInfo}) ->
    maps:get(listener, ClientInfo);
info(clientid, #channel{clientinfo = ClientInfo}) ->
    maps:get(clientid, ClientInfo, undefined);
info(username, #channel{clientinfo = ClientInfo}) ->
    maps:get(username, ClientInfo, undefined);
info(is_bridge, #channel{clientinfo = ClientInfo}) ->
    maps:get(is_bridge, ClientInfo, undefined);
info(session, #channel{session = Session}) ->
    emqx_utils:maybe_apply(fun emqx_session:info/1, Session);
info({session, Info}, #channel{session = Session}) ->
    emqx_utils:maybe_apply(fun(S) -> emqx_session:info(Info, S) end, Session);
info(conn_state, #channel{conn_state = ConnState}) ->
    ConnState;
info(keepalive, #channel{keepalive = KeepAlive}) ->
    emqx_utils:maybe_apply(fun emqx_keepalive:info/1, KeepAlive);
info(will_msg, #channel{will_msg = undefined}) ->
    undefined;
info(will_msg, #channel{will_msg = WillMsg}) ->
    emqx_message:to_map(WillMsg);
info(topic_aliases, #channel{topic_aliases = Aliases}) ->
    Aliases;
info(alias_maximum, #channel{alias_maximum = Limits}) ->
    Limits;
info(timers, #channel{timers = Timers}) ->
    Timers;
info(session_state, #channel{session = Session}) ->
    Session;
info(impl, #channel{session = Session}) ->
    emqx_session:info(impl, Session);
info(namespace, #channel{clientinfo = ClientInfo}) ->
    get_tenant_namespace(ClientInfo).

inspect(#channel{} = Channel) ->
    lists:foldl(
        fun({I, Field}, Acc) ->
            Acc#{Field => element(I, Channel)}
        end,
        #{},
        lists:enumerate(2, record_info(fields, channel))
    ).

-spec set_conn_state(conn_state(), channel()) -> channel().
set_conn_state(ConnState, Channel) ->
    Channel#channel{conn_state = ConnState}.

-spec stats(channel()) -> emqx_types:stats().
stats(#channel{session = undefined}) ->
    emqx_pd:get_counters(?CHANNEL_METRICS);
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
        peername := {PeerHost, PeerPort} = PeerName,
        sockname := {_Host, SockPort}
    },
    #{
        zone := Zone,
        listener := {Type, Listener}
    } = Opts
) ->
    PeerCert = maps:get(peercert, ConnInfo, undefined),
    Protocol = maps:get(protocol, ConnInfo, mqtt),
    MountPoint =
        case emqx_config:get_listener_conf(Type, Listener, [mountpoint]) of
            <<>> -> undefined;
            MP -> MP
        end,
    ListenerId = emqx_listeners:listener_id(Type, Listener),
    ClientInfo = set_peercert_infos(
        PeerCert,
        #{
            zone => Zone,
            listener => ListenerId,
            protocol => Protocol,
            peerhost => PeerHost,
            peerport => PeerPort,
            %% We copy peername to clientinfo because some event contexts only have access
            %% to client info (e.g.: authn/authz).
            peername => PeerName,
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
    {NClientInfo, NConnInfo0} = take_conn_info_fields([ws_cookie, peersni], ClientInfo, ConnInfo),
    ok = validate_clientinfo_string(peersni, maps:get(peersni, NClientInfo, undefined)),
    NConnInfo = maybe_quic_shared_state(NConnInfo0, Opts),
    #channel{
        conninfo = NConnInfo,
        clientinfo = NClientInfo,
        topic_aliases = #{
            inbound => #{},
            outbound => #{}
        },
        auth_cache = #{},
        timers = #{},
        conn_state = idle,
        conn_flags = []
    }.

maybe_quic_shared_state(ConnInfo, #{conn_shared_state := QSS}) ->
    ConnInfo#{conn_shared_state => QSS};
maybe_quic_shared_state(ConnInfo, _) ->
    ConnInfo.

set_peercert_infos(NoSSL, ClientInfo, _) when
    NoSSL =:= nossl;
    NoSSL =:= undefined
->
    ClientInfo#{username => undefined};
set_peercert_infos(PeerCert, ClientInfo, Zone) ->
    DN = esockd_peercert:subject(PeerCert),
    CN = esockd_peercert:common_name(PeerCert),
    ok = validate_peercert_string(dn, DN, PeerCert),
    ok = validate_peercert_string(cn, CN, PeerCert),
    PeercetAs = fun(Key) ->
        case get_mqtt_conf(Zone, Key) of
            cn -> CN;
            dn -> DN;
            crt -> PeerCert;
            pem when is_binary(PeerCert) -> base64:encode(PeerCert);
            md5 when is_binary(PeerCert) -> emqx_passwd:hash_data(md5, PeerCert);
            _ -> undefined
        end
    end,
    Username = PeercetAs(peer_cert_as_username),
    ClientId = PeercetAs(peer_cert_as_clientid),
    ClientInfo#{username => Username, clientid => ClientId, dn => DN, cn => CN}.

%% Reject control characters (0x00-0x1F, 0x7F-0x9F) in peer certificate strings.
%% Mirrors the byte-class check `emqx_frame:validate_utf8/1` applies to
%% MQTT-ingested clientid/username/password. Closes a CRLF-injection primitive
%% where bytes sourced from a peer certificate or PROXY-Protocol v2 SSL TLV could
%% be smuggled into outbound HTTP authn/authz/bridge requests via
%% `${cert_common_name}` / `${cert_subject}` / `${cert_san.*}`.
validate_peercert_string(_Field, undefined, _Source) ->
    ok;
validate_peercert_string(Field, Value, Source) when is_binary(Value) ->
    case emqx_utils:is_mqtt_safe_utf8(Value) of
        true ->
            ok;
        false ->
            ?SLOG(warning, #{
                msg => "peer_certificate_field_rejected",
                field => Field,
                source => peercert_source(Source),
                reason => contains_control_characters
            }),
            erlang:exit({shutdown, peercert_field_invalid})
    end.

peercert_source(PeerCert) when is_binary(PeerCert) -> tls_certificate;
peercert_source(PP2Info) when is_list(PP2Info) -> proxy_protocol_v2;
peercert_source(_) -> unknown.

%% Same byte-class check as `validate_peercert_string/3', for binary fields
%% that flow into `ClientInfo' from the connection layer (currently `peersni',
%% which comes from the TLS handshake SNI or the PROXY-Protocol v2
%% `pp2_authority' TLV). On bad bytes, terminate the connection and log a
%% warning, mirroring how `emqx_frame:validate_utf8/1' rejects MQTT-ingested
%% UTF-8 strings.
validate_clientinfo_string(_Field, undefined) ->
    ok;
validate_clientinfo_string(Field, Value) when is_binary(Value) ->
    case emqx_utils:is_mqtt_safe_utf8(Value) of
        true ->
            ok;
        false ->
            ?SLOG(warning, #{
                msg => "clientinfo_field_rejected",
                field => Field,
                reason => contains_control_characters
            }),
            erlang:exit({shutdown, clientinfo_field_invalid})
    end.

take_conn_info_fields(Fields, ClientInfo, ConnInfo) ->
    lists:foldl(
        fun(Field, {ClientInfo0, ConnInfo0}) ->
            case maps:take(Field, ConnInfo0) of
                {Value, NConnInfo} ->
                    {ClientInfo0#{Field => Value}, NConnInfo};
                _ ->
                    {ClientInfo0, ConnInfo0}
            end
        end,
        {ClientInfo, ConnInfo},
        Fields
    ).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec handle_in(emqx_types:packet(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {continue, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_in(?CONNECT_PACKET(), Channel = #channel{conn_state = ConnState}) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    ?TRACE("MQTT", "unexpected_connect_packet", #{conn_state => ConnState}),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
handle_in(?CONNECT_PACKET(), Channel = #channel{conn_state = connecting}) ->
    ?TRACE("MQTT", "unexpected_connect_packet", #{conn_state => connecting}),
    handle_out(connack, ?RC_PROTOCOL_ERROR, Channel);
handle_in(?PACKET(?CONNECT) = Packet, Channel) ->
    ?EXT_TRACE_CLIENT_CONNECT(
        ?EXT_TRACE_ATTR(connect_attrs(Packet, Channel)),
        fun(NPacket) ->
            process_connect(NPacket, Channel)
        end,
        [Packet]
    );
%% TODO: trace CONNECT with AUTH
handle_in(
    Packet = ?AUTH_PACKET(ReasonCode, _Properties),
    Channel = #channel{conn_state = ConnState}
) ->
    try
        case {ReasonCode, ConnState} of
            {?RC_CONTINUE_AUTHENTICATION, connecting} ->
                ok;
            {?RC_CONTINUE_AUTHENTICATION, reauthenticating} ->
                ok;
            {?RC_RE_AUTHENTICATE, connected} ->
                ok;
            _ ->
                ?TRACE("MQTT", "unexpected_auth_packet", #{conn_state => ConnState}),
                error(protocol_error)
        end,
        case authenticate(Packet, Channel) of
            {ok, NProperties, NChannel} ->
                case ConnState of
                    connecting ->
                        post_process_connect(NProperties, NChannel);
                    _ ->
                        handle_out(
                            auth,
                            {?RC_SUCCESS, NProperties},
                            NChannel#channel{conn_state = connected}
                        )
                end;
            {continue, NProperties, NChannel} ->
                NConnState =
                    case ConnState of
                        connected -> reauthenticating;
                        _ -> ConnState
                    end,
                handle_out(
                    auth,
                    {?RC_CONTINUE_AUTHENTICATION, NProperties},
                    NChannel#channel{conn_state = NConnState}
                );
            {error, NReasonCode, NChannel} ->
                case ConnState of
                    connecting ->
                        handle_out(connack, NReasonCode, NChannel);
                    _ ->
                        handle_out(disconnect, NReasonCode, NChannel)
                end;
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
handle_in(?PACKET(Type), Channel = #channel{conn_state = ConnState}) when
    ConnState =/= connected andalso ConnState =/= reauthenticating
->
    ?TRACE("MQTT", "unexpected_packet", #{type => Type, conn_state => ConnState}),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
handle_in(?PUBLISH_PACKET(_QoS, _Topic, _PacketId) = Packet, Channel) ->
    case emqx_packet:check(Packet) of
        ok ->
            ?EXT_TRACE_CLIENT_PUBLISH(
                ?EXT_TRACE_ATTR((basic_attrs(Channel))#{'message.topic' => _Topic}),
                fun(NPacket) -> process_publish(NPacket, Channel) end,
                [Packet]
            );
        {error, ReasonCode} ->
            ?TRACE("MQTT", "invalid_publish_packet", #{reason => emqx_reason_codes:name(ReasonCode)}),
            handle_out(disconnect, ReasonCode, Channel)
    end;
handle_in(
    ?PACKET(?PUBACK) = Packet,
    Channel
) ->
    ?EXT_TRACE_CLIENT_PUBACK(
        ?EXT_TRACE_ATTR(basic_attrs(Channel)),
        fun(NPacket) -> process_puback(NPacket, Channel) end,
        [Packet]
    );
handle_in(
    ?PACKET(?PUBREC) = Packet,
    Channel
) ->
    ?EXT_TRACE_CLIENT_PUBREC(
        ?EXT_TRACE_ATTR(basic_attrs(Channel)),
        fun(NPacket) -> process_pubrec(NPacket, Channel) end,
        [Packet]
    );
handle_in(
    ?PACKET(?PUBREL) = Packet,
    Channel
) ->
    ?EXT_TRACE_CLIENT_PUBREL(
        ?EXT_TRACE_ATTR(basic_attrs(Channel)),
        fun(NPacket) -> process_pubrel(NPacket, Channel) end,
        [Packet]
    );
handle_in(
    ?PACKET(?PUBCOMP) = Packet,
    Channel
) ->
    ?EXT_TRACE_CLIENT_PUBCOMP(
        ?EXT_TRACE_ATTR(basic_attrs(Channel)),
        fun(NPacket) -> process_pubcomp(NPacket, Channel) end,
        [Packet]
    );
handle_in(?SUBSCRIBE_PACKET(_PacketId, _Properties, _TopicFilters0) = Packet, Channel) ->
    ?EXT_TRACE_CLIENT_SUBSCRIBE(
        ?EXT_TRACE_ATTR(maps:merge(basic_attrs(Channel), topic_attrs(Packet))),
        fun(NPacket) -> process_subscribe(NPacket, Channel) end,
        [Packet]
    );
handle_in(
    Packet = ?UNSUBSCRIBE_PACKET(_PacketId, _Properties, _TopicFilters),
    Channel
) ->
    ?EXT_TRACE_CLIENT_UNSUBSCRIBE(
        ?EXT_TRACE_ATTR(maps:merge(basic_attrs(Channel), topic_attrs(Packet))),
        fun(NPacket) -> process_unsubscribe(NPacket, Channel) end,
        [Packet]
    );
handle_in(?PACKET(?PINGREQ), Channel = #channel{keepalive = KeepAlive}) ->
    {ok, NKeepAlive} = emqx_keepalive:check(KeepAlive),
    NChannel = Channel#channel{keepalive = NKeepAlive},
    _ = run_hooks(
        'client.ping',
        [NChannel#channel.clientinfo, NChannel#channel.conninfo],
        ok,
        NChannel
    ),
    {ok, ?REPLY_OUTGOING(?PACKET(?PINGRESP)), reset_timer(keepalive, NChannel)};
handle_in(
    ?PACKET(?DISCONNECT, _PktVar) = Packet,
    Channel
) ->
    ?EXT_TRACE_CLIENT_DISCONNECT(
        ?EXT_TRACE_ATTR((basic_attrs(Channel))#{
            'client.proto_name' => info(proto_name, Channel),
            'client.proto_ver' => info(proto_ver, Channel),
            'client.is_bridge' => info(is_bridge, Channel),
            'client.sockname' => emqx_utils:ntoa(info(sockname, Channel)),
            'client.peername' => emqx_utils:ntoa(info(peername, Channel)),
            'client.disconnect.reason' =>
                emqx_reason_codes:name(emqx_packet:info(reason_code, _PktVar)),
            'client.disconnect.reason_desc' => undefined
        }),
        fun(NPacket) -> process_disconnect(NPacket, Channel) end,
        [Packet]
    );
handle_in(?AUTH_PACKET(), Channel) ->
    handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);
handle_in({frame_error, Reason}, Channel) ->
    handle_frame_error(Reason, Channel);
handle_in(Packet, Channel) ->
    ?SLOG(error, #{msg => "disconnecting_due_to_unexpected_message", packet => Packet}),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel).

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(?CONNECT_PACKET(ConnPkt) = Packet, Channel0) ->
    case
        emqx_utils:pipeline(
            [
                fun overload_protection/2,
                fun enrich_conninfo/2,
                fun run_conn_hooks/2,
                fun check_connect/2,
                fun enrich_client/2,
                %% `set_log_meta' should happen after `enrich_client'
                %% because client ID assign and override.
                %% Even though authentication may also inject new attributes, we call it
                %% here so that next steps have more logger context, and call it again after auth.
                fun set_log_meta/2,
                fun check_banned/2,
                fun count_flapping_event/2
            ],
            ConnPkt,
            Channel0#channel{conn_state = connecting}
        )
    of
        {ok, NConnPkt, Channel1 = #channel{clientinfo = ClientInfo}} ->
            ?TRACE("MQTT", "mqtt_packet_received", #{packet => Packet}),
            %% NOTE: Set alias_maximum before authentication, because enhanced
            %% authentication completes in `handle_in(?AUTH_PACKET(...))', which
            %% reaches `post_process_connect/2' without the connect packet.
            Channel2 = Channel1#channel{
                alias_maximum = init_alias_maximum(NConnPkt, ClientInfo)
            },
            case authenticate(?CONNECT_PACKET(NConnPkt), Channel2) of
                {ok, Properties, Channel3} ->
                    Channel = Channel3#channel{
                        %% NOTE: Only store will_msg after successful authn.
                        will_msg = emqx_packet:will_msg(NConnPkt)
                    },
                    post_process_connect(Properties, Channel);
                {continue, Properties, Channel} ->
                    handle_out(auth, {?RC_CONTINUE_AUTHENTICATION, Properties}, Channel);
                {error, ReasonCode, Channel} ->
                    handle_out(connack, ReasonCode, Channel)
            end;
        {error, ReasonCode, NChannel} ->
            handle_out(connack, ReasonCode, NChannel)
    end.

post_process_connect(
    AckProps,
    Channel0 = #channel{
        conninfo = #{clean_start := CleanStart} = ConnInfo,
        clientinfo = #{clientid := ClientId} = ClientInfo,
        will_msg = MaybeWillMsg
    }
) ->
    Channel = Channel0#channel{
        quota = create_limiter(ClientInfo)
    },
    case emqx_cm:open_session(CleanStart, ClientInfo, ConnInfo, MaybeWillMsg) of
        {ok, #{session := Session, present := false}} ->
            ok = emqx_cm:register_channel(ClientId, self(), ConnInfo),
            NChannel = Channel#channel{session = Session},
            handle_out(connack, {?RC_SUCCESS, sp(false), AckProps}, ensure_connected(NChannel));
        {ok, #{session := Session, present := true, replay := ReplayContext}} ->
            ok = emqx_cm:register_channel(ClientId, self(), ConnInfo),
            NChannel = Channel#channel{
                session = Session,
                takeover = #resumption{replayctx = ReplayContext}
            },
            handle_out(connack, {?RC_SUCCESS, sp(true), AckProps}, ensure_connected(NChannel));
        {error, client_id_unavailable} ->
            ReasonString = <<"THROTTLED">>,
            handle_out(connack, {?RC_SERVER_BUSY, ReasonString}, Channel);
        {error, Reason} ->
            ?SLOG(error, #{msg => "failed_to_open_session", reason => Reason}),
            handle_out(connack, ?RC_UNSPECIFIED_ERROR, Channel)
    end.

create_limiter(#{zone := Zone, listener := ListenerId} = ClientInfo) ->
    Limiter = emqx_limiter:create_channel_client_container(Zone, ListenerId),
    emqx_hooks:run_fold('channel.limiter_adjustment', [ClientInfo], Limiter).

handle_zone_change(
    _Output,
    #channel{clientinfo = #{old_zone := _}} = Channel0
) ->
    Channel1 = recreate_limiter(Channel0),
    Channel = handle_session_zone_change(Channel1),
    {ok, Channel};
handle_zone_change(_Output, Channel) ->
    {ok, Channel}.

recreate_limiter(#channel{quota = undefined} = Channel) ->
    Channel;
recreate_limiter(#channel{clientinfo = ClientInfo} = Channel) ->
    Channel#channel{quota = create_limiter(ClientInfo)}.

handle_session_zone_change(#channel{session = undefined} = Channel) ->
    Channel;
handle_session_zone_change(#channel{session = Session, clientinfo = ClientInfo} = Channel) ->
    Channel#channel{
        session = emqx_session:handle_info(ClientInfo, zone_changed, Session)
    }.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

process_publish(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId), Channel) ->
    case
        emqx_utils:pipeline(
            [
                fun check_quota_exceeded/2,
                fun process_alias/2,
                fun check_pub_alias/2,
                fun packet_to_message/2,
                fun check_pub_authz/2,
                fun check_pub_caps/2,
                fun finalize_publish/2
            ],
            Packet,
            Channel
        )
    of
        {ok, Msg, NChannel} ->
            ok = ?EXT_TRACE_ADD_ATTRS(emqx_otel_trace:msg_attrs(Msg)),
            do_publish(PacketId, Msg, NChannel);
        {error, RC, NChannel} ->
            handle_publish_error(QoS, Topic, PacketId, RC, NChannel)
    end.

handle_publish_error(QoS, Topic, PacketId, RC = ?RC_NOT_AUTHORIZED, Channel) ->
    ?SLOG_THROTTLE(
        warning,
        #{
            msg => cannot_publish_to_topic_due_to_not_authorized,
            reason => emqx_reason_codes:name(RC)
        },
        #{topic => Topic, tag => "AUTHZ"}
    ),
    case emqx:get_config([authorization, deny_action], ignore) of
        ignore ->
            case QoS of
                ?QOS_0 -> {ok, Channel};
                ?QOS_1 -> handle_out(puback, {PacketId, RC}, Channel);
                ?QOS_2 -> handle_out(pubrec, {PacketId, RC}, Channel)
            end;
        disconnect ->
            handle_out(disconnect, RC, Channel)
    end;
handle_publish_error(QoS, _Topic, PacketId, RC = ?RC_QUOTA_EXCEEDED, Channel) ->
    ok = inc_metrics('packets.publish.quota_exceeded', Channel),
    case QoS of
        ?QOS_0 ->
            ok = inc_metrics('messages.dropped', Channel),
            ok = inc_metrics('messages.dropped.quota_exceeded', Channel),
            {ok, Channel};
        ?QOS_1 ->
            handle_out(puback, {PacketId, RC}, Channel);
        ?QOS_2 ->
            handle_out(pubrec, {PacketId, RC}, Channel)
    end;
handle_publish_error(_QoS, Topic, _PacketId, RC, Channel) ->
    ?SLOG(
        warning,
        #{
            msg => "cannot_publish_to_topic",
            topic => Topic,
            reason => emqx_reason_codes:name(RC)
        },
        #{topic => Topic}
    ),
    handle_out(disconnect, RC, Channel).

packet_to_message(
    Packet,
    Channel = #channel{
        conninfo = #{
            peername := PeerName,
            proto_ver := ProtoVer
        },
        clientinfo =
            #{
                protocol := Protocol,
                clientid := ClientId,
                username := Username,
                peerhost := PeerHost
            } = ClientInfo
    }
) ->
    ClientAttrs = maps:get(client_attrs, ClientInfo, #{}),
    Msg = emqx_packet:to_message(
        Packet,
        ClientId,
        #{
            client_attrs => ClientAttrs,
            peername => PeerName,
            proto_ver => ProtoVer,
            protocol => Protocol,
            username => Username,
            peerhost => PeerHost
        }
    ),
    {ok, Msg, Channel}.

finalize_publish(Msg, Channel = #channel{clientinfo = ClientInfo}) ->
    {ok, emqx_message_ingress:finalize(ClientInfo, Msg), Channel}.

do_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    Result = emqx_broker:publish(Msg),
    case Result of
        disconnect ->
            handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);
        _ ->
            {ok, Channel}
    end;
do_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    PubRes = emqx_broker:publish(Msg),
    RC = puback_reason_code(PacketId, Msg, PubRes),
    case RC of
        undefined ->
            {ok, Channel};
        disconnect ->
            handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);
        _Value ->
            do_finish_publish(PacketId, PubRes, RC, Channel)
    end;
do_publish(
    PacketId,
    Msg = #message{qos = ?QOS_2},
    Channel = #channel{clientinfo = ClientInfo, session = Session}
) ->
    case emqx_session:publish(ClientInfo, PacketId, Msg, Session) of
        {ok, disconnect, _NSession} ->
            handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);
        {ok, PubRes, NSession} ->
            RC = pubrec_reason_code(PubRes),
            NChannel0 = Channel#channel{session = NSession},
            NChannel1 = ensure_timer(expire_awaiting_rel, NChannel0),
            handle_out(pubrec, {PacketId, RC}, NChannel1);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = inc_metrics('packets.publish.inuse', Channel),
            handle_out(pubrec, {PacketId, RC}, Channel);
        {error, RC = ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ok = inc_metrics('messages.dropped.receive_maximum', Channel),
            handle_out(disconnect, RC, Channel)
    end.

do_finish_publish(PacketId, _PubRes, RC, Channel) ->
    handle_out(puback, {PacketId, RC}, Channel).

-compile({inline, [pubrec_reason_code/1]}).
pubrec_reason_code([]) -> ?RC_NO_MATCHING_SUBSCRIBERS;
pubrec_reason_code([_ | _]) -> ?RC_SUCCESS.

puback_reason_code(PacketId, Msg, [] = PubRes) ->
    emqx_hooks:run_fold('message.puback', [PacketId, Msg, PubRes], ?RC_NO_MATCHING_SUBSCRIBERS);
puback_reason_code(PacketId, Msg, [_ | _] = PubRes) ->
    emqx_hooks:run_fold('message.puback', [PacketId, Msg, PubRes], ?RC_SUCCESS);
puback_reason_code(_PacketId, _Msg, disconnect) ->
    disconnect.

%%--------------------------------------------------------------------
%% Process PUBACK
%%--------------------------------------------------------------------

process_puback(
    ?PUBACK_PACKET(PacketId, ReasonCode, Properties),
    Channel = #channel{
        clientinfo = ClientInfo,
        session = Session
    }
) ->
    case emqx_session:puback(ClientInfo, PacketId, ReasonCode, Session) of
        {ok, Msg, [], NSession} ->
            ok = after_message_acked(Msg, Properties, Channel),
            {ok, Channel#channel{session = NSession}};
        {OkEffects, Msg, Publishes, NSession} ->
            ok = after_message_acked(Msg, Properties, Channel),
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            handle_out(publish, Publishes, NChannel);
        {error, ?RC_PROTOCOL_ERROR} ->
            handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{msg => "puback_packetId_inuse", packetId => PacketId}),
            ok = inc_metrics('packets.puback.inuse', Channel),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?TRACE("MQTT", "puback_packetId_not_found", #{packetId => PacketId}),
            ok = inc_metrics('packets.puback.missed', Channel),
            {ok, Channel}
    end.

%%--------------------------------------------------------------------
%% Process PUBREC
%%--------------------------------------------------------------------

process_pubrec(
    %% TODO: Why discard the Reason Code?
    ?PUBREC_PACKET(PacketId, _ReasonCode, Properties),
    Channel =
        #channel{clientinfo = ClientInfo, session = Session}
) ->
    case emqx_session:pubrec(ClientInfo, PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(Msg, Properties, Channel),
            NChannel = Channel#channel{session = NSession},
            handle_out(pubrel, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PROTOCOL_ERROR} ->
            handle_out(disconnect, RC, Channel);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?SLOG(warning, #{msg => "pubrec_packetId_inuse", packetId => PacketId}),
            ok = inc_metrics('packets.pubrec.inuse', Channel),
            handle_out(pubrel, {PacketId, RC}, Channel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?TRACE("MQTT", "pubrec_packetId_not_found", #{packetId => PacketId}),
            ok = inc_metrics('packets.pubrec.missed', Channel),
            handle_out(pubrel, {PacketId, RC}, Channel)
    end.

%%--------------------------------------------------------------------
%% Process PUBREL
%%--------------------------------------------------------------------

process_pubrel(
    ?PUBREL_PACKET(PacketId, _ReasonCode),
    Channel = #channel{
        clientinfo = ClientInfo,
        session = Session
    }
) ->
    case emqx_session:pubrel(ClientInfo, PacketId, Session) of
        {ok, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(pubcomp, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?TRACE("MQTT", "pubrel_packetId_not_found", #{packetId => PacketId}),
            ok = inc_metrics('packets.pubrel.missed', Channel),
            handle_out(pubcomp, {PacketId, RC}, Channel)
    end.

%%--------------------------------------------------------------------
%% Process PUBCOMP
%%--------------------------------------------------------------------

process_pubcomp(
    ?PUBCOMP_PACKET(PacketId, ReasonCode),
    Channel = #channel{
        clientinfo = ClientInfo,
        session = Session
    }
) ->
    case emqx_session:pubcomp(ClientInfo, PacketId, ReasonCode, Session) of
        {ok, [], NSession} ->
            {ok, Channel#channel{session = NSession}};
        {OkEffects, Publishes, NSession} ->
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            handle_out(publish, Publishes, NChannel);
        {error, ?RC_PROTOCOL_ERROR} ->
            handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = inc_metrics('packets.pubcomp.inuse', Channel),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?TRACE("MQTT", "pubcomp_packetId_not_found", #{packetId => PacketId}),
            ok = inc_metrics('packets.pubcomp.missed', Channel),
            {ok, Channel}
    end.

-compile({inline, [after_message_acked/3]}).
after_message_acked(Msg, PubAckProps, Channel) ->
    ok = inc_metrics('messages.acked', Channel),
    run_hook_with_context(
        'message.acked',
        [
            Channel#channel.clientinfo,
            emqx_message:set_header(puback_props, PubAckProps, Msg)
        ],
        Channel
    ).

%%--------------------------------------------------------------------
%% Process Subscribe
%%--------------------------------------------------------------------

process_subscribe(SubPkt = ?SUBSCRIBE_PACKET(PacketId, _Properties, TopicFilters0), Channel0) ->
    Operation0 = subscribe_operation(SubPkt),
    case prepare_subscribe(Operation0, Channel0) of
        {ok, Operation = #subscribe_operation{topic_filters = TFChecked}, Channel} ->
            {TFSubedWithNRC, NChannel} = post_process_subscribe(
                run_sub_hooks(Operation, Channel), Channel
            ),
            ReasonCodes = gen_reason_codes(TFChecked, TFSubedWithNRC),
            handle_out(suback, {PacketId, ReasonCodes}, NChannel);
        {error, {disconnect, RC}, Channel} ->
            %% funcs in pipeline almost always cause action: `disconnect`
            %% And Only one ReasonCode in DISCONNECT packet
            handle_out(disconnect, RC, Channel);
        {error, RC, Channel} ->
            %% if rate-limited, we just reply a reason code without disconnecting
            ReasonCodes = lists:map(fun(_TF) -> RC end, TopicFilters0),
            handle_out(suback, {PacketId, ReasonCodes}, Channel)
    end.

prepare_subscribe(Operation, Channel) ->
    emqx_utils:pipeline(
        [
            fun check_subscribe_quota_exceeded/2,
            fun check_subscribe/2,
            fun enrich_subscribe/2,
            %% TODO && FIXME (EMQX-10786): mount topic before authz check.
            fun check_sub_authzs/2,
            fun check_sub_caps/2
        ],
        Operation,
        Channel
    ).

-spec subscribe_operation(emqx_types:packet() | [subscribe_filter()]) ->
    subscribe_operation().
subscribe_operation(?SUBSCRIBE_PACKET(_PacketId, Properties, TopicFilters)) ->
    #subscribe_operation{properties = Properties, topic_filters = TopicFilters};
subscribe_operation(TopicFilters) when is_list(TopicFilters) ->
    #subscribe_operation{topic_filters = TopicFilters}.

-compile(
    {inline, [
        post_process_subscribe/2,
        post_process_subscribe/3
    ]}
).
post_process_subscribe(TopicFilters, Channel) ->
    post_process_subscribe(TopicFilters, Channel, []).

post_process_subscribe([], Channel, Acc) ->
    {lists:reverse(Acc), Channel};
post_process_subscribe([Filter = {TopicFilter, SubOpts} | More], Channel, Acc) ->
    {NReasonCode, NChannel} = do_subscribe(TopicFilter, SubOpts, Channel),
    post_process_subscribe(More, NChannel, [{Filter, NReasonCode} | Acc]).

do_subscribe(
    TopicFilter,
    SubOpts = #{qos := QoS},
    Channel =
        #channel{
            clientinfo = ClientInfo = #{mountpoint := MountPoint},
            session = Session
        }
) ->
    %% TODO && FIXME (EMQX-10786): mount topic before authz check.
    NTopicFilter = emqx_mountpoint:mount(MountPoint, TopicFilter),
    case emqx_session:subscribe(ClientInfo, NTopicFilter, SubOpts, Session) of
        {ok, NSession} ->
            %% RC should be granted qos, rewrited by check_sub_caps/2
            NRC = QoS,
            {NRC, Channel#channel{session = NSession}};
        {error, NRC} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_subscribe_topic_filter",
                    reason => emqx_reason_codes:text(NRC)
                },
                #{topic => NTopicFilter}
            ),
            {NRC, Channel}
    end.

gen_reason_codes(TFChecked, TFSubedWitNhRC) ->
    do_gen_reason_codes([], TFChecked, TFSubedWitNhRC).

%% Initial RC is `RC_SUCCESS | RC_NOT_AUTHORIZED`, generated by check_sub_authzs/2
%% And then TF with `RC_SUCCESS` will be passing to `process_subscribe/2` and the qos will be NRC
%% NRC should override the initial RC here.
do_gen_reason_codes(Acc, [], []) ->
    lists:reverse(Acc);
do_gen_reason_codes(
    Acc,
    [{_, ?RC_SUCCESS} | RestTF],
    [{_, NRC} | RestWithNRC]
) ->
    %% will passing through `process_subscribe/2`
    %% use NRC to override IintialRC
    do_gen_reason_codes([NRC | Acc], RestTF, RestWithNRC);
do_gen_reason_codes(
    Acc,
    [{_, InitialRC} | Rest],
    RestWithNRC
) ->
    %% InitialRC is not `RC_SUCCESS`, use it.
    do_gen_reason_codes([InitialRC | Acc], Rest, RestWithNRC).

%%--------------------------------------------------------------------
%% Process Unsubscribe
%%--------------------------------------------------------------------

process_unsubscribe(
    Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
    Channel = #channel{clientinfo = ClientInfo = #{zone := Zone}}
) ->
    case
        emqx_packet:check(
            Packet,
            #{
                subscription_message_filter => get_mqtt_conf(
                    Zone, subscription_message_filter, disable
                )
            }
        )
    of
        ok ->
            TopicFilters1 = run_hooks(
                'client.unsubscribe',
                [ClientInfo, Properties],
                parse_raw_topic_filters(TopicFilters, Channel),
                Channel
            ),
            {ReasonCodes, NChannel} = post_process_unsubscribe(TopicFilters1, Properties, Channel),
            handle_out(unsuback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end.

-compile(
    {inline, [
        post_process_unsubscribe/3,
        post_process_unsubscribe/4
    ]}
).
post_process_unsubscribe(TopicFilters, UnSubProps, Channel) ->
    post_process_unsubscribe(TopicFilters, UnSubProps, Channel, []).

post_process_unsubscribe([], _UnSubProps, Channel, Acc) ->
    {lists:reverse(Acc), Channel};
post_process_unsubscribe([{TopicFilter, SubOpts} | More], UnSubProps, Channel, Acc) ->
    {RC, NChannel} = do_unsubscribe(TopicFilter, SubOpts#{unsub_props => UnSubProps}, Channel),
    post_process_unsubscribe(More, UnSubProps, NChannel, [RC | Acc]).

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
            {?RC_SUCCESS, Channel#channel{session = NSession}};
        {error, RC} ->
            {RC, Channel}
    end.
%%--------------------------------------------------------------------
%% Process Disconnect
%%--------------------------------------------------------------------

%% MQTT-v5.0: 3.14.4 DISCONNECT Actions
maybe_clean_will_msg(?RC_SUCCESS, Channel = #channel{session = Session0}) ->
    %% [MQTT-3.14.4-3]
    Session = emqx_session:clear_will_message(Session0),
    Channel#channel{will_msg = undefined, session = Session};
maybe_clean_will_msg(_ReasonCode, Channel) ->
    Channel.

process_disconnect(
    ?DISCONNECT_PACKET(ReasonCode, Properties),
    Channel = #channel{conninfo = ConnInfo}
) ->
    NConnInfo = ConnInfo#{disconn_props => Properties},
    NChannel = maybe_clean_will_msg(ReasonCode, Channel#channel{conninfo = NConnInfo}),
    post_process_disconnect(ReasonCode, Properties, NChannel).

%% MQTT-v5.0: 3.14.2.2.2 Session Expiry Interval
post_process_disconnect(
    _ReasonCode,
    #{'Session-Expiry-Interval' := Interval},
    Channel = #channel{conninfo = #{expiry_interval := 0}}
) when
    Interval > 0
->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);
post_process_disconnect(ReasonCode, Properties, Channel) ->
    NChannel = maybe_update_expiry_interval(Properties, Channel),
    {ok, {close, disconnect_reason(ReasonCode)}, NChannel}.

maybe_update_expiry_interval(
    #{'Session-Expiry-Interval' := Interval},
    Channel = #channel{conninfo = ConnInfo, clientinfo = #{zone := Zone}}
) ->
    MaxMs = get_mqtt_conf(Zone, max_session_expiry_interval),
    ClampedSec = clamp_session_expiry(Interval, MaxMs),
    case ClampedSec < Interval of
        true ->
            ?TRACE("MQTT", "session_expiry_interval_clamped", #{
                requested_seconds => Interval,
                clamped_seconds => ClampedSec
            });
        false ->
            ok
    end,
    EI = timer:seconds(ClampedSec),
    OldEI = maps:get(expiry_interval, ConnInfo, 0),
    case OldEI =:= EI of
        true ->
            Channel;
        false ->
            NChannel = Channel#channel{conninfo = ConnInfo#{expiry_interval => EI}},
            %% Check if the client turns off persistence (turning it on is disallowed)
            case EI =:= 0 andalso OldEI > 0 of
                true ->
                    ok = emqx_session:destroy(NChannel#channel.session),
                    NChannel#channel{session = undefined};
                false ->
                    NChannel
            end
    end;
maybe_update_expiry_interval(_Properties, Channel) ->
    Channel.

process_kick(
    Channel = #channel{
        conn_state = ConnState,
        conninfo = #{proto_ver := ProtoVer},
        session = Session
    }
) ->
    emqx_session:destroy(Session),
    Channel0 = maybe_publish_will_msg(kicked, Channel),
    Channel1 =
        case ConnState of
            connected -> ensure_disconnected(kicked, Channel0);
            _ -> Channel0
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
    end.

process_maybe_shutdown(
    Reason,
    Channel0 =
        #channel{
            clientinfo = ClientInfo,
            conninfo = ConnInfo,
            session = Session0
        }
) ->
    {Intent, Session} = session_disconnect(ClientInfo, ConnInfo, Session0),
    Channel1 = Channel0#channel{session = Session},
    Channel2 = ensure_disconnected(Reason, maybe_publish_will_msg(sock_closed, Channel1)),
    case maybe_shutdown(Reason, Intent, Channel2) of
        {ok, Channel} -> {ok, ?REPLY_EVENT(disconnected), Channel};
        Shutdown -> Shutdown
    end.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec handle_deliver(list(emqx_types:deliver()), channel()) ->
    {ok, channel()} | {ok, replies(), channel()}.

handle_deliver(
    Delivers,
    Channel = #channel{
        takeover = Takeover = #takeover{pendings = Pendings}
    }
) ->
    %% NOTE: Order is important here. While the takeover is in
    %% progress, the session cannot enqueue messages, since it already
    %% passed on the queue to the new connection in the session state.
    NPendings = lists:append(Pendings, maybe_nack(Delivers)),
    NTakeover = Takeover#takeover{pendings = NPendings},
    {ok, Channel#channel{takeover = NTakeover}};
handle_deliver(
    Delivers,
    Channel = #channel{
        conn_state = disconnected,
        takeover = undefined,
        session = Session,
        clientinfo = ClientInfo
    }
) ->
    %% NOTE
    %% This is essentially part of `emqx_session_mem` logic, thus call it directly.
    Delivers1 = maybe_nack(Delivers),
    Messages = emqx_session:enrich_delivers(ClientInfo, Delivers1, Session),
    NSession = emqx_session_mem:enqueue(ClientInfo, Messages, Session),
    %% we need to update stats here, as the stats_timer is canceled after disconnected
    {ok, {event, updated}, Channel#channel{session = NSession}};
handle_deliver(Delivers, Channel) ->
    Delivers1 =
        ?EXT_TRACE_BROKER_PUBLISH(
            basic_attrs(Channel),
            Delivers
        ),
    do_handle_deliver(Delivers1, Channel).

do_handle_deliver(
    Delivers,
    Channel = #channel{
        session = Session,
        clientinfo = ClientInfo,
        conn_flags = Flags
    }
) ->
    %% NOTE: Connflags ordset is a list.
    case emqx_session:deliver(ClientInfo, Delivers, Flags, Session) of
        {OkEffects, [], NSession} ->
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            {ok, NChannel};
        {OkEffects, Publishes, NSession} ->
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            handle_out(publish, Publishes, ensure_timer(retry_delivery, NChannel))
    end.

%% Nack delivers from shared subscription
maybe_nack(Delivers0) ->
    Delivers = lists:filter(fun not_nacked_by_shared_sub/1, Delivers0),
    lists:filter(fun not_nacked_by_hook/1, Delivers).

not_nacked_by_shared_sub({deliver, _Topic, Msg}) ->
    case emqx_shared_sub:is_ack_required(Msg) of
        true ->
            ok = emqx_shared_sub:nack_no_connection(Msg),
            false;
        false ->
            true
    end.

not_nacked_by_hook({deliver, _Topic, Msg}) ->
    %% NOTE if a callback nacks a message, it returns `true`
    %% nack'ed messages will be dropped from the session.
    not emqx_hooks:run_fold('message.nack', [Msg], false).

%%--------------------------------------------------------------------
%% Handle Frame Error
%%--------------------------------------------------------------------

%% Packet too large on an already-established connection.
handle_frame_error(
    Reason = #{cause := frame_too_large},
    Channel = #channel{conn_state = ConnState, conninfo = ConnInfo}
) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    ShutdownCount = shutdown_count(frame_error_kind(Reason, frame_error), Reason, Channel),
    case proto_ver(Reason, ConnInfo) of
        ?MQTT_PROTO_V5 ->
            handle_out(disconnect, {?RC_PACKET_TOO_LARGE, frame_too_large}, Channel);
        _ ->
            shutdown(ShutdownCount, Channel)
    end;
%% Frame error before CONNECT is parsed (conn_state still idle).
%% This happens when the first packet is not a valid MQTT CONNECT,
%% e.g. an HTTP request or other non-MQTT protocol sent to the MQTT port.
%% No CONNACK is sent because no MQTT version has been negotiated yet.
handle_frame_error(
    Reason,
    Channel = #channel{conn_state = idle}
) ->
    shutdown(
        shutdown_count(frame_error_kind(Reason, invalid_connect_packet), Reason, Channel),
        Channel
    );
%% Frame error while parsing CONNECT.
%% For MQTT-v5.0, reply with CONNACK carrying the appropriate reason code.
%% For older protocol versions, silently shut down.
handle_frame_error(
    Reason,
    Channel = #channel{conn_state = connecting, conninfo = ConnInfo}
) ->
    ShutdownCount = shutdown_count(frame_error_kind(Reason, frame_error), Reason, Channel),
    ProtoVer = proto_ver(Reason, ConnInfo),
    NChannel = Channel#channel{conninfo = ConnInfo#{proto_ver => ProtoVer}},
    case ProtoVer of
        ?MQTT_PROTO_V5 ->
            RC = frame_error_reason_code(Reason),
            shutdown(ShutdownCount, ?CONNACK_PACKET(RC), NChannel);
        _ ->
            shutdown(ShutdownCount, NChannel)
    end;
%% Frame error on an established connection: send DISCONNECT and close.
%% The close reason doubles as the connection process exit reason, so it must be
%% a plain atom: that is what lets the connection supervisor keep a shutdown
%% counter instead of reporting an unidentified shutdown at error level. The
%% parse error itself is reported by the connection through `emqx_trace'.
handle_frame_error(
    Reason,
    Channel = #channel{conn_state = ConnState}
) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    handle_out(
        disconnect,
        {frame_error_reason_code(Reason), frame_error_kind(Reason, frame_error)},
        Channel
    );
handle_frame_error(
    Reason,
    Channel = #channel{conn_state = disconnected}
) ->
    %% Invalid client input, not a broker failure.
    ?SLOG(info, #{msg => "malformed_mqtt_message", reason => Reason}),
    {ok, Channel}.

-doc """
Name the shutdown counter for a frame error.

A counter name must come from a bounded set. Errors reported as an atom are
already such a set, fixed by the parser, so the atom names its own counter.
Errors reported as a map carry detail derived from the offending packet, so they
share `Default'; the specific cause remains in the shutdown reason and in the
trace. Oversized frames keep their own counter in every connection state, being
a distinct operational signal rather than a malformed packet.
""".
frame_error_kind(Reason, _Default) when is_atom(Reason) -> Reason;
frame_error_kind(#{cause := frame_too_large}, _Default) -> frame_too_large;
frame_error_kind(_Reason, Default) -> Default.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec handle_out(atom(), term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}
    | {shutdown, Reason :: term(), replies(), channel()}.
handle_out(connack, {?RC_SUCCESS, SP, Props}, Channel = #channel{conninfo = ConnInfo}) ->
    AckProps = emqx_utils:run_fold(
        [
            fun enrich_connack_caps/2,
            fun enrich_server_keepalive/2,
            fun enrich_response_information/2,
            fun enrich_assigned_clientid/2,
            fun enrich_clamped_session_expiry_interval/2
        ],
        Props,
        Channel
    ),
    NAckProps = run_client_connack_hooks(
        Channel,
        ConnInfo,
        emqx_reason_codes:name(?RC_SUCCESS),
        AckProps
    ),
    return_connack(
        ?CONNACK_PACKET(?RC_SUCCESS, SP, NAckProps),
        ensure_keepalive(NAckProps, Channel)
    );
handle_out(connack, ReasonCode, Channel) when is_integer(ReasonCode) ->
    handle_out(connack, {ReasonCode, <<>>}, Channel);
handle_out(connack, {ReasonCode, ReasonString}, Channel = #channel{conninfo = ConnInfo}) ->
    Reason = emqx_reason_codes:name(ReasonCode),
    AckProps0 = emqx_mqtt_props:new(),
    AckProps1 =
        case ReasonString =:= <<>> of
            true ->
                AckProps0;
            false ->
                emqx_mqtt_props:set('Reason-String', ReasonString, AckProps0)
        end,
    AckProps = run_client_connack_hooks(Channel, ConnInfo, Reason, AckProps1),
    AckPacket = ?CONNACK_PACKET(
        case maps:get(proto_ver, ConnInfo) of
            ?MQTT_PROTO_V5 -> ReasonCode;
            _ -> emqx_reason_codes:compat(connack, ReasonCode)
        end,
        sp(false),
        AckProps
    ),
    %% Per MQTT v5: If a Server sends a CONNACK with a Reason Code other than 0x00 (Success),
    %% the Will Message MUST NOT be published.
    %% Per MQTT v3: If the Will Flag is set to 1 this indicates that,
    %% if the Connect request is accepted, a Will Message MUST be stored…
    shutdown(Reason, AckPacket, Channel#channel{will_msg = undefined});
%% Optimize?
handle_out(publish, [], Channel) ->
    {ok, Channel};
handle_out(publish, Publishes, Channel) ->
    {Packets, NChannel} = do_deliver(Publishes, Channel),
    {ok, ?REPLY_OUTGOING(Packets), NChannel};
handle_out(puback, {PacketId, ReasonCode}, Channel) ->
    Packet = ?EXT_TRACE_OUTGOING_START(
        basic_attrs(Channel),
        ?PUBACK_PACKET(PacketId, ReasonCode)
    ),
    {ok, ?REPLY_OUTGOING(Packet), Channel};
handle_out(pubrec, {PacketId, ReasonCode}, Channel) ->
    Packet = ?EXT_TRACE_OUTGOING_START(
        basic_attrs(Channel),
        ?PUBREC_PACKET(PacketId, ReasonCode)
    ),
    {ok, ?REPLY_OUTGOING(Packet), Channel};
handle_out(pubrel, {PacketId, ReasonCode}, Channel) ->
    Packet = ?EXT_TRACE_OUTGOING_START(
        basic_attrs(Channel),
        ?PUBREL_PACKET(PacketId, ReasonCode)
    ),
    {ok, ?REPLY_OUTGOING(Packet), Channel};
handle_out(pubcomp, {PacketId, ReasonCode}, Channel) ->
    Packet = ?EXT_TRACE_OUTGOING_START(
        basic_attrs(Channel),
        ?PUBCOMP_PACKET(PacketId, ReasonCode)
    ),
    {ok, ?REPLY_OUTGOING(Packet), Channel};
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
handle_out(disconnect, {ReasonCode, ReasonName}, Channel) ->
    handle_out(disconnect, {ReasonCode, ReasonName, #{}}, Channel);
handle_out(disconnect, {ReasonCode, ReasonName, Props}, Channel = ?IS_MQTT_V5) ->
    Packet = ?DISCONNECT_PACKET(ReasonCode, Props),
    {ok, [?REPLY_OUTGOING(Packet), ?REPLY_CLOSE(ReasonName)], Channel};
handle_out(disconnect, {_ReasonCode, ReasonName, _Props}, Channel) ->
    {ok, ?REPLY_CLOSE(ReasonName), Channel};
handle_out(auth, {ReasonCode, Properties}, Channel0) ->
    Replies0 = {outgoing, ?AUTH_PACKET(ReasonCode, Properties)},
    {Replies1, Channel} = maybe_add_zone_changed_event(Replies0, Channel0),
    Replies = add_set_namespace_event(Replies1, Channel),
    {ok, Replies, Channel};
handle_out(Type, Data, Channel) ->
    ?SLOG(error, #{msg => "unexpected_outgoing", type => Type, data => Data}),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Return ConnAck
%%--------------------------------------------------------------------

return_connack(?CONNACK_PACKET(_RC, _SessPresent) = AckPacket, Channel0) ->
    ?EXT_TRACE_ADD_ATTRS(#{'client.connack.reason_code' => _RC}),
    Replies0 = [?REPLY_EVENT(connected), ?REPLY_CONNACK(AckPacket)],
    {Replies1, Channel} = maybe_add_zone_changed_event(Replies0, Channel0),
    Replies = add_set_namespace_event(Replies1, Channel),
    {continue, Replies, Channel}.

maybe_add_zone_changed_event(Replies, #channel{clientinfo = ClientInfo0} = Channel0) ->
    case ClientInfo0 of
        #{old_zone := _, zone := NewZone} ->
            ClientInfo = maps:remove(old_zone, ClientInfo0),
            Channel = Channel0#channel{clientinfo = ClientInfo},
            {[?REPLY_EVENT({zone_changed, NewZone}) | wrap_list(Replies)], Channel};
        _ ->
            {Replies, Channel0}
    end.

add_set_namespace_event(Replies, #channel{clientinfo = ClientInfo}) ->
    case get_tenant_namespace(ClientInfo) of
        undefined ->
            [?REPLY_EVENT({set_namespace, ?global_ns}) | wrap_list(Replies)];
        Namespace ->
            [?REPLY_EVENT({set_namespace, Namespace}) | wrap_list(Replies)]
    end.

wrap_list(Xs) when is_list(Xs) ->
    Xs;
wrap_list(X) ->
    [X].

%%--------------------------------------------------------------------
%% Deliver publish: broker -> client
%%--------------------------------------------------------------------

-spec do_deliver(emqx_session:replies(), channel()) ->
    {emqx_types:packet() | [emqx_types:packet()], channel()}.
do_deliver({pubrel, PacketId}, Channel) ->
    {?PUBREL_PACKET(PacketId, ?RC_SUCCESS), Channel};
do_deliver(
    {PacketId, Msg},
    Channel = #channel{
        clientinfo = ClientInfo = #{mountpoint := MountPoint}
    }
) ->
    ok = inc_metrics('messages.delivered', Channel),
    Msg1 = emqx_hooks:run_fold(
        'message.delivered',
        [ClientInfo],
        emqx_message:update_expiry(Msg)
    ),
    Msg2 = emqx_mountpoint:unmount(MountPoint, Msg1),
    Packet = emqx_message:to_packet(PacketId, Msg2),
    {NPacket, NChannel} = packing_alias(Packet, Channel),
    {NPacket, NChannel};
do_deliver([Publish], Channel) ->
    do_deliver(Publish, Channel);
do_deliver([], Channel) ->
    {[], Channel};
do_deliver(Publishes, Channel) ->
    do_deliver_many(Publishes, [], Channel).

-spec do_deliver_many([emqx_session:reply()], channel()) ->
    {[emqx_types:packet()], channel()}.
do_deliver_many(Publishes, Channel) ->
    do_deliver_many(Publishes, [], Channel).

do_deliver_many([Publish | Rest], Acc, Channel) ->
    {Packet, NChannel} = do_deliver(Publish, Channel),
    do_deliver_many(Rest, [Packet | Acc], NChannel);
do_deliver_many([], Packets, Channel) ->
    {lists:reverse(Packets), Channel}.

%%--------------------------------------------------------------------
%% Handle out suback
%%--------------------------------------------------------------------

return_sub_unsub_ack(Packet, Channel) ->
    {ok, [?REPLY_OUTGOING(Packet), ?REPLY_EVENT(updated)], Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec handle_call(Req :: term(), channel()) ->
    {reply, Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), channel()}
    | {shutdown, Reason :: term(), Reply :: term(), emqx_types:packet(), channel()}.
handle_call(kick, Channel = #channel{conn_state = ConnState}) when
    ConnState =/= disconnected
->
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), disconnect_attrs(kick, Channel))
        ),
        fun() -> process_kick(Channel) end,
        []
    );
handle_call(kick, Channel) ->
    process_kick(Channel);
handle_call(discard, Channel = #channel{conninfo = ConnInfo}) ->
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), disconnect_attrs(discard, Channel))
        ),
        fun() ->
            %% Record disconnected_at here to ensure it's earlier than the new session's connected_at.
            %% This prevents race conditions where the new session's connected_at is set before
            %% the old session's disconnected_at.
            NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
            Channel0 = maybe_publish_will_msg(discarded, Channel#channel{conninfo = NConnInfo}),
            disconnect_and_shutdown(discarded, ok, Channel0)
        end,
        []
    );
%% Session Takeover
handle_call(
    {takeover, 'begin'},
    Channel = #channel{conninfo = #{expiry_interval := 0}}
) ->
    %% MQTT 5.0 3.1.2.11.2: with Session Expiry Interval 0 the session ends
    %% when the network connection is closed. The takeover closes it, so
    %% there is no session to hand over: publish the will message because
    %% the session ends now, then shut down like a takeover kick.
    %% `noreply' makes the connection process shut down without answering
    %% the call; the caller observes the `{shutdown, _}' exit, classifies
    %% it as `noproc', and falls back to creating a fresh session. Peer
    %% nodes running older code handle this the same way.
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), disconnect_attrs(takeover, Channel))
        ),
        fun() ->
            Channel0 = maybe_publish_will_msg(expired, Channel),
            disconnect_and_shutdown(takenover, noreply, Channel0)
        end,
        []
    );
handle_call(
    {takeover, 'begin'},
    Channel = #channel{
        session = Session0,
        clientinfo = #{clientid := ClientId},
        conninfo = ConnInfo
    }
) ->
    %% Called during RPC via `emqx_cm_proto_v{1..4}`, only by `emqx_session_mem`.
    %% NOTE
    %% Ensure channel has enough time left to react to takeover end call. At the same
    %% time ensure that channel dies off reasonably quickly if no call will arrive.
    %% Record disconnected_at here to ensure it's earlier than the new session's connected_at.
    %% This prevents race conditions where the new session's connected_at is set before
    %% the old session's disconnected_at.
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(millisecond)},
    Interval = interval(expire_takeover, Channel),
    NChannel = reset_timer(expire_session, Interval, Channel#channel{conninfo = NConnInfo}),
    ok = emqx_cm:unregister_channel(ClientId),
    Session = emqx_session_mem:save_subopts(Session0),
    Takeover = #takeover{pendings = []},
    SessionData = emqx_session_mem:export(Session),
    reply(SessionData, NChannel#channel{takeover = Takeover});
handle_call(
    {takeover, 'end'},
    Channel = #channel{
        session = Session,
        takeover = #takeover{pendings = Pendings},
        clientinfo = #{clientid := ClientId}
    }
) ->
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), disconnect_attrs(takeover, Channel))
        ),
        fun() ->
            %% NOTE
            %% This is essentially part of `emqx_session_mem` logic, thus call it directly.
            ok = emqx_session_mem:takeover(Session),
            %% TODO: Should not drain deliver here (side effect)
            Delivers = emqx_utils:drain_deliver(),
            AllPendings = lists:append(Pendings, maybe_nack(Delivers)),
            ?tp(
                debug,
                emqx_channel_takeover_end,
                #{clientid => ClientId}
            ),
            Channel0 = maybe_publish_will_msg(takenover, Channel),
            disconnect_and_shutdown(takenover, AllPendings, Channel0)
        end,
        []
    );
handle_call(takeover_kick, Channel) ->
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(
                basic_attrs(Channel), disconnect_attrs(takeover_kick, Channel)
            )
        ),
        fun() ->
            %% The channel-side will publish only has an effect for an
            %% in-memory session; with durable sessions enabled a live
            %% in-memory session implies session expiry interval 0
            %% (MQTT 5.0 3.1.2.11.2), so it ends now and the will message
            %% must be published regardless of the will delay interval.
            %% For a durable session this publish is a no-op and
            %% `emqx_durable_will' decides at channel terminate.
            Channel0 = maybe_publish_will_msg(expired, Channel),
            disconnect_and_shutdown(takenover, ok, Channel0)
        end,
        []
    );
handle_call(list_authz_cache, Channel) ->
    {reply, emqx_authz_cache:list_authz_cache(), Channel};
handle_call({Type, _Meta} = MsgsReq, Channel = #channel{session = Session}) when
    Type =:= mqueue_msgs; Type =:= inflight_msgs
->
    {reply, emqx_session:info(MsgsReq, Session), Channel};
handle_call(Req, Channel) ->
    ?SLOG(error, #{msg => "unexpected_call", call => Req}),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle cast

-spec handle_cast(Req :: term(), channel()) -> channel().
handle_cast(
    {keepalive, Interval},
    Channel = #channel{
        keepalive = KeepAlive,
        conninfo = ConnInfo,
        clientinfo = #{zone := Zone}
    }
) ->
    ClientId = info(clientid, Channel),
    OldInterval = maps:get(keepalive, ConnInfo, undefined),
    ?TRACE("MQTT", "keepalive_updated", #{
        old_keepalive => OldInterval,
        new_keepalive => Interval
    }),
    NKeepAlive = emqx_keepalive:update(Zone, Interval, KeepAlive),
    NConnInfo = maps:put(keepalive, Interval, ConnInfo),
    NChannel = Channel#channel{keepalive = NKeepAlive, conninfo = NConnInfo},
    SockInfo = maps:get(sockinfo, emqx_cm:get_chan_info(ClientId), #{}),
    ChanInfo1 = info(NChannel),
    emqx_cm:set_chan_info(ClientId, ChanInfo1#{sockinfo => SockInfo}),
    reset_timer(keepalive, NChannel);
handle_cast(Req, Channel) ->
    ?SLOG(error, #{msg => "unexpected_cast", cast => Req}),
    Channel.

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec handle_info(Info :: term(), channel()) ->
    ok
    | {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.

handle_info(continue, Channel0) ->
    case maybe_resume_session(Channel0) of
        ignore ->
            ok;
        {Publishes, Channel1} ->
            {Packets, Channel} = do_deliver_many(Publishes, Channel1),
            Outgoing = [?REPLY_OUTGOING(Packets) || length(Packets) > 0],
            %% Refresh chan-info / chan-stats so the dashboard and REST API
            %% reflect post-replay inflight immediately, not on the next stats tick.
            {ok, Outgoing ++ [?REPLY_EVENT(updated)], Channel}
    end;
handle_info({subscribe, TopicFilters}, Channel) ->
    ?EXT_TRACE_BROKER_SUBSCRIBE(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), topic_attrs({subscribe, TopicFilters}))
        ),
        fun() ->
            case emqx_security_profile:policy(internal_subscription_checks) of
                true ->
                    Operation = subscribe_operation(TopicFilters),
                    case prepare_subscribe(Operation, Channel) of
                        {ok, NOperation, Channel1} ->
                            {_TopicFiltersWithRC, Channel2} = post_process_subscribe(
                                run_sub_hooks(NOperation, Channel1), Channel1
                            ),
                            {ok, Channel2};
                        {error, {disconnect, RC}, Channel1} ->
                            handle_out(disconnect, RC, Channel1)
                    end;
                false ->
                    NTopicFilters = do_enrich_subscribe(#{}, TopicFilters, Channel),
                    {_TopicFiltersWithRC, NChannel} = post_process_subscribe(
                        NTopicFilters, Channel
                    ),
                    {ok, NChannel}
            end
        end,
        []
    );
handle_info({force_subscribe, TopicFilters}, Channel) ->
    ?EXT_TRACE_BROKER_SUBSCRIBE(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), topic_attrs({subscribe, TopicFilters}))
        ),
        fun() ->
            NTopicFilters = do_enrich_subscribe(#{}, TopicFilters, Channel),
            {_TopicFiltersWithRC, NChannel} = post_process_subscribe(NTopicFilters, Channel),
            {ok, NChannel}
        end,
        []
    );
handle_info({unsubscribe, TopicFilters}, Channel) ->
    ?EXT_TRACE_BROKER_UNSUBSCRIBE(
        ?EXT_TRACE_ATTR(
            maps:merge(basic_attrs(Channel), topic_attrs({unsubscribe, TopicFilters}))
        ),
        fun() ->
            {_RC, NChannel} = post_process_unsubscribe(TopicFilters, #{}, Channel),
            {ok, NChannel}
        end,
        []
    );
handle_info(
    {sock_closed, ?normal},
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = #{clientid := ClientId}
    }
) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    %% `normal`, aka `?RC_SUCCES`, disconnect(by client's DISCONNECT packet) and close socket
    %% already traced `client.disconnect`, no need to trace `broker.disconnect`
    ?tp(sock_closed_normal, #{clientid => ClientId, conn_state => ConnState}),
    process_maybe_shutdown(normal, Channel);
handle_info(
    {sock_closed, Reason},
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = #{clientid := ClientId}
    }
) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    %% Socket closed when `connected` or `reauthenticating`
    ?tp(sock_closed_with_other_reason, #{clientid => ClientId, conn_state => ConnState}),
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(
                (basic_attrs(Channel))#{},
                disconnect_attrs(sock_closed, Channel)
            )
        ),
        fun() -> process_maybe_shutdown(Reason, Channel) end,
        []
    );
handle_info(
    {sock_closed, Reason},
    Channel = #channel{
        conn_state = ConnState,
        clientinfo = #{clientid := ClientId}
    }
) when
    ConnState =:= idle orelse
        ConnState =:= connecting
->
    ?tp(sock_closed_when_idle_or_connecting, #{clientid => ClientId, conn_state => ConnState}),
    ?EXT_TRACE_BROKER_DISCONNECT(
        ?EXT_TRACE_ATTR(
            maps:merge(
                (basic_attrs(Channel))#{},
                disconnect_attrs(sock_closed, Channel)
            )
        ),
        fun() -> shutdown(Reason, Channel) end,
        []
    );
handle_info({sock_closed, _Reason}, Channel = #channel{conn_state = disconnected}) ->
    %% This can happen as a race:
    %% EMQX closes socket and marks 'disconnected' but 'tcp_closed' or 'ssl_closed'
    %% is already in process mailbox
    {ok, Channel};
handle_info(clean_authz_cache, Channel) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    {ok, Channel};
handle_info(die_if_test = Info, Channel) ->
    die_if_test_compiled(),
    ?SLOG(error, #{msg => "unexpected_info", info => Info}),
    {ok, Channel};
handle_info({disconnect, ReasonCode, ReasonName, Props}, Channel) ->
    handle_out(disconnect, {ReasonCode, ReasonName, Props}, Channel);
handle_info({puback, PacketId, PubRes, RC}, Channel) ->
    do_finish_publish(PacketId, PubRes, RC, Channel);
handle_info(heal_cluster_partition, Channel) ->
    handle_heal_partition(Channel),
    {ok, Channel};
handle_info({session, Info}, Channel = #channel{session = Session0, clientinfo = ClientInfo}) ->
    Session = emqx_session:handle_info(ClientInfo, Info, Session0),
    {ok, Channel#channel{session = Session}};
handle_info(Info, Channel0 = #channel{session = Session0, clientinfo = ClientInfo}) ->
    Session = emqx_session:handle_info(ClientInfo, Info, Session0),
    Channel1 = Channel0#channel{session = Session},
    Acc0 = #{deliver => [], replies => []},
    Acc = run_fold_with_context('client.handle_info', [ClientInfo, Info], Acc0, Channel1),
    HookDelivers = maps:get(deliver, Acc, []),
    HookReplies = maps:get(replies, Acc, []),
    {AllReplies, Channel} =
        case handle_deliver(HookDelivers, Channel1) of
            {ok, Channel2} ->
                {HookReplies, Channel2};
            {ok, DeliverReplies, Channel2} ->
                {append_replies(DeliverReplies, HookReplies), Channel2}
        end,
    case AllReplies of
        [] ->
            {ok, Channel};
        _ ->
            {ok, AllReplies, Channel}
    end.

append_replies(Replies1, Replies2) ->
    replies_as_list(Replies1) ++ replies_as_list(Replies2).

replies_as_list(Replies) when is_list(Replies) -> Replies;
replies_as_list(Replies) -> [Replies].

-ifdef(TEST).

-spec die_if_test_compiled() -> no_return().
die_if_test_compiled() ->
    exit(normal).

-else.

die_if_test_compiled() ->
    ok.

-endif.

%%--------------------------------------------------------------------
%% Handle signal
%%--------------------------------------------------------------------

-spec handle_signal(term(), channel()) ->
    {ok, channel()}
    | {ok, reply() | [reply()], channel()}.
handle_signal(
    {connection, congested, _Info} = Signal,
    Channel = #channel{conn_flags = Flags, clientinfo = ClientInfo, session = Session}
) ->
    case ordsets:is_element(congested, Flags) of
        true ->
            {ok, Channel};
        false ->
            NFlags = ordsets:add_element(congested, Flags),
            handle_session_signal(ClientInfo, Signal, Channel#channel{conn_flags = NFlags}, Session)
    end;
handle_signal(
    {connection, decongested, _Info} = Signal,
    Channel = #channel{conn_flags = Flags, clientinfo = ClientInfo, session = Session}
) ->
    case ordsets:is_element(congested, Flags) of
        false ->
            {ok, Channel};
        true ->
            NFlags = ordsets:del_element(congested, Flags),
            handle_session_signal(ClientInfo, Signal, Channel#channel{conn_flags = NFlags}, Session)
    end.

handle_session_signal(_ClientInfo, _Signal, Channel, undefined) ->
    {ok, Channel};
handle_session_signal(ClientInfo, Signal, Channel, Session) ->
    case emqx_session:handle_signal(ClientInfo, Signal, Session) of
        {OkEffects, NSession} ->
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            {ok, NChannel};
        {OkEffects, Publishes, NSession} ->
            NChannel = apply_session_effects(OkEffects, Channel#channel{session = NSession}),
            handle_signal_publishes(Publishes, NChannel)
    end.

handle_signal_publishes(Publishes, Channel) ->
    case do_deliver(Publishes, Channel) of
        {[], NChannel} ->
            {ok, NChannel};
        {Packets, NChannel} ->
            {ok, ?REPLY_OUTGOING(Packets), NChannel}
    end.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec handle_timeout(reference(), Msg :: term(), channel()) ->
    {ok, channel()}
    | {ok, replies(), channel()}
    | {shutdown, Reason :: term(), channel()}.
handle_timeout(
    _TRef,
    keepalive,
    Channel = #channel{keepalive = undefined}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    keepalive,
    Channel = #channel{conn_state = disconnected}
) ->
    {ok, Channel};
handle_timeout(
    _TRef,
    keepalive,
    Channel = #channel{keepalive = KeepAlive}
) ->
    case emqx_keepalive:check(KeepAlive) of
        {ok, NKeepAlive} ->
            NChannel = Channel#channel{keepalive = NKeepAlive},
            {ok, reset_timer(keepalive, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, ?RC_KEEP_ALIVE_TIMEOUT, Channel)
    end;
handle_timeout(
    _TRef,
    TimerName,
    Channel = #channel{conn_state = disconnected}
) when ?IS_COMMON_SESSION_ONLINE_TIMER(TimerName) ->
    %% Skip session timers that require a connected client
    {ok, Channel};
handle_timeout(
    _TRef,
    TimerName,
    Channel0 = #channel{session = Session, clientinfo = ClientInfo}
) when ?IS_COMMON_SESSION_TIMER(TimerName) ->
    %% NOTE
    %% Responsibility for these timers is smeared across both this module and the
    %% `emqx_session` module: the latter holds configured timer intervals, and is
    %% responsible for the actual timeout logic. Yet they are managed here, since
    %% they are kind of common to all session implementations.
    Channel1 = clean_timer(TimerName, Channel0),
    case emqx_session:handle_timeout(ClientInfo, TimerName, Session) of
        {OkEffects, Publishes, NSession} ->
            Channel = apply_session_effects(OkEffects, Channel1#channel{session = NSession}),
            handle_out(publish, Publishes, Channel);
        {OkEffects, Publishes, Timeout, NSession} when is_integer(Timeout) ->
            Channel = apply_session_effects(OkEffects, Channel1#channel{session = NSession}),
            handle_out(publish, Publishes, reset_timer(TimerName, Timeout, Channel))
    end;
handle_timeout(
    _TRef,
    {emqx_session, TimerName} = Timer,
    Channel0 = #channel{session = Session, clientinfo = ClientInfo}
) ->
    Channel1 = clean_timer(Timer, Channel0),
    case emqx_session:handle_timeout(ClientInfo, TimerName, Session) of
        {ok, [], NSession} ->
            {ok, Channel1#channel{session = NSession}};
        {OkEffects, Replies, NSession} ->
            Channel = apply_session_effects(OkEffects, Channel1#channel{session = NSession}),
            handle_out(publish, Replies, Channel)
    end;
handle_timeout(_TRef, expire_session, Channel = #channel{session = Session}) ->
    Channel0 = maybe_publish_will_msg(expired, Channel),
    ok = emqx_session:destroy(Session),
    shutdown(expired, Channel0);
handle_timeout(
    _TRef,
    will_message = TimerName,
    Channel = #channel{will_msg = WillMsg}
) ->
    (WillMsg =/= undefined) andalso publish_will_msg(Channel),
    {ok, clean_timer(TimerName, Channel#channel{will_msg = undefined})};
handle_timeout(
    _TRef,
    connection_auth_expire,
    #channel{conn_state = ConnState} = Channel0
) ->
    Channel1 = clean_timer(connection_auth_expire, Channel0),
    case ConnState of
        disconnected ->
            {ok, Channel1};
        _ ->
            Channel2 = maybe_publish_will_msg(auth_expired, Channel1),
            handle_out(disconnect, ?RC_NOT_AUTHORIZED, Channel2)
    end;
handle_timeout(TRef, Msg, Channel) ->
    case run_fold_with_context('client.timeout', [TRef, Msg], [], Channel) of
        [] ->
            {ok, Channel};
        Msgs ->
            {ok, Msgs, Channel}
    end.

%%--------------------------------------------------------------------
%% Session effects
%%--------------------------------------------------------------------

-compile({inline, [apply_session_effects/2, apply_session_effect/2]}).

apply_session_effects(ok, Channel) ->
    Channel;
apply_session_effects(Effect, Channel) when not is_list(Effect) ->
    apply_session_effect(Effect, Channel);
apply_session_effects([E | Rest], Channel) ->
    apply_session_effects(Rest, apply_session_effect(E, Channel));
apply_session_effects([], Channel) ->
    Channel.

apply_session_effect({set_timer, Name, Time}, Channel) ->
    set_session_timer(Name, Time, Channel);
apply_session_effect({reset_timer, Name, Time}, Channel) ->
    reset_session_timer(Name, Time, Channel).

ensure_timer(Name, Channel = #channel{timers = Timers}) ->
    TRef = maps:get(Name, Timers, undefined),
    Time = interval(Name, Channel),
    case TRef == undefined andalso Time > 0 of
        true -> ensure_timer(Name, Time, Channel);
        %% Timer disabled or exists
        false -> Channel
    end.

ensure_timer(Name, Time, Channel = #channel{timers = Timers}) ->
    TRef = emqx_utils:start_timer(Time, Name),
    Channel#channel{timers = Timers#{Name => TRef}}.

set_session_timer(Name, Time, Channel) when is_integer(Time) ->
    Timer = {emqx_session, Name},
    case Channel#channel.timers of
        #{Timer := _TRef} ->
            Channel;
        #{} ->
            ensure_timer(Timer, Time, Channel)
    end;
set_session_timer(_Name, _Infinity, Channel) ->
    Channel.

reset_session_timer(Name, Time, Channel) when is_integer(Time) ->
    reset_timer({emqx_session, Name}, Time, Channel);
reset_session_timer(Name, _Infinity, Channel) ->
    clean_timer({emqx_session, Name}, Channel).

reset_timer(Name, Channel) ->
    ensure_timer(Name, clean_timer(Name, Channel)).

reset_timer(Name, Time, Channel) ->
    ensure_timer(Name, Time, clean_timer(Name, Channel)).

clean_timer(Name, Channel = #channel{timers = Timers}) ->
    case maps:take(Name, Timers) of
        error ->
            Channel;
        {TRef, NTimers} ->
            ok = emqx_utils:cancel_timer(TRef),
            Channel#channel{timers = NTimers}
    end.

interval(keepalive, #channel{keepalive = KeepAlive}) ->
    emqx_keepalive:info(check_interval, KeepAlive);
interval(retry_delivery, #channel{session = Session}) ->
    emqx_session:info(retry_interval, Session);
interval(expire_awaiting_rel, #channel{session = Session}) ->
    emqx_session:info(await_rel_timeout, Session);
interval(expire_session, #channel{conninfo = ConnInfo}) ->
    maps:get(expiry_interval, ConnInfo);
interval(expire_takeover, #channel{}) ->
    %% NOTE: Equivalent to 2 × `?T_TAKEOVER` for simplicity.
    2 * 5_000;
interval(will_message, #channel{will_msg = WillMsg}) ->
    timer:seconds(will_delay_interval(WillMsg)).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

-spec terminate(any(), channel()) -> ok.
terminate(_, #channel{conn_state = idle} = _Channel) ->
    ok;
terminate(normal, Channel) ->
    run_terminate_hook(normal, Channel);
terminate({shutdown, Reason}, Channel) when
    Reason =:= expired orelse
        Reason =:= takenover orelse
        Reason =:= kicked orelse
        Reason =:= discarded
->
    run_terminate_hook(Reason, Channel);
terminate(Reason, Channel) ->
    Channel1 = maybe_publish_will_msg(?chan_terminating, Channel),
    run_terminate_hook(Reason, Channel1).

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
        keepalive = KeepAlive,
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
        keepalive => KeepAlive,
        clientid => ClientId,
        username => Username,
        conn_props => ConnProps,
        expiry_interval => ExpiryInterval,
        receive_maximum => receive_maximum(Zone, ConnProps)
    },
    {ok, Channel#channel{conninfo = NConnInfo}}.

%% If the Session Expiry Interval is absent the value 0 is used.
expiry_interval(Zone, #mqtt_packet_connect{
    proto_ver = ?MQTT_PROTO_V5,
    properties = ConnProps
}) ->
    RequestedSec = emqx_mqtt_props:get('Session-Expiry-Interval', ConnProps, 0),
    MaxMs = get_mqtt_conf(Zone, max_session_expiry_interval),
    timer:seconds(clamp_session_expiry(RequestedSec, MaxMs));
expiry_interval(Zone, #mqtt_packet_connect{clean_start = false}) ->
    get_mqtt_conf(Zone, session_expiry_interval);
expiry_interval(_, #mqtt_packet_connect{clean_start = true}) ->
    0.

clamp_session_expiry(RequestedSec, infinity) ->
    RequestedSec;
clamp_session_expiry(RequestedSec, MaxMs) when is_integer(MaxMs) ->
    min(RequestedSec, MaxMs div 1000).

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
    case run_hooks('client.connect', [ConnInfo], ConnProps, Channel) of
        Error = {error, _Reason} -> Error;
        NConnProps -> {ok, emqx_packet:set_props(NConnProps, ConnPkt), Channel}
    end.

%%--------------------------------------------------------------------
%% Check Connect Packet

check_connect(ConnPkt, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_packet:check(ConnPkt, emqx_mqtt_caps:get_caps(Zone)).

%%--------------------------------------------------------------------
%% Enrich Client Info

enrich_client(ConnPkt, Channel = #channel{clientinfo = ClientInfo, conninfo = ConnInfo}) ->
    Pipe = emqx_utils:pipeline(
        [
            fun set_username/2,
            fun set_bridge_mode/2,
            fun maybe_username_as_clientid/2,
            fun maybe_assign_clientid/2,
            %% attr init should happen after clientid and username assign
            fun(NConnPkt, NClientInfo) ->
                maybe_set_client_initial_attrs(NConnPkt, NClientInfo, ConnInfo)
            end,
            %% clientid override should happen after client_attrs is initialized
            fun maybe_override_clientid/2
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

%% The `is_bridge` bit flag in CONNECT packet (parsed as `bridge_mode`)
%% is invented by mosquitto, named 'try_private': https://mosquitto.org/man/mosquitto-conf-5.html
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
    RandClientId = emqx_utils:rand_id(?RAND_CLIENTID_BYTES),
    ?EXT_TRACE_ADD_ATTRS(#{'client.clientid' => RandClientId}),
    {ok, ClientInfo#{clientid => RandClientId}};
maybe_assign_clientid(#mqtt_packet_connect{clientid = ClientId}, ClientInfo) ->
    {ok, ClientInfo#{clientid => ClientId}}.

get_client_attrs_init_config(Zone) ->
    get_mqtt_conf(Zone, client_attrs_init, []).

maybe_set_client_initial_attrs(ConnPkt, #{zone := Zone} = ClientInfo, ConnInfo) ->
    case get_client_attrs_init_config(Zone) of
        [] ->
            {ok, ClientInfo};
        Inits ->
            UserProperty = get_user_property_as_map(ConnPkt),
            Password = get_connect_password(ConnPkt),
            RenderCtx = client_attrs_init_render_ctx(
                ClientInfo#{user_property => UserProperty, password => Password},
                ConnInfo
            ),
            Attrs0 = maps:get(client_attrs, ClientInfo, #{}),
            Attrs1 = initialize_client_attrs(Inits, RenderCtx),
            {ok, ClientInfo#{client_attrs => maps:merge(Attrs0, Attrs1)}}
    end.

get_connect_password(#mqtt_packet_connect{password = Password}) ->
    Password.

client_attrs_init_render_ctx(Ctx, ConnInfo) ->
    client_attrs_init_render_ctx_cn(add_cert_san_render_ctx(Ctx, ConnInfo)).

client_attrs_init_render_ctx_cn(#{cn := CN} = Ctx) ->
    client_attrs_init_render_ctx_dn(Ctx#{cert_common_name => CN});
client_attrs_init_render_ctx_cn(Ctx) ->
    client_attrs_init_render_ctx_dn(Ctx).

client_attrs_init_render_ctx_dn(#{dn := DN} = Ctx) ->
    Ctx#{cert_subject => DN};
client_attrs_init_render_ctx_dn(Ctx) ->
    Ctx.

add_cert_san_render_ctx(Ctx, ConnInfo) ->
    PeerCert = maps:get(peercert, ConnInfo, undefined),
    CertSAN = esockd_peercert:subject_alt_names(PeerCert),
    ok = validate_cert_san(CertSAN, PeerCert),
    Ctx#{cert_san => cert_san_render_ctx(CertSAN)}.

validate_cert_san(nosan, _Source) ->
    ok;
validate_cert_san(CertSAN, Source) when is_map(CertSAN) ->
    maps:foreach(
        fun(NameType, Values) ->
            lists:foreach(
                fun(Value) ->
                    validate_peercert_string({cert_san, NameType}, Value, Source)
                end,
                Values
            )
        end,
        CertSAN
    ).

cert_san_render_ctx(nosan) ->
    empty_cert_san_render_ctx();
cert_san_render_ctx(CertSAN) when is_map(CertSAN) ->
    maps:merge(empty_cert_san_render_ctx(), CertSAN).

empty_cert_san_render_ctx() ->
    #{dns => [], ip => [], email => [], uri => []}.

initialize_client_attrs(Inits, #{clientid := ClientId} = ClientInfo) ->
    lists:foldl(
        fun(#{expression := Variform, set_as_attr := Name}, Acc) ->
            case emqx_variform:render(Variform, ClientInfo) of
                {ok, <<>>} ->
                    ?SLOG(
                        debug,
                        #{
                            msg => "client_attr_rednered_to_empty_string",
                            set_as_attr => Name
                        },
                        #{clientid => ClientId}
                    ),
                    Acc;
                {ok, Value} ->
                    case emqx_utils:is_mqtt_safe_utf8(Value) of
                        true ->
                            ?SLOG(
                                debug,
                                #{
                                    msg => "client_attr_initialized",
                                    set_as_attr => Name,
                                    attr_value => Value
                                },
                                #{clientid => ClientId}
                            ),
                            Acc#{Name => Value};
                        false ->
                            ?SLOG(
                                warning,
                                #{
                                    msg => "client_attr_dropped",
                                    set_as_attr => Name,
                                    reason => contains_control_characters
                                },
                                #{clientid => ClientId}
                            ),
                            Acc
                    end;
                {error, Reason} ->
                    ?SLOG(
                        warning,
                        #{
                            msg => "client_attr_initialization_failed",
                            reason => Reason
                        },
                        #{clientid => ClientId}
                    ),
                    Acc
            end
        end,
        #{},
        Inits
    ).

maybe_override_clientid(_ConnPkt, #{zone := Zone} = ClientInfo) ->
    Expression = get_mqtt_conf(Zone, clientid_override, disabled),
    override_clientid(Expression, ClientInfo).

override_clientid(disabled, ClientInfo) ->
    {ok, ClientInfo};
override_clientid(Expression, #{clientid := OrigClientId} = ClientInfo) ->
    case emqx_variform:render(Expression, ClientInfo) of
        {ok, <<>>} ->
            ?SLOG(
                error,
                #{
                    msg => "clientid_override_expression_returned_empty_string"
                },
                #{clientid => OrigClientId}
            ),
            {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID, ClientInfo};
        {ok, ClientId} ->
            % Must add 'clientid' log meta for trace log filter
            ?TRACE("MQTT", "clientid_overridden", #{
                clientid => ClientId, original_clientid => OrigClientId
            }),
            {ok, ClientInfo#{clientid => ClientId}};
        {error, Reason} ->
            ?SLOG(
                error,
                #{
                    msg => "clientid_override_expression_failed",
                    reason => Reason
                },
                #{clientid => OrigClientId}
            ),
            {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID, ClientInfo}
    end.

get_user_property_as_map(#mqtt_packet_connect{properties = #{'User-Property' := UserProperty}}) when
    is_list(UserProperty)
->
    maps:from_list(UserProperty);
get_user_property_as_map(_) ->
    #{}.

fix_mountpoint(#{mountpoint := undefined, zone := Zone} = ClientInfo) ->
    case get_mqtt_conf(Zone, namespace_as_mountpoint, false) of
        true ->
            case get_tenant_namespace(ClientInfo) of
                undefined ->
                    ClientInfo;
                Tns ->
                    ClientInfo#{mountpoint => iolist_to_binary([Tns, "/"])}
            end;
        false ->
            ClientInfo
    end;
fix_mountpoint(#{mountpoint := MountPoint} = ClientInfo) ->
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    ClientInfo#{mountpoint := MountPoint1}.

fix_mountpoint(_PipelineOutput, #channel{clientinfo = ClientInfo0} = Channel0) ->
    ClientInfo = fix_mountpoint(ClientInfo0),
    Channel = Channel0#channel{clientinfo = ClientInfo},
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Set log metadata

set_log_meta(_ConnPkt, #channel{clientinfo = #{clientid := ClientId} = ClientInfo}) ->
    proc_lib:set_label(ClientId),
    Username = maps:get(username, ClientInfo, undefined),
    Tns = get_tenant_namespace(ClientInfo),
    emqx_logger:set_metadata_clientid(ClientId),
    emqx_logger:set_proc_metadata([{username, Username}, {tns, Tns}]).

get_tenant_namespace(ClientInfo) ->
    Attrs = maps:get(client_attrs, ClientInfo, #{}),
    maps:get(?CLIENT_ATTR_NAME_TNS, Attrs, undefined).

%%--------------------------------------------------------------------
%% Check banned

check_banned(_ConnPkt, #channel{clientinfo = ClientInfo}) ->
    case emqx_banned:check(ClientInfo) of
        true ->
            ok = emqx_metrics:inc_global('client.banned'),
            {error, ?RC_BANNED};
        false ->
            ok
    end.

%%--------------------------------------------------------------------
%% Flapping

count_flapping_event(_ConnPkt, #channel{clientinfo = ClientInfo}) ->
    _ = emqx_flapping:detect(ClientInfo),
    ok.

%%--------------------------------------------------------------------
%% Authenticate

%% If peercert exists, add it as `cert_pem` credential field.
maybe_add_cert(Map, #channel{conninfo = ConnInfo}) ->
    maybe_add_cert(Map, ConnInfo);
maybe_add_cert(Map, #{peercert := PeerCert}) when is_binary(PeerCert) ->
    %% NOTE: it's raw binary at this point,
    %% encoding to PEM (base64) is done lazy in emqx_auth_template:render_var
    Map#{cert_pem => PeerCert};
maybe_add_cert(Map, _) ->
    Map.

authenticate(?PACKET(?AUTH) = Packet, Channel) ->
    %% TODO: extended authentication sub-span
    process_authenticate(Packet, Channel);
authenticate(Packet, Channel) ->
    %% Authenticate by CONNECT Packet
    ?EXT_TRACE_CLIENT_AUTHN(
        ?EXT_TRACE_ATTR(
            #{
                'client.clientid' => info(clientid, Channel),
                'client.username' => info(username, Channel)
            }
        ),
        fun(NPacket) ->
            Res = process_authenticate(NPacket, Channel),
            ?EXT_TRACE_ADD_ATTRS(authn_attrs(Res)),
            case Res of
                {ok, _, _} -> ?EXT_TRACE_SET_STATUS_OK();
                %% TODO: Enhanced AUTH
                {continue, _, _} -> ok;
                {error, _, _} -> ?EXT_TRACE_SET_STATUS_ERROR()
            end,
            Res
        end,
        [Packet]
    ).

process_authenticate(
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
    %% Auth with CONNECT packet for MQTT v5
    AuthData = emqx_mqtt_props:get('Authentication-Data', Properties, undefined),
    Credential0 =
        ClientInfo#{
            auth_method => AuthMethod,
            auth_data => AuthData,
            auth_cache => AuthCache
        },
    Credential = maybe_add_cert(Credential0, Channel),
    authentication_pipeline(Credential, Channel);
process_authenticate(
    ?CONNECT_PACKET(#mqtt_packet_connect{password = Password}),
    #channel{clientinfo = ClientInfo} = Channel
) ->
    %% Auth with CONNECT packet for MQTT v3
    Credential = maybe_add_cert(ClientInfo#{password => Password}, Channel),
    authentication_pipeline(Credential, Channel);
process_authenticate(
    ?AUTH_PACKET(_, #{'Authentication-Method' := AuthMethod} = Properties),
    #channel{
        clientinfo = ClientInfo,
        conninfo = #{conn_props := ConnProps},
        auth_cache = AuthCache
    } = Channel
) ->
    %% Enhanced auth
    case emqx_mqtt_props:get('Authentication-Method', ConnProps, undefined) of
        AuthMethod ->
            AuthData = emqx_mqtt_props:get('Authentication-Data', Properties, undefined),
            authentication_pipeline(
                ClientInfo#{
                    auth_method => AuthMethod,
                    auth_data => AuthData,
                    auth_cache => AuthCache
                },
                Channel
            );
        _ ->
            log_auth_failure("bad_authentication_method"),
            {error, ?RC_BAD_AUTHENTICATION_METHOD}
    end.

authentication_pipeline(Credential, Channel) ->
    %% Slightly adapted version of `emqx_utils:pipeline' because `do_authenticate/2' may
    %% return `{continue, _, _}', which serves as a special short-circuit return value.
    emqx_utils:foldl_while(
        fun(Fn, {ok, OutputAcc, ChanAcc}) ->
            case Fn(OutputAcc, ChanAcc) of
                ok -> {cont, {ok, OutputAcc, ChanAcc}};
                {ok, NewChan} -> {cont, {ok, OutputAcc, NewChan}};
                {ok, NewOutput, NewChan} -> {cont, {ok, NewOutput, NewChan}};
                {error, Reason} -> {halt, {error, Reason, ChanAcc}};
                {error, Reason, NewChan} -> {halt, {error, Reason, NewChan}};
                {continue, _Props, _Chan} = Res -> {halt, Res}
            end
        end,
        {ok, Credential, Channel},
        [
            fun do_authenticate/2,
            fun fix_mountpoint/2,
            fun handle_zone_change/2,
            %% We call `set_log_meta' again here because authentication may have injected
            %% different attributes.  Note that `clientid` might have changed as well, if
            %% authentication returned a non-empty `clientid_override` value.
            fun set_log_meta/2
        ]
    ).

do_authenticate(
    #{auth_method := AuthMethod} = Credential,
    #channel{clientinfo = ClientInfo} = Channel
) ->
    Properties = #{'Authentication-Method' => AuthMethod},
    case emqx_access_control:authenticate(Credential) of
        {ok, AuthResult} ->
            Channel1 = Channel#channel{
                clientinfo = merge_auth_result(ClientInfo, AuthResult),
                auth_cache = #{}
            },
            with_post_authn(Channel1, Properties);
        {ok, AuthResult, AuthData} ->
            Channel1 = Channel#channel{
                clientinfo = merge_auth_result(ClientInfo, AuthResult),
                auth_cache = #{}
            },
            with_post_authn(Channel1, Properties#{'Authentication-Data' => AuthData});
        {continue, AuthCache} ->
            {continue, Properties, Channel#channel{auth_cache = AuthCache}};
        {continue, AuthData, AuthCache} ->
            {continue, Properties#{'Authentication-Data' => AuthData}, Channel#channel{
                auth_cache = AuthCache
            }};
        {error, Reason} ->
            log_auth_failure(Reason),
            {error, emqx_reason_codes:connack_error(Reason)}
    end;
do_authenticate(Credential, #channel{clientinfo = ClientInfo} = Channel) ->
    case emqx_access_control:authenticate(Credential) of
        {ok, AuthResult} ->
            Channel1 = Channel#channel{clientinfo = merge_auth_result(ClientInfo, AuthResult)},
            with_post_authn(Channel1, #{});
        {error, Reason} ->
            log_auth_failure(Reason),
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

%% Runs the 'client.post_authn' hook. The accumulator is a context map
%% (`emqx_hookpoints:post_authn_context()'); a callback may return
%% `{ok, NewContext}' to replace it, or `{stop, {error, Reason}}' to reject
%% the client (treated like an authn failure). Using a map leaves room to
%% pass additional inputs/outputs in the future without breaking callbacks.
with_post_authn(#channel{clientinfo = ClientInfo} = Channel, Properties) ->
    Ctx0 = #{client_info => ClientInfo},
    case run_hooks('client.post_authn', [], Ctx0, Channel) of
        {error, Reason} ->
            log_auth_failure(Reason),
            {error, emqx_reason_codes:connack_error(Reason)};
        #{client_info := NewClientInfo} ->
            {ok, Properties, Channel#channel{clientinfo = NewClientInfo}}
    end.

log_auth_failure(Reason) ->
    ?SLOG_THROTTLE(
        warning,
        #{
            msg => authentication_failure,
            reason => Reason
        },
        #{tag => "AUTHN"}
    ).

%% Merge authentication result into ClientInfo
%% Authentication result may include:
%% 1. `is_superuser': The superuser flag from various backends
%% 2. `expire_at`: Authentication validity deadline, the client will be disconnected after this time
%% 3. `acl': ACL rules from JWT, HTTP auth backend
%% 4. `client_attrs': Extra client attributes from JWT, HTTP auth backend
%% 5. `clientid_override': This result should override the current clientid.
%% 6. `zone_override': This result should override the current zone.
%% 7. Maybe more non-standard fields used by hook callbacks
merge_auth_result(ClientInfo0, AuthResult0) when is_map(ClientInfo0) andalso is_map(AuthResult0) ->
    IsSuperuser = maps:get(is_superuser, AuthResult0, false),
    ExpireAt = maps:get(expire_at, AuthResult0, undefined),
    AuthResult = maps:without([client_attrs, expire_at], AuthResult0),
    Attrs0 = maps:get(client_attrs, ClientInfo0, #{}),
    Attrs1 = maps:get(client_attrs, AuthResult0, #{}),
    Attrs = maps:merge(Attrs0, Attrs1),
    ClientIdOverride = maps:get(clientid_override, AuthResult0, undefined),
    ZoneOverride = maps:get(zone_override, AuthResult0, undefined),
    ClientInfo1 =
        case parse_zone_override(ZoneOverride) of
            false ->
                ClientInfo0;
            {ok, NewZone} ->
                OldZone = maps:get(zone, ClientInfo0, default),
                ?TRACE("MQTT", "zone_overridden_by_authn", #{
                    zone => NewZone,
                    original_zone => OldZone
                }),
                ClientInfo0#{zone => NewZone, old_zone => OldZone}
        end,
    ClientInfo =
        case is_binary(ClientIdOverride) andalso ClientIdOverride /= <<"">> of
            true ->
                ?TRACE("MQTT", "clientid_overridden_by_authn", #{
                    clientid => ClientIdOverride,
                    original_clientid => maps:get(clientid, ClientInfo1, undefined)
                }),
                ClientInfo1#{clientid => ClientIdOverride};
            false ->
                ClientInfo1
        end,
    maps:merge(
        ClientInfo#{client_attrs => Attrs},
        AuthResult#{
            is_superuser => IsSuperuser,
            auth_expire_at => ExpireAt
        }
    ).

parse_zone_override(undefined) ->
    false;
parse_zone_override(ZoneOverride) when is_binary(ZoneOverride) ->
    try emqx_config_zones:assert_zone_exists(ZoneOverride) of
        ok -> {ok, binary_to_existing_atom(ZoneOverride, utf8)}
    catch
        throw:{unknown_zone, _} ->
            ?TRACE("MQTT", "unknown_zone_override_in_authn", #{zone => ZoneOverride}),
            false
    end.

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

check_quota_exceeded(
    ?PUBLISH_PACKET(_QoS, Topic, _PacketId, Payload), #channel{quota = Quota} = Chann
) ->
    Result = emqx_limiter_client_container:try_consume(
        Quota, [{bytes, erlang:byte_size(Payload)}, {messages, 1}]
    ),
    case Result of
        {true, Quota2} ->
            {ok, Chann#channel{quota = Quota2}};
        {false, Quota2, Reason} ->
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => cannot_publish_to_topic_due_to_quota_exceeded,
                    reason => Reason
                },
                #{topic => Topic, tag => "QUOTA"}
            ),
            {error, ?RC_QUOTA_EXCEEDED, Chann#channel{quota = Quota2}}
    end.

check_subscribe_quota_exceeded(
    #subscribe_operation{topic_filters = TopicFilters}, #channel{quota = Limiter0} = Chann
) ->
    Result = emqx_limiter_client_container:try_consume(
        Limiter0, [{subscribes, 1}]
    ),
    case Result of
        {true, Limiter} ->
            {ok, Chann#channel{quota = Limiter}};
        {false, Limiter, Reason} ->
            ?tp("subscribe_rate_limit", #{reason => Reason}),
            ?SLOG_THROTTLE(
                warning,
                #{
                    msg => cannot_subscribe_to_topic_filters_due_to_quota_exceeded,
                    reason => Reason
                },
                #{topics => TopicFilters, tag => "QUOTA"}
            ),
            {error, ?RC_QUOTA_EXCEEDED, Chann#channel{quota = Limiter}}
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
%% Authorization action

authz_action({_Topic, #{qos := QoS} = _SubOpts} = _TopicFilter) ->
    ?AUTHZ_SUBSCRIBE(QoS).

%%--------------------------------------------------------------------
%% Check Pub Authorization

check_pub_authz(Msg, Channel = #channel{clientinfo = ClientInfo}) ->
    ?EXT_TRACE_CLIENT_AUTHZ(
        ?EXT_TRACE_ATTR(
            (basic_attrs(Channel))#{'authz.action_type' => publish}
        ),
        fun(NMsg) ->
            do_check_pub_authz(NMsg, Channel, ClientInfo)
        end,
        [Msg]
    ).

do_check_pub_authz(
    Msg,
    Channel,
    ClientInfo
) ->
    case emqx_message_ingress:ingress_and_authorize(ClientInfo, Msg) of
        {allow, NMsg = #message{topic = Topic}} ->
            ?EXT_TRACE_ADD_ATTRS(#{
                'authz.publish.topic' => Topic,
                'authz.publish.result' => allow
            }),
            ?EXT_TRACE_SET_STATUS_OK(),
            {ok, NMsg, Channel};
        deny ->
            ?EXT_TRACE_ADD_ATTRS(#{
                'authz.publish.result' => deny,
                'authz.reason_code' => ?RC_NOT_AUTHORIZED
            }),
            ?EXT_TRACE_SET_STATUS_ERROR(),
            {error, ?RC_NOT_AUTHORIZED, Channel};
        {error, invalid_topic_name} ->
            {error, ?RC_TOPIC_NAME_INVALID, Channel};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "message_ingress_failed", reason => Reason}),
            {error, ?RC_NOT_AUTHORIZED, Channel}
    end.

%%--------------------------------------------------------------------
%% Check Pub Caps

check_pub_caps(
    #message{qos = QoS, flags = #{retain := Retain}, topic = Topic},
    #channel{clientinfo = #{zone := Zone}}
) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain, topic => Topic}).

%%--------------------------------------------------------------------
%% Check Subscribe Packet

check_subscribe(
    #subscribe_operation{properties = Properties, topic_filters = TopicFilters},
    #channel{clientinfo = #{zone := Zone}}
) ->
    case
        emqx_packet:check_subscribe(
            Properties,
            TopicFilters,
            #{
                subscription_message_filter => get_mqtt_conf(
                    Zone, subscription_message_filter, disable
                )
            }
        )
    of
        ok -> ok;
        {error, RC} -> {error, {disconnect, RC}}
    end.

%%--------------------------------------------------------------------
%% Check Sub Authorization

check_sub_authzs(Operation, Channel) ->
    ?EXT_TRACE_CLIENT_AUTHZ(
        ?EXT_TRACE_ATTR((basic_attrs(Channel))#{'authz.action_type' => subscribe}),
        fun(NOperation) -> _Res = do_check_sub_authzs(NOperation, Channel) end,
        [Operation]
    ).

do_check_sub_authzs(
    Operation = #subscribe_operation{topic_filters = TopicFilters0},
    Channel = #channel{clientinfo = ClientInfo}
) ->
    CheckResult = do_check_sub_authzs2(TopicFilters0, ClientInfo),
    HasAuthzDeny = lists:any(
        fun({{_TopicFilter, _SubOpts}, ReasonCode}) ->
            ReasonCode =:= ?RC_NOT_AUTHORIZED
        end,
        CheckResult
    ),
    DenyAction = emqx:get_config([authorization, deny_action], ignore),
    case {HasAuthzDeny, DenyAction} of
        {true, disconnect} ->
            ?EXT_TRACE_ADD_ATTRS((sub_authz_attrs(CheckResult))#{
                'authz.deny_action' => disconnect
            }),
            ?EXT_TRACE_SET_STATUS_ERROR(),
            {error, {disconnect, ?RC_NOT_AUTHORIZED}, Channel};
        {true, ignore} ->
            ?EXT_TRACE_ADD_ATTRS((sub_authz_attrs(CheckResult))#{
                'authz.deny_action' => ignore
            }),
            ?EXT_TRACE_SET_STATUS_ERROR(),
            {ok, Operation#subscribe_operation{topic_filters = CheckResult}, Channel};
        {false, _} ->
            ?EXT_TRACE_SET_STATUS_OK(),
            {ok, Operation#subscribe_operation{topic_filters = CheckResult}, Channel}
    end.

do_check_sub_authzs2(TopicFilters, ClientInfo) ->
    AuthzContext = emqx_authz_context:make(ClientInfo),
    do_check_sub_authzs2(AuthzContext, TopicFilters, []).

do_check_sub_authzs2(_AuthzContext, [], Acc) ->
    lists:reverse(Acc);
do_check_sub_authzs2(AuthzContext, [TopicFilter = {Topic, _SubOpts} | More], Acc) ->
    %% subscribe authz check only cares the real topic filter when shared-sub
    %% e.g. only check <<"t/#">> for <<"$share/g/t/#">>
    Action = authz_action(TopicFilter),
    case
        emqx_access_control:authorize(
            AuthzContext,
            Action,
            emqx_topic:get_shared_real_topic(Topic)
        )
    of
        %% TODO: support maximum QoS granted check by authz
        %% for now, we only check sub caps for QoS granted
        %% MQTT-3.1.1 [MQTT-3.8.4-6] and MQTT-5.0 [MQTT-3.8.4-7]
        %% Not implemented yet:
        %% {allow, RC} -> do_check_sub_authzs(ClientInfo, More, [{TopicFilter, RC} | Acc]);
        allow ->
            do_check_sub_authzs2(AuthzContext, More, [{TopicFilter, ?RC_SUCCESS} | Acc]);
        deny ->
            do_check_sub_authzs2(AuthzContext, More, [{TopicFilter, ?RC_NOT_AUTHORIZED} | Acc])
    end.

%%--------------------------------------------------------------------
%% Check Sub Caps

check_sub_caps(
    Operation = #subscribe_operation{topic_filters = TopicFilters},
    Channel = #channel{clientinfo = ClientInfo}
) ->
    CheckResult = do_check_sub_caps(ClientInfo, TopicFilters),
    case lists:any(fun is_shared_sub_not_supported/1, CheckResult) of
        true ->
            %% [MQTT-4.13.1-1]: attempting a shared subscription when shared
            %% subscriptions are not supported is a protocol error, the
            %% network connection must be closed.
            {error, {disconnect, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}, Channel};
        false ->
            {ok, Operation#subscribe_operation{topic_filters = CheckResult}, Channel}
    end.

is_shared_sub_not_supported({{_Topic, _SubOpts}, ?RC_SHARED_SUBSCRIPTIONS_NOT_SUPPORTED}) ->
    true;
is_shared_sub_not_supported(_) ->
    false.

do_check_sub_caps(ClientInfo, TopicFilters) ->
    do_check_sub_caps(ClientInfo, TopicFilters, []).

do_check_sub_caps(_ClientInfo, [], Acc) ->
    lists:reverse(Acc);
do_check_sub_caps(ClientInfo, [TopicFilter = {{Topic, SubOpts}, ?RC_SUCCESS} | More], Acc) ->
    case emqx_mqtt_caps:check_sub(ClientInfo, Topic, SubOpts) of
        ok ->
            do_check_sub_caps(ClientInfo, More, [TopicFilter | Acc]);
        {ok, MaxQoS} ->
            ?SLOG(
                debug,
                #{
                    msg => "subscribe_allowed_but_with_lower_granted_qos",
                    reason => emqx_reason_codes:name(MaxQoS)
                },
                #{topic => Topic}
            ),
            do_check_sub_caps(ClientInfo, More, [
                {{Topic, SubOpts#{qos => MaxQoS}}, ?RC_SUCCESS} | Acc
            ]);
        {error, NRC} ->
            ?SLOG(
                warning,
                #{
                    msg => "cannot_subscribe_topic_filter",
                    reason => emqx_reason_codes:name(NRC)
                },
                #{topic => Topic}
            ),
            do_check_sub_caps(ClientInfo, More, [{{Topic, SubOpts}, NRC} | Acc])
    end;
do_check_sub_caps(ClientInfo, [TopicFilter = {{_Topic, _SubOpts}, _OtherRC} | More], Acc) ->
    do_check_sub_caps(ClientInfo, More, [TopicFilter | Acc]).

%%--------------------------------------------------------------------
%% Run Subscribe Hooks

run_sub_hooks(
    #subscribe_operation{properties = Properties, topic_filters = TopicFilters0},
    Channel = #channel{clientinfo = ClientInfo}
) ->
    TopicFilters = [
        TopicFilter
     || {TopicFilter, ?RC_SUCCESS} <- TopicFilters0
    ],
    _NTopicFilters = run_hooks('client.subscribe', [ClientInfo, Properties], TopicFilters, Channel).

%%--------------------------------------------------------------------
%% Enrich SubOpts

enrich_subscribe(
    Operation = #subscribe_operation{properties = Properties, topic_filters = TopicFilters},
    Channel
) ->
    NTopicFilters = do_enrich_subscribe(Properties, TopicFilters, Channel),
    {ok, Operation#subscribe_operation{topic_filters = NTopicFilters}, Channel}.

do_enrich_subscribe(Properties, TopicFilters, Channel) ->
    _NTopicFilters = emqx_utils:run_fold(
        [
            %% TODO: do try catch with reason code here
            fun(TFs, #{channel := Channel0}) -> parse_raw_topic_filters(TFs, Channel0) end,
            fun enrich_subopts_subid/2,
            fun enrich_subopts_props/2,
            fun enrich_subopts_flags/2
        ],
        TopicFilters,
        #{sub_props => Properties, channel => Channel}
    ).

enrich_subopts_subid(TopicFilters, #{sub_props := #{'Subscription-Identifier' := SubId}}) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
enrich_subopts_subid(TopicFilters, _State) ->
    TopicFilters.

enrich_subopts_props(TopicFilters, #{sub_props := SubProps}) ->
    [{Topic, SubOpts#{sub_props => SubProps}} || {Topic, SubOpts} <- TopicFilters].

enrich_subopts_flags(TopicFilters, #{channel := Channel}) ->
    do_enrich_subopts_flags(TopicFilters, Channel).

do_enrich_subopts_flags(TopicFilters, ?IS_MQTT_V5) ->
    [{Topic, merge_default_subopts(SubOpts)} || {Topic, SubOpts} <- TopicFilters];
do_enrich_subopts_flags(TopicFilters, #channel{clientinfo = #{zone := Zone, is_bridge := IsBridge}}) ->
    Rap = flag(IsBridge),
    NL = flag(get_mqtt_conf(Zone, ignore_loop_deliver)),
    [
        {Topic, (merge_default_subopts(SubOpts))#{rap => Rap, nl => NL}}
     || {Topic, SubOpts} <- TopicFilters
    ].

merge_default_subopts(SubOpts) ->
    maps:merge(?DEFAULT_SUBOPTS, SubOpts).

%%--------------------------------------------------------------------
%% Enrich ConnAck Caps

enrich_connack_caps(AckProps, ?IS_MQTT_V5 = Channel) ->
    #channel{
        clientinfo = #{
            zone := Zone
        },
        conninfo = #{
            receive_maximum := ReceiveMaximum
        }
    } = Channel,
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
        'Shared-Subscription-Available' => flag(Shared),
        'Receive-Maximum' => ReceiveMaximum
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
        KeepAlive -> AckProps#{'Server-Keep-Alive' => KeepAlive}
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
                    case get_mqtt_conf(Zone, response_information, <<"">>) of
                        <<"">> -> undefined;
                        RspInfo when is_binary(RspInfo) -> RspInfo;
                        %% Backward compatibility, handle iolist
                        "" -> undefined;
                        RspInfo when is_list(RspInfo) -> iolist_to_binary(RspInfo)
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
%% Enrich Clamped Session-Expiry-Interval
%% MQTT 5.0 §3.2.2.3.2: when the server modifies the value requested by the
%% client, it MUST return the chosen value in CONNACK so the client knows when
%% its session will actually expire.
enrich_clamped_session_expiry_interval(AckProps, ?IS_MQTT_V5 = #channel{conninfo = ConnInfo}) ->
    ClampedSec = maps:get(expiry_interval, ConnInfo, 0) div 1000,
    ConnProps = maps:get(conn_props, ConnInfo, #{}),
    RequestedSec = emqx_mqtt_props:get('Session-Expiry-Interval', ConnProps, 0),
    case ClampedSec =:= RequestedSec of
        true -> AckProps;
        false -> AckProps#{'Session-Expiry-Interval' => ClampedSec}
    end;
enrich_clamped_session_expiry_interval(AckProps, _Channel) ->
    AckProps.

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(millisecond)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo], Channel),
    schedule_connection_auth_expire(Channel#channel{
        conninfo = trim_conninfo(NConnInfo),
        conn_state = connected
    }).

schedule_connection_auth_expire(Channel = #channel{clientinfo = #{auth_expire_at := undefined}}) ->
    Channel;
schedule_connection_auth_expire(Channel = #channel{clientinfo = #{auth_expire_at := ExpireAt}}) ->
    Interval = max(0, ExpireAt - erlang:system_time(millisecond)),
    ensure_timer(connection_auth_expire, Interval, Channel).

trim_conninfo(ConnInfo) ->
    maps:without(
        [
            %% NOTE
            %% We remove the peercert because it duplicates what's stored in the socket,
            %% otherwise it wastes about 1KB per connection.
            %% Retrieve with: esockd_transport:peercert(Socket).
            %% Decode with APIs exported from esockd_peercert and esockd_ssl
            peercert
        ],
        ConnInfo
    ).

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
%% Ensure KeepAlive

%% MQTT 5
ensure_keepalive(#{'Server-Keep-Alive' := Interval}, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_quic_conn_idle_timeout(Interval, Channel),
    ensure_keepalive_timer(Interval, Channel#channel{conninfo = ConnInfo#{keepalive => Interval}});
%% MQTT 3,4
ensure_keepalive(_AckProps, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_quic_conn_idle_timeout(maps:get(keepalive, ConnInfo), Channel),
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_quic_conn_idle_timeout(Timeout, #channel{
    clientinfo = #{zone := Zone},
    conninfo = #{socktype := quic, sock := Sock}
}) ->
    Conn = element(2, Sock),
    #{keepalive_multiplier := Mul} =
        emqx_config:get_zone_conf(Zone, [mqtt]),
    %%% The original idle_timeout is from the listener, now we update it per connection
    %%% Conn could be closed so we don't check the ret val
    _ = quicer:setopt(Conn, settings, #{idle_timeout_ms => timer:seconds(Timeout * Mul)}, false),
    ok;
ensure_quic_conn_idle_timeout(_, _) ->
    ok.

ensure_keepalive_timer(0, Channel) ->
    Channel;
ensure_keepalive_timer(disabled, Channel) ->
    Channel;
ensure_keepalive_timer(
    Interval, Channel = #channel{clientinfo = #{zone := Zone}, conninfo = ConnInfo}
) ->
    Val =
        case maps:get(conn_shared_state, ConnInfo, undefined) of
            #{cnts_ref := CntRef} ->
                _MFA = {emqx_quic_connection, read_cnt, [CntRef, control_packet]};
            undefined ->
                emqx_pd:get_counter(recv_pkt)
        end,
    KeepAlive = emqx_keepalive:init(Zone, Val, Interval),
    ensure_timer(keepalive, Channel#channel{keepalive = KeepAlive}).

clear_keepalive(Channel = #channel{timers = Timers}) ->
    case maps:get(keepalive, Timers, undefined) of
        undefined ->
            Channel;
        TRef ->
            emqx_utils:cancel_timer(TRef),
            Channel#channel{timers = maps:without([keepalive], Timers)}
    end.
%%--------------------------------------------------------------------
%% Maybe Resume Session

maybe_resume_session(#channel{takeover = undefined}) ->
    ignore;
maybe_resume_session(
    #channel{
        session = Session,
        takeover = #resumption{replayctx = ReplayCtx},
        clientinfo = ClientInfo
    } = Channel
) ->
    {OkEffects, Publishes, NSession} = emqx_session:replay(ClientInfo, ReplayCtx, Session),
    NChannel = Channel#channel{takeover = undefined, session = NSession},
    {Publishes, apply_session_effects(OkEffects, NChannel)}.

%%--------------------------------------------------------------------
%% Maybe Shutdown the Channel

maybe_shutdown(Reason, _Intent = idle, Channel = #channel{conninfo = ConnInfo}) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?EXPIRE_INTERVAL_INFINITE ->
            {ok, Channel};
        I when I > 0 ->
            {ok, ensure_timer(expire_session, I, Channel)};
        _ ->
            shutdown(Reason, Channel)
    end;
maybe_shutdown(Reason, _Intent = shutdown, Channel) ->
    shutdown(Reason, Channel).

%%--------------------------------------------------------------------
%% Parse Topic Filters

%% [{<<"$share/group/topic">>, _SubOpts = #{}} | _]
parse_raw_topic_filters(TopicFilters, #channel{clientinfo = #{zone := Zone}}) ->
    FilterEnabled = is_subscription_message_filter_enabled(Zone),
    lists:map(
        fun(TopicFilter) -> parse_raw_topic_filter(TopicFilter, FilterEnabled) end,
        TopicFilters
    ).

parse_raw_topic_filter({TopicFilter, SubOpts0}, false) ->
    emqx_topic:parse({TopicFilter, SubOpts0});
parse_raw_topic_filter({TopicFilter, SubOpts0}, true) ->
    case emqx_subscription_filter:validate_subscription(TopicFilter) of
        {ok, no_filter} ->
            emqx_topic:parse({TopicFilter, SubOpts0});
        {ok, #{base_topic := BaseTopic, raw_expr := RawExpr, ast := AST}} ->
            emqx_topic:parse(
                {BaseTopic, SubOpts0#{
                    sub_filter_raw => RawExpr,
                    sub_filter_ast => AST,
                    sub_filter_enabled => true,
                    sub_filter_source => TopicFilter
                }}
            );
        {error, malformed_filter} ->
            error({invalid_subscription_filter, TopicFilter})
    end;
parse_raw_topic_filter(TopicFilter, false) ->
    emqx_topic:parse(TopicFilter);
parse_raw_topic_filter(TopicFilter, true) ->
    emqx_topic:parse(emqx_subscription_filter:normalize_topic_filter(TopicFilter)).

is_subscription_message_filter_enabled(Zone) ->
    get_mqtt_conf(Zone, subscription_message_filter, disable) =:= enable.

%%--------------------------------------------------------------------
%% Maybe & Ensure disconnected

ensure_disconnected(_Reason, Channel = #channel{conn_state = disconnected}) ->
    Channel;
ensure_disconnected(
    Reason,
    Channel = #channel{
        conninfo = ConnInfo,
        clientinfo = ClientInfo
    }
) ->
    ok = emqx_authz_cache:empty_authz_cache(),
    %% If disconnected_at is already set (e.g., during takeover begin), don't overwrite it.
    %% This ensures disconnected_at is recorded early enough to be before the new session's connected_at.
    NConnInfo =
        case maps:is_key(disconnected_at, ConnInfo) of
            true -> ConnInfo;
            false -> ConnInfo#{disconnected_at => erlang:system_time(millisecond)}
        end,
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo], Channel),
    ChanPid = self(),
    emqx_cm:mark_channel_disconnected(ChanPid),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

session_disconnect(ClientInfo, ConnInfo, Session) when Session /= undefined ->
    emqx_session:disconnect(ClientInfo, ConnInfo, Session);
session_disconnect(_ClientInfo, _ConnInfo, undefined) ->
    {shutdown, undefined}.

%%--------------------------------------------------------------------
%% Maybe Publish will msg
%% @doc Maybe publish will message [MQTT-3.1.2-8]
%%   When willmsg presents the decision whether or when to publish the Will Message are effected by
%%   the followings:
%%   - connecion state
%%   - If it is MQTT normal disconnection (RC: 0) or abnormal (RC != 0) from the *client*
%%   - will delay interval (MQTT 5.0 only)
%%   - session expire Session Expiry (MQTT 5.0 only)
%%   - EMQX operations on the client
%% @NOTE:
%%    Connection close with session expiry interval = 0 means session close.
%% @NOTE:
%%    The caller does not need to take care of the case when process terminates while will_msg is published
%%    as it is designed by the spec.
%% @NOTE:
%%    this function should be safe to be called multiple times in the life time of the connecion process, the willmsg
%%    must be delete from the state if it is published or cleared.
-spec maybe_publish_will_msg(Reason, channel()) -> channel() when
    Reason ::
        %% Connection is terminating because session is taken over by another process.
        takenover
        %% Connection is terminating because of EMQX mgmt operation, the session state is deleted with none-zero RC code
        | kicked
        %% Connection is terminating because of client clean start new session.
        | discarded
        %% Connection is terminating because session is expired
        | expired
        %% Connection is terminating because of socket close/error
        | sock_closed
        %% Connection is terminating because auth expired
        | auth_expired
        %% Session is terminating, delay willmsg publish is impossible.
        | ?chan_terminating.
maybe_publish_will_msg(_Reason, Channel = #channel{will_msg = undefined}) ->
    %% No will message to publish
    Channel;
maybe_publish_will_msg(
    _Reason,
    Channel = #channel{
        conn_state = ConnState,
        conninfo = #{clientid := ClientId}
    }
) when
    ConnState =:= idle orelse
        ConnState =:= connecting orelse
        ConnState =:= reauthenticating
->
    %% Wrong state to publish, they are intermediate state
    ?tp(debug, willmsg_wrong_state, #{clientid => ClientId}),
    Channel;
maybe_publish_will_msg(
    _Reason,
    Channel0 = #channel{
        conninfo = #{proto_ver := ?MQTT_PROTO_V3, clientid := ClientId}
    }
) ->
    %% Unconditionally publish will message for MQTT 3.1.1
    ?tp(debug, maybe_publish_willmsg_v3, #{clientid => ClientId}),
    Channel = publish_will_msg(Channel0),
    remove_willmsg(Channel);
maybe_publish_will_msg(
    Reason,
    Channel0 = #channel{
        conninfo = #{clientid := ClientId}
    }
) when
    Reason =:= expired orelse
        Reason =:= discarded orelse
        Reason =:= kicked orelse
        Reason =:= ?chan_terminating orelse
        %% Depends on the session backend, we may lost the session
        Reason =:= {shutdown, internal_error}
->
    %% For the cases that session MUST be gone impiles that the will message MUST be published
    %% a. expired (session expired)
    %% b. discarded (Session ends because of clean start)
    %% c. kicked. (kicked by operation, abnormal conn close)
    %% d. internal_error (maybe not recoverable)
    %% This ensures willmsg will be published if the willmsg timer is scheduled but not fired
    %% OR fired but not yet handled
    %% NOTE! For durable sessions, `?chan_terminating' does NOT imply that the session is
    %% gone.
    case is_durable_session(Channel0) andalso Reason =:= ?chan_terminating of
        false ->
            ?tp(debug, maybe_publish_willmsg_session_ends, #{clientid => ClientId, reason => Reason}),
            Channel = publish_will_msg(Channel0),
            remove_willmsg(Channel);
        true ->
            Channel0
    end;
maybe_publish_will_msg(
    takenover,
    Channel0 = #channel{
        will_msg = WillMsg,
        conninfo = #{clientid := ClientId}
    }
) ->
    %% TAKEOVER [MQTT-3.1.4-3]
    %% MQTT 5, Non-normative comment:
    %% """"
    %% If a Network Connection uses a Client Identifier of an existing Network Connection to the Server,
    %% the Will Message for the exiting connection is sent unless the new connection specifies Clean Start
    %% of 0 and the Will Delay is greater than zero. If the Will Delay is 0 the Will Message is sent at
    %% the close of the existing Network Connection, and if Clean Start is 1 the Will Message is sent
    %% because the Session ends.
    %% """"
    %% NOTE, above clean start=1 is `discard' scenarios not `takeover' scenario.
    %% NOTE: a session with expiry interval 0 never reaches here: the
    %% takeover-begin and takeover-kick paths publish its will message with
    %% reason `expired' before shutting down.
    case will_delay_interval(WillMsg) of
        0 ->
            ?tp(debug, maybe_publish_willmsg_takenover_pub, #{clientid => ClientId}),
            Channel = publish_will_msg(Channel0),
            ok;
        I when I > 0 ->
            %% @NOTE Non-normative comment in MQTT 5.0 spec
            %% """
            %% One use of this is to avoid publishing Will Messages if there is a temporary network
            %% disconnection and the Client succeeds in reconnecting and continuing its Session
            %% before the Will Message is published.
            %% """
            ?tp(debug, maybe_publish_willmsg_takenover_skip, #{clientid => ClientId}),
            Channel = Channel0,
            skip
    end,
    remove_willmsg(Channel);
maybe_publish_will_msg(
    Reason,
    Channel0 = #channel{
        will_msg = WillMsg,
        conninfo = #{clientid := ClientId}
    }
) ->
    %% Default to handle other reasons
    case will_delay_interval(WillMsg) of
        0 ->
            ?tp(debug, maybe_publish_will_msg_other_publish, #{
                clientid => ClientId, reason => Reason
            }),
            %% In case of Reason = auth_expired, the very expiration of auth is the reason of disconnection,
            %% so the will message should won't pass the auth check.
            %% We want to publish the will message as it would be published at the very moment of auth expiration,
            %% so we inject the auth expiration time as the current time used for the auth check.
            Channel1 = maybe_with_injected_now(Reason, Channel0, fun(Channel) ->
                publish_will_msg(Channel)
            end),
            remove_willmsg(Channel1);
        I when I > 0 ->
            ?tp(debug, maybe_publish_will_msg_other_delay, #{clientid => ClientId, reason => Reason}),
            ensure_timer(will_message, timer:seconds(I), Channel0)
    end.

will_delay_interval(WillMsg) ->
    maps:get(
        'Will-Delay-Interval',
        emqx_message:get_header(properties, WillMsg, #{}),
        0
    ).

maybe_with_injected_now(
    auth_expired, #channel{clientinfo = #{auth_expire_at := AuthExpiredAt}} = Channel, Fun
) ->
    with_now(AuthExpiredAt, Channel, Fun);
maybe_with_injected_now(_, Channel, Fun) ->
    Fun(Channel).

publish_will_msg(
    #channel{
        session = Session,
        clientinfo = ClientInfo,
        will_msg = Msg = #message{topic = Topic}
    } = Channel
) ->
    case prepare_will_message_for_publishing(ClientInfo, Msg) of
        {ok, PreparedMessage} ->
            NSession = emqx_session:publish_will_message_now(Session, PreparedMessage),
            Channel#channel{session = NSession};
        {error, #{client_banned := ClientBanned, publishing_disallowed := PublishingDisallowed}} ->
            ?tp(
                warning,
                last_will_testament_publish_denied,
                #{
                    topic => Topic,
                    client_banned => ClientBanned,
                    publishing_disallowed => PublishingDisallowed
                }
            ),
            Channel
    end.

prepare_will_message_for_publishing(ClientInfo, Msg) ->
    case emqx_message_ingress:ingress_and_authorize(ClientInfo, Msg) of
        {allow, NMsg0} ->
            ClientBanned = emqx_banned:check(ClientInfo),
            case ClientBanned of
                true ->
                    {error, #{
                        client_banned => true,
                        publishing_disallowed => false
                    }};
                false ->
                    NMsg = emqx_message_ingress:finalize(ClientInfo, NMsg0),
                    {ok, NMsg#message{timestamp = emqx_message:timestamp_now()}}
            end;
        deny ->
            {error, #{
                client_banned => false,
                publishing_disallowed => true
            }};
        {error, Reason} ->
            ?SLOG(warning, #{msg => "will_message_ingress_failed", reason => Reason}),
            {error, #{client_banned => false, publishing_disallowed => true}}
    end.

%%--------------------------------------------------------------------
%% Disconnect Reason

disconnect_reason(?RC_SUCCESS) -> ?normal;
disconnect_reason(ReasonCode) -> emqx_reason_codes:name(ReasonCode).

reason_code(takenover) -> ?RC_SESSION_TAKEN_OVER;
reason_code(discarded) -> ?RC_SESSION_TAKEN_OVER.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [run_hooks/3, run_hooks/4, run_hook_with_context/3, run_fold_with_context/4]}).
run_hooks(Name, Args, Channel) ->
    ok = inc_metrics(Name, Channel),
    emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc, Channel) ->
    ok = inc_metrics(Name, Channel),
    emqx_hooks:run_fold(Name, Args, Acc).

run_hook_with_context(Name, Args, Channel) ->
    HookContext = mk_common_hook_context(Channel),
    emqx_hooks:run(Name, HookContext, Args).

run_fold_with_context(Name, Args, Acc, Channel) ->
    HookContext = mk_common_hook_context(Channel),
    emqx_hooks:run_fold(Name, HookContext, Args, Acc).

run_client_connack_hooks(Channel, ConnInfo, Reason, AckProps) ->
    Name = 'client.connack',
    Ns = get_tenant_namespace(Channel#channel.clientinfo),
    Ctx0 = mk_common_hook_context(Channel),
    Ctx = Ctx0#{namespace => Ns},
    Args = [ConnInfo, Reason],
    emqx_hooks:run_fold(Name, Ctx, Args, AckProps).

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

-compile({inline, [reply/2, shutdown/2, shutdown/3]}).
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

shutdown_count(Kind, Reason, #channel{conninfo = ConnInfo}) ->
    Keys = [clientid, username, sockname, peername, proto_name, proto_ver],
    ShutdownCntMeta = maps:with(Keys, ConnInfo),
    maps:merge(shutdown_count(Kind, Reason), ShutdownCntMeta).

%% process exits with {shutdown, #{shutdown_count := Kind}} will trigger
%% the connection supervisor (esockd) to keep a shutdown-counter grouped by Kind.
%% Kind names a counter, so callers must pick it from a bounded set rather than
%% from client input; see `frame_error_kind/2'. The specific cause stays in the
%% reason map for the shutdown report.
shutdown_count(Kind, Reason) when is_map(Reason) ->
    Reason#{shutdown_count => Kind};
shutdown_count(Kind, Reason) ->
    #{shutdown_count => Kind, reason => Reason}.

%% mqtt v5 connected sessions
disconnect_and_shutdown(
    Reason,
    Reply,
    Channel =
        ?IS_MQTT_V5 =
        #channel{conn_state = ConnState}
) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    NChannel = ensure_disconnected(Reason, Channel),
    shutdown(Reason, Reply, ?DISCONNECT_PACKET(reason_code(Reason)), NChannel);
%% mqtt v3/v4 connected sessions
disconnect_and_shutdown(Reason, Reply, Channel = #channel{conn_state = ConnState}) when
    ?IS_CONNECTED_OR_REAUTHENTICATING(ConnState)
->
    NChannel = ensure_disconnected(Reason, Channel),
    shutdown(Reason, Reply, NChannel);
%% other conn_state sessions
disconnect_and_shutdown(Reason, Reply, Channel) ->
    shutdown(Reason, Reply, Channel).

-compile({inline, [sp/1, flag/1]}).
sp(true) -> 1;
sp(false) -> 0.

flag(true) -> 1;
flag(false) -> 0.

get_mqtt_conf(Zone, Key) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key]).

get_mqtt_conf(Zone, Key, Default) ->
    emqx_config:get_zone_conf(Zone, [mqtt, Key], Default).

%% @doc unset will_msg and cancel the will_message timer
-spec remove_willmsg(Old :: channel()) -> New :: channel().
remove_willmsg(Channel = #channel{timers = Timers}) ->
    case maps:get(will_message, Timers, undefined) of
        undefined ->
            Channel#channel{will_msg = undefined};
        DelayedWillTimer ->
            ok = erlang:cancel_timer(DelayedWillTimer, [{async, true}, {info, false}]),
            Channel#channel{
                will_msg = undefined,
                timers = maps:remove(will_message, Timers)
            }
    end.

is_durable_session(#channel{session = Session}) ->
    case emqx_session:info(impl, Session) of
        emqx_persistent_session_ds ->
            true;
        _ ->
            false
    end.

with_now(NowMs, #channel{clientinfo = ClientInfo0} = Channel0, Fun) ->
    ClientInfo1 = ClientInfo0#{now_time => NowMs},
    #channel{clientinfo = ClientInfo2} = Channel1 = Fun(Channel0#channel{clientinfo = ClientInfo1}),
    Channel1#channel{clientinfo = maps:without([now_time], ClientInfo2)}.

proto_ver(#{proto_ver := ProtoVer}, _ConnInfo) ->
    ProtoVer;
proto_ver(_Reason, #{proto_ver := ProtoVer}) ->
    ProtoVer;
proto_ver(_, _) ->
    ?MQTT_PROTO_V4.

frame_error_reason_code(#{cause := frame_too_large}) -> ?RC_PACKET_TOO_LARGE;
frame_error_reason_code(#{cause := duplicate_property}) -> ?RC_PROTOCOL_ERROR;
frame_error_reason_code(_) -> ?RC_MALFORMED_PACKET.

handle_heal_partition(
    #channel{
        session = Session,
        clientinfo = #{clientid := ClientId}
    }
) ->
    case Session of
        undefined ->
            ok;
        _ ->
            emqx_cm_registry:register_channel(ClientId)
    end.

connect_attrs(Packet, Channel) ->
    emqx_external_trace:connect_attrs(Packet, Channel).

basic_attrs(Channel) ->
    emqx_external_trace:basic_attrs(Channel).

topic_attrs(Packet) ->
    emqx_external_trace:topic_attrs(Packet).

authn_attrs(AuthResult) ->
    emqx_external_trace:authn_attrs(AuthResult).

sub_authz_attrs(AuthzResult) ->
    emqx_external_trace:sub_authz_attrs(AuthzResult).

disconnect_attrs(Reason, Channel) ->
    emqx_external_trace:disconnect_attrs(Reason, Channel).

mk_common_hook_context(#channel{session = Session} = Channel) ->
    #{
        chan_info_fn => fun(Prop) -> emqx_channel:info(Prop, Channel) end,
        session_info_fn => fun(Prop) -> emqx_session:info(Prop, Session) end
    }.

inc_metrics(Name, _Channel) ->
    emqx_metrics:inc_global(Name).

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

-ifdef(TEST).
dummy() -> #channel{}.

set_field(Name, Value, Channel) ->
    Pos = emqx_utils:index_of(Name, record_info(fields, channel)),
    setelement(Pos + 1, Channel, Value).
-endif.
