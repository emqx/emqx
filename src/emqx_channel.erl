%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-logger_header("[Channel]").

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-export([ info/1
        , info/2
        , stats/1
        , caps/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_deliver/2
        , handle_out/3
        , handle_timeout/3
        , handle_call/2
        , handle_info/2
        , terminate/2
        ]).

%% Exports for CT
-export([set_field/3]).

-import(emqx_misc,
        [ run_fold/3
        , pipeline/3
        , maybe_apply/2
        ]).

-export_type([channel/0]).

-record(channel, {
          %% MQTT ConnInfo
          conninfo :: emqx_types:conninfo(),
          %% MQTT ClientInfo
          clientinfo :: emqx_types:clientinfo(),
          %% MQTT Session
          session :: emqx_session:session(),
          %% Keepalive
          keepalive :: emqx_keepalive:keepalive(),
          %% MQTT Will Msg
          will_msg :: maybe(emqx_types:message()),
          %% MQTT Topic Aliases
          topic_aliases :: emqx_types:topic_aliases(),
          %% MQTT Topic Alias Maximum
          alias_maximum :: maybe(map()),
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

-opaque(channel() :: #channel{}).

-type(conn_state() :: idle | connecting | connected | disconnected).

-type(reply() :: {outgoing, emqx_types:packet()}
               | {outgoing, [emqx_types:packet()]}
               | {event, conn_state()|updated}
               | {close, Reason :: atom()}).

-type(replies() :: emqx_types:packet() | reply() | [reply()]).

-define(IS_MQTT_V5, #channel{conninfo = #{proto_ver := ?MQTT_PROTO_V5}}).

-define(TIMER_TABLE, #{
          alive_timer  => keepalive,
          retry_timer  => retry_delivery,
          await_timer  => expire_awaiting_rel,
          expire_timer => expire_session,
          will_timer   => will_message
         }).

-define(INFO_KEYS, [conninfo, conn_state, clientinfo, session, will_msg]).

%%--------------------------------------------------------------------
%% Info, Attrs and Caps
%%--------------------------------------------------------------------

%% @doc Get infos of the channel.
-spec(info(channel()) -> emqx_types:infos()).
info(Channel) ->
    maps:from_list(info(?INFO_KEYS, Channel)).

-spec(info(list(atom())|atom(), channel()) -> term()).
info(Keys, Channel) when is_list(Keys) ->
    [{Key, info(Key, Channel)} || Key <- Keys];
info(conninfo, #channel{conninfo = ConnInfo}) ->
    ConnInfo;
info(zone, #channel{clientinfo = #{zone := Zone}}) ->
    Zone;
info(clientid, #channel{clientinfo = #{clientid := ClientId}}) ->
    ClientId;
info(clientinfo, #channel{clientinfo = ClientInfo}) ->
    ClientInfo;
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
info(timers, #channel{timers = Timers}) -> Timers.

%% TODO: Add more stats.
-spec(stats(channel()) -> emqx_types:stats()).
stats(#channel{session = Session})->
    emqx_session:stats(Session).

-spec(caps(channel()) -> emqx_types:caps()).
caps(#channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).


%%--------------------------------------------------------------------
%% Init the channel
%%--------------------------------------------------------------------

-spec(init(emqx_types:conninfo(), proplists:proplist()) -> channel()).
init(ConnInfo = #{peername := {PeerHost, _Port},
                  sockname := {_Host, SockPort}}, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Username = case peer_cert_as_username(Options) of
                   cn  -> esockd_peercert:common_name(Peercert);
                   dn  -> esockd_peercert:subject(Peercert);
                   crt -> Peercert;
                   _   -> undefined
               end,
    Protocol = maps:get(protocol, ConnInfo, mqtt),
    MountPoint = emqx_zone:mountpoint(Zone),
    ClientInfo = #{zone         => Zone,
                   protocol     => Protocol,
                   peerhost     => PeerHost,
                   sockport     => SockPort,
                   peercert     => Peercert,
                   clientid     => undefined,
                   username     => Username,
                   mountpoint   => MountPoint,
                   is_bridge    => false,
                   is_superuser => false
                  },
    #channel{conninfo   = ConnInfo,
             clientinfo = ClientInfo,
             topic_aliases = #{inbound => #{},
                               outbound => #{}
                              },
             timers     = #{},
             conn_state = idle,
             takeover   = false,
             resuming   = false,
             pendings   = []
            }.

peer_cert_as_username(Options) ->
    proplists:get_value(peer_cert_as_username, Options).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(emqx_types:packet(), channel())
      -> {ok, channel()}
       | {ok, replies(), channel()}
       | {shutdown, Reason :: term(), channel()}
       | {shutdown, Reason :: term(), replies(), channel()}).
handle_in(?CONNECT_PACKET(), Channel = #channel{conn_state = connected}) ->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);

handle_in(?CONNECT_PACKET(ConnPkt), Channel) ->
    case pipeline([fun enrich_conninfo/2,
                   fun run_conn_hooks/2,
                   fun check_connect/2,
                   fun enrich_client/2,
                   fun set_log_meta/2,
                   fun check_banned/2,
                   fun auth_connect/2
                  ], ConnPkt, Channel#channel{conn_state = connecting}) of
        {ok, NConnPkt, NChannel} ->
            process_connect(NConnPkt, NChannel);
        {error, ReasonCode, NChannel} ->
            handle_out(connack, {ReasonCode, ConnPkt}, NChannel)
    end;

handle_in(Packet = ?PUBLISH_PACKET(_QoS), Channel) ->
    case emqx_packet:check(Packet) of
        ok -> process_publish(Packet, Channel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(?PUBACK_PACKET(PacketId, _ReasonCode), Channel
          = #channel{clientinfo = ClientInfo, session = Session}) ->
    case emqx_session:puback(PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(ClientInfo, Msg),
            {ok, Channel#channel{session = NSession}};
        {ok, Msg, Publishes, NSession} ->
            ok = after_message_acked(ClientInfo, Msg),
            handle_out(publish, Publishes, Channel#channel{session = NSession});
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBACK PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBACK PacketId ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.puback.missed'),
            {ok, Channel}
    end;

handle_in(?PUBREC_PACKET(PacketId, _ReasonCode), Channel
          = #channel{clientinfo = ClientInfo, session = Session}) ->
    case emqx_session:pubrec(PacketId, Session) of
        {ok, Msg, NSession} ->
            ok = after_message_acked(ClientInfo, Msg),
            NChannel = Channel#channel{session = NSession},
            handle_out(pubrel, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ?LOG(warning, "The PUBREC PacketId ~w is inuse.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.inuse'),
            handle_out(pubrel, {PacketId, RC}, Channel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBREC ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrec.missed'),
            handle_out(pubrel, {PacketId, RC}, Channel)
    end;

handle_in(?PUBREL_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(pubcomp, {PacketId, ?RC_SUCCESS}, NChannel);
        {error, RC = ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBREL PacketId ~w is not found.", [PacketId]),
            ok = emqx_metrics:inc('packets.pubrel.missed'),
            handle_out(pubcomp, {PacketId, RC}, Channel)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, _ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {ok, Publishes, NSession} ->
            handle_out(publish, Publishes, Channel#channel{session = NSession});
        {error, ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.pubcomp.inuse'),
            {ok, Channel};
        {error, ?RC_PACKET_IDENTIFIER_NOT_FOUND} ->
            ?LOG(warning, "The PUBCOMP PacketId ~w is not found", [PacketId]),
            ok = emqx_metrics:inc('packets.pubcomp.missed'),
            {ok, Channel}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{clientinfo = ClientInfo = #{zone := Zone}}) ->
    case emqx_packet:check(Packet) of
        ok -> TopicFilters1 = parse_topic_filters(TopicFilters),
              TopicFilters2 = enrich_subid(Properties, TopicFilters1),
              TopicFilters3 = run_hooks('client.subscribe',
                                        [ClientInfo, Properties],
                                        TopicFilters2
                                       ),
              {ReasonCodes, NChannel} = process_subscribe(TopicFilters3, Channel),
              case emqx_zone:get_env(Zone, acl_deny_action, ignore) =:= disconnect andalso
                   lists:any(fun(ReasonCode) ->
                                 ReasonCode =:= ?RC_NOT_AUTHORIZED
                             end, ReasonCodes) of
                  true ->
                      handle_out(disconnect, ?RC_NOT_AUTHORIZED, NChannel);
                  false ->
                      handle_out(suback, {PacketId, ReasonCodes}, NChannel)
              end;
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          Channel = #channel{clientinfo = ClientInfo}) ->
    case emqx_packet:check(Packet) of
        ok -> TopicFilters1 = run_hooks('client.unsubscribe',
                                        [ClientInfo, Properties],
                                        parse_topic_filters(TopicFilters)
                                       ),
              {ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Channel),
              handle_out(unsuback, {PacketId, ReasonCodes}, NChannel);
        {error, ReasonCode} ->
            handle_out(disconnect, ReasonCode, Channel)
    end;

handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};

handle_in(?DISCONNECT_PACKET(ReasonCode, Properties), Channel) ->
    NChannel = maybe_clean_will_msg(ReasonCode, Channel),
    process_disconnect(ReasonCode, Properties, NChannel);

handle_in(?AUTH_PACKET(), Channel) ->
    handle_out(disconnect, ?RC_IMPLEMENTATION_SPECIFIC_ERROR, Channel);

handle_in({frame_error, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);

handle_in({frame_error, frame_too_large}, Channel = #channel{conn_state = connecting}) ->
    shutdown(frame_too_large, ?CONNACK_PACKET(?RC_PACKET_TOO_LARGE), Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, ?CONNACK_PACKET(?RC_MALFORMED_PACKET), Channel);

handle_in({frame_error, frame_too_large}, Channel = #channel{conn_state = connected}) ->
    handle_out(disconnect, {?RC_PACKET_TOO_LARGE, frame_too_large}, Channel);
handle_in({frame_error, Reason}, Channel = #channel{conn_state = connected}) ->
    handle_out(disconnect, {?RC_MALFORMED_PACKET, Reason}, Channel);

handle_in({frame_error, Reason}, Channel = #channel{conn_state = disconnected}) ->
    ?LOG(error, "Unexpected frame error: ~p", [Reason]),
    {ok, Channel};

handle_in(Packet, Channel) ->
    ?LOG(error, "Unexpected incoming: ~p", [Packet]),
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel).

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(ConnPkt = #mqtt_packet_connect{clean_start = CleanStart},
                Channel = #channel{conninfo = ConnInfo, clientinfo = ClientInfo}) ->
    case emqx_cm:open_session(CleanStart, ClientInfo, ConnInfo) of
        {ok, #{session := Session, present := false}} ->
            NChannel = Channel#channel{session = Session},
            handle_out(connack, {?RC_SUCCESS, sp(false), ConnPkt}, NChannel);
        {ok, #{session := Session, present := true, pendings := Pendings}} ->
            Pendings1 = lists:usort(lists:append(Pendings, emqx_misc:drain_deliver())),
            NChannel = Channel#channel{session  = Session,
                                       resuming = true,
                                       pendings = Pendings1
                                      },
            handle_out(connack, {?RC_SUCCESS, sp(true), ConnPkt}, NChannel);
        {error, client_id_unavailable} ->
            handle_out(connack, {?RC_CLIENT_IDENTIFIER_NOT_VALID, ConnPkt}, Channel);
        {error, Reason} ->
            ?LOG(error, "Failed to open session due to ~p", [Reason]),
            handle_out(connack, {?RC_UNSPECIFIED_ERROR, ConnPkt}, Channel)
    end.

%%--------------------------------------------------------------------
%% Process Publish
%%--------------------------------------------------------------------

process_publish(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId), 
                Channel = #channel{clientinfo = #{zone := Zone}}) ->
    case pipeline([fun process_alias/2,
                   fun check_pub_alias/2,
                   fun check_pub_acl/2,
                   fun check_pub_caps/2
                  ], Packet, Channel) of
        {ok, NPacket, NChannel} ->
            Msg = packet_to_message(NPacket, NChannel),
            do_publish(PacketId, Msg, NChannel);
        {error, ReasonCode, NChannel} when ReasonCode =:= ?RC_NOT_AUTHORIZED ->
            ?LOG(warning, "Cannot publish message to ~s due to ~s.",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            case emqx_zone:get_env(Zone, acl_deny_action, ignore) of
                ignore ->
                    case QoS of
                       ?QOS_0 -> {ok, NChannel};
                        _ ->
                           handle_out(puback, {PacketId, ReasonCode}, NChannel) 
                    end;
                disconnect ->
                    handle_out(disconnect, ReasonCode, NChannel)
            end;
        {error, ReasonCode, NChannel} ->
            ?LOG(warning, "Cannot publish message to ~s due to ~s.",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            handle_out(disconnect, ReasonCode, NChannel)
    end.

packet_to_message(Packet, #channel{conninfo = #{proto_ver := ProtoVer},
                                   clientinfo = ClientInfo =
                                   #{mountpoint := MountPoint}}) ->
    emqx_mountpoint:mount(
      MountPoint, emqx_packet:to_message(
                    ClientInfo, #{proto_ver => ProtoVer}, Packet)).

do_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    _ = emqx_broker:publish(Msg),
    {ok, Channel};

do_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    Results = emqx_broker:publish(Msg),
    RC = puback_reason_code(Results),
    handle_out(puback, {PacketId, RC}, Channel);

do_publish(PacketId, Msg = #message{qos = ?QOS_2},
           Channel = #channel{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Results, NSession} ->
            RC = puback_reason_code(Results),
            NChannel = Channel#channel{session = NSession},
            NChannel1 = ensure_timer(await_timer, NChannel),
            handle_out(pubrec, {PacketId, RC}, NChannel1);
        {error, RC = ?RC_PACKET_IDENTIFIER_IN_USE} ->
            ok = emqx_metrics:inc('packets.publish.inuse'),
            handle_out(pubrec, {PacketId, RC}, Channel);
        {error, RC = ?RC_RECEIVE_MAXIMUM_EXCEEDED} ->
            ?LOG(warning, "Dropped the qos2 packet ~w "
                 "due to awaiting_rel is full.", [PacketId]),
            ok = emqx_metrics:inc('packets.publish.dropped'),
            handle_out(pubrec, {PacketId, RC}, Channel)
    end.

-compile({inline, [puback_reason_code/1]}).
puback_reason_code([])    -> ?RC_NO_MATCHING_SUBSCRIBERS;
puback_reason_code([_|_]) -> ?RC_SUCCESS.

-compile({inline, [after_message_acked/2]}).
after_message_acked(ClientInfo, Msg) ->
    ok = emqx_metrics:inc('messages.acked'),
    emqx_hooks:run('message.acked', [ClientInfo, Msg]).

%%--------------------------------------------------------------------
%% Process Subscribe
%%--------------------------------------------------------------------

-compile({inline, [process_subscribe/2]}).
process_subscribe(TopicFilters, Channel) ->
    process_subscribe(TopicFilters, [], Channel).

process_subscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_subscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_subscribe(TopicFilter, SubOpts, Channel),
    process_subscribe(More, [RC|Acc], NChannel).

do_subscribe(TopicFilter, SubOpts = #{qos := QoS}, Channel =
             #channel{clientinfo = ClientInfo = #{mountpoint := MountPoint},
                      session = Session}) ->
    case check_subscribe(TopicFilter, SubOpts, Channel) of
        ok -> TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
              SubOpts1 = enrich_subopts(maps:merge(?DEFAULT_SUBOPTS, SubOpts), Channel),
              case emqx_session:subscribe(ClientInfo, TopicFilter1, SubOpts1, Session) of
                  {ok, NSession} ->
                      {QoS, Channel#channel{session = NSession}};
                  {error, RC} -> {RC, Channel}
              end;
        {error, RC} -> {RC, Channel}
    end.

%%--------------------------------------------------------------------
%% Process Unsubscribe
%%--------------------------------------------------------------------

-compile({inline, [process_unsubscribe/2]}).
process_unsubscribe(TopicFilters, Channel) ->
    process_unsubscribe(TopicFilters, [], Channel).

process_unsubscribe([], Acc, Channel) ->
    {lists:reverse(Acc), Channel};

process_unsubscribe([{TopicFilter, SubOpts}|More], Acc, Channel) ->
    {RC, NChannel} = do_unsubscribe(TopicFilter, SubOpts, Channel),
    process_unsubscribe(More, [RC|Acc], NChannel).

do_unsubscribe(TopicFilter, _SubOpts, Channel =
               #channel{clientinfo = ClientInfo = #{mountpoint := MountPoint},
                        session = Session}) ->
    TopicFilter1 = emqx_mountpoint:mount(MountPoint, TopicFilter),
    case emqx_session:unsubscribe(ClientInfo, TopicFilter1, Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, Channel#channel{session = NSession}};
        {error, RC} -> {RC, Channel}
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
process_disconnect(_ReasonCode, #{'Session-Expiry-Interval' := Interval},
                   Channel = #channel{conninfo = #{expiry_interval := 0}})
    when Interval > 0 ->
    handle_out(disconnect, ?RC_PROTOCOL_ERROR, Channel);

process_disconnect(ReasonCode, Properties, Channel) ->
    NChannel = maybe_update_expiry_interval(Properties, Channel),
    {ok, {close, disconnect_reason(ReasonCode)}, NChannel}.

maybe_update_expiry_interval(#{'Session-Expiry-Interval' := Interval},
                             Channel = #channel{conninfo = ConnInfo}) ->
    Channel#channel{conninfo = ConnInfo#{expiry_interval => Interval}};
maybe_update_expiry_interval(_Properties, Channel) -> Channel.

%%--------------------------------------------------------------------
%% Handle Delivers from broker to client
%%--------------------------------------------------------------------

-spec(handle_deliver(list(emqx_types:deliver()), channel())
      -> {ok, channel()} | {ok, replies(), channel()}).
handle_deliver(Delivers, Channel = #channel{conn_state = disconnected,
                                            session    = Session}) ->
    NSession = emqx_session:enqueue(maybe_nack(Delivers), Session),
    {ok, Channel#channel{session = NSession}};

handle_deliver(Delivers, Channel = #channel{takeover = true,
                                            pendings = Pendings}) ->
    NPendings = lists:append(Pendings, maybe_nack(Delivers)),
    {ok, Channel#channel{pendings = NPendings}};

handle_deliver(Delivers, Channel = #channel{session = Session}) ->
    case emqx_session:deliver(Delivers, Session) of
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(publish, Publishes, ensure_timer(retry_timer, NChannel));
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end.

%% Nack delivers from shared subscription
maybe_nack(Delivers) ->
    lists:filter(fun not_nacked/1, Delivers).

not_nacked({deliver, _Topic, Msg}) ->
    not (emqx_shared_sub:is_ack_required(Msg)
         andalso (ok == emqx_shared_sub:nack_no_connection(Msg))).

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

-spec(handle_out(atom(), term(), channel())
      -> {ok, channel()}
       | {ok, replies(), channel()}
       | {shutdown, Reason :: term(), channel()}
       | {shutdown, Reason :: term(), replies(), channel()}).
handle_out(connack, {?RC_SUCCESS, SP, ConnPkt}, Channel = #channel{conninfo = ConnInfo}) ->
    AckProps = run_fold([fun enrich_connack_caps/2,
                         fun enrich_server_keepalive/2,
                         fun enrich_assigned_clientid/2
                        ], #{}, Channel),
    NAckProps = run_hooks('client.connack', [ConnInfo, emqx_reason_codes:name(?RC_SUCCESS)], AckProps),

    return_connack(?CONNACK_PACKET(?RC_SUCCESS, SP, NAckProps),
                   ensure_keepalive(NAckProps,
                                    ensure_connected(ConnPkt, Channel)));

handle_out(connack, {ReasonCode, _ConnPkt}, Channel = #channel{conninfo = ConnInfo}) ->
    Reason = emqx_reason_codes:name(ReasonCode),
    AckProps = run_hooks('client.connack', [ConnInfo, Reason], emqx_mqtt_props:new()),
    AckPacket = ?CONNACK_PACKET(case maps:get(proto_ver, ConnInfo) of
                                    ?MQTT_PROTO_V5 -> ReasonCode;
                                    _ -> emqx_reason_codes:compat(connack, ReasonCode)
                                end, sp(false), AckProps),
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
    return_suback(?SUBACK_PACKET(PacketId, ReasonCodes), Channel);

handle_out(suback, {PacketId, ReasonCodes}, Channel) ->
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    return_suback(?SUBACK_PACKET(PacketId, ReasonCodes1), Channel);

handle_out(unsuback, {PacketId, ReasonCodes}, Channel = ?IS_MQTT_V5) ->
    return_unsuback(?UNSUBACK_PACKET(PacketId, ReasonCodes), Channel);

handle_out(unsuback, {PacketId, _ReasonCodes}, Channel) ->
    return_unsuback(?UNSUBACK_PACKET(PacketId), Channel);

handle_out(disconnect, ReasonCode, Channel) when is_integer(ReasonCode) ->
    ReasonName = disconnect_reason(ReasonCode),
    handle_out(disconnect, {ReasonCode, ReasonName}, Channel);

handle_out(disconnect, {ReasonCode, ReasonName}, Channel = ?IS_MQTT_V5) ->
    Packet = ?DISCONNECT_PACKET(ReasonCode),
    {ok, [{outgoing, Packet}, {close, ReasonName}], Channel};

handle_out(disconnect, {_ReasonCode, ReasonName}, Channel) ->
    {ok, {close, ReasonName}, Channel};

handle_out(Type, Data, Channel) ->
    ?LOG(error, "Unexpected outgoing: ~s, ~p", [Type, Data]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Return ConnAck
%%--------------------------------------------------------------------

return_connack(AckPacket, Channel) ->
    Replies = [{event, connected}, {connack, AckPacket}],
    case maybe_resume_session(Channel) of
        ignore -> {ok, Replies, Channel};
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session  = NSession,
                                       resuming = false,
                                       pendings = []
                                      },
            {Packets, NChannel1} = do_deliver(Publishes, NChannel),
            Outgoing = [{outgoing, Packets} || length(Packets) > 0],
            {ok, Replies ++ Outgoing, NChannel1}
    end.

%%--------------------------------------------------------------------
%% Deliver publish: broker -> client
%%--------------------------------------------------------------------

%% return list(emqx_types:packet())
do_deliver({pubrel, PacketId}, Channel) ->
    {[?PUBREL_PACKET(PacketId, ?RC_SUCCESS)], Channel};

do_deliver({PacketId, Msg}, Channel = #channel{clientinfo = ClientInfo =
                                     #{mountpoint := MountPoint}}) ->
    case ignore_local(Msg, ClientInfo) of
        true ->
            ok = emqx_metrics:inc('delivery.dropped'),
            ok = emqx_metrics:inc('delivery.dropped.no_local'),
            {[], Channel};
        false ->
            ok = emqx_metrics:inc('messages.delivered'),
            Msg1 = emqx_hooks:run_fold('message.delivered',
                                       [ClientInfo],
                                       emqx_message:update_expiry(Msg)
                                      ),
            Msg2 = emqx_mountpoint:unmount(MountPoint, Msg1),
            Packet = emqx_message:to_packet(PacketId, Msg2),
            {NPacket, NChannel} = packing_alias(Packet, Channel),
            {[NPacket], NChannel}
    end;

do_deliver([Publish], Channel) ->
    do_deliver(Publish, Channel);

do_deliver(Publishes, Channel) when is_list(Publishes) ->
    {Packets, NChannel} =
        lists:foldl(fun(Publish, {Acc, Chann}) ->
            {Packets, NChann} = do_deliver(Publish, Chann),
            {Packets ++ Acc, NChann}
        end, {[], Channel}, Publishes),
    {lists:reverse(Packets), NChannel}.

ignore_local(#message{flags = #{nl := true}, from = ClientId},
             #{clientid := ClientId}) ->
    true;
ignore_local(_Msg, _ClientInfo) -> false.

%%--------------------------------------------------------------------
%% Handle out suback
%%--------------------------------------------------------------------

return_suback(Packet, Channel) ->
    {ok, [{outgoing, Packet}, {event, updated}], Channel}.

return_unsuback(Packet, Channel) ->
    {ok, [{outgoing, Packet}, {event, updated}], Channel}.

%%--------------------------------------------------------------------
%% Handle call
%%--------------------------------------------------------------------

-spec(handle_call(Req :: term(), channel())
      -> {reply, Reply :: term(), channel()}
       | {shutdown, Reason :: term(), Reply :: term(), channel()}).
handle_call(kick, Channel) ->
    Channel1 = ensure_disconnected(kicked, Channel),
    disconnect_and_shutdown(kicked, ok, Channel1);

handle_call(discard, Channel) ->
    disconnect_and_shutdown(discarded, ok, Channel);

%% Session Takeover
handle_call({takeover, 'begin'}, Channel = #channel{session = Session}) ->
    reply(Session, Channel#channel{takeover = true});

handle_call({takeover, 'end'}, Channel = #channel{session  = Session,
                                                  pendings = Pendings}) ->
    ok = emqx_session:takeover(Session),
    %% TODO: Should not drain deliver here (side effect)
    Delivers = emqx_misc:drain_deliver(),
    AllPendings = lists:append(Delivers, Pendings),
    disconnect_and_shutdown(takeovered, AllPendings, Channel);

handle_call(list_acl_cache, Channel) ->
    {reply, emqx_acl_cache:list_acl_cache(), Channel};

handle_call(Req, Channel) ->
    ?LOG(error, "Unexpected call: ~p", [Req]),
    reply(ignored, Channel).

%%--------------------------------------------------------------------
%% Handle Info
%%--------------------------------------------------------------------

-spec(handle_info(Info :: term(), channel())
      -> ok | {ok, channel()} | {shutdown, Reason :: term(), channel()}).
handle_info({subscribe, TopicFilters}, Channel = #channel{clientinfo = ClientInfo}) ->
    TopicFilters1 = run_hooks('client.subscribe',
                              [ClientInfo, #{'Internal' => true}],
                              parse_topic_filters(TopicFilters)
                             ),
    {_ReasonCodes, NChannel} = process_subscribe(TopicFilters1, Channel),
    {ok, NChannel};

handle_info({unsubscribe, TopicFilters}, Channel = #channel{clientinfo = ClientInfo}) ->
    TopicFilters1 = run_hooks('client.unsubscribe',
                              [ClientInfo, #{'Internal' => true}],
                              parse_topic_filters(TopicFilters)
                             ),
    {_ReasonCodes, NChannel} = process_unsubscribe(TopicFilters1, Channel),
    {ok, NChannel};

handle_info({sock_closed, Reason}, Channel = #channel{conn_state = idle}) ->
    shutdown(Reason, Channel);

handle_info({sock_closed, Reason}, Channel = #channel{conn_state = connecting}) ->
    shutdown(Reason, Channel);

handle_info({sock_closed, Reason}, Channel =
            #channel{conn_state = connected,
                     clientinfo = ClientInfo = #{zone := Zone}}) ->
    emqx_zone:enable_flapping_detect(Zone)
        andalso emqx_flapping:detect(ClientInfo),
    Channel1 = ensure_disconnected(Reason, mabye_publish_will_msg(Channel)),
    case maybe_shutdown(Reason, Channel1) of
        {ok, Channel2} -> {ok, {event, disconnected}, Channel2};
        Shutdown -> Shutdown
    end;

handle_info({sock_closed, Reason}, Channel = #channel{conn_state = disconnected}) ->
    ?LOG(error, "Unexpected sock_closed: ~p", [Reason]),
    {ok, Channel};

handle_info(clean_acl_cache, Channel) ->
    ok = emqx_acl_cache:empty_acl_cache(),
    {ok, Channel};

handle_info(Info, Channel) ->
    ?LOG(error, "Unexpected info: ~p", [Info]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

-spec(handle_timeout(reference(), Msg :: term(), channel())
      -> {ok, channel()}
       | {ok, replies(), channel()}
       | {shutdown, Reason :: term(), channel()}).
handle_timeout(_TRef, {keepalive, _StatVal},
               Channel = #channel{keepalive = undefined}) ->
    {ok, Channel};
handle_timeout(_TRef, {keepalive, _StatVal},
               Channel = #channel{conn_state = disconnected}) ->
    {ok, Channel};
handle_timeout(_TRef, {keepalive, StatVal},
               Channel = #channel{keepalive = Keepalive}) ->
    case emqx_keepalive:check(StatVal, Keepalive) of
        {ok, NKeepalive} ->
            NChannel = Channel#channel{keepalive = NKeepalive},
            {ok, reset_timer(alive_timer, NChannel)};
        {error, timeout} ->
            handle_out(disconnect, ?RC_KEEP_ALIVE_TIMEOUT, Channel)
    end;

handle_timeout(_TRef, retry_delivery,
               Channel = #channel{conn_state = disconnected}) ->
    {ok, Channel};
handle_timeout(_TRef, retry_delivery,
               Channel = #channel{session = Session}) ->
    case emqx_session:retry(Session) of
        {ok, NSession} ->
            {ok, clean_timer(retry_timer, Channel#channel{session = NSession})};
        {ok, Publishes, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(publish, Publishes, reset_timer(retry_timer, NChannel));
        {ok, Publishes, Timeout, NSession} ->
            NChannel = Channel#channel{session = NSession},
            handle_out(publish, Publishes, reset_timer(retry_timer, Timeout, NChannel))
    end;

handle_timeout(_TRef, expire_awaiting_rel,
               Channel = #channel{conn_state = disconnected}) ->
    {ok, Channel};
handle_timeout(_TRef, expire_awaiting_rel,
               Channel = #channel{session = Session}) ->
    case emqx_session:expire(awaiting_rel, Session) of
        {ok, Session} ->
            {ok, clean_timer(await_timer, Channel#channel{session = Session})};
        {ok, Timeout, Session} ->
            {ok, reset_timer(await_timer, Timeout, Channel#channel{session = Session})}
    end;

handle_timeout(_TRef, expire_session, Channel) ->
    shutdown(expired, Channel);

handle_timeout(_TRef, will_message, Channel = #channel{will_msg = WillMsg}) ->
    (WillMsg =/= undefined) andalso publish_will_msg(WillMsg),
    {ok, clean_timer(will_timer, Channel#channel{will_msg = undefined})};

handle_timeout(_TRef, Msg, Channel) ->
    ?LOG(error, "Unexpected timeout: ~p~n", [Msg]),
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
        true  -> ensure_timer(Name, Time, Channel);
        false -> Channel %% Timer disabled or exists
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
    timer:seconds(emqx_session:info(retry_interval, Session));
interval(await_timer, #channel{session = Session}) ->
    timer:seconds(emqx_session:info(await_rel_timeout, Session));
interval(expire_timer, #channel{conninfo = ConnInfo}) ->
    timer:seconds(maps:get(expiry_interval, ConnInfo));
interval(will_timer, #channel{will_msg = WillMsg}) ->
    timer:seconds(will_delay_interval(WillMsg)).

%%--------------------------------------------------------------------
%% Terminate
%%--------------------------------------------------------------------

terminate(_, #channel{conn_state = idle}) -> ok;
terminate(normal, Channel) ->
    run_terminate_hook(normal, Channel);
terminate({shutdown, Reason}, Channel)
  when Reason =:= kicked; Reason =:= discarded; Reason =:= takeovered ->
    run_terminate_hook(Reason, Channel);
terminate(Reason, Channel = #channel{will_msg = WillMsg}) ->
    (WillMsg =/= undefined) andalso publish_will_msg(WillMsg),
    run_terminate_hook(Reason, Channel).

run_terminate_hook(_Reason, #channel{session = undefined}) -> ok;
run_terminate_hook(Reason, #channel{clientinfo = ClientInfo, session = Session}) ->
    emqx_session:terminate(ClientInfo, Reason, Session).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%%--------------------------------------------------------------------
%% Enrich MQTT Connect Info

enrich_conninfo(ConnPkt = #mqtt_packet_connect{
                             proto_name  = ProtoName,
                             proto_ver   = ProtoVer,
                             clean_start = CleanStart,
                             keepalive   = Keepalive,
                             properties  = ConnProps,
                             clientid    = ClientId,
                             username    = Username
                            },
                Channel = #channel{conninfo   = ConnInfo,
                                   clientinfo = ClientInfo
                                  }) ->
    ExpiryInterval = expiry_interval(ClientInfo, ConnPkt),
    ReceiveMaximum = receive_maximum(ClientInfo, ConnProps),
    NConnInfo = ConnInfo#{proto_name  => ProtoName,
                          proto_ver   => ProtoVer,
                          clean_start => CleanStart,
                          keepalive   => Keepalive,
                          clientid    => ClientId,
                          username    => Username,
                          conn_props  => ConnProps,
                          expiry_interval => ExpiryInterval,
                          receive_maximum => ReceiveMaximum
                         },
    {ok, Channel#channel{conninfo = NConnInfo}}.

%% If the Session Expiry Interval is absent the value 0 is used.
-compile({inline, [expiry_interval/2]}).
expiry_interval(_ClientInfo, #mqtt_packet_connect{proto_ver  = ?MQTT_PROTO_V5,
                                                  properties = ConnProps}) ->
    emqx_mqtt_props:get('Session-Expiry-Interval', ConnProps, 0);
expiry_interval(#{zone := Zone}, #mqtt_packet_connect{clean_start = false}) ->
    emqx_zone:session_expiry_interval(Zone);
expiry_interval(_ClientInfo, #mqtt_packet_connect{clean_start = true}) ->
    0.

-compile({inline, [receive_maximum/2]}).
receive_maximum(#{zone := Zone}, ConnProps) ->
    emqx_mqtt_props:get('Receive-Maximum', ConnProps, emqx_zone:max_inflight(Zone)).

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
    {ok, NConnPkt, NClientInfo} = pipeline([fun set_username/2,
                                            fun set_bridge_mode/2,
                                            fun maybe_username_as_clientid/2,
                                            fun maybe_assign_clientid/2,
                                            fun fix_mountpoint/2
                                           ], ConnPkt, ClientInfo),
    {ok, NConnPkt, Channel#channel{clientinfo = NClientInfo}}.

set_username(#mqtt_packet_connect{username = Username},
             ClientInfo = #{username := undefined}) ->
    {ok, ClientInfo#{username => Username}};
set_username(_ConnPkt, ClientInfo) -> {ok, ClientInfo}.

set_bridge_mode(#mqtt_packet_connect{is_bridge = true}, ClientInfo) ->
    {ok, ClientInfo#{is_bridge => true}};
set_bridge_mode(_ConnPkt, _ClientInfo) -> ok.

maybe_username_as_clientid(_ConnPkt, ClientInfo = #{username := undefined}) ->
    {ok, ClientInfo};
maybe_username_as_clientid(_ConnPkt, ClientInfo = #{zone := Zone, username := Username}) ->
    case emqx_zone:use_username_as_clientid(Zone) of
        true  -> {ok, ClientInfo#{clientid => Username}};
        false -> ok
    end.

maybe_assign_clientid(_ConnPkt, ClientInfo = #{clientid := ClientId})
  when ClientId /= undefined ->
    {ok, ClientInfo};
maybe_assign_clientid(#mqtt_packet_connect{clientid = <<>>}, ClientInfo) ->
    %% Generate a rand clientId
    {ok, ClientInfo#{clientid => emqx_guid:to_base62(emqx_guid:gen())}};
maybe_assign_clientid(#mqtt_packet_connect{clientid = ClientId}, ClientInfo) ->
    {ok, ClientInfo#{clientid => ClientId}}.

fix_mountpoint(_ConnPkt, #{mountpoint := undefined}) -> ok;
fix_mountpoint(_ConnPkt, ClientInfo = #{mountpoint := MountPoint}) ->
    MountPoint1 = emqx_mountpoint:replvar(MountPoint, ClientInfo),
    {ok, ClientInfo#{mountpoint := MountPoint1}}.

%%--------------------------------------------------------------------
%% Set log metadata

set_log_meta(_ConnPkt, #channel{clientinfo = #{clientid := ClientId}}) ->
    emqx_logger:set_metadata_clientid(ClientId).

%%--------------------------------------------------------------------
%% Check banned

check_banned(_ConnPkt, #channel{clientinfo = ClientInfo = #{zone := Zone}}) ->
    case emqx_zone:enable_ban(Zone) andalso emqx_banned:check(ClientInfo) of
        true  -> {error, ?RC_BANNED};
        false -> ok
    end.

%%--------------------------------------------------------------------
%% Auth Connect

auth_connect(#mqtt_packet_connect{clientid  = ClientId,
                                  username  = Username,
                                  password  = Password
                                 },
             #channel{clientinfo = ClientInfo} = Channel) ->
    case emqx_access_control:authenticate(ClientInfo#{password => Password}) of
        {ok, AuthResult} ->
            is_anonymous(AuthResult) andalso
                emqx_metrics:inc('client.auth.anonymous'),
            NClientInfo = maps:merge(ClientInfo, AuthResult),
            {ok, Channel#channel{clientinfo = NClientInfo}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

is_anonymous(#{anonymous := true}) -> true;
is_anonymous(_AuthResult)          -> false.

%%--------------------------------------------------------------------
%% Process Topic Alias

process_alias(Packet = #mqtt_packet{
                          variable = #mqtt_packet_publish{topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}
                                                         } = Publish
                         },
              Channel = ?IS_MQTT_V5 = #channel{topic_aliases = TopicAliases}) ->
    case find_alias(inbound, AliasId, TopicAliases) of
        {ok, Topic} ->
            NPublish = Publish#mqtt_packet_publish{topic_name = Topic},
            {ok, Packet#mqtt_packet{variable = NPublish}, Channel};
        false -> {error, ?RC_PROTOCOL_ERROR}
    end;

process_alias(#mqtt_packet{
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 properties = #{'Topic-Alias' := AliasId}
                                                }
                },
              Channel = ?IS_MQTT_V5 = #channel{topic_aliases = TopicAliases}) ->
    NTopicAliases = save_alias(inbound, AliasId, Topic, TopicAliases),
    {ok, Channel#channel{topic_aliases = NTopicAliases}};

process_alias(_Packet, Channel) -> {ok, Channel}.

%%--------------------------------------------------------------------
%% Packing Topic Alias

packing_alias(Packet = #mqtt_packet{
                            variable = #mqtt_packet_publish{topic_name = Topic} = Publish
                        },
              Channel = ?IS_MQTT_V5 = #channel{topic_aliases = TopicAliases, alias_maximum = Limits}) ->
    case find_alias(outbound, Topic, TopicAliases) of
        {ok, AliasId} -> 
            NPublish = Publish#mqtt_packet_publish{
                            topic_name = <<>>,
                            properties = #{'Topic-Alias' => AliasId}
                            },
            {Packet#mqtt_packet{variable = NPublish}, Channel};
        error ->
            #{outbound := Aliases} = TopicAliases,
            AliasId = maps:size(Aliases) + 1,
            case (Limits =:= undefined) orelse
                    (AliasId =< maps:get(outbound, Limits, 0)) of
                true ->
                    NTopicAliases = save_alias(outbound, AliasId, Topic, TopicAliases),
                    NChannel = Channel#channel{topic_aliases = NTopicAliases},
                    NPublish = Publish#mqtt_packet_publish{
                                    topic_name = Topic,
                                    properties = #{'Topic-Alias' => AliasId}
                                    },
                    {Packet#mqtt_packet{variable = NPublish}, NChannel};
                false -> {Packet, Channel}
            end
    end;
packing_alias(Packet, Channel) -> {Packet, Channel}.

%%--------------------------------------------------------------------
%% Check Pub Alias

check_pub_alias(#mqtt_packet{
                   variable = #mqtt_packet_publish{
                                 properties = #{'Topic-Alias' := AliasId}
                                }
                  },
                #channel{alias_maximum = Limits}) ->
    case (Limits =:= undefined) orelse
         (AliasId =< maps:get(inbound, Limits, ?MAX_TOPIC_AlIAS)) of
        true  -> ok;
        false -> {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
check_pub_alias(_Packet, _Channel) -> ok.

%%--------------------------------------------------------------------
%% Check Pub ACL

check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #channel{clientinfo = ClientInfo}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, publish, Topic) of
        false -> ok;
        allow -> ok;
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%%--------------------------------------------------------------------
%% Check Pub Caps

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos    = QoS,
                                                         retain = Retain},
                            variable = #mqtt_packet_publish{topic_name = Topic}
                           },
               #channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain, topic => Topic}).

%%--------------------------------------------------------------------
%% Check Subscribe

check_subscribe(TopicFilter, SubOpts, Channel) ->
    case check_sub_acl(TopicFilter, Channel) of
        allow -> check_sub_caps(TopicFilter, SubOpts, Channel);
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%%--------------------------------------------------------------------
%% Check Sub ACL

check_sub_acl(TopicFilter, #channel{clientinfo = ClientInfo}) ->
    case is_acl_enabled(ClientInfo) andalso
         emqx_access_control:check_acl(ClientInfo, subscribe, TopicFilter) of
        false  -> allow;
        Result -> Result
    end.

%%--------------------------------------------------------------------
%% Check Sub Caps

check_sub_caps(TopicFilter, SubOpts, #channel{clientinfo = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_sub(Zone, TopicFilter, SubOpts).

%%--------------------------------------------------------------------
%% Enrich SubId

enrich_subid(#{'Subscription-Identifier' := SubId}, TopicFilters) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
enrich_subid(_Properties, TopicFilters) -> TopicFilters.

%%--------------------------------------------------------------------
%% Enrich SubOpts

enrich_subopts(SubOpts, _Channel = ?IS_MQTT_V5) ->
    SubOpts;
enrich_subopts(SubOpts, #channel{clientinfo = #{zone := Zone, is_bridge := IsBridge}}) ->
    NL = flag(emqx_zone:ignore_loop_deliver(Zone)),
    SubOpts#{rap => flag(IsBridge), nl => NL}.

%%--------------------------------------------------------------------
%% Enrich ConnAck Caps

enrich_connack_caps(AckProps, ?IS_MQTT_V5 = #channel{clientinfo = #{zone := Zone}}) ->
    #{max_packet_size       := MaxPktSize,
      max_qos_allowed       := MaxQoS,
      retain_available      := Retain,
      max_topic_alias       := MaxAlias,
      shared_subscription   := Shared,
      wildcard_subscription := Wildcard
     } = emqx_mqtt_caps:get_caps(Zone),
    NAckProps = AckProps#{'Retain-Available'    => flag(Retain),
                          'Maximum-Packet-Size' => MaxPktSize,
                          'Topic-Alias-Maximum' => MaxAlias,
                          'Wildcard-Subscription-Available'   => flag(Wildcard),
                          'Subscription-Identifier-Available' => 1,
                          'Shared-Subscription-Available'     => flag(Shared)
                         },
    %% MQTT 5.0 - 3.2.2.3.4:
    %% It is a Protocol Error to include Maximum QoS more than once,
    %% or to have a value other than 0 or 1. If the Maximum QoS is absent,
    %% the Client uses a Maximum QoS of 2.
    case MaxQoS =:= 2 of
        true -> NAckProps;
        _ -> NAckProps#{'Maximum-QoS' => MaxQoS}
    end;

enrich_connack_caps(AckProps, _Channel) -> AckProps.

%%--------------------------------------------------------------------
%% Enrich server keepalive

enrich_server_keepalive(AckProps, #channel{clientinfo = #{zone := Zone}}) ->
    case emqx_zone:server_keepalive(Zone) of
        undefined -> AckProps;
        Keepalive -> AckProps#{'Server-Keep-Alive' => Keepalive}
    end.

%%--------------------------------------------------------------------
%% Enrich Assigned ClientId

enrich_assigned_clientid(AckProps, #channel{conninfo   = ConnInfo,
                                            clientinfo = #{clientid := ClientId}}) ->
    case maps:get(clientid, ConnInfo) of
        <<>> -> %% Original ClientId is null.
            AckProps#{'Assigned-Client-Identifier' => ClientId};
        _Origin -> AckProps
    end.

%%--------------------------------------------------------------------
%% Ensure connected

ensure_connected(ConnPkt, Channel = #channel{conninfo = ConnInfo,
                                             clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{connected_at => erlang:system_time(second)},
    ok = run_hooks('client.connected', [ClientInfo, NConnInfo]),
    Channel#channel{conninfo   = NConnInfo,
                    conn_state = connected,
                    will_msg   = emqx_packet:will_msg(ConnPkt),
                    alias_maximum = init_alias_maximum(ConnPkt, ClientInfo)
                   }.

%%--------------------------------------------------------------------
%% Init Alias Maximum

init_alias_maximum(#mqtt_packet_connect{proto_ver  = ?MQTT_PROTO_V5,
                                        properties = Properties},
                   #{zone := Zone} = _ClientInfo) ->
    #{outbound => emqx_mqtt_props:get('Topic-Alias-Maximum', Properties, 0),
      inbound  => emqx_mqtt_caps:get_caps(Zone, max_topic_alias, ?MAX_TOPIC_AlIAS)
     };
init_alias_maximum(_ConnPkt, _ClientInfo) -> undefined.

%%--------------------------------------------------------------------
%% Enrich Keepalive

ensure_keepalive(#{'Server-Keep-Alive' := Interval}, Channel) ->
    ensure_keepalive_timer(Interval, Channel);
ensure_keepalive(_AckProps, Channel = #channel{conninfo = ConnInfo}) ->
    ensure_keepalive_timer(maps:get(keepalive, ConnInfo), Channel).

ensure_keepalive_timer(0, Channel) -> Channel;
ensure_keepalive_timer(Interval, Channel = #channel{clientinfo = #{zone := Zone}}) ->
    Backoff = emqx_zone:keepalive_backoff(Zone),
    Keepalive = emqx_keepalive:init(round(timer:seconds(Interval) * Backoff)),
    ensure_timer(alive_timer, Channel#channel{keepalive = Keepalive}).

%%--------------------------------------------------------------------
%% Maybe Resume Session

maybe_resume_session(#channel{resuming = false}) ->
    ignore;
maybe_resume_session(#channel{session  = Session,
                              resuming = true,
                              pendings = Pendings}) ->
    {ok, Publishes, Session1} = emqx_session:replay(Session),
    case emqx_session:deliver(Pendings, Session1) of
        {ok, Session2} ->
            {ok, Publishes, Session2};
        {ok, More, Session2} ->
            {ok, lists:append(Publishes, More), Session2}
    end.

%%--------------------------------------------------------------------
%% Maybe Shutdown the Channel

maybe_shutdown(Reason, Channel = #channel{conninfo = ConnInfo}) ->
    case maps:get(expiry_interval, ConnInfo) of
        ?UINT_MAX -> {ok, Channel};
        I when I > 0 ->
            {ok, ensure_timer(expire_timer, timer:seconds(I), Channel)};
        _ -> shutdown(Reason, Channel)
    end.

%%--------------------------------------------------------------------
%% Is ACL enabled?

-compile({inline, [is_acl_enabled/1]}).
is_acl_enabled(#{zone := Zone, is_superuser := IsSuperuser}) ->
    (not IsSuperuser) andalso emqx_zone:enable_acl(Zone).

%%--------------------------------------------------------------------
%% Parse Topic Filters

-compile({inline, [parse_topic_filters/1]}).
parse_topic_filters(TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Ensure disconnected

ensure_disconnected(Reason, Channel = #channel{conninfo = ConnInfo,
                                               clientinfo = ClientInfo}) ->
    NConnInfo = ConnInfo#{disconnected_at => erlang:system_time(second)},
    ok = run_hooks('client.disconnected', [ClientInfo, Reason, NConnInfo]),
    Channel#channel{conninfo = NConnInfo, conn_state = disconnected}.

%%--------------------------------------------------------------------
%% Maybe Publish will msg

mabye_publish_will_msg(Channel = #channel{will_msg = undefined}) ->
    Channel;
mabye_publish_will_msg(Channel = #channel{will_msg = WillMsg}) ->
    case will_delay_interval(WillMsg) of
        0 -> publish_will_msg(WillMsg),
             Channel#channel{will_msg = undefined};
        I -> ensure_timer(will_timer, timer:seconds(I), Channel)
    end.

will_delay_interval(WillMsg) ->
    emqx_message:get_header('Will-Delay-Interval', WillMsg, 0).

publish_will_msg(Msg) -> emqx_broker:publish(Msg).

%%--------------------------------------------------------------------
%% Disconnect Reason

disconnect_reason(?RC_SUCCESS) -> normal;
disconnect_reason(ReasonCode)  -> emqx_reason_codes:name(ReasonCode).

reason_code(takeovered) -> ?RC_SESSION_TAKEN_OVER;
reason_code(discarded) -> ?RC_SESSION_TAKEN_OVER;
reason_code(_) -> ?RC_NORMAL_DISCONNECTION.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

-compile({inline, [run_hooks/2, run_hooks/3]}).
run_hooks(Name, Args) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run(Name, Args).

run_hooks(Name, Args, Acc) ->
    ok = emqx_metrics:inc(Name), emqx_hooks:run_fold(Name, Args, Acc).

-compile({inline, [find_alias/3, save_alias/4]}).

find_alias(_, _ ,undefined) -> false;
find_alias(inbound, AliasId, _TopicAliases = #{inbound := Aliases}) ->
    maps:find(AliasId, Aliases);
find_alias(outbound, Topic, _TopicAliases = #{outbound := Aliases}) ->
    maps:find(Topic, Aliases).

save_alias(_, _, _, undefined) -> false;
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

disconnect_and_shutdown(Reason, Reply, Channel = ?IS_MQTT_V5
                                               = #channel{conn_state = connected}) ->
    shutdown(Reason, Reply, ?DISCONNECT_PACKET(reason_code(Reason)), Channel);

disconnect_and_shutdown(Reason, Reply, Channel) ->
    shutdown(Reason, Reply, Channel).

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

%%--------------------------------------------------------------------
%% For CT tests
%%--------------------------------------------------------------------

set_field(Name, Value, Channel) ->
    Pos = emqx_misc:index_of(Name, record_info(fields, channel)),
    setelement(Pos+1, Channel, Value).

