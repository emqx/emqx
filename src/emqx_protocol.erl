%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([init/2]).
-export([info/1]).
-export([attrs/1]).
-export([caps/1]).
-export([stats/1]).
-export([client_id/1]).
-export([credentials/1]).
-export([parser/1]).
-export([session/1]).
-export([received/2]).
-export([process_packet/2]).
-export([deliver/2]).
-export([send/2]).
-export([shutdown/2]).

-record(pstate, {
         zone,
         sendfun,
         peername,
         peercert,
         proto_ver,
         proto_name,
         ackprops,
         client_id,
         is_assigned,
         conn_pid,
         conn_props,
         ack_props,
         username,
         session,
         clean_start,
         topic_aliases,
         packet_size,
         will_topic,
         will_msg,
         keepalive,
         mountpoint,
         is_super,
         is_bridge,
         enable_ban,
         enable_acl,
         recv_stats,
         send_stats,
         connected,
         connected_at
        }).

-type(state() :: #pstate{}).
-export_type([state/0]).

-ifdef(TEST).
-compile(export_all).
-endif.

-define(LOG(Level, Format, Args, PState),
        emqx_logger:Level([{client, PState#pstate.client_id}], "MQTT(~s@~s): " ++ Format,
                          [PState#pstate.client_id, esockd_net:format(PState#pstate.peername) | Args])).

%%------------------------------------------------------------------------------
%% Init
%%------------------------------------------------------------------------------

-spec(init(map(), list()) -> state()).
init(#{peername := Peername, peercert := Peercert, sendfun := SendFun}, Options) ->
    Zone = proplists:get_value(zone, Options),
    #pstate{zone         = Zone,
            sendfun      = SendFun,
            peername     = Peername,
            peercert     = Peercert,
            proto_ver    = ?MQTT_PROTO_V4,
            proto_name   = <<"MQTT">>,
            client_id    = <<>>,
            is_assigned  = false,
            conn_pid     = self(),
            username     = init_username(Peercert, Options),
            is_super     = false,
            clean_start  = false,
            topic_aliases = #{},
            packet_size  = emqx_zone:get_env(Zone, max_packet_size),
            mountpoint   = emqx_zone:get_env(Zone, mountpoint),
            is_bridge    = false,
            enable_ban   = emqx_zone:get_env(Zone, enable_ban, false),
            enable_acl   = emqx_zone:get_env(Zone, enable_acl),
            recv_stats   = #{msg => 0, pkt => 0},
            send_stats   = #{msg => 0, pkt => 0},
            connected    = false}.

init_username(Peercert, Options) ->
    case proplists:get_value(peer_cert_as_username, Options) of
        cn -> esockd_peercert:common_name(Peercert);
        dn -> esockd_peercert:subject(Peercert);
        _  -> undefined
    end.

set_username(Username, PState = #pstate{username = undefined}) ->
    PState#pstate{username = Username};
set_username(_Username, PState) ->
    PState.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

info(PState = #pstate{conn_props    = ConnProps,
                      ack_props     = AckProps,
                      session       = Session,
                      topic_aliases = Aliases,
                      will_msg      = WillMsg,
                      enable_acl    = EnableAcl}) ->
    attrs(PState) ++ [{conn_props, ConnProps},
                      {ack_props, AckProps},
                      {session, Session},
                      {topic_aliases, Aliases},
                      {will_msg, WillMsg},
                      {enable_acl, EnableAcl}].

attrs(#pstate{zone         = Zone,
              client_id    = ClientId,
              username     = Username,
              peername     = Peername,
              peercert     = Peercert,
              clean_start  = CleanStart,
              proto_ver    = ProtoVer,
              proto_name   = ProtoName,
              keepalive    = Keepalive,
              will_topic   = WillTopic,
              mountpoint   = Mountpoint,
              is_super     = IsSuper,
              is_bridge    = IsBridge,
              connected_at = ConnectedAt}) ->
    [{zone, Zone},
     {client_id, ClientId},
     {username, Username},
     {peername, Peername},
     {peercert, Peercert},
     {proto_ver, ProtoVer},
     {proto_name, ProtoName},
     {clean_start, CleanStart},
     {keepalive, Keepalive},
     {will_topic, WillTopic},
     {mountpoint, Mountpoint},
     {is_super, IsSuper},
     {is_bridge, IsBridge},
     {connected_at, ConnectedAt}].

caps(#pstate{zone = Zone}) ->
    emqx_mqtt_caps:get_caps(Zone).

client_id(#pstate{client_id = ClientId}) ->
    ClientId.

credentials(#pstate{zone       = Zone,
                    client_id  = ClientId,
                    username   = Username,
                    peername   = Peername}) ->
    #{zone      => Zone,
      client_id => ClientId,
      username  => Username,
      peername  => Peername}.

stats(#pstate{recv_stats = #{pkt := RecvPkt, msg := RecvMsg},
              send_stats = #{pkt := SendPkt, msg := SendMsg}}) ->
    [{recv_pkt, RecvPkt},
     {recv_msg, RecvMsg},
     {send_pkt, SendPkt},
     {send_msg, SendMsg}].

session(#pstate{session = SPid}) ->
    SPid.

parser(#pstate{packet_size = Size, proto_ver = Ver}) ->
    emqx_frame:initial_state(#{max_packet_size => Size, version => Ver}).

%%------------------------------------------------------------------------------
%% Packet Received
%%------------------------------------------------------------------------------

-spec(received(emqx_mqtt_types:packet(), state()) ->
    {ok, state()} | {error, term()} | {error, term(), state()} | {stop, term(), state()}).
received(?PACKET(Type), PState = #pstate{connected = false}) when Type =/= ?CONNECT ->
    {error, proto_not_connected, PState};

received(?PACKET(?CONNECT), PState = #pstate{connected = true}) ->
    {error, proto_unexpected_connect, PState};

received(Packet = ?PACKET(Type), PState) ->
    trace(recv, Packet, PState),
    case catch emqx_packet:validate(Packet) of
        true ->
            {Packet1, PState1} = preprocess_properties(Packet, PState),
            process_packet(Packet1, inc_stats(recv, Type, PState1));
        {'EXIT', {Reason, _Stacktrace}} ->
            deliver({disconnect, rc(Reason)}, PState),
            {error, Reason, PState}
    end.

%%------------------------------------------------------------------------------
%% Preprocess MQTT Properties
%%------------------------------------------------------------------------------

%% Subscription Identifier
preprocess_properties(Packet = #mqtt_packet{
                                  variable = Subscribe = #mqtt_packet_subscribe{
                                                            properties    = #{'Subscription-Identifier' := SubId},
                                                            topic_filters = TopicFilters
                                                           }
                                 },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5}) ->
    TopicFilters1 = [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters],
    {Packet#mqtt_packet{variable = Subscribe#mqtt_packet_subscribe{topic_filters = TopicFilters1}}, PState};

%% Topic Alias Mapping
preprocess_properties(Packet = #mqtt_packet{
                                  variable = Publish = #mqtt_packet_publish{
                                                          topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}}
                                 },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5, topic_aliases = Aliases}) ->
    {Packet#mqtt_packet{variable = Publish#mqtt_packet_publish{
                                     topic_name = maps:get(AliasId, Aliases, <<>>)}}, PState};

preprocess_properties(Packet = #mqtt_packet{
                                  variable = #mqtt_packet_publish{
                                                topic_name = Topic,
                                                properties = #{'Topic-Alias' := AliasId}}
                                 },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5, topic_aliases = Aliases}) ->
    {Packet, PState#pstate{topic_aliases = maps:put(AliasId, Topic, Aliases)}};

preprocess_properties(Packet, PState) ->
    {Packet, PState}.

%%------------------------------------------------------------------------------
%% Process MQTT Packet
%%------------------------------------------------------------------------------

process_packet(?CONNECT_PACKET(
                  #mqtt_packet_connect{proto_name  = ProtoName,
                                       proto_ver   = ProtoVer,
                                       is_bridge   = IsBridge,
                                       clean_start = CleanStart,
                                       keepalive   = Keepalive,
                                       properties  = ConnProps,
                                       will_topic  = WillTopic,
                                       client_id   = ClientId,
                                       username    = Username,
                                       password    = Password} = Connect), PState) ->

    %% TODO: Mountpoint...
    %% Msg -> emqx_mountpoint:mount(MountPoint, Msg)
    WillMsg = emqx_packet:will_msg(Connect),

    PState1 = set_username(Username,
                           PState#pstate{client_id    = ClientId,
                                         proto_ver    = ProtoVer,
                                         proto_name   = ProtoName,
                                         clean_start  = CleanStart,
                                         keepalive    = Keepalive,
                                         conn_props   = ConnProps,
                                         will_topic   = WillTopic,
                                         will_msg     = WillMsg,
                                         is_bridge    = IsBridge,
                                         connected_at = os:timestamp()}),
    connack(
      case check_connect(Connect, PState1) of
          {ok, PState2} ->
              case authenticate(credentials(PState2), Password) of
                  {ok, IsSuper} ->
                      %% Maybe assign a clientId
                      PState3 = maybe_assign_client_id(PState2#pstate{is_super = IsSuper}),
                      %% Open session
                      case try_open_session(PState3) of
                          {ok, SPid, SP} ->
                              PState4 = PState3#pstate{session = SPid, connected = true},
                              ok = emqx_cm:register_connection(client_id(PState4), attrs(PState4)),
                              %% Start keepalive
                              start_keepalive(Keepalive, PState4),
                              %% Success
                              {?RC_SUCCESS, SP, PState4};
                          {error, Error} ->
                              ?LOG(error, "Failed to open session: ~p", [Error], PState1),
                              {?RC_UNSPECIFIED_ERROR, PState1}
                    end;
                  {error, Reason} ->
                      ?LOG(error, "Username '~s' login failed for ~p", [Username, Reason], PState2),
                      {?RC_NOT_AUTHORIZED, PState1}
              end;
          {error, ReasonCode} ->
              {ReasonCode, PState1}
      end);

process_packet(Packet = ?PUBLISH_PACKET(?QOS_0, Topic, _PacketId, _Payload), PState) ->
    case check_publish(Packet, PState) of
        {ok, PState1} ->
            do_publish(Packet, PState1);
        {error, ?RC_TOPIC_ALIAS_INVALID} ->
            ?LOG(error, "Protocol error - ~p", [?RC_TOPIC_ALIAS_INVALID], PState),
            {error, ?RC_TOPIC_ALIAS_INVALID, PState};
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos0 message to ~s for ~s", [Topic, ReasonCode], PState),
            {error, ReasonCode, PState}
    end;

process_packet(Packet = ?PUBLISH_PACKET(?QOS_1, PacketId), PState) ->
    case check_publish(Packet, PState) of
        {ok, PState1} ->
            do_publish(Packet, PState1);
        {error, ReasonCode} ->
            deliver({puback, PacketId, ReasonCode}, PState)
    end;

process_packet(Packet = ?PUBLISH_PACKET(?QOS_2, PacketId), PState) ->
    case check_publish(Packet, PState) of
        {ok, PState1} ->
            do_publish(Packet, PState1);
        {error, ReasonCode} ->
            deliver({pubrec, PacketId, ReasonCode}, PState)
    end;

process_packet(?PUBACK_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    {ok = emqx_session:puback(SPid, PacketId, ReasonCode), PState};

process_packet(?PUBREC_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    case emqx_session:pubrec(SPid, PacketId, ReasonCode) of
        ok ->
            send(?PUBREL_PACKET(PacketId), PState);
        {error, NotFound} ->
            send(?PUBREL_PACKET(PacketId, NotFound), PState)
    end;

process_packet(?PUBREL_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    case emqx_session:pubrel(SPid, PacketId, ReasonCode) of
        ok ->
            send(?PUBCOMP_PACKET(PacketId), PState);
        {error, NotFound} ->
            send(?PUBCOMP_PACKET(PacketId, NotFound), PState)
    end;

process_packet(?PUBCOMP_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    {ok = emqx_session:pubcomp(SPid, PacketId, ReasonCode), PState};

process_packet(?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
               PState = #pstate{session = SPid, mountpoint = Mountpoint, proto_ver = ProtoVer, is_bridge = IsBridge}) ->
    RawTopicFilters1 =  if ProtoVer < ?MQTT_PROTO_V5 ->
                            case IsBridge of
                                true -> [{RawTopic, SubOpts#{rap => 1}} || {RawTopic, SubOpts} <- RawTopicFilters];
                                false -> [{RawTopic, SubOpts#{rap => 0}} || {RawTopic, SubOpts} <- RawTopicFilters]
                            end;
                           true ->
                               RawTopicFilters
                        end,
    case check_subscribe(
           parse_topic_filters(?SUBSCRIBE, RawTopicFilters1), PState) of
        {ok, TopicFilters} ->
            case emqx_hooks:run('client.subscribe', [credentials(PState)], TopicFilters) of
                {ok, TopicFilters1} ->
                    ok = emqx_session:subscribe(SPid, PacketId, Properties,
                                                emqx_mountpoint:mount(Mountpoint, TopicFilters1)),
                    {ok, PState};
                {stop, _} ->
                    ReasonCodes = lists:duplicate(length(TopicFilters),
                                                  ?RC_IMPLEMENTATION_SPECIFIC_ERROR),
                    deliver({suback, PacketId, ReasonCodes}, PState)
            end;
        {error, TopicFilters} ->
            ReasonCodes = lists:map(fun({_, #{rc := ?RC_SUCCESS}}) ->
                                            ?RC_IMPLEMENTATION_SPECIFIC_ERROR;
                                       ({_, #{rc := ReasonCode}}) ->
                                            ReasonCode
                                    end, TopicFilters),
            deliver({suback, PacketId, ReasonCodes}, PState)
    end;

process_packet(?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
               PState = #pstate{session = SPid, mountpoint = MountPoint}) ->
    case emqx_hooks:run('client.unsubscribe', [credentials(PState)],
                        parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters)) of
        {ok, TopicFilters} ->
            ok = emqx_session:unsubscribe(SPid, PacketId, Properties,
                                          emqx_mountpoint:mount(MountPoint, TopicFilters)),
            {ok, PState};
        {stop, _Acc} ->
            ReasonCodes = lists:duplicate(length(RawTopicFilters),
                                          ?RC_IMPLEMENTATION_SPECIFIC_ERROR),
            deliver({unsuback, PacketId, ReasonCodes}, PState)
    end;

process_packet(?PACKET(?PINGREQ), PState) ->
    send(?PACKET(?PINGRESP), PState);

process_packet(?DISCONNECT_PACKET(?RC_SUCCESS, #{'Session-Expiry-Interval' := Interval}), 
                PState = #pstate{session = SPid, conn_props = #{'Session-Expiry-Interval' := OldInterval}}) ->
    case Interval =/= 0 andalso OldInterval =:= 0 of
        true -> 
            deliver({disconnect, ?RC_PROTOCOL_ERROR}, PState),
            {error, protocol_error, PState#pstate{will_msg = undefined}};
        false -> 
            emqx_session:update_expiry_interval(SPid, Interval),
            %% Clean willmsg
            {stop, normal, PState#pstate{will_msg = undefined}}
    end;
process_packet(?DISCONNECT_PACKET(?RC_SUCCESS), PState) ->
    {stop, normal, PState#pstate{will_msg = undefined}};
process_packet(?DISCONNECT_PACKET(_), PState) ->
    {stop, normal, PState}.

%%------------------------------------------------------------------------------
%% ConnAck --> Client
%%------------------------------------------------------------------------------

connack({?RC_SUCCESS, SP, PState}) ->
    emqx_hooks:run('client.connected', [credentials(PState), ?RC_SUCCESS, attrs(PState)]),
    deliver({connack, ?RC_SUCCESS, sp(SP)}, update_mountpoint(PState));

connack({ReasonCode, PState = #pstate{proto_ver = ProtoVer}}) ->
    emqx_hooks:run('client.connected', [credentials(PState), ReasonCode, attrs(PState)]),
    ReasonCode1 = if ProtoVer =:= ?MQTT_PROTO_V5 ->
                         ReasonCode;
                     true ->
                         emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    _ = deliver({connack, ReasonCode1}, PState),
    {error, emqx_reason_codes:name(ReasonCode1, ProtoVer), PState}.

%%------------------------------------------------------------------------------
%% Publish Message -> Broker
%%------------------------------------------------------------------------------

do_publish(Packet = ?PUBLISH_PACKET(QoS, PacketId),
           PState = #pstate{session = SPid, mountpoint = MountPoint}) ->
    Msg = emqx_mountpoint:mount(MountPoint,
                                emqx_packet:to_message(credentials(PState), Packet)),
    puback(QoS, PacketId, emqx_session:publish(SPid, PacketId, Msg), PState).

%%------------------------------------------------------------------------------
%% Puback -> Client
%%------------------------------------------------------------------------------

puback(?QOS_0, _PacketId, _Result, PState) ->
    {ok, PState};
puback(?QOS_1, PacketId, {error, ReasonCode}, PState) ->
    deliver({puback, PacketId, ReasonCode}, PState);
puback(?QOS_1, PacketId, {ok, []}, PState) ->
    deliver({puback, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
puback(?QOS_1, PacketId, {ok, _}, PState) ->
    deliver({puback, PacketId, ?RC_SUCCESS}, PState);
puback(?QOS_2, PacketId, {error, ReasonCode}, PState) ->
    deliver({pubrec, PacketId, ReasonCode}, PState);
puback(?QOS_2, PacketId, {ok, []}, PState) ->
    deliver({pubrec, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
puback(?QOS_2, PacketId, {ok, _}, PState) ->
    deliver({pubrec, PacketId, ?RC_SUCCESS}, PState).

%%------------------------------------------------------------------------------
%% Deliver Packet -> Client
%%------------------------------------------------------------------------------

-spec(deliver(tuple(), state()) -> {ok, state()} | {error, term()}).
deliver({connack, ReasonCode}, PState) ->
    send(?CONNACK_PACKET(ReasonCode), PState);

deliver({connack, ?RC_SUCCESS, SP}, PState = #pstate{zone = Zone,
                                                     proto_ver = ?MQTT_PROTO_V5,
                                                     client_id = ClientId,
                                                     is_assigned = IsAssigned}) ->
    #{max_packet_size := MaxPktSize,
      max_qos_allowed := MaxQoS,
      mqtt_retain_available := Retain,
      max_topic_alias := MaxAlias,
      mqtt_shared_subscription := Shared,
      mqtt_wildcard_subscription := Wildcard} = caps(PState),
    Props = #{'Retain-Available' => flag(Retain),
              'Maximum-Packet-Size' => MaxPktSize,
              'Topic-Alias-Maximum' => MaxAlias,
              'Wildcard-Subscription-Available' => flag(Wildcard),
              'Subscription-Identifier-Available' => 1,
              'Shared-Subscription-Available' => flag(Shared)},

    Props1 = if 
                MaxQoS =:= ?QOS_2 -> 
                    Props;
                true ->
                    maps:put('Maximum-QoS', MaxQoS, Props)
            end,
    
    Props2 = if IsAssigned ->
                    Props1#{'Assigned-Client-Identifier' => ClientId};
                true -> Props1
             end,

    Props3 = case emqx_zone:get_env(Zone, server_keepalive) of
                 undefined -> Props2;
                 Keepalive -> Props2#{'Server-Keep-Alive' => Keepalive}
             end,
    send(?CONNACK_PACKET(?RC_SUCCESS, SP, Props3), PState);

deliver({connack, ReasonCode, SP}, PState) ->
    send(?CONNACK_PACKET(ReasonCode, SP), PState);

deliver({publish, PacketId, Msg}, PState = #pstate{mountpoint = MountPoint}) ->
    _ = emqx_hooks:run('message.delivered', [credentials(PState)], Msg),
    Msg1 = emqx_message:update_expiry(Msg),
    Msg2 = emqx_mountpoint:unmount(MountPoint, Msg1),
    send(emqx_packet:from_message(PacketId, Msg2), PState);

deliver({puback, PacketId, ReasonCode}, PState) ->
    send(?PUBACK_PACKET(PacketId, ReasonCode), PState);

deliver({pubrel, PacketId}, PState) ->
    send(?PUBREL_PACKET(PacketId), PState);

deliver({pubrec, PacketId, ReasonCode}, PState) ->
    send(?PUBREC_PACKET(PacketId, ReasonCode), PState);

deliver({suback, PacketId, ReasonCodes}, PState = #pstate{proto_ver = ProtoVer}) ->
    send(?SUBACK_PACKET(PacketId,
                        if ProtoVer =:= ?MQTT_PROTO_V5 ->
                               ReasonCodes;
                           true ->
                               [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes]
                        end), PState);

deliver({unsuback, PacketId, ReasonCodes}, PState) ->
    send(?UNSUBACK_PACKET(PacketId, ReasonCodes), PState);

%% Deliver a disconnect for mqtt 5.0
deliver({disconnect, ReasonCode}, PState = #pstate{proto_ver = ?MQTT_PROTO_V5}) ->
    send(?DISCONNECT_PACKET(ReasonCode), PState);

deliver({disconnect, _ReasonCode}, PState) ->
    {ok, PState}.

%%------------------------------------------------------------------------------
%% Send Packet to Client

-spec(send(emqx_mqtt_types:packet(), state()) -> {ok, state()} | {error, term()}).
send(Packet = ?PACKET(Type), PState = #pstate{proto_ver = Ver, sendfun = SendFun}) ->
    trace(send, Packet, PState),
    case SendFun(emqx_frame:serialize(Packet, #{version => Ver})) of
        ok ->
            emqx_metrics:sent(Packet),
            {ok, inc_stats(send, Type, PState)};
        {binary, _Data} ->
            emqx_metrics:sent(Packet),
            {ok, inc_stats(send, Type, PState)};
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Assign a clientid

maybe_assign_client_id(PState = #pstate{client_id = <<>>, ackprops = AckProps}) ->
    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
    AckProps1 = set_property('Assigned-Client-Identifier', ClientId, AckProps),
    PState#pstate{client_id = ClientId, is_assigned = true, ackprops = AckProps1};
maybe_assign_client_id(PState) ->
    PState.

try_open_session(PState = #pstate{zone        = Zone,
                                  client_id   = ClientId,
                                  conn_pid    = ConnPid,
                                  username    = Username,
                                  clean_start = CleanStart}) ->

    SessAttrs = #{
        zone        => Zone,
        client_id   => ClientId,
        conn_pid    => ConnPid,
        username    => Username,
        clean_start => CleanStart
    },

    SessAttrs1 = lists:foldl(fun set_session_attrs/2, SessAttrs, [{max_inflight, PState}, {expiry_interval, PState}, {topic_alias_maximum, PState}]),
    case emqx_sm:open_session(SessAttrs1) of
        {ok, SPid} ->
            {ok, SPid, false};
        Other -> Other
    end.

set_session_attrs({max_inflight, #pstate{zone = Zone, proto_ver = ProtoVer, conn_props = ConnProps}}, SessAttrs) ->
    maps:put(max_inflight, if
                               ProtoVer =:= ?MQTT_PROTO_V5 ->
                                   maps:get('Receive-Maximum', ConnProps, 65535);
                               true -> 
                                   emqx_zone:get_env(Zone, max_inflight, 65535)
                           end, SessAttrs);
set_session_attrs({expiry_interval, #pstate{zone = Zone, proto_ver = ProtoVer, conn_props = ConnProps, clean_start = CleanStart}}, SessAttrs) ->
    maps:put(expiry_interval, if
                               ProtoVer =:= ?MQTT_PROTO_V5 ->
                                   maps:get('Session-Expiry-Interval', ConnProps, 0);
                               true -> 
                                   case CleanStart of
                                       true -> 0;
                                       false -> 
                                           emqx_zone:get_env(Zone, session_expiry_interval, 16#ffffffff)
                                   end
                           end, SessAttrs);
set_session_attrs({topic_alias_maximum, #pstate{zone = Zone, proto_ver = ProtoVer, conn_props = ConnProps}}, SessAttrs) ->
    maps:put(topic_alias_maximum, if
                                    ProtoVer =:= ?MQTT_PROTO_V5 ->
                                        maps:get('Topic-Alias-Maximum', ConnProps, 0);
                                    true -> 
                                        emqx_zone:get_env(Zone, max_topic_alias, 0)
                                  end, SessAttrs);
set_session_attrs({_, #pstate{}}, SessAttrs) ->
    SessAttrs.


authenticate(Credentials, Password) ->
    case emqx_access_control:authenticate(Credentials, Password) of
        ok -> {ok, false};
        {ok, IsSuper} when is_boolean(IsSuper) ->
            {ok, IsSuper};
        {ok, Result} when is_map(Result) ->
            {ok, maps:get(is_superuser, Result, false)};
        {error, Error} ->
            {error, Error}
    end.

set_property(Name, Value, undefined) ->
    #{Name => Value};
set_property(Name, Value, Props) ->
    Props#{Name => Value}.

%%------------------------------------------------------------------------------
%% Check Packet
%%------------------------------------------------------------------------------

check_connect(Packet, PState) ->
    run_check_steps([fun check_proto_ver/2,
                     fun check_client_id/2,
                     fun check_banned/2], Packet, PState).

check_proto_ver(#mqtt_packet_connect{proto_ver  = Ver,
                                     proto_name = Name}, _PState) ->
    case lists:member({Ver, Name}, ?PROTOCOL_NAMES) of
        true  -> ok;
        false -> {error, ?RC_PROTOCOL_ERROR}
    end.

%% MQTT3.1 does not allow null clientId
check_client_id(#mqtt_packet_connect{proto_ver = ?MQTT_PROTO_V3,
                                     client_id = <<>>}, _PState) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

%% Issue#599: Null clientId and clean_start = false
check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = false}, _PState) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = true}, _PState) ->
    ok;

check_client_id(#mqtt_packet_connect{client_id = ClientId}, #pstate{zone = Zone}) ->
    Len = byte_size(ClientId),
    MaxLen = emqx_zone:get_env(Zone, max_clientid_len),
    case (1 =< Len) andalso (Len =< MaxLen) of
        true  -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

check_banned(_Connect, #pstate{enable_ban = false}) ->
    ok;
check_banned(#mqtt_packet_connect{client_id = ClientId, username = Username},
             #pstate{peername = Peername}) ->
    case emqx_banned:check(#{client_id => ClientId,
                             username  => Username,
                             peername  => Peername}) of
        true  -> {error, ?RC_BANNED};
        false -> ok
    end.

check_publish(Packet, PState) ->
    run_check_steps([fun check_pub_caps/2,
                     fun check_pub_acl/2], Packet, PState).

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS, retain = Retain},
                            variable = #mqtt_packet_publish{
                                          properties = #{'Topic-Alias' := TopicAlias}
                                         }},
               #pstate{zone = Zone}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain, topic_alias => TopicAlias});
check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS, retain = Retain},
                            variable = #mqtt_packet_publish{ properties = _Properties}},
               #pstate{zone = Zone}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).


check_pub_acl(_Packet, #pstate{is_super = IsSuper, enable_acl = EnableAcl})
    when IsSuper orelse (not EnableAcl) ->
    ok;

check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}}, PState) ->
    case emqx_access_control:check_acl(credentials(PState), publish, Topic) of
        allow -> ok;
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

run_check_steps([], _Packet, PState) ->
    {ok, PState};
run_check_steps([Check|Steps], Packet, PState) ->
    case Check(Packet, PState) of
        ok ->
            run_check_steps(Steps, Packet, PState);
        {ok, PState1} ->
            run_check_steps(Steps, Packet, PState1);
        Error = {error, _RC} ->
            Error
    end.

check_subscribe(TopicFilters, PState = #pstate{zone = Zone}) ->
    case emqx_mqtt_caps:check_sub(Zone, TopicFilters) of
        {ok, TopicFilter1} ->
            check_sub_acl(TopicFilter1, PState);
        {error, TopicFilter1} ->
            {error, TopicFilter1}
    end.

check_sub_acl(TopicFilters, #pstate{is_super = IsSuper, enable_acl = EnableAcl})
    when IsSuper orelse (not EnableAcl) ->
    {ok, TopicFilters};

check_sub_acl(TopicFilters, PState) ->
    Credentials = credentials(PState),
    lists:foldr(
      fun({Topic, SubOpts}, {Ok, Acc}) ->
              case emqx_access_control:check_acl(Credentials, subscribe, Topic) of
                  allow -> {Ok, [{Topic, SubOpts}|Acc]};
                  deny  -> {error, [{Topic, SubOpts#{rc := ?RC_NOT_AUTHORIZED}}|Acc]}
              end
      end, {ok, []}, TopicFilters).

trace(recv, Packet, PState) ->
    ?LOG(debug, "RECV ~s", [emqx_packet:format(Packet)], PState);
trace(send, Packet, PState) ->
    ?LOG(debug, "SEND ~s", [emqx_packet:format(Packet)], PState).

inc_stats(recv, Type, PState = #pstate{recv_stats = Stats}) ->
    PState#pstate{recv_stats = inc_stats(Type, Stats)};

inc_stats(send, Type, PState = #pstate{send_stats = Stats}) ->
    PState#pstate{send_stats = inc_stats(Type, Stats)}.

inc_stats(Type, Stats = #{pkt := PktCnt, msg := MsgCnt}) ->
    Stats#{pkt := PktCnt + 1, msg := case Type =:= ?PUBLISH of
                                         true  -> MsgCnt + 1;
                                         false -> MsgCnt
                                     end}.

shutdown(_Reason, #pstate{client_id = undefined}) ->
    ok;
shutdown(_Reason, #pstate{connected = false}) ->
    ok;
shutdown(Reason, #pstate{client_id = ClientId}) when Reason =:= conflict;
                                                     Reason =:= discard ->
    emqx_cm:unregister_connection(ClientId);
shutdown(Reason, PState = #pstate{connected = true,
                                  client_id = ClientId,
                                  will_msg  = WillMsg}) ->
    ?LOG(info, "Shutdown for ~p", [Reason], PState),
    _ = send_willmsg(WillMsg),
    emqx_hooks:run('client.disconnected', [credentials(PState), Reason]),
    emqx_cm:unregister_connection(ClientId).

send_willmsg(undefined) ->
    ignore;
send_willmsg(WillMsg = #message{topic = Topic,
                                headers = #{'Will-Delay-Interval' := Interval}})
            when is_integer(Interval), Interval > 0 ->
    SendAfter = integer_to_binary(Interval),
    emqx_broker:publish(WillMsg#message{topic = <<"$delayed/", SendAfter/binary, "/", Topic/binary>>});
send_willmsg(WillMsg) ->
    emqx_broker:publish(WillMsg).

start_keepalive(0, _PState) ->
    ignore;
start_keepalive(Secs, #pstate{zone = Zone}) when Secs > 0 ->
    Backoff = emqx_zone:get_env(Zone, keepalive_backoff, 0.75),
    self() ! {keepalive, start, round(Secs * Backoff)}.

rc(Reason) ->
    case Reason of
        protocol_error -> ?RC_PROTOCOL_ERROR;
        topic_filters_invalid -> ?RC_TOPIC_FILTER_INVALID;
        topic_name_invalid -> ?RC_TOPIC_NAME_INVALID;
        _ -> ?RC_MALFORMED_PACKET
    end.

%%-----------------------------------------------------------------------------
%% Parse topic filters
%%-----------------------------------------------------------------------------

parse_topic_filters(?SUBSCRIBE, RawTopicFilters) ->
    [emqx_topic:parse(RawTopic, SubOpts) || {RawTopic, SubOpts} <- RawTopicFilters];

parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters) ->
    lists:map(fun emqx_topic:parse/1, RawTopicFilters).

%%------------------------------------------------------------------------------
%% Update mountpoint

update_mountpoint(PState = #pstate{mountpoint = undefined}) ->
    PState;
update_mountpoint(PState = #pstate{mountpoint = MountPoint}) ->
    PState#pstate{mountpoint = emqx_mountpoint:replvar(MountPoint, credentials(PState))}.

sp(true)  -> 1;
sp(false) -> 0.

flag(false) -> 0;
flag(true)  -> 1.
