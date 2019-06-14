%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_protocol).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").

-export([ info/1
        , attrs/1
        , attr/2
        , caps/1
        , caps/2
        , stats/1
        , client_id/1
        , credentials/1
        , session/1
        ]).

-export([ init/2
        , received/2
        , process/2
        , deliver/2
        , send/2
        , terminate/2
        ]).

-record(pstate, {
          zone,
          sendfun,
          sockname,
          peername,
          peercert,
          proto_ver,
          proto_name,
          client_id,
          is_assigned,
          conn_pid,
          conn_props,
          ack_props,
          username,
          session,
          clean_start,
          topic_aliases,
          will_topic,
          will_msg,
          keepalive,
          is_bridge,
          recv_stats,
          send_stats,
          connected,
          connected_at,
          topic_alias_maximum,
          conn_mod,
          credentials,
          ws_cookie
        }).

-opaque(state() :: #pstate{}).

-export_type([state/0]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(NO_PROPS, undefined).

%%------------------------------------------------------------------------------
%% Init
%%------------------------------------------------------------------------------

-spec(init(map(), list()) -> state()).
init(SocketOpts = #{ sockname := Sockname
                   , peername := Peername
                   , peercert := Peercert
                   , sendfun := SendFun}, Options)  ->
    Zone = proplists:get_value(zone, Options),
    #pstate{zone                   = Zone,
            sendfun                = SendFun,
            sockname               = Sockname,
            peername               = Peername,
            peercert               = Peercert,
            proto_ver              = ?MQTT_PROTO_V4,
            proto_name             = <<"MQTT">>,
            client_id              = <<>>,
            is_assigned            = false,
            conn_pid               = self(),
            username               = init_username(Peercert, Options),
            clean_start            = false,
            topic_aliases          = #{},
            is_bridge              = false,
            recv_stats             = #{msg => 0, pkt => 0},
            send_stats             = #{msg => 0, pkt => 0},
            connected              = false,
            topic_alias_maximum    = #{to_client => 0, from_client => 0},
            conn_mod               = maps:get(conn_mod, SocketOpts, undefined),
            credentials            = #{},
            ws_cookie              = maps:get(ws_cookie, SocketOpts, undefined)}.

init_username(Peercert, Options) ->
    case proplists:get_value(peer_cert_as_username, Options) of
        cn  -> esockd_peercert:common_name(Peercert);
        dn  -> esockd_peercert:subject(Peercert);
        crt -> Peercert;
        _   -> undefined
    end.

set_username(Username, PState = #pstate{username = undefined}) ->
    PState#pstate{username = Username};
set_username(_Username, PState) ->
    PState.

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

info(PState = #pstate{zone          = Zone,
                      conn_props    = ConnProps,
                      ack_props     = AckProps,
                      session       = Session,
                      topic_aliases = Aliases,
                      will_msg      = WillMsg}) ->
    maps:merge(attrs(PState), #{conn_props => ConnProps,
                                ack_props => AckProps,
                                session => Session,
                                topic_aliases => Aliases,
                                will_msg => WillMsg,
                                enable_acl => emqx_zone:get_env(Zone, enable_acl, false)
                               }).

attrs(#pstate{zone         = Zone,
              client_id    = ClientId,
              username     = Username,
              peername     = Peername,
              peercert     = Peercert,
              clean_start  = CleanStart,
              proto_ver    = ProtoVer,
              proto_name   = ProtoName,
              keepalive    = Keepalive,
              is_bridge    = IsBridge,
              connected_at = ConnectedAt,
              conn_mod     = ConnMod,
              credentials  = Credentials}) ->
    #{ zone => Zone
     , client_id => ClientId
     , username => Username
     , peername => Peername
     , peercert => Peercert
     , proto_ver => ProtoVer
     , proto_name => ProtoName
     , clean_start => CleanStart
     , keepalive => Keepalive
     , is_bridge => IsBridge
     , connected_at => ConnectedAt
     , conn_mod => ConnMod
     , credentials => Credentials
     }.

attr(proto_ver, #pstate{proto_ver = ProtoVer}) ->
    ProtoVer;
attr(max_inflight, #pstate{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Receive-Maximum', ConnProps, 65535);
attr(max_inflight, #pstate{zone = Zone}) ->
    emqx_zone:get_env(Zone, max_inflight, 65535);
attr(expiry_interval, #pstate{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Session-Expiry-Interval', ConnProps, 0);
attr(expiry_interval, #pstate{zone = Zone, clean_start = CleanStart}) ->
    case CleanStart of
        true -> 0;
        false -> emqx_zone:get_env(Zone, session_expiry_interval, 16#ffffffff)
    end;
attr(topic_alias_maximum, #pstate{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Topic-Alias-Maximum', ConnProps, 0);
attr(topic_alias_maximum, #pstate{zone = Zone}) ->
    emqx_zone:get_env(Zone, max_topic_alias, 0);
attr(Name, PState) ->
    Attrs = lists:zip(record_info(fields, pstate), tl(tuple_to_list(PState))),
    case lists:keyfind(Name, 1, Attrs) of
        {_, Value} -> Value;
        false -> undefined
    end.

caps(Name, PState) ->
    maps:get(Name, caps(PState)).

caps(#pstate{zone = Zone}) ->
    emqx_mqtt_caps:get_caps(Zone).

client_id(#pstate{client_id = ClientId}) ->
    ClientId.

credentials(#pstate{zone      = Zone,
                    client_id = ClientId,
                    username  = Username,
                    sockname  = Sockname,
                    peername  = Peername,
                    peercert  = Peercert,
                    ws_cookie = WsCookie}) ->
    with_cert(#{zone => Zone,
                client_id => ClientId,
                sockname => Sockname,
                username => Username,
                peername => Peername,
                ws_cookie => WsCookie,
                mountpoint => emqx_zone:get_env(Zone, mountpoint)}, Peercert).

with_cert(Credentials, undefined) -> Credentials;
with_cert(Credentials, Peercert) ->
    Credentials#{dn => esockd_peercert:subject(Peercert),
                 cn => esockd_peercert:common_name(Peercert)}.

keepsafety(Credentials) ->
    maps:filter(fun(password, _) -> false;
                   (dn, _) -> false;
                   (cn, _) -> false;
                   (_,  _) -> true end, Credentials).

stats(#pstate{recv_stats = #{pkt := RecvPkt, msg := RecvMsg},
              send_stats = #{pkt := SendPkt, msg := SendMsg}}) ->
    [{recv_pkt, RecvPkt},
     {recv_msg, RecvMsg},
     {send_pkt, SendPkt},
     {send_msg, SendMsg}].

session(#pstate{session = SPid}) ->
    SPid.

%%------------------------------------------------------------------------------
%% Packet Received
%%------------------------------------------------------------------------------

set_protover(?CONNECT_PACKET(#mqtt_packet_connect{proto_ver = ProtoVer}), PState) ->
    PState#pstate{proto_ver = ProtoVer};
set_protover(_Packet, PState) ->
    PState.

-spec(received(emqx_mqtt_types:packet(), state())
      -> {ok, state()}
       | {error, term()}
       | {error, term(), state()}
       | {stop, term(), state()}).
received(?PACKET(Type), PState = #pstate{connected = false}) when Type =/= ?CONNECT ->
    {error, proto_not_connected, PState};

received(?PACKET(?CONNECT), PState = #pstate{connected = true}) ->
    {error, proto_unexpected_connect, PState};

received(Packet = ?PACKET(Type), PState) ->
    trace(recv, Packet),
    PState1 = set_protover(Packet, PState),
    try emqx_packet:validate(Packet) of
        true ->
            case preprocess_properties(Packet, PState1) of
                {ok, Packet1, PState2} ->
                    process(Packet1, inc_stats(recv, Type, PState2));
                {error, ReasonCode} ->
                    {error, ReasonCode, PState1}
            end
    catch
        error:protocol_error ->
            deliver({disconnect, ?RC_PROTOCOL_ERROR}, PState1),
            {error, protocol_error, PState};
        error:subscription_identifier_invalid ->
            deliver({disconnect, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED}, PState1),
            {error, subscription_identifier_invalid, PState1};
        error:topic_alias_invalid ->
            deliver({disconnect, ?RC_TOPIC_ALIAS_INVALID}, PState1),
            {error, topic_alias_invalid, PState1};
        error:topic_filters_invalid ->
            deliver({disconnect, ?RC_TOPIC_FILTER_INVALID}, PState1),
            {error, topic_filters_invalid, PState1};
        error:topic_name_invalid ->
            deliver({disconnect, ?RC_TOPIC_FILTER_INVALID}, PState1),
            {error, topic_filters_invalid, PState1};
        error:Reason ->
            deliver({disconnect, ?RC_MALFORMED_PACKET}, PState1),
            {error, Reason, PState1}
    end.

%%------------------------------------------------------------------------------
%% Preprocess MQTT Properties
%%------------------------------------------------------------------------------

preprocess_properties(Packet = #mqtt_packet{
                                   variable = #mqtt_packet_connect{
                                                  properties = #{'Topic-Alias-Maximum' := ToClient}
                                              }
                               },
                      PState = #pstate{topic_alias_maximum = TopicAliasMaximum}) ->
    {ok, Packet, PState#pstate{topic_alias_maximum = TopicAliasMaximum#{to_client => ToClient}}};

%% Subscription Identifier
preprocess_properties(Packet = #mqtt_packet{
                                  variable = Subscribe = #mqtt_packet_subscribe{
                                                            properties    = #{'Subscription-Identifier' := SubId},
                                                            topic_filters = TopicFilters
                                                           }
                                 },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5}) ->
    TopicFilters1 = [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters],
    {ok, Packet#mqtt_packet{variable = Subscribe#mqtt_packet_subscribe{topic_filters = TopicFilters1}}, PState};

%% Topic Alias Mapping
preprocess_properties(#mqtt_packet{
                          variable = #mqtt_packet_publish{
                              properties = #{'Topic-Alias' := 0}}
                      },
                      PState) ->
    deliver({disconnect, ?RC_TOPIC_ALIAS_INVALID}, PState),
    {error, ?RC_TOPIC_ALIAS_INVALID};

preprocess_properties(Packet = #mqtt_packet{
                                  variable = Publish = #mqtt_packet_publish{
                                                          topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}}
                                 },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5,
                                       topic_aliases = Aliases,
                                       topic_alias_maximum = #{from_client := TopicAliasMaximum}}) ->
    case AliasId =< TopicAliasMaximum of
        true ->
            {ok, Packet#mqtt_packet{variable = Publish#mqtt_packet_publish{
                                                 topic_name = maps:get(AliasId, Aliases, <<>>)}}, PState};
        false ->
            deliver({disconnect, ?RC_TOPIC_ALIAS_INVALID}, PState),
            {error, ?RC_TOPIC_ALIAS_INVALID}
    end;

preprocess_properties(Packet = #mqtt_packet{
                                   variable = #mqtt_packet_publish{
                                                  topic_name = Topic,
                                                  properties = #{'Topic-Alias' := AliasId}}
                               },
                      PState = #pstate{proto_ver = ?MQTT_PROTO_V5,
                                       topic_aliases = Aliases,
                                       topic_alias_maximum = #{from_client := TopicAliasMaximum}}) ->
    case AliasId =< TopicAliasMaximum of
        true ->
            {ok, Packet, PState#pstate{topic_aliases = maps:put(AliasId, Topic, Aliases)}};
        false ->
            deliver({disconnect, ?RC_TOPIC_ALIAS_INVALID}, PState),
            {error, ?RC_TOPIC_ALIAS_INVALID}
    end;

preprocess_properties(Packet, PState) ->
    {ok, Packet, PState}.

%%------------------------------------------------------------------------------
%% Process MQTT Packet
%%------------------------------------------------------------------------------
process(?CONNECT_PACKET(
           #mqtt_packet_connect{proto_name  = ProtoName,
                                proto_ver   = ProtoVer,
                                is_bridge   = IsBridge,
                                clean_start = CleanStart,
                                keepalive   = Keepalive,
                                properties  = ConnProps,
                                client_id   = ClientId,
                                username    = Username,
                                password    = Password} = ConnPkt), PState) ->

    %% TODO: Mountpoint...
    %% Msg -> emqx_mountpoint:mount(MountPoint, Msg)
    PState0 = maybe_use_username_as_clientid(ClientId,
                set_username(Username,
                             PState#pstate{proto_ver    = ProtoVer,
                                           proto_name   = ProtoName,
                                           clean_start  = CleanStart,
                                           keepalive    = Keepalive,
                                           conn_props   = ConnProps,
                                           is_bridge    = IsBridge,
                                           connected_at = os:timestamp()})),

    NewClientId = PState0#pstate.client_id,

    emqx_logger:set_metadata_client_id(NewClientId),

    Credentials = credentials(PState0),
    PState1 = PState0#pstate{credentials = Credentials},
    connack(
      case check_connect(ConnPkt, PState1) of
          ok ->
              case emqx_access_control:authenticate(Credentials#{password => Password}) of
                  {ok, Credentials0} ->
                      PState3 = maybe_assign_client_id(PState1),
                      emqx_logger:set_metadata_client_id(PState3#pstate.client_id),
                      %% Open session
                      SessAttrs = #{will_msg => make_will_msg(ConnPkt)},
                      case try_open_session(SessAttrs, PState3) of
                          {ok, SPid, SP} ->
                              PState4 = PState3#pstate{session = SPid, connected = true,
                                                       credentials = keepsafety(Credentials0)},
                              ok = emqx_cm:register_connection(client_id(PState4)),
                              true = emqx_cm:set_conn_attrs(client_id(PState4), attrs(PState4)),
                              %% Start keepalive
                              start_keepalive(Keepalive, PState4),
                              %% Success
                              {?RC_SUCCESS, SP, PState4};
                          {error, Error} ->
                              ?LOG(error, "[Protocol] Failed to open session: ~p", [Error]),
                              {?RC_UNSPECIFIED_ERROR, PState1#pstate{credentials = Credentials0}}
                      end;
                  {error, Reason} ->
                      ?LOG(warning, "[Protocol] Client ~s (Username: '~s') login failed for ~p", [NewClientId, Username, Reason]),
                      {emqx_reason_codes:connack_error(Reason), PState1#pstate{credentials = Credentials}}
              end;
          {error, ReasonCode} ->
              {ReasonCode, PState1}
      end);

process(Packet = ?PUBLISH_PACKET(?QOS_0, Topic, _PacketId, _Payload), PState = #pstate{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "[Protocol] Cannot publish qos0 message to ~s for ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
            do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState)
    end;

process(Packet = ?PUBLISH_PACKET(?QOS_1, Topic, PacketId, _Payload), PState = #pstate{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "[Protocol] Cannot publish qos1 message to ~s for ~s", [Topic, emqx_reason_codes:text(ReasonCode)]),
            case deliver({puback, PacketId, ReasonCode}, PState) of
                {ok, PState1} ->
                    AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
                    do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState1);
                Error -> Error
            end
    end;

process(Packet = ?PUBLISH_PACKET(?QOS_2, Topic, PacketId, _Payload), PState = #pstate{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "[Protocol] Cannot publish qos2 message to ~s for ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            case deliver({pubrec, PacketId, ReasonCode}, PState) of
                {ok, PState1} ->
                    AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
                    do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState1);
                Error -> Error
            end
    end;

process(?PUBACK_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    {ok = emqx_session:puback(SPid, PacketId, ReasonCode), PState};

process(?PUBREC_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    case emqx_session:pubrec(SPid, PacketId, ReasonCode) of
        ok ->
            send(?PUBREL_PACKET(PacketId), PState);
        {error, NotFound} ->
            send(?PUBREL_PACKET(PacketId, NotFound), PState)
    end;

process(?PUBREL_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    case emqx_session:pubrel(SPid, PacketId, ReasonCode) of
        ok ->
            send(?PUBCOMP_PACKET(PacketId), PState);
        {error, NotFound} ->
            send(?PUBCOMP_PACKET(PacketId, NotFound), PState)
    end;

process(?PUBCOMP_PACKET(PacketId, ReasonCode), PState = #pstate{session = SPid}) ->
    {ok = emqx_session:pubcomp(SPid, PacketId, ReasonCode), PState};

process(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
        PState = #pstate{zone = Zone, session = SPid, credentials = Credentials}) ->
    case check_subscribe(parse_topic_filters(?SUBSCRIBE, raw_topic_filters(PState, RawTopicFilters)), PState) of
        {ok, TopicFilters} ->
            TopicFilters0 = emqx_hooks:run_fold('client.subscribe', [Credentials], TopicFilters),
            TopicFilters1 = emqx_mountpoint:mount(mountpoint(Credentials), TopicFilters0),
            ok = emqx_session:subscribe(SPid, PacketId, Properties, TopicFilters1),
            {ok, PState};
        {error, TopicFilters} ->
            {SubTopics, ReasonCodes} =
                lists:foldr(fun({Topic, #{rc := ?RC_SUCCESS}}, {Topics, Codes}) ->
                                    {[Topic|Topics], [?RC_IMPLEMENTATION_SPECIFIC_ERROR | Codes]};
                                ({Topic, #{rc := Code}}, {Topics, Codes}) ->
                                    {[Topic|Topics], [Code|Codes]}
                            end, {[], []}, TopicFilters),
            ?LOG(warning, "[Protocol] Cannot subscribe ~p for ~p",
                 [SubTopics, [emqx_reason_codes:text(R) || R <- ReasonCodes]]),
            case deliver({suback, PacketId, ReasonCodes}, PState) of
                {ok, PState1} ->
                    AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
                    do_acl_deny_action(AclDenyAction, Packet, ReasonCodes, PState1);
                Error ->
                    Error
            end
    end;

process(?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
        PState = #pstate{session = SPid, credentials = Credentials}) ->
    TopicFilters = emqx_hooks:run_fold('client.unsubscribe', [Credentials],
                                       parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters)),
    ok = emqx_session:unsubscribe(SPid, PacketId, Properties,
                                  emqx_mountpoint:mount(mountpoint(Credentials), TopicFilters)),
    {ok, PState};

process(?PACKET(?PINGREQ), PState) ->
    send(?PACKET(?PINGRESP), PState);

process(?DISCONNECT_PACKET(?RC_SUCCESS, #{'Session-Expiry-Interval' := Interval}),
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

process(?DISCONNECT_PACKET(?RC_SUCCESS), PState) ->
    {stop, normal, PState#pstate{will_msg = undefined}};

process(?DISCONNECT_PACKET(_), PState) ->
    {stop, {shutdown, abnormal_disconnet}, PState}.

%%------------------------------------------------------------------------------
%% ConnAck --> Client
%%------------------------------------------------------------------------------

connack({?RC_SUCCESS, SP, PState = #pstate{credentials = Credentials}}) ->
    ok = emqx_hooks:run('client.connected', [Credentials, ?RC_SUCCESS, attrs(PState)]),
    deliver({connack, ?RC_SUCCESS, sp(SP)}, PState);

connack({ReasonCode, PState = #pstate{proto_ver = ProtoVer, credentials = Credentials}}) ->
    ok = emqx_hooks:run('client.connected', [Credentials, ReasonCode, attrs(PState)]),
    [ReasonCode1] = reason_codes_compat(connack, [ReasonCode], ProtoVer),
    _ = deliver({connack, ReasonCode1}, PState),
    {error, emqx_reason_codes:name(ReasonCode1, ProtoVer), PState}.

%%------------------------------------------------------------------------------
%% Publish Message -> Broker
%%------------------------------------------------------------------------------

do_publish(Packet = ?PUBLISH_PACKET(QoS, PacketId),
           PState = #pstate{session = SPid, credentials = Credentials}) ->
    Msg = emqx_mountpoint:mount(mountpoint(Credentials),
                                emqx_packet:to_message(Credentials, Packet)),
    puback(QoS, PacketId, emqx_session:publish(SPid, PacketId, emqx_message:set_flag(dup, false, Msg)), PState).

%%------------------------------------------------------------------------------
%% Puback -> Client
%%------------------------------------------------------------------------------

puback(?QOS_0, _PacketId, _Result, PState) ->
    {ok, PState};
puback(?QOS_1, PacketId, {ok, []}, PState) ->
    deliver({puback, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
%%TODO: calc the deliver count?
puback(?QOS_1, PacketId, {ok, _Result}, PState) ->
    deliver({puback, PacketId, ?RC_SUCCESS}, PState);
puback(?QOS_1, PacketId, {error, ReasonCode}, PState) ->
    deliver({puback, PacketId, ReasonCode}, PState);
puback(?QOS_2, PacketId, {ok, []}, PState) ->
    deliver({pubrec, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
puback(?QOS_2, PacketId, {ok, _Result}, PState) ->
    deliver({pubrec, PacketId, ?RC_SUCCESS}, PState);
puback(?QOS_2, PacketId, {error, ReasonCode}, PState) ->
    deliver({pubrec, PacketId, ReasonCode}, PState).

%%------------------------------------------------------------------------------
%% Deliver Packet -> Client
%%------------------------------------------------------------------------------

-spec(deliver(list(tuple()) | tuple(), state()) -> {ok, state()} | {error, term()}).
deliver([], PState) ->
    {ok, PState};
deliver([Pub|More], PState) ->
    case deliver(Pub, PState) of
        {ok, PState1} ->
            deliver(More, PState1);
        {error, _} = Error ->
            Error
    end;

deliver({connack, ReasonCode}, PState) ->
    send(?CONNACK_PACKET(ReasonCode), PState);

deliver({connack, ?RC_SUCCESS, SP}, PState = #pstate{zone = Zone,
                                                     proto_ver = ?MQTT_PROTO_V5,
                                                     client_id = ClientId,
                                                     is_assigned = IsAssigned,
                                                     topic_alias_maximum = TopicAliasMaximum}) ->
    #{max_packet_size := MaxPktSize,
      max_qos_allowed := MaxQoS,
      mqtt_retain_available := Retain,
      max_topic_alias := MaxAlias,
      mqtt_shared_subscription := Shared,
      mqtt_wildcard_subscription := Wildcard} = caps(PState),
    %% Response-Information is so far not set by broker.
    %% i.e. It's a Client-to-Client contract for the request-response topic naming scheme.
    %% According to MQTT 5.0 spec:
    %%   A common use of this is to pass a globally unique portion of the topic tree which
    %%   is reserved for this Client for at least the lifetime of its Session.
    %%   This often cannot just be a random name as both the requesting Client and the
    %%   responding Client need to be authorized to use it.
    %% If we are to support it in the feature, the implementation should be flexible
    %% to allow prefixing the response topic based on different ACL config.
    %% e.g. prefix by username or client-id, so that unauthorized clients can not
    %% subscribe requests or responses that are not intended for them.
    Props = #{'Retain-Available' => flag(Retain),
              'Maximum-Packet-Size' => MaxPktSize,
              'Topic-Alias-Maximum' => MaxAlias,
              'Wildcard-Subscription-Available' => flag(Wildcard),
              'Subscription-Identifier-Available' => 1,
              %'Response-Information' =>
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

    PState1 = PState#pstate{topic_alias_maximum = TopicAliasMaximum#{from_client => MaxAlias}},

    send(?CONNACK_PACKET(?RC_SUCCESS, SP, Props3), PState1);

deliver({connack, ReasonCode, SP}, PState) ->
    send(?CONNACK_PACKET(ReasonCode, SP), PState);

deliver({publish, PacketId, Msg}, PState = #pstate{credentials = Credentials}) ->
    Msg0 = emqx_hooks:run_fold('message.deliver', [Credentials], Msg),
    Msg1 = emqx_message:update_expiry(Msg0),
    Msg2 = emqx_mountpoint:unmount(mountpoint(Credentials), Msg1),
    send(emqx_packet:from_message(PacketId, Msg2), PState);

deliver({puback, PacketId, ReasonCode}, PState) ->
    send(?PUBACK_PACKET(PacketId, ReasonCode), PState);

deliver({pubrel, PacketId}, PState) ->
    send(?PUBREL_PACKET(PacketId), PState);

deliver({pubrec, PacketId, ReasonCode}, PState) ->
    send(?PUBREC_PACKET(PacketId, ReasonCode), PState);

deliver({suback, PacketId, ReasonCodes}, PState = #pstate{proto_ver = ProtoVer}) ->
    send(?SUBACK_PACKET(PacketId, reason_codes_compat(suback, ReasonCodes, ProtoVer)), PState);

deliver({unsuback, PacketId, ReasonCodes}, PState = #pstate{proto_ver = ProtoVer}) ->
    send(?UNSUBACK_PACKET(PacketId, reason_codes_compat(unsuback, ReasonCodes, ProtoVer)), PState);

%% Deliver a disconnect for mqtt 5.0
deliver({disconnect, ReasonCode}, PState = #pstate{proto_ver = ?MQTT_PROTO_V5}) ->
    send(?DISCONNECT_PACKET(ReasonCode), PState);

deliver({disconnect, _ReasonCode}, PState) ->
    {ok, PState}.

%%------------------------------------------------------------------------------
%% Send Packet to Client

-spec(send(emqx_mqtt_types:packet(), state()) -> {ok, state()} | {error, term()}).
send(Packet = ?PACKET(Type), PState = #pstate{proto_ver = Ver, sendfun = Send}) ->
    case Send(Packet, #{version => Ver}) of
        ok ->
            trace(send, Packet),
            {ok, PState};
        {ok, Data} ->
            trace(send, Packet),
            emqx_metrics:inc_sent(Packet),
            ok = emqx_metrics:inc('bytes.sent', iolist_size(Data)),
            {ok, inc_stats(send, Type, PState)};
        {error, Reason} ->
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% Maybe use username replace client id

maybe_use_username_as_clientid(ClientId, PState = #pstate{username = undefined}) ->
    PState#pstate{client_id = ClientId};
maybe_use_username_as_clientid(ClientId, PState = #pstate{username = Username, zone = Zone}) ->
    case emqx_zone:get_env(Zone, use_username_as_clientid, false) of
        true -> PState#pstate{client_id = Username};
        false -> PState#pstate{client_id = ClientId}
    end.

%%------------------------------------------------------------------------------
%% Assign a clientId

maybe_assign_client_id(PState = #pstate{client_id = <<>>, ack_props = AckProps}) ->
    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
    AckProps1 = set_property('Assigned-Client-Identifier', ClientId, AckProps),
    PState#pstate{client_id = ClientId, is_assigned = true, ack_props = AckProps1};
maybe_assign_client_id(PState) ->
    PState.

try_open_session(SessAttrs, PState = #pstate{zone = Zone,
                                             client_id = ClientId,
                                             conn_pid = ConnPid,
                                             username = Username,
                                             clean_start = CleanStart}) ->
    case emqx_sm:open_session(
           maps:merge(#{zone => Zone,
                        client_id => ClientId,
                        conn_pid => ConnPid,
                        username => Username,
                        clean_start => CleanStart,
                        max_inflight => attr(max_inflight, PState),
                        expiry_interval => attr(expiry_interval, PState),
                        topic_alias_maximum => attr(topic_alias_maximum, PState)},
                      SessAttrs)) of
        {ok, SPid} ->
            {ok, SPid, false};
        Other -> Other
    end.

set_property(Name, Value, ?NO_PROPS) ->
    #{Name => Value};
set_property(Name, Value, Props) ->
    Props#{Name => Value}.

get_property(_Name, undefined, Default) ->
    Default;
get_property(Name, Props, Default) ->
    maps:get(Name, Props, Default).

make_will_msg(#mqtt_packet_connect{proto_ver   = ProtoVer,
                                   will_props  = WillProps} = ConnPkt) ->
    emqx_packet:will_msg(
        case ProtoVer of
            ?MQTT_PROTO_V5 ->
                WillDelayInterval = get_property('Will-Delay-Interval', WillProps, 0),
                ConnPkt#mqtt_packet_connect{
                    will_props = set_property('Will-Delay-Interval', WillDelayInterval, WillProps)};
            _ ->
                ConnPkt
        end).

%%------------------------------------------------------------------------------
%% Check Packet
%%------------------------------------------------------------------------------

check_connect(Packet, PState) ->
    run_check_steps([fun check_proto_ver/2,
                     fun check_client_id/2,
                     fun check_flapping/2,
                     fun check_banned/2,
                     fun check_will_topic/2,
                     fun check_will_retain/2], Packet, PState).

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

check_flapping(#mqtt_packet_connect{}, PState) ->
    do_flapping_detect(connect, PState).

check_banned(#mqtt_packet_connect{client_id = ClientId, username = Username},
             #pstate{zone = Zone, peername = Peername}) ->
    Credentials = #{client_id => ClientId,
                    username  => Username,
                    peername  => Peername},
    EnableBan = emqx_zone:get_env(Zone, enable_ban, false),
    do_check_banned(EnableBan, Credentials).

check_will_topic(#mqtt_packet_connect{will_flag = false}, _PState) ->
    ok;
check_will_topic(#mqtt_packet_connect{will_topic = WillTopic} = ConnPkt, PState) ->
    try emqx_topic:validate(WillTopic) of
        true -> check_will_acl(ConnPkt, PState)
    catch error : _Error ->
            {error, ?RC_TOPIC_NAME_INVALID}
    end.

check_will_retain(#mqtt_packet_connect{will_retain = false, proto_ver = ?MQTT_PROTO_V5}, _PState) ->
    ok;
check_will_retain(#mqtt_packet_connect{will_retain = true, proto_ver = ?MQTT_PROTO_V5}, #pstate{zone = Zone}) ->
    case emqx_zone:get_env(Zone, mqtt_retain_available, true) of
        true -> {error, ?RC_RETAIN_NOT_SUPPORTED};
        false -> ok
    end;
check_will_retain(_Packet, _PState) ->
    ok.

check_will_acl(#mqtt_packet_connect{will_topic = WillTopic},
               #pstate{zone = Zone, credentials = Credentials}) ->
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    case do_acl_check(EnableAcl, publish, Credentials, WillTopic) of
        ok -> ok;
        Other ->
            ?LOG(warning, "[Protocol] Cannot publish will message to ~p for acl denied", [WillTopic]),
            Other
    end.

check_publish(Packet, PState) ->
    run_check_steps([fun check_pub_caps/2,
                     fun check_pub_acl/2], Packet, PState).

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS, retain = Retain},
                            variable = #mqtt_packet_publish{properties = _Properties}},
               #pstate{zone = Zone}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).

check_pub_acl(_Packet, #pstate{credentials = #{is_superuser := IsSuper}})
        when IsSuper ->
    ok;
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #pstate{zone = Zone, credentials = Credentials}) ->
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    do_acl_check(EnableAcl, publish, Credentials, Topic).

run_check_steps([], _Packet, _PState) ->
    ok;
run_check_steps([Check|Steps], Packet, PState) ->
    case Check(Packet, PState) of
        ok ->
            run_check_steps(Steps, Packet, PState);
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

check_sub_acl(TopicFilters, #pstate{credentials = #{is_superuser := IsSuper}})
        when IsSuper ->
    {ok, TopicFilters};
check_sub_acl(TopicFilters, #pstate{zone = Zone, credentials = Credentials}) ->
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    lists:foldr(
      fun({Topic, SubOpts}, {Ok, Acc}) when EnableAcl ->
              AllowTerm = {Ok, [{Topic, SubOpts}|Acc]},
              DenyTerm = {error, [{Topic, SubOpts#{rc := ?RC_NOT_AUTHORIZED}}|Acc]},
              do_acl_check(subscribe, Credentials, Topic, AllowTerm, DenyTerm);
         (TopicFilter, Acc) ->
              {ok, [TopicFilter | Acc]}
      end, {ok, []}, TopicFilters).

trace(recv, Packet) ->
    ?LOG(debug, "[Protocol] RECV ~s", [emqx_packet:format(Packet)]);
trace(send, Packet) ->
    ?LOG(debug, "[Protocol] SEND ~s", [emqx_packet:format(Packet)]).

inc_stats(recv, Type, PState = #pstate{recv_stats = Stats}) ->
    PState#pstate{recv_stats = inc_stats(Type, Stats)};

inc_stats(send, Type, PState = #pstate{send_stats = Stats}) ->
    PState#pstate{send_stats = inc_stats(Type, Stats)}.

inc_stats(Type, Stats = #{pkt := PktCnt, msg := MsgCnt}) ->
    Stats#{pkt := PktCnt + 1, msg := case Type =:= ?PUBLISH of
                                         true  -> MsgCnt + 1;
                                         false -> MsgCnt
                                     end}.

terminate(_Reason, #pstate{client_id = undefined}) ->
    ok;
terminate(_Reason, PState = #pstate{connected = false}) ->
    do_flapping_detect(disconnect, PState),
    ok;
terminate(Reason, PState) when Reason =:= conflict;
                               Reason =:= discard ->
    do_flapping_detect(disconnect, PState),
    ok;

terminate(Reason, PState = #pstate{credentials = Credentials}) ->
    do_flapping_detect(disconnect, PState),
    ?LOG(info, "[Protocol] Shutdown for ~p", [Reason]),
    ok = emqx_hooks:run('client.disconnected', [Credentials, Reason]).

start_keepalive(0, _PState) ->
    ignore;
start_keepalive(Secs, #pstate{zone = Zone}) when Secs > 0 ->
    Backoff = emqx_zone:get_env(Zone, keepalive_backoff, 0.75),
    self() ! {keepalive, start, round(Secs * Backoff)}.

%%-----------------------------------------------------------------------------
%% Parse topic filters
%%-----------------------------------------------------------------------------

parse_topic_filters(?SUBSCRIBE, RawTopicFilters) ->
    [emqx_topic:parse(RawTopic, SubOpts) || {RawTopic, SubOpts} <- RawTopicFilters];

parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters) ->
    lists:map(fun emqx_topic:parse/1, RawTopicFilters).

sp(true)  -> 1;
sp(false) -> 0.

flag(false) -> 0;
flag(true)  -> 1.

%%------------------------------------------------------------------------------
%% Execute actions in case acl deny

do_flapping_detect(Action, #pstate{zone = Zone,
                                   client_id = ClientId}) ->
    ok = case emqx_zone:get_env(Zone, enable_flapping_detect, false) of
             true ->
                 Threshold = emqx_zone:get_env(Zone, flapping_threshold, {10, 60}),
                 case emqx_flapping:check(Action, ClientId, Threshold) of
                     flapping ->
                         BanExpiryInterval = emqx_zone:get_env(Zone, flapping_ban_expiry_interval, 3600000),
                         Until = erlang:system_time(second) + BanExpiryInterval,
                         emqx_banned:add(#banned{who = {client_id, ClientId},
                                                 reason = <<"flapping">>,
                                                 by = <<"flapping_checker">>,
                                                 until = Until}),
                         ok;
                     _Other ->
                         ok
                 end;
             _EnableFlappingDetect -> ok
         end.

do_acl_deny_action(disconnect, ?PUBLISH_PACKET(?QOS_0, _Topic, _PacketId, _Payload),
                   ?RC_NOT_AUTHORIZED, PState = #pstate{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};

do_acl_deny_action(disconnect, ?PUBLISH_PACKET(QoS, _Topic, _PacketId, _Payload),
                   ?RC_NOT_AUTHORIZED, PState = #pstate{proto_ver = ProtoVer})
  when QoS =:= ?QOS_1; QoS =:= ?QOS_2 ->
    deliver({disconnect, ?RC_NOT_AUTHORIZED}, PState),
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};

do_acl_deny_action(Action, ?SUBSCRIBE_PACKET(_PacketId, _Properties, _RawTopicFilters), ReasonCodes, PState)
  when is_list(ReasonCodes) ->
    traverse_reason_codes(ReasonCodes, Action, PState);
do_acl_deny_action(_OtherAction, _PubSubPacket, ?RC_NOT_AUTHORIZED, PState) ->
    {ok, PState};
do_acl_deny_action(_OtherAction, _PubSubPacket, ReasonCode, PState = #pstate{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(ReasonCode, ProtoVer), PState}.

traverse_reason_codes([], _Action, PState) ->
    {ok, PState};
traverse_reason_codes([?RC_SUCCESS | LeftReasonCodes], Action, PState) ->
    traverse_reason_codes(LeftReasonCodes, Action, PState);
traverse_reason_codes([?RC_NOT_AUTHORIZED | _LeftReasonCodes], disconnect, PState = #pstate{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};
traverse_reason_codes([?RC_NOT_AUTHORIZED | LeftReasonCodes], Action, PState) ->
    traverse_reason_codes(LeftReasonCodes, Action, PState);
traverse_reason_codes([OtherCode | _LeftReasonCodes], _Action, PState =  #pstate{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(OtherCode, ProtoVer), PState}.

%% Reason code compat
reason_codes_compat(_PktType, ReasonCodes, ?MQTT_PROTO_V5) ->
    ReasonCodes;
reason_codes_compat(unsuback, _ReasonCodes, _ProtoVer) ->
    undefined;
reason_codes_compat(PktType, ReasonCodes, _ProtoVer) ->
    [emqx_reason_codes:compat(PktType, RC) || RC <- ReasonCodes].

raw_topic_filters(#pstate{zone = Zone, proto_ver = ProtoVer, is_bridge = IsBridge}, RawTopicFilters) ->
    IgnoreLoop = emqx_zone:get_env(Zone, ignore_loop_deliver, false),
    case ProtoVer < ?MQTT_PROTO_V5 of
        true ->
            IfIgnoreLoop = case IgnoreLoop of true -> 1; false -> 0 end,
            case IsBridge of
               true -> [{RawTopic, SubOpts#{rap => 1, nl => IfIgnoreLoop}} || {RawTopic, SubOpts} <- RawTopicFilters];
               false -> [{RawTopic, SubOpts#{rap => 0, nl => IfIgnoreLoop}} || {RawTopic, SubOpts} <- RawTopicFilters]
            end;
        false ->
            RawTopicFilters
    end.

mountpoint(Credentials) ->
    maps:get(mountpoint, Credentials, undefined).

do_check_banned(_EnableBan = true, Credentials) ->
    case emqx_banned:check(Credentials) of
        true  -> {error, ?RC_BANNED};
        false -> ok
    end;
do_check_banned(_EnableBan, _Credentials) -> ok.

do_acl_check(_EnableAcl = true, Action, Credentials, Topic) ->
    AllowTerm = ok,
    DenyTerm = {error, ?RC_NOT_AUTHORIZED},
    do_acl_check(Action, Credentials, Topic, AllowTerm, DenyTerm);
do_acl_check(_EnableAcl, _Action, _Credentials, _Topic) ->
    ok.

do_acl_check(Action, Credentials, Topic, AllowTerm, DenyTerm) ->
    case emqx_access_control:check_acl(Credentials, Action, Topic) of
        allow -> AllowTerm;
        deny -> DenyTerm
    end.
