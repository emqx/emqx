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

%% MQTT Protocol
-module(emqx_protocol).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Protocol]").

-export([ info/1
        , attrs/1
        , attr/2
        , caps/1
        , caps/2
        , client_id/1
        , credentials/1
        , session/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_out/2
        , handle_timeout/3
        , terminate/2
        ]).

-export_type([protocol/0]).

-record(protocol, {
          zone         :: emqx_zone:zone(),
          conn_mod     :: module(),
          sendfun,
          sockname,
          peername,
          peercert,
          proto_ver    :: emqx_mqtt:version(),
          proto_name,
          client_id    :: maybe(emqx_types:client_id()),
          is_assigned,
          username     :: maybe(emqx_types:username()),
          conn_props,
          ack_props,
          credentials  :: map(),
          session      :: maybe(emqx_session:session()),
          clean_start,
          topic_aliases,
          will_topic,
          will_msg,
          keepalive,
          is_bridge    :: boolean(),
          connected    :: boolean(),
          connected_at :: erlang:timestamp(),
          topic_alias_maximum,
          ws_cookie
         }).

-opaque(protocol() :: #protocol{}).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-define(NO_PROPS, undefined).

%%--------------------------------------------------------------------
%% Init
%%--------------------------------------------------------------------

-spec(init(map(), list()) -> protocol()).
init(SocketOpts = #{sockname := Sockname,
                    peername := Peername,
                    peercert := Peercert}, Options)  ->
    Zone = proplists:get_value(zone, Options),
    #protocol{zone                = Zone,
              %%sendfun           = SendFun,
              sockname            = Sockname,
              peername            = Peername,
              peercert            = Peercert,
              proto_ver           = ?MQTT_PROTO_V4,
              proto_name          = <<"MQTT">>,
              client_id           = <<>>,
              is_assigned         = false,
              %%conn_pid            = self(),
              username            = init_username(Peercert, Options),
              clean_start         = false,
              topic_aliases       = #{},
              is_bridge           = false,
              connected           = false,
              topic_alias_maximum = #{to_client => 0, from_client => 0},
              conn_mod            = maps:get(conn_mod, SocketOpts, undefined),
              credentials         = #{},
              ws_cookie           = maps:get(ws_cookie, SocketOpts, undefined)
             }.

init_username(Peercert, Options) ->
    case proplists:get_value(peer_cert_as_username, Options) of
        cn  -> esockd_peercert:common_name(Peercert);
        dn  -> esockd_peercert:subject(Peercert);
        crt -> Peercert;
        _   -> undefined
    end.

set_username(Username, PState = #protocol{username = undefined}) ->
    PState#protocol{username = Username};
set_username(_Username, PState) ->
    PState.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

info(PState = #protocol{zone          = Zone,
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

attrs(#protocol{zone         = Zone,
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
    #{zone => Zone,
      client_id => ClientId,
      username => Username,
      peername => Peername,
      peercert => Peercert,
      proto_ver => ProtoVer,
      proto_name => ProtoName,
      clean_start => CleanStart,
      keepalive => Keepalive,
      is_bridge => IsBridge,
      connected_at => ConnectedAt,
      conn_mod => ConnMod,
      credentials => Credentials
     }.

attr(proto_ver, #protocol{proto_ver = ProtoVer}) ->
    ProtoVer;
attr(max_inflight, #protocol{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Receive-Maximum', ConnProps, 65535);
attr(max_inflight, #protocol{zone = Zone}) ->
    emqx_zone:get_env(Zone, max_inflight, 65535);
attr(expiry_interval, #protocol{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Session-Expiry-Interval', ConnProps, 0);
attr(expiry_interval, #protocol{zone = Zone, clean_start = CleanStart}) ->
    case CleanStart of
        true -> 0;
        false -> emqx_zone:get_env(Zone, session_expiry_interval, 16#ffffffff)
    end;
attr(topic_alias_maximum, #protocol{proto_ver = ?MQTT_PROTO_V5, conn_props = ConnProps}) ->
    get_property('Topic-Alias-Maximum', ConnProps, 0);
attr(topic_alias_maximum, #protocol{zone = Zone}) ->
    emqx_zone:get_env(Zone, max_topic_alias, 0);
attr(Name, PState) ->
    Attrs = lists:zip(record_info(fields, protocol), tl(tuple_to_list(PState))),
    case lists:keyfind(Name, 1, Attrs) of
        {_, Value} -> Value;
        false -> undefined
    end.

caps(Name, PState) ->
    maps:get(Name, caps(PState)).

caps(#protocol{zone = Zone}) ->
    emqx_mqtt_caps:get_caps(Zone).

client_id(#protocol{client_id = ClientId}) ->
    ClientId.

credentials(#protocol{zone      = Zone,
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

session(#protocol{session = Session}) ->
    Session.

%%--------------------------------------------------------------------
%% Packet Received
%%--------------------------------------------------------------------

set_protover(?CONNECT_PACKET(#mqtt_packet_connect{proto_ver = ProtoVer}), PState) ->
    PState#protocol{proto_ver = ProtoVer};
set_protover(_Packet, PState) ->
    PState.

handle_in(?PACKET(Type), PState = #protocol{connected = false}) when Type =/= ?CONNECT ->
    {error, proto_not_connected, PState};

handle_in(?PACKET(?CONNECT), PState = #protocol{connected = true}) ->
    {error, proto_unexpected_connect, PState};

handle_in(Packet = ?PACKET(_Type), PState) ->
    PState1 = set_protover(Packet, PState),
    try emqx_packet:validate(Packet) of
        true ->
            case preprocess_properties(Packet, PState1) of
                {ok, Packet1, PState2} ->
                    process(Packet1, PState2);
                {error, ReasonCode} ->
                    handle_out({disconnect, ReasonCode}, PState1)
            end
    catch
        error:protocol_error ->
            handle_out({disconnect, ?RC_PROTOCOL_ERROR}, PState1);
        error:subscription_identifier_invalid ->
            handle_out({disconnect, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED}, PState1);
        error:topic_alias_invalid ->
            handle_out({disconnect, ?RC_TOPIC_ALIAS_INVALID}, PState1);
        error:topic_filters_invalid ->
            handle_out({disconnect, ?RC_TOPIC_FILTER_INVALID}, PState1);
        error:topic_name_invalid ->
            handle_out({disconnect, ?RC_TOPIC_FILTER_INVALID}, PState1);
        error:_Reason ->
            %% TODO: {error, Reason, PState1}
            handle_out({disconnect, ?RC_MALFORMED_PACKET}, PState1)
    end.

%%--------------------------------------------------------------------
%% Preprocess MQTT Properties
%%--------------------------------------------------------------------

preprocess_properties(Packet = #mqtt_packet{
                                   variable = #mqtt_packet_connect{
                                                  properties = #{'Topic-Alias-Maximum' := ToClient}
                                              }
                               },
                      PState = #protocol{topic_alias_maximum = TopicAliasMaximum}) ->
    {ok, Packet, PState#protocol{topic_alias_maximum = TopicAliasMaximum#{to_client => ToClient}}};

%% Subscription Identifier
preprocess_properties(Packet = #mqtt_packet{
                                  variable = Subscribe = #mqtt_packet_subscribe{
                                                            properties    = #{'Subscription-Identifier' := SubId},
                                                            topic_filters = TopicFilters
                                                           }
                                 },
                      PState = #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    TopicFilters1 = [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters],
    {ok, Packet#mqtt_packet{variable = Subscribe#mqtt_packet_subscribe{topic_filters = TopicFilters1}}, PState};

%% Topic Alias Mapping
preprocess_properties(#mqtt_packet{
                          variable = #mqtt_packet_publish{
                              properties = #{'Topic-Alias' := 0}}
                      },
                      PState) ->
    {error, ?RC_TOPIC_ALIAS_INVALID};

preprocess_properties(Packet = #mqtt_packet{
                                  variable = Publish = #mqtt_packet_publish{
                                                          topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}}
                                 },
                      PState = #protocol{proto_ver = ?MQTT_PROTO_V5,
                                       topic_aliases = Aliases,
                                       topic_alias_maximum = #{from_client := TopicAliasMaximum}}) ->
    case AliasId =< TopicAliasMaximum of
        true ->
            {ok, Packet#mqtt_packet{variable = Publish#mqtt_packet_publish{
                                                 topic_name = maps:get(AliasId, Aliases, <<>>)}}, PState};
        false ->
            {error, ?RC_TOPIC_ALIAS_INVALID}
    end;

preprocess_properties(Packet = #mqtt_packet{
                                   variable = #mqtt_packet_publish{
                                                  topic_name = Topic,
                                                  properties = #{'Topic-Alias' := AliasId}}
                               },
                      PState = #protocol{proto_ver = ?MQTT_PROTO_V5,
                                       topic_aliases = Aliases,
                                       topic_alias_maximum = #{from_client := TopicAliasMaximum}}) ->
    case AliasId =< TopicAliasMaximum of
        true ->
            {ok, Packet, PState#protocol{topic_aliases = maps:put(AliasId, Topic, Aliases)}};
        false ->
            {error, ?RC_TOPIC_ALIAS_INVALID}
    end;

preprocess_properties(Packet, PState) ->
    {ok, Packet, PState}.

%%--------------------------------------------------------------------
%% Process MQTT Packet
%%--------------------------------------------------------------------

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
                             PState#protocol{proto_ver    = ProtoVer,
                                           proto_name   = ProtoName,
                                           clean_start  = CleanStart,
                                           keepalive    = Keepalive,
                                           conn_props   = ConnProps,
                                           is_bridge    = IsBridge,
                                           connected_at = os:timestamp()})),

    NewClientId = PState0#protocol.client_id,

    emqx_logger:set_metadata_client_id(NewClientId),

    Credentials = credentials(PState0),
    PState1 = PState0#protocol{credentials = Credentials},
    connack(
      case check_connect(ConnPkt, PState1) of
          ok ->
              case emqx_access_control:authenticate(Credentials#{password => Password}) of
                  {ok, Credentials0} ->
                      PState3 = maybe_assign_client_id(PState1),
                      emqx_logger:set_metadata_client_id(PState3#protocol.client_id),
                      %% Open session
                      SessAttrs = #{will_msg => make_will_msg(ConnPkt)},
                      case try_open_session(SessAttrs, PState3) of
                          {ok, Session, SP} ->
                              PState4 = PState3#protocol{session = Session, connected = true,
                                                       credentials = keepsafety(Credentials0)},
                              ok = emqx_cm:register_channel(client_id(PState4)),
                              true = emqx_cm:set_conn_attrs(client_id(PState4), attrs(PState4)),
                              %% Start keepalive
                              start_keepalive(Keepalive, PState4),
                              %% Success
                              {?RC_SUCCESS, SP, PState4};
                          {error, Error} ->
                              ?LOG(error, "Failed to open session: ~p", [Error]),
                              {?RC_UNSPECIFIED_ERROR, PState1#protocol{credentials = Credentials0}}
                      end;
                  {error, Reason} ->
                      ?LOG(warning, "Client ~s (Username: '~s') login failed for ~p", [NewClientId, Username, Reason]),
                      {emqx_reason_codes:connack_error(Reason), PState1#protocol{credentials = Credentials}}
              end;
          {error, ReasonCode} ->
              {ReasonCode, PState1}
      end);

process(Packet = ?PUBLISH_PACKET(?QOS_0, Topic, _PacketId, _Payload), PState = #protocol{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos0 message to ~s for ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            %% TODO: ...
            AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
            do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState)
    end;

process(Packet = ?PUBLISH_PACKET(?QOS_1, Topic, PacketId, _Payload), PState = #protocol{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos1 message to ~s for ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            handle_out({puback, PacketId, ReasonCode}, PState)
    end;

process(Packet = ?PUBLISH_PACKET(?QOS_2, Topic, PacketId, _Payload), PState = #protocol{zone = Zone}) ->
    case check_publish(Packet, PState) of
        ok ->
            do_publish(Packet, PState);
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos2 message to ~s for ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode)]),
            handle_out({pubrec, PacketId, ReasonCode}, PState)
    end;

process(?PUBACK_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:puback(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _NotFound} ->
            {ok, PState} %% TODO: Fixme later
    end;

process(?PUBREC_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrec(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, ?PUBREL_PACKET(PacketId), PState#protocol{session = NSession}};
        {error, NotFound} ->
            {ok, ?PUBREL_PACKET(PacketId, NotFound), PState}
    end;

process(?PUBREL_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrel(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, ?PUBCOMP_PACKET(PacketId), PState#protocol{session = NSession}};
        {error, NotFound} ->
            {ok, ?PUBCOMP_PACKET(PacketId, NotFound), PState}
    end;

process(?PUBCOMP_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubcomp(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _NotFound} -> ok
            %% TODO: How to handle NotFound?
    end;

process(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
        PState = #protocol{zone = Zone, session = Session, credentials = Credentials}) ->
    case check_subscribe(parse_topic_filters(?SUBSCRIBE, raw_topic_filters(PState, RawTopicFilters)), PState) of
        {ok, TopicFilters} ->
            TopicFilters0 = emqx_hooks:run_fold('client.subscribe', [Credentials], TopicFilters),
            TopicFilters1 = emqx_mountpoint:mount(mountpoint(Credentials), TopicFilters0),
            {ok, ReasonCodes, NSession} = emqx_session:subscribe(TopicFilters1, Session),
            handle_out({suback, PacketId, ReasonCodes}, PState#protocol{session = NSession});
        {error, TopicFilters} ->
            {SubTopics, ReasonCodes} =
                lists:foldr(fun({Topic, #{rc := ?RC_SUCCESS}}, {Topics, Codes}) ->
                                    {[Topic|Topics], [?RC_IMPLEMENTATION_SPECIFIC_ERROR | Codes]};
                                ({Topic, #{rc := Code}}, {Topics, Codes}) ->
                                    {[Topic|Topics], [Code|Codes]}
                            end, {[], []}, TopicFilters),
            ?LOG(warning, "Cannot subscribe ~p for ~p",
                 [SubTopics, [emqx_reason_codes:text(R) || R <- ReasonCodes]]),
            handle_out({suback, PacketId, ReasonCodes}, PState)
    end;

process(?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
        PState = #protocol{session = Session, credentials = Credentials}) ->
    TopicFilters = emqx_hooks:run_fold('client.unsubscribe', [Credentials],
                                       parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters)),
    TopicFilters1 = emqx_mountpoint:mount(mountpoint(Credentials), TopicFilters),
    {ok, ReasonCodes, NSession} = emqx_session:unsubscribe(TopicFilters1, Session),
    handle_out({unsuback, PacketId, ReasonCodes}, PState#protocol{session = NSession});

process(?PACKET(?PINGREQ), PState) ->
    {ok, ?PACKET(?PINGRESP), PState};

process(?DISCONNECT_PACKET(?RC_SUCCESS, #{'Session-Expiry-Interval' := Interval}),
        PState = #protocol{session = Session, conn_props = #{'Session-Expiry-Interval' := OldInterval}}) ->
    case Interval =/= 0 andalso OldInterval =:= 0 of
        true ->
            handle_out({disconnect, ?RC_PROTOCOL_ERROR}, PState#protocol{will_msg = undefined});
        false ->
            %% TODO:
            %% emqx_session:update_expiry_interval(SPid, Interval),
            %% Clean willmsg
            {stop, normal, PState#protocol{will_msg = undefined}}
    end;

process(?DISCONNECT_PACKET(?RC_SUCCESS), PState) ->
    {stop, normal, PState#protocol{will_msg = undefined}};

process(?DISCONNECT_PACKET(_), PState) ->
    {stop, {shutdown, abnormal_disconnet}, PState};

process(?AUTH_PACKET(), State) ->
    %%TODO: implement later.
    {ok, State}.

%%--------------------------------------------------------------------
%% ConnAck --> Client
%%--------------------------------------------------------------------

connack({?RC_SUCCESS, SP, PState = #protocol{credentials = Credentials}}) ->
    ok = emqx_hooks:run('client.connected', [Credentials, ?RC_SUCCESS, attrs(PState)]),
    handle_out({connack, ?RC_SUCCESS, sp(SP)}, PState);

connack({ReasonCode, PState = #protocol{proto_ver = ProtoVer, credentials = Credentials}}) ->
    ok = emqx_hooks:run('client.connected', [Credentials, ReasonCode, attrs(PState)]),
    [ReasonCode1] = reason_codes_compat(connack, [ReasonCode], ProtoVer),
    handle_out({connack, ReasonCode1}, PState).

%%------------------------------------------------------------------------------
%% Publish Message -> Broker
%%------------------------------------------------------------------------------

do_publish(Packet = ?PUBLISH_PACKET(QoS, PacketId),
           PState = #protocol{session = Session, credentials = Credentials}) ->
    Msg = emqx_mountpoint:mount(mountpoint(Credentials),
                                emqx_packet:to_message(Credentials, Packet)),
    Msg1 = emqx_message:set_flag(dup, false, Msg),
    case emqx_session:publish(PacketId, Msg1, Session) of
        {ok, Results} ->
            puback(QoS, PacketId, Results, PState);
        {ok, Results, NSession} ->
            puback(QoS, PacketId, Results, PState#protocol{session = NSession});
        {error, Reason} ->
            puback(QoS, PacketId, {error, Reason}, PState)
    end.

%%------------------------------------------------------------------------------
%% Puback -> Client
%%------------------------------------------------------------------------------

puback(?QOS_0, _PacketId, _Result, PState) ->
    {ok, PState};
puback(?QOS_1, PacketId, {ok, []}, PState) ->
    handle_out({puback, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
%%TODO: calc the deliver count?
puback(?QOS_1, PacketId, {ok, _Result}, PState) ->
    handle_out({puback, PacketId, ?RC_SUCCESS}, PState);
puback(?QOS_1, PacketId, {error, ReasonCode}, PState) ->
    handle_out({puback, PacketId, ReasonCode}, PState);
puback(?QOS_2, PacketId, {ok, []}, PState) ->
    handle_out({pubrec, PacketId, ?RC_NO_MATCHING_SUBSCRIBERS}, PState);
puback(?QOS_2, PacketId, {ok, _Result}, PState) ->
    handle_out({pubrec, PacketId, ?RC_SUCCESS}, PState);
puback(?QOS_2, PacketId, {error, ReasonCode}, PState) ->
    handle_out({pubrec, PacketId, ReasonCode}, PState).

%%--------------------------------------------------------------------
%% Handle outgoing
%%--------------------------------------------------------------------

handle_out({connack, ?RC_SUCCESS, SP}, PState = #protocol{zone = Zone,
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

    PState1 = PState#protocol{topic_alias_maximum = TopicAliasMaximum#{from_client => MaxAlias}},

    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, Props3), PState1};

handle_out({connack, ?RC_SUCCESS, SP}, PState) ->
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP), PState};

handle_out({connack, ReasonCode}, PState = #protocol{proto_ver = ProtoVer}) ->
    Reason = emqx_reason_codes:name(ReasonCode, ProtoVer),
    {error, Reason, ?CONNACK_PACKET(ReasonCode), PState};

handle_out({puback, PacketId, ReasonCode}, PState) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), PState};
    %% TODO:
    %% AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
    %% do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState1);

handle_out({pubrel, PacketId}, PState) ->
    {ok, ?PUBREL_PACKET(PacketId), PState};

handle_out({pubrec, PacketId, ReasonCode}, PState) ->
    %% TODO:
    %% AclDenyAction = emqx_zone:get_env(Zone, acl_deny_action, ignore),
    %% do_acl_deny_action(AclDenyAction, Packet, ReasonCode, PState1);
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), PState};

%%handle_out({pubrec, PacketId, ReasonCode}, PState) ->
%%    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), PState};

handle_out({suback, PacketId, ReasonCodes}, PState = #protocol{proto_ver = ProtoVer}) ->
    %% TODO: ACL Deny
    {ok, ?SUBACK_PACKET(PacketId, reason_codes_compat(suback, ReasonCodes, ProtoVer)), PState};

handle_out({unsuback, PacketId, ReasonCodes}, PState = #protocol{proto_ver = ProtoVer}) ->
    {ok, ?UNSUBACK_PACKET(PacketId, reason_codes_compat(unsuback, ReasonCodes, ProtoVer)), PState};

%% Deliver a disconnect for mqtt 5.0
handle_out({disconnect, RC}, PState = #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    {error, emqx_reason_codes:name(RC), ?DISCONNECT_PACKET(RC), PState};

handle_out({disconnect, RC}, PState) ->
    {error, emqx_reason_codes:name(RC), PState}.

handle_timeout(Timer, Name, PState) ->
    {ok, PState}.

%%------------------------------------------------------------------------------
%% Maybe use username replace client id

maybe_use_username_as_clientid(ClientId, PState = #protocol{username = undefined}) ->
    PState#protocol{client_id = ClientId};
maybe_use_username_as_clientid(ClientId, PState = #protocol{username = Username, zone = Zone}) ->
    case emqx_zone:get_env(Zone, use_username_as_clientid, false) of
        true ->
            PState#protocol{client_id = Username};
        false ->
            PState#protocol{client_id = ClientId}
    end.

%%------------------------------------------------------------------------------
%% Assign a clientId

maybe_assign_client_id(PState = #protocol{client_id = <<>>, ack_props = AckProps}) ->
    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
    AckProps1 = set_property('Assigned-Client-Identifier', ClientId, AckProps),
    PState#protocol{client_id = ClientId, is_assigned = true, ack_props = AckProps1};
maybe_assign_client_id(PState) ->
    PState.

try_open_session(SessAttrs, PState = #protocol{zone = Zone,
                                             client_id = ClientId,
                                             username = Username,
                                             clean_start = CleanStart}) ->
    case emqx_cm:open_session(
           maps:merge(#{zone => Zone,
                        client_id => ClientId,
                        username => Username,
                        clean_start => CleanStart,
                        max_inflight => attr(max_inflight, PState),
                        expiry_interval => attr(expiry_interval, PState),
                        topic_alias_maximum => attr(topic_alias_maximum, PState)},
                      SessAttrs)) of
        {ok, Session} ->
            {ok, Session, false};
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

%%--------------------------------------------------------------------
%% Check Packet
%%--------------------------------------------------------------------

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

check_client_id(#mqtt_packet_connect{client_id = ClientId}, #protocol{zone = Zone}) ->
    Len = byte_size(ClientId),
    MaxLen = emqx_zone:get_env(Zone, max_clientid_len),
    case (1 =< Len) andalso (Len =< MaxLen) of
        true  -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

check_flapping(#mqtt_packet_connect{}, PState) ->
    do_flapping_detect(connect, PState).

check_banned(#mqtt_packet_connect{client_id = ClientId, username = Username},
             #protocol{zone = Zone, peername = Peername}) ->
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
check_will_retain(#mqtt_packet_connect{will_retain = true, proto_ver = ?MQTT_PROTO_V5}, #protocol{zone = Zone}) ->
    case emqx_zone:get_env(Zone, mqtt_retain_available, true) of
        true -> {error, ?RC_RETAIN_NOT_SUPPORTED};
        false -> ok
    end;
check_will_retain(_Packet, _PState) ->
    ok.

check_will_acl(#mqtt_packet_connect{will_topic = WillTopic},
               #protocol{zone = Zone, credentials = Credentials}) ->
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    case do_acl_check(EnableAcl, publish, Credentials, WillTopic) of
        ok -> ok;
        Other ->
            ?LOG(warning, "Cannot publish will message to ~p for acl denied", [WillTopic]),
            Other
    end.

check_publish(Packet, PState) ->
    run_check_steps([fun check_pub_caps/2,
                     fun check_pub_acl/2], Packet, PState).

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS, retain = Retain},
                            variable = #mqtt_packet_publish{properties = _Properties}},
               #protocol{zone = Zone}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).

check_pub_acl(_Packet, #protocol{credentials = #{is_superuser := IsSuper}})
        when IsSuper ->
    ok;
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #protocol{zone = Zone, credentials = Credentials}) ->
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

check_subscribe(TopicFilters, PState = #protocol{zone = Zone}) ->
    case emqx_mqtt_caps:check_sub(Zone, TopicFilters) of
        {ok, TopicFilter1} ->
            check_sub_acl(TopicFilter1, PState);
        {error, TopicFilter1} ->
            {error, TopicFilter1}
    end.

check_sub_acl(TopicFilters, #protocol{credentials = #{is_superuser := IsSuper}})
        when IsSuper ->
    {ok, TopicFilters};
check_sub_acl(TopicFilters, #protocol{zone = Zone, credentials = Credentials}) ->
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    lists:foldr(
      fun({Topic, SubOpts}, {Ok, Acc}) when EnableAcl ->
              AllowTerm = {Ok, [{Topic, SubOpts}|Acc]},
              DenyTerm = {error, [{Topic, SubOpts#{rc := ?RC_NOT_AUTHORIZED}}|Acc]},
              do_acl_check(subscribe, Credentials, Topic, AllowTerm, DenyTerm);
         (TopicFilter, Acc) ->
              {ok, [TopicFilter | Acc]}
      end, {ok, []}, TopicFilters).

terminate(_Reason, #protocol{client_id = undefined}) ->
    ok;
terminate(_Reason, PState = #protocol{connected = false}) ->
    do_flapping_detect(disconnect, PState),
    ok;
terminate(Reason, PState) when Reason =:= conflict;
                               Reason =:= discard ->
    do_flapping_detect(disconnect, PState),
    ok;

terminate(Reason, PState = #protocol{credentials = Credentials}) ->
    do_flapping_detect(disconnect, PState),
    ?LOG(info, "Shutdown for ~p", [Reason]),
    ok = emqx_hooks:run('client.disconnected', [Credentials, Reason]).

start_keepalive(0, _PState) ->
    ignore;
start_keepalive(Secs, #protocol{zone = Zone}) when Secs > 0 ->
    Backoff = emqx_zone:get_env(Zone, keepalive_backoff, 0.75),
    self() ! {keepalive, start, round(Secs * Backoff)}.

%%--------------------------------------------------------------------
%% Parse topic filters
%%--------------------------------------------------------------------

parse_topic_filters(?SUBSCRIBE, RawTopicFilters) ->
    [emqx_topic:parse(RawTopic, SubOpts) || {RawTopic, SubOpts} <- RawTopicFilters];

parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters) ->
    lists:map(fun emqx_topic:parse/1, RawTopicFilters).

sp(true)  -> 1;
sp(false) -> 0.

flag(false) -> 0;
flag(true)  -> 1.

%%--------------------------------------------------------------------
%% Execute actions in case acl deny

do_flapping_detect(Action, #protocol{zone = Zone,
                                   client_id = ClientId}) ->
    ok = case emqx_zone:get_env(Zone, enable_flapping_detect, false) of
             true ->
                 Threshold = emqx_zone:get_env(Zone, flapping_threshold, {10, 60}),
                 case emqx_flapping:check(Action, ClientId, Threshold) of
                     flapping ->
                         BanExpiryInterval = emqx_zone:get_env(Zone, flapping_banned_expiry_interval, 3600000),
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
                   ?RC_NOT_AUTHORIZED, PState = #protocol{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};

do_acl_deny_action(disconnect, ?PUBLISH_PACKET(QoS, _Topic, _PacketId, _Payload),
                   ?RC_NOT_AUTHORIZED, PState = #protocol{proto_ver = ProtoVer})
  when QoS =:= ?QOS_1; QoS =:= ?QOS_2 ->
    %% TODO:...
    %% deliver({disconnect, ?RC_NOT_AUTHORIZED}, PState),
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};

do_acl_deny_action(Action, ?SUBSCRIBE_PACKET(_PacketId, _Properties, _RawTopicFilters), ReasonCodes, PState)
  when is_list(ReasonCodes) ->
    traverse_reason_codes(ReasonCodes, Action, PState);
do_acl_deny_action(_OtherAction, _PubSubPacket, ?RC_NOT_AUTHORIZED, PState) ->
    {ok, PState};
do_acl_deny_action(_OtherAction, _PubSubPacket, ReasonCode, PState = #protocol{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(ReasonCode, ProtoVer), PState}.

traverse_reason_codes([], _Action, PState) ->
    {ok, PState};
traverse_reason_codes([?RC_SUCCESS | LeftReasonCodes], Action, PState) ->
    traverse_reason_codes(LeftReasonCodes, Action, PState);
traverse_reason_codes([?RC_NOT_AUTHORIZED | _LeftReasonCodes], disconnect, PState = #protocol{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(?RC_NOT_AUTHORIZED, ProtoVer), PState};
traverse_reason_codes([?RC_NOT_AUTHORIZED | LeftReasonCodes], Action, PState) ->
    traverse_reason_codes(LeftReasonCodes, Action, PState);
traverse_reason_codes([OtherCode | _LeftReasonCodes], _Action, PState =  #protocol{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(OtherCode, ProtoVer), PState}.

%% Reason code compat
reason_codes_compat(_PktType, ReasonCodes, ?MQTT_PROTO_V5) ->
    ReasonCodes;
reason_codes_compat(unsuback, _ReasonCodes, _ProtoVer) ->
    undefined;
reason_codes_compat(PktType, ReasonCodes, _ProtoVer) ->
    [emqx_reason_codes:compat(PktType, RC) || RC <- ReasonCodes].

raw_topic_filters(#protocol{zone = Zone, proto_ver = ProtoVer, is_bridge = IsBridge}, RawTopicFilters) ->
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

