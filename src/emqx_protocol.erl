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
        , info/2
        , attrs/1
        , caps/1
        ]).

%% for tests
-export([set/3]).

-export([ init/2
        , handle_in/2
        , handle_req/2
        , handle_deliver/2
        , handle_out/2
        , handle_timeout/3
        , terminate/2
        ]).

-import(emqx_access_control,
        [ authenticate/1
        , check_acl/3
        ]).

-export_type([proto_state/0]).

-record(protocol, {
          client        :: emqx_types:client(),
          session       :: emqx_session:session(),
          proto_name    :: binary(),
          proto_ver     :: emqx_types:ver(),
          keepalive     :: non_neg_integer(),
          will_msg      :: emqx_types:message(),
          topic_aliases :: maybe(map()),
          alias_maximum :: maybe(map()),
          ack_props     :: maybe(emqx_types:properties()) %% Tmp props
         }).

-opaque(proto_state() :: #protocol{}).

-define(NO_PROPS, undefined).

-spec(info(proto_state()) -> emqx_types:infos()).
info(#protocol{client     = Client,
               session    = Session,
               proto_name = ProtoName,
               proto_ver  = ProtoVer,
               keepalive  = Keepalive,
               will_msg   = WillMsg,
               topic_aliases = Aliases}) ->
    #{client => Client,
      session => session_info(Session),
      proto_name => ProtoName,
      proto_ver => ProtoVer,
      keepalive => Keepalive,
      will_msg => WillMsg,
      topic_aliases => Aliases
     }.

-spec(info(atom(), proto_state()) -> term()).
info(client, #protocol{client = Client}) ->
    Client;
info(zone, #protocol{client = #{zone := Zone}}) ->
    Zone;
info(client_id, #protocol{client = #{client_id := ClientId}}) ->
    ClientId;
info(session, #protocol{session = Session}) ->
    Session;
info(proto_name, #protocol{proto_name = ProtoName}) ->
    ProtoName;
info(proto_ver, #protocol{proto_ver = ProtoVer}) ->
    ProtoVer;
info(keepalive, #protocol{keepalive = Keepalive}) ->
    Keepalive;
info(will_msg, #protocol{will_msg = WillMsg}) ->
    WillMsg;
info(topic_aliases, #protocol{topic_aliases = Aliases}) ->
    Aliases.

%% For tests
set(client, Client, PState) ->
    PState#protocol{client = Client};
set(session, Session, PState) ->
    PState#protocol{session = Session}.

attrs(#protocol{client     = Client,
                session    = Session,
                proto_name = ProtoName,
                proto_ver  = ProtoVer,
                keepalive  = Keepalive}) ->
    #{client     => Client,
      session    => emqx_session:attrs(Session),
      proto_name => ProtoName,
      proto_ver  => ProtoVer,
      keepalive  => Keepalive
     }.

caps(#protocol{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:get_caps(Zone).


-spec(init(emqx_types:conn(), proplists:proplist()) -> proto_state()).
init(ConnInfo, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, undefined),
    Username = case peer_cert_as_username(Options) of
                   cn  -> esockd_peercert:common_name(Peercert);
                   dn  -> esockd_peercert:subject(Peercert);
                   crt -> Peercert;
                   _   -> undefined
               end,
    MountPoint = emqx_zone:get_env(Zone, mountpoint),
    Client = maps:merge(#{zone         => Zone,
                          username     => Username,
                          client_id    => <<>>,
                          mountpoint   => MountPoint,
                          is_bridge    => false,
                          is_superuser => false
                         }, ConnInfo),
    #protocol{client     = Client,
              proto_name = <<"MQTT">>,
              proto_ver  = ?MQTT_PROTO_V4
             }.

peer_cert_as_username(Options) ->
    proplists:get_value(peer_cert_as_username, Options).

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(emqx_types:packet(), proto_state())
      -> {ok, proto_state()}
       | {ok, emqx_types:packet(), proto_state()}
       | {ok, list(emqx_types:packet()), proto_state()}
       | {error, Reason :: term(), proto_state()}
       | {stop, Error :: atom(), proto_state()}).
handle_in(?CONNECT_PACKET(
             #mqtt_packet_connect{proto_name = ProtoName,
                                  proto_ver  = ProtoVer,
                                  keepalive  = Keepalive,
                                  client_id  = ClientId
                                 } = ConnPkt), PState) ->
    PState1 = PState#protocol{proto_name = ProtoName,
                              proto_ver  = ProtoVer,
                              keepalive  = Keepalive
                             },
    ok = emqx_logger:set_metadata_client_id(ClientId),
    case pipeline([fun validate_in/2,
                   fun process_props/2,
                   fun check_connect/2,
                   fun enrich_client/2,
                   fun auth_connect/2], ConnPkt, PState1) of
        {ok, NConnPkt, NPState = #protocol{client = #{client_id := ClientId1}}} ->
            ok = emqx_logger:set_metadata_client_id(ClientId1),
            process_connect(NConnPkt, NPState);
        {error, ReasonCode, NPState} ->
            handle_out({connack, ReasonCode}, NPState)
    end;

handle_in(Packet = ?PUBLISH_PACKET(_QoS, Topic, _PacketId), PState= #protocol{proto_ver = Ver}) ->
    case pipeline([fun validate_in/2,
                   fun process_alias/2,
                   fun check_publish/2], Packet, PState) of
        {ok, NPacket, NPState} ->
            process_publish(NPacket, NPState);
        {error, ReasonCode, NPState} ->
            ?LOG(warning, "Cannot publish message to ~s due to ~s",
                 [Topic, emqx_reason_codes:text(ReasonCode, Ver)]),
            handle_out({disconnect, ReasonCode}, NPState)
    end;

handle_in(?PUBACK_PACKET(PacketId, _ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:puback(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, PState#protocol{session = NSession});
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _NotFound} ->
            {ok, PState}
    end;

handle_in(?PUBREC_PACKET(PacketId, _ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrec(PacketId, Session) of
        {ok, NSession} ->
            handle_out({pubrel, PacketId}, PState#protocol{session = NSession});
        {error, ReasonCode1} ->
            handle_out({pubrel, PacketId, ReasonCode1}, PState)
    end;

handle_in(?PUBREL_PACKET(PacketId, _ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrel(PacketId, Session) of
        {ok, NSession} ->
            handle_out({pubcomp, PacketId}, PState#protocol{session = NSession});
        {error, ReasonCode1} ->
            handle_out({pubcomp, PacketId, ReasonCode1}, PState)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, _ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubcomp(PacketId, Session) of
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, PState#protocol{session = NSession});
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _NotFound} ->
            {ok, PState}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          PState = #protocol{client = Client}) ->
    case validate_in(Packet, PState) of
        ok -> TopicFilters1 = [emqx_topic:parse(TopicFilter, SubOpts)
                               || {TopicFilter, SubOpts} <- TopicFilters],
              TopicFilters2 = emqx_hooks:run_fold('client.subscribe',
                                                  [Client, Properties],
                                                  TopicFilters1),
              TopicFilters3 = enrich_subid(Properties, TopicFilters2),
              {ReasonCodes, NPState} = process_subscribe(TopicFilters3, PState),
              handle_out({suback, PacketId, ReasonCodes}, NPState);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, PState)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          PState = #protocol{client = Client}) ->
    case validate_in(Packet, PState) of
        ok -> TopicFilters1 = lists:map(fun emqx_topic:parse/1, TopicFilters),
              TopicFilters2 = emqx_hooks:run_fold('client.unsubscribe',
                                                  [Client, Properties],
                                                  TopicFilters1),
              {ReasonCodes, NPState} = process_unsubscribe(TopicFilters2, PState),
              handle_out({unsuback, PacketId, ReasonCodes}, NPState);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, PState)
    end;

handle_in(?PACKET(?PINGREQ), PState) ->
    {ok, ?PACKET(?PINGRESP), PState};

handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), PState) ->
    %% Clear will msg
    {stop, normal, PState#protocol{will_msg = undefined}};

handle_in(?DISCONNECT_PACKET(RC), PState = #protocol{proto_ver = Ver}) ->
    {stop, {shutdown, emqx_reason_codes:name(RC, Ver)}, PState};

handle_in(?AUTH_PACKET(), PState) ->
    %%TODO: implement later.
    {ok, PState};

handle_in(Packet, PState) ->
    io:format("In: ~p~n", [Packet]),
    {ok, PState}.

%%--------------------------------------------------------------------
%% Handle internal request
%%--------------------------------------------------------------------

-spec(handle_req(Req:: term(), proto_state())
      -> {ok, Result :: term(), proto_state()} |
         {error, Reason :: term(), proto_state()}).
handle_req({subscribe, TopicFilters}, PState = #protocol{client = Client}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.subscribe',
                                        [Client, #{'Internal' => true}],
                                        parse(subscribe, TopicFilters)),
    {ReasonCodes, NPState} = process_subscribe(TopicFilters1, PState),
    {ok, ReasonCodes, NPState};

handle_req({unsubscribe, TopicFilters}, PState = #protocol{client = Client}) ->
    TopicFilters1 = emqx_hooks:run_fold('client.unsubscribe',
                                        [Client, #{'Internal' => true}],
                                        parse(unsubscribe, TopicFilters)),
    {ReasonCodes, NPState} = process_unsubscribe(TopicFilters1, PState),
    {ok, ReasonCodes, NPState};

handle_req(Req, PState) ->
    ?LOG(error, "Unexpected request: ~p~n", [Req]),
    {ok, ignored, PState}.

%%--------------------------------------------------------------------
%% Handle delivers
%%--------------------------------------------------------------------

handle_deliver(Delivers, PState = #protocol{session = Session})
  when is_list(Delivers) ->
    case emqx_session:deliver(Delivers, Session) of
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, PState#protocol{session = NSession});
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

handle_out({connack, ?RC_SUCCESS, SP},
           PState = #protocol{client = Client = #{zone := Zone},
                              ack_props = AckProps,
                              alias_maximum = AliasMaximum}) ->
    ok = emqx_hooks:run('client.connected', [Client, ?RC_SUCCESS, attrs(PState)]),
    #{max_packet_size := MaxPktSize,
      max_qos_allowed := MaxQoS,
      retain_available := Retain,
      max_topic_alias := MaxAlias,
      shared_subscription := Shared,
      wildcard_subscription := Wildcard
     } = caps(PState),
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
    AckProps1 = if AckProps == undefined -> #{}; true -> AckProps end,
    AckProps2 = AckProps1#{'Retain-Available' => flag(Retain),
                           'Maximum-Packet-Size' => MaxPktSize,
                           'Topic-Alias-Maximum' => MaxAlias,
                           'Wildcard-Subscription-Available' => flag(Wildcard),
                           'Subscription-Identifier-Available' => 1,
                           %'Response-Information' =>
                           'Shared-Subscription-Available' => flag(Shared),
                           'Maximum-QoS' => MaxQoS
                          },
    AckProps3 = case emqx_zone:get_env(Zone, server_keepalive) of
                    undefined -> AckProps2;
                    Keepalive -> AckProps2#{'Server-Keep-Alive' => Keepalive}
                end,
    AliasMaximum1 = set_property(inbound, MaxAlias, AliasMaximum),
    PState1 = PState#protocol{alias_maximum = AliasMaximum1,
                              ack_props = undefined
                             },
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, AckProps3), PState1};

handle_out({connack, ReasonCode}, PState = #protocol{client = Client,
                                                     proto_ver = ProtoVer}) ->
    ok = emqx_hooks:run('client.connected', [Client, ReasonCode, attrs(PState)]),
    ReasonCode1 = if
                      ProtoVer == ?MQTT_PROTO_V5 -> ReasonCode;
                      true -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    {error, Reason, ?CONNACK_PACKET(ReasonCode1), PState};

handle_out({publish, Publishes}, PState) ->
    Packets = [element(2, handle_out(Publish, PState)) || Publish <- Publishes],
    {ok, Packets, PState};

handle_out({publish, PacketId, Msg}, PState = #protocol{client = Client}) ->
    Msg1 = emqx_hooks:run_fold('message.deliver', [Client],
                               emqx_message:update_expiry(Msg)),
    Packet = emqx_packet:from_message(PacketId, unmount(Client, Msg1)),
    {ok, Packet, PState};

handle_out({puback, PacketId, ReasonCode}, PState) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), PState};

handle_out({pubrel, PacketId}, PState) ->
    {ok, ?PUBREL_PACKET(PacketId), PState};
handle_out({pubrel, PacketId, ReasonCode}, PState) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), PState};

handle_out({pubrec, PacketId, ReasonCode}, PState) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), PState};

handle_out({pubcomp, PacketId}, PState) ->
    {ok, ?PUBCOMP_PACKET(PacketId), PState};
handle_out({pubcomp, PacketId, ReasonCode}, PState) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), PState};

handle_out({suback, PacketId, ReasonCodes}, PState = #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    %% TODO: ACL Deny
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes), PState};
handle_out({suback, PacketId, ReasonCodes}, PState) ->
    %% TODO: ACL Deny
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes1), PState};

handle_out({unsuback, PacketId, ReasonCodes}, PState = #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    {ok, ?UNSUBACK_PACKET(PacketId, ReasonCodes), PState};
%% Ignore reason codes if not MQTT5
handle_out({unsuback, PacketId, _ReasonCodes}, PState) ->
    {ok, ?UNSUBACK_PACKET(PacketId), PState};

handle_out({disconnect, ReasonCode}, PState = #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    Reason = emqx_reason_codes:name(ReasonCode),
    {error, Reason, ?DISCONNECT_PACKET(ReasonCode), PState};

handle_out({disconnect, ReasonCode}, PState = #protocol{proto_ver = ProtoVer}) ->
    {error, emqx_reason_codes:name(ReasonCode, ProtoVer), PState};

handle_out(Packet, PState) ->
    ?LOG(error, "Unexpected out:~p", [Packet]),
    {ok, PState}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(TRef, Msg, PState = #protocol{session = Session}) ->
    case emqx_session:timeout(TRef, Msg, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {ok, Publishes, NSession} ->
            handle_out({publish, Publishes}, PState#protocol{session = NSession})
    end.

terminate(normal, #protocol{client = Client}) ->
    ok = emqx_hooks:run('client.disconnected', [Client, normal]);
terminate(Reason, #protocol{client = Client, will_msg = WillMsg}) ->
    ok = emqx_hooks:run('client.disconnected', [Client, Reason]),
    publish_will_msg(WillMsg).

publish_will_msg(undefined) ->
    ok;
publish_will_msg(Msg) ->
    emqx_broker:publish(Msg).

%%--------------------------------------------------------------------
%% Validate incoming packet
%%--------------------------------------------------------------------

-spec(validate_in(emqx_types:packet(), proto_state())
      -> ok | {error, emqx_types:reason_code()}).
validate_in(Packet, _PState) ->
    try emqx_packet:validate(Packet) of
        true -> ok
    catch
        error:protocol_error ->
            {error, ?RC_PROTOCOL_ERROR};
        error:subscription_identifier_invalid ->
            {error, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED};
        error:topic_alias_invalid ->
            {error, ?RC_TOPIC_ALIAS_INVALID};
        error:topic_filters_invalid ->
            {error, ?RC_TOPIC_FILTER_INVALID};
        error:topic_name_invalid ->
            {error, ?RC_TOPIC_FILTER_INVALID};
        error:_Reason ->
            {error, ?RC_MALFORMED_PACKET}
    end.

%%--------------------------------------------------------------------
%% Preprocess properties
%%--------------------------------------------------------------------

process_props(#mqtt_packet_connect{
                 properties = #{'Topic-Alias-Maximum' := Max}
                },
              PState = #protocol{alias_maximum = AliasMaximum}) ->
    NAliasMaximum = if AliasMaximum == undefined ->
                           #{outbound => Max};
                       true -> AliasMaximum#{outbound => Max}
                    end,
    {ok, PState#protocol{alias_maximum = NAliasMaximum}};

process_props(Packet, PState) ->
    {ok, Packet, PState}.

%%--------------------------------------------------------------------
%% Check Connect Packet
%%--------------------------------------------------------------------

check_connect(ConnPkt, PState) ->
    case pipeline([fun check_proto_ver/2,
                   fun check_client_id/2,
                   %%fun check_flapping/2,
                   fun check_banned/2,
                   fun check_will_topic/2,
                   fun check_will_retain/2], ConnPkt, PState) of
        {ok, NConnPkt, NPState} -> {ok, NConnPkt, NPState};
        Error -> Error
    end.

check_proto_ver(#mqtt_packet_connect{proto_ver  = Ver,
                                     proto_name = Name}, _PState) ->
    case lists:member({Ver, Name}, ?PROTOCOL_NAMES) of
        true  -> ok;
        false -> {error, ?RC_UNSUPPORTED_PROTOCOL_VERSION}
    end.

%% MQTT3.1 does not allow null clientId
check_client_id(#mqtt_packet_connect{proto_ver = ?MQTT_PROTO_V3,
                                     client_id = <<>>
                                    }, _PState) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

%% Issue#599: Null clientId and clean_start = false
check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = false}, _PState) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};

check_client_id(#mqtt_packet_connect{client_id   = <<>>,
                                     clean_start = true}, _PState) ->
    ok;

check_client_id(#mqtt_packet_connect{client_id = ClientId},
                #protocol{client = #{zone := Zone}}) ->
    Len = byte_size(ClientId),
    MaxLen = emqx_zone:get_env(Zone, max_clientid_len),
    case (1 =< Len) andalso (Len =< MaxLen) of
        true  -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

%%TODO: check banned...
check_banned(#mqtt_packet_connect{client_id = ClientId,
                                  username = Username},
             #protocol{client = Client = #{zone := Zone}}) ->
    case emqx_zone:get_env(Zone, enable_ban, false) of
        true ->
            case emqx_banned:check(Client#{client_id => ClientId,
                                           username  => Username}) of
                true  -> {error, ?RC_BANNED};
                false -> ok
            end;
        false -> ok
    end.

check_will_topic(#mqtt_packet_connect{will_flag = false}, _PState) ->
    ok;
check_will_topic(#mqtt_packet_connect{will_topic = WillTopic}, _PState) ->
    try emqx_topic:validate(WillTopic) of
        true -> ok
    catch error:_Error ->
        {error, ?RC_TOPIC_NAME_INVALID}
    end.

check_will_retain(#mqtt_packet_connect{will_retain = false}, _PState) ->
    ok;
check_will_retain(#mqtt_packet_connect{will_retain = true},
                  #protocol{client = #{zone := Zone}}) ->
    case emqx_zone:get_env(Zone, mqtt_retain_available, true) of
        true  -> ok;
        false -> {error, ?RC_RETAIN_NOT_SUPPORTED}
    end.

%%--------------------------------------------------------------------
%% Enrich client
%%--------------------------------------------------------------------

enrich_client(ConnPkt, PState) ->
    case pipeline([fun set_username/2,
                   fun maybe_use_username_as_clientid/2,
                   fun maybe_assign_clientid/2,
                   fun set_rest_client_fields/2], ConnPkt, PState) of
        {ok, NConnPkt, NPState} -> {ok, NConnPkt, NPState};
        Error -> Error
    end.

maybe_use_username_as_clientid(_ConnPkt, PState = #protocol{client = #{username := undefined}}) ->
    {ok, PState};
maybe_use_username_as_clientid(_ConnPkt, PState = #protocol{client = Client = #{zone := Zone,
                                                                                username := Username}}) ->
    NClient = 
        case emqx_zone:get_env(Zone, use_username_as_clientid, false) of
            true -> Client#{client_id => Username};
            false -> Client
        end,
    {ok, PState#protocol{client = NClient}}.

maybe_assign_clientid(#mqtt_packet_connect{client_id = <<>>},
             PState = #protocol{client = Client, ack_props = AckProps}) ->
    ClientId = emqx_guid:to_base62(emqx_guid:gen()),
    AckProps1 = set_property('Assigned-Client-Identifier', ClientId, AckProps),
    {ok, PState#protocol{client = Client#{client_id => ClientId}, ack_props = AckProps1}};
maybe_assign_clientid(#mqtt_packet_connect{client_id = ClientId}, PState = #protocol{client = Client}) ->
    {ok, PState#protocol{client = Client#{client_id => ClientId}}}.

%% Username maybe not undefined if peer_cert_as_username
set_username(#mqtt_packet_connect{username = Username},
             PState = #protocol{client = Client = #{username := undefined}}) ->
    {ok, PState#protocol{client = Client#{username => Username}}};
set_username(_ConnPkt, PState) -> 
    {ok, PState}.

set_rest_client_fields(#mqtt_packet_connect{is_bridge  = IsBridge}, PState = #protocol{client = Client}) ->
    {ok, PState#protocol{client = Client#{is_bridge => IsBridge}}}.

%%--------------------------------------------------------------------
%% Auth Connect
%%--------------------------------------------------------------------

auth_connect(#mqtt_packet_connect{client_id = ClientId,
                                  username  = Username,
                                  password  = Password},
             PState = #protocol{client = Client}) ->
    case authenticate(Client#{password => Password}) of
        {ok, AuthResult} ->
            {ok, PState#protocol{client = maps:merge(Client, AuthResult)}};
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~0p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason)}
    end.

%%--------------------------------------------------------------------
%% Process Connect
%%--------------------------------------------------------------------

process_connect(ConnPkt, PState) ->
    case open_session(ConnPkt, PState) of
        {ok, Session, SP} ->
            WillMsg = emqx_packet:will_msg(ConnPkt),
            NPState = PState#protocol{session = Session,
                                      will_msg = WillMsg
                                     },
            handle_out({connack, ?RC_SUCCESS, sp(SP)}, NPState);
        {error, Reason} ->
            %% TODO: Unknown error?
            ?LOG(error, "Failed to open session: ~p", [Reason]),
            handle_out({connack, ?RC_UNSPECIFIED_ERROR}, PState)
    end.

%%--------------------------------------------------------------------
%% Open session
%%--------------------------------------------------------------------

open_session(#mqtt_packet_connect{clean_start = CleanStart,
                                  properties  = ConnProps},
             #protocol{client = Client = #{zone := Zone}}) ->
    MaxInflight = get_property('Receive-Maximum', ConnProps,
                               emqx_zone:get_env(Zone, max_inflight, 65535)),
    Interval = get_property('Session-Expiry-Interval', ConnProps,
                            emqx_zone:get_env(Zone, session_expiry_interval, 0)),
    emqx_cm:open_session(CleanStart, Client, #{max_inflight => MaxInflight,
                                               expiry_interval => Interval
                                              }).

%%--------------------------------------------------------------------
%% Process publish message: Client -> Broker
%%--------------------------------------------------------------------

process_alias(Packet = #mqtt_packet{
                          variable = #mqtt_packet_publish{topic_name = <<>>,
                                                          properties = #{'Topic-Alias' := AliasId}
                                                         } = Publish
                         }, PState = #protocol{topic_aliases = Aliases}) ->
    case find_alias(AliasId, Aliases) of
        {ok, Topic} ->
            {ok, Packet#mqtt_packet{
                   variable = Publish#mqtt_packet_publish{
                                topic_name = Topic}}, PState};
        false -> {error, ?RC_PROTOCOL_ERROR}
    end;

process_alias(#mqtt_packet{
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 properties = #{'Topic-Alias' := AliasId}
                                                }
                }, PState = #protocol{topic_aliases = Aliases}) ->
    {ok, PState#protocol{topic_aliases = save_alias(AliasId, Topic, Aliases)}};

process_alias(_Packet, PState) ->
    {ok, PState}.

find_alias(_AliasId, undefined) ->
    false;
find_alias(AliasId, Aliases) ->
    maps:find(AliasId, Aliases).

save_alias(AliasId, Topic, undefined) ->
    #{AliasId => Topic};
save_alias(AliasId, Topic, Aliases) ->
    maps:put(AliasId, Topic, Aliases).

%% Check Publish
check_publish(Packet, PState) ->
    pipeline([fun check_pub_acl/2,
              fun check_pub_alias/2,
              fun check_pub_caps/2], Packet, PState).

%% Check Pub ACL
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #protocol{client = Client}) ->
    case is_acl_enabled(Client) andalso check_acl(Client, publish, Topic) of
        false -> ok;
        allow -> ok;
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%% Check Pub Alias
check_pub_alias(#mqtt_packet{
                   variable = #mqtt_packet_publish{
                                 properties = #{'Topic-Alias' := AliasId}
                                }
                  },
                #protocol{alias_maximum = Limits}) ->
    case (Limits == undefined)
            orelse (Max = maps:get(inbound, Limits, 0)) == 0
                orelse (AliasId > Max) of
        false -> ok;
        true  -> {error, ?RC_TOPIC_ALIAS_INVALID}
    end;
check_pub_alias(_Packet, _PState) -> ok.

%% Check Pub Caps
check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS,
                                                         retain = Retain
                                                        }
                           },
               #protocol{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).

%% Process Publish
process_publish(Packet = ?PUBLISH_PACKET(_QoS, _Topic, PacketId),
                PState = #protocol{client = Client}) ->
    Msg = emqx_packet:to_message(Client, Packet),
    %%TODO: Improve later.
    Msg1 = emqx_message:set_flag(dup, false, Msg),
    process_publish(PacketId, mount(Client, Msg1), PState).

process_publish(_PacketId, Msg = #message{qos = ?QOS_0}, PState) ->
    _ = emqx_broker:publish(Msg),
    {ok, PState};

process_publish(PacketId, Msg = #message{qos = ?QOS_1}, PState) ->
    Deliveries = emqx_broker:publish(Msg),
    ReasonCode = emqx_reason_codes:puback(Deliveries),
    handle_out({puback, PacketId, ReasonCode}, PState);

process_publish(PacketId, Msg = #message{qos = ?QOS_2},
                PState = #protocol{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Deliveries, NSession} ->
            ReasonCode = emqx_reason_codes:puback(Deliveries),
            handle_out({pubrec, PacketId, ReasonCode},
                       PState#protocol{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubrec, PacketId, ReasonCode}, PState)
    end.

%%--------------------------------------------------------------------
%% Process subscribe request
%%--------------------------------------------------------------------

process_subscribe(TopicFilters, PState) ->
    process_subscribe(TopicFilters, [], PState).

process_subscribe([], Acc, PState) ->
    {lists:reverse(Acc), PState};

process_subscribe([{TopicFilter, SubOpts}|More], Acc, PState) ->
    {RC, NPState} = do_subscribe(TopicFilter, SubOpts, PState),
    process_subscribe(More, [RC|Acc], NPState).

do_subscribe(TopicFilter, SubOpts = #{qos := QoS},
             PState = #protocol{client = Client, session = Session}) ->
    case check_subscribe(TopicFilter, SubOpts, PState) of
        ok -> TopicFilter1 = mount(Client, TopicFilter),
              SubOpts1 = enrich_subopts(maps:merge(?DEFAULT_SUBOPTS, SubOpts), PState),
              case emqx_session:subscribe(Client, TopicFilter1, SubOpts1, Session) of
                  {ok, NSession} ->
                      {QoS, PState#protocol{session = NSession}};
                  {error, RC} -> {RC, PState}
              end;
        {error, RC} -> {RC, PState}
    end.

enrich_subid(#{'Subscription-Identifier' := SubId}, TopicFilters) ->
    [{Topic, SubOpts#{subid => SubId}} || {Topic, SubOpts} <- TopicFilters];
enrich_subid(_Properties, TopicFilters) ->
    TopicFilters.

enrich_subopts(SubOpts, #protocol{proto_ver = ?MQTT_PROTO_V5}) ->
    SubOpts;
enrich_subopts(SubOpts, #protocol{client = #{zone := Zone, is_bridge := IsBridge}}) ->
    Rap = flag(IsBridge),
    Nl = flag(emqx_zone:get_env(Zone, ignore_loop_deliver, false)),
    SubOpts#{rap => Rap, nl => Nl}.

%% Check Sub
check_subscribe(TopicFilter, SubOpts, PState) ->
    case check_sub_acl(TopicFilter, PState) of
        allow -> check_sub_caps(TopicFilter, SubOpts, PState);
        deny  -> {error, ?RC_NOT_AUTHORIZED}
    end.

%% Check Sub ACL
check_sub_acl(TopicFilter, #protocol{client = Client}) ->
    case is_acl_enabled(Client) andalso
         check_acl(Client, subscribe, TopicFilter) of
        false  -> allow;
        Result -> Result
    end.

%% Check Sub Caps
check_sub_caps(TopicFilter, SubOpts, #protocol{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_sub(Zone, TopicFilter, SubOpts).

%%--------------------------------------------------------------------
%% Process unsubscribe request
%%--------------------------------------------------------------------

process_unsubscribe(TopicFilters, PState) ->
    process_unsubscribe(TopicFilters, [], PState).

process_unsubscribe([], Acc, PState) ->
    {lists:reverse(Acc), PState};

process_unsubscribe([{TopicFilter, SubOpts}|More], Acc, PState) ->
    {RC, PState1} = do_unsubscribe(TopicFilter, SubOpts, PState),
    process_unsubscribe(More, [RC|Acc], PState1).

do_unsubscribe(TopicFilter, _SubOpts, PState = #protocol{client = Client,
                                                         session = Session}) ->
    case emqx_session:unsubscribe(Client, mount(Client, TopicFilter), Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, PState#protocol{session = NSession}};
        {error, RC} -> {RC, PState}
    end.

%%--------------------------------------------------------------------
%% Is ACL enabled?
%%--------------------------------------------------------------------

is_acl_enabled(#{zone := Zone, is_superuser := IsSuperuser}) ->
    (not IsSuperuser) andalso emqx_zone:get_env(Zone, enable_acl, true).

%%--------------------------------------------------------------------
%% Parse topic filters
%%--------------------------------------------------------------------

parse(subscribe, TopicFilters) ->
    [emqx_topic:parse(TopicFilter, SubOpts) || {TopicFilter, SubOpts} <- TopicFilters];

parse(unsubscribe, TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Mount/Unmount
%%--------------------------------------------------------------------

mount(Client = #{mountpoint := MountPoint}, TopicOrMsg) ->
    emqx_mountpoint:mount(emqx_mountpoint:replvar(MountPoint, Client), TopicOrMsg).

unmount(Client = #{mountpoint := MountPoint}, TopicOrMsg) ->
    emqx_mountpoint:unmount(emqx_mountpoint:replvar(MountPoint, Client), TopicOrMsg).

%%--------------------------------------------------------------------
%% Pipeline
%%--------------------------------------------------------------------

pipeline([], Packet, PState) ->
    {ok, Packet, PState};

pipeline([Fun|More], Packet, PState) ->
    case Fun(Packet, PState) of
        ok -> pipeline(More, Packet, PState);
        {ok, NPState} ->
            pipeline(More, Packet, NPState);
        {ok, NPacket, NPState} ->
            pipeline(More, NPacket, NPState);
        {error, ReasonCode} ->
            {error, ReasonCode, PState};
        {error, ReasonCode, NPState} ->
            {error, ReasonCode, NPState}
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

set_property(Name, Value, ?NO_PROPS) ->
    #{Name => Value};
set_property(Name, Value, Props) ->
    Props#{Name => Value}.

get_property(_Name, undefined, Default) ->
    Default;
get_property(Name, Props, Default) ->
    maps:get(Name, Props, Default).

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

session_info(undefined) ->
    undefined;
session_info(Session) ->
    emqx_session:info(Session).
