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

%% MQTT Channel
-module(emqx_channel).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("logger.hrl").
-include("types.hrl").

-logger_header("[Channel]").

-export([ attrs/1 ]).

-export([ zone/1
        , client_id/1
        , conn_mod/1
        , endpoint/1
        , proto_ver/1
        , keepalive/1
        , session/1
        ]).

-export([ init/2
        , handle_in/2
        , handle_out/2
        , handle_timeout/3
        , terminate/2
        ]).

-export_type([channel/0]).

-record(channel, {
          conn_mod   :: maybe(module()),
          endpoint   :: emqx_endpoint:endpoint(),
          proto_name :: binary(),
          proto_ver  :: emqx_mqtt:version(),
          keepalive  :: non_neg_integer(),
          session    :: emqx_session:session(),
          will_msg   :: emqx_types:message(),
          enable_acl :: boolean(),
          is_bridge  :: boolean(),
          connected  :: boolean(),
          topic_aliases :: map(),
          alias_maximum :: map(),
          connected_at  :: erlang:timestamp()
         }).

-opaque(channel() :: #channel{}).

attrs(#channel{endpoint = Endpoint, session = Session}) ->
    maps:merge(emqx_endpoint:to_map(Endpoint),
               emqx_session:attrs(Session)).

zone(#channel{endpoint = Endpoint}) ->
    emqx_endpoint:zone(Endpoint).

-spec(client_id(channel()) -> emqx_types:client_id()).
client_id(#channel{endpoint = Endpoint}) ->
    emqx_endpoint:client_id(Endpoint).

-spec(conn_mod(channel()) -> module()).
conn_mod(#channel{conn_mod = ConnMod}) ->
    ConnMod.

-spec(endpoint(channel()) -> emqx_endpoint:endpoint()).
endpoint(#channel{endpoint = Endpoint}) ->
    Endpoint.

-spec(proto_ver(channel()) -> emqx_mqtt:version()).
proto_ver(#channel{proto_ver = ProtoVer}) ->
    ProtoVer.

keepalive(#channel{keepalive = Keepalive}) ->
    Keepalive.

-spec(session(channel()) -> emqx_session:session()).
session(#channel{session = Session}) ->
    Session.

-spec(init(map(), proplists:proplist()) -> channel()).
init(ConnInfo = #{peername := Peername,
                  sockname := Sockname,
                  conn_mod := ConnMod}, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, nossl),
    Username = peer_cert_as_username(Peercert, Options),
    Mountpoint = emqx_zone:get_env(Zone, mountpoint),
    WsCookie = maps:get(ws_cookie, ConnInfo, undefined),
    Endpoint = emqx_endpoint:new(#{zone       => Zone,
                                   peername   => Peername,
                                   sockname   => Sockname,
                                   username   => Username,
                                   peercert   => Peercert,
                                   mountpoint => Mountpoint,
                                   ws_cookie  => WsCookie
                                  }),
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    #channel{conn_mod   = ConnMod,
             endpoint   = Endpoint,
             enable_acl = EnableAcl,
             is_bridge  = false,
             connected  = false
            }.

peer_cert_as_username(Peercert, Options) ->
    case proplists:get_value(peer_cert_as_username, Options) of
        cn  -> esockd_peercert:common_name(Peercert);
        dn  -> esockd_peercert:subject(Peercert);
        crt -> Peercert;
        _   -> undefined
    end.

%%--------------------------------------------------------------------
%% Handle incoming packet
%%--------------------------------------------------------------------

-spec(handle_in(emqx_mqtt:packet(), channel())
      -> {ok, channel()}
       | {ok, emqx_mqtt:packet(), channel()}
       | {error, Reason :: term(), channel()}
       | {stop, Error :: atom(), channel()}).
handle_in(?CONNECT_PACKET(
             #mqtt_packet_connect{proto_name = ProtoName,
                                  proto_ver  = ProtoVer,
                                  is_bridge  = IsBridge,
                                  client_id  = ClientId,
                                  username   = Username,
                                  password   = Password,
                                  keepalive  = Keepalive} = ConnPkt),
          Channel = #channel{endpoint = Endpoint}) ->
    Endpoint1 = emqx_endpoint:update(#{client_id => ClientId,
                                       username  => Username,
                                       password  => Password
                                      }, Endpoint),
    emqx_logger:set_metadata_client_id(ClientId),
    WillMsg = emqx_packet:will_msg(ConnPkt),
    Channel1 = Channel#channel{endpoint   = Endpoint1,
                               proto_name = ProtoName,
                               proto_ver  = ProtoVer,
                               is_bridge  = IsBridge,
                               keepalive  = Keepalive,
                               will_msg   = WillMsg
                              },
    %% fun validate_packet/2,
    case pipeline([fun check_connect/2,
                   fun handle_connect/2], ConnPkt, Channel1) of
        {ok, SP, Channel2} ->
            handle_out({connack, ?RC_SUCCESS, sp(SP)}, Channel2);
        {error, ReasonCode} ->
            handle_out({connack, ReasonCode}, Channel1);
        {error, ReasonCode, Channel2} ->
            handle_out({connack, ReasonCode}, Channel2)
    end;

handle_in(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId), Channel) ->
    case pipeline([fun validate_packet/2,
                   fun check_pub_caps/2,
                   fun check_pub_acl/2,
                   fun handle_publish/2], Packet, Channel) of
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos~w message to ~s due to ~s",
                 [QoS, Topic, emqx_reason_codes:text(ReasonCode)]),
            handle_out(case QoS of
                           ?QOS_0 -> {puberr, ReasonCode};
                           ?QOS_1 -> {puback, PacketId, ReasonCode};
                           ?QOS_2 -> {pubrec, PacketId, ReasonCode}
                       end, Channel);
        Result -> Result
    end;

handle_in(?PUBACK_PACKET(PacketId, ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:puback(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {error, _NotFound} ->
            %% TODO: metrics? error msg?
            {ok, Channel}
    end;

handle_in(?PUBREC_PACKET(PacketId, ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrec(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            handle_out({pubrel, PacketId}, Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubrel, PacketId, ReasonCode}, Channel)
    end;

handle_in(?PUBREL_PACKET(PacketId, ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubrel(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            handle_out({pubcomp, PacketId}, Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubcomp, PacketId, ReasonCode}, Channel)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, ReasonCode), Channel = #channel{session = Session}) ->
    case emqx_session:pubcomp(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}};
        {error, _ReasonCode} ->
            %% TODO: How to handle the reason code?
            {ok, Channel}
    end;

handle_in(?SUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
          Channel = #channel{endpoint = Endpoint, session = Session}) ->
    case check_subscribe(parse_topic_filters(?SUBSCRIBE, RawTopicFilters), Channel) of
        {ok, TopicFilters} ->
            TopicFilters1 = preprocess_topic_filters(?SUBSCRIBE, Endpoint,
                                                     enrich_subopts(TopicFilters, Channel)),
            {ok, ReasonCodes, NSession} = emqx_session:subscribe(emqx_endpoint:to_map(Endpoint),
                                                                 TopicFilters1, Session),
            handle_out({suback, PacketId, ReasonCodes}, Channel#channel{session = NSession});
        {error, TopicFilters} ->
            {Topics, ReasonCodes} = lists:unzip([{Topic, RC} || {Topic, #{rc := RC}} <- TopicFilters]),
            ?LOG(warning, "Cannot subscribe ~p due to ~p",
                 [Topics, [emqx_reason_codes:text(R) || R <- ReasonCodes]]),
            %% Tell the client that all subscriptions has been failed.
            ReasonCodes1 = lists:map(fun(?RC_SUCCESS) ->
                                             ?RC_IMPLEMENTATION_SPECIFIC_ERROR;
                                        (RC) -> RC
                                     end, ReasonCodes),
            handle_out({suback, PacketId, ReasonCodes1}, Channel)
    end;

handle_in(?UNSUBSCRIBE_PACKET(PacketId, Properties, RawTopicFilters),
          Channel = #channel{endpoint = Endpoint, session = Session}) ->
    TopicFilters = preprocess_topic_filters(
                     ?UNSUBSCRIBE, Endpoint,
                     parse_topic_filters(?UNSUBSCRIBE, RawTopicFilters)),
    {ok, ReasonCodes, NSession} = emqx_session:unsubscribe(emqx_endpoint:to_map(Endpoint), TopicFilters, Session),
    handle_out({unsuback, PacketId, ReasonCodes}, Channel#channel{session = NSession});

handle_in(?PACKET(?PINGREQ), Channel) ->
    {ok, ?PACKET(?PINGRESP), Channel};

handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), Channel) ->
    %% Clear will msg
    {stop, normal, Channel#channel{will_msg = undefined}};

handle_in(?DISCONNECT_PACKET(RC), Channel = #channel{proto_ver = Ver}) ->
    %% TODO:
    %% {stop, {shutdown, abnormal_disconnet}, Channel};
    {stop, {shutdown, emqx_reason_codes:name(RC, Ver)}, Channel};

handle_in(?AUTH_PACKET(), Channel) ->
    %%TODO: implement later.
    {ok, Channel};

handle_in(Packet, Channel) ->
    io:format("In: ~p~n", [Packet]),
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

handle_out({connack, ?RC_SUCCESS, SP}, Channel = #channel{endpoint = Endpoint}) ->
    ok = emqx_hooks:run('client.connected',
                        [emqx_endpoint:to_map(Endpoint), ?RC_SUCCESS, attrs(Channel)]),
    Props = #{}, %% TODO: ...
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, Props), Channel};

handle_out({connack, ReasonCode}, Channel = #channel{endpoint = Endpoint,
                                                     proto_ver = ProtoVer}) ->
    ok = emqx_hooks:run('client.connected',
                        [emqx_endpoint:to_map(Endpoint), ReasonCode, attrs(Channel)]),
    ReasonCode1 = if
                      ProtoVer == ?MQTT_PROTO_V5 -> ReasonCode;
                      true -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    {error, Reason, ?CONNACK_PACKET(ReasonCode1), Channel};

handle_out(Delivers, Channel = #channel{endpoint = Endpoint, session = Session})
  when is_list(Delivers) ->
    case emqx_session:deliver([{Topic, Msg} || {deliver, Topic, Msg} <- Delivers], Session) of
        {ok, Publishes, NSession} ->
            Credentials = emqx_endpoint:credentials(Endpoint),
            Packets = lists:map(fun({publish, PacketId, Msg}) ->
                                        Msg0 = emqx_hooks:run_fold('message.deliver', [Credentials], Msg),
                                        Msg1 = emqx_message:update_expiry(Msg0),
                                        Msg2 = emqx_mountpoint:unmount(emqx_endpoint:mountpoint(Endpoint), Msg1),
                                        emqx_packet:from_message(PacketId, Msg2)
                                end, Publishes),
            {ok, Packets, Channel#channel{session = NSession}};
        {ok, NSession} ->
            {ok, Channel#channel{session = NSession}}
    end;

handle_out({publish, PacketId, Msg}, Channel = #channel{endpoint = Endpoint}) ->
    Credentials = emqx_endpoint:credentials(Endpoint),
    Msg0 = emqx_hooks:run_fold('message.deliver', [Credentials], Msg),
    Msg1 = emqx_message:update_expiry(Msg0),
    Msg2 = emqx_mountpoint:unmount(
             emqx_endpoint:mountpoint(Credentials), Msg1),
    {ok, emqx_packet:from_message(PacketId, Msg2), Channel};

handle_out({puberr, ReasonCode}, Channel) ->
    {ok, Channel};

handle_out({puback, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBACK_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubrel, PacketId}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId), Channel};
handle_out({pubrel, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREL_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubrec, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBREC_PACKET(PacketId, ReasonCode), Channel};

handle_out({pubcomp, PacketId}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId), Channel};
handle_out({pubcomp, PacketId, ReasonCode}, Channel) ->
    {ok, ?PUBCOMP_PACKET(PacketId, ReasonCode), Channel};

handle_out({suback, PacketId, ReasonCodes}, Channel = #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    %% TODO: ACL Deny
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes), Channel};
handle_out({suback, PacketId, ReasonCodes}, Channel) ->
    %% TODO: ACL Deny
    ReasonCodes1 = [emqx_reason_codes:compat(suback, RC) || RC <- ReasonCodes],
    {ok, ?SUBACK_PACKET(PacketId, ReasonCodes1), Channel};

handle_out({unsuback, PacketId, ReasonCodes}, Channel = #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    {ok, ?UNSUBACK_PACKET(PacketId, ReasonCodes), Channel};
%% Ignore reason codes if not MQTT5
handle_out({unsuback, PacketId, _ReasonCodes}, Channel) ->
    {ok, ?UNSUBACK_PACKET(PacketId), Channel};

handle_out(Packet, State) ->
    io:format("Out: ~p~n", [Packet]),
    {ok, State}.

handle_deliver(Msg, State) ->
    io:format("Msg: ~p~n", [Msg]),
    %% Msg -> Pub
    {ok, State}.

handle_timeout(Name, TRef, State) ->
    io:format("Timeout: ~s ~p~n", [Name, TRef]),
    {ok, State}.

terminate(Reason, _State) ->
    %%io:format("Terminated for ~p~n", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% Check Connect Packet
%%--------------------------------------------------------------------

check_connect(_ConnPkt, Channel) ->
    {ok, Channel}.

%%--------------------------------------------------------------------
%% Handle Connect Packet
%%--------------------------------------------------------------------

handle_connect(#mqtt_packet_connect{proto_name  = ProtoName,
                                    proto_ver   = ProtoVer,
                                    is_bridge   = IsBridge,
                                    clean_start = CleanStart,
                                    keepalive   = Keepalive,
                                    properties  = ConnProps,
                                    client_id   = ClientId,
                                    username    = Username,
                                    password    = Password} = ConnPkt,
               Channel = #channel{endpoint = Endpoint}) ->
    Credentials = emqx_endpoint:credentials(Endpoint),
    case emqx_access_control:authenticate(
           Credentials#{password => Password}) of
        {ok, Credentials1} ->
            Endpoint1 = emqx_endpoint:update(Credentials1, Endpoint),
            %% Open session
            case open_session(ConnPkt, Channel) of
                {ok, Session, SP} ->
                    Channel1 = Channel#channel{endpoint = Endpoint1,
                                               session = Session,
                                               connected = true,
                                               connected_at = os:timestamp()
                                              },
                    ok = emqx_cm:register_channel(ClientId),
                    {ok, SP, Channel1};
                {error, Error} ->
                    ?LOG(error, "Failed to open session: ~p", [Error]),
                    {error, ?RC_UNSPECIFIED_ERROR, Channel#channel{endpoint = Endpoint1}}
            end;
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason), Channel}
    end.

open_session(#mqtt_packet_connect{clean_start = CleanStart,
                                  %%properties  = ConnProps,
                                  client_id   = ClientId,
                                  username    = Username} = ConnPkt,
             Channel = #channel{endpoint = Endpoint}) ->
    emqx_cm:open_session(maps:merge(emqx_endpoint:to_map(Endpoint),
                                    #{clean_start => CleanStart,
                                      max_inflight => 0,
                                      expiry_interval => 0})).

%%--------------------------------------------------------------------
%% Handle Publish Message: Client -> Broker
%%--------------------------------------------------------------------

handle_publish(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId),
               Channel = #channel{endpoint = Endpoint}) ->
    Credentials = emqx_endpoint:credentials(Endpoint),
    %% TODO: ugly... publish_to_msg(...)
    Msg = emqx_packet:to_message(Credentials, Packet),
    Msg1 = emqx_mountpoint:mount(
             emqx_endpoint:mountpoint(Endpoint), Msg),
    Msg2 = emqx_message:set_flag(dup, false, Msg1),
    handle_publish(PacketId, Msg2, Channel).

handle_publish(_PacketId, Msg = #message{qos = ?QOS_0}, Channel) ->
    _ = emqx_broker:publish(Msg),
    {ok, Channel};

handle_publish(PacketId, Msg = #message{qos = ?QOS_1}, Channel) ->
    Results = emqx_broker:publish(Msg),
    ReasonCode = emqx_reason_codes:puback(Results),
    handle_out({puback, PacketId, ReasonCode}, Channel);

handle_publish(PacketId, Msg = #message{qos = ?QOS_2},
               Channel = #channel{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Results, NSession} ->
            ReasonCode = emqx_reason_codes:puback(Results),
            handle_out({pubrec, PacketId, ReasonCode},
                       Channel#channel{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubrec, PacketId, ReasonCode}, Channel)
      end.

%%--------------------------------------------------------------------
%% Validate Incoming Packet
%%--------------------------------------------------------------------

-spec(validate_packet(emqx_mqtt:packet(), channel()) -> ok).
validate_packet(Packet, _Channel) ->
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
%% Preprocess MQTT Properties
%%--------------------------------------------------------------------

%% TODO:...

%%--------------------------------------------------------------------
%% Check Publish
%%--------------------------------------------------------------------

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS,
                                                         retain = Retain},
                            variable = #mqtt_packet_publish{}},
               #channel{endpoint = Endpoint}) ->
    emqx_mqtt_caps:check_pub(emqx_endpoint:zone(Endpoint),
                             #{qos => QoS, retain => Retain}).

check_pub_acl(_Packet, #channel{enable_acl = false}) ->
    ok;
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #channel{endpoint = Endpoint}) ->
    case emqx_endpoint:is_superuser(Endpoint) of
        true  -> ok;
        false ->
            do_acl_check(Endpoint, publish, Topic)
    end.

check_sub_acl(_Packet, #channel{enable_acl = false}) ->
    ok.

do_acl_check(Endpoint, PubSub, Topic) ->
    case emqx_access_control:check_acl(
           emqx_endpoint:to_map(Endpoint), PubSub, Topic) of
        allow -> ok;
        deny -> {error, ?RC_NOT_AUTHORIZED}
    end.

%%--------------------------------------------------------------------
%% Check Subscribe Packet
%%--------------------------------------------------------------------

check_subscribe(TopicFilters, _Channel) ->
    {ok, TopicFilters}.

%%--------------------------------------------------------------------
%% Pipeline
%%--------------------------------------------------------------------

pipeline([Fun], Packet, Channel) ->
    Fun(Packet, Channel);
pipeline([Fun|More], Packet, Channel) ->
    case Fun(Packet, Channel) of
        ok -> pipeline(More, Packet, Channel);
        {ok, NChannel} ->
            pipeline(More, Packet, NChannel);
        {ok, NPacket, NChannel} ->
            pipeline(More, NPacket, NChannel);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Preprocess topic filters
%%--------------------------------------------------------------------

preprocess_topic_filters(Type, Endpoint, TopicFilters) ->
    TopicFilters1 = emqx_hooks:run_fold(case Type of
                                            ?SUBSCRIBE -> 'client.subscribe';
                                            ?UNSUBSCRIBE -> 'client.unsubscribe'
                                        end,
                                        [emqx_endpoint:credentials(Endpoint)],
                                        TopicFilters),
    emqx_mountpoint:mount(emqx_endpoint:mountpoint(Endpoint), TopicFilters1).

%%--------------------------------------------------------------------
%% Enrich subopts
%%--------------------------------------------------------------------

enrich_subopts(TopicFilters, #channel{proto_ver = ?MQTT_PROTO_V5}) ->
    TopicFilters;
enrich_subopts(TopicFilters, #channel{endpoint = Endpoint, is_bridge = IsBridge}) ->
    Rap = flag(IsBridge),
    Nl = flag(emqx_zone:get_env(emqx_endpoint:zone(Endpoint), ignore_loop_deliver, false)),
    [{Topic, SubOpts#{rap => Rap, nl => Nl}} || {Topic, SubOpts} <- TopicFilters].

%%--------------------------------------------------------------------
%% Parse topic filters
%%--------------------------------------------------------------------

parse_topic_filters(?SUBSCRIBE, TopicFilters) ->
    [emqx_topic:parse(Topic, SubOpts) || {Topic, SubOpts} <- TopicFilters];

parse_topic_filters(?UNSUBSCRIBE, TopicFilters) ->
    lists:map(fun emqx_topic:parse/1, TopicFilters).

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

