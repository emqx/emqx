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
        ]).

-export([ init/2
        , handle_in/2
        , handle_deliver/2
        , handle_out/2
        , handle_timeout/3
        , terminate/2
        ]).

-export_type([proto_state/0]).

-record(protocol, {
          proto_name    :: binary(),
          proto_ver     :: emqx_types:version(),
          client        :: emqx_types:client(),
          session       :: emqx_session:session(),
          mountfun      :: fun((emqx_topic:topic()) -> emqx_topic:topic()),
          keepalive     :: non_neg_integer(),
          will_msg      :: emqx_types:message(),
          enable_acl    :: boolean(),
          is_bridge     :: boolean(),
          topic_aliases :: map(),
          alias_maximum :: map()
        }).

-opaque(proto_state() :: #protocol{}).

-spec(info(proto_state()) -> emqx_types:infos()).
info(#protocol{proto_name = ProtoName,
               proto_ver  = ProtoVer,
               client     = Client,
               session    = Session,
               keepalive  = Keepalive,
               will_msg   = WillMsg,
               enable_acl = EnableAcl,
               is_bridge  = IsBridge,
               topic_aliases = Aliases}) ->
    #{proto_name => ProtoName,
      proto_ver => ProtoVer,
      client => Client,
      session => emqx_session:info(Session),
      keepalive => Keepalive,
      will_msg => WillMsg,
      enable_acl => EnableAcl,
      is_bridge => IsBridge,
      topic_aliases => Aliases
     }.

-spec(info(atom(), proto_state()) -> term()).
info(proto_name, #protocol{proto_name = ProtoName}) ->
    ProtoName;
info(proto_ver, #protocol{proto_ver = ProtoVer}) ->
    ProtoVer;
info(client, #protocol{client = Client}) ->
    Client;
info(zone, #protocol{client = #{zone := Zone}}) ->
    Zone;
info(client_id, #protocol{client = #{client_id := ClientId}}) ->
    ClientId;
info(session, #protocol{session = Session}) ->
    Session;
info(keepalive, #protocol{keepalive = Keepalive}) ->
    Keepalive;
info(is_bridge, #protocol{is_bridge = IsBridge}) ->
    IsBridge;
info(topic_aliases, #protocol{topic_aliases = Aliases}) ->
    Aliases.

attrs(#protocol{proto_name = ProtoName,
                proto_ver  = ProtoVer,
                client     = Client,
                session    = Session,
                keepalive  = Keepalive,
                is_bridge  = IsBridge}) ->
    #{proto_name => ProtoName,
      proto_ver => ProtoVer,
      client => Client,
      session => emqx_session:attrs(Session),
      keepalive => Keepalive,
      is_bridge => IsBridge
     }.

-spec(init(map(), proplists:proplist()) -> proto_state()).
init(ConnInfo, Options) ->
    Zone = proplists:get_value(zone, Options),
    Peercert = maps:get(peercert, ConnInfo, nossl),
    Username = peer_cert_as_username(Peercert, Options),
    Mountpoint = emqx_zone:get_env(Zone, mountpoint),
    Client = maps:merge(#{zone       => Zone,
                          username   => Username,
                          mountpoint => Mountpoint
                         }, ConnInfo),
    EnableAcl = emqx_zone:get_env(Zone, enable_acl, false),
    MountFun = fun(Topic) ->
                       emqx_mountpoint:mount(Mountpoint, Topic)
               end,
    #protocol{client     = Client,
              mountfun   = MountFun,
              enable_acl = EnableAcl,
              is_bridge  = false
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

-spec(handle_in(emqx_types:packet(), proto_state())
      -> {ok, proto_state()}
       | {ok, emqx_types:packet(), proto_state()}
       | {error, Reason :: term(), proto_state()}
       | {stop, Error :: atom(), proto_state()}).
handle_in(?CONNECT_PACKET(
             #mqtt_packet_connect{proto_name = ProtoName,
                                  proto_ver  = ProtoVer,
                                  is_bridge  = IsBridge,
                                  client_id  = ClientId,
                                  username   = Username,
                                  password   = Password,
                                  keepalive  = Keepalive} = ConnPkt),
          PState = #protocol{client = Client}) ->
    Client1 = maps:merge(Client, #{client_id => ClientId,
                                   username  => Username,
                                   password  => Password
                                  }),
    emqx_logger:set_metadata_client_id(ClientId),
    WillMsg = emqx_packet:will_msg(ConnPkt),
    PState1 = PState#protocol{client     = Client1,
                              proto_name = ProtoName,
                              proto_ver  = ProtoVer,
                              is_bridge  = IsBridge,
                              keepalive  = Keepalive,
                              will_msg   = WillMsg
                             },
    %% fun validate_packet/2,
    case pipeline([fun check_connect/2,
                   fun handle_connect/2], ConnPkt, PState1) of
        {ok, SP, PState2} ->
            handle_out({connack, ?RC_SUCCESS, sp(SP)}, PState2);
        {error, ReasonCode} ->
            handle_out({connack, ReasonCode}, PState1);
        {error, ReasonCode, PState2} ->
            handle_out({connack, ReasonCode}, PState2)
    end;

handle_in(Packet = ?PUBLISH_PACKET(QoS, Topic, PacketId), PState) ->
    case pipeline([fun validate_packet/2,
                   fun check_pub_caps/2,
                   fun check_pub_acl/2,
                   fun handle_publish/2], Packet, PState) of
        {error, ReasonCode} ->
            ?LOG(warning, "Cannot publish qos~w message to ~s due to ~s",
                 [QoS, Topic, emqx_reason_codes:text(ReasonCode)]),
            handle_out(case QoS of
                           ?QOS_0 -> {puberr, ReasonCode};
                           ?QOS_1 -> {puback, PacketId, ReasonCode};
                           ?QOS_2 -> {pubrec, PacketId, ReasonCode}
                       end, PState);
        Result -> Result
    end;

handle_in(?PUBACK_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:puback(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _NotFound} ->
            %% TODO: metrics? error msg?
            {ok, PState}
    end;

handle_in(?PUBREC_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrec(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            handle_out({pubrel, PacketId}, PState#protocol{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubrel, PacketId, ReasonCode}, PState)
    end;

handle_in(?PUBREL_PACKET(PacketId, ReasonCode), PState = #protocol{session = Session}) ->
    case emqx_session:pubrel(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            handle_out({pubcomp, PacketId}, PState#protocol{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubcomp, PacketId, ReasonCode}, PState)
    end;

handle_in(?PUBCOMP_PACKET(PacketId, ReasonCode),
          PState = #protocol{session = Session}) ->
    case emqx_session:pubcomp(PacketId, ReasonCode, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {error, _ReasonCode} ->
            %% TODO: How to handle the reason code?
            {ok, PState}
    end;

handle_in(Packet = ?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          PState = #protocol{client = Client}) ->
    case validate(Packet) of
        ok -> ok = emqx_hooks:run('client.subscribe',
                                  [Client, Properties, TopicFilters]),
              TopicFilters1 = enrich_subid(Properties, TopicFilters),
              {ReasonCodes, PState1} = handle_subscribe(TopicFilters1, PState),
              handle_out({suback, PacketId, ReasonCodes}, PState1);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, PState)
    end;

handle_in(Packet = ?UNSUBSCRIBE_PACKET(PacketId, Properties, TopicFilters),
          PState = #protocol{client = Client}) ->
    case validate(Packet) of
        ok -> ok = emqx_hooks:run('client.unsubscribe',
                                  [Client, Properties, TopicFilters]),
              {ReasonCodes, PState1} = handle_unsubscribe(TopicFilters, PState),
              handle_out({unsuback, PacketId, ReasonCodes}, PState1);
        {error, ReasonCode} ->
            handle_out({disconnect, ReasonCode}, PState)
    end;

handle_in(?PACKET(?PINGREQ), PState) ->
    {ok, ?PACKET(?PINGRESP), PState};

handle_in(?DISCONNECT_PACKET(?RC_SUCCESS), PState) ->
    %% Clear will msg
    {stop, normal, PState#protocol{will_msg = undefined}};

handle_in(?DISCONNECT_PACKET(RC), PState = #protocol{proto_ver = Ver}) ->
    %% TODO:
    %% {stop, {shutdown, abnormal_disconnet}, PState};
    {sto, {shutdown, emqx_reason_codes:name(RC, Ver)}, PState};

handle_in(?AUTH_PACKET(), PState) ->
    %%TODO: implement later.
    {ok, PState};

handle_in(Packet, PState) ->
    io:format("In: ~p~n", [Packet]),
    {ok, PState}.

%%--------------------------------------------------------------------
%% Handle delivers
%%--------------------------------------------------------------------

handle_deliver(Delivers, PState = #protocol{client = Client, session = Session})
  when is_list(Delivers) ->
    case emqx_session:handle(Delivers, Session) of
        {ok, Publishes, NSession} ->
            Packets = lists:map(fun({publish, PacketId, Msg}) ->
                                        Msg0 = emqx_hooks:run_fold('message.deliver', [Client], Msg),
                                        Msg1 = emqx_message:update_expiry(Msg0),
                                        Msg2 = emqx_mountpoint:unmount(maps:get(mountpoint, Client), Msg1),
                                        emqx_packet:from_message(PacketId, Msg2)
                                end, Publishes),
            {ok, Packets, PState#protocol{session = NSession}};
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}}
    end.

%%--------------------------------------------------------------------
%% Handle outgoing packet
%%--------------------------------------------------------------------

handle_out({connack, ?RC_SUCCESS, SP}, PState = #protocol{client = Client}) ->
    ok = emqx_hooks:run('client.connected', [Client, ?RC_SUCCESS, info(PState)]),
    Props = #{}, %% TODO: ...
    {ok, ?CONNACK_PACKET(?RC_SUCCESS, SP, Props), PState};

handle_out({connack, ReasonCode}, PState = #protocol{client = Client,
                                                     proto_ver = ProtoVer}) ->
    ok = emqx_hooks:run('client.connected', [Client, ReasonCode, info(PState)]),
    ReasonCode1 = if
                      ProtoVer == ?MQTT_PROTO_V5 -> ReasonCode;
                      true -> emqx_reason_codes:compat(connack, ReasonCode)
                  end,
    Reason = emqx_reason_codes:name(ReasonCode1, ProtoVer),
    {error, Reason, ?CONNACK_PACKET(ReasonCode1), PState};

handle_out({publish, PacketId, Msg}, PState = #protocol{client = Client}) ->
    Msg0 = emqx_hooks:run_fold('message.deliver', [Client], Msg),
    Msg1 = emqx_message:update_expiry(Msg0),
    Msg2 = emqx_mountpoint:unmount(maps:get(mountpoint, Client), Msg1),
    {ok, emqx_packet:from_message(PacketId, Msg2), PState};

handle_out({puberr, ReasonCode}, PState) ->
    {ok, PState};

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

handle_out(Packet, State) ->
    io:format("Out: ~p~n", [Packet]),
    {ok, State}.

%%--------------------------------------------------------------------
%% Handle timeout
%%--------------------------------------------------------------------

handle_timeout(TRef, Msg, PState = #protocol{session = Session}) ->
    case emqx_session:timeout(TRef, Msg, Session) of
        {ok, NSession} ->
            {ok, PState#protocol{session = NSession}};
        {ok, Publishes, NSession} ->
            %% TODO: handle out...
            io:format("Timeout publishes: ~p~n", [Publishes]),
            {ok, PState#protocol{session = NSession}}
    end.

terminate(Reason, _State) ->
    io:format("Terminated for ~p~n", [Reason]),
    ok.

%%--------------------------------------------------------------------
%% Check Connect Packet
%%--------------------------------------------------------------------

check_connect(_ConnPkt, PState) ->
    {ok, PState}.

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
                                    password    = Password
                                   } = ConnPkt,
               PState = #protocol{client = Client}) ->
    case emqx_access_control:authenticate(
           Client#{password => Password}) of
        {ok, AuthResult} ->
            Client1 = maps:merge(Client, AuthResult),
            %% Open session
            case open_session(ConnPkt, PState) of
                {ok, Session, SP} ->
                    PState1 = PState#protocol{client = Client1,
                                              session = Session},
                    ok = emqx_cm:register_channel(ClientId),
                    {ok, SP, PState1};
                {error, Error} ->
                    ?LOG(error, "Failed to open session: ~p", [Error]),
                    {error, ?RC_UNSPECIFIED_ERROR, PState#protocol{client = Client1}}
            end;
        {error, Reason} ->
            ?LOG(warning, "Client ~s (Username: '~s') login failed for ~p",
                 [ClientId, Username, Reason]),
            {error, emqx_reason_codes:connack_error(Reason), PState}
    end.

open_session(#mqtt_packet_connect{clean_start = CleanStart,
                                  %%properties  = ConnProps,
                                  client_id   = ClientId,
                                  username    = Username} = ConnPkt,
             PState = #protocol{client = Client}) ->
    emqx_cm:open_session(maps:merge(Client, #{clean_start     => CleanStart,
                                              max_inflight    => 0,
                                              expiry_interval => 0})).

%%--------------------------------------------------------------------
%% Handle Publish Message: Client -> Broker
%%--------------------------------------------------------------------

handle_publish(Packet = ?PUBLISH_PACKET(_QoS, Topic, PacketId),
               PState = #protocol{client = Client = #{mountpoint := Mountpoint}}) ->
    %% TODO: ugly... publish_to_msg(...)
    Msg = emqx_packet:to_message(Client, Packet),
    Msg1 = emqx_mountpoint:mount(Mountpoint, Msg),
    Msg2 = emqx_message:set_flag(dup, false, Msg1),
    handle_publish(PacketId, Msg2, PState).

handle_publish(_PacketId, Msg = #message{qos = ?QOS_0}, PState) ->
    _ = emqx_broker:publish(Msg),
    {ok, PState};

handle_publish(PacketId, Msg = #message{qos = ?QOS_1}, PState) ->
    Results = emqx_broker:publish(Msg),
    ReasonCode = emqx_reason_codes:puback(Results),
    handle_out({puback, PacketId, ReasonCode}, PState);

handle_publish(PacketId, Msg = #message{qos = ?QOS_2},
               PState = #protocol{session = Session}) ->
    case emqx_session:publish(PacketId, Msg, Session) of
        {ok, Results, NSession} ->
            ReasonCode = emqx_reason_codes:puback(Results),
            handle_out({pubrec, PacketId, ReasonCode},
                       PState#protocol{session = NSession});
        {error, ReasonCode} ->
            handle_out({pubrec, PacketId, ReasonCode}, PState)
    end.

%%--------------------------------------------------------------------
%% Handle Subscribe Request
%%--------------------------------------------------------------------

handle_subscribe(TopicFilters, PState) ->
    handle_subscribe(TopicFilters, [], PState).

handle_subscribe([], Acc, PState) ->
    {lists:reverse(Acc), PState};

handle_subscribe([{TopicFilter, SubOpts}|More], Acc, PState) ->
    {RC, PState1} = do_subscribe(TopicFilter, SubOpts, PState),
    handle_subscribe(More, [RC|Acc], PState1).

do_subscribe(TopicFilter, SubOpts = #{qos := QoS},
             PState = #protocol{client   = Client,
                                session  = Session,
                                mountfun = Mount}) ->
    %% 1. Parse 2. Check 3. Enrich 5. MountPoint 6. Session
    SubOpts1 = maps:merge(?DEFAULT_SUBOPTS, SubOpts),
    {TopicFilter1, SubOpts2} = emqx_topic:parse(TopicFilter, SubOpts1),
    SubOpts3 = enrich_subopts(SubOpts2, PState),
    case check_subscribe(TopicFilter1, PState) of
        ok ->
            TopicFilter2 = Mount(TopicFilter1),
            case emqx_session:subscribe(Client, TopicFilter2, SubOpts3, Session) of
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
enrich_subopts(SubOpts, #protocol{client = #{zone := Zone},
                                  is_bridge = IsBridge}) ->
    Rap = flag(IsBridge),
    Nl = flag(emqx_zone:get_env(Zone, ignore_loop_deliver, false)),
    SubOpts#{rap => Rap, nl => Nl}.

check_subscribe(_TopicFilter, _PState) ->
    ok.

%%--------------------------------------------------------------------
%% Handle Unsubscribe Request
%%--------------------------------------------------------------------

handle_unsubscribe(TopicFilters, PState) ->
    handle_unsubscribe(TopicFilters, [], PState).

handle_unsubscribe([], Acc, PState) ->
    {lists:reverse(Acc), PState};

handle_unsubscribe([TopicFilter|More], Acc, PState) ->
    {RC, PState1} = do_unsubscribe(TopicFilter, PState),
    handle_unsubscribe(More, [RC|Acc], PState1).

do_unsubscribe(TopicFilter, PState = #protocol{client   = Client,
                                               session  = Session,
                                               mountfun = Mount}) ->
    TopicFilter1 = Mount(element(1, emqx_topic:parse(TopicFilter))),
    case emqx_session:unsubscribe(Client, TopicFilter1, Session) of
        {ok, NSession} ->
            {?RC_SUCCESS, PState#protocol{session = NSession}};
        {error, RC} -> {RC, PState}
    end.

%%--------------------------------------------------------------------
%% Validate Incoming Packet
%%--------------------------------------------------------------------

validate_packet(Packet, _PState) ->
    validate(Packet).

-spec(validate(emqx_types:packet()) -> ok | {error, emqx_types:reason_code()}).
validate(Packet) ->
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
%% Check Publish
%%--------------------------------------------------------------------

check_pub_caps(#mqtt_packet{header = #mqtt_packet_header{qos = QoS,
                                                         retain = Retain},
                            variable = #mqtt_packet_publish{}},
               #protocol{client = #{zone := Zone}}) ->
    emqx_mqtt_caps:check_pub(Zone, #{qos => QoS, retain => Retain}).

check_pub_acl(_Packet, #protocol{enable_acl = false}) ->
    ok;
check_pub_acl(_Packet, #protocol{client = #{is_superuser := true}}) ->
    ok;
check_pub_acl(#mqtt_packet{variable = #mqtt_packet_publish{topic_name = Topic}},
              #protocol{client = Client}) ->
    do_acl_check(Client, publish, Topic).

check_sub_acl(_Packet, #protocol{enable_acl = false}) ->
    ok.

do_acl_check(Client, PubSub, Topic) ->
    case emqx_access_control:check_acl(Client, PubSub, Topic) of
        allow -> ok;
        deny -> {error, ?RC_NOT_AUTHORIZED}
    end.

%%--------------------------------------------------------------------
%% Pipeline
%%--------------------------------------------------------------------

pipeline([Fun], Packet, PState) ->
    Fun(Packet, PState);
pipeline([Fun|More], Packet, PState) ->
    case Fun(Packet, PState) of
        ok -> pipeline(More, Packet, PState);
        {ok, NPState} ->
            pipeline(More, Packet, NPState);
        {ok, NPacket, NPState} ->
            pipeline(More, NPacket, NPState);
        {error, Reason} ->
            {error, Reason}
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

sp(true)  -> 1;
sp(false) -> 0.

flag(true)  -> 1;
flag(false) -> 0.

