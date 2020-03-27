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

-module(emqx_packet).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

%% Header APIs
-export([ type/1
        , type_name/1
        , dup/1
        , qos/1
        , retain/1
        ]).

%% Field APIs
-export([ proto_name/1
        , proto_ver/1
        , info/2
        , set_props/2
        ]).

%% Check API
-export([ check/1
        , check/2
        ]).

-export([ to_message/2
        , to_message/3
        , will_msg/1
        ]).

-export([format/1]).

-type(connect() :: #mqtt_packet_connect{}).
-type(publish() :: #mqtt_packet_publish{}).
-type(subscribe() :: #mqtt_packet_subscribe{}).
-type(unsubscribe() :: #mqtt_packet_unsubscribe{}).

%%--------------------------------------------------------------------
%% MQTT Packet Type and Flags.
%%--------------------------------------------------------------------

%% @doc MQTT packet type.
-spec(type(emqx_types:packet()) -> emqx_types:packet_type()).
type(#mqtt_packet{header = #mqtt_packet_header{type = Type}}) ->
    Type.

%% @doc Name of MQTT packet type.
-spec(type_name(emqx_types:packet()) -> atom()).
type_name(Packet) when is_record(Packet, mqtt_packet) ->
    lists:nth(type(Packet), ?TYPE_NAMES).

%% @doc Dup flag of MQTT packet.
-spec(dup(emqx_types:packet()) -> boolean()).
dup(#mqtt_packet{header = #mqtt_packet_header{dup = Dup}}) ->
    Dup.

%% @doc QoS of MQTT packet type.
-spec(qos(emqx_types:packet()) -> emqx_types:qos()).
qos(#mqtt_packet{header = #mqtt_packet_header{qos = QoS}}) ->
    QoS.

%% @doc Retain flag of MQTT packet.
-spec(retain(emqx_types:packet()) -> boolean()).
retain(#mqtt_packet{header = #mqtt_packet_header{retain = Retain}}) ->
    Retain.

%%--------------------------------------------------------------------
%% Protocol name and version of MQTT CONNECT Packet.
%%--------------------------------------------------------------------

%% @doc Protocol name of the CONNECT Packet.
-spec(proto_name(emqx_types:packet()|connect()) -> binary()).
proto_name(?CONNECT_PACKET(ConnPkt)) ->
    proto_name(ConnPkt);
proto_name(#mqtt_packet_connect{proto_name = Name}) ->
    Name.

%% @doc Protocol version of the CONNECT Packet.
-spec(proto_ver(emqx_types:packet()|connect()) -> emqx_types:version()).
proto_ver(?CONNECT_PACKET(ConnPkt)) ->
    proto_ver(ConnPkt);
proto_ver(#mqtt_packet_connect{proto_ver = Ver}) ->
    Ver.

%%--------------------------------------------------------------------
%% Field Info
%%--------------------------------------------------------------------

info(proto_name, #mqtt_packet_connect{proto_name = Name}) ->
    Name;
info(proto_ver, #mqtt_packet_connect{proto_ver = Ver}) ->
    Ver;
info(is_bridge, #mqtt_packet_connect{is_bridge = IsBridge}) ->
    IsBridge;
info(clean_start, #mqtt_packet_connect{clean_start = CleanStart}) ->
    CleanStart;
info(will_flag, #mqtt_packet_connect{will_flag = WillFlag}) ->
    WillFlag;
info(will_qos, #mqtt_packet_connect{will_qos = WillQoS}) ->
    WillQoS;
info(will_retain, #mqtt_packet_connect{will_retain = WillRetain}) ->
    WillRetain;
info(keepalive, #mqtt_packet_connect{keepalive = KeepAlive}) ->
    KeepAlive;
info(properties, #mqtt_packet_connect{properties = Props}) ->
    Props;
info(clientid, #mqtt_packet_connect{clientid = ClientId}) ->
    ClientId;
info(will_props, #mqtt_packet_connect{will_props = WillProps}) ->
    WillProps;
info(will_topic, #mqtt_packet_connect{will_topic = WillTopic}) ->
    WillTopic;
info(will_payload, #mqtt_packet_connect{will_payload = Payload}) ->
    Payload;
info(username, #mqtt_packet_connect{username = Username}) ->
    Username;
info(password, #mqtt_packet_connect{password = Password}) ->
    Password;

info(ack_flags, #mqtt_packet_connack{ack_flags = Flags}) ->
    Flags;
info(reason_code, #mqtt_packet_connack{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_connack{properties = Props}) ->
    Props;

info(topic_name, #mqtt_packet_publish{topic_name = Topic}) ->
    Topic;
info(packet_id, #mqtt_packet_publish{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_publish{properties = Props}) ->
    Props;

info(packet_id, #mqtt_packet_puback{packet_id = PacketId}) ->
    PacketId;
info(reason_code, #mqtt_packet_puback{reason_code = RC}) ->
    RC;
info(properties,  #mqtt_packet_puback{properties = Props}) ->
    Props;

info(packet_id, #mqtt_packet_subscribe{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_subscribe{properties = Props}) ->
    Props;
info(topic_filters, #mqtt_packet_subscribe{topic_filters = Topics}) ->
    Topics;

info(packet_id, #mqtt_packet_suback{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_suback{properties = Props}) ->
    Props;
info(reason_codes, #mqtt_packet_suback{reason_codes = RCs}) ->
    RCs;

info(packet_id, #mqtt_packet_unsubscribe{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_unsubscribe{properties = Props}) ->
    Props;
info(topic_filters, #mqtt_packet_unsubscribe{topic_filters = Topics}) ->
    Topics;

info(packet_id, #mqtt_packet_unsuback{packet_id = PacketId}) ->
    PacketId;
info(properties, #mqtt_packet_unsuback{properties = Props}) ->
    Props;
info(reason_codes, #mqtt_packet_unsuback{reason_codes = RCs}) ->
    RCs;

info(reason_code, #mqtt_packet_disconnect{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_disconnect{properties = Props}) ->
    Props;

info(reason_code, #mqtt_packet_auth{reason_code = RC}) ->
    RC;
info(properties, #mqtt_packet_auth{properties = Props}) ->
    Props.

set_props(Props, #mqtt_packet_connect{} = Pkt) ->
    Pkt#mqtt_packet_connect{properties = Props};

set_props(Props, #mqtt_packet_connack{} = Pkt) ->
    Pkt#mqtt_packet_connack{properties = Props};

set_props(Props, #mqtt_packet_publish{} = Pkt) ->
    Pkt#mqtt_packet_publish{properties = Props};

set_props(Props, #mqtt_packet_puback{} = Pkt) ->
    Pkt#mqtt_packet_puback{properties = Props};

set_props(Props, #mqtt_packet_subscribe{} = Pkt) ->
    Pkt#mqtt_packet_subscribe{properties = Props};

set_props(Props, #mqtt_packet_suback{} = Pkt) ->
    Pkt#mqtt_packet_suback{properties = Props};

set_props(Props, #mqtt_packet_unsubscribe{} = Pkt) ->
    Pkt#mqtt_packet_unsubscribe{properties = Props};

set_props(Props, #mqtt_packet_unsuback{} = Pkt) ->
    Pkt#mqtt_packet_unsuback{properties = Props};

set_props(Props, #mqtt_packet_disconnect{} = Pkt) ->
    Pkt#mqtt_packet_disconnect{properties = Props};

set_props(Props, #mqtt_packet_auth{} = Pkt) ->
    Pkt#mqtt_packet_auth{properties = Props}.

%%--------------------------------------------------------------------
%% Check MQTT Packet
%%--------------------------------------------------------------------

%% @doc Check PubSub Packet.
-spec(check(emqx_types:packet()|publish()|subscribe()|unsubscribe())
      -> ok | {error, emqx_types:reason_code()}).
check(#mqtt_packet{variable = PubPkt}) when is_record(PubPkt, mqtt_packet_publish) ->
    check(PubPkt);

check(#mqtt_packet{variable = SubPkt}) when is_record(SubPkt, mqtt_packet_subscribe) ->
    check(SubPkt);

check(#mqtt_packet{variable = UnsubPkt}) when is_record(UnsubPkt, mqtt_packet_unsubscribe) ->
    check(UnsubPkt);

%% A Topic Alias of 0 is not permitted.
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias':= 0}}) ->
    {error, ?RC_PROTOCOL_ERROR};
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{'Topic-Alias':= _Alias}}) ->
    ok;
check(#mqtt_packet_publish{topic_name = <<>>, properties = #{}}) ->
    {error, ?RC_PROTOCOL_ERROR};
check(#mqtt_packet_publish{topic_name = TopicName, properties = Props}) ->
    try emqx_topic:validate(name, TopicName) of
        true -> check_pub_props(Props)
    catch
        error:_Error ->
            {error, ?RC_TOPIC_NAME_INVALID}
    end;

check(#mqtt_packet_subscribe{properties = #{'Subscription-Identifier' := I}})
  when I =< 0; I >= 16#FFFFFFF ->
    {error, ?RC_SUBSCRIPTION_IDENTIFIERS_NOT_SUPPORTED};

check(#mqtt_packet_subscribe{topic_filters = []}) ->
    {error, ?RC_TOPIC_FILTER_INVALID};

check(#mqtt_packet_subscribe{topic_filters = TopicFilters}) ->
    try validate_topic_filters(TopicFilters)
    catch
        error:_Error ->
            {error, ?RC_TOPIC_FILTER_INVALID}
    end;

check(#mqtt_packet_unsubscribe{topic_filters = []}) ->
    {error, ?RC_TOPIC_FILTER_INVALID};

check(#mqtt_packet_unsubscribe{topic_filters = TopicFilters}) ->
    try validate_topic_filters(TopicFilters)
    catch
        error:_Error ->
            {error, ?RC_TOPIC_FILTER_INVALID}
    end.

check_pub_props(#{'Topic-Alias' := 0}) ->
    {error, ?RC_TOPIC_ALIAS_INVALID};

check_pub_props(#{'Subscription-Identifier' := 0}) ->
    {error, ?RC_PROTOCOL_ERROR};

check_pub_props(#{'Response-Topic' := ResponseTopic}) ->
    try emqx_topic:validate(name, ResponseTopic) of
        true -> ok
    catch
        error:_Error ->
            {error, ?RC_PROTOCOL_ERROR}
    end;
check_pub_props(_Props) -> ok.

%% @doc Check CONNECT Packet.
-spec(check(emqx_types:packet()|connect(), Opts :: map())
      -> ok | {error, emqx_types:reason_code()}).
check(?CONNECT_PACKET(ConnPkt), Opts) ->
    check(ConnPkt, Opts);
check(ConnPkt, Opts) when is_record(ConnPkt, mqtt_packet_connect) ->
    run_checks([fun check_proto_ver/2,
                fun check_client_id/2,
                fun check_conn_props/2,
                fun check_will_msg/2], ConnPkt, Opts).

check_proto_ver(#mqtt_packet_connect{proto_ver  = Ver,
                                     proto_name = Name}, _Opts) ->
    case proplists:get_value(Ver, ?PROTOCOL_NAMES) of
        Name   -> ok;
        _Other -> {error, ?RC_UNSUPPORTED_PROTOCOL_VERSION}
    end.

%% MQTT3.1 does not allow null clientId
check_client_id(#mqtt_packet_connect{proto_ver = ?MQTT_PROTO_V3,
                                     clientid  = <<>>}, _Opts) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};
%% Issue#599: Null clientId and clean_start = false
check_client_id(#mqtt_packet_connect{clientid    = <<>>,
                                     clean_start = false}, _Opts) ->
    {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID};
check_client_id(#mqtt_packet_connect{clientid    = <<>>,
                                     clean_start = true}, _Opts) ->
    ok;
check_client_id(#mqtt_packet_connect{clientid = ClientId},
                #{max_clientid_len := MaxLen} = _Opts) ->
    case (1 =< (Len = byte_size(ClientId))) andalso (Len =< MaxLen) of
        true  -> ok;
        false -> {error, ?RC_CLIENT_IDENTIFIER_NOT_VALID}
    end.

check_conn_props(#mqtt_packet_connect{properties = undefined}, _Opts) ->
    ok;
check_conn_props(#mqtt_packet_connect{properties = #{'Receive-Maximum' := 0}}, _Opts) ->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(#mqtt_packet_connect{properties = #{'Request-Response-Information' := ReqRespInfo}}, _Opts)
  when ReqRespInfo =/= 0, ReqRespInfo =/= 1 ->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(#mqtt_packet_connect{properties = #{'Request-Problem-Information' := ReqProInfo}}, _Opts)
    when ReqProInfo =/= 0, ReqProInfo =/= 1 ->
    {error, ?RC_PROTOCOL_ERROR};
check_conn_props(_ConnPkt, _Opts) -> ok.

check_will_msg(#mqtt_packet_connect{will_flag = false}, _Caps) ->
    ok;
check_will_msg(#mqtt_packet_connect{will_retain = true},
               _Opts = #{mqtt_retain_available := false}) ->
    {error, ?RC_RETAIN_NOT_SUPPORTED};
check_will_msg(#mqtt_packet_connect{will_qos = WillQoS},
               _Opts = #{max_qos_allowed := MaxQoS}) when WillQoS > MaxQoS ->
    {error, ?RC_QOS_NOT_SUPPORTED};
check_will_msg(#mqtt_packet_connect{will_topic = WillTopic}, _Opts) ->
    try emqx_topic:validate(name, WillTopic) of
        true -> ok
    catch error:_Error ->
        {error, ?RC_TOPIC_NAME_INVALID}
    end.

run_checks([], _Packet, _Options) ->
    ok;
run_checks([Check|More], Packet, Options) ->
    case Check(Packet, Options) of
        ok -> run_checks(More, Packet, Options);
        Error = {error, _Reason} -> Error
    end.

%% @doc Validate MQTT Packet
%% @private
validate_topic_filters(TopicFilters) ->
    lists:foreach(
      fun({TopicFilter, _SubOpts}) ->
              emqx_topic:validate(TopicFilter);
         (TopicFilter) ->
              emqx_topic:validate(TopicFilter)
      end, TopicFilters).

-spec(to_message(emqx_types:clientinfo(), emqx_ypes:packet()) -> emqx_types:message()).
to_message(ClientInfo, Packet) ->
    to_message(ClientInfo, #{}, Packet).

%% @doc Transform Publish Packet to Message.
-spec(to_message(emqx_types:clientinfo(), map(), emqx_ypes:packet())
      -> emqx_types:message()).
to_message(#{protocol := Protocol,
             clientid := ClientId,
             username := Username,
             peerhost := PeerHost
            }, Headers,
           #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                       retain = Retain,
                                                       qos    = QoS,
                                                       dup    = Dup
                                                      },
                        variable = #mqtt_packet_publish{topic_name = Topic,
                                                        properties = Props
                                                       },
                        payload  = Payload
                       }) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Headers1 = merge_props(Headers#{protocol => Protocol,
                                    username => Username,
                                    peerhost => PeerHost
                                   }, Props),
    Msg#message{flags = #{dup => Dup, retain => Retain}, headers = Headers1}.

-spec(will_msg(#mqtt_packet_connect{}) -> emqx_types:message()).
will_msg(#mqtt_packet_connect{will_flag = false}) ->
    undefined;
will_msg(#mqtt_packet_connect{clientid     = ClientId,
                              username     = Username,
                              will_retain  = Retain,
                              will_qos     = QoS,
                              will_topic   = Topic,
                              will_props   = Props,
                              will_payload = Payload}) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Headers = merge_props(#{username => Username}, Props),
    Msg#message{flags = #{dup => false, retain => Retain}, headers = Headers}.

merge_props(Headers, undefined) ->
    Headers;
merge_props(Headers, Props) ->
    maps:merge(Headers, Props).

%% @doc Format packet
-spec(format(emqx_types:packet()) -> iolist()).
format(#mqtt_packet{header = Header, variable = Variable, payload = Payload}) ->
    format_header(Header, format_variable(Variable, Payload)).

format_header(#mqtt_packet_header{type = Type,
                                  dup = Dup,
                                  qos = QoS,
                                  retain = Retain}, S) ->
    S1 = if
             S == undefined -> <<>>;
             true -> [", ", S]
         end,
    io_lib:format("~s(Q~p, R~p, D~p~s)",
                  [lists:nth(Type, ?TYPE_NAMES), QoS, i(Retain), i(Dup), S1]).

format_variable(undefined, _) ->
    undefined;
format_variable(Variable, undefined) ->
    format_variable(Variable);
format_variable(Variable, Payload) ->
    io_lib:format("~s, Payload=~0p", [format_variable(Variable), Payload]).

format_variable(#mqtt_packet_connect{
                 proto_ver    = ProtoVer,
                 proto_name   = ProtoName,
                 will_retain  = WillRetain,
                 will_qos     = WillQoS,
                 will_flag    = WillFlag,
                 clean_start  = CleanStart,
                 keepalive    = KeepAlive,
                 clientid     = ClientId,
                 will_topic   = WillTopic,
                 will_payload = WillPayload,
                 username     = Username,
                 password     = Password}) ->
    Format = "ClientId=~s, ProtoName=~s, ProtoVsn=~p, CleanStart=~s, KeepAlive=~p, Username=~s, Password=~s",
    Args = [ClientId, ProtoName, ProtoVer, CleanStart, KeepAlive, Username, format_password(Password)],
    {Format1, Args1} = if
                        WillFlag -> {Format ++ ", Will(Q~p, R~p, Topic=~s, Payload=~0p)",
                                     Args ++ [WillQoS, i(WillRetain), WillTopic, WillPayload]};
                        true -> {Format, Args}
                       end,
    io_lib:format(Format1, Args1);

format_variable(#mqtt_packet_disconnect
                {reason_code = ReasonCode}) ->
    io_lib:format("ReasonCode=~p", [ReasonCode]);

format_variable(#mqtt_packet_connack{ack_flags   = AckFlags,
                                     reason_code = ReasonCode}) ->
    io_lib:format("AckFlags=~p, ReasonCode=~p", [AckFlags, ReasonCode]);

format_variable(#mqtt_packet_publish{topic_name = TopicName,
                                     packet_id  = PacketId}) ->
    io_lib:format("Topic=~s, PacketId=~p", [TopicName, PacketId]);

format_variable(#mqtt_packet_puback{packet_id = PacketId,
                                    reason_code = ReasonCode}) ->
    io_lib:format("PacketId=~p, ReasonCode=~p", [PacketId, ReasonCode]);

format_variable(#mqtt_packet_subscribe{packet_id     = PacketId,
                                       topic_filters = TopicFilters}) ->
    io_lib:format("PacketId=~p, TopicFilters=~0p", [PacketId, TopicFilters]);

format_variable(#mqtt_packet_unsubscribe{packet_id     = PacketId,
                                         topic_filters = Topics}) ->
    io_lib:format("PacketId=~p, TopicFilters=~0p", [PacketId, Topics]);

format_variable(#mqtt_packet_suback{packet_id = PacketId,
                                    reason_codes = ReasonCodes}) ->
    io_lib:format("PacketId=~p, ReasonCodes=~p", [PacketId, ReasonCodes]);

format_variable(#mqtt_packet_unsuback{packet_id = PacketId}) ->
    io_lib:format("PacketId=~p", [PacketId]);

format_variable(PacketId) when is_integer(PacketId) ->
    io_lib:format("PacketId=~p", [PacketId]);

format_variable(undefined) -> undefined.

format_password(undefined) -> undefined;
format_password(_Password) -> '******'.

i(true)  -> 1;
i(false) -> 0;
i(I) when is_integer(I) -> I.

