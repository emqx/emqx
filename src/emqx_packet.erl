%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_packet).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([ protocol_name/1
        , type_name/1
        , validate/1
        , format/1
        , to_message/2
        , from_message/2
        , will_msg/1
        ]).

%% @doc Protocol name of version
-spec(protocol_name(emqx_mqtt_types:version()) -> binary()).
protocol_name(?MQTT_PROTO_V3) ->
    <<"MQIsdp">>;
protocol_name(?MQTT_PROTO_V4) ->
    <<"MQTT">>;
protocol_name(?MQTT_PROTO_V5) ->
    <<"MQTT">>.

%% @doc Name of MQTT packet type
-spec(type_name(emqx_mqtt_types:packet_type()) -> atom()).
type_name(Type) when Type > ?RESERVED andalso Type =< ?AUTH ->
    lists:nth(Type, ?TYPE_NAMES).

%%------------------------------------------------------------------------------
%% Validate MQTT Packet
%%------------------------------------------------------------------------------

validate(?SUBSCRIBE_PACKET(_PacketId, _Properties, [])) ->
    error(topic_filters_invalid);
validate(?SUBSCRIBE_PACKET(PacketId, Properties, TopicFilters)) ->
    validate_packet_id(PacketId)
        andalso validate_properties(?SUBSCRIBE, Properties)
            andalso ok == lists:foreach(fun validate_subscription/1, TopicFilters);

validate(?UNSUBSCRIBE_PACKET(_PacketId, [])) ->
    error(topic_filters_invalid);
validate(?UNSUBSCRIBE_PACKET(PacketId, TopicFilters)) ->
    validate_packet_id(PacketId)
        andalso ok == lists:foreach(fun emqx_topic:validate/1, TopicFilters);

validate(?PUBLISH_PACKET(_QoS, <<>>, _, #{'Topic-Alias':= _I}, _)) ->
    true;
validate(?PUBLISH_PACKET(_QoS, <<>>, _, _, _)) ->
    error(topic_name_invalid);
validate(?PUBLISH_PACKET(_QoS, Topic, _, Properties, _)) ->
    ((not emqx_topic:wildcard(Topic)) orelse error(topic_name_invalid))
        andalso validate_properties(?PUBLISH, Properties);

validate(?CONNECT_PACKET(#mqtt_packet_connect{properties = Properties})) ->
    validate_properties(?CONNECT, Properties);

validate(_Packet) ->
    true.

validate_packet_id(0) ->
    error(packet_id_invalid);
validate_packet_id(_) ->
    true.

validate_properties(?SUBSCRIBE, #{'Subscription-Identifier' := I})
    when I =< 0; I >= 16#FFFFFFF ->
    error(subscription_identifier_invalid);
validate_properties(?PUBLISH, #{'Topic-Alias':= I})
    when I =:= 0 ->
    error(topic_alias_invalid);
validate_properties(?PUBLISH, #{'Subscription-Identifier' := _I}) ->
    error(protocol_error);
validate_properties(?PUBLISH, #{'Response-Topic' := ResponseTopic}) ->
    case emqx_topic:wildcard(ResponseTopic) of
        true ->
            error(protocol_error);
        false ->
            true
    end;
validate_properties(?CONNECT, #{'Receive-Maximum' := 0}) ->
    error(protocol_error);
validate_properties(?CONNECT, #{'Request-Response-Information' := ReqRespInfo})
    when ReqRespInfo =/= 0, ReqRespInfo =/= 1 ->
    error(protocol_error);
validate_properties(?CONNECT, #{'Request-Problem-Information' := ReqProInfo})
    when ReqProInfo =/= 0, ReqProInfo =/= 1 ->
    error(protocol_error);
validate_properties(_, _) ->
    true.

validate_subscription({Topic, #{qos := QoS}}) ->
    emqx_topic:validate(filter, Topic) andalso validate_qos(QoS).

validate_qos(QoS) when ?QOS_0 =< QoS, QoS =< ?QOS_2 ->
    true;
validate_qos(_) -> error(bad_qos).

%% @doc From message to packet
-spec(from_message(emqx_mqtt_types:packet_id(), emqx_types:message()) -> emqx_mqtt_types:packet()).
from_message(PacketId, #message{qos = QoS, flags = Flags, headers = Headers,
                                topic = Topic, payload = Payload}) ->
    Flags1 = if Flags =:= undefined ->
                    #{};
                true -> Flags
             end,
    Dup = maps:get(dup, Flags1, false),
    Retain = maps:get(retain, Flags1, false),
    Publish = #mqtt_packet_publish{topic_name = Topic,
                                   packet_id  = PacketId,
                                   properties = publish_props(Headers)},
    #mqtt_packet{header = #mqtt_packet_header{type   = ?PUBLISH,
                                              dup    = Dup,
                                              qos    = QoS,
                                              retain = Retain},
                 variable = Publish, payload = Payload}.

publish_props(Headers) ->
    maps:with(['Payload-Format-Indicator',
               'Response-Topic',
               'Correlation-Data',
               'User-Property',
               'Subscription-Identifier',
               'Content-Type',
               'Message-Expiry-Interval'], Headers).

%% @doc Message from Packet
-spec(to_message(emqx_types:credentials(), emqx_mqtt_types:packet())
      -> emqx_types:message()).
to_message(#{client_id := ClientId, username := Username, peername := Peername},
           #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                       retain = Retain,
                                                       qos    = QoS,
                                                       dup    = Dup},
                        variable = #mqtt_packet_publish{topic_name = Topic,
                                                        properties = Props},
                        payload  = Payload}) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg#message{flags = #{dup => Dup, retain => Retain},
                headers = merge_props(#{username => Username,
                                        peername => Peername}, Props)}.

-spec(will_msg(#mqtt_packet_connect{}) -> emqx_types:message()).
will_msg(#mqtt_packet_connect{will_flag = false}) ->
    undefined;
will_msg(#mqtt_packet_connect{client_id    = ClientId,
                              username     = Username,
                              will_retain  = Retain,
                              will_qos     = QoS,
                              will_topic   = Topic,
                              will_props   = Properties,
                              will_payload = Payload}) ->
    Msg = emqx_message:make(ClientId, QoS, Topic, Payload),
    Msg#message{flags = #{dup => false, retain => Retain},
                headers = merge_props(#{username => Username}, Properties)}.

merge_props(Headers, undefined) ->
    Headers;
merge_props(Headers, Props) ->
    maps:merge(Headers, Props).

%% @doc Format packet
-spec(format(emqx_mqtt_types:packet()) -> iolist()).
format(#mqtt_packet{header = Header, variable = Variable, payload = Payload}) ->
    format_header(Header, format_variable(Variable, Payload)).

format_header(#mqtt_packet_header{type = Type,
                                  dup = Dup,
                                  qos = QoS,
                                  retain = Retain}, S) ->
    S1 = if
             S == undefined -> <<>>;
             true           -> [", ", S]
         end,
    io_lib:format("~s(Q~p, R~p, D~p~s)", [type_name(Type), QoS, i(Retain), i(Dup), S1]).

format_variable(undefined, _) ->
    undefined;
format_variable(Variable, undefined) ->
    format_variable(Variable);
format_variable(Variable, Payload) ->
    io_lib:format("~s, Payload=~p", [format_variable(Variable), Payload]).

format_variable(#mqtt_packet_connect{
                 proto_ver    = ProtoVer,
                 proto_name   = ProtoName,
                 will_retain  = WillRetain,
                 will_qos     = WillQoS,
                 will_flag    = WillFlag,
                 clean_start  = CleanStart,
                 keepalive    = KeepAlive,
                 client_id    = ClientId,
                 will_topic   = WillTopic,
                 will_payload = WillPayload,
                 username     = Username,
                 password     = Password}) ->
    Format = "ClientId=~s, ProtoName=~s, ProtoVsn=~p, CleanStart=~s, KeepAlive=~p, Username=~s, Password=~s",
    Args = [ClientId, ProtoName, ProtoVer, CleanStart, KeepAlive, Username, format_password(Password)],
    {Format1, Args1} = if
                        WillFlag -> {Format ++ ", Will(Q~p, R~p, Topic=~s, Payload=~p)",
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

format_variable(#mqtt_packet_puback{packet_id = PacketId}) ->
    io_lib:format("PacketId=~p", [PacketId]);

format_variable(#mqtt_packet_subscribe{packet_id     = PacketId,
                                       topic_filters = TopicFilters}) ->
    io_lib:format("PacketId=~p, TopicFilters=~p", [PacketId, TopicFilters]);

format_variable(#mqtt_packet_unsubscribe{packet_id     = PacketId,
                                         topic_filters = Topics}) ->
    io_lib:format("PacketId=~p, TopicFilters=~p", [PacketId, Topics]);

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

