%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc MQTT Packet Functions
-module(emqttd_packet).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

%% API
-export([protocol_name/1, type_name/1, connack_name/1]).

-export([format/1]).

%% @doc Protocol name of version
-spec(protocol_name(mqtt_vsn()) -> binary()).
protocol_name(Ver) when Ver =:= ?MQTT_PROTO_V31; Ver =:= ?MQTT_PROTO_V311 ->
    proplists:get_value(Ver, ?PROTOCOL_NAMES).

%% @doc Name of MQTT packet type
-spec(type_name(mqtt_packet_type()) -> atom()).
type_name(Type) when Type > ?RESERVED andalso Type =< ?DISCONNECT ->
    lists:nth(Type, ?TYPE_NAMES).

%% @doc Connack Name
-spec(connack_name(mqtt_connack()) -> atom()).
connack_name(?CONNACK_ACCEPT)      -> 'CONNACK_ACCEPT';
connack_name(?CONNACK_PROTO_VER)   -> 'CONNACK_PROTO_VER';
connack_name(?CONNACK_INVALID_ID)  -> 'CONNACK_INVALID_ID';
connack_name(?CONNACK_SERVER)      -> 'CONNACK_SERVER';
connack_name(?CONNACK_CREDENTIALS) -> 'CONNACK_CREDENTIALS';
connack_name(?CONNACK_AUTH)        -> 'CONNACK_AUTH'.

%% @doc Format packet
-spec(format(mqtt_packet()) -> iolist()).
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
                 proto_ver     = ProtoVer,
                 proto_name    = ProtoName,
                 will_retain   = WillRetain,
                 will_qos      = WillQoS,
                 will_flag     = WillFlag,
                 clean_sess    = CleanSess,
                 keep_alive    = KeepAlive,
                 client_id     = ClientId,
                 will_topic    = WillTopic, 
                 will_msg      = WillMsg, 
                 username      = Username, 
                 password      = Password}) ->
    Format = "ClientId=~s, ProtoName=~s, ProtoVsn=~p, CleanSess=~s, KeepAlive=~p, Username=~s, Password=~s",
    Args = [ClientId, ProtoName, ProtoVer, CleanSess, KeepAlive, Username, format_password(Password)],
    {Format1, Args1} = if 
                        WillFlag -> { Format ++ ", Will(Q~p, R~p, Topic=~s, Msg=~s)",
                                      Args ++ [WillQoS, i(WillRetain), WillTopic, WillMsg] };
                        true -> {Format, Args}
                       end,
    io_lib:format(Format1, Args1);

format_variable(#mqtt_packet_connack{ack_flags   = AckFlags,
                                     return_code = ReturnCode}) ->
    io_lib:format("AckFlags=~p, RetainCode=~p", [AckFlags, ReturnCode]);

format_variable(#mqtt_packet_publish{topic_name = TopicName,
                                     packet_id  = PacketId}) ->
    io_lib:format("Topic=~s, PacketId=~p", [TopicName, PacketId]);

format_variable(#mqtt_packet_puback{packet_id = PacketId}) ->
    io_lib:format("PacketId=~p", [PacketId]);

format_variable(#mqtt_packet_subscribe{packet_id   = PacketId,
                                       topic_table = TopicTable}) ->
    io_lib:format("PacketId=~p, TopicTable=~p", [PacketId, TopicTable]);

format_variable(#mqtt_packet_unsubscribe{packet_id = PacketId,
                                         topics    = Topics}) ->
    io_lib:format("PacketId=~p, Topics=~p", [PacketId, Topics]);

format_variable(#mqtt_packet_suback{packet_id = PacketId,
                                    qos_table = QosTable}) ->
    io_lib:format("PacketId=~p, QosTable=~p", [PacketId, QosTable]);

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
