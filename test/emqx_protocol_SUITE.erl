%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqtt.io)
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

-module(emqx_protocol_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").

-import(emqx_serializer, [serialize/1]).

all() ->
    [%% {group, parser},
     %% {group, serializer},
     {group, packet},
     {group, message}].

groups() ->
    [%% {parser, [],
     %%  [
     %%   parse_connect,
     %%   parse_bridge,
     %%   parse_publish,
     %%   parse_puback,
     %%   parse_pubrec,
     %%   parse_pubrel,
     %%   parse_pubcomp,
     %%   parse_subscribe,
     %%   parse_unsubscribe,
     %%   parse_pingreq,
     %%   parse_disconnect]},
     %% {serializer, [],
     %%  [serialize_connect,
     %%   serialize_connack,
     %%   serialize_publish,
     %%   serialize_puback,
     %%   serialize_pubrel,
     %%   serialize_subscribe,
     %%   serialize_suback,
     %%   serialize_unsubscribe,
     %%   serialize_unsuback,
     %%   serialize_pingreq,
     %%   serialize_pingresp,
     %%   serialize_disconnect]},
     {packet, [],
      [packet_proto_name,
       packet_type_name,
       packet_format]},
     {message, [],
      [message_make
       %% message_from_packet
      ]}
    ].



%%--------------------------------------------------------------------
%% Packet Cases
%%--------------------------------------------------------------------

packet_proto_name(_) ->
    ?assertEqual(<<"MQIsdp">>, emqx_packet:protocol_name(3)),
    ?assertEqual(<<"MQTT">>, emqx_packet:protocol_name(4)).

packet_type_name(_) ->
    ?assertEqual('CONNECT',     emqx_packet:type_name(?CONNECT)),
    ?assertEqual('UNSUBSCRIBE', emqx_packet:type_name(?UNSUBSCRIBE)).

%% packet_connack_name(_) ->
%%     ?assertEqual('CONNACK_ACCEPT',      emqx_packet:connack_name(?CONNACK_ACCEPT)),
%%     ?assertEqual('CONNACK_PROTO_VER',   emqx_packet:connack_name(?CONNACK_PROTO_VER)),
%%     ?assertEqual('CONNACK_INVALID_ID',  emqx_packet:connack_name(?CONNACK_INVALID_ID)),
%%     ?assertEqual('CONNACK_SERVER',      emqx_packet:connack_name(?CONNACK_SERVER)),
%%     ?assertEqual('CONNACK_CREDENTIALS', emqx_packet:connack_name(?CONNACK_CREDENTIALS)),
%%     ?assertEqual('CONNACK_AUTH',        emqx_packet:connack_name(?CONNACK_AUTH)).

packet_format(_) ->
    io:format("~s", [emqx_packet:format(?CONNECT_PACKET(#mqtt_packet_connect{}))]),
    io:format("~s", [emqx_packet:format(?CONNACK_PACKET(?CONNACK_SERVER))]),
    io:format("~s", [emqx_packet:format(?PUBLISH_PACKET(?QOS_1, 1))]),
    io:format("~s", [emqx_packet:format(?PUBLISH_PACKET(?QOS_2, <<"topic">>, 10, <<"payload">>))]),
    io:format("~s", [emqx_packet:format(?PUBACK_PACKET(?PUBACK, 98))]),
    io:format("~s", [emqx_packet:format(?PUBREL_PACKET(99))]),
    io:format("~s", [emqx_packet:format(?SUBSCRIBE_PACKET(15, [{<<"topic">>, ?QOS0}, {<<"topic1">>, ?QOS1}]))]),
    io:format("~s", [emqx_packet:format(?SUBACK_PACKET(40, [?QOS0, ?QOS1]))]),
    io:format("~s", [emqx_packet:format(?UNSUBSCRIBE_PACKET(89, [<<"t">>, <<"t2">>]))]),
    io:format("~s", [emqx_packet:format(?UNSUBACK_PACKET(90))]).

%%--------------------------------------------------------------------
%% Message Cases
%%--------------------------------------------------------------------

message_make(_) ->
    Msg = emqx_message:make(<<"clientid">>, <<"topic">>, <<"payload">>),
    ?assertEqual(0, Msg#message.qos),
    Msg1 = emqx_message:make(<<"clientid">>, qos2, <<"topic">>, <<"payload">>),
    ?assert(is_binary(Msg1#message.id)),
    ?assertEqual(qos2, Msg1#message.qos).

%% message_from_packet(_) ->
%%     Msg = emqx_message:from_packet(?PUBLISH_PACKET(1, <<"topic">>, 10, <<"payload">>)),
%%     ?assertEqual(1, Msg#message.qos),
%%     %% ?assertEqual(10, Msg#message.pktid),
%%     ?assertEqual(<<"topic">>, Msg#message.topic),
%%     WillMsg = emqx_message:from_packet(#mqtt_packet_connect{will_flag  = true,
%%                                                             will_topic = <<"WillTopic">>,
%%                                                             will_payload   = <<"WillMsg">>}),
%%     ?assertEqual(<<"WillTopic">>, WillMsg#message.topic),
%%     ?assertEqual(<<"WillMsg">>, WillMsg#message.payload).

    %% Msg2 = emqx_message:fomat_packet(<<"username">>, <<"clientid">>,
    %%                                   ?PUBLISH_PACKET(1, <<"topic">>, 20, <<"payload">>)),


