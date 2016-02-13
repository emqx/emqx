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

-module(emqttd_packet_tests).

-ifdef(TEST).

-include("emqttd_protocol.hrl").

-include_lib("eunit/include/eunit.hrl").

-define(P, emqttd_packet).

protocol_name_test() ->
    ?assertEqual(<<"MQIsdp">>, ?P:protocol_name(3)),
    ?assertEqual(<<"MQTT">>,   ?P:protocol_name(4)).

type_name_test() ->
    ?assertEqual( 'CONNECT',     ?P:type_name(?CONNECT) ),
    ?assertEqual( 'UNSUBSCRIBE', ?P:type_name(?UNSUBSCRIBE) ).

connack_name_test() ->
    ?assertEqual( 'CONNACK_ACCEPT',      ?P:connack_name(?CONNACK_ACCEPT) ),
    ?assertEqual( 'CONNACK_PROTO_VER',   ?P:connack_name(?CONNACK_PROTO_VER) ),
    ?assertEqual( 'CONNACK_INVALID_ID',  ?P:connack_name(?CONNACK_INVALID_ID) ),
    ?assertEqual( 'CONNACK_SERVER',      ?P:connack_name(?CONNACK_SERVER) ),
    ?assertEqual( 'CONNACK_CREDENTIALS', ?P:connack_name(?CONNACK_CREDENTIALS) ),
    ?assertEqual( 'CONNACK_AUTH',        ?P:connack_name(?CONNACK_AUTH) ).

format_test() ->
    ?debugFmt("~s", [?P:format(?CONNECT_PACKET(#mqtt_packet_connect{}))]),
    ?debugFmt("~s", [?P:format(?CONNACK_PACKET(?CONNACK_SERVER))]),
    ?debugFmt("~s", [?P:format(?PUBLISH_PACKET(?QOS_1, 1))]),
    ?debugFmt("~s", [?P:format(?PUBLISH_PACKET(?QOS_2, <<"topic">>, 10, <<"payload">>))]),
    ?debugFmt("~s", [?P:format(?PUBACK_PACKET(?PUBACK, 98))]),
    ?debugFmt("~s", [?P:format(?PUBREL_PACKET(99))]),
    ?debugFmt("~s", [?P:format(?SUBSCRIBE_PACKET(15, [{<<"topic">>, ?QOS0}, {<<"topic1">>, ?QOS1}]))]),
    ?debugFmt("~s", [?P:format(?SUBACK_PACKET(40, [?QOS0, ?QOS1]))]),
    ?debugFmt("~s", [?P:format(?UNSUBSCRIBE_PACKET(89, [<<"t">>, <<"t2">>]))]),
    ?debugFmt("~s", [?P:format(?UNSUBACK_PACKET(90))]).

-endif.

