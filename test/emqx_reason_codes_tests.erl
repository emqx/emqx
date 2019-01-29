%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License")
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

-module(emqx_reason_codes_tests).

-include_lib("eunit/include/eunit.hrl").

-include("emqx_mqtt.hrl").

-import(lists, [seq/2, zip/2, foreach/2]).

-define(MQTTV4_CODE_NAMES, [connection_acceptd,
                            unacceptable_protocol_version,
                            client_identifier_not_valid,
                            server_unavaliable,
                            malformed_username_or_password,
                            unauthorized_client,
                            unknown_error]).

-define(MQTTV5_CODE_NAMES, [success, granted_qos1, granted_qos2, disconnect_with_will_message,
                            no_matching_subscribers, no_subscription_existed, continue_authentication,
                            re_authenticate, unspecified_error, malformed_Packet, protocol_error,
                            implementation_specific_error, unsupported_protocol_version,
                            client_identifier_not_valid, bad_username_or_password, not_authorized,
                            server_unavailable, server_busy, banned,server_shutting_down,
                            bad_authentication_method, keepalive_timeout, session_taken_over,
                            topic_filter_invalid, topic_name_invalid, packet_identifier_inuse,
                            packet_identifier_not_found, receive_maximum_exceeded, topic_alias_invalid,
                            packet_too_large, message_rate_too_high, quota_exceeded,
                            administrative_action, payload_format_invalid, retain_not_supported,
                            qos_not_supported, use_another_server, server_moved,
                            shared_subscriptions_not_supported, connection_rate_exceeded,
                            maximum_connect_time, subscription_identifiers_not_supported,
                            wildcard_subscriptions_not_supported, unknown_error]).

-define(MQTTV5_CODES, [16#00, 16#01, 16#02, 16#04, 16#10, 16#11, 16#18, 16#19, 16#80, 16#81, 16#82,
                       16#83, 16#84, 16#85, 16#86, 16#87, 16#88, 16#89, 16#8A, 16#8B, 16#8C, 16#8D,
                       16#8E, 16#8F, 16#90, 16#91, 16#92, 16#93, 16#94, 16#95, 16#96, 16#97, 16#98,
                       16#99, 16#9A, 16#9B, 16#9C, 16#9D, 16#9E, 16#9F, 16#A0, 16#A1, 16#A2, code]).

-define(MQTTV5_TXT, [<<"Success">>, <<"Granted QoS 1">>, <<"Granted QoS 2">>,
                     <<"Disconnect with Will Message">>, <<"No matching subscribers">>,
                     <<"No subscription existed">>, <<"Continue authentication">>,
                     <<"Re-authenticate">>, <<"Unspecified error">>, <<"Malformed Packet">>,
                     <<"Protocol Error">>, <<"Implementation specific error">>,
                     <<"Unsupported Protocol Version">>, <<"Client Identifier not valid">>,
                     <<"Bad User Name or Password">>, <<"Not authorized">>,
                     <<"Server unavailable">>, <<"Server busy">>, <<"Banned">>,
                     <<"Server shutting down">>, <<"Bad authentication method">>,
                     <<"Keep Alive timeout">>, <<"Session taken over">>,
                     <<"Topic Filter invalid">>, <<"Topic Name invalid">>,
                     <<"Packet Identifier in use">>, <<"Packet Identifier not found">>,
                     <<"Receive Maximum exceeded">>, <<"Topic Alias invalid">>,
                     <<"Packet too large">>, <<"Message rate too high">>, <<"Quota exceeded">>,
                     <<"Administrative action">>, <<"Payload format invalid">>,
                     <<"Retain not supported">>, <<"QoS not supported">>,
                     <<"Use another server">>, <<"Server moved">>,
                     <<"Shared Subscriptions not supported">>, <<"Connection rate exceeded">>,
                     <<"Maximum connect time">>, <<"Subscription Identifiers not supported">>,
                     <<"Wildcard Subscriptions not supported">>, <<"Unknown error">>]).

-define(COMPAT_CODES_V5, [16#80, 16#81, 16#82, 16#83, 16#84, 16#85, 16#86, 16#87,
                          16#88, 16#89, 16#8A, 16#8B, 16#8C, 16#97, 16#9C, 16#9D,
                          16#9F]).

-define(COMPAT_CODES_V4, [?CONNACK_PROTO_VER, ?CONNACK_PROTO_VER, ?CONNACK_PROTO_VER,
                          ?CONNACK_PROTO_VER, ?CONNACK_PROTO_VER,
                          ?CONNACK_INVALID_ID,
                          ?CONNACK_CREDENTIALS,
                          ?CONNACK_AUTH,
                          ?CONNACK_SERVER,
                          ?CONNACK_SERVER,
                          ?CONNACK_AUTH,
                          ?CONNACK_SERVER,
                          ?CONNACK_AUTH,
                          ?CONNACK_SERVER, ?CONNACK_SERVER, ?CONNACK_SERVER, ?CONNACK_SERVER]).

mqttv4_name_test() ->
    (((codes_test(?MQTT_PROTO_V4))
        (seq(0,6)))
       (?MQTTV4_CODE_NAMES))
      (fun emqx_reason_codes:name/2).

mqttv5_name_test() ->
    (((codes_test(?MQTT_PROTO_V5))
        (?MQTTV5_CODES))
       (?MQTTV5_CODE_NAMES))
      (fun emqx_reason_codes:name/2).

text_test() ->
    (((codes_test(?MQTT_PROTO_V5))
        (?MQTTV5_CODES))
       (?MQTTV5_TXT))
      (fun emqx_reason_codes:text/1).

compat_test() ->
    (((codes_test(connack))
        (?COMPAT_CODES_V5))
       (?COMPAT_CODES_V4))
      (fun emqx_reason_codes:compat/2),
    (((codes_test(suback))
        ([0,1,2, 16#80]))
       ([0,1,2, 16#80]))
      (fun emqx_reason_codes:compat/2),
    (((codes_test(unsuback))
        ([0, 1, 2]))
       ([undefined, undefined, undefined]))
      (fun emqx_reason_codes:compat/2).

codes_test(AsistVar) ->
    fun(CODES) ->
        fun(NAMES) ->
            fun(Procedure) ->
                foreach(fun({Code, Result}) ->
                            ?assertEqual(Result, case erlang:fun_info(Procedure, name) of
                                                     {name, text}   -> Procedure(Code);
                                                     {name, name}   -> Procedure(Code, AsistVar);
                                                     {name, compat} -> Procedure(AsistVar, Code)
                                                 end)
                        end, zip(CODES, NAMES))
            end
        end
    end.
