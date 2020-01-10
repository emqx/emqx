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

%% @doc MQTT5 reason codes
-module(emqx_reason_codes).

-include("emqx_mqtt.hrl").

-export([ name/1
        , name/2
        , text/1
        , text/2
        ]).

-export([ frame_error/1
        , connack_error/1
        ]).

-export([compat/2]).

name(I, Ver) when Ver >= ?MQTT_PROTO_V5 ->
    name(I);
name(0, _Ver) -> connection_accepted;
name(1, _Ver) -> unacceptable_protocol_version;
name(2, _Ver) -> client_identifier_not_valid;
name(3, _Ver) -> server_unavaliable;
name(4, _Ver) -> malformed_username_or_password;
name(5, _Ver) -> unauthorized_client;
name(_, _Ver) -> unknown_error.

name(16#00) -> success;
name(16#01) -> granted_qos1;
name(16#02) -> granted_qos2;
name(16#04) -> disconnect_with_will_message;
name(16#10) -> no_matching_subscribers;
name(16#11) -> no_subscription_existed;
name(16#18) -> continue_authentication;
name(16#19) -> re_authenticate;
name(16#80) -> unspecified_error;
name(16#81) -> malformed_Packet;
name(16#82) -> protocol_error;
name(16#83) -> implementation_specific_error;
name(16#84) -> unsupported_protocol_version;
name(16#85) -> client_identifier_not_valid;
name(16#86) -> bad_username_or_password;
name(16#87) -> not_authorized;
name(16#88) -> server_unavailable;
name(16#89) -> server_busy;
name(16#8A) -> banned;
name(16#8B) -> server_shutting_down;
name(16#8C) -> bad_authentication_method;
name(16#8D) -> keepalive_timeout;
name(16#8E) -> session_taken_over;
name(16#8F) -> topic_filter_invalid;
name(16#90) -> topic_name_invalid;
name(16#91) -> packet_identifier_inuse;
name(16#92) -> packet_identifier_not_found;
name(16#93) -> receive_maximum_exceeded;
name(16#94) -> topic_alias_invalid;
name(16#95) -> packet_too_large;
name(16#96) -> message_rate_too_high;
name(16#97) -> quota_exceeded;
name(16#98) -> administrative_action;
name(16#99) -> payload_format_invalid;
name(16#9A) -> retain_not_supported;
name(16#9B) -> qos_not_supported;
name(16#9C) -> use_another_server;
name(16#9D) -> server_moved;
name(16#9E) -> shared_subscriptions_not_supported;
name(16#9F) -> connection_rate_exceeded;
name(16#A0) -> maximum_connect_time;
name(16#A1) -> subscription_identifiers_not_supported;
name(16#A2) -> wildcard_subscriptions_not_supported;
name(_Code) -> unknown_error.

text(I, Ver) when Ver >= ?MQTT_PROTO_V5 ->
    text(I);
text(0, _Ver) -> <<"Connection accepted">>;
text(1, _Ver) -> <<"unacceptable_protocol_version">>;
text(2, _Ver) -> <<"client_identifier_not_valid">>;
text(3, _Ver) -> <<"server_unavaliable">>;
text(4, _Ver) -> <<"malformed_username_or_password">>;
text(5, _Ver) -> <<"unauthorized_client">>;
text(_, _Ver) -> <<"unknown_error">>.

text(16#00) -> <<"Success">>;
text(16#01) -> <<"Granted QoS 1">>;
text(16#02) -> <<"Granted QoS 2">>;
text(16#04) -> <<"Disconnect with Will Message">>;
text(16#10) -> <<"No matching subscribers">>;
text(16#11) -> <<"No subscription existed">>;
text(16#18) -> <<"Continue authentication">>;
text(16#19) -> <<"Re-authenticate">>;
text(16#80) -> <<"Unspecified error">>;
text(16#81) -> <<"Malformed Packet">>;
text(16#82) -> <<"Protocol Error">>;
text(16#83) -> <<"Implementation specific error">>;
text(16#84) -> <<"Unsupported Protocol Version">>;
text(16#85) -> <<"Client Identifier not valid">>;
text(16#86) -> <<"Bad User Name or Password">>;
text(16#87) -> <<"Not authorized">>;
text(16#88) -> <<"Server unavailable">>;
text(16#89) -> <<"Server busy">>;
text(16#8A) -> <<"Banned">>;
text(16#8B) -> <<"Server shutting down">>;
text(16#8C) -> <<"Bad authentication method">>;
text(16#8D) -> <<"Keep Alive timeout">>;
text(16#8E) -> <<"Session taken over">>;
text(16#8F) -> <<"Topic Filter invalid">>;
text(16#90) -> <<"Topic Name invalid">>;
text(16#91) -> <<"Packet Identifier in use">>;
text(16#92) -> <<"Packet Identifier not found">>;
text(16#93) -> <<"Receive Maximum exceeded">>;
text(16#94) -> <<"Topic Alias invalid">>;
text(16#95) -> <<"Packet too large">>;
text(16#96) -> <<"Message rate too high">>;
text(16#97) -> <<"Quota exceeded">>;
text(16#98) -> <<"Administrative action">>;
text(16#99) -> <<"Payload format invalid">>;
text(16#9A) -> <<"Retain not supported">>;
text(16#9B) -> <<"QoS not supported">>;
text(16#9C) -> <<"Use another server">>;
text(16#9D) -> <<"Server moved">>;
text(16#9E) -> <<"Shared Subscriptions not supported">>;
text(16#9F) -> <<"Connection rate exceeded">>;
text(16#A0) -> <<"Maximum connect time">>;
text(16#A1) -> <<"Subscription Identifiers not supported">>;
text(16#A2) -> <<"Wildcard Subscriptions not supported">>;
text(_Code) -> <<"Unknown error">>.

compat(connack, 16#80) -> ?CONNACK_PROTO_VER;
compat(connack, 16#81) -> ?CONNACK_PROTO_VER;
compat(connack, 16#82) -> ?CONNACK_PROTO_VER;
compat(connack, 16#83) -> ?CONNACK_PROTO_VER;
compat(connack, 16#84) -> ?CONNACK_PROTO_VER;
compat(connack, 16#85) -> ?CONNACK_INVALID_ID;
compat(connack, 16#86) -> ?CONNACK_CREDENTIALS;
compat(connack, 16#87) -> ?CONNACK_AUTH;
compat(connack, 16#88) -> ?CONNACK_SERVER;
compat(connack, 16#89) -> ?CONNACK_SERVER;
compat(connack, 16#8A) -> ?CONNACK_AUTH;
compat(connack, 16#8B) -> ?CONNACK_SERVER;
compat(connack, 16#8C) -> ?CONNACK_AUTH;
compat(connack, 16#90) -> ?CONNACK_SERVER;
compat(connack, 16#97) -> ?CONNACK_SERVER;
compat(connack, 16#9C) -> ?CONNACK_SERVER;
compat(connack, 16#9D) -> ?CONNACK_SERVER;
compat(connack, 16#9F) -> ?CONNACK_SERVER;

compat(suback, Code) when Code =< ?QOS_2 -> Code;
compat(suback, Code) when Code >= 16#80  -> 16#80;

compat(unsuback, _Code) -> undefined;
compat(_Other, _Code) -> undefined.

frame_error(frame_too_large) -> ?RC_PACKET_TOO_LARGE;
frame_error(_) -> ?RC_MALFORMED_PACKET.

connack_error(client_identifier_not_valid) -> ?RC_CLIENT_IDENTIFIER_NOT_VALID;
connack_error(bad_username_or_password) -> ?RC_BAD_USER_NAME_OR_PASSWORD;
connack_error(bad_clientid_or_password) -> ?RC_BAD_USER_NAME_OR_PASSWORD;
connack_error(username_or_password_undefined) -> ?RC_BAD_USER_NAME_OR_PASSWORD;
connack_error(password_error) -> ?RC_BAD_USER_NAME_OR_PASSWORD;
connack_error(not_authorized) -> ?RC_NOT_AUTHORIZED;
connack_error(server_unavailable) -> ?RC_SERVER_UNAVAILABLE;
connack_error(server_busy) -> ?RC_SERVER_BUSY;
connack_error(banned) -> ?RC_BANNED;
connack_error(bad_authentication_method) -> ?RC_BAD_AUTHENTICATION_METHOD;
%% TODO: ???
connack_error(_) -> ?RC_NOT_AUTHORIZED.

