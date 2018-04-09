%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_mqtt_rscode).

-export([value/1]).

%%--------------------------------------------------------------------
%% Reason code to name
%%--------------------------------------------------------------------

%% 00: Success; CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH
value('Success') -> 16#00;
%% 00: Normal disconnection; DISCONNECT
value('Normal-Disconnection') -> 16#00;
%% 00: Granted QoS 0; SUBACK
value('Granted-QoS0') -> 16#00;
%% 01: Granted QoS 1; SUBACK
value('Granted-QoS1') -> 16#01;
%% 02: Granted QoS 2; SUBACK
value('Granted-QoS2') -> 16#02;
%% 04: Disconnect with Will Message; DISCONNECT
value('Disconnect-With-Will-Message') -> 16#04;
%% 16: No matching subscribers; PUBACK, PUBREC
value('No-Matching-Subscribers') -> 16#10;
%% 17: No subscription existed; UNSUBACK
value('No-Subscription-Existed') -> 16#11;
%% 24: Continue authentication; AUTH
value('Continue-Authentication') -> 16#18;
%% 25: Re-Authenticate; AUTH
value('Re-Authenticate') -> 16#19;
%% 128: Unspecified error; CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
value('Unspecified-Error') -> 16#80;
%% 129: Malformed Packet; CONNACK, DISCONNECT
value('Malformed-Packet') -> 16#81;
%% 130: Protocol Error; CONNACK, DISCONNECT
value('Protocol-Error') -> 16#82;
%% 131: Implementation specific error; CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
value('Implementation-Specific-Error') -> 16#83;
%% 132: Unsupported Protocol Version; CONNACK
value('Unsupported-Protocol-Version') -> 16#84;
%% 133: Client Identifier not valid; CONNACK
value('Client-Identifier-not-Valid') -> 16#85;
%% 134: Bad User Name or Password; CONNACK
value('Bad-Username-or-Password') -> 16#86;
%% 135: Not authorized; CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
value('Not-Authorized') -> 16#87;
%% 136: Server unavailable; CONNACK
value('Server-Unavailable') -> 16#88;
%% 137: Server busy; CONNACK, DISCONNECT
value('Server-Busy') -> 16#89;
%% 138: Banned; CONNACK
value('Banned') -> 16#8A;
%% 139: Server shutting down; DISCONNECT
value('Server-Shutting-Down') -> 16#8B;
%% 140: Bad authentication method; CONNACK, DISCONNECT
value('Bad-Authentication-Method') -> 16#8C;
%% 141: Keep Alive timeout; DISCONNECT
value('Keep-Alive-Timeout') -> 16#8D;
%% 142: Session taken over; DISCONNECT
value('Session-Taken-Over') -> 16#8E;
%% 143: Topic Filter invalid; SUBACK, UNSUBACK, DISCONNECT
value('Topic-Filter-Invalid') -> 16#8F;
%% 144: Topic Name invalid; CONNACK, PUBACK, PUBREC, DISCONNECT
value('Topic-Name-Invalid') -> 16#90;
%% 145: Packet Identifier in use; PUBACK, PUBREC, SUBACK, UNSUBACK
value('Packet-Identifier-Inuse') -> 16#91;
%% 146: Packet Identifier not found; PUBREL, PUBCOMP
value('Packet-Identifier-Not-Found') -> 16#92;
%% 147: Receive Maximum exceeded; DISCONNECT
value('Receive-Maximum-Exceeded') -> 16#93;
%% 148: Topic Alias invalid; DISCONNECT
value('Topic-Alias-Invalid') -> 16#94;
%% 149: Packet too large; CONNACK, DISCONNECT
value('Packet-Too-Large') -> 16#95;
%% 150: Message rate too high; DISCONNECT
value('Message-Rate-Too-High') -> 16#96;
%% 151: Quota exceeded; CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT
value('Quota-Exceeded') -> 16#97;
%% 152: Administrative action; DISCONNECT
value('Administrative-Action') -> 16#98;
%% 153: Payload format invalid; CONNACK, PUBACK, PUBREC, DISCONNECT
value('Payload-Format-Invalid') -> 16#99;
%% 154: Retain not supported; CONNACK, DISCONNECT
value('Retain-Not-Supported') -> 16#9A;
%% 155: QoS not supported; CONNACK, DISCONNECT
value('QoS-Not-Supported') -> 16#9B;
%% 156: Use another server; CONNACK, DISCONNECT
value('Use-Another-Server') -> 16#9C;
%% 157: Server moved; CONNACK, DISCONNECT
value('Server-Moved') -> 16#9D;
%% 158: Shared Subscriptions not supported; SUBACK, DISCONNECT
value('Shared-Subscriptions-Not-Supported') -> 16#9E;
%% 159: Connection rate exceeded; CONNACK, DISCONNECT
value('Connection-Rate-Exceeded') -> 16#9F;
%% 160: Maximum connect time; DISCONNECT
value('Maximum-Connect-Time') -> 16#A0;
%% 161: Subscription Identifiers not supported; SUBACK, DISCONNECT
value('Subscription-Identifiers-Not-Supported') -> 16#A1;
%% 162: Wildcard-Subscriptions-Not-Supported; SUBACK, DISCONNECT
value('Wildcard-Subscriptions-Not-Supported') -> 16#A2.

