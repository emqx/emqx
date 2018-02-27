%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. (http://emqx.io)
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

-module(emqx_mqtt5_rscode).

-author("Feng Lee <feng@emqtt.io>").

-export([name/1, value/1]).

%%--------------------------------------------------------------------
%% Reason code to name
%%--------------------------------------------------------------------

0
name(0x00
Success
CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, UNSUBACK, AUTH
0
name(0x00
Normal disconnection
DISCONNECT
0
name(0x00
Granted QoS 0
SUBACK
1
name(0x01
Granted QoS 1
SUBACK
2
name(0x02
Granted QoS 2
SUBACK
4
name(0x04
Disconnect with Will Message
DISCONNECT
16
name(0x10
No matching subscribers
PUBACK, PUBREC
17
name(0x11
No subscription existed
UNSUBACK
24
name(0x18
Continue authentication
AUTH
25
name(0x19
Re-authenticate
AUTH
128
name(0x80
Unspecified error
CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
129
name(0x81
Malformed Packet
CONNACK, DISCONNECT
130
name(0x82
Protocol Error
CONNACK, DISCONNECT
131
name(0x83
Implementation specific error
CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
132
name(0x84
Unsupported Protocol Version
CONNACK
133
name(0x85
Client Identifier not valid
CONNACK
134
name(0x86
Bad User Name or Password
CONNACK
135
name(0x87
Not authorized
CONNACK, PUBACK, PUBREC, SUBACK, UNSUBACK, DISCONNECT
136
name(0x88
Server unavailable
CONNACK
137
name(0x89
Server busy
CONNACK, DISCONNECT
138
name(0x8A
Banned
CONNACK
139
name(0x8B
Server shutting down
DISCONNECT
140
name(0x8C
Bad authentication method
CONNACK, DISCONNECT
141
name(0x8D
Keep Alive timeout
DISCONNECT
142
name(0x8E
Session taken over
DISCONNECT
143
name(0x8F
Topic Filter invalid
SUBACK, UNSUBACK, DISCONNECT
144
name(0x90
Topic Name invalid
CONNACK, PUBACK, PUBREC, DISCONNECT
145
name(0x91
Packet Identifier in use
PUBACK, PUBREC, SUBACK, UNSUBACK
146
name(0x92
Packet Identifier not found
PUBREL, PUBCOMP
147
name(0x93
Receive Maximum exceeded
DISCONNECT
148
name(0x94
Topic Alias invalid
DISCONNECT
149
name(0x95
Packet too large
CONNACK, DISCONNECT
150
name(0x96
Message rate too high
DISCONNECT
151
name(0x97
Quota exceeded
CONNACK, PUBACK, PUBREC, SUBACK, DISCONNECT
%% 152
name(0x98
Administrative action
DISCONNECT
%% 153
name(0x99
Payload format invalid
CONNACK, PUBACK, PUBREC, DISCONNECT
%% 154
name(0x9A
Retain not supported
CONNACK, DISCONNECT
%% 155
name(0x9B
QoS not supported
CONNACK, DISCONNECT
%% 156
name(0x9C
Use another server
CONNACK, DISCONNECT
%% 157: CONNACK, DISCONNECT
name(0x9D) -> 'Server-Moved';
%% 158: SUBACK, DISCONNECT
name(0x9E) -> 'Shared-Subscriptions-Not-Supported';
%% 159: CONNACK, DISCONNECT
name(0x9F) -> 'Connection-Rate-Exceeded';
%% 160: DISCONNECT
name(0xA0) -> 'Maximum-Connect-Time';
%% 161: SUBACK, DISCONNECT
name(0xA1) -> 'Subscription-Identifiers-Not-Supported';
%% 162: SUBACK, DISCONNECT
name(0xA2) -> 'Wildcard-Subscriptions-Not-Supported';

