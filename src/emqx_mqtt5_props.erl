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

-module(emqx_mqtt5_props).

-author("Feng Lee <feng@emqtt.io>").

-export([name/1, id/1]).

%%--------------------------------------------------------------------
%% Property id to name
%%--------------------------------------------------------------------

%% 01: Byte; PUBLISH, Will Properties
name(16#01) -> 'Payload-Format-Indicator';
%% 02: Four Byte Integer; PUBLISH, Will Properties
name(16#02) -> 'Message-Expiry-Interval';
%% 03: UTF-8 Encoded String; PUBLISH, Will Properties
name(16#03) -> 'Content-Type';
%% 08: UTF-8 Encoded String; PUBLISH, Will Properties
name(16#08) -> 'Response-Topic';
%% 09: Binary Data; PUBLISH, Will Properties
name(16#09) -> 'Correlation-Data';
%% 11: Variable Byte Integer; PUBLISH, SUBSCRIBE
name(16#0B) -> 'Subscription-Identifier';
%% 17: Four Byte Integer; CONNECT, CONNACK, DISCONNECT
name(16#11) -> 'Session-Expiry-Interval';
%% 18: UTF-8 Encoded String; CONNACK
name(16#12) -> 'Assigned-Client-Identifier';
%% 19: Two Byte Integer; CONNACK
name(16#13) -> 'Server-Keep-Alive';
%% 21: UTF-8 Encoded String; CONNECT, CONNACK, AUTH
name(16#15) -> 'Authentication-Method';
%% 22: Binary Data; CONNECT, CONNACK, AUTH
name(16#16) -> 'Authentication-Data';
%% 23: Byte; CONNECT
name(16#17) -> 'Request-Problem-Information';
%% 24: Four Byte Integer; Will Properties
name(16#18) -> 'Will-Delay-Interval';
%% 25: Byte; CONNECT
name(16#19) -> 'Request-Response-Information';
%% 26: UTF-8 Encoded String; CONNACK
name(16#1A) -> 'Response Information';
%% 28: UTF-8 Encoded String; CONNACK, DISCONNECT
name(16#1C) -> 'Server-Reference';
%% 31: UTF-8 Encoded String; CONNACK, PUBACK, PUBREC, PUBREL, PUBCOMP, SUBACK, UNSUBACK, DISCONNECT, AUTH
name(16#1F) -> 'Reason-String';
%% 33: Two Byte Integer; CONNECT, CONNACK
name(16#21) -> 'Receive-Maximum';
%% 34: Two Byte Integer; CONNECT, CONNACK
name(16#22) -> 'Topic-Alias-Maximum';
%% 35: Two Byte Integer; PUBLISH
name(16#23) -> 'Topic Alias';
%% 36: Byte; CONNACK
name(16#24) -> 'Maximum-QoS';
%% 37: Byte; CONNACK
name(16#25) -> 'Retain-Available';
%% 38: UTF-8 String Pair; ALL
name(16#26) -> 'User-Property';
%% 39: Four Byte Integer; CONNECT, CONNACK
name(16#27) -> 'Maximum-Packet-Size';
%% 40: Byte; CONNACK
name(16#28) -> 'Wildcard-Subscription-Available';
%% 41: Byte; CONNACK
name(16#29) -> 'Subscription-Identifier-Available';
%% 42: Byte; CONNACK
name(16#2A) -> 'Shared-Subscription-Available'.

%%--------------------------------------------------------------------
%% Property name to id
%%--------------------------------------------------------------------

id('Payload-Format-Indicator')          -> 16#01;
id('Message-Expiry-Interval')           -> 16#02;
id('Content-Type')                      -> 16#03;
id('Response-Topic')                    -> 16#08;
id('Correlation-Data')                  -> 16#09;
id('Subscription-Identifier')           -> 16#0B;
id('Session-Expiry-Interval')           -> 16#11;
id('Assigned-Client-Identifier')        -> 16#12;
id('Server-Keep-Alive')                 -> 16#13;
id('Authentication-Method')             -> 16#15;
id('Authentication Data')               -> 16#16;
id('Request-Problem-Information')       -> 16#17;
id('Will-Delay-Interval')               -> 16#18;
id('Request-Response-Information')      -> 16#19;
id('Response Information')              -> 16#1A;
id('Server-Reference')                  -> 16#1C;
id('Reason-String')                     -> 16#1F;
id('Receive-Maximum')                   -> 16#21;
id('Topic-Alias-Maximum')               -> 16#22;
id('Topic Alias')                       -> 16#23;
id('Maximum-QoS')                       -> 16#24;
id('Retain-Available')                  -> 16#25;
id('User-Property')                     -> 16#26;
id('Maximum-Packet-Size')               -> 16#27;
id('Wildcard-Subscription-Available')   -> 16#28;
id('Subscription-Identifier-Available') -> 16#29;
id('Shared-Subscription-Available')     -> 16#2A.

