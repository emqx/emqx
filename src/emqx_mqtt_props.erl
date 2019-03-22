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

%% @doc MQTT5 Properties
-module(emqx_mqtt_props).

-include("emqx_mqtt.hrl").

-export([ id/1
        , name/1
        , filter/2
        , validate/1
        ]).

-define(PROPS_TABLE,
        #{16#01 => {'Payload-Format-Indicator', 'Byte', [?PUBLISH]},
          16#02 => {'Message-Expiry-Interval', 'Four-Byte-Integer', [?PUBLISH]},
          16#03 => {'Content-Type', 'UTF8-Encoded-String', [?PUBLISH]},
          16#08 => {'Response-Topic', 'UTF8-Encoded-String', [?PUBLISH]},
          16#09 => {'Correlation-Data', 'Binary-Data', [?PUBLISH]},
          16#0B => {'Subscription-Identifier', 'Variable-Byte-Integer', [?PUBLISH, ?SUBSCRIBE]},
          16#11 => {'Session-Expiry-Interval', 'Four-Byte-Integer', [?CONNECT, ?CONNACK, ?DISCONNECT]},
          16#12 => {'Assigned-Client-Identifier', 'UTF8-Encoded-String', [?CONNACK]},
          16#13 => {'Server-Keep-Alive', 'Two-Byte-Integer', [?CONNACK]},
          16#15 => {'Authentication-Method', 'UTF8-Encoded-String', [?CONNECT, ?CONNACK, ?AUTH]},
          16#16 => {'Authentication-Data', 'Binary-Data', [?CONNECT, ?CONNACK, ?AUTH]},
          16#17 => {'Request-Problem-Information', 'Byte', [?CONNECT]},
          16#18 => {'Will-Delay-Interval', 'Four-Byte-Integer', ['WILL']},
          16#19 => {'Request-Response-Information', 'Byte', [?CONNECT]},
          16#1A => {'Response-Information', 'UTF8-Encoded-String', [?CONNACK]},
          16#1C => {'Server-Reference', 'UTF8-Encoded-String', [?CONNACK, ?DISCONNECT]},
          16#1F => {'Reason-String', 'UTF8-Encoded-String', 'ALL'},
          16#21 => {'Receive-Maximum', 'Two-Byte-Integer', [?CONNECT, ?CONNACK]},
          16#22 => {'Topic-Alias-Maximum', 'Two-Byte-Integer', [?CONNECT, ?CONNACK]},
          16#23 => {'Topic-Alias', 'Two-Byte-Integer', [?PUBLISH]},
          16#24 => {'Maximum-QoS', 'Byte', [?CONNACK]},
          16#25 => {'Retain-Available', 'Byte', [?CONNACK]},
          16#26 => {'User-Property', 'UTF8-String-Pair', 'ALL'},
          16#27 => {'Maximum-Packet-Size', 'Four-Byte-Integer', [?CONNECT, ?CONNACK]},
          16#28 => {'Wildcard-Subscription-Available', 'Byte', [?CONNACK]},
          16#29 => {'Subscription-Identifier-Available', 'Byte', [?CONNACK]},
          16#2A => {'Shared-Subscription-Available', 'Byte', [?CONNACK]}}).

name(16#01) -> 'Payload-Format-Indicator';
name(16#02) -> 'Message-Expiry-Interval';
name(16#03) -> 'Content-Type';
name(16#08) -> 'Response-Topic';
name(16#09) -> 'Correlation-Data';
name(16#0B) -> 'Subscription-Identifier';
name(16#11) -> 'Session-Expiry-Interval';
name(16#12) -> 'Assigned-Client-Identifier';
name(16#13) -> 'Server-Keep-Alive';
name(16#15) -> 'Authentication-Method';
name(16#16) -> 'Authentication-Data';
name(16#17) -> 'Request-Problem-Information';
name(16#18) -> 'Will-Delay-Interval';
name(16#19) -> 'Request-Response-Information';
name(16#1A) -> 'Response-Information';
name(16#1C) -> 'Server-Reference';
name(16#1F) -> 'Reason-String';
name(16#21) -> 'Receive-Maximum';
name(16#22) -> 'Topic-Alias-Maximum';
name(16#23) -> 'Topic-Alias';
name(16#24) -> 'Maximum-QoS';
name(16#25) -> 'Retain-Available';
name(16#26) -> 'User-Property';
name(16#27) -> 'Maximum-Packet-Size';
name(16#28) -> 'Wildcard-Subscription-Available';
name(16#29) -> 'Subscription-Identifier-Available';
name(16#2A) -> 'Shared-Subscription-Available'.

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
id('Authentication-Data')               -> 16#16;
id('Request-Problem-Information')       -> 16#17;
id('Will-Delay-Interval')               -> 16#18;
id('Request-Response-Information')      -> 16#19;
id('Response-Information')              -> 16#1A;
id('Server-Reference')                  -> 16#1C;
id('Reason-String')                     -> 16#1F;
id('Receive-Maximum')                   -> 16#21;
id('Topic-Alias-Maximum')               -> 16#22;
id('Topic-Alias')                       -> 16#23;
id('Maximum-QoS')                       -> 16#24;
id('Retain-Available')                  -> 16#25;
id('User-Property')                     -> 16#26;
id('Maximum-Packet-Size')               -> 16#27;
id('Wildcard-Subscription-Available')   -> 16#28;
id('Subscription-Identifier-Available') -> 16#29;
id('Shared-Subscription-Available')     -> 16#2A.

filter(PacketType, Props) when is_map(Props) ->
    maps:from_list(filter(PacketType, maps:to_list(Props)));

filter(PacketType, Props) when ?CONNECT =< PacketType, PacketType =< ?AUTH, is_list(Props) ->
    Filter = fun(Name) ->
                 case maps:find(id(Name), ?PROPS_TABLE) of
                     {ok, {Name, _Type, 'ALL'}} ->
                         true;
                     {ok, {Name, _Type, AllowedTypes}} ->
                         lists:member(PacketType, AllowedTypes);
                     error -> false
                 end
             end,
    [Prop || Prop = {Name, _} <- Props, Filter(Name)].

validate(Props) when is_map(Props) ->
    lists:foreach(fun validate_prop/1, maps:to_list(Props)).

validate_prop(Prop = {Name, Val}) ->
    case maps:find(id(Name), ?PROPS_TABLE) of
        {ok, {Name, Type, _}} ->
            validate_value(Type, Val)
              orelse error(bad_property, Prop);
        error ->
            error({bad_property, Prop})
    end.

validate_value('Byte', Val) ->
    is_integer(Val);
validate_value('Two-Byte-Integer', Val) ->
    is_integer(Val);
validate_value('Four-Byte-Integer', Val) ->
    is_integer(Val);
validate_value('Variable-Byte-Integer', Val) ->
    is_integer(Val);
validate_value('UTF8-Encoded-String', Val)  ->
    is_binary(Val);
validate_value('Binary-Data', Val) ->
    is_binary(Val);
validate_value('UTF8-String-Pair', Val) ->
    is_tuple(Val) orelse is_list(Val).

