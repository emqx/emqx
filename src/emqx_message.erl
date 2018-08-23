%% Copyright (c) 2018 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_message).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").

-export([make/2, make/3, make/4]).
-export([set_flags/2]).
-export([get_flag/2, get_flag/3, set_flag/2, set_flag/3, unset_flag/2]).
-export([set_headers/2]).
-export([get_header/2, get_header/3, set_header/3]).

-spec(make(topic(), payload()) -> message()).
make(Topic, Payload) ->
    make(undefined, Topic, Payload).

-spec(make(atom() | client_id(), topic(), payload()) -> message()).
make(From, Topic, Payload) ->
    make(From, ?QOS0, Topic, Payload).

-spec(make(atom() | client_id(), qos(), topic(), payload()) -> message()).
make(From, QoS, Topic, Payload) ->
    #message{id         = msgid(QoS),
             qos        = QoS,
             from       = From,
             flags      = #{dup => false},
             topic      = Topic,
             payload    = Payload,
             timestamp  = os:timestamp()}.

msgid(?QOS0) -> undefined;
msgid(_QoS)  -> emqx_guid:gen().

set_flags(Flags, Msg = #message{flags = undefined}) when is_map(Flags) ->
    Msg#message{flags = Flags};
set_flags(New, Msg = #message{flags = Old}) when is_map(New) ->
    Msg#message{flags = maps:merge(Old, New)}.

get_flag(Flag, Msg) ->
    get_flag(Flag, Msg, false).
get_flag(Flag, #message{flags = Flags}, Default) ->
    maps:get(Flag, Flags, Default).

-spec(set_flag(message_flag(), message()) -> message()).
set_flag(Flag, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, true, Flags)}.

-spec(set_flag(message_flag(), boolean() | integer(), message()) -> message()).
set_flag(Flag, Val, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, Val, Flags)}.

-spec(unset_flag(message_flag(), message()) -> message()).
unset_flag(Flag, Msg = #message{flags = Flags}) ->
    Msg#message{flags = maps:remove(Flag, Flags)}.

set_headers(Headers, Msg = #message{headers = undefined}) when is_map(Headers) ->
    Msg#message{headers = Headers};
set_headers(New, Msg = #message{headers = Old}) when is_map(New) ->
    Msg#message{headers = maps:merge(Old, New)};
set_headers(_, Msg) ->
    Msg.

get_header(Hdr, Msg) ->
    get_header(Hdr, Msg, undefined).
get_header(Hdr, #message{headers = Headers}, Default) ->
    maps:get(Hdr, Headers, Default).

set_header(Hdr, Val, Msg = #message{headers = undefined}) ->
    Msg#message{headers = #{Hdr => Val}};
set_header(Hdr, Val, Msg = #message{headers = Headers}) ->
    Msg#message{headers = maps:put(Hdr, Val, Headers)}.

