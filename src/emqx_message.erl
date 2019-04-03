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

-module(emqx_message).

-include("emqx.hrl").
-include("emqx_mqtt.hrl").
-include("types.hrl").

%% Create
-export([ make/2
        , make/3
        , make/4
        ]).

%% Fields
-export([ id/1
        , qos/1
        , from/1
        , topic/1
        , payload/1
        , timestamp/1
        ]).

%% Flags
-export([ get_flag/2
        , get_flag/3
        , set_flag/2
        , set_flag/3
        , set_flags/2
        , unset_flag/2
        ]).

%% Headers
-export([ get_headers/1
        , get_header/2
        , get_header/3
        , set_header/3
        , set_headers/2
        , remove_header/2
        ]).

-export([ is_expired/1
        , update_expiry/1
        ]).

-export([ to_map/1
        , to_list/1
        ]).

-export([format/1]).

-type(flag() :: atom()).

-spec(make(emqx_topic:topic(), emqx_types:payload()) -> emqx_types:message()).
make(Topic, Payload) ->
    make(undefined, Topic, Payload).

-spec(make(atom() | emqx_types:client_id(),
           emqx_topic:topic(),
           emqx_types:payload()) -> emqx_types:message()).
make(From, Topic, Payload) ->
    make(From, ?QOS_0, Topic, Payload).

-spec(make(atom() | emqx_types:client_id(),
           emqx_mqtt_types:qos(),
           emqx_topic:topic(),
           emqx_types:payload()) -> emqx_types:message()).
make(From, QoS, Topic, Payload) when ?QOS_0 =< QoS, QoS =< ?QOS_2 ->
    #message{id = emqx_guid:gen(),
             qos = QoS,
             from = From,
             flags = #{dup => false},
             topic = Topic,
             payload = Payload,
             timestamp = os:timestamp()}.

-spec(id(emqx_types:message()) -> maybe(binary())).
id(#message{id = Id}) -> Id.

-spec(qos(emqx_types:message()) -> emqx_mqtt_types:qos()).
qos(#message{qos = QoS}) -> QoS.

-spec(from(emqx_types:message()) -> atom() | binary()).
from(#message{from = From}) -> From.

-spec(topic(emqx_types:message()) -> emqx_types:topic()).
topic(#message{topic = Topic}) -> Topic.

-spec(payload(emqx_types:message()) -> emqx_types:payload()).
payload(#message{payload = Payload}) -> Payload.

-spec(timestamp(emqx_types:message()) -> erlang:timestamp()).
timestamp(#message{timestamp = TS}) -> TS.

-spec(set_flags(map(), emqx_types:message()) -> emqx_types:message()).
set_flags(Flags, Msg = #message{flags = undefined}) when is_map(Flags) ->
    Msg#message{flags = Flags};
set_flags(New, Msg = #message{flags = Old}) when is_map(New) ->
    Msg#message{flags = maps:merge(Old, New)}.

-spec(get_flag(flag(), emqx_types:message()) -> boolean()).
get_flag(Flag, Msg) ->
    get_flag(Flag, Msg, false).
get_flag(Flag, #message{flags = Flags}, Default) ->
    maps:get(Flag, Flags, Default).

-spec(set_flag(flag(), emqx_types:message()) -> emqx_types:message()).
set_flag(Flag, Msg = #message{flags = undefined}) when is_atom(Flag) ->
    Msg#message{flags = #{Flag => true}};
set_flag(Flag, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, true, Flags)}.

-spec(set_flag(flag(), boolean() | integer(), emqx_types:message())
      -> emqx_types:message()).
set_flag(Flag, Val, Msg = #message{flags = undefined}) when is_atom(Flag) ->
    Msg#message{flags = #{Flag => Val}};
set_flag(Flag, Val, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, Val, Flags)}.

-spec(unset_flag(flag(), emqx_types:message()) -> emqx_types:message()).
unset_flag(Flag, Msg = #message{flags = Flags}) ->
    case maps:is_key(Flag, Flags) of
        true ->
            Msg#message{flags = maps:remove(Flag, Flags)};
        false -> Msg
    end.

-spec(set_headers(undefined | map(), emqx_types:message()) -> emqx_types:message()).
set_headers(Headers, Msg = #message{headers = undefined}) when is_map(Headers) ->
    Msg#message{headers = Headers};
set_headers(New, Msg = #message{headers = Old}) when is_map(New) ->
    Msg#message{headers = maps:merge(Old, New)};
set_headers(undefined, Msg) -> Msg.

-spec(get_headers(emqx_types:message()) -> map()).
get_headers(Msg) ->
    Msg#message.headers.

-spec(get_header(term(), emqx_types:message()) -> term()).
get_header(Hdr, Msg) ->
    get_header(Hdr, Msg, undefined).
-spec(get_header(term(), emqx_types:message(), Default :: term()) -> term()).
get_header(Hdr, #message{headers = Headers}, Default) ->
    maps:get(Hdr, Headers, Default).

-spec(set_header(term(), term(), emqx_types:message()) -> emqx_types:message()).
set_header(Hdr, Val, Msg = #message{headers = undefined}) ->
    Msg#message{headers = #{Hdr => Val}};
set_header(Hdr, Val, Msg = #message{headers = Headers}) ->
    Msg#message{headers = maps:put(Hdr, Val, Headers)}.

-spec(remove_header(term(), emqx_types:message()) -> emqx_types:message()).
remove_header(Hdr, Msg = #message{headers = Headers}) ->
    case maps:is_key(Hdr, Headers) of
        true ->
            Msg#message{headers = maps:remove(Hdr, Headers)};
        false -> Msg
    end.

-spec(is_expired(emqx_types:message()) -> boolean()).
is_expired(#message{headers = #{'Message-Expiry-Interval' := Interval},
                    timestamp = CreatedAt}) ->
    elapsed(CreatedAt) > timer:seconds(Interval);
is_expired(_Msg) ->
    false.

-spec(update_expiry(emqx_types:message()) -> emqx_types:message()).
update_expiry(Msg = #message{headers = #{'Message-Expiry-Interval' := Interval},
                             timestamp = CreatedAt}) ->
    case elapsed(CreatedAt) of
        Elapsed when Elapsed > 0 ->
            set_header('Message-Expiry-Interval', max(1, Interval - (Elapsed div 1000)), Msg);
        _ -> Msg
    end;
update_expiry(Msg) -> Msg.

%% @doc Message to map
-spec(to_map(emqx_types:message()) -> map()).
to_map(Msg) ->
    maps:from_list(to_list(Msg)).

%% @doc Message to tuple list
-spec(to_list(emqx_types:message()) -> map()).
to_list(Msg) ->
    lists:zip(record_info(fields, message), tl(tuple_to_list(Msg))).

%% MilliSeconds
elapsed(Since) ->
    max(0, timer:now_diff(os:timestamp(), Since) div 1000).

format(#message{id = Id, qos = QoS, topic = Topic, from = From, flags = Flags, headers = Headers}) ->
    io_lib:format("Message(Id=~s, QoS=~w, Topic=~s, From=~p, Flags=~s, Headers=~s)",
                  [Id, QoS, Topic, From, format(flags, Flags), format(headers, Headers)]).

format(_, undefined) ->
    "";
format(flags, Flags) ->
    io_lib:format("~p", [[Flag || {Flag, true} <- maps:to_list(Flags)]]);
format(headers, Headers) ->
    io_lib:format("~p", [Headers]).
