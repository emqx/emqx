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

-module(emqx_message).

-compile(inline).

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
-export([ is_sys/1
        , clean_dup/1
        , get_flag/2
        , get_flag/3
        , get_flags/1
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

-export([ to_packet/2
        , to_map/1
        , to_list/1
        ]).

-export([format/1]).

-type(flag() :: atom()).

-spec(make(emqx_topic:topic(), emqx_types:payload()) -> emqx_types:message()).
make(Topic, Payload) ->
    make(undefined, Topic, Payload).

-spec(make(emqx_types:clientid(),
           emqx_topic:topic(),
           emqx_types:payload()) -> emqx_types:message()).
make(From, Topic, Payload) ->
    make(From, ?QOS_0, Topic, Payload).

-spec(make(emqx_types:clientid(),
           emqx_types:qos(),
           emqx_topic:topic(),
           emqx_types:payload()) -> emqx_types:message()).
make(From, QoS, Topic, Payload) when ?QOS_0 =< QoS, QoS =< ?QOS_2 ->
    Now = erlang:system_time(millisecond),
    #message{id = emqx_guid:gen(),
             qos = QoS,
             from = From,
             topic = Topic,
             payload = Payload,
             timestamp = Now
            }.

-spec(id(emqx_types:message()) -> maybe(binary())).
id(#message{id = Id}) -> Id.

-spec(qos(emqx_types:message()) -> emqx_types:qos()).
qos(#message{qos = QoS}) -> QoS.

-spec(from(emqx_types:message()) -> atom() | binary()).
from(#message{from = From}) -> From.

-spec(topic(emqx_types:message()) -> emqx_types:topic()).
topic(#message{topic = Topic}) -> Topic.

-spec(payload(emqx_types:message()) -> emqx_types:payload()).
payload(#message{payload = Payload}) -> Payload.

-spec(timestamp(emqx_types:message()) -> integer()).
timestamp(#message{timestamp = TS}) -> TS.

-spec(is_sys(emqx_types:message()) -> boolean()).
is_sys(#message{flags = #{sys := true}}) ->
    true;
is_sys(#message{topic = <<"$SYS/", _/binary>>}) ->
    true;
is_sys(_Msg) -> false.

-spec(clean_dup(emqx_types:message()) -> emqx_types:message()).
clean_dup(Msg = #message{flags = Flags = #{dup := true}}) ->
    Msg#message{flags = Flags#{dup => false}};
clean_dup(Msg) -> Msg.

-spec(set_flags(map(), emqx_types:message()) -> emqx_types:message()).
set_flags(Flags, Msg = #message{flags = undefined}) when is_map(Flags) ->
    Msg#message{flags = Flags};
set_flags(New, Msg = #message{flags = Old}) when is_map(New) ->
    Msg#message{flags = maps:merge(Old, New)}.

-spec(get_flag(flag(), emqx_types:message()) -> boolean()).
get_flag(_Flag, #message{flags = undefined}) ->
    false;
get_flag(Flag, Msg) ->
    get_flag(Flag, Msg, false).

get_flag(_Flag, #message{flags = undefined}, Default) ->
    Default;
get_flag(Flag, #message{flags = Flags}, Default) ->
    maps:get(Flag, Flags, Default).

-spec(get_flags(emqx_types:message()) -> maybe(map())).
get_flags(#message{flags = Flags}) -> Flags.

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
        true  -> Msg#message{flags = maps:remove(Flag, Flags)};
        false -> Msg
    end.

-spec(set_headers(map(), emqx_types:message()) -> emqx_types:message()).
set_headers(Headers, Msg = #message{headers = undefined}) when is_map(Headers) ->
    Msg#message{headers = Headers};
set_headers(New, Msg = #message{headers = Old}) when is_map(New) ->
    Msg#message{headers = maps:merge(Old, New)}.

-spec(get_headers(emqx_types:message()) -> maybe(map())).
get_headers(Msg) -> Msg#message.headers.

-spec(get_header(term(), emqx_types:message()) -> term()).
get_header(_Hdr, #message{headers = undefined}) ->
    undefined;
get_header(Hdr, Msg) ->
    get_header(Hdr, Msg, undefined).
-spec(get_header(term(), emqx_types:message(), term()) -> term()).
get_header(_Hdr, #message{headers = undefined}, Default) ->
    Default;
get_header(Hdr, #message{headers = Headers}, Default) ->
    maps:get(Hdr, Headers, Default).

-spec(set_header(term(), term(), emqx_types:message()) -> emqx_types:message()).
set_header(Hdr, Val, Msg = #message{headers = undefined}) ->
    Msg#message{headers = #{Hdr => Val}};
set_header(Hdr, Val, Msg = #message{headers = Headers}) ->
    Msg#message{headers = maps:put(Hdr, Val, Headers)}.

-spec(remove_header(term(), emqx_types:message()) -> emqx_types:message()).
remove_header(_Hdr, Msg = #message{headers = undefined}) ->
    Msg;
remove_header(Hdr, Msg = #message{headers = Headers}) ->
    case maps:is_key(Hdr, Headers) of
        true  -> Msg#message{headers = maps:remove(Hdr, Headers)};
        false -> Msg
    end.

-spec(is_expired(emqx_types:message()) -> boolean()).
is_expired(#message{headers = #{'Message-Expiry-Interval' := Interval},
                    timestamp = CreatedAt}) ->
    elapsed(CreatedAt) > timer:seconds(Interval);
is_expired(_Msg) -> false.

-spec(update_expiry(emqx_types:message()) -> emqx_types:message()).
update_expiry(Msg = #message{headers = #{'Message-Expiry-Interval' := Interval},
                             timestamp = CreatedAt}) ->
    case elapsed(CreatedAt) of
        Elapsed when Elapsed > 0 ->
            Interval1 = max(1, Interval - (Elapsed div 1000)),
            set_header('Message-Expiry-Interval', Interval1, Msg);
        _ -> Msg
    end;
update_expiry(Msg) -> Msg.

%% @doc Message to PUBLISH Packet.
-spec(to_packet(emqx_types:packet_id(), emqx_types:message())
      -> emqx_types:packet()).
to_packet(PacketId, Msg = #message{qos = QoS, headers = Headers,
                                   topic = Topic, payload = Payload}) ->
    #mqtt_packet{header   = #mqtt_packet_header{type   = ?PUBLISH,
                                                dup    = get_flag(dup, Msg),
                                                qos    = QoS,
                                                retain = get_flag(retain, Msg)
                                               },
                 variable = #mqtt_packet_publish{topic_name = Topic,
                                                 packet_id  = PacketId,
                                                 properties = props(Headers)
                                                },
                 payload  = Payload
                }.

props(undefined) -> undefined;
props(Headers)   -> maps:with(['Payload-Format-Indicator',
                               'Response-Topic',
                               'Correlation-Data',
                               'User-Property',
                               'Subscription-Identifier',
                               'Content-Type',
                               'Message-Expiry-Interval'
                              ], Headers).

%% @doc Message to map
-spec(to_map(emqx_types:message()) -> map()).
to_map(#message{
          id = Id,
          qos = QoS,
          from = From,
          flags = Flags,
          headers = Headers,
          topic = Topic,
          payload = Payload,
          timestamp = Timestamp
        }) ->
    #{id => Id,
      qos => QoS,
      from => From,
      flags => Flags,
      headers => Headers,
      topic => Topic,
      payload => Payload,
      timestamp => Timestamp
     }.

%% @doc Message to tuple list
-spec(to_list(emqx_types:message()) -> map()).
to_list(Msg) ->
    lists:zip(record_info(fields, message), tl(tuple_to_list(Msg))).

%% MilliSeconds
elapsed(Since) ->
    max(0, erlang:system_time(millisecond) - Since).

format(#message{id = Id, qos = QoS, topic = Topic, from = From, flags = Flags, headers = Headers}) ->
    io_lib:format("Message(Id=~s, QoS=~w, Topic=~s, From=~p, Flags=~s, Headers=~s)",
                  [Id, QoS, Topic, From, format(flags, Flags), format(headers, Headers)]).

format(_, undefined) ->
    "";
format(flags, Flags) ->
    io_lib:format("~p", [[Flag || {Flag, true} <- maps:to_list(Flags)]]);
format(headers, Headers) ->
    io_lib:format("~p", [Headers]).

