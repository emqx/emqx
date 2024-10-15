%%--------------------------------------------------------------------
%% Copyright (c) 2017-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-export([
    make/2,
    make/3,
    make/4,
    make/6,
    make/7
]).

%% Fields
-export([
    id/1,
    qos/1,
    from/1,
    topic/1,
    payload/1,
    timestamp/1,
    timestamp/2
]).

%% Flags
-export([
    is_sys/1,
    clean_dup/1,
    get_flag/2,
    get_flag/3,
    get_flags/1,
    set_flag/2,
    set_flag/3,
    set_flags/2,
    unset_flag/2
]).

%% Headers
-export([
    get_headers/1,
    get_header/2,
    get_header/3,
    set_header/3,
    set_headers/2,
    remove_header/2
]).

-export([
    is_expired/2,
    update_expiry/1,
    timestamp_now/0
]).

-export([
    to_packet/2,
    to_map/1,
    to_log_map/1,
    to_list/1,
    from_map/1,
    estimate_size/1
]).

-export_type([
    timestamp/0,
    message_map/0
]).

-type message_map() :: #{
    id := binary(),
    qos := 0 | 1 | 2,
    from := atom() | binary(),
    flags := emqx_types:flags(),
    headers := emqx_types:headers(),
    topic := emqx_types:topic(),
    payload := emqx_types:payload(),
    timestamp := timestamp(),
    extra := _
}.

%% Message timestamp
%% Granularity: milliseconds.
-type timestamp() :: non_neg_integer().

-elvis([{elvis_style, god_modules, disable}]).

-spec make(emqx_types:topic(), emqx_types:payload()) -> emqx_types:message().
make(Topic, Payload) ->
    make(undefined, Topic, Payload).

-spec make(
    emqx_types:clientid(),
    emqx_types:topic(),
    emqx_types:payload()
) -> emqx_types:message().
make(From, Topic, Payload) ->
    make(From, ?QOS_0, Topic, Payload).

-spec make(
    emqx_types:clientid(),
    emqx_types:qos(),
    emqx_types:topic(),
    emqx_types:payload()
) -> emqx_types:message().
make(From, QoS, Topic, Payload) when ?QOS_0 =< QoS, QoS =< ?QOS_2 ->
    #message{
        id = emqx_guid:gen(),
        qos = QoS,
        from = From,
        topic = Topic,
        payload = Payload,
        timestamp = timestamp_now()
    }.

-spec make(
    emqx_types:clientid(),
    emqx_types:qos(),
    emqx_types:topic(),
    emqx_types:payload(),
    emqx_types:flags(),
    emqx_types:headers()
) -> emqx_types:message().
make(From, QoS, Topic, Payload, Flags, Headers) when
    ?QOS_0 =< QoS,
    QoS =< ?QOS_2,
    is_map(Flags),
    is_map(Headers)
->
    #message{
        id = emqx_guid:gen(),
        qos = QoS,
        from = From,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = timestamp_now()
    }.

-spec make(
    MsgId :: binary(),
    emqx_types:clientid(),
    emqx_types:qos(),
    emqx_types:topic(),
    emqx_types:payload(),
    emqx_types:flags(),
    emqx_types:headers()
) -> emqx_types:message().
make(MsgId, From, QoS, Topic, Payload, Flags, Headers) when
    ?QOS_0 =< QoS,
    QoS =< ?QOS_2,
    is_map(Flags),
    is_map(Headers)
->
    #message{
        id = MsgId,
        qos = QoS,
        from = From,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = timestamp_now()
    }.

%% optimistic esitmation of a message size after serialization
%% not including MQTT v5 message headers/user properties etc.
-spec estimate_size(emqx_types:message()) -> non_neg_integer().
estimate_size(#message{topic = Topic, payload = Payload}) ->
    FixedHeaderSize = 1,
    VarLenSize = 4,
    TopicSize = iolist_size(Topic),
    PayloadSize = iolist_size(Payload),
    PacketIdSize = 2,
    TopicLengthSize = 2,
    FixedHeaderSize + VarLenSize + TopicLengthSize + TopicSize + PacketIdSize + PayloadSize.

-spec id(emqx_types:message()) -> option(binary()).
id(#message{id = Id}) -> Id.

-spec qos(emqx_types:message()) -> emqx_types:qos().
qos(#message{qos = QoS}) -> QoS.

-spec from(emqx_types:message()) -> atom() | binary().
from(#message{from = From}) -> From.

-spec topic(emqx_types:message()) -> emqx_types:topic().
topic(#message{topic = Topic}) -> Topic.

-spec payload(emqx_types:message()) -> emqx_types:payload().
payload(#message{payload = Payload}) -> Payload.

-spec timestamp(emqx_types:message()) -> timestamp().
timestamp(#message{timestamp = TS}) -> TS.

-spec timestamp(emqx_types:message(), second | millisecond | microsecond) -> non_neg_integer().
timestamp(#message{timestamp = TS}, second) -> TS div 1000;
timestamp(#message{timestamp = TS}, millisecond) -> TS;
timestamp(#message{timestamp = TS}, microsecond) -> TS * 1000.

-spec is_sys(emqx_types:message()) -> boolean().
is_sys(#message{flags = #{sys := true}}) ->
    true;
is_sys(#message{topic = <<"$SYS/", _/binary>>}) ->
    true;
is_sys(_Msg) ->
    false.

-spec clean_dup(emqx_types:message()) -> emqx_types:message().
clean_dup(Msg = #message{flags = Flags = #{dup := true}}) ->
    Msg#message{flags = Flags#{dup => false}};
clean_dup(Msg) ->
    Msg.

-spec set_flags(map(), emqx_types:message()) -> emqx_types:message().
set_flags(New, Msg = #message{flags = Old}) when is_map(New) ->
    Msg#message{flags = maps:merge(Old, New)}.

-spec get_flag(emqx_types:flag(), emqx_types:message()) -> boolean().
get_flag(Flag, Msg) ->
    get_flag(Flag, Msg, false).

get_flag(Flag, #message{flags = Flags}, Default) ->
    maps:get(Flag, Flags, Default).

-spec get_flags(emqx_types:message()) -> option(map()).
get_flags(#message{flags = Flags}) -> Flags.

-spec set_flag(emqx_types:flag(), emqx_types:message()) -> emqx_types:message().
set_flag(Flag, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, true, Flags)}.

-spec set_flag(emqx_types:flag(), boolean() | integer(), emqx_types:message()) ->
    emqx_types:message().
set_flag(Flag, Val, Msg = #message{flags = Flags}) when is_atom(Flag) ->
    Msg#message{flags = maps:put(Flag, Val, Flags)}.

-spec unset_flag(emqx_types:flag(), emqx_types:message()) -> emqx_types:message().
unset_flag(Flag, Msg = #message{flags = Flags}) ->
    case maps:is_key(Flag, Flags) of
        true -> Msg#message{flags = maps:remove(Flag, Flags)};
        false -> Msg
    end.

-spec set_headers(map(), emqx_types:message()) -> emqx_types:message().
set_headers(New, Msg = #message{headers = Old}) when is_map(New) ->
    Msg#message{headers = maps:merge(Old, New)}.

-spec get_headers(emqx_types:message()) -> option(map()).
get_headers(Msg) -> Msg#message.headers.

-spec get_header(term(), emqx_types:message()) -> term().
get_header(Hdr, Msg) ->
    get_header(Hdr, Msg, undefined).
-spec get_header(term(), emqx_types:message(), term()) -> term().
get_header(Hdr, #message{headers = Headers}, Default) ->
    maps:get(Hdr, Headers, Default).

-spec set_header(term(), term(), emqx_types:message()) -> emqx_types:message().
set_header(Hdr, Val, Msg = #message{headers = Headers}) ->
    Msg#message{headers = maps:put(Hdr, Val, Headers)}.

-spec remove_header(term(), emqx_types:message()) -> emqx_types:message().
remove_header(Hdr, Msg = #message{headers = Headers}) ->
    case maps:is_key(Hdr, Headers) of
        true -> Msg#message{headers = maps:remove(Hdr, Headers)};
        false -> Msg
    end.

-spec is_expired(emqx_types:message(), atom()) -> boolean().
is_expired(
    #message{
        headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
        timestamp = CreatedAt
    },
    _
) ->
    elapsed(CreatedAt) > timer:seconds(Interval);
is_expired(#message{timestamp = CreatedAt}, Zone) ->
    case emqx_config:get_zone_conf(Zone, [mqtt, message_expiry_interval], infinity) of
        infinity -> false;
        Interval -> elapsed(CreatedAt) > Interval
    end.

-spec update_expiry(emqx_types:message()) -> emqx_types:message().
update_expiry(
    Msg = #message{
        headers = #{properties := #{'Message-Expiry-Interval' := Interval}},
        timestamp = CreatedAt
    }
) ->
    Props = maps:get(properties, Msg#message.headers),
    case elapsed(CreatedAt) of
        Elapsed when Elapsed > 0 ->
            Interval1 = max(1, Interval - (Elapsed div 1000)),
            set_header(properties, Props#{'Message-Expiry-Interval' => Interval1}, Msg);
        _ ->
            Msg
    end;
update_expiry(Msg) ->
    Msg.

%% @doc Message to PUBLISH Packet.
%%
%% When QoS=0 then packet id must be `undefined'
-spec to_packet(emqx_types:packet_id() | undefined, emqx_types:message()) ->
    emqx_types:packet().
to_packet(
    PacketId,
    Msg = #message{
        qos = QoS,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        extra = Extra
    }
) ->
    #mqtt_packet{
        header = #mqtt_packet_header{
            type = ?PUBLISH,
            dup = get_flag(dup, Msg),
            qos = QoS,
            retain = get_flag(retain, Msg)
        },
        variable = #mqtt_packet_publish{
            topic_name = Topic,
            packet_id = PacketId,
            properties = maybe_put_extra(
                Extra, filter_pub_props(maps:get(properties, Headers, #{}))
            )
        },
        payload = Payload
    }.

filter_pub_props(Props) ->
    maps:with(
        [
            'Payload-Format-Indicator',
            'Message-Expiry-Interval',
            'Response-Topic',
            'Correlation-Data',
            'User-Property',
            'Subscription-Identifier',
            'Content-Type'
        ],
        Props
    ).

maybe_put_extra(Extra, Props) when map_size(Extra) > 0 ->
    Props#{?MQTT_INTERNAL_EXTRA => Extra};
maybe_put_extra(_Extra, Props) ->
    Props.

%% @doc Message to map
-spec to_map(emqx_types:message()) -> message_map().
to_map(#message{
    id = Id,
    qos = QoS,
    from = From,
    flags = Flags,
    headers = Headers,
    topic = Topic,
    payload = Payload,
    timestamp = Timestamp,
    extra = Extra
}) ->
    #{
        id => Id,
        qos => QoS,
        from => From,
        flags => Flags,
        headers => Headers,
        topic => Topic,
        payload => Payload,
        timestamp => Timestamp,
        extra => Extra
    }.

%% @doc To map for logging, with payload dropped.
to_log_map(Msg) -> maps:without([payload], to_map(Msg)).

%% @doc Message to tuple list
-spec to_list(emqx_types:message()) -> list().
to_list(Msg) ->
    lists:zip(record_info(fields, message), tl(tuple_to_list(Msg))).

%% @doc Map to message
-spec from_map(message_map()) -> emqx_types:message().
from_map(#{
    id := Id,
    qos := QoS,
    from := From,
    flags := Flags,
    headers := Headers,
    topic := Topic,
    payload := Payload,
    timestamp := Timestamp,
    extra := Extra
}) ->
    #message{
        id = Id,
        qos = QoS,
        from = From,
        flags = Flags,
        headers = Headers,
        topic = Topic,
        payload = Payload,
        timestamp = Timestamp,
        extra = Extra
    }.

%% @doc Get current timestamp in milliseconds.
-spec timestamp_now() -> timestamp().
timestamp_now() ->
    erlang:system_time(millisecond).

%% MilliSeconds
elapsed(Since) ->
    max(0, timestamp_now() - Since).
