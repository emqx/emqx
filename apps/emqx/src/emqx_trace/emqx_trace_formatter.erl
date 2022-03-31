%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_trace_formatter).

-export([format/2]).
-export([format_meta/1]).

%%%-----------------------------------------------------------------
%%% API
-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: logger:config().
format(
    #{level := debug, meta := Meta = #{trace_tag := Tag}, msg := Msg},
    #{payload_encode := PEncode}
) ->
    Time = calendar:system_time_to_rfc3339(erlang:system_time(second)),
    ClientId = to_iolist(maps:get(clientid, Meta, "")),
    Peername = maps:get(peername, Meta, ""),
    MetaBin = format_meta(Meta, PEncode),
    [Time, " [", Tag, "] ", ClientId, "@", Peername, " msg: ", Msg, MetaBin, "\n"];
format(Event, Config) ->
    emqx_logger_textfmt:format(Event, Config).

format_meta(Meta) ->
    Encode = emqx_trace_handler:payload_encode(),
    do_format_meta(Meta, Encode).

format_meta(Meta0, Encode) ->
    Meta1 = #{packet := Packet0, payload := Payload0} = do_format_meta(Meta0, Encode),
    Packet = enrich(", packet: ", Packet0),
    Payload = enrich(", payload: ", Payload0),
    Meta2 = maps:without([msg, clientid, peername, packet, payload, trace_tag], Meta1),
    case Meta2 =:= #{} of
        true -> [Packet, Payload];
        false -> [Packet, ", ", map_to_iolist(Meta2), Payload]
    end.

enrich(_, "") -> "";
enrich(Key, IoData) -> [Key, IoData].

do_format_meta(Meta, Encode) ->
    Meta#{
        packet => format_packet(maps:get(packet, Meta, undefined), Encode),
        payload => format_payload(maps:get(payload, Meta, undefined), Encode)
    }.

format_packet(undefined, _) -> "";
format_packet(Packet, Encode) -> emqx_packet:format(Packet, Encode).

format_payload(undefined, _) -> "";
format_payload(Payload, text) -> io_lib:format("~ts", [Payload]);
format_payload(Payload, hex) -> emqx_packet:encode_hex(Payload);
format_payload(_, hidden) -> "******".

to_iolist(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_iolist(Int) when is_integer(Int) -> integer_to_list(Int);
to_iolist(Float) when is_float(Float) -> float_to_list(Float, [{decimals, 2}]);
to_iolist(SubMap) when is_map(SubMap) -> ["[", map_to_iolist(SubMap), "]"];
to_iolist(Char) -> emqx_logger_textfmt:try_format_unicode(Char).

map_to_iolist(Map) ->
    lists:join(
        ",",
        lists:map(
            fun({K, V}) -> [to_iolist(K), ": ", to_iolist(V)] end,
            maps:to_list(Map)
        )
    ).
