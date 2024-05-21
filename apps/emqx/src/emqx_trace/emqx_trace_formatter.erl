%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include("emqx_mqtt.hrl").

-export([format/2]).
-export([format_meta_map/1]).

%% logger_formatter:config/0 is not exported.
-type config() :: map().

%%%-----------------------------------------------------------------
%%% API
-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: config().
format(
    #{level := debug, meta := _Meta = #{trace_tag := _Tag}, msg := _Msg} = Entry,
    #{payload_encode := PEncode}
) ->
    #{level := debug, meta := Meta = #{trace_tag := Tag}, msg := Msg} =
        emqx_logger_textfmt:evaluate_lazy_values(Entry),
    Time = emqx_utils_calendar:now_to_rfc3339(microsecond),
    ClientId = to_iolist(maps:get(clientid, Meta, "")),
    Peername = maps:get(peername, Meta, ""),
    MetaBin = format_meta(Meta, PEncode),
    Msg1 = to_iolist(Msg),
    Tag1 = to_iolist(Tag),
    [Time, " [", Tag1, "] ", ClientId, "@", Peername, " msg: ", Msg1, ", ", MetaBin, "\n"];
format(Event, Config) ->
    emqx_logger_textfmt:format(Event, Config).

format_meta_map(Meta) ->
    Encode = emqx_trace_handler:payload_encode(),
    format_meta_map(Meta, Encode).

format_meta_map(Meta, Encode) ->
    format_meta_map(Meta, Encode, [{packet, fun format_packet/2}, {payload, fun format_payload/2}]).

format_meta_map(Meta, _Encode, []) ->
    Meta;
format_meta_map(Meta, Encode, [{Name, FormatFun} | Rest]) ->
    case Meta of
        #{Name := Value} ->
            NewMeta = Meta#{Name => FormatFun(Value, Encode)},
            format_meta_map(NewMeta, Encode, Rest);
        #{} ->
            format_meta_map(Meta, Encode, Rest)
    end.

format_meta(Meta0, Encode) ->
    Meta1 = maps:without([msg, clientid, peername, trace_tag], Meta0),
    Meta2 = format_meta_map(Meta1, Encode),
    kvs_to_iolist(lists:sort(fun compare_meta_kvs/2, maps:to_list(Meta2))).

%% packet always goes first; payload always goes last
compare_meta_kvs(KV1, KV2) -> weight(KV1) =< weight(KV2).

weight({packet, _}) -> {0, packet};
weight({payload, _}) -> {2, payload};
weight({K, _}) -> {1, K}.

format_packet(undefined, _) -> "";
format_packet(Packet, Encode) -> emqx_packet:format(Packet, Encode).

format_payload(undefined, _) ->
    "";
format_payload(_, hidden) ->
    "******";
format_payload(Payload, text) when ?MAX_PAYLOAD_FORMAT_LIMIT(Payload) ->
    unicode:characters_to_list(Payload);
format_payload(Payload, hex) when ?MAX_PAYLOAD_FORMAT_LIMIT(Payload) -> binary:encode_hex(Payload);
format_payload(<<Part:?TRUNCATED_PAYLOAD_SIZE/binary, _/binary>> = Payload, Type) ->
    emqx_packet:format_truncated_payload(Part, byte_size(Payload), Type).

to_iolist(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_iolist(Int) when is_integer(Int) -> integer_to_list(Int);
to_iolist(Float) when is_float(Float) -> float_to_list(Float, [{decimals, 2}]);
to_iolist(SubMap) when is_map(SubMap) -> ["[", kvs_to_iolist(maps:to_list(SubMap)), "]"];
to_iolist(Char) -> emqx_logger_textfmt:try_format_unicode(Char).

kvs_to_iolist(KVs) ->
    lists:join(
        ", ",
        lists:map(
            fun({K, V}) -> [to_iolist(K), ": ", to_iolist(V)] end,
            KVs
        )
    ).
