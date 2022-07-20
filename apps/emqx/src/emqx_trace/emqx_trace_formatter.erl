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
-export([format_meta_map/1]).

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
    [Time, " [", Tag, "] ", ClientId, "@", Peername, " msg: ", Msg, ", ", MetaBin, "\n"];
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

format_payload(undefined, _) -> "";
format_payload(Payload, text) -> io_lib:format("~ts", [Payload]);
format_payload(Payload, hex) -> emqx_packet:encode_hex(Payload);
format_payload(_, hidden) -> "******".

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
