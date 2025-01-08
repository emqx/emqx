%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Tns =
        case to_iolist(maps:get(tns, Meta, "")) of
            "" -> "";
            X -> [" tns: ", X]
        end,
    ClientId = to_iolist(maps:get(clientid, Meta, "")),
    Peername = maps:get(peername, Meta, ""),
    MetaBin = format_meta(Meta, PEncode),
    Msg1 = to_iolist(Msg),
    Tag1 = to_iolist(Tag),
    [Time, " [", Tag1, "] ", ClientId, "@", Peername, Tns, " msg: ", Msg1, ", ", MetaBin, "\n"];
format(Event, Config) ->
    emqx_logger_textfmt:format(Event, Config).

format_meta_map(Meta, Encode) ->
    format_meta_map(Meta, Encode, [
        {packet, fun format_packet/2},
        {payload, fun format_payload/2},
        {<<"payload">>, fun format_payload/2}
    ]).

format_meta_map(Meta, _Encode, []) ->
    Meta;
format_meta_map(Meta, Encode, [{Name, FormatFun} | Rest]) ->
    case Meta of
        #{Name := Value} ->
            NewMeta =
                case FormatFun(Value, Encode) of
                    {NewValue, NewMeta0} ->
                        maps:merge(Meta#{Name => NewValue}, NewMeta0);
                    NewValue ->
                        Meta#{Name => NewValue}
                end,
            format_meta_map(NewMeta, Encode, Rest);
        #{} ->
            format_meta_map(Meta, Encode, Rest)
    end.

format_meta_data(Meta0, Encode) when is_map(Meta0) ->
    Meta1 = format_meta_map(Meta0, Encode),
    maps:map(fun(_K, V) -> format_meta_data(V, Encode) end, Meta1);
format_meta_data(Meta, Encode) when is_list(Meta) ->
    [format_meta_data(Item, Encode) || Item <- Meta];
format_meta_data(Meta, Encode) when is_tuple(Meta) ->
    List = erlang:tuple_to_list(Meta),
    FormattedList = [format_meta_data(Item, Encode) || Item <- List],
    erlang:list_to_tuple(FormattedList);
format_meta_data(Meta, _Encode) ->
    Meta.

format_meta(Meta0, Encode) ->
    Meta1 = maps:without([msg, tns, clientid, peername, trace_tag], Meta0),
    Meta2 = format_meta_data(Meta1, Encode),
    kvs_to_iolist(lists:sort(fun compare_meta_kvs/2, maps:to_list(Meta2))).

%% packet always goes first; payload always goes last
compare_meta_kvs(KV1, KV2) -> weight(KV1) =< weight(KV2).

weight({packet, _}) -> {0, packet};
weight({payload, _}) -> {2, payload};
weight({K, _}) -> {1, K}.

format_packet(undefined, _) ->
    "";
format_packet(Packet, Encode) ->
    try
        emqx_packet:format(Packet, Encode)
    catch
        _:_ ->
            %% We don't want to crash if there is a field named packet with
            %% some other type of value
            Packet
    end.

format_payload(undefined, Type) ->
    {"", #{payload_encode => Type}};
format_payload(Payload, Type) when is_binary(Payload) ->
    {Payload1, Type1} = emqx_packet:format_payload(Payload, Type),
    {Payload1, #{payload_encode => Type1}};
format_payload(Payload, Type) ->
    {Payload, #{payload_encode => Type}}.

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
