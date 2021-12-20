%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%%%-----------------------------------------------------------------
%%% API
-spec format(LogEvent, Config) -> unicode:chardata() when
    LogEvent :: logger:log_event(),
    Config :: logger:config().
format(#{level := trace, msg := Msg, meta := Meta, action := Action}, _Config) ->
    Time = calendar:system_time_to_rfc3339(erlang:system_time(second)),
    ClientId = maps:get(clientid, Meta, ""),
    Peername = maps:get(peername, Meta, ""),
    MsgBin = format_msg(Msg),
    MetaBin = format_map(maps:without([clientid, peername], Meta)),
    [Time, " [", Action, "] ", ClientId, "@", Peername, " ", MsgBin, " ( ",
        MetaBin, ")\n"];

format(Event, Config) ->
    emqx_logger_textfmt:format(Event, Config).

format_msg(Bin)when is_binary(Bin) -> Bin;
format_msg(List) when is_list(List) -> List;
format_msg({publish, Payload}) ->
    io_lib:format("Publish Payload:(~ts) TO ", [Payload]);
format_msg({subscribe, SubId, SubOpts}) ->
    [io_lib:format("SUBSCRIBE ~ts, Opts( ", [SubId]),
        format_map(SubOpts), ")"];
format_msg({unsubscribe, SubOpts}) ->
    [io_lib:format("UNSUBSCRIBE ~ts, Opts( ", [maps:get(subid, SubOpts, "undefined")]),
        format_map(maps:without([subid], SubOpts)), ")"];
format_msg(Packet) ->
    emqx_packet:format(Packet).

format_map(Map) ->
    maps:fold(fun(K, V, Acc) ->
        [to_iolist(K), ":", to_iolist(V), " "|Acc]
              end, [], Map).

to_iolist(Atom) when is_atom(Atom) -> atom_to_list(Atom);
to_iolist(Int) when is_integer(Int) -> integer_to_list(Int);
to_iolist(Float) when is_float(Float) -> float_to_list(Float, [{decimals, 2}]);
to_iolist(Bin)when is_binary(Bin)  -> unicode:characters_to_binary(Bin);
to_iolist(List) when is_list(List) -> unicode:characters_to_list(List);
to_iolist(Term) -> io_lib:format("~0p", [Term]).
