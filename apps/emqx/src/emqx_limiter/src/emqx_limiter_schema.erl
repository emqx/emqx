%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_limiter_schema).

-feature(maybe_expr, enable).

-include_lib("typerefl/include/types.hrl").
-include_lib("hocon/include/hoconsc.hrl").

-export([
    roots/0,
    fields/1,
    namespace/0,
    desc/1
]).

-export([
    to_rate/1,
    to_burst/1,
    rate_type/0,
    burst_type/0
]).
-export([rate_to_str/2]).

-export([
    mqtt_limiter_names/0
]).

-define(KILOBYTE, 1024).
-define(MEGABYTE, (?KILOBYTE * ?KILOBYTE)).
-define(GIGABYTE, (?KILOBYTE * ?KILOBYTE * ?KILOBYTE)).

-type interval_ms() :: pos_integer().
-type rate() :: infinity | {number(), interval_ms()}.
-type burst() :: {number(), interval_ms()}.

%% the processing strategy after the failure of the token request
-typerefl_from_string({rate/0, ?MODULE, to_rate}).
-typerefl_from_string({burst/0, ?MODULE, to_burst}).

-reflect_type([
    rate/0,
    burst/0
]).

%%--------------------------------------------------------------------
%% schema
%%--------------------------------------------------------------------

namespace() -> limiter.

roots() ->
    [].

fields(mqtt) ->
    lists:foldl(fun make_mqtt_limiters_schema/2, [], mqtt_limiter_names()).

make_mqtt_limiters_schema(Name, Fields) ->
    NameStr = erlang:atom_to_list(Name),
    Rate = erlang:list_to_atom(NameStr ++ "_rate"),
    Burst = erlang:list_to_atom(NameStr ++ "_burst"),
    [
        {Rate,
            ?HOCON(rate_type(), #{
                desc => ?DESC(Rate),
                required => false
            })},
        {Burst,
            ?HOCON(burst_type(), #{
                desc => ?DESC(Burst),
                required => false
            })}
        | Fields
    ].

rate_type() ->
    typerefl:alias("string", rate()).

burst_type() ->
    typerefl:alias("string", burst()).

desc(mqtt) ->
    ?DESC(mqtt);
desc(_) ->
    undefined.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------
mqtt_limiter_names() ->
    [
        max_conn,
        messages,
        bytes
    ].

to_rate(Str) ->
    case parse_rate(Str) of
        {ok, {0, _}} ->
            {error, {invalid_rate, Str}};
        {ok, Rate} ->
            {ok, Rate};
        {error, _} = Error ->
            Error
    end.

to_burst(Str) ->
    case parse_rate(Str) of
        {ok, infinity} ->
            {error, {invalid_burst, Str}};
        {ok, Burst} ->
            {ok, Burst};
        {error, _} = Error ->
            Error
    end.

rate_to_str(infinity, _CapacityUnit) ->
    <<"infinity">>;
rate_to_str({Capacity, Interval}, no_unit = _CapacityUnit) ->
    iolist_to_binary([
        integer_to_binary(Capacity), <<"/">>, duration_ms_to_str(Interval)
    ]);
rate_to_str({Capacity, Interval}, bytes = _CapacityUnit) ->
    iolist_to_binary([
        byte_capacity_to_str(Capacity), <<"/">>, duration_ms_to_str(Interval)
    ]).

duration_ms_to_str(Interval) ->
    AllowedTimeUnits = [<<"d">>, <<"h">>, <<"m">>, <<"s">>, <<"ms">>],
    emqx_schema:duration_ms_to_str(Interval, AllowedTimeUnits).

byte_capacity_to_str(0) ->
    <<"0">>;
byte_capacity_to_str(C0) when C0 rem ?GIGABYTE == 0 ->
    C1 = C0 div ?GIGABYTE,
    <<(integer_to_binary(C1))/binary, "gb">>;
byte_capacity_to_str(C0) when C0 rem ?MEGABYTE == 0 ->
    C1 = C0 div ?MEGABYTE,
    <<(integer_to_binary(C1))/binary, "mb">>;
byte_capacity_to_str(C0) when C0 rem ?KILOBYTE == 0 ->
    C1 = C0 div ?KILOBYTE,
    <<(integer_to_binary(C1))/binary, "kb">>;
byte_capacity_to_str(C) ->
    integer_to_binary(C).

%% rate can be: 10 10MB 10MB/s 10MB/2s infinity
%% e.g. the bytes_in regex tree is:
%%
%%        __ infinity
%%        |                 - xMB
%%  rate -|                 |
%%        __ ?Size(/?Time) -|            - xMB/s
%%                          |            |
%%                          - xMB/?Time -|
%%                                       - xMB/ys
%%
parse_rate(Str) when is_binary(Str) ->
    parse_rate(binary_to_list(Str));
parse_rate(Str) when is_list(Str) ->
    do_parse_rate(string:to_lower(string:trim(Str))).

do_parse_rate("infinity") ->
    {ok, infinity};
do_parse_rate(Str) ->
    Regex = """
        ^
        # Capacity with optional unit
        (\d+)(kb|mb|gb|)
        # Optional interval with required unit
        (?:
            /(\d*)([mshd]{1,2})
        )?
        $
    """,
    {ok, MP} = re:compile(Regex, [extended]),
    case re:run(Str, MP, [{capture, all_but_first, list}]) of
        {match, [Capacity, CapacityUnit]} ->
            do_parse_rate(Capacity, CapacityUnit, "1", "s");
        {match, [Capacity, CapacityUnit, Interval, IntervalUnit]} ->
            do_parse_rate(Capacity, CapacityUnit, Interval, IntervalUnit);
        _ ->
            {error, {invalid_rate, Str}}
    end.

do_parse_rate(CapacityStr, CapacityUnitStr, IntervalStr, IntervalUnitStr) ->
    maybe
        {ok, Capacity} ?= capacity_from_str(CapacityStr, CapacityUnitStr),
        {ok, Interval} ?= interval_from_str(IntervalStr, IntervalUnitStr),
        {ok, {Capacity, Interval}}
    end.

capacity_from_str(ValueStr, UnitStr) ->
    case unit_scale(UnitStr) of
        {ok, Scale} ->
            %% ValueStr is \d+, so converting to integer is safe
            {ok, erlang:list_to_integer(ValueStr) * Scale};
        error ->
            {error, {invalid_unit, UnitStr}}
    end.

unit_scale("") -> {ok, 1};
unit_scale("kb") -> {ok, ?KILOBYTE};
unit_scale("mb") -> {ok, ?MEGABYTE};
unit_scale("gb") -> {ok, ?GIGABYTE};
unit_scale(_) -> error.

interval_from_str("", UnitStr) ->
    interval_from_str("1", UnitStr);
interval_from_str(ValueStr, UnitStr) ->
    case emqx_schema:to_duration_ms(ValueStr ++ UnitStr) of
        {ok, 0} ->
            {error, {invalid_interval, ValueStr}};
        {ok, Val} ->
            {ok, Val};
        {error, _} ->
            {error, {invalid_interval, ValueStr}}
    end.
