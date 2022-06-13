%%--------------------------------------------------------------------
%% Copyright (c) 2017-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_datetime).

-include_lib("typerefl/include/types.hrl").

%% API
-export([
    to_epoch_millisecond/1,
    to_epoch_second/1,
    human_readable_duration_string/1
]).
-export([
    epoch_to_rfc3339/1,
    epoch_to_rfc3339/2
]).

-reflect_type([
    epoch_millisecond/0,
    epoch_second/0
]).

-type epoch_second() :: non_neg_integer().
-type epoch_millisecond() :: non_neg_integer().
-typerefl_from_string({epoch_second/0, ?MODULE, to_epoch_second}).
-typerefl_from_string({epoch_millisecond/0, ?MODULE, to_epoch_millisecond}).

to_epoch_second(DateTime) ->
    to_epoch(DateTime, second).

to_epoch_millisecond(DateTime) ->
    to_epoch(DateTime, millisecond).

to_epoch(DateTime, Unit) ->
    try
        case string:to_integer(DateTime) of
            {Epoch, []} when Epoch >= 0 -> {ok, Epoch};
            {_Epoch, []} -> {error, bad_epoch};
            _ -> {ok, calendar:rfc3339_to_system_time(DateTime, [{unit, Unit}])}
        end
    catch
        error:_ ->
            {error, bad_rfc3339_timestamp}
    end.

epoch_to_rfc3339(TimeStamp) ->
    epoch_to_rfc3339(TimeStamp, millisecond).

epoch_to_rfc3339(TimeStamp, Unit) when is_integer(TimeStamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(TimeStamp, [{unit, Unit}])).

-spec human_readable_duration_string(integer()) -> string().
human_readable_duration_string(Milliseconds) ->
    Seconds = Milliseconds div 1000,
    {D, {H, M, S}} = calendar:seconds_to_daystime(Seconds),
    L0 = [{D, " days"}, {H, " hours"}, {M, " minutes"}, {S, " seconds"}],
    L1 = lists:dropwhile(fun({K, _}) -> K =:= 0 end, L0),
    L2 = lists:map(fun({Time, Unit}) -> [integer_to_list(Time), Unit] end, L1),
    lists:flatten(lists:join(", ", L2)).

-ifdef(TEST).
-include_lib("eunit/include/eunit.hrl").
-compile(nowarn_export_all).
-compile(export_all).
roots() -> [bar].

fields(bar) ->
    [
        {second, ?MODULE:epoch_second()},
        {millisecond, ?MODULE:epoch_millisecond()}
    ].

-define(FORMAT(_Sec_, _Ms_),
    lists:flatten(
        io_lib:format("bar={second=~w,millisecond=~w}", [_Sec_, _Ms_])
    )
).

epoch_ok_test() ->
    Args = [
        {0, 0, 0, 0},
        {1, 1, 1, 1},
        {"2022-01-01T08:00:00+08:00", "2022-01-01T08:00:00+08:00", 1640995200, 1640995200000}
    ],
    lists:foreach(
        fun({Sec, Ms, EpochSec, EpochMs}) ->
            check_ok(?FORMAT(Sec, Ms), EpochSec, EpochMs)
        end,
        Args
    ),
    ok.

check_ok(Input, Sec, Ms) ->
    {ok, Data} = hocon:binary(Input, #{}),
    ?assertMatch(
        #{bar := #{second := Sec, millisecond := Ms}},
        hocon_tconf:check_plain(?MODULE, Data, #{atom_key => true}, [bar])
    ),
    ok.

epoch_failed_test() ->
    Args = [
        {-1, -1},
        {"1s", "1s"},
        {"2022-13-13T08:00:00+08:00", "2022-13-13T08:00:00+08:00"}
    ],
    lists:foreach(
        fun({Sec, Ms}) ->
            check_failed(?FORMAT(Sec, Ms))
        end,
        Args
    ),
    ok.

check_failed(Input) ->
    {ok, Data} = hocon:binary(Input, #{}),
    ?assertException(
        throw,
        _,
        hocon_tconf:check_plain(?MODULE, Data, #{atom_key => true}, [bar])
    ),
    ok.

-endif.
