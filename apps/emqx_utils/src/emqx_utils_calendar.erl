%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_calendar).

-include_lib("typerefl/include/types.hrl").

-export([
    formatter/1,
    format/3,
    format/4,
    formatted_datetime_to_system_time/3,
    offset_second/1
]).

%% API
-export([
    to_epoch_millisecond/1,
    to_epoch_microsecond/1,
    to_epoch_second/1,
    human_readable_duration_string/1,
    now_time/1
]).
-export([
    epoch_to_rfc3339/1,
    epoch_to_rfc3339/2,
    now_to_rfc3339/0,
    now_to_rfc3339/1
]).

-export([time_unit/1]).

-define(SECONDS_PER_MINUTE, 60).
-define(SECONDS_PER_HOUR, 3600).
-define(SECONDS_PER_DAY, 86400).
-define(DAYS_PER_YEAR, 365).
-define(DAYS_PER_LEAP_YEAR, 366).
-define(DAYS_FROM_0_TO_1970, 719528).
-define(DAYS_FROM_0_TO_10000, 2932897).
-define(SECONDS_FROM_0_TO_1970, ?DAYS_FROM_0_TO_1970 * ?SECONDS_PER_DAY).
-define(SECONDS_FROM_0_TO_10000, (?DAYS_FROM_0_TO_10000 * ?SECONDS_PER_DAY)).
%% the maximum value is the SECONDS_FROM_0_TO_10000 in the calendar.erl,
%% here minus SECONDS_PER_DAY to tolerate timezone time offset,
%% so the maximum date can reach 9999-12-31 which is ample.
-define(MAXIMUM_EPOCH, 253402214400).
-define(MAXIMUM_EPOCH_MILLI, 253402214400_000).
-define(MAXIMUM_EPOCH_MICROS, 253402214400_000_000).

-define(DATE_PART, [
    year,
    month,
    day,
    hour,
    minute,
    second,
    nanosecond,
    millisecond,
    microsecond
]).

-define(DATE_ZONE_NAME, [
    timezone,
    timezone1,
    timezone2
]).

-reflect_type([
    epoch_millisecond/0,
    epoch_second/0,
    epoch_microsecond/0
]).

-type epoch_second() :: non_neg_integer().
-type epoch_millisecond() :: non_neg_integer().
-type epoch_microsecond() :: non_neg_integer().
-typerefl_from_string({epoch_second/0, ?MODULE, to_epoch_second}).
-typerefl_from_string({epoch_millisecond/0, ?MODULE, to_epoch_millisecond}).
-typerefl_from_string({epoch_microsecond/0, ?MODULE, to_epoch_microsecond}).

%%--------------------------------------------------------------------
%% Epoch <-> RFC 3339
%%--------------------------------------------------------------------

to_epoch_second(DateTime) ->
    to_epoch(DateTime, second).

to_epoch_millisecond(DateTime) ->
    to_epoch(DateTime, millisecond).

to_epoch_microsecond(DateTime) ->
    to_epoch(DateTime, microsecond).

to_epoch(DateTime, Unit) ->
    try
        case string:to_integer(DateTime) of
            {Epoch, []} -> validate_epoch(Epoch, Unit);
            _ -> {ok, calendar:rfc3339_to_system_time(DateTime, [{unit, Unit}])}
        end
    catch
        error:_ ->
            {error, bad_rfc3339_timestamp}
    end.

epoch_to_rfc3339(Timestamp) ->
    epoch_to_rfc3339(Timestamp, millisecond).

epoch_to_rfc3339(Timestamp, Unit) when is_integer(Timestamp) ->
    list_to_binary(calendar:system_time_to_rfc3339(Timestamp, [{unit, Unit}])).

now_to_rfc3339() ->
    now_to_rfc3339(second).

now_to_rfc3339(Unit) ->
    epoch_to_rfc3339(erlang:system_time(Unit), Unit).

-spec human_readable_duration_string(integer()) -> string().
human_readable_duration_string(Milliseconds) ->
    Seconds = Milliseconds div 1000,
    {D, {H, M, S}} = calendar:seconds_to_daystime(Seconds),
    L0 = [{D, " days"}, {H, " hours"}, {M, " minutes"}, {S, " seconds"}],
    L1 = lists:dropwhile(fun({K, _}) -> K =:= 0 end, L0),
    L2 = lists:map(fun({Time, Unit}) -> [integer_to_list(Time), Unit] end, L1),
    lists:flatten(lists:join(", ", L2)).

%% This is the same human-readable timestamp format as
%% hocon-cli generated app.<time>.config file name.
-spec now_time(millisecond | second) -> string().
now_time(Unit) ->
    Ts = os:system_time(Unit),
    {{Y, M, D}, {HH, MM, SS}} = calendar:system_time_to_local_time(Ts, Unit),
    {Fmt, Args} =
        case Unit of
            millisecond ->
                {"~0p.~2..0b.~2..0b.~2..0b.~2..0b.~2..0b.~3..0b", [
                    Y, M, D, HH, MM, SS, Ts rem 1000
                ]};
            second ->
                {"~0p.~2..0b.~2..0b.~2..0b.~2..0b.~2..0b", [Y, M, D, HH, MM, SS]}
        end,
    lists:flatten(io_lib:format(Fmt, Args)).

validate_epoch(Epoch, _Unit) when Epoch < 0 ->
    {error, bad_epoch};
validate_epoch(Epoch, second) when Epoch =< ?MAXIMUM_EPOCH ->
    {ok, Epoch};
validate_epoch(Epoch, millisecond) when Epoch =< ?MAXIMUM_EPOCH_MILLI ->
    {ok, Epoch};
%% http api use millisecond but we should transform to microsecond
validate_epoch(Epoch, microsecond) when
    Epoch >= ?MAXIMUM_EPOCH andalso
        Epoch =< ?MAXIMUM_EPOCH_MILLI
->
    {ok, Epoch * 1000};
validate_epoch(Epoch, microsecond) when Epoch =< ?MAXIMUM_EPOCH_MICROS ->
    {ok, Epoch};
validate_epoch(_Epoch, _Unit) ->
    {error, bad_epoch}.

%%--------------------------------------------------------------------
%% Timestamp <-> any format date string
%% Timestamp treat as a superset for epoch, it can be any positive integer
%%--------------------------------------------------------------------

formatter(FormatterStr) when is_list(FormatterStr) ->
    formatter(list_to_binary(FormatterStr));
formatter(FormatterBin) when is_binary(FormatterBin) ->
    do_formatter(FormatterBin, []).

offset_second(Offset) ->
    offset_second_(Offset).

format(Time, Unit, Formatter) ->
    format(Time, Unit, undefined, Formatter).

format(Time, Unit, Offset, FormatterBin) when is_binary(FormatterBin) ->
    format(Time, Unit, Offset, formatter(FormatterBin));
format(Time, Unit, Offset, Formatter) ->
    do_format(Time, time_unit(Unit), offset_second(Offset), Formatter).

formatted_datetime_to_system_time(DateStr, Unit, FormatterBin) when is_binary(FormatterBin) ->
    formatted_datetime_to_system_time(DateStr, Unit, formatter(FormatterBin));
formatted_datetime_to_system_time(DateStr, Unit, Formatter) ->
    do_formatted_datetime_to_system_time(DateStr, Unit, Formatter).

%%--------------------------------------------------------------------
%% Time unit
%%--------------------------------------------------------------------

time_unit(second) -> second;
time_unit(millisecond) -> millisecond;
time_unit(microsecond) -> microsecond;
time_unit(nanosecond) -> nanosecond;
time_unit("second") -> second;
time_unit("millisecond") -> millisecond;
time_unit("microsecond") -> microsecond;
time_unit("nanosecond") -> nanosecond;
time_unit(<<"second">>) -> second;
time_unit(<<"millisecond">>) -> millisecond;
time_unit(<<"microsecond">>) -> microsecond;
time_unit(<<"nanosecond">>) -> nanosecond;
time_unit(Any) -> error({invalid_time_unit, Any}).

%%--------------------------------------------------------------------
%% internal: format part
%%--------------------------------------------------------------------

do_formatter(<<>>, Formatter) ->
    lists:reverse(Formatter);
do_formatter(<<"%Y", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [year | Formatter]);
do_formatter(<<"%m", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [month | Formatter]);
do_formatter(<<"%d", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [day | Formatter]);
do_formatter(<<"%H", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [hour | Formatter]);
do_formatter(<<"%M", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [minute | Formatter]);
do_formatter(<<"%S", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [second | Formatter]);
do_formatter(<<"%N", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [nanosecond | Formatter]);
do_formatter(<<"%3N", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [millisecond | Formatter]);
do_formatter(<<"%6N", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [microsecond | Formatter]);
do_formatter(<<"%z", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [timezone | Formatter]);
do_formatter(<<"%:z", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [timezone1 | Formatter]);
do_formatter(<<"%::z", Tail/binary>>, Formatter) ->
    do_formatter(Tail, [timezone2 | Formatter]);
do_formatter(<<Char:8, Tail/binary>>, [Str | Formatter]) when is_list(Str) ->
    do_formatter(Tail, [lists:append(Str, [Char]) | Formatter]);
do_formatter(<<Char:8, Tail/binary>>, Formatter) ->
    do_formatter(Tail, [[Char] | Formatter]).

offset_second_(OffsetSecond) when is_integer(OffsetSecond) -> OffsetSecond;
offset_second_(undefined) ->
    0;
offset_second_("local") ->
    offset_second_(local);
offset_second_(<<"local">>) ->
    offset_second_(local);
offset_second_(local) ->
    UniversalTime = calendar:system_time_to_universal_time(erlang:system_time(second), second),
    LocalTime = erlang:universaltime_to_localtime(UniversalTime),
    LocalSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    UniversalSecs = calendar:datetime_to_gregorian_seconds(UniversalTime),
    LocalSecs - UniversalSecs;
offset_second_(Offset) when is_binary(Offset) ->
    offset_second_(erlang:binary_to_list(Offset));
offset_second_("Z") ->
    0;
offset_second_("z") ->
    0;
offset_second_(Offset) when is_list(Offset) ->
    [Sign | HM] = Offset,
    PosNeg =
        case Sign of
            $+ -> 1;
            $- -> -1;
            _ -> error({bad_time_offset, Offset})
        end,
    {HourStr, MinuteStr, SecondStr} =
        case string:tokens(HM, ":") of
            [H, M] ->
                {H, M, "0"};
            [H, M, S] ->
                {H, M, S};
            [HHMM] when erlang:length(HHMM) == 4 ->
                {string:sub_string(HHMM, 1, 2), string:sub_string(HHMM, 3, 4), "0"};
            _ ->
                error({bad_time_offset, Offset})
        end,
    Hour = str_to_int_or_error(HourStr, {bad_time_offset_hour, HourStr}),
    Minute = str_to_int_or_error(MinuteStr, {bad_time_offset_minute, MinuteStr}),
    Second = str_to_int_or_error(SecondStr, {bad_time_offset_second, SecondStr}),
    (Hour =< 23) orelse error({bad_time_offset_hour, Hour}),
    (Minute =< 59) orelse error({bad_time_offset_minute, Minute}),
    (Second =< 59) orelse error({bad_time_offset_second, Second}),
    PosNeg * (Hour * 3600 + Minute * 60 + Second).

do_format(Time, Unit, Offset, Formatter) ->
    Adjustment = erlang:convert_time_unit(Offset, second, Unit),
    AdjustedTime = Time + Adjustment,
    Factor = factor(Unit),
    Secs = AdjustedTime div Factor,
    DateTime = system_time_to_datetime(Secs),
    {{Year, Month, Day}, {Hour, Min, Sec}} = DateTime,
    Date = #{
        year => padding(Year, 4),
        month => padding(Month, 2),
        day => padding(Day, 2),
        hour => padding(Hour, 2),
        minute => padding(Min, 2),
        second => padding(Sec, 2),
        millisecond => trans_x_second(Unit, millisecond, Time),
        microsecond => trans_x_second(Unit, microsecond, Time),
        nanosecond => trans_x_second(Unit, nanosecond, Time)
    },
    Timezones = formatter_timezones(Offset, Formatter, #{}),
    DateWithZone = maps:merge(Date, Timezones),
    [maps:get(Key, DateWithZone, Key) || Key <- Formatter].

formatter_timezones(_Offset, [], Zones) ->
    Zones;
formatter_timezones(Offset, [Timezone | Formatter], Zones) ->
    case lists:member(Timezone, [timezone, timezone1, timezone2]) of
        true ->
            NZones = Zones#{Timezone => offset_to_timezone(Offset, Timezone)},
            formatter_timezones(Offset, Formatter, NZones);
        false ->
            formatter_timezones(Offset, Formatter, Zones)
    end.

offset_to_timezone(Offset, Timezone) ->
    Sign =
        case Offset >= 0 of
            true ->
                $+;
            false ->
                $-
        end,
    {H, M, S} = seconds_to_time(abs(Offset)),
    %% TODO: Support zone define %:::z
    %%  Numeric time zone with ":" to necessary precision (e.g., -04, +05:30).
    case Timezone of
        timezone ->
            %% +0800
            io_lib:format("~c~2.10.0B~2.10.0B", [Sign, H, M]);
        timezone1 ->
            %% +08:00
            io_lib:format("~c~2.10.0B:~2.10.0B", [Sign, H, M]);
        timezone2 ->
            %% +08:00:00
            io_lib:format("~c~2.10.0B:~2.10.0B:~2.10.0B", [Sign, H, M, S])
    end.

factor(second) -> 1;
factor(millisecond) -> 1000;
factor(microsecond) -> 1000000;
factor(nanosecond) -> 1000000000.

system_time_to_datetime(Seconds) ->
    gregorian_seconds_to_datetime(Seconds + ?SECONDS_FROM_0_TO_1970).

gregorian_seconds_to_datetime(Secs) when Secs >= 0 ->
    Days = Secs div ?SECONDS_PER_DAY,
    Rest = Secs rem ?SECONDS_PER_DAY,
    {gregorian_days_to_date(Days), seconds_to_time(Rest)}.

seconds_to_time(Secs) when Secs >= 0, Secs < ?SECONDS_PER_DAY ->
    Secs0 = Secs rem ?SECONDS_PER_DAY,
    Hour = Secs0 div ?SECONDS_PER_HOUR,
    Secs1 = Secs0 rem ?SECONDS_PER_HOUR,
    Minute = Secs1 div ?SECONDS_PER_MINUTE,
    Second = Secs1 rem ?SECONDS_PER_MINUTE,
    {Hour, Minute, Second}.

gregorian_days_to_date(Days) ->
    {Year, DayOfYear} = day_to_year(Days),
    {Month, DayOfMonth} = year_day_to_date(Year, DayOfYear),
    {Year, Month, DayOfMonth}.

day_to_year(DayOfEpoch) when DayOfEpoch >= 0 ->
    YMax = DayOfEpoch div ?DAYS_PER_YEAR,
    YMin = DayOfEpoch div ?DAYS_PER_LEAP_YEAR,
    {Y1, D1} = dty(YMin, YMax, DayOfEpoch, dy(YMin), dy(YMax)),
    {Y1, DayOfEpoch - D1}.

year_day_to_date(Year, DayOfYear) ->
    ExtraDay =
        case is_leap_year(Year) of
            true ->
                1;
            false ->
                0
        end,
    {Month, Day} = year_day_to_date2(ExtraDay, DayOfYear),
    {Month, Day + 1}.

dty(Min, Max, _D1, DMin, _DMax) when Min == Max ->
    {Min, DMin};
dty(Min, Max, D1, DMin, DMax) ->
    Diff = Max - Min,
    Mid = Min + Diff * (D1 - DMin) div (DMax - DMin),
    MidLength =
        case is_leap_year(Mid) of
            true ->
                ?DAYS_PER_LEAP_YEAR;
            false ->
                ?DAYS_PER_YEAR
        end,
    case dy(Mid) of
        D2 when D1 < D2 ->
            NewMax = Mid - 1,
            dty(Min, NewMax, D1, DMin, dy(NewMax));
        D2 when D1 - D2 >= MidLength ->
            NewMin = Mid + 1,
            dty(NewMin, Max, D1, dy(NewMin), DMax);
        D2 ->
            {Mid, D2}
    end.

dy(Y) when Y =< 0 ->
    0;
dy(Y) ->
    X = Y - 1,
    X div 4 - X div 100 + X div 400 + X * ?DAYS_PER_YEAR + ?DAYS_PER_LEAP_YEAR.

is_leap_year(Y) when is_integer(Y), Y >= 0 ->
    is_leap_year1(Y).

is_leap_year1(Year) when Year rem 4 =:= 0, Year rem 100 > 0 ->
    true;
is_leap_year1(Year) when Year rem 400 =:= 0 ->
    true;
is_leap_year1(_) ->
    false.

year_day_to_date2(_, Day) when Day < 31 ->
    {1, Day};
year_day_to_date2(E, Day) when 31 =< Day, Day < 59 + E ->
    {2, Day - 31};
year_day_to_date2(E, Day) when 59 + E =< Day, Day < 90 + E ->
    {3, Day - (59 + E)};
year_day_to_date2(E, Day) when 90 + E =< Day, Day < 120 + E ->
    {4, Day - (90 + E)};
year_day_to_date2(E, Day) when 120 + E =< Day, Day < 151 + E ->
    {5, Day - (120 + E)};
year_day_to_date2(E, Day) when 151 + E =< Day, Day < 181 + E ->
    {6, Day - (151 + E)};
year_day_to_date2(E, Day) when 181 + E =< Day, Day < 212 + E ->
    {7, Day - (181 + E)};
year_day_to_date2(E, Day) when 212 + E =< Day, Day < 243 + E ->
    {8, Day - (212 + E)};
year_day_to_date2(E, Day) when 243 + E =< Day, Day < 273 + E ->
    {9, Day - (243 + E)};
year_day_to_date2(E, Day) when 273 + E =< Day, Day < 304 + E ->
    {10, Day - (273 + E)};
year_day_to_date2(E, Day) when 304 + E =< Day, Day < 334 + E ->
    {11, Day - (304 + E)};
year_day_to_date2(E, Day) when 334 + E =< Day ->
    {12, Day - (334 + E)}.

trans_x_second(FromUnit, ToUnit, Time) ->
    XSecond = do_trans_x_second(FromUnit, ToUnit, Time),
    Len =
        case ToUnit of
            millisecond -> 3;
            microsecond -> 6;
            nanosecond -> 9
        end,
    padding(XSecond, Len).

do_trans_x_second(second, _, _Time) -> 0;
do_trans_x_second(millisecond, millisecond, Time) -> Time rem 1000;
do_trans_x_second(millisecond, microsecond, Time) -> (Time rem 1000) * 1000;
do_trans_x_second(millisecond, nanosecond, Time) -> (Time rem 1000) * 1000_000;
do_trans_x_second(microsecond, millisecond, Time) -> Time div 1000 rem 1000;
do_trans_x_second(microsecond, microsecond, Time) -> Time rem 1000000;
do_trans_x_second(microsecond, nanosecond, Time) -> (Time rem 1000000) * 1000;
do_trans_x_second(nanosecond, millisecond, Time) -> Time div 1000000 rem 1000;
do_trans_x_second(nanosecond, microsecond, Time) -> Time div 1000 rem 1000000;
do_trans_x_second(nanosecond, nanosecond, Time) -> Time rem 1000000000.

padding(Data, Len) when is_integer(Data) ->
    padding(integer_to_list(Data), Len);
padding(Data, Len) when Len > 0 andalso erlang:length(Data) < Len ->
    [$0 | padding(Data, Len - 1)];
padding(Data, _Len) ->
    Data.

%%--------------------------------------------------------------------
%% internal: formatted_datetime_to_system_time part
%%--------------------------------------------------------------------

do_formatted_datetime_to_system_time(DateStr, Unit, Formatter) ->
    DateInfo = do_parse_date_str(DateStr, Formatter, #{}),
    PrecisionUnit = precision(DateInfo),
    ToPrecisionUnit = fun(Time, FromUnit) ->
        erlang:convert_time_unit(Time, FromUnit, PrecisionUnit)
    end,
    GetRequiredPart = fun(Key) ->
        case maps:get(Key, DateInfo, undefined) of
            undefined -> throw({missing_date_part, Key});
            Value -> Value
        end
    end,
    GetOptionalPart = fun(Key) -> maps:get(Key, DateInfo, 0) end,
    Year = GetRequiredPart(year),
    Month = GetRequiredPart(month),
    Day = GetRequiredPart(day),
    Hour = GetRequiredPart(hour),
    Min = GetRequiredPart(minute),
    Sec = GetRequiredPart(second),
    DateTime = {{Year, Month, Day}, {Hour, Min, Sec}},
    TotalSecs = datetime_to_system_time(DateTime) - GetOptionalPart(parsed_offset),
    check(TotalSecs, DateStr, Unit),
    TotalTime =
        ToPrecisionUnit(TotalSecs, second) +
            ToPrecisionUnit(GetOptionalPart(millisecond), millisecond) +
            ToPrecisionUnit(GetOptionalPart(microsecond), microsecond) +
            ToPrecisionUnit(GetOptionalPart(nanosecond), nanosecond),
    erlang:convert_time_unit(TotalTime, PrecisionUnit, Unit).

check(Secs, _, _) when Secs >= -?SECONDS_FROM_0_TO_1970, Secs < ?SECONDS_FROM_0_TO_10000 ->
    ok;
check(_Secs, DateStr, Unit) ->
    throw({bad_format, #{date_string => DateStr, to_unit => Unit}}).

datetime_to_system_time(DateTime) ->
    calendar:datetime_to_gregorian_seconds(DateTime) - ?SECONDS_FROM_0_TO_1970.

precision(#{nanosecond := _}) -> nanosecond;
precision(#{microsecond := _}) -> microsecond;
precision(#{millisecond := _}) -> millisecond;
precision(#{second := _}) -> second;
precision(_) -> second.

do_parse_date_str(<<>>, _, Result) ->
    Result;
do_parse_date_str(_, [], Result) ->
    Result;
do_parse_date_str(Date, [Key | Formatter], Result) ->
    Size = date_size(Key),
    <<DatePart:Size/binary-unit:8, Tail/binary>> = Date,
    case lists:member(Key, ?DATE_PART) of
        true ->
            %% Note: Here is a fix to make the error reason more sense
            %% when the format or date can't be matched,
            %% but the root reason comment underneath at <ROOT>
            PartValue = str_to_int_or_error(DatePart, bad_formatter_or_date),
            do_parse_date_str(Tail, Formatter, Result#{Key => PartValue});
        false ->
            case lists:member(Key, ?DATE_ZONE_NAME) of
                true ->
                    do_parse_date_str(Tail, Formatter, Result#{
                        parsed_offset => offset_second(DatePart)
                    });
                false ->
                    %% <ROOT>
                    %% Here should have compared the date part with the key,
                    %% but for compatibility, we can't fix it here
                    %% e.g.
                    %% date_to_unix_ts('second','%Y-%m-%d %H-%M-%S', '2022-05-26 10:40:12')
                    %% this is valid in 4.x, but actually '%H-%M-%S' can't match with '10:40:12'
                    %% We cannot ensure whether there are more exceptions in the user's rule
                    do_parse_date_str(Tail, Formatter, Result)
            end
    end.

date_size(Str) when is_list(Str) -> erlang:length(Str);
date_size(year) -> 4;
date_size(month) -> 2;
date_size(day) -> 2;
date_size(hour) -> 2;
date_size(minute) -> 2;
date_size(second) -> 2;
date_size(millisecond) -> 3;
date_size(microsecond) -> 6;
date_size(nanosecond) -> 9;
date_size(timezone) -> 5;
date_size(timezone1) -> 6;
date_size(timezone2) -> 9.

str_to_int_or_error(Str, Error) ->
    case string:to_integer(Str) of
        {Int, []} ->
            Int;
        {Int, <<>>} ->
            Int;
        _ ->
            error(Error)
    end.

%%--------------------------------------------------------------------
%% Unit Test
%%--------------------------------------------------------------------

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
    BigStamp = 1 bsl 37,
    Args = [
        {0, 0, 0, 0},
        {1, 1, 1, 1},
        {BigStamp, BigStamp * 1000, BigStamp, BigStamp * 1000},
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
    BigStamp = 1 bsl 38,
    Args = [
        {-1, -1},
        {"1s", "1s"},
        {BigStamp, 0},
        {0, BigStamp * 1000},
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
