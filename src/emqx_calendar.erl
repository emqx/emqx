%%--------------------------------------------------------------------
%% Copyright (c) 2019-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_calendar).

-define(SECONDS_PER_MINUTE, 60).
-define(SECONDS_PER_HOUR, 3600).
-define(SECONDS_PER_DAY, 86400).
-define(DAYS_PER_YEAR, 365).
-define(DAYS_PER_LEAP_YEAR, 366).
-define(DAYS_FROM_0_TO_1970, 719528).
-define(SECONDS_FROM_0_TO_1970, ?DAYS_FROM_0_TO_1970 * ?SECONDS_PER_DAY).

-export([ formatter/1
        , format/3
        , format/4
        , parse/3
        , offset_second/1
        ]).

-define(DATE_PART,
    [ year
    , month
    , day
    , hour
    , minute
    , second
    , nanosecond
    , millisecond
    , microsecond
    ]).

-define(DATE_ZONE_NAME,
    [ timezone
    , timezone1
    , timezone2
    ]).

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

parse(DateStr, Unit, FormatterBin) when is_binary(FormatterBin) ->
    parse(DateStr, Unit, formatter(FormatterBin));
parse(DateStr, Unit, Formatter) ->
    do_parse(DateStr, Unit, Formatter).
%% -------------------------------------------------------------------------------------------------
%% internal

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
time_unit(<<"nanosecond">>) -> nanosecond.

%% -------------------------------------------------------------------------------------------------
%% internal: format part

do_formatter(<<>>, Formatter) -> lists:reverse(Formatter);
do_formatter(<<"%Y", Tail/binary>>, Formatter) -> do_formatter(Tail, [year | Formatter]);
do_formatter(<<"%m", Tail/binary>>, Formatter) -> do_formatter(Tail, [month | Formatter]);
do_formatter(<<"%d", Tail/binary>>, Formatter) -> do_formatter(Tail, [day | Formatter]);
do_formatter(<<"%H", Tail/binary>>, Formatter) -> do_formatter(Tail, [hour | Formatter]);
do_formatter(<<"%M", Tail/binary>>, Formatter) -> do_formatter(Tail, [minute | Formatter]);
do_formatter(<<"%S", Tail/binary>>, Formatter) -> do_formatter(Tail, [second | Formatter]);
do_formatter(<<"%N", Tail/binary>>, Formatter) -> do_formatter(Tail, [nanosecond | Formatter]);
do_formatter(<<"%3N", Tail/binary>>, Formatter) -> do_formatter(Tail, [millisecond | Formatter]);
do_formatter(<<"%6N", Tail/binary>>, Formatter) -> do_formatter(Tail, [microsecond | Formatter]);
do_formatter(<<"%z", Tail/binary>>, Formatter) -> do_formatter(Tail, [timezone | Formatter]);
do_formatter(<<"%:z", Tail/binary>>, Formatter) -> do_formatter(Tail, [timezone1 | Formatter]);
do_formatter(<<"%::z", Tail/binary>>, Formatter) -> do_formatter(Tail, [timezone2 | Formatter]);
do_formatter(<<Char:8, Tail/binary>>, [Str | Formatter]) when is_list(Str) ->
    do_formatter(Tail, [lists:append(Str, [Char]) | Formatter]);
do_formatter(<<Char:8, Tail/binary>>, Formatter) -> do_formatter(Tail, [[Char] | Formatter]).

offset_second_(OffsetSecond) when is_integer(OffsetSecond) -> OffsetSecond;
offset_second_(undefined) -> 0;
offset_second_("local") -> offset_second_(local);
offset_second_(<<"local">>) -> offset_second_(local);
offset_second_(local) ->
    UniversalTime = calendar:system_time_to_universal_time(erlang:system_time(second), second),
    LocalTime = erlang:universaltime_to_localtime(UniversalTime),
    LocalSecs = calendar:datetime_to_gregorian_seconds(LocalTime),
    UniversalSecs = calendar:datetime_to_gregorian_seconds(UniversalTime),
    LocalSecs - UniversalSecs;
offset_second_(Offset) when is_binary(Offset) ->
    offset_second_(erlang:binary_to_list(Offset));
offset_second_("Z") -> 0;
offset_second_("z") -> 0;
offset_second_(Offset) when is_list(Offset) ->
    Sign = hd(Offset),
    ((Sign == $+) orelse (Sign == $-))
        orelse error({bad_time_offset, Offset}),
    Signs = #{$+ => 1, $- => -1},
    PosNeg = maps:get(Sign, Signs),
    [Sign | HM] = Offset,
    {HourStr, MinuteStr, SecondStr} =
        case string:tokens(HM, ":") of
            [H, M] ->
                {H, M, "0"};
            [H, M, S] ->
                {H, M, S};
            [HHMM] when erlang:length(HHMM) == 4 ->
                {string:sub_string(HHMM, 1,2), string:sub_string(HHMM, 3,4), "0"};
            _ ->
                error({bad_time_offset, Offset})
        end,
    Hour = erlang:list_to_integer(HourStr),
    Minute = erlang:list_to_integer(MinuteStr),
    Second = erlang:list_to_integer(SecondStr),
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

formatter_timezones(_Offset, [], Zones) -> Zones;
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

factor(second)      -> 1;
factor(millisecond) -> 1000;
factor(microsecond) -> 1000000;
factor(nanosecond)  -> 1000000000.

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

%% -------------------------------------------------------------------------------------------------
%% internal
%% parse part

do_parse(DateStr, Unit, Formatter) ->
    DateInfo = do_parse_date_str(DateStr, Formatter, #{}),
    {Precise, PrecisionUnit} = precision(DateInfo),
    Counter =
        fun
            (year, V, Res) ->
                Res + dy(V) * ?SECONDS_PER_DAY * Precise - (?SECONDS_FROM_0_TO_1970 * Precise);
            (month, V, Res) ->
                Res + dm(V) * ?SECONDS_PER_DAY * Precise;
            (day, V, Res) ->
                Res + (V * ?SECONDS_PER_DAY * Precise);
            (hour, V, Res) ->
                Res + (V * ?SECONDS_PER_HOUR * Precise);
            (minute, V, Res) ->
                Res + (V * ?SECONDS_PER_MINUTE * Precise);
            (second, V, Res) ->
                Res + V * Precise;
            (millisecond, V, Res) ->
                case PrecisionUnit of
                    millisecond ->
                        Res + V;
                    microsecond ->
                        Res + (V * 1000);
                    nanosecond ->
                        Res + (V * 1000000)
                end;
            (microsecond, V, Res) ->
                case PrecisionUnit of
                    microsecond ->
                        Res + V;
                    nanosecond ->
                        Res + (V * 1000)
                end;
            (nanosecond, V, Res) ->
                Res + V;
            (parsed_offset, V, Res) ->
                Res - V
        end,
    Count = maps:fold(Counter, 0, DateInfo) - (?SECONDS_PER_DAY * Precise),
    erlang:convert_time_unit(Count, PrecisionUnit, Unit).

precision(#{nanosecond := _}) -> {1000_000_000, nanosecond};
precision(#{microsecond := _}) -> {1000_000, microsecond};
precision(#{millisecond := _}) -> {1000, millisecond};
precision(#{second := _}) -> {1, second};
precision(_) -> {1, second}.

do_parse_date_str(<<>>, _, Result) -> Result;
do_parse_date_str(_, [], Result) -> Result;
do_parse_date_str(Date, [Key | Formatter], Result) ->
    Size = date_size(Key),
    <<DatePart:Size/binary-unit:8, Tail/binary>> = Date,
    case lists:member(Key, ?DATE_PART) of
        true ->
            do_parse_date_str(Tail, Formatter, Result#{Key => erlang:binary_to_integer(DatePart)});
        false ->
            case lists:member(Key, ?DATE_ZONE_NAME) of
                true ->
                    do_parse_date_str(Tail, Formatter, Result#{parsed_offset => offset_second(DatePart)});
                false ->
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

dm(1) -> 0;
dm(2) -> 31;
dm(3) -> 59;
dm(4) -> 90;
dm(5) -> 120;
dm(6) -> 151;
dm(7) -> 181;
dm(8) -> 212;
dm(9) -> 243;
dm(10) -> 273;
dm(11) -> 304;
dm(12) -> 334.
