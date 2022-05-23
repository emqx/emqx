%%--------------------------------------------------------------------
%% Copyright (c) 2021-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger_textfmt).

-define(SECONDS_PER_MINUTE, 60).
-define(SECONDS_PER_HOUR, 3600).
-define(SECONDS_PER_DAY, 86400).
-define(DAYS_PER_YEAR, 365).
-define(DAYS_PER_LEAP_YEAR, 366).
-define(DAYS_FROM_0_TO_1970, 719528).
-define(DAYS_FROM_0_TO_10000, 2932897).
-define(SECONDS_FROM_0_TO_1970, ?DAYS_FROM_0_TO_1970 * ?SECONDS_PER_DAY).
-define(SECONDS_FROM_0_TO_10000, ?DAYS_FROM_0_TO_10000 * ?SECONDS_PER_DAY).

-export([format/2]).
-export([check_config/1]).

%% metadata fields which we do not wish to merge into log data
-define(WITHOUT_MERGE,
        [ report_cb % just a callback
        , time      % formatted as a part of templated message
        , peername  % formatted as a part of templated message
        , clientid  % formatted as a part of templated message
        , gl        % not interesting
        ]).

check_config(Config0) ->
    Config = maps:without([date_format, timezone_offset, timezone], Config0),
    logger_formatter:check_config(Config).

format(#{msg := Msg0, meta := Meta} = Event,
       #{date_format := rfc3339, template := Template0} = Config) ->
    Msg = maybe_merge(Msg0, Meta),
    Template = [time | Template0],
    logger_formatter:format(Event#{msg := Msg}, Config#{template => Template});

format(#{msg := Msg0, meta := Meta} = Event,
       #{timezone_offset := TZO, timezone := TZ, date_format := DFS} = Config) ->
    Msg = maybe_merge(Msg0, Meta),
    Time = maps:get(time, Event, undefined),
    [date_format(Time, TZO, TZ, DFS) | logger_formatter:format(Event#{msg := Msg}, Config)].

maybe_merge({report, Report}, Meta) when is_map(Report) ->
    {report, maps:merge(Report, filter(Meta))};
maybe_merge(Report, _Meta) ->
    Report.

filter(Meta) ->
    maps:without(?WITHOUT_MERGE, Meta).

date_format(undefined, Offset, OffsetStr, Formatter) ->
    Time = erlang:system_time(microsecond),
    date_format(Time, Offset, OffsetStr, Formatter);
date_format(Time, Offset, OffsetStr, Formatter) ->
    Adjustment = erlang:convert_time_unit(Offset, second, microsecond),
    AdjustedTime = Time + Adjustment,
    %% Factor 1000000 for microsecond.
    Factor = 1000000,
    Secs = AdjustedTime div Factor,
    DateTime = system_time_to_datetime(Secs),
    {{Year, Month, Day}, {Hour, Min, Sec}} = DateTime,
    FractionStr = fraction_str(Factor, AdjustedTime),
    Date = #{
        year => padding(Year, 4),
        month => padding(Month, 2),
        day => padding(Day, 2),
        hour => padding(Hour, 2),
        minute => padding(Min, 2),
        second => padding(Sec, 2),
        nano_second => FractionStr,
        timezone => OffsetStr
    },
    [maps:get(Key, Date, Key) || Key <- Formatter].

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

fraction_str(1, _Time) ->
    "";
fraction_str(Factor, Time) ->
    Fraction = Time rem Factor,
    S = integer_to_list(abs(Fraction)),
    [padding(S, log10(Factor) - length(S))].

log10(1000) ->
    3;
log10(1000000) ->
    6;
log10(1000000000) ->
    9.

padding(Data, Len) when is_integer(Data) ->
    padding(integer_to_list(Data), Len);
padding(Data, Len) when Len > 0 andalso erlang:length(Data) < Len ->
    [$0 | padding(Data, Len - 1)];
padding(Data, _Len) ->
    Data.
