%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    Config = maps:without([date_format], Config0),
    logger_formatter:check_config(Config).

format(Event, Config) ->
    case maps:get(date_format, Config, rfc3339) of
        rfc3339 ->
            format(Event, Config, rfc3339);
        DateFormatString ->
            format(Event, Config, DateFormatString)
    end.

format(#{msg := Msg0, meta := Meta} = Event, Config, rfc3339) ->
    Msg = maybe_merge(Msg0, Meta),
    logger_formatter:format(Event#{msg := Msg}, Config);
format(#{msg := Msg0, meta := Meta} = Event,  #{template := Template} = Config, DFS) ->
    Msg = maybe_merge(Msg0, Meta),
    Time =
        case maps:get(time, Event, undefined) of
            undefined ->
                erlang:system_time(microsecond);
            T ->
                T
        end,
    Date = emqx_calendar:format(Time, microsecond, local, DFS),
    [Date | logger_formatter:format(Event#{msg := Msg}, Config#{template => remove_time(Template)})].

remove_time([time | Tail]) -> Tail;
remove_time(Template) -> Template.

maybe_merge({report, Report}, Meta) when is_map(Report) ->
    {report, maps:merge(Report, filter(Meta))};
maybe_merge(Report, _Meta) ->
    Report.

filter(Meta) ->
    maps:without(?WITHOUT_MERGE, Meta).
