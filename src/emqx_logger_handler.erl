%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_logger_handler).

-export([log/2]).

-export([init/0]).

init() ->
    logger:add_handler(emqx_logger_handler, 
                       emqx_logger_handler, 
                       #{level => error,
                         filters => [{easy_filter, {fun filter_by_level/2, []}}],
                         filters_default => stop}).

-spec log(LogEvent, Config) -> ok when LogEvent :: logger:log_event(), Config :: logger:handler_config().
log(#{msg := {report, #{report := [{supervisor, SupName},
                                   {errorContext, Error},
                                   {reason, Reason},
                                   {offender, _}]}}}, _Config) ->
    alarm_handler:set_alarm({supervisor_report, [{supervisor, SupName},
                                                 {errorContext, Error},
                                                 {reason, Reason}]}),
    ok;
log(_LogEvent, _Config) ->
    ok.

filter_by_level(LogEvent = #{level := error}, _Extra) ->
    LogEvent;
filter_by_level(_LogEvent, _Extra) ->
    stop.
