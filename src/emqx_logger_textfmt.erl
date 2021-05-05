%%--------------------------------------------------------------------
%% Copyright (c) 2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

check_config(X) -> logger_formatter:check_config(X).

format(#{msg := Msg0, meta := Meta} = Event, Config) ->
    Msg = maybe_merge(Msg0, Meta),
    logger_formatter:format(Event#{msg := Msg}, Config).

maybe_merge({report, Report}, Meta) when is_map(Report) ->
    {report, maps:merge(rename(Report), filter(Meta))};
maybe_merge(Report, _Meta) ->
    Report.

filter(Meta) ->
    maps:without(?WITHOUT_MERGE, Meta).

rename(#{'$kind' := Kind} = Meta0) -> % snabbkaffe
    Meta = maps:remove('$kind', Meta0),
    Meta#{msg => Kind};
rename(Meta) ->
    Meta.
