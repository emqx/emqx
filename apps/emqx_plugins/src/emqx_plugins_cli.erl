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

-module(emqx_plugins_cli).

-export([
    list/1,
    describe/2,
    ensure_installed/2,
    ensure_uninstalled/2,
    ensure_started/2,
    ensure_stopped/2,
    restart/2,
    ensure_disabled/2,
    ensure_enabled/3
]).

-include_lib("emqx/include/logger.hrl").

-define(PRINT(EXPR, LOG_FUN),
    print(NameVsn, fun() -> EXPR end(), LOG_FUN, ?FUNCTION_NAME)
).

list(LogFun) ->
    LogFun("~ts~n", [to_json(emqx_plugins:list())]).

describe(NameVsn, LogFun) ->
    case emqx_plugins:describe(NameVsn) of
        {ok, Plugin} ->
            LogFun("~ts~n", [to_json(Plugin)]);
        {error, Reason} ->
            %% this should not happen unless the package is manually installed
            %% corrupted packages installed from emqx_plugins:ensure_installed
            %% should not leave behind corrupted files
            ?SLOG(error, #{
                msg => "failed_to_describe_plugin",
                name_vsn => NameVsn,
                cause => Reason
            }),
            %% do nothing to the CLI console
            ok
    end.

ensure_installed(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_installed(NameVsn), LogFun).

ensure_uninstalled(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_uninstalled(NameVsn), LogFun).

ensure_started(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_started(NameVsn), LogFun).

ensure_stopped(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_stopped(NameVsn), LogFun).

restart(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:restart(NameVsn), LogFun).

ensure_enabled(NameVsn, Position, LogFun) ->
    ?PRINT(emqx_plugins:ensure_enabled(NameVsn, Position), LogFun).

ensure_disabled(NameVsn, LogFun) ->
    ?PRINT(emqx_plugins:ensure_disabled(NameVsn), LogFun).

to_json(Input) ->
    emqx_logger_jsonfmt:best_effort_json(Input).

print(NameVsn, Res, LogFun, Action) ->
    Obj = #{
        action => Action,
        name_vsn => NameVsn
    },
    JsonReady =
        case Res of
            ok ->
                Obj#{result => ok};
            {error, Reason} ->
                Obj#{
                    result => not_ok,
                    cause => Reason
                }
        end,
    LogFun("~ts~n", [to_json(JsonReady)]).
