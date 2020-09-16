%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_exhook_cli).

-include("emqx_exhook.hrl").

-export([cli/1]).

cli(["drivers", "list"]) ->
    if_enabled(fun() ->
        Drivers = emqx_exhook:list(),
        [emqx_ctl:print("Driver(~s)~n", [emqx_exhook_driver:format(Driver)]) || Driver <- Drivers]
    end);

cli(["drivers", "enable", Name0]) ->
    if_enabled(fun() ->
        Name = list_to_atom(Name0),
        case proplists:get_value(Name, application:get_env(?APP, drivers, [])) of
            undefined ->
                emqx_ctl:print("not_found~n");
            Opts ->
                print(emqx_exhook:enable(Name, Opts))
        end
    end);

cli(["drivers", "disable", Name]) ->
    if_enabled(fun() ->
        print(emqx_exhook:disable(list_to_atom(Name)))
    end);

cli(["drivers", "stats"]) ->
    if_enabled(fun() ->
        [emqx_ctl:print("~-35s:~w~n", [Name, N]) || {Name, N} <- stats()]
    end);

cli(_) ->
    emqx_ctl:usage([{"exhook drivers list", "List all running drivers"},
                    {"exhook drivers enable <Name>", "Enable a driver with configurations"},
                    {"exhook drivers disable <Name>", "Disable a driver"},
                    {"exhook drivers stats", "Print drivers statistic"}]).

print(ok) ->
    emqx_ctl:print("ok~n");
print({error, Reason}) ->
    emqx_ctl:print("~p~n", [Reason]).

%%--------------------------------------------------------------------
%% Internal funcs
%%--------------------------------------------------------------------

if_enabled(Fun) ->
    case lists:keymember(?APP, 1, application:which_applications()) of
        true -> Fun();
        _ -> hint()
    end.

hint() ->
    emqx_ctl:print("Please './bin/emqx_ctl plugins load emqx_exhook' first.~n").

stats() ->
    lists:foldr(fun({K, N}, Acc) ->
        case atom_to_list(K) of
            "exhook." ++ Key -> [{Key, N}|Acc];
            _ -> Acc
        end
    end, [], emqx_metrics:all()).
