%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_machine_boot_runtime_deps).

-export([inject/2]).

-type app_name() :: atom().
-type app_deps() :: {app_name(), [app_name()]}.
-type app_selector() :: app_name() | fun((app_name()) -> boolean()).
-type runtime_dep() :: {_WhatDepends :: app_name(), _OnWhat :: app_selector()}.

-spec inject([app_deps()], [runtime_dep()]) -> [app_deps()].
inject(AppDepList, DepSpecs) ->
    AppDep0 = maps:from_list(AppDepList),
    AppDep1 = lists:foldl(
        fun(DepSpec, AppDepAcc) ->
            inject_one_dep(AppDepAcc, DepSpec)
        end,
        AppDep0,
        DepSpecs
    ),
    maps:to_list(AppDep1).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

inject_one_dep(AppDep, {WhatDepends, OnWhatSelector}) ->
    OnWhat = select_apps(OnWhatSelector, maps:keys(AppDep)),
    case AppDep of
        #{WhatDepends := Deps} when is_list(Deps) ->
            AppDep#{WhatDepends => lists:usort(Deps ++ OnWhat)};
        _ ->
            AppDep
    end.

select_apps(AppName, AppNames) when is_atom(AppName) ->
    lists:filter(fun(App) -> App =:= AppName end, AppNames);
select_apps(AppSelector, AppNames) when is_function(AppSelector, 1) ->
    lists:filter(AppSelector, AppNames).
