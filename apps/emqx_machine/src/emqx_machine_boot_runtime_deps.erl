%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
