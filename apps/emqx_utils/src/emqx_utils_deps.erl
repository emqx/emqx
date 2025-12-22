%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_deps).

-feature(maybe_expr, enable).

-export([
    get_call_dependents/2,
    get_include_dependents/2,
    compute_transitive_closure/2
]).

%% @doc Get "used-by" dependencies from remote function calls.
%% RelDir Path to the release lib directory (e.g., "_build/emqx-enterprise/lib")
%% ModToAppMap Map of Module => App atom.
%% Returns map of App => Set of apps that call it (directly)
get_call_dependents(RelDir, ModToAppMap) ->
    AllRemoteCalls = get_all_remote_calls(RelDir),
    % Build EmqxAppsSet from ModToAppMap values
    EmqxAppsSet = sets:from_list(maps:values(ModToAppMap)),
    EmqxApps = sets:to_list(EmqxAppsSet),
    % Initialize with empty sets for all apps
    Acc0 = maps:from_list([{App, sets:new()} || App <- EmqxApps]),
    collect_callee_to_caller_map(AllRemoteCalls, ModToAppMap, EmqxAppsSet, Acc0).

%% @doc Get "used-by" dependencies from include_lib directives.
%% SrcDir Path to the apps directory (e.g., "apps")
%% AppNames List of app atoms
%% Returns map of App => Set of apps that include headers from it (directly)
get_include_dependents(SrcDir, AppNames) ->
    EmqxAppsSet = sets:from_list(AppNames),
    % Filter to only apps that have src directories
    AppsWithSrc = lists:filter(
        fun(App) ->
            AppName = atom_to_list(App),
            SrcPath = filename:join([SrcDir, AppName, "src"]),
            filelib:is_dir(SrcPath)
        end,
        AppNames
    ),
    % Initialize with empty sets for all apps
    Acc0 = maps:from_list([{App, sets:new()} || App <- AppNames]),
    get_include_dependents_loop(AppsWithSrc, SrcDir, EmqxAppsSet, Acc0).

get_include_dependents_loop([], _SrcDir, _EmqxAppsSet, Acc) ->
    Acc;
get_include_dependents_loop([App | Rest], SrcDir, EmqxAppsSet, Acc) ->
    AppName = atom_to_list(App),
    AppSrcPath = filename:join([SrcDir, AppName, "src"]),
    IncludeDeps = get_include_lib_deps(AppSrcPath, EmqxAppsSet),
    % For each app that App includes headers from, add App to that app's including set
    NewAcc = sets:fold(
        fun(IncludedApp, AccMap) ->
            add_dependent(AccMap, IncludedApp, App)
        end,
        Acc,
        IncludeDeps
    ),
    get_include_dependents_loop(Rest, SrcDir, EmqxAppsSet, NewAcc).

%% Internal functions for remote calls

get_all_remote_calls(LibDir) ->
    filelib:is_dir(LibDir) orelse error({directory_not_found, LibDir}),
    XrefServer = gen_deps_xref,
    {ok, _} = xref:start(XrefServer),
    try
        ok = xref:set_default(XrefServer, [{warnings, false}]),
        {ok, _} = xref:add_release(XrefServer, LibDir),
        {ok, Calls} = xref:q(XrefServer, "XC"),
        length(Calls) > 0 orelse error(no_remote_calls_found),
        Calls
    after
        xref:stop(XrefServer)
    end.

collect_callee_to_caller_map(RemoteCalls, ModuleToAppMap, EmqxAppsSet, Acc0) ->
    collect_callee_to_caller_map_loop(RemoteCalls, ModuleToAppMap, EmqxAppsSet, Acc0).

collect_callee_to_caller_map_loop([], _ModuleToAppMap, _EmqxAppsSet, Acc) ->
    Acc;
collect_callee_to_caller_map_loop(
    [{{CallerModule, _Fun, _Arity}, {CalleeModule, _Fun2, _Arity2}} | Rest],
    ModuleToAppMap,
    EmqxAppsSet,
    Acc
) ->
    NewAcc =
        maybe
            {ok, CallerApp} ?= get_emqx_app(CallerModule, ModuleToAppMap, EmqxAppsSet),
            {ok, CalleeApp} ?= get_emqx_app(CalleeModule, ModuleToAppMap, EmqxAppsSet),
            false ?= (CallerApp =:= CalleeApp),
            % Add CallerApp to CalleeApp's caller set
            add_dependent(Acc, CalleeApp, CallerApp)
        else
            _ ->
                Acc
        end,
    collect_callee_to_caller_map_loop(Rest, ModuleToAppMap, EmqxAppsSet, NewAcc).

get_emqx_app(Module, ModuleToAppMap, EmqxAppsSet) ->
    case maps:get(Module, ModuleToAppMap, undefined) of
        undefined ->
            not_emqx_app;
        App ->
            case sets:is_element(App, EmqxAppsSet) of
                true -> {ok, App};
                false -> not_emqx_app
            end
    end.

%% Internal functions for include_lib directives

get_include_lib_deps(SrcPath, EmqxAppsSet) ->
    % Use wildcard to recursively find all .erl files in src directory
    Pattern = filename:join(SrcPath, "**/*.erl"),
    ErlFiles = filelib:wildcard(Pattern),
    lists:foldl(
        fun(ErlFile, Acc) ->
            case file:read_file(ErlFile) of
                {ok, Content} ->
                    Deps = parse_include_lib_deps(Content, EmqxAppsSet),
                    sets:union(Acc, Deps);
                {error, _} ->
                    Acc
            end
        end,
        sets:new(),
        ErlFiles
    ).

parse_include_lib_deps(Content, EmqxAppsSet) ->
    Lines = binary:split(Content, <<"\n">>, [global]),
    lists:foldl(
        fun(Line, Acc) ->
            case parse_include_lib_line(Line) of
                {ok, App} ->
                    case sets:is_element(App, EmqxAppsSet) of
                        true -> sets:add_element(App, Acc);
                        false -> Acc
                    end;
                error ->
                    Acc
            end
        end,
        sets:new(),
        Lines
    ).

parse_include_lib_line(Line) when is_binary(Line) ->
    % Match: -include_lib("emqx/include/...") or -include_lib("emqx_*/include/...")
    Trimmed = trim_whitespace(Line),
    TrimmedStr =
        case is_binary(Trimmed) of
            true -> binary_to_list(Trimmed);
            false -> Trimmed
        end,
    case re:run(TrimmedStr, "^-include_lib\\(\"([^\"]+)\"\\)", [{capture, all_but_first, list}]) of
        {match, [Path]} ->
            extract_app_from_path(Path);
        nomatch ->
            error
    end;
parse_include_lib_line(_Line) ->
    error.

extract_app_from_path(Path) ->
    % string:split with leading option always returns at least one element when Path is non-empty
    % Path is guaranteed to be non-empty as it comes from a regex match
    [AppName | _] = string:split(Path, "/", leading),
    % All apps in apps/ directory are emqx apps, so accept any app name
    {ok, list_to_atom(AppName)}.

trim_whitespace(Bin) when is_binary(Bin) ->
    re:replace(Bin, "^\\s+|\\s+$", "", [global, {return, binary}]).

%% Helper functions

%% @doc Add a dependent app to the dependents map.
%% Dependents Map of App => Set of apps that use it
%% App The app that is being used
%% UserApp The app that uses App
%% Returns updated Dependents map
add_dependent(Dependents, App, UserApp) ->
    maps:update_with(
        App,
        fun(OldSet) -> sets:add_element(UserApp, OldSet) end,
        sets:from_list([UserApp]),
        Dependents
    ).

%% @doc Compute transitive closure of "used-by" relationships.
%% UsedByMaps List of maps, each map is App => Set of apps that directly use it
%% AllApps List of all apps
%% Returns map of App => Set of apps that transitively use it
compute_transitive_closure(UsedByMaps, AllApps) when is_list(UsedByMaps) ->
    % Merge all maps first
    MergedMap = merge_dependents(UsedByMaps),
    % Initialize result map with all apps (even if they have no dependencies)
    % For each app, compute all apps that transitively use it
    % Using BFS to traverse the dependency graph
    lists:foldl(
        fun(App, Acc) ->
            TransitiveUsers = compute_transitive_users(App, MergedMap, AllApps),
            maps:put(App, TransitiveUsers, Acc)
        end,
        #{},
        AllApps
    ).

%% @doc Merge multiple dependency maps into a single map.
%% UsedByMaps List of maps, each map is App => Set of apps that directly use it
%% Returns merged map where sets are unioned for overlapping keys
merge_dependents(UsedByMaps) ->
    merge_dependents_loop(UsedByMaps, #{}).

merge_dependents_loop([], Acc) ->
    Acc;
merge_dependents_loop([Map | Rest], Acc) ->
    NewAcc = maps:fold(
        fun(App, CallerSet, AccMap) ->
            case maps:find(App, AccMap) of
                {ok, ExistingSet} ->
                    maps:put(App, sets:union(ExistingSet, CallerSet), AccMap);
                error ->
                    maps:put(App, CallerSet, AccMap)
            end
        end,
        Acc,
        Map
    ),
    merge_dependents_loop(Rest, NewAcc).

%% BFS traversal: start with TargetApp, find all apps that use it (directly or transitively)
%% UsedByMap: App => Set of apps that directly use it
%% We want: all apps that transitively use TargetApp
compute_transitive_users(TargetApp, UsedByMap, _AllApps) ->
    compute_transitive_users_bfs([TargetApp], UsedByMap, sets:new(), sets:new()).

compute_transitive_users_bfs([], _UsedByMap, _Visited, Acc) ->
    Acc;
compute_transitive_users_bfs([App | Queue], UsedByMap, Visited, Acc) ->
    case sets:is_element(App, Visited) of
        true ->
            % Already visited, skip
            compute_transitive_users_bfs(Queue, UsedByMap, Visited, Acc);
        false ->
            % Mark as visited
            NewVisited = sets:add_element(App, Visited),
            % Get direct users of this app
            DirectUsers = maps:get(App, UsedByMap, sets:new()),
            % Add direct users to accumulator
            NewAcc = sets:union(Acc, DirectUsers),
            % Add direct users to queue for further traversal (find their users)
            NewQueue = Queue ++ sets:to_list(DirectUsers),
            compute_transitive_users_bfs(NewQueue, UsedByMap, NewVisited, NewAcc)
    end.
