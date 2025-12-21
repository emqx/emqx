#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
%% Generate "used-by" dependency relationships for EMQX apps
%%
%% This script analyzes BEAM files and source code to determine which apps
%% use (depend on) each app. The output is in "used-by" format (reverse
%% dependency direction), meaning if app1 uses app2, the output will show
%% "app2: app1" (app2 is used by app1).
%%
%% Analysis methods:
%% 1. Remote function calls: Uses xref to scan all BEAM files and find
%%    all remote calls (XC query). Maps caller and callee modules to their
%%    respective apps using module-to-app mapping from .app files.
%% 2. Include directives: Parses -include_lib() directives from source files
%%    to find header file dependencies between apps.
%%
%% Special handling:
%% - emqx and emqx_conf are considered used by all other apps
%% - Self-dependencies are excluded
%% - Transitive closure is computed: if app1 uses app2, and app2 uses app3,
%%   then app1 transitively uses app3
%%
%% Output format (deps.txt):
%%   app_name: user1 user2 user3
%%   app_name: all          (if used by all apps)
%%   app_name: none         (if used by no other apps)
%%
%% Usage:
%%   ./scripts/gen_deps.escript
%%
%% Requirements:
%%   - Project must be compiled (BEAM files in _build/emqx-enterprise/lib/)
%%   - OTP 25+ (for maybe expressions)
%%--------------------------------------------------------------------

-feature(maybe_expr, enable).
-mode(compile).

main(_Args) ->
    BuildDir = "_build/emqx-enterprise",
    AppsDir = "apps",
    OutputFile = "deps.txt",

    io:format("Scanning beam files in ~s...~n", [BuildDir]),

    % Get all emqx apps from apps/ directory
    EmqxApps = get_emqx_apps(AppsDir),
    io:format("Found ~p emqx apps~n", [length(EmqxApps)]),

    % Build used_by relationships (reverse dependency direction)
    DepsList = build_deps_map(BuildDir, AppsDir, EmqxApps),

    % Write to file
    ok = file:write_file(OutputFile, DepsList),

    io:format("Used-by relationships written to ~s~n", [OutputFile]).

get_emqx_apps(AppsDir) ->
    case file:list_dir(AppsDir) of
        {ok, Files} ->
            lists:filtermap(
                fun(Name) ->
                    case is_emqx_app(Name) of
                        true -> {true, list_to_atom(Name)};
                        false -> false
                    end
                end,
                Files
            );
        {error, _} ->
            []
    end.

is_emqx_app("emqx") ->
    true;
is_emqx_app(Name) ->
    case string:prefix(Name, "emqx_") of
        nomatch -> false;
        _ -> true
    end.

build_deps_map(BuildDir, AppsDir, EmqxApps) ->
    EmqxAppsSet = sets:from_list(EmqxApps),

    % Filter to only apps that have beam directories
    AppsWithBeams = lists:filter(
        fun(App) ->
            AppName = atom_to_list(App),
            BeamPath = filename:join([BuildDir, "lib", AppName, "ebin"]),
            filelib:is_dir(BeamPath)
        end,
        EmqxApps
    ),

    % Build module-to-app map by reading .app files
    % e.g., emqx_zone_schema -> emqx (from emqx.app modules list)
    ModuleToAppMap = build_module_to_app_map(BuildDir, AppsWithBeams),

    % Use xref to get all remote calls from all apps at once
    LibDir = filename:join([BuildDir, "lib"]),
    AllRemoteCalls = get_all_remote_calls(LibDir),

    % Collect callee-to-caller map from remote calls
    % When CallerApp calls CalleeApp, add CallerApp to CalleeApp's caller set
    % Skip collecting callers for emqx and emqx_conf since they're used by all apps
    CalleeToCallerMap = collect_callee_to_caller_map(
        AllRemoteCalls, ModuleToAppMap, EmqxAppsSet, EmqxApps
    ),

    % Collect included-to-including map from include_lib directives
    % When App1 includes headers from App2, add App1 to App2's including set
    % Skip collecting includers for emqx and emqx_conf since they're used by all apps
    IncludedToIncludingMap = collect_included_to_including_map(
        AppsDir, AppsWithBeams, EmqxAppsSet, EmqxApps
    ),

    % Merge the two maps
    UsedByMap = maps:fold(
        fun(App, CallerSet, Acc) ->
            case maps:find(App, Acc) of
                {ok, ExistingSet} ->
                    maps:put(App, sets:union(ExistingSet, CallerSet), Acc);
                error ->
                    maps:put(App, CallerSet, Acc)
            end
        end,
        IncludedToIncludingMap,
        CalleeToCallerMap
    ),
    All = sets:from_list(EmqxApps),
    UsedBy = maps:put(emqx, All, maps:put(emqx_conf, All, UsedByMap)),

    % Compute transitive closure: for each app, find all apps that transitively use it
    UsedByTransitive = compute_transitive_closure(UsedBy, EmqxApps),

    % Final step: Convert sets to sorted lists and format output
    % Format: app1: app2,app3 (where app2 and app3 transitively use app1)
    % If UsedBySet + {App} = AllApps, output "all" instead
    lists:foldr(
        fun({App, UsedBySet}, Acc) ->
            UsedByList = sets:to_list(UsedBySet),
            Filtered = lists:filter(fun(User) -> User =/= App end, UsedByList),
            AppStr = atom_to_list(App),
            % Check if UsedBySet + {App} = AllApps
            UsedByWithSelf = sets:add_element(App, UsedBySet),
            case sets:is_subset(UsedByWithSelf, All) andalso sets:is_subset(All, UsedByWithSelf) of
                true ->
                    [[AppStr, ": all\n"] | Acc];
                false ->
                    case Filtered of
                        [] ->
                            [[AppStr, ": none\n"] | Acc];
                        _ ->
                            Sorted = lists:sort(Filtered),
                            UsedByStr = string:join([atom_to_list(U) || U <- Sorted], " "),
                            [[AppStr, ": ", UsedByStr, "\n"] | Acc]
                    end
            end
        end,
        [],
        lists:keysort(1, maps:to_list(UsedByTransitive))
    ).

compute_transitive_closure(UsedByMap, AllApps) ->
    % For each app, compute all apps that transitively use it
    % Using BFS to traverse the dependency graph
    maps:map(
        fun(App, _DirectUsers) ->
            compute_transitive_users(App, UsedByMap, AllApps)
        end,
        UsedByMap
    ).

compute_transitive_users(TargetApp, UsedByMap, _AllApps) ->
    % BFS traversal: start with TargetApp, find all apps that use it (directly or transitively)
    % UsedByMap: App => Set of apps that directly use it
    % We want: all apps that transitively use TargetApp
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

collect_callee_to_caller_map(RemoteCalls, ModuleToAppMap, EmqxAppsSet, EmqxApps) ->
    % Collect callee-to-caller map from remote calls
    % Format: [{CallerMFA, CalleeMFA}, ...] where MFA = {Module, Function, Arity}
    % When CallerApp calls CalleeApp, add CallerApp to CalleeApp's caller set
    % Skip collecting callers for emqx and emqx_conf since they're used by all apps
    % Initialize with empty sets for all apps
    Acc0 = maps:from_list([{App, sets:new()} || App <- EmqxApps]),
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
            % Skip if CalleeApp is emqx or emqx_conf (they're used by all apps)
            false ?= (CalleeApp =:= emqx),
            false ?= (CalleeApp =:= emqx_conf),
            % Add CallerApp to CalleeApp's caller set
            maps:update_with(
                CalleeApp,
                fun(OldSet) -> sets:add_element(CallerApp, OldSet) end,
                sets:from_list([CallerApp]),
                Acc
            )
        else
            _ -> Acc
        end,
    collect_callee_to_caller_map_loop(Rest, ModuleToAppMap, EmqxAppsSet, NewAcc).

collect_included_to_including_map(AppsDir, AppsWithBeams, EmqxAppsSet, EmqxApps) ->
    % Collect included-to-including map from include_lib directives
    % When App1 includes headers from App2, add App1 to App2's including set
    % Skip collecting includers for emqx and emqx_conf since they're used by all apps
    % Initialize with empty sets for all apps
    Acc0 = maps:from_list([{App, sets:new()} || App <- EmqxApps]),
    lists:foldl(
        fun(App, Acc) ->
            AppName = atom_to_list(App),
            SrcPath = filename:join([AppsDir, AppName, "src"]),
            IncludeDeps = get_include_lib_deps(SrcPath, EmqxAppsSet),
            % For each app that App includes headers from, add App to that app's including set
            % Skip if IncludedApp is emqx or emqx_conf (they're used by all apps)
            sets:fold(
                fun(IncludedApp, AccMap) ->
                    case IncludedApp =/= emqx andalso IncludedApp =/= emqx_conf of
                        true ->
                            maps:update_with(
                                IncludedApp,
                                fun(OldSet) -> sets:add_element(App, OldSet) end,
                                sets:from_list([App]),
                                AccMap
                            );
                        false ->
                            AccMap
                    end
                end,
                Acc,
                IncludeDeps
            )
        end,
        Acc0,
        AppsWithBeams
    ).

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

build_module_to_app_map(BuildDir, AppsWithBeams) ->
    lists:foldl(
        fun(App, Acc) ->
            AppName = atom_to_list(App),
            AppFile = filename:join([BuildDir, "lib", AppName, "ebin", AppName ++ ".app"]),
            {ok, [{application, _AppAtom, AppData}]} = file:consult(AppFile),
            Modules = proplists:get_value(modules, AppData, []),
            lists:foldl(
                fun(Module, MapAcc) ->
                    maps:put(Module, App, MapAcc)
                end,
                Acc,
                Modules
            )
        end,
        #{},
        AppsWithBeams
    ).

get_all_remote_calls(LibDir) ->
    filelib:is_dir(LibDir) orelse error({directory_not_found, LibDir}),
    XrefServer = gen_deps_xref,
    {ok, _} = xref:start(XrefServer),
    try
        xref:set_default(XrefServer, [{warnings, false}]),
        {ok, _} = xref:add_release(XrefServer, LibDir),
        {ok, Calls} = xref:q(XrefServer, "XC"),
        length(Calls) > 0 orelse error(no_remote_calls_found),
        Calls
    after
        xref:stop(XrefServer)
    end.

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
    case string:split(Path, "/", leading) of
        [AppName | _] ->
            case is_emqx_app(AppName) of
                true ->
                    {ok, list_to_atom(AppName)};
                false ->
                    error
            end;
        _ ->
            error
    end.

trim_whitespace(Bin) when is_binary(Bin) ->
    re:replace(Bin, "^\\s+|\\s+$", "", [global, {return, binary}]).
