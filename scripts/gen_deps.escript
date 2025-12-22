#!/usr/bin/env escript
%%! -pa _build/emqx-enterprise/lib/emqx_utils/ebin
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
                    % All apps in apps/ directory are emqx apps
                    % Check if it's a directory
                    Path = filename:join([AppsDir, Name]),
                    case filelib:is_dir(Path) of
                        true -> {true, list_to_atom(Name)};
                        false -> false
                    end
                end,
                Files
            );
        {error, _} ->
            []
    end.

build_deps_map(BuildDir, AppsDir, EmqxApps) ->
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
    CalleeToCallerMap = emqx_utils_deps:get_call_dependents(LibDir, ModuleToAppMap),

    % Collect included-to-including map from include_lib directives
    % When App1 includes headers from App2, add App1 to App2's including set
    IncludedToIncludingMap = emqx_utils_deps:get_include_dependents(AppsDir, EmqxApps),

    % Set emqx and emqx_conf to be used by all apps
    All = sets:from_list(EmqxApps),
    CommonDepsMap = #{
        emqx => All,
        emqx_conf => All
    },

    % Compute transitive closure: merge all maps and find all apps that transitively use each app
    UsedByTransitive = emqx_utils_deps:compute_transitive_closure(
        [CalleeToCallerMap, IncludedToIncludingMap, CommonDepsMap],
        EmqxApps
    ),

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
