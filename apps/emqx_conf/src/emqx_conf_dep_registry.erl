%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_conf_dep_registry).

-moduledoc """
This module tracks dependencies between configuration importer modules.  This order is
used when importing configurations or deleting multiple configuration roots.
""".

%% API
-export([
    sorted_importer_modules/0,
    sorted_root_keys/0
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-doc """
Returns the list of importer modules in the order they should be imported.
""".
-spec sorted_importer_modules() -> [module()].
sorted_importer_modules() ->
    {_DepMapping, SortedMods} = sorted_importer_modules_and_root_key_mapping(),
    SortedMods.

-spec sorted_root_keys() -> [binary()].
sorted_root_keys() ->
    {DepMapping, SortedMods} = sorted_importer_modules_and_root_key_mapping(),
    Map = maps:fold(
        fun(RK, Mod, Acc) ->
            maps:update_with(
                Mod,
                fun(RKs) -> [RK | RKs] end,
                [RK],
                Acc
            )
        end,
        #{},
        DepMapping
    ),
    lists:flatmap(
        fun(Mod) ->
            case Map of
                #{Mod := RootKeys} ->
                    RootKeys;
                #{} ->
                    []
            end
        end,
        SortedMods
    ).
%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

sorted_importer_modules_and_root_key_mapping() ->
    AllModules = emqx_behaviour_utils:find_behaviours(emqx_config_backup),
    G = new_digraph(),
    DepMapping = initialize_dependency_graph(G),
    {DepMapping, do_sorted_importer_modules(G, AllModules)}.

read_importer_dependencies() ->
    AppLibDir = code:lib_dir(emqx_conf),
    Filepath = filename:join([AppLibDir, "priv", "importer_dependencies.eterm"]),
    {ok, [ImporterMapping]} = file:consult(Filepath),
    ImporterMapping.

%% If this process restarts, we need to repopulate the dependency graph.
initialize_dependency_graph(G) ->
    maps:fold(
        fun(Module, #{root_keys := RootKeys, dependencies := Dependencies}, Acc) ->
            do_initialize_dependency_graph(G, Module, RootKeys, Dependencies, Acc)
        end,
        #{},
        read_importer_dependencies()
    ).

do_initialize_dependency_graph(G, Module, RootKeys, Dependencies, Acc) ->
    %% Assert
    lists:foreach(
        fun(RootKey) -> {false, RootKey} = {is_map_key(RootKey, Acc), RootKey} end,
        RootKeys
    ),
    lists:foreach(
        fun(Dep) -> ok = do_add_dependency(G, Module, Dep) end,
        Dependencies
    ),
    RKM = maps:from_keys(RootKeys, Module),
    maps:merge(Acc, RKM).

has_edge(G, Module, Dependency) ->
    case digraph:get_path(G, Dependency, Module) of
        false ->
            false;
        _ ->
            true
    end.

do_add_dependency(G, Module, Dependency) ->
    digraph:add_vertex(G, Module),
    digraph:add_vertex(G, Dependency),
    case has_edge(G, Module, Dependency) of
        true ->
            ok;
        false ->
            case digraph:add_edge(G, Dependency, Module) of
                {error, _} ->
                    {error, {cycle, Dependency, Module}};
                _ ->
                    ok
            end
    end.

do_sorted_importer_modules(G, AllModules) ->
    SortedDeps = digraph_utils:topsort(G),
    OtherModules = AllModules -- SortedDeps,
    SortedDeps ++ OtherModules.

new_digraph() ->
    digraph:new([acyclic, protected]).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------
-ifdef(TEST).
-include_lib("proper/include/proper_common.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(withDigraph(NAME, BIND, BODY),
    {setup, local, fun new_digraph/0, fun digraph:delete/1, fun(BIND) ->
        {NAME, ?_test(BODY)}
    end}
).

sort_test_() ->
    [
        ?withDigraph("simple test 0", G, begin
            AllModules = [a, b, c, d],
            ?assertEqual(AllModules, do_sorted_importer_modules(G, AllModules))
        end),
        ?withDigraph("simple test 1", G, begin
            AllModules = [a, b, c, d],
            ok = do_add_dependency(G, b, c),
            ?assertEqual([c, b, a, d], do_sorted_importer_modules(G, AllModules))
        end),
        ?withDigraph("simple test 2", G, begin
            AllModules = [a, b, c, d],
            ok = do_add_dependency(G, b, c),
            ok = do_add_dependency(G, c, a),
            ?assertEqual([a, c, b, d], do_sorted_importer_modules(G, AllModules))
        end),
        ?withDigraph("simple test 3", G, begin
            AllModules = [a, b, c, d],
            ok = do_add_dependency(G, b, c),
            ok = do_add_dependency(G, b, c),
            ok = do_add_dependency(G, b, c),
            ?assertEqual([c, b, a, d], do_sorted_importer_modules(G, AllModules))
        end),
        ?withDigraph("no self cycle", G, begin
            ?assertMatch({error, {cycle, a, a}}, do_add_dependency(G, a, a))
        end),
        ?withDigraph("no cycles", G, begin
            ok = do_add_dependency(G, a, b),
            ok = do_add_dependency(G, b, c),
            ?assertMatch({error, {cycle, a, c}}, do_add_dependency(G, c, a))
        end)
    ].

-endif.
