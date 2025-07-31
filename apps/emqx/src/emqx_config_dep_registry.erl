%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_config_dep_registry).

-moduledoc """
This module tracks dependencies between configuration importer modules.  This order is
used when importing configurations or deleting multiple configuration roots.  Application
register their dependencies when they start.
""".

-behaviour(gen_server).

%% API
-export([
    start_link/0,

    register_dependencies/1,
    sorted_importer_modules/0
]).

%% `gen_server' API
-export([
    init/1,
    terminate/2,
    handle_call/3,
    handle_cast/2,
    handle_info/2
]).

%%------------------------------------------------------------------------------
%% Type declarations
%%------------------------------------------------------------------------------

-define(g, g).
-define(root_key_mapping, root_key_mapping).

-define(DIGRAPH_PT_KEY, {?MODULE, ?g}).
-define(ROOT_KEY_MAPPING_PT_KEY, {?MODULE, ?root_key_mapping}).

%% Calls/casts/infos
-record(register_dependencies, {module}).

%%------------------------------------------------------------------------------
%% API
%%------------------------------------------------------------------------------

-spec start_link() -> gen_server:start_ret().
start_link() ->
    gen_server:start_link({local, ?MODULE}, ?MODULE, _Opts = #{}, []).

-spec register_dependencies(module()) -> ok | {error, {cycle, module(), module()}}.
register_dependencies(Module) ->
    gen_server:call(?MODULE, #register_dependencies{module = Module}, infinity).

-doc """
Returns the list of importer modules in the order they should be imported.
""".
-spec sorted_importer_modules() -> [module()].
sorted_importer_modules() ->
    AllModules = emqx_behaviour_utils:find_behaviours(emqx_config_backup),
    %% We assume here that the digraph owner process crashes rarely.
    G = persistent_term:get(?DIGRAPH_PT_KEY),
    do_sorted_importer_modules(G, AllModules).

%%------------------------------------------------------------------------------
%% `gen_server' API
%%------------------------------------------------------------------------------

init(_Opts) ->
    erlang:process_flag(trap_exit, true),
    G = new_digraph(),
    RootKeyMapping = initialize_dependency_graph(G),
    persistent_term:put(?DIGRAPH_PT_KEY, G),
    persistent_term:put(?ROOT_KEY_MAPPING_PT_KEY, RootKeyMapping),
    State = #{
        ?g => G,
        ?root_key_mapping => RootKeyMapping
    },
    {ok, State}.

terminate(_Reason, _State) ->
    persistent_term:erase(?DIGRAPH_PT_KEY),
    persistent_term:erase(?ROOT_KEY_MAPPING_PT_KEY),
    ok.

handle_call(#register_dependencies{module = Module}, _From, State0) ->
    {Reply, State} = handle_register_dependencies(Module, State0),
    {reply, Reply, State};
handle_call(Call, _From, State) ->
    {reply, {error, {unknown_call, Call}}, State}.

handle_cast(_Cast, State) ->
    {noreply, State}.

handle_info(_Info, State) ->
    {noreply, State}.

%%------------------------------------------------------------------------------
%% Internal fns
%%------------------------------------------------------------------------------

%% If this process restarts, we need to repopulate the dependency graph.
initialize_dependency_graph(G) ->
    lists:foldl(
        fun(Module, Acc) ->
            case emqx_config_backup:config_dependencies(Module) of
                undefined ->
                    Acc;
                #{
                    root_keys := RootKeys,
                    dependencies := Dependencies
                } ->
                    do_initialize_dependency_graph(G, Module, RootKeys, Dependencies, Acc)
            end
        end,
        #{},
        emqx_behaviour_utils:find_behaviours(emqx_config_backup)
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

handle_register_dependencies(Module, State0) ->
    case emqx_config_backup:config_dependencies(Module) of
        undefined ->
            {ok, State0};
        #{
            root_keys := RootKeys,
            dependencies := Dependencies
        } ->
            case validate_root_key_conflicts(Module, RootKeys, State0) of
                ok ->
                    do_handle_register_dependencies(Module, RootKeys, Dependencies, State0);
                Error ->
                    {Error, State0}
            end
    end.

validate_root_key_conflicts(Module, RootKeys, State) ->
    #{?root_key_mapping := RootKeyMapping} = State,
    emqx_utils:foldl_while(
        fun(RootKey, Ok) ->
            case RootKeyMapping of
                #{RootKey := OtherModule} when OtherModule /= Module ->
                    {halt, {error, {already_registered, RootKey, OtherModule}}};
                #{} ->
                    {cont, Ok}
            end
        end,
        ok,
        RootKeys
    ).

do_handle_register_dependencies(Module, RootKeys, Dependencies, State0) ->
    #{
        ?g := G,
        ?root_key_mapping := RootKeyMapping0
    } = State0,
    Res = emqx_utils:foldl_while(
        fun(Dep, Ok) ->
            case do_add_dependency(G, Module, Dep) of
                ok -> {cont, Ok};
                Error -> {halt, Error}
            end
        end,
        ok,
        Dependencies
    ),
    case Res of
        ok ->
            RKM = maps:from_keys(RootKeys, Module),
            RootKeyMapping = maps:merge(RootKeyMapping0, RKM),
            persistent_term:put(?ROOT_KEY_MAPPING_PT_KEY, RootKeyMapping),
            State = State0#{?root_key_mapping := RootKeyMapping},
            {ok, State};
        Error ->
            {Error, State0}
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
-include_lib("proper/include/proper.hrl").
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
