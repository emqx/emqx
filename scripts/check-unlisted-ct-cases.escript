#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Check that every t_* test case a Common Test suite exports is
%% reachable from all/0 -- directly, or through a group listed in
%% groups/0. A case that is listed nowhere never runs, and nothing
%% reports that: the compiler sees an exported function, elvis sees
%% nothing, and CT never looks at the case at all. Such a case rots
%% silently until the code it covers has been removed under it.
%%
%% The check evaluates the suite instead of reading its source. Many
%% suites build all/0 and groups/0 from helpers -- for example
%% emqx_common_test_helpers:all/1, emqx_bridge_v2_testlib:
%% local_and_cluster_groups/3, emqx_authz_test_lib:table_groups/2 --
%% that collect case names through module_info/1. A source scan cannot
%% tell a listed case from an unlisted one in those suites.
%%
%% A t_*/1 function that is not a test case, or a case disabled on
%% purpose, must not carry the t_ prefix. Rename it -- for example to
%% disabled__t_foo -- and write the reason next to it.
%%
%% Usage:
%%   ./scripts/check-unlisted-ct-cases.escript <lib_dir>
%%
%%   lib_dir: path to _build/<profile>-test/lib (contains app dirs)

-mode(compile).

main([LibDir]) ->
    CodeDirs = code_dirs(LibDir),
    code:add_pathsa(CodeDirs),
    case find_suites(CodeDirs) of
        [] ->
            io:format(standard_error, "ERROR: no CT suites found under ~s~n", [LibDir]),
            halt(1);
        Suites ->
            {Unlisted, Errors} = lists:foldl(fun check_suite/2, {[], []}, Suites),
            report(length(Suites), lists:reverse(Unlisted), lists:reverse(Errors))
    end;
main(_) ->
    io:format(
        standard_error,
        "Usage: check-unlisted-ct-cases.escript <lib_dir>~n",
        []
    ),
    halt(1).

%% Suite beams land under lib/<app>/test with the mix build and under
%% lib/<app>/ebin with rebar3. Take both, and put every app dir on the
%% code path so the helper modules all/0 and groups/0 call are found.
code_dirs(LibDir) ->
    filelib:wildcard(LibDir ++ "/*/ebin") ++
        filelib:wildcard(LibDir ++ "/*/test").

find_suites(CodeDirs) ->
    Mods = [
        list_to_atom(filename:rootname(filename:basename(Beam)))
     || Dir <- CodeDirs, Beam <- filelib:wildcard(Dir ++ "/*_SUITE.beam")
    ],
    lists:usort(Mods).

check_suite(Mod, {Unlisted, Errors}) ->
    case code:ensure_loaded(Mod) of
        {module, Mod} ->
            check_loaded_suite(Mod, {Unlisted, Errors});
        {error, Reason} ->
            {Unlisted, [{Mod, load, Reason} | Errors]}
    end.

check_loaded_suite(Mod, {Unlisted, Errors}) ->
    case {eval(Mod, all), eval_groups(Mod)} of
        {{error, E}, _} ->
            {Unlisted, [{Mod, all, E} | Errors]};
        {_, {error, E}} ->
            {Unlisted, [{Mod, groups, E} | Errors]};
        %% all() -> {skip, Reason} skips the whole suite on purpose.
        {{ok, {skip, _}}, _} ->
            {Unlisted, Errors};
        {{ok, All}, {ok, Groups}} when is_list(All), is_list(Groups) ->
            Reachable = reachable(Mod, All, Groups),
            Missing = [C || C <- candidates(Mod), not lists:member(C, Reachable)],
            {[{Mod, C} || C <- Missing] ++ Unlisted, Errors};
        {{ok, All}, {ok, Groups}} ->
            {Unlisted, [{Mod, shape, {All, Groups}} | Errors]}
    end.

eval_groups(Mod) ->
    case erlang:function_exported(Mod, groups, 0) of
        true -> eval(Mod, groups);
        false -> {ok, []}
    end.

eval(Mod, Fun) ->
    try
        {ok, Mod:Fun()}
    catch
        Class:Reason:Stack -> {error, {Class, Reason, Stack}}
    end.

%%--------------------------------------------------------------------
%% Reachability
%%--------------------------------------------------------------------

%% Every case CT can reach from all/0. Group definitions are indexed
%% by name first -- including definitions nested inside another group,
%% which CT also finds by name -- then all/0 is expanded against that
%% index. A name may be defined more than once; CT runs every matching
%% definition, so the index keeps them all.
reachable(Mod, All, Groups) ->
    Defs = index_groups(Groups, #{}),
    lists:usort(expand(Mod, All, Defs, [], [])).

index_groups(Items, Defs) ->
    lists:foldl(fun index_group/2, Defs, Items).

index_group({Name, Props, Tests}, Defs) when
    is_atom(Name), is_list(Props), is_list(Tests)
->
    index_groups(Tests, add_def(Name, Tests, Defs));
index_group({Name, Tests}, Defs) when is_atom(Name), is_list(Tests) ->
    index_groups(Tests, add_def(Name, Tests, Defs));
index_group(_Other, Defs) ->
    Defs.

add_def(Name, Tests, Defs) ->
    maps:update_with(Name, fun(Ts) -> Ts ++ [Tests] end, [Tests], Defs).

%% Path is the group names between all/0 and Items. It guards against
%% a group that reaches itself, and it must not carry over between
%% siblings: the same group name legitimately appears under several
%% parents -- with_batch under both sync and async, say -- and pruning
%% the second one would report its cases as unlisted.
expand(Mod, Items, Defs, Path, Acc) ->
    lists:foldl(
        fun(Item, Acc1) -> expand_item(Mod, Item, Defs, Path, Acc1) end,
        Acc,
        Items
    ).

%% all/0 may nest lists, and CT flattens them.
expand_item(Mod, Items, Defs, Path, Acc) when is_list(Items) ->
    expand(Mod, Items, Defs, Path, Acc);
expand_item(_Mod, Case, _Defs, _Path, Acc) when is_atom(Case) ->
    [Case | Acc];
expand_item(Mod, {group, Name}, Defs, Path, Acc) when is_atom(Name) ->
    expand_group(Mod, Name, Defs, Path, Acc);
expand_item(Mod, {group, Name, Props}, Defs, Path, Acc) when
    is_atom(Name), is_list(Props)
->
    expand_group(Mod, Name, Defs, Path, Acc);
expand_item(Mod, {group, Name, Props, _SubProps}, Defs, Path, Acc) when
    is_atom(Name), is_list(Props)
->
    expand_group(Mod, Name, Defs, Path, Acc);
expand_item(_Mod, {testcase, Case, Props}, _Defs, _Path, Acc) when
    is_atom(Case), is_list(Props)
->
    [Case | Acc];
%% Group definition inline in a list of tests.
expand_item(Mod, {Name, Props, Tests}, Defs, Path, Acc) when
    is_atom(Name), is_list(Props), is_list(Tests)
->
    expand_nested(Mod, Name, Tests, Defs, Path, Acc);
expand_item(Mod, {Name, Tests}, Defs, Path, Acc) when
    is_atom(Name), is_list(Tests)
->
    expand_nested(Mod, Name, Tests, Defs, Path, Acc);
%% A case borrowed from another suite. Only ours counts here; the
%% other suite is checked on its own.
expand_item(Mod, {Mod, Case}, _Defs, _Path, Acc) when is_atom(Case) ->
    [Case | Acc];
expand_item(_Mod, _Other, _Defs, _Path, Acc) ->
    Acc.

expand_group(Mod, Name, Defs, Path, Acc) ->
    case lists:member(Name, Path) of
        true ->
            Acc;
        false ->
            Bodies = maps:get(Name, Defs, []),
            expand(Mod, lists:append(Bodies), Defs, [Name | Path], Acc)
    end.

expand_nested(Mod, Name, Tests, Defs, Path, Acc) ->
    case lists:member(Name, Path) of
        true -> Acc;
        false -> expand(Mod, Tests, Defs, [Name | Path], Acc)
    end.

%%--------------------------------------------------------------------
%% Candidates
%%--------------------------------------------------------------------

candidates(Mod) ->
    [
        F
     || {F, 1} <- Mod:module_info(exports),
        lists:prefix("t_", atom_to_list(F))
    ].

%%--------------------------------------------------------------------
%% Report
%%--------------------------------------------------------------------

report(Count, [], []) ->
    io:format(
        "OK: ~p CT suite(s); every t_* case reachable from all/0~n",
        [Count]
    ),
    halt(0);
report(_Count, Unlisted, Errors) ->
    print_errors(Errors),
    print_unlisted(Unlisted),
    halt(1).

print_errors([]) ->
    ok;
print_errors(Errors) ->
    io:format(
        standard_error,
        "ERROR: ~p CT suite(s) could not be evaluated:~n",
        [length(Errors)]
    ),
    lists:foreach(
        fun({Mod, What, Reason}) ->
            io:format(standard_error, "  ~s ~p/0: ~p~n", [Mod, What, Reason])
        end,
        Errors
    ),
    io:format(
        standard_error,
        "~nall/0 and groups/0 must be callable without a running node. "
        "Move setup that needs one into init_per_suite/1.~n~n",
        []
    ).

print_unlisted([]) ->
    ok;
print_unlisted(Unlisted) ->
    io:format(
        standard_error,
        "ERROR: ~p test case(s) reachable from neither all/0 nor "
        "any group in groups/0, so they never run:~n",
        [length(Unlisted)]
    ),
    lists:foreach(
        fun({Mod, Case}) ->
            io:format(standard_error, "  ~s: ~s~n", [Mod, Case])
        end,
        Unlisted
    ),
    io:format(
        standard_error,
        "~nAdd each case to all/0 or to a group that all/0 runs. "
        "Delete it if what it tested is gone. Rename a function that "
        "is not a test case, or a case disabled on purpose, so it does "
        "not start with t_ (for example disabled__t_foo).~n",
        []
    ).
