#!/usr/bin/env escript
%% -*- erlang -*-
%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
%%
%% Check that every path returned by a minirest_api module's paths/0
%% has a corresponding entry in that module's scopes/0 map. The runtime
%% collector (emqx_mgmt_api_key_scopes) emits a warning at boot for
%% every gap; this script promotes that check into a fast post-compile
%% CI step so the regression is caught before CT runs (~5-10 min later).
%%
%% Mirrors the CT case t_init_cache_no_missing_path_warnings in
%% apps/emqx_management/test/emqx_mgmt_api_key_scopes_SUITE.erl --
%% if you change the rule, change both.
%%
%% Usage:
%%   ./scripts/check-api-scope-coverage.escript <lib_dir>
%%
%%   lib_dir: path to _build/<profile>/lib (contains app ebin dirs)

-mode(compile).

main([LibDir]) ->
    Globs = filelib:wildcard(LibDir ++ "/*/ebin"),
    case Globs of
        [] ->
            io:format(
                standard_error,
                "ERROR: no ebin dirs under ~s~n",
                [LibDir]
            ),
            maybe_hint_test_profile(LibDir),
            halt(1);
        _ ->
            ok
    end,
    code:add_pathsa(Globs),
    load_all_beams(Globs),
    Modules = find_api_modules(),
    Missing = lists:flatmap(fun check_module/1, lists:sort(Modules)),
    case Missing of
        [] ->
            io:format(
                "OK: ~p minirest_api module(s); every path in "
                "paths/0 covered by scopes/0~n",
                [length(Modules)]
            ),
            halt(0);
        _ ->
            io:format(
                standard_error,
                "ERROR: ~p path(s) missing from scopes/0 map "
                "(would warn at boot):~n",
                [length(Missing)]
            ),
            lists:foreach(
                fun({Mod, Path}) ->
                    io:format(standard_error, "  ~s -> ~s~n", [Mod, Path])
                end,
                Missing
            ),
            io:format(
                standard_error,
                "~nAdd the path to the module's scopes/0 map. Use "
                "?SCOPE_PUBLIC (apps/emqx_utils/include/"
                "emqx_api_key_scopes.hrl) for paths that are "
                "intentionally unscoped (pre-login entry points, "
                "static catalog endpoints).~n",
                []
            ),
            halt(1)
    end;
main(_) ->
    io:format(
        standard_error,
        "Usage: check-api-scope-coverage.escript <lib_dir>~n",
        []
    ),
    halt(1).

load_all_beams(EbinDirs) ->
    lists:foreach(
        fun(Dir) ->
            Beams = filelib:wildcard(Dir ++ "/*.beam"),
            lists:foreach(
                fun(B) ->
                    Mod = list_to_atom(filename:rootname(filename:basename(B))),
                    code:ensure_loaded(Mod)
                end,
                Beams
            )
        end,
        EbinDirs
    ).

find_api_modules() ->
    [M || {M, _} <- code:all_loaded(), is_api_module(M)].

is_api_module(Module) ->
    try
        Attrs = Module:module_info(attributes),
        Behaviours =
            proplists:get_value(behaviour, Attrs, []) ++
                proplists:get_value(behavior, Attrs, []),
        lists:member(minirest_api, Behaviours)
    catch
        _:_ -> false
    end.

%% Only the map form of scopes/0 has per-path entries that can be
%% missed. The binary form covers every path the module declares.
check_module(Mod) ->
    case {safe_paths(Mod), safe_scopes(Mod)} of
        {Paths, Map} when is_list(Paths), is_map(Map) ->
            [
                {Mod, path_to_bin(P)}
             || P <- Paths,
                not maps:is_key(path_to_bin(P), Map),
                not maps:is_key(P, Map)
            ];
        _ ->
            []
    end.

safe_paths(Mod) ->
    try
        Mod:paths()
    catch
        _:_ -> undefined
    end.

safe_scopes(Mod) ->
    try
        Mod:scopes()
    catch
        _:_ -> undefined
    end.

path_to_bin(P) when is_binary(P) -> ensure_slash(P);
path_to_bin(P) when is_list(P) ->
    ensure_slash(iolist_to_binary(P)).

ensure_slash(<<"/", _/binary>> = P) -> P;
ensure_slash(P) -> <<"/", P/binary>>.

%% On release-60 `make test-compile` puts beams under
%% _build/<profile>-test/lib (mix-driven build). If the caller passed
%% the release/non-test path by mistake, point them at the test path
%% if it exists -- saves a round trip when CI is misconfigured.
maybe_hint_test_profile(LibDir) ->
    case re:run(LibDir, "_build/([^/]+)/lib$", [{capture, all_but_first, list}]) of
        {match, [Profile]} ->
            case lists:suffix("-test", Profile) of
                true ->
                    ok;
                false ->
                    TestDir = "_build/" ++ Profile ++ "-test/lib",
                    case filelib:wildcard(TestDir ++ "/*/ebin") of
                        [] ->
                            ok;
                        _ ->
                            io:format(
                                standard_error,
                                "HINT: did you mean ~s? "
                                "(test builds use the ${PROFILE}-test profile)~n",
                                [TestDir]
                            )
                    end
            end;
        _ ->
            ok
    end.
