%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_utils_deps_tests).

-include_lib("eunit/include/eunit.hrl").

%% Test compute_transitive_closure/2
compute_transitive_closure_test_() ->
    [
        {"empty list returns empty map", fun() ->
            Result = emqx_utils_deps:compute_transitive_closure([], []),
            ?assertEqual(#{}, Result)
        end},
        {"empty map returns empty map", fun() ->
            Result = emqx_utils_deps:compute_transitive_closure([#{}], []),
            ?assertEqual(#{}, Result)
        end},
        {"single app with no users", fun() ->
            UsedByMap = #{app1 => sets:new()},
            AllApps = [app1],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            ?assertEqual(#{app1 => sets:new()}, Result)
        end},
        {"direct dependency only", fun() ->
            UsedByMap = #{
                app1 => sets:from_list([app2]),
                app2 => sets:new()
            },
            AllApps = [app1, app2],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            ?assertEqual(#{app1 => sets:from_list([app2]), app2 => sets:new()}, Result)
        end},
        {"transitive dependency", fun() ->
            % app1 uses app2, app2 uses app3
            % So app1 transitively uses app3
            UsedByMap = #{
                app1 => sets:from_list([app2]),
                app2 => sets:from_list([app3]),
                app3 => sets:new()
            },
            AllApps = [app1, app2, app3],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            % app1 should transitively use both app2 and app3
            ?assertEqual(sets:from_list([app2, app3]), maps:get(app1, Result)),
            ?assertEqual(sets:from_list([app3]), maps:get(app2, Result)),
            ?assertEqual(sets:new(), maps:get(app3, Result))
        end},
        {"circular dependency", fun() ->
            % app1 uses app2, app2 uses app1 (circular)
            UsedByMap = #{
                app1 => sets:from_list([app2]),
                app2 => sets:from_list([app1])
            },
            AllApps = [app1, app2],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            % Both should transitively use each other
            ?assertEqual(sets:from_list([app1, app2]), maps:get(app1, Result)),
            ?assertEqual(sets:from_list([app1, app2]), maps:get(app2, Result))
        end},
        {"complex transitive chain", fun() ->
            % app1 -> app2 -> app3 -> app4
            UsedByMap = #{
                app1 => sets:from_list([app2]),
                app2 => sets:from_list([app3]),
                app3 => sets:from_list([app4]),
                app4 => sets:new()
            },
            AllApps = [app1, app2, app3, app4],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            % app1 should transitively use app2, app3, app4
            ?assertEqual(sets:from_list([app2, app3, app4]), maps:get(app1, Result)),
            % app2 should transitively use app3, app4
            ?assertEqual(sets:from_list([app3, app4]), maps:get(app2, Result)),
            % app3 should transitively use app4
            ?assertEqual(sets:from_list([app4]), maps:get(app3, Result)),
            % app4 has no users
            ?assertEqual(sets:new(), maps:get(app4, Result))
        end},
        {"multiple users of same app", fun() ->
            % app2 and app3 both use app1
            UsedByMap = #{
                app1 => sets:new(),
                app2 => sets:from_list([app1]),
                app3 => sets:from_list([app1])
            },
            AllApps = [app1, app2, app3],
            Result = emqx_utils_deps:compute_transitive_closure([UsedByMap], AllApps),
            ?assertEqual(sets:new(), maps:get(app1, Result)),
            ?assertEqual(sets:from_list([app1]), maps:get(app2, Result)),
            ?assertEqual(sets:from_list([app1]), maps:get(app3, Result))
        end},
        {"merges multiple maps", fun() ->
            % Map1: app1 uses app2
            % Map2: app2 uses app3
            % After merge: app1 uses app2, app2 uses app3
            Map1 = #{app1 => sets:from_list([app2])},
            Map2 = #{app2 => sets:from_list([app3])},
            AllApps = [app1, app2, app3],
            Result = emqx_utils_deps:compute_transitive_closure([Map1, Map2], AllApps),
            % app1 should transitively use app2 and app3
            ?assertEqual(sets:from_list([app2, app3]), maps:get(app1, Result)),
            % app2 should transitively use app3
            ?assertEqual(sets:from_list([app3]), maps:get(app2, Result)),
            % app3 has no users
            ?assertEqual(sets:new(), maps:get(app3, Result))
        end},
        {"merges overlapping maps", fun() ->
            % Map1: app1 uses app2
            % Map2: app1 uses app3 (overlapping key)
            % After merge: app1 uses both app2 and app3
            Map1 = #{app1 => sets:from_list([app2])},
            Map2 = #{app1 => sets:from_list([app3])},
            AllApps = [app1, app2, app3],
            Result = emqx_utils_deps:compute_transitive_closure([Map1, Map2], AllApps),
            % app1 should use both app2 and app3
            ?assertEqual(sets:from_list([app2, app3]), maps:get(app1, Result))
        end}
    ].

%% Test get_include_dependents/2 with mocked beam_lib
get_include_dependents_test_() ->
    {setup, fun setup_include_meck/0, fun cleanup_include_meck/1, [
        {"no include_lib directives", fun() ->
            LibDir = "/mock/lib",
            AppNames = [emqx_test1, emqx_test2],
            % Mock beam_lib to return no abstract code
            meck:expect(filelib, is_dir, fun
                ("/mock/lib/emqx_test1/ebin") -> true;
                ("/mock/lib/emqx_test2/ebin") -> true;
                (_) -> false
            end),
            meck:expect(filelib, wildcard, fun(_) -> [] end),
            Result = emqx_utils_deps:get_include_dependents(LibDir, AppNames),
            % Should return map with empty sets for all apps
            ?assert(is_map(Result)),
            lists:foreach(
                fun(App) ->
                    Set = maps:get(App, Result),
                    ?assertEqual(sets:new(), Set)
                end,
                AppNames
            )
        end},
        {"single include_lib directive", fun() ->
            LibDir = "/mock/lib",
            AppNames = [emqx_test1, emqx_test2],
            BeamFile1 = "/mock/lib/emqx_test1/ebin/test1.beam",
            % Mock beam_lib to return abstract code with include_lib
            meck:expect(filelib, is_dir, fun
                ("/mock/lib/emqx_test1/ebin") -> true;
                ("/mock/lib/emqx_test2/ebin") -> true;
                (_) -> false
            end),
            meck:expect(filelib, wildcard, fun(Pattern) ->
                case Pattern of
                    "/mock/lib/emqx_test1/ebin/*.beam" -> [BeamFile1];
                    _ -> []
                end
            end),
            meck:expect(beam_lib, chunks, fun(BeamFile, Chunks) ->
                case {BeamFile, Chunks} of
                    {BeamFile1, [abstract_code]} ->
                        {ok,
                            {test1, [
                                {abstract_code,
                                    {raw_abstract_v1, [
                                        {attribute, 1, include_lib, "emqx_test2/include/test.hrl"}
                                    ]}}
                            ]}};
                    _ ->
                        {error, no_abstract_code}
                end
            end),
            Result = emqx_utils_deps:get_include_dependents(LibDir, AppNames),
            % emqx_test2 should have emqx_test1 in its includers set
            App2Set = maps:get(emqx_test2, Result, sets:new()),
            ?assert(sets:is_element(emqx_test1, App2Set))
        end},
        {"multiple include_lib directives", fun() ->
            LibDir = "/mock/lib",
            AppNames = [emqx_test1, emqx_test2],
            BeamFile1 = "/mock/lib/emqx_test1/ebin/test1.beam",
            BeamFile2 = "/mock/lib/emqx_test2/ebin/test2.beam",
            meck:expect(filelib, is_dir, fun
                ("/mock/lib/emqx_test1/ebin") -> true;
                ("/mock/lib/emqx_test2/ebin") -> true;
                (_) -> false
            end),
            meck:expect(filelib, wildcard, fun(Pattern) ->
                case Pattern of
                    "/mock/lib/emqx_test1/ebin/*.beam" -> [BeamFile1];
                    "/mock/lib/emqx_test2/ebin/*.beam" -> [BeamFile2];
                    _ -> []
                end
            end),
            meck:expect(beam_lib, chunks, fun(BeamFile, Chunks) ->
                case {BeamFile, Chunks} of
                    {BeamFile1, [abstract_code]} ->
                        {ok,
                            {test1, [
                                {abstract_code,
                                    {raw_abstract_v1, [
                                        {attribute, 1, include_lib, "emqx_test2/include/test.hrl"}
                                    ]}}
                            ]}};
                    {BeamFile2, [abstract_code]} ->
                        {ok,
                            {test2, [
                                {abstract_code,
                                    {raw_abstract_v1, [
                                        {attribute, 1, include_lib, "emqx_test1/include/test.hrl"}
                                    ]}}
                            ]}};
                    _ ->
                        {error, no_abstract_code}
                end
            end),
            Result = emqx_utils_deps:get_include_dependents(LibDir, AppNames),
            % emqx_test2 should have emqx_test1 in its includers set
            App2Set = maps:get(emqx_test2, Result, sets:new()),
            ?assert(sets:is_element(emqx_test1, App2Set)),
            % emqx_test1 should have emqx_test2 in its includers set
            App1Set = maps:get(emqx_test1, Result, sets:new()),
            ?assert(sets:is_element(emqx_test2, App1Set))
        end},
        {"non-existent app is filtered out", fun() ->
            LibDir = "/mock/lib",
            AppNames = [emqx_test1],
            BeamFile1 = "/mock/lib/emqx_test1/ebin/test1.beam",
            meck:expect(filelib, is_dir, fun
                ("/mock/lib/emqx_test1/ebin") -> true;
                (_) -> false
            end),
            meck:expect(filelib, wildcard, fun(Pattern) ->
                case Pattern of
                    "/mock/lib/emqx_test1/ebin/*.beam" -> [BeamFile1];
                    _ -> []
                end
            end),
            meck:expect(beam_lib, chunks, fun(BeamFile, Chunks) ->
                case {BeamFile, Chunks} of
                    {BeamFile1, [abstract_code]} ->
                        {ok,
                            {test1, [
                                {abstract_code,
                                    {raw_abstract_v1, [
                                        {attribute, 1, include_lib, "other_app/include/test.hrl"}
                                    ]}}
                            ]}};
                    _ ->
                        {error, no_abstract_code}
                end
            end),
            Result = emqx_utils_deps:get_include_dependents(LibDir, AppNames),
            % other_app should not be in the result (not in AppNames)
            ?assertNot(maps:is_key(other_app, Result))
        end}
    ]}.

setup_include_meck() ->
    % Mock beam_lib and filelib modules
    meck:new(beam_lib, [unstick, passthrough]),
    meck:new(filelib, [unstick, passthrough]),
    ok.

cleanup_include_meck(_) ->
    meck:unload([beam_lib, filelib]).

%% Test get_call_dependents/2 with mocked xref
get_call_dependents_test_() ->
    {setup, fun setup_meck/0, fun cleanup_meck/1, [
        {"non-existent directory raises error", fun() ->
            meck:expect(filelib, is_dir, fun
                ("/nonexistent/path") -> false;
                (_) -> true
            end),
            ModToAppMap = #{module1 => app1},
            ?assertError(
                {directory_not_found, _},
                emqx_utils_deps:get_call_dependents("/nonexistent/path", ModToAppMap)
            )
        end},
        {"empty ModToAppMap returns empty map", fun() ->
            % Mock xref to return empty remote calls list
            % Note: get_all_remote_calls requires at least one call, so we return a dummy call
            setup_xref_mocks([{{mod1, func1, 1}, {mod2, func2, 1}}]),
            ModToAppMap = #{},
            Result = emqx_utils_deps:get_call_dependents("/mock/lib", ModToAppMap),
            % Should return a map (empty in this case since mod1/mod2 not in ModToAppMap)
            ?assert(is_map(Result)),
            ?assertEqual(#{}, Result)
        end},
        {"maps remote calls to app dependencies", fun() ->
            % Mock xref to return some remote calls
            setup_xref_mocks([
                {{mod1, func1, 1}, {mod2, func2, 1}},
                {{mod1, func1, 2}, {mod3, func3, 1}}
            ]),
            % ModToAppMap: mod1 -> app1, mod2 -> app2, mod3 -> app3
            ModToAppMap = #{
                mod1 => app1,
                mod2 => app2,
                mod3 => app3
            },
            Result = emqx_utils_deps:get_call_dependents("/mock/lib", ModToAppMap),
            % app2 should have app1 in its callers set (app1 calls app2)
            App2Set = maps:get(app2, Result, sets:new()),
            ?assert(sets:is_element(app1, App2Set)),
            % app3 should have app1 in its callers set (app1 calls app3)
            App3Set = maps:get(app3, Result, sets:new()),
            ?assert(sets:is_element(app1, App3Set))
        end},
        {"skips self-dependencies", fun() ->
            setup_xref_mocks([
                {{mod1, func1, 1}, {mod2, func2, 1}},
                % Self-call
                {{mod1, func1, 2}, {mod1, func3, 1}}
            ]),
            ModToAppMap = #{
                mod1 => app1,
                mod2 => app2
            },
            Result = emqx_utils_deps:get_call_dependents("/mock/lib", ModToAppMap),
            % app1 should not have itself in its callers set
            App1Set = maps:get(app1, Result, sets:new()),
            ?assertNot(sets:is_element(app1, App1Set))
        end}
    ]}.

setup_meck() ->
    % Mock xref and filelib modules
    meck:new(xref, [unstick, passthrough]),
    meck:new(filelib, [unstick, passthrough]),
    ok.

cleanup_meck(_) ->
    meck:unload([xref, filelib]).

%% Helper function to setup xref mocks with default expectations
%% RemoteCalls is the list of remote calls to return from xref:q/2
setup_xref_mocks(RemoteCalls) ->
    meck:expect(xref, start, fun(_) -> {ok, mock_xref_server} end),
    meck:expect(xref, set_default, fun(_, _) -> ok end),
    meck:expect(xref, add_release, fun(_, _) -> {ok, []} end),
    meck:expect(xref, q, fun(_, _) -> {ok, RemoteCalls} end),
    meck:expect(xref, stop, fun(_) -> ok end).
