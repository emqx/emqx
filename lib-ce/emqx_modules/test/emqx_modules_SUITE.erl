%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_modules_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:8081/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").

all() -> emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_management, emqx_modules], fun set_special_cfg/1),
    emqx_ct_http:create_default_app(),
    Config.

set_special_cfg(_) ->
    application:set_env(emqx, modules_loaded_file, emqx_ct_helpers:deps_path(emqx, "test/emqx_SUITE_data/loaded_modules")),
    ok.

end_per_suite(_Config) ->
    emqx_ct_http:delete_default_app(),
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management]).

init_per_testcase(t_ensure_default_loaded_modules_file, Config) ->
    {ok, LoadedModulesFilepath} = application:get_env(emqx, modules_loaded_file),
    ok = application:stop(emqx_modules),
    TmpFilepath = filename:join(["/", "tmp", "loaded_modules_tmp"]),
    case file:delete(TmpFilepath) of
        ok -> ok;
        {error, enoent} -> ok
    end,
    application:set_env(emqx, modules_loaded_file, TmpFilepath),
    [ {loaded_modules_filepath, LoadedModulesFilepath}
    , {tmp_filepath, TmpFilepath}
    | Config];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_ensure_default_loaded_modules_file, Config) ->
    LoadedModulesFilepath = ?config(loaded_modules_filepath, Config),
    TmpFilepath = ?config(tmp_filepath, Config),
    file:delete(TmpFilepath),
    ok = application:stop(emqx_modules),
    application:set_env(emqx, modules_loaded_file, LoadedModulesFilepath),
    ok = application:start(emqx_modules),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

t_load(_) ->
    ?assertEqual(ok, emqx_modules:unload()),
    ?assertEqual(ok, emqx_modules:load()),
    ?assertEqual({error, not_found}, emqx_modules:load(not_existed_module)),
    ?assertEqual({error, not_started}, emqx_modules:unload(emqx_mod_rewrite)),
    ?assertEqual(ignore, emqx_modules:reload(emqx_mod_rewrite)),
    ?assertEqual(ok, emqx_modules:reload(emqx_mod_acl_internal)).

t_ensure_default_loaded_modules_file(_Config) ->
    ok = application:start(emqx_modules),
    ?assertEqual(
       [ {emqx_mod_acl_internal,true}
       , {emqx_mod_delayed,false}
       , {emqx_mod_presence,true}
       , {emqx_mod_rewrite,false}
       , {emqx_mod_slow_subs,false}
       , {emqx_mod_subscription,false}
       , {emqx_mod_topic_metrics,false}
       , {emqx_mod_trace,false}
       ],
       lists:sort(emqx_modules:list())),
    ok.

t_list(_) ->
    ?assertMatch([{_, _} | _ ], emqx_modules:list()).

t_modules_api(_) ->
    emqx_modules:load_module(emqx_mod_presence, false),
    timer:sleep(50),
    {ok, Modules1} = request_api(get, api_path(["modules"]), auth_header_()),
    [Modules11] = filter(get(<<"data">>, Modules1), <<"node">>, atom_to_binary(node(), utf8)),
    [Module1] = filter(maps:get(<<"modules">>, Modules11), <<"name">>, <<"emqx_mod_presence">>),
    ?assertEqual(<<"emqx_mod_presence">>, maps:get(<<"name">>, Module1)),
    ?assertEqual(true, maps:get(<<"active">>, Module1)),

    {ok, _} = request_api(put,
                          api_path(["modules",
                                    atom_to_list(emqx_mod_presence),
                                    "unload"]),
                          auth_header_()),
    {ok, Error1} = request_api(put,
                               api_path(["modules",
                                         atom_to_list(emqx_mod_presence),
                                         "unload"]),
                               auth_header_()),
    ?assertEqual(<<"not_started">>, get(<<"message">>, Error1)),
    {ok, Modules2} = request_api(get,
                                 api_path(["nodes", atom_to_list(node()), "modules"]),
                                 auth_header_()),
    [Module2] = filter(get(<<"data">>, Modules2), <<"name">>, <<"emqx_mod_presence">>),
    ?assertEqual(<<"emqx_mod_presence">>, maps:get(<<"name">>, Module2)),
    ?assertEqual(false, maps:get(<<"active">>, Module2)),

    {ok, _} = request_api(put,
                          api_path(["nodes",
                                    atom_to_list(node()),
                                    "modules",
                                    atom_to_list(emqx_mod_presence),
                                    "load"]),
                          auth_header_()),
    {ok, Modules3} = request_api(get,
                                 api_path(["nodes", atom_to_list(node()), "modules"]),
                                 auth_header_()),
    [Module3] = filter(get(<<"data">>, Modules3), <<"name">>, <<"emqx_mod_presence">>),
    ?assertEqual(<<"emqx_mod_presence">>, maps:get(<<"name">>, Module3)),
    ?assertEqual(true, maps:get(<<"active">>, Module3)),

    {ok, _} = request_api(put,
                          api_path(["nodes",
                                    atom_to_list(node()),
                                    "modules",
                                    atom_to_list(emqx_mod_presence),
                                    "unload"]),
                          auth_header_()),
    {ok, Error2} = request_api(put,
                               api_path(["nodes",
                                         atom_to_list(node()),
                                         "modules",
                                         atom_to_list(emqx_mod_presence),
                                         "unload"]),
                               auth_header_()),
    ?assertEqual(<<"not_started">>, get(<<"message">>, Error2)),
    emqx_modules:unload(emqx_mod_presence).


t_modules_cmd(_) ->
    mock_print(),
    meck:new(emqx_modules, [non_strict, passthrough]),
    meck:expect(emqx_modules, load, fun(_) -> ok end),
    meck:expect(emqx_modules, unload, fun(_) -> ok end),
    meck:expect(emqx_modules, reload, fun(_) -> ok end),
    ?assertEqual(emqx_modules:cli(["list"]), ok),
    ?assertEqual(emqx_modules:cli(["load", "emqx_mod_presence"]),
                 "Module emqx_mod_presence loaded successfully.\n"),
    ?assertEqual(emqx_modules:cli(["unload", "emqx_mod_presence"]),
                 "Module emqx_mod_presence unloaded successfully.\n"),
    unmock_print().

%% For: https://github.com/emqx/emqx/issues/4511
t_join_cluster(_) ->
    %% Started by emqx application
    {error, {already_started, emqx_modules}} = application:start(emqx_modules),
    %% After clustered
    emqx:shutdown(),
    emqx:reboot(),
    {error,{already_started,emqx_modules}} = application:start(emqx_modules),
    %% After emqx reboot, we should not interfere with other tests
    _ = end_per_suite([]),
    _ = init_per_suite([]),
    ok.

mock_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end).

unmock_print() ->
    meck:unload(emqx_ctl).

get(Key, ResponseBody) ->
   maps:get(Key, jiffy:decode(list_to_binary(ResponseBody), [return_maps])).

request_api(Method, Url, Auth) ->
    request_api(Method, Url, [], Auth, []).

request_api(Method, Url, QueryParams, Auth) ->
    request_api(Method, Url, QueryParams, Auth, []).

request_api(Method, Url, QueryParams, Auth, []) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth]});
request_api(Method, Url, QueryParams, Auth, Body) ->
    NewUrl = case QueryParams of
                 "" -> Url;
                 _ -> Url ++ "?" ++ QueryParams
             end,
    do_request_api(Method, {NewUrl, [Auth], "application/json", emqx_json:encode(Body)}).

do_request_api(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _, Return} }
            when Code =:= 200 orelse Code =:= 201 ->
            {ok, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    AppId = <<"admin">>,
    AppSecret = <<"public">>,
    auth_header_(binary_to_list(AppId), binary_to_list(AppSecret)).

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Parts)->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION] ++ Parts).

filter(List, Key, Value) ->
    lists:filter(fun(Item) ->
        maps:get(Key, Item) == Value
    end, List).
