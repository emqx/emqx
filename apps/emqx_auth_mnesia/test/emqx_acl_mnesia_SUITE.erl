%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_acl_mnesia_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_auth_mnesia.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
                      , delete_default_app/0
                      , default_auth_header/0
                      ]).

-define(HOST, "http://127.0.0.1:8081/").
-define(API_VERSION, "v4").
-define(BASE_PATH, "api").

all() ->
    emqx_ct:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_modules, emqx_management, emqx_auth_mnesia], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    delete_default_app(),
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management, emqx_auth_mnesia]).

init_per_testcase(t_check_acl_as_clientid, Config) ->
    emqx:hook('client.check_acl', fun emqx_acl_mnesia:check_acl/5, [#{key_as => clientid}]),
    Config;

init_per_testcase(_, Config) ->
    emqx:hook('client.check_acl', fun emqx_acl_mnesia:check_acl/5, [#{key_as => username}]),
    Config.

end_per_testcase(_, Config) ->
    emqx:unhook('client.check_acl', fun emqx_acl_mnesia:check_acl/5),
    Config.

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(_App) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_management(_Config) ->
    clean_all_acls(),
    ?assertEqual("Acl with Mnesia", emqx_acl_mnesia:description()),
    ?assertEqual([], emqx_acl_mnesia_cli:all_acls()),

    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/%c">>, sub, allow),
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/+">>, pub, deny),
    ok = emqx_acl_mnesia_cli:add_acl({username, <<"test_username">>}, <<"topic/%u">>, sub, deny),
    ok = emqx_acl_mnesia_cli:add_acl({username, <<"test_username">>}, <<"topic/+">>, pub, allow),
    ok = emqx_acl_mnesia_cli:add_acl(all, <<"#">>, pubsub, deny),
    %% Sleeps below are needed to hide the race condition between
    %% mnesia and ets dirty select in check_acl, that make this test
    %% flaky
    timer:sleep(100),

    ?assertEqual(2, length(emqx_acl_mnesia_cli:lookup_acl({clientid, <<"test_clientid">>}))),
    ?assertEqual(2, length(emqx_acl_mnesia_cli:lookup_acl({username, <<"test_username">>}))),
    ?assertEqual(2, length(emqx_acl_mnesia_cli:lookup_acl(all))),
    ?assertEqual(6, length(emqx_acl_mnesia_cli:all_acls())),

    User1 = #{zone => external, clientid => <<"test_clientid">>},
    User2 = #{zone => external, clientid => <<"no_exist">>, username => <<"test_username">>},
    User3 = #{zone => external, clientid => <<"test_clientid">>, username => <<"test_username">>},
    allow = emqx_access_control:check_acl(User1, subscribe, <<"topic/test_clientid">>),
    deny  = emqx_access_control:check_acl(User1, publish,   <<"topic/A">>),
    deny  = emqx_access_control:check_acl(User2, subscribe, <<"topic/test_username">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"topic/A">>),
    allow = emqx_access_control:check_acl(User3, subscribe, <<"topic/test_clientid">>),
    deny  = emqx_access_control:check_acl(User3, subscribe, <<"topic/test_username">>),
    deny  = emqx_access_control:check_acl(User3, publish,   <<"topic/A">>),
    deny  = emqx_access_control:check_acl(User3, subscribe, <<"topic/A/B">>),
    deny  = emqx_access_control:check_acl(User3, publish,   <<"topic/A/B">>),

    %% Test merging of pubsub capability:
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pubsub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pub, allow),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pubsub, allow),
    timer:sleep(100),
    allow = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, sub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    allow = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),
    ok = emqx_acl_mnesia_cli:add_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>, pub, deny),
    timer:sleep(100),
    deny  = emqx_access_control:check_acl(User1, subscribe,   <<"topic/mix">>),
    deny  = emqx_access_control:check_acl(User1, publish,     <<"topic/mix">>),

    ok = emqx_acl_mnesia_cli:remove_acl({clientid, <<"test_clientid">>}, <<"topic/%c">>),
    ok = emqx_acl_mnesia_cli:remove_acl({clientid, <<"test_clientid">>}, <<"topic/+">>),
    ok = emqx_acl_mnesia_cli:remove_acl({clientid, <<"test_clientid">>}, <<"topic/mix">>),
    ok = emqx_acl_mnesia_cli:remove_acl({username, <<"test_username">>}, <<"topic/%u">>),
    ok = emqx_acl_mnesia_cli:remove_acl({username, <<"test_username">>}, <<"topic/+">>),
    ok = emqx_acl_mnesia_cli:remove_acl(all, <<"#">>),
    timer:sleep(100),

    ?assertEqual([], emqx_acl_mnesia_cli:all_acls()).

t_acl_cli(_Config) ->
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end),

    clean_all_acls(),

    ?assertEqual(0, length(emqx_acl_mnesia_cli:cli(["list"]))),

    emqx_acl_mnesia_cli:cli(["add", "clientid", "test_clientid", "topic/A", "pub", "deny"]),
    emqx_acl_mnesia_cli:cli(["add", "clientid", "test_clientid", "topic/A", "pub", "allow"]),
    R1 = emqx_ctl:format("Acl(clientid = ~p topic = ~p action = ~p access = ~p)~n",
                         [<<"test_clientid">>, <<"topic/A">>, pub, allow]),
    ?assertEqual([R1], emqx_acl_mnesia_cli:cli(["show", "clientid", "test_clientid"])),
    ?assertEqual([R1], emqx_acl_mnesia_cli:cli(["list", "clientid"])),

    emqx_acl_mnesia_cli:cli(["add", "username", "test_username", "topic/B", "sub", "deny"]),
    R2 = emqx_ctl:format("Acl(username = ~p topic = ~p action = ~p access = ~p)~n",
                         [<<"test_username">>, <<"topic/B">>, sub, deny]),
    ?assertEqual([R2], emqx_acl_mnesia_cli:cli(["show", "username", "test_username"])),
    ?assertEqual([R2], emqx_acl_mnesia_cli:cli(["list", "username"])),

    emqx_acl_mnesia_cli:cli(["add", "_all", "#", "pub", "allow"]),
    emqx_acl_mnesia_cli:cli(["add", "_all", "#", "pubsub", "deny"]),
    ?assertMatch(["",
                  "Acl($all topic = <<\"#\">> action = pub access = deny)",
                  "Acl($all topic = <<\"#\">> action = sub access = deny)"],
                 lists:sort(string:split(emqx_acl_mnesia_cli:cli(["list", "_all"]), "\n", all))
                ),
    ?assertEqual(4, length(emqx_acl_mnesia_cli:cli(["list"]))),

    emqx_acl_mnesia_cli:cli(["del", "clientid", "test_clientid", "topic/A"]),
    emqx_acl_mnesia_cli:cli(["del", "username", "test_username", "topic/B"]),
    emqx_acl_mnesia_cli:cli(["del", "_all", "#"]),
    ?assertEqual(0, length(emqx_acl_mnesia_cli:cli(["list"]))),

    meck:unload(emqx_ctl).

t_rest_api(_Config) ->
    clean_all_acls(),

    Params1 = [#{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"clientid">> => <<"test_clientid">>,
                 <<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }],
    {ok, _} = request_http_rest_add([], Params1),
    {ok, Re1} = request_http_rest_list(["clientid", "test_clientid"]),
    ?assertMatch(4, length(get_http_data(Re1))),
    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["clientid", "test_clientid", "topic", "topic/C"]),
    {ok, Res1} = request_http_rest_list(["clientid"]),
    ?assertMatch([], get_http_data(Res1)),

    Params2 = [#{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"username">> => <<"test_username">>,
                 <<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }],
    {ok, _} = request_http_rest_add([], Params2),
    {ok, Re2} = request_http_rest_list(["username", "test_username"]),
    ?assertMatch(4, length(get_http_data(Re2))),
    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["username", "test_username", "topic", "topic/C"]),
    {ok, Res2} = request_http_rest_list(["username"]),
    ?assertMatch([], get_http_data(Res2)),

    Params3 = [#{<<"topic">> => <<"topic/A">>,
                 <<"action">> => <<"pub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"topic">> => <<"topic/B">>,
                 <<"action">> => <<"sub">>,
                 <<"access">> => <<"allow">>
                },
               #{<<"topic">> => <<"topic/C">>,
                 <<"action">> => <<"pubsub">>,
                 <<"access">> => <<"deny">>
                }],
    {ok, _} = request_http_rest_add([], Params3),
    {ok, Re3} = request_http_rest_list(["$all"]),
    ?assertMatch(4, length(get_http_data(Re3))),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/A"]),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/B"]),
    {ok, _} = request_http_rest_delete(["$all", "topic", "topic/C"]),
    {ok, Res3} = request_http_rest_list(["$all"]),
    ?assertMatch([], get_http_data(Res3)).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

clean_all_acls() ->
    [ mnesia:dirty_delete({emqx_acl, Login})
      || Login <- mnesia:dirty_all_keys(emqx_acl)].

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_lookup(Path) ->
    request_api(get, uri(Path), default_auth_header()).

request_http_rest_add(Path, Params) ->
    request_api(post, uri(Path), [], default_auth_header(), Params).

request_http_rest_delete(Path) ->
    request_api(delete, uri(Path), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, "acl"| NParts]).

b2l(B) -> binary_to_list(emqx_http_lib:uri_encode(iolist_to_binary(B))).
