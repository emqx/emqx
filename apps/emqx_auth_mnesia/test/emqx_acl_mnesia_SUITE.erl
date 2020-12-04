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

-compile(export_all).

-include("emqx_auth_mnesia.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_ct_http, [ request_api/3
                      , request_api/5
                      , get_http_data/1
                      , create_default_app/0
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
    emqx_ct_helpers:start_apps([emqx_management, emqx_auth_mnesia], fun set_special_configs/1),
    create_default_app(),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_auth_mnesia]).

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
    ?assertEqual([], emqx_auth_mnesia_cli:all_acls()),

    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/A">>, <<"sub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/B">>, <<"pub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/C">>, <<"pubsub">>, true),
    
    ?assertEqual([{emqx_acl,<<"test_username">>,<<"Topic/A">>,<<"sub">>, true},
                  {emqx_acl,<<"test_username">>,<<"Topic/B">>,<<"pub">>, true},
                  {emqx_acl,<<"test_username">>,<<"Topic/C">>,<<"pubsub">>, true}],emqx_auth_mnesia_cli:lookup_acl(<<"test_username">>)),
    ok = emqx_auth_mnesia_cli:remove_acl(<<"test_username">>, <<"Topic/A">>),
    ?assertEqual([{emqx_acl,<<"test_username">>,<<"Topic/B">>,<<"pub">>, true},
                  {emqx_acl,<<"test_username">>,<<"Topic/C">>,<<"pubsub">>, true}], emqx_auth_mnesia_cli:lookup_acl(<<"test_username">>)),


    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/A">>, <<"sub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/B">>, <<"pub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/C">>, <<"pubsub">>, true),

    ?assertEqual([{emqx_acl,<<"$all">>,<<"Topic/A">>,<<"sub">>, true},
                  {emqx_acl,<<"$all">>,<<"Topic/B">>,<<"pub">>, true},
                  {emqx_acl,<<"$all">>,<<"Topic/C">>,<<"pubsub">>, true}],emqx_auth_mnesia_cli:lookup_acl(<<"$all">>)),
    ok = emqx_auth_mnesia_cli:remove_acl(<<"$all">>, <<"Topic/A">>),
    ?assertEqual([{emqx_acl,<<"$all">>,<<"Topic/B">>,<<"pub">>, true},
                  {emqx_acl,<<"$all">>,<<"Topic/C">>,<<"pubsub">>, true}], emqx_auth_mnesia_cli:lookup_acl(<<"$all">>)).

t_check_acl_as_clientid(_) ->
    clean_all_acls(),
    emqx_modules:load_module(emqx_mod_acl_internal, false),

    User1 = #{zone => external, clientid => <<"test_clientid">>},
    User2 = #{zone => external, clientid => <<"no_exist">>},

    ok = emqx_auth_mnesia_cli:add_acl(<<"test_clientid">>, <<"#">>, <<"sub">>, false),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_clientid">>, <<"+/A">>, <<"pub">>, false),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_clientid">>, <<"Topic/A/B">>, <<"pubsub">>, true),

    deny  = emqx_access_control:check_acl(User1, subscribe, <<"Any">>),
    deny  = emqx_access_control:check_acl(User1, publish, <<"Any/A">>),
    allow  = emqx_access_control:check_acl(User1, publish, <<"Any/C">>),
    allow = emqx_access_control:check_acl(User1, publish, <<"Topic/A/B">>),

    allow = emqx_access_control:check_acl(User2, subscribe, <<"Topic/C">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"Topic/D">>).

t_check_acl_as_username(_Config) ->
    clean_all_acls(),
    emqx_modules:load_module(emqx_mod_acl_internal, false),
    
    User1 = #{zone => external, username => <<"test_username">>},
    User2 = #{zone => external, username => <<"no_exist">>},

    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/A">>, <<"sub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/B">>, <<"pub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/A/B">>, <<"pubsub">>, false),
    allow = emqx_access_control:check_acl(User1, subscribe, <<"Topic/A">>),
    allow = emqx_access_control:check_acl(User1, subscribe, <<"Topic/B">>),
    deny  = emqx_access_control:check_acl(User1, subscribe, <<"Topic/A/B">>),
    allow = emqx_access_control:check_acl(User1, publish,   <<"Topic/A">>),
    allow = emqx_access_control:check_acl(User1, publish,   <<"Topic/B">>),
    deny  = emqx_access_control:check_acl(User1, publish,   <<"Topic/A/B">>),

    allow = emqx_access_control:check_acl(User2, subscribe, <<"Topic/C">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"Topic/D">>).

t_check_acl_as_all(_) ->
    clean_all_acls(),
    emqx_modules:load_module(emqx_mod_acl_internal, false),

    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/A">>, <<"sub">>, false),
    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/B">>, <<"pub">>, false),
    ok = emqx_auth_mnesia_cli:add_acl(<<"$all">>, <<"Topic/A/B">>, <<"pubsub">>, true),

    User1 = #{zone => external, username => <<"test_username">>},
    User2 = #{zone => external, username => <<"no_exist">>},

    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/A">>, <<"sub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/B">>, <<"pub">>, true),
    ok = emqx_auth_mnesia_cli:add_acl(<<"test_username">>, <<"Topic/A/B">>, <<"pubsub">>, false),

    allow = emqx_access_control:check_acl(User1, subscribe, <<"Topic/A">>),
    allow = emqx_access_control:check_acl(User1, subscribe, <<"Topic/B">>),
    deny  = emqx_access_control:check_acl(User1, subscribe, <<"Topic/A/B">>),
    allow = emqx_access_control:check_acl(User1, publish,   <<"Topic/A">>),
    allow = emqx_access_control:check_acl(User1, publish,   <<"Topic/B">>),
    deny  = emqx_access_control:check_acl(User1, publish,   <<"Topic/A/B">>),

    deny  = emqx_access_control:check_acl(User2, subscribe, <<"Topic/A">>),
    deny  = emqx_access_control:check_acl(User2, publish,   <<"Topic/B">>),
    allow = emqx_access_control:check_acl(User2, subscribe, <<"Topic/A/B">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"Topic/A/B">>),
    allow = emqx_access_control:check_acl(User2, subscribe, <<"Topic/C">>),
    allow = emqx_access_control:check_acl(User2, publish,   <<"Topic/D">>).

t_rest_api(_Config) ->
    clean_all_acls(),

    {ok, Result} = request_http_rest_list(),
    [] = get_http_data(Result),

    Params = #{<<"login">> => <<"test_username">>, <<"topic">> => <<"Topic/A">>, <<"action">> => <<"pubsub">>, <<"allow">> => true},
    {ok, _} = request_http_rest_add(Params),
    {ok, Result1} = request_http_rest_lookup(<<"test_username">>),
    #{<<"login">> := <<"test_username">>, <<"topic">> := <<"Topic/A">>, <<"action">> := <<"pubsub">>, <<"allow">> := true} = get_http_data(Result1),

    Params1 = [
                #{<<"login">> => <<"$all">>, <<"topic">> => <<"+/A">>, <<"action">> => <<"pub">>, <<"allow">> => true},
                #{<<"login">> => <<"test_username">>, <<"topic">> => <<"+/A">>, <<"action">> => <<"pub">>, <<"allow">> => true},
                #{<<"login">> => <<"test_username/1">>, <<"topic">> => <<"#">>, <<"action">> => <<"sub">>, <<"allow">> => true},
                #{<<"login">> => <<"test_username/2">>, <<"topic">> => <<"+/A">>, <<"action">> => <<"error_format">>, <<"allow">> => true}
                ],
    {ok, Result2} = request_http_rest_add(Params1),
    #{
        <<"$all">> := <<"ok">>,
        <<"test_username">> := <<"ok">>,
        <<"test_username/1">> := <<"ok">>,
        <<"test_username/2">> := <<"{error,action}">>
        } = get_http_data(Result2),

    {ok, Result3} = request_http_rest_lookup(<<"test_username">>),
    [#{<<"login">> := <<"test_username">>, <<"topic">> := <<"+/A">>, <<"action">> := <<"pub">>, <<"allow">> := true},
     #{<<"login">> := <<"test_username">>, <<"topic">> := <<"Topic/A">>, <<"action">> := <<"pubsub">>, <<"allow">> := true}]
     = get_http_data(Result3),

    {ok, Result4} = request_http_rest_lookup(<<"$all">>),
    #{<<"login">> := <<"$all">>, <<"topic">> := <<"+/A">>, <<"action">> := <<"pub">>, <<"allow">> := true}
      = get_http_data(Result4),

    {ok, _} = request_http_rest_delete(<<"$all">>, <<"+/A">>),
    {ok, _} = request_http_rest_delete(<<"test_username">>, <<"+/A">>),
    {ok, _} = request_http_rest_delete(<<"test_username">>, <<"Topic/A">>),
    {ok, _} = request_http_rest_delete(<<"test_username/1">>, <<"#">>),
    {ok, Result5} = request_http_rest_list(),
    [] = get_http_data(Result5).


t_run_command(_) ->
    clean_all_acls(),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-acl", "add", "TestUser", "Topic/A", "sub", true])),
    ?assertEqual([{emqx_acl,<<"TestUser">>,<<"Topic/A">>,<<"sub">>, true}],emqx_auth_mnesia_cli:lookup_acl(<<"TestUser">>)),

    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-acl", "del", "TestUser", "Topic/A"])),
    ?assertEqual([],emqx_auth_mnesia_cli:lookup_acl(<<"TestUser">>)),

    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-acl", "show", "TestUser"])),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-acl", "list"])),
    ?assertEqual(ok, emqx_ctl:run_command(["mqtt-acl"])).

t_cli(_) ->
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end),

    clean_all_acls(),
    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:acl_cli(["add", "TestUser", "Topic/A", "sub", true]), "ok")),
    ?assertMatch(["Acl(login = <<\"TestUser\">> topic = <<\"Topic/A\">> action = <<\"sub\">> allow = true)\n"], emqx_auth_mnesia_cli:acl_cli(["show", "TestUser"])),
    ?assertMatch(["Acl(login = <<\"TestUser\">>)\n"], emqx_auth_mnesia_cli:acl_cli(["list"])),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:acl_cli(["del", "TestUser", "Topic/A"]), "ok")),
    ?assertMatch([], emqx_auth_mnesia_cli:acl_cli(["show", "TestUser"])),
    ?assertMatch([], emqx_auth_mnesia_cli:acl_cli(["list"])),

    ?assertMatch({match, _}, re:run(emqx_auth_mnesia_cli:acl_cli([]), "mqtt-acl")),

    meck:unload(emqx_ctl).

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

clean_all_acls() ->
    [ mnesia:dirty_delete({emqx_acl, Login})
      || Login <- mnesia:dirty_all_keys(emqx_acl)].

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------

request_http_rest_list() ->
    request_api(get, uri(), default_auth_header()).

request_http_rest_lookup(Login) ->
    request_api(get, uri([Login]), default_auth_header()).

request_http_rest_add(Params) ->
    request_api(post, uri(), [], default_auth_header(), Params).

request_http_rest_delete(Login, Topic) ->
    request_api(delete, uri([Login, Topic]), default_auth_header()).

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [b2l(E) || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, "mqtt_acl"| NParts]).

%% @private
b2l(B) when is_binary(B) ->
    http_uri:encode(binary_to_list(B));
b2l(L) when is_list(L) ->
    http_uri:encode(L).
