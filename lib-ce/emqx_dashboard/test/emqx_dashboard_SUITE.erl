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

-module(emqx_dashboard_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_ct_http,
        [ request_api/3
        , request_api/5
        , get_http_data/1
        ]).

-include_lib("eunit/include/eunit.hrl").

-include_lib("emqx/include/emqx.hrl").

-include("emqx_dashboard.hrl").

-define(CONTENT_TYPE, "application/x-www-form-urlencoded").

-define(HOST, "http://127.0.0.1:18083/").

-define(API_VERSION, "v4").

-define(BASE_PATH, "api").

-define(OVERVIEWS, ['alarms/activated', 'alarms/deactivated', banned, brokers, stats, metrics, listeners, clients, subscriptions, routes, plugins]).

all() ->
    [
     {group, overview},
     {group, admins},
     {group, rest},
     {group, cli}
     ].

groups() ->
    [
     {overview, [sequence], [t_overview]},
     {admins, [sequence], [t_admins_add_delete, t_admins_persist_default_password, t_default_password_persists_after_leaving_cluster]},
     {rest, [sequence], [t_rest_api]},
     {cli, [sequence], [t_cli, t_start_listener_failed_log]}
    ].

init_per_suite(Config) ->
    application:load(emqx_plugin_libs),
    ok = emqx_ct_helpers:start_apps([emqx_modules, emqx_management, emqx_dashboard]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_dashboard, emqx_management, emqx_modules]),
    ekka_mnesia:ensure_stopped().

init_per_testcase(Case, Config) ->
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    %% revert to default password
    emqx_dashboard_admin:force_change_password(<<"admin">>, <<"public">>),
    ?MODULE:Case({'end', Config}).

t_overview({init, Config}) -> Config;
t_overview({'end', _Config}) -> ok;
t_overview(_) ->
    [?assert(request_dashboard(get, api_path(erlang:atom_to_list(Overview)), auth_header_()))|| Overview <- ?OVERVIEWS].

t_admins_add_delete({init, Config}) -> Config;
t_admins_add_delete({'end', _Config}) -> ok;
t_admins_add_delete(_) ->
    ?assertEqual({error,<<"0 < Length =< 256">>},
        emqx_dashboard_admin:add_user(<<"">>, <<"password">>, <<"tag1">>)),

    ?assertEqual({error,<<"The password length: 8-64">>},
        emqx_dashboard_admin:add_user(<<"badusername">>, <<"">>, <<"tag1">>)),
    ?assertEqual({error,<<"The password length: 8-64">>},
        emqx_dashboard_admin:add_user(<<"badusername">>, <<"p">>, <<"tag1">>)),
    P65 = iolist_to_binary(lists:duplicate(65, <<"p">>)),
    ?assertEqual({error,<<"The password length: 8-64">>},
        emqx_dashboard_admin:add_user(<<"badusername">>, P65, <<"tag1">>)),
    P64 = iolist_to_binary([<<"1">> | lists:duplicate(63, <<"p">>)]),
    ?assertEqual(ok, emqx_dashboard_admin:add_user(<<"goodusername">>, P64, <<"tag1">>)),
    ok = emqx_dashboard_admin:remove_user(<<"goodusername">>),

    ok = emqx_dashboard_admin:add_user(<<"username1">>, <<"password1">>, <<"tag">>),
    ok = emqx_dashboard_admin:add_user(<<"username2">>, <<"password2">>, <<"tag1">>),
    ok = emqx_dashboard_admin:add_user(<<"1username1">>, <<"password1">>, <<"tag1">>),
    {error, _} = emqx_dashboard_admin:add_user(<<"u/sername1">>, <<"password1">>, <<"tag1">>),
    {error, _} = emqx_dashboard_admin:add_user(<<"/username1">>, <<"password1">>, <<"tag1">>),
    Admins = emqx_dashboard_admin:all_users(),
    ?assertEqual(4, length(Admins)),
    ok = emqx_dashboard_admin:remove_user(<<"username2">>),
    ok = emqx_dashboard_admin:remove_user(<<"1username1">>),
    Users = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(Users)),
    {error, _} = emqx_dashboard_admin:change_password(<<"username1">>, <<"password1">>, <<"password">>),
    ok = emqx_dashboard_admin:change_password(<<"username1">>, <<"password1">>, <<"password+">>),
    timer:sleep(10),
    ?assert(request_dashboard(get, api_path("brokers"), auth_header_("username1", "password+"))),

    ok = emqx_dashboard_admin:remove_user(<<"username1">>),
    ?assertNotEqual(true, request_dashboard(get, api_path("brokers"),
        auth_header_("username1", "password+"))).

t_admins_persist_default_password({init, Config}) -> Config;
t_admins_persist_default_password({'end', _Config}) -> ok;
t_admins_persist_default_password(_) ->
    emqx_dashboard_admin:change_password(<<"admin">>, <<"new_password">>),
    ct:sleep(100),
    [#mqtt_admin{password=Password, tags= <<"administrator">>}] = emqx_dashboard_admin:lookup_user(<<"admin">>),

    %% To ensure that state persists even if the process dies
    application:stop(emqx_dashboard),
    application:start(emqx_dashboard),

    ct:sleep(100),

    %% It gets restarted by the app automatically
    [#mqtt_admin{password=PasswordAfterRestart}] = emqx_dashboard_admin:lookup_user(<<"admin">>),
    ?assertEqual(Password, PasswordAfterRestart).

debug(Label, Slave) ->
    ct:print(
      "[~p]~nusers local ~p~nusers remote: ~p~nenv local: ~p~nenv remote: ~p",
      [
       Label,
       ets:tab2list(mqtt_admin),
       rpc:call(Slave, ets, tab2list, [mqtt_admin]),
       application:get_all_env(emqx_dashboard),
       rpc:call(Slave, application, get_all_env, [emqx_dashboard])
      ]).


t_default_password_persists_after_leaving_cluster({init, Config}) ->
    Slave = start_slave('test1', [emqx_modules, emqx_management, emqx_dashboard]),
    [{slave, Slave} | Config];
t_default_password_persists_after_leaving_cluster({'end', Config}) ->
    Slave = proplists:get_value(slave, Config),
    {ok, _} = stop_slave(Slave, [emqx_dashboard, emqx_management, emqx_modules]),
    ok;
t_default_password_persists_after_leaving_cluster(Config) ->
    Slave = proplists:get_value(slave, Config),
    [#mqtt_admin{password=InitialPassword}] = emqx_dashboard_admin:lookup_user(<<"admin">>),

    ct:print("Cluster status: ~p", [ekka_cluster:info()]),
    ct:print("Table nodes: ~p", [mnesia:table_info(mqtt_admin, active_replicas)]),


    %% To make sure that subscription is not lost during reconnection
    rpc:call(Slave, ekka, leave, []),
    ct:sleep(100), %% To ensure that leave gets processed
    rpc:call(Slave, ekka, join, [node()]),
    ct:sleep(100), %% To ensure that join gets processed

    ct:print("Cluster status: ~p", [ekka_cluster:info()]),
    ct:print("Table nodes: ~p", [mnesia:table_info(mqtt_admin, active_replicas)]),

    ct:print("Apps: ~p", [
                          rpc:call(Slave, application, which_applications, [])
                         ]),

    debug(0, Slave),

    emqx_dashboard_admin:change_password(<<"admin">>, <<"new_password">>),
    ct:sleep(100), %% To ensure that event gets processed

    debug(1, Slave),

    [#mqtt_admin{password=Password}] = rpc:call(Slave, emqx_dashboard_admin, lookup_user, [<<"admin">>]),
    ?assertNotEqual(InitialPassword, Password),

    rpc:call(Slave, ekka, leave, []),

    debug(2, Slave),

    rpc:call(Slave, application, stop, [emqx_dashboard]),

    debug(3, Slave),

    rpc:call(Slave, application, start, [emqx_dashboard]),

    debug(4, Slave),

    ?assertEqual(
       ok,
       rpc:call(Slave, emqx_dashboard_admin, check, [<<"admin">>, <<"new_password">>])),

    ?assertMatch(
       {error, _},
       rpc:call(Slave, emqx_dashboard_admin, check, [<<"admin">>, <<"password">>])),
    ok.

t_rest_api({init, Config}) -> Config;
t_rest_api({'end', _Config}) -> ok;
t_rest_api(_Config) ->
    {ok, Res0} = http_get("users"),
    Users = get_http_data(Res0),
    ct:pal("~p", [emqx_dashboard_admin:all_users()]),
    ?assert(lists:member(#{<<"username">> => <<"admin">>, <<"tags">> => <<"administrator">>},
        Users)),

    {ok, ErrorRes} = http_put("change_pwd/admin", [{<<"old_pwd">>, <<"public">>}, {<<"new_pwd">>, <<"simplepwd">>}]),
    ?assertMatch(#{<<"message">> := _}, json(ErrorRes)),

    AssertSuccess = fun({ok, Res}) ->
                        ?assertEqual(#{<<"code">> => 0}, json(Res))
                    end,
    [AssertSuccess(R)
     || R <- [ http_put("users/admin", #{<<"tags">> => <<"a_new_tag">>})
             , http_post("users", #{<<"username">> => <<"username1">>, <<"password">> => <<"passwd+123">>})
             , http_post("auth", #{<<"username">> => <<"username1">>, <<"password">> => <<"passwd+123">>})
             , http_delete("users/username1")
             , http_put("change_pwd/admin", #{<<"old_pwd">> => <<"public">>, <<"new_pwd">> => <<"newpwd+123">>})
             , http_post("auth", #{<<"username">> => <<"admin">>, <<"password">> => <<"newpwd+123">>})
             ]],
    ok.

t_auth_exhaustive_attack({init, Config}) -> Config;
t_auth_exhaustive_attack({'end', _Config}) -> ok;
t_auth_exhaustive_attack(_Config) ->
    {ok, Res0} = http_post("auth", #{<<"username">> => <<"invalid_login">>, <<"password">> => <<"newpwd">>}),
    {ok, Res1} = http_post("auth", #{<<"username">> => <<"admin">>, <<"password">> => <<"invalid_password">>}),
    ?assertEqual(Res0, Res1).

t_cli({init, Config}) -> Config;
t_cli({'end', _Config}) -> ok;
t_cli(_Config) ->
    [mnesia:dirty_delete({mqtt_admin, Admin}) ||  Admin <- mnesia:dirty_all_keys(mqtt_admin)],
    emqx_dashboard_cli:admins(["add", "username", "password1"]),
    [{mqtt_admin, <<"username">>, <<Salt:4/binary, Hash/binary>>, _}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(Hash, erlang:md5(<<Salt/binary, <<"password1">>/binary>>)),
    emqx_dashboard_cli:admins(["passwd", "username", "newpassword1"]),
    [{mqtt_admin, <<"username">>, <<Salt1:4/binary, Hash1/binary>>, _}] =
        emqx_dashboard_admin:lookup_user(<<"username">>),
    ?assertEqual(Hash1, erlang:md5(<<Salt1/binary, <<"newpassword1">>/binary>>)),
    emqx_dashboard_cli:admins(["del", "username"]),
    [] = emqx_dashboard_admin:lookup_user(<<"username">>),
    emqx_dashboard_cli:admins(["add", "admin1", "password+1"]),
    emqx_dashboard_cli:admins(["add", "admin2", "password+2"]),
    AdminList = emqx_dashboard_admin:all_users(),
    ?assertEqual(2, length(AdminList)).

t_start_listener_failed_log({init, Config}) ->
    _ = application:stop(emqx_dashboard),
    Config;
t_start_listener_failed_log({'end', _Config}) ->
    _ = application:start(emqx_dashboard),
    ok;
t_start_listener_failed_log(_Config) ->
    ct:capture_start(),
    Options = [{num_acceptors,4}, {max_connections,512}, {inet6,false}, {ipv6_v6only,false}],
    ?assertError(_, emqx_dashboard:start_listener({http, {"1.1.1.1", 8080}, Options})),
    ct:capture_stop(),
    I0 = ct:capture_get(),
    ?assertMatch({match, _}, re:run(iolist_to_binary(I0), "eaddrnotavail", [])),
    ok.

%%------------------------------------------------------------------------------
%% Internal functions
%%------------------------------------------------------------------------------

http_get(Path) ->
    request_api(get, api_path(Path), auth_header_()).

http_delete(Path) ->
    request_api(delete, api_path(Path), auth_header_()).

http_post(Path, Body) ->
    request_api(post, api_path(Path), [], auth_header_(), Body).

http_put(Path, Body) ->
    request_api(put, api_path(Path), [], auth_header_(), Body).

request_dashboard(Method, Url, Auth) ->
    Request = {Url, [Auth]},
    do_request_dashboard(Method, Request).
request_dashboard(Method, Url, QueryParams, Auth) ->
    Request = {Url ++ "?" ++ QueryParams, [Auth]},
    do_request_dashboard(Method, Request).
do_request_dashboard(Method, Request)->
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], []) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", 200, _}, _, _Return} }  ->
            true;
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

auth_header_() ->
    auth_header_("admin", "public").

auth_header_(User, Pass) ->
    Encoded = base64:encode_to_string(lists:append([User,":",Pass])),
    {"Authorization","Basic " ++ Encoded}.

api_path(Path) ->
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION, Path]).

json(Data) ->
    {ok, Jsx} = emqx_json:safe_decode(Data, [return_maps]), Jsx.

start_slave(Name, Apps) ->
    {ok, Node} = ct_slave:start(list_to_atom(atom_to_list(Name) ++ "@" ++ host()),
                                [{kill_if_fail, true},
                                 {monitor_master, true},
                                 {init_timeout, 10000},
                                 {startup_timeout, 10000},
                                 {erl_flags, ebin_path()}]),

    pong = net_adm:ping(Node),
    setup_node(Node, Apps),
    Node.

stop_slave(Node, Apps) ->
    [ok = Res || Res <- rpc:call(Node, emqx_ct_helpers, stop_apps, [Apps])],
    rpc:call(Node, ekka, leave, []),
    ct_slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"), Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

setup_node(Node, Apps) ->
    LoadedPlugins = emqx_ct_helpers:deps_path(
                      emqx,
                      filename:join(["test", "emqx_SUITE_data", "loaded_plugins"])),
    LoadedModules = emqx_ct_helpers:deps_path(
                         emqx,
                         filename:join(["test", "emqx_SUITE_data", "loaded_modules"])),
    EnvHandler =
        fun(emqx) ->
                application:set_env(emqx, plugins_loaded_file, LoadedPlugins),
                application:set_env(emqx, modules_loaded_file, LoadedModules),
                application:set_env(emqx, listeners, []),
                application:set_env(gen_rpc, port_discovery, manual),
                ok;
           (emqx_management) ->
                application:set_env(emqx_management, listeners, []),
                ok;
           (emqx_dashboard) ->
                Options = [{http,{"127.0.0.1",18184},
                            [{num_acceptors,4},
                            {max_connections,512},
                            {inet6,false},
                            {ipv6_v6only,false}]}],
                application:set_env(emqx_dashboard, listeners, Options),
                ok;
           (_) ->
                ok
        end,

    [ok = rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx | Apps]],
    ok = rpc:call(Node, emqx_ct_helpers, start_apps, [Apps, EnvHandler]),

    rpc:call(Node, ekka, join, [node()]),
    rpc:call(Node, application, stop, [emqx_dashboard]),
    rpc:call(Node, application, start, [emqx_dashboard]),
    ok.
