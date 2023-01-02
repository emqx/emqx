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

-module(emqx_auth_http_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(APP, emqx_auth_http).

-define(USER(ClientId, Username, Protocol, Peerhost, Zone),
        #{clientid => ClientId, username => Username, protocol => Protocol,
          peerhost => Peerhost, zone => Zone}).

-define(USER(ClientId, Username, Protocol, Peerhost, Zone, Mountpoint),
        #{clientid => ClientId, username => Username, protocol => Protocol,
          peerhost => Peerhost, zone => Zone, mountpoint => Mountpoint}).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [
     {group, http_inet},
     {group, http_inet6},
     {group, https_inet},
     {group, https_inet6},
     pub_sub_no_acl,
     no_hook_if_config_unset
    ].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{Name, Cases} || Name <- [http_inet, http_inet6, https_inet, https_inet6]].

init_per_group(GrpName, Cfg) ->
    [Scheme, Inet] = [list_to_atom(X) || X <- string:tokens(atom_to_list(GrpName), "_")],
    ok = setup(Scheme, Inet),
    Cfg.

end_per_group(_GrpName, _Cfg) ->
    teardown().

init_per_testcase(pub_sub_no_acl, Cfg) ->
    Scheme = http,
    Inet = inet,
    http_auth_server:start(Scheme, Inet),
    Fun = fun(App) -> set_special_configs(App, Scheme, Inet, no_acl) end,
    emqx_ct_helpers:start_apps([emqx_auth_http], Fun),
    ?assert(is_hooked('client.authenticate')),
    ?assertNot(is_hooked('client.check_acl')),
    Cfg;
init_per_testcase(no_hook_if_config_unset, Cfg) ->
    setup(http, inet),
    Cfg;
init_per_testcase(_, Cfg) ->
    %% init per group
    Cfg.

end_per_testcase(pub_sub_no_acl, _Cfg) ->
    teardown();
end_per_testcase(no_hook_if_config_unset, _Cfg) ->
    teardown();
end_per_testcase(_, _Cfg) ->
    %% teardown per group
    ok.

setup(Scheme, Inet) ->
    http_auth_server:start(Scheme, Inet),
    Fun = fun(App) -> set_special_configs(App, Scheme, Inet, normal) end,
    emqx_ct_helpers:start_apps([emqx_auth_http], Fun),
    ?assert(is_hooked('client.authenticate')),
    ?assert(is_hooked('client.check_acl')).

teardown() ->
    http_auth_server:stop(),
    application:stop(emqx_auth_http),
    ?assertNot(is_hooked('client.authenticate')),
    ?assertNot(is_hooked('client.check_acl')),
    emqx_ct_helpers:stop_apps([emqx]).

set_special_configs(emqx, _Scheme, _Inet, _AuthConfig) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(emqx_auth_http, Scheme, Inet, PluginConfig) ->
    [application:unset_env(?APP, Par) || Par <- [acl_req, auth_req]],
    ServerAddr = http_server(Scheme, Inet),

    AuthReq = #{method => get,
                url => ServerAddr ++ "/mqtt/auth",
                headers => [{"content-type", "application/json"}],
                params => [{"clientid", "%c"}, {"username", "%u"}, {"password", "%P"}]},
    SuperReq = #{method => post,
                 url => ServerAddr ++ "/mqtt/superuser",
                 headers => [{"content-type", "application/json"}],
                 params => [{"clientid", "%c"}, {"username", "%u"}]},
    AclReq = #{method => post,
               url => ServerAddr ++ "/mqtt/acl",
               headers => [{"content-type", "application/json"}],
               params => [{"access", "%A"}, {"username", "%u"}, {"clientid", "%c"}, {"ipaddr", "%a"}, {"topic", "%t"}, {"mountpoint", "%m"}]},

    Scheme =:= https andalso set_https_client_opts(),

    application:set_env(emqx_auth_http, auth_req, maps:to_list(AuthReq)),
    application:set_env(emqx_auth_http, super_req, maps:to_list(SuperReq)),
    case PluginConfig of
        normal -> ok = application:set_env(emqx_auth_http, acl_req, maps:to_list(AclReq));
        no_acl -> ok
    end.

%% @private
set_https_client_opts() ->
    SSLOpt = emqx_ct_helpers:client_ssl_twoway(),
    application:set_env(emqx_auth_http, cacertfile, proplists:get_value(cacertfile, SSLOpt, undefined)),
    application:set_env(emqx_auth_http, certfile, proplists:get_value(certfile, SSLOpt, undefined)),
    application:set_env(emqx_auth_http, keyfile, proplists:get_value(keyfile, SSLOpt, undefined)),
    application:set_env(emqx_auth_http, verify, true),
    application:set_env(emqx_auth_http, server_name_indication, "disable").

%% @private
http_server(http, inet) -> "http://127.0.0.1:8991"; % ipv4
http_server(http, inet6) -> "http://localhost:8991"; % test hostname resolution
http_server(https, inet) -> "https://localhost:8991"; % test hostname resolution
http_server(https, inet6) -> "https://[::1]:8991". % ipv6

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_check_acl(Cfg) when is_list(Cfg) ->
    SuperUser = ?USER(<<"superclient">>, <<"superuser">>, mqtt, {127,0,0,1}, external),
    deny = emqx_access_control:check_acl(SuperUser, subscribe, <<"users/testuser/1">>),
    deny = emqx_access_control:check_acl(SuperUser, publish, <<"anytopic">>),

    User1 = ?USER(<<"client1">>, <<"testuser">>, mqtt, {127,0,0,1}, external),
    UnIpUser1 = ?USER(<<"client1">>, <<"testuser">>, mqtt, {192,168,0,4}, external),
    UnClientIdUser1 = ?USER(<<"unkonwc">>, <<"testuser">>, mqtt, {127,0,0,1}, external),
    UnnameUser1= ?USER(<<"client1">>, <<"unuser">>, mqtt, {127,0,0,1}, external),
    allow = emqx_access_control:check_acl(User1, subscribe, <<"users/testuser/1">>),
    deny = emqx_access_control:check_acl(User1, publish, <<"users/testuser/1">>),
    deny = emqx_access_control:check_acl(UnIpUser1, subscribe, <<"users/testuser/1">>),
    deny = emqx_access_control:check_acl(UnClientIdUser1, subscribe, <<"users/testuser/1">>),
    deny  = emqx_access_control:check_acl(UnnameUser1, subscribe, <<"$SYS/testuser/1">>),

    User2 = ?USER(<<"client2">>, <<"xyz">>, mqtt, {127,0,0,1}, external),
    UserC = ?USER(<<"client2">>, <<"xyz">>, mqtt, {192,168,1,3}, external),
    allow = emqx_access_control:check_acl(UserC, publish, <<"a/b/c">>),
    deny = emqx_access_control:check_acl(User2, publish, <<"a/b/c">>),
    deny  = emqx_access_control:check_acl(User2, subscribe, <<"$SYS/testuser/1">>).

t_check_auth(Cfg) when is_list(Cfg) ->
    User1 = ?USER(<<"client1">>, <<"testuser1">>, mqtt, {127,0,0,1}, external, undefined),
    User2 = ?USER(<<"client2">>, <<"testuser2">>, mqtt, {127,0,0,1}, exteneral, undefined),
    User3 = ?USER(<<"client3">>, undefined, mqtt, {127,0,0,1}, exteneral, undefined),

    {ok, #{auth_result := success,
           anonymous := false,
           is_superuser := false}} = emqx_access_control:authenticate(User1#{password => <<"pass1">>}),
    {error, bad_username_or_password} = emqx_access_control:authenticate(User1#{password => <<"pass">>}),
    {error, bad_username_or_password} = emqx_access_control:authenticate(User1#{password => <<>>}),

    {ok, #{is_superuser := false}} = emqx_access_control:authenticate(User2#{password => <<"pass2">>}),
    {error, bad_username_or_password} = emqx_access_control:authenticate(User2#{password => <<>>}),
    {error, bad_username_or_password} = emqx_access_control:authenticate(User2#{password => <<"errorpwd">>}),

    {error, bad_username_or_password} = emqx_access_control:authenticate(User3#{password => <<"pwd">>}).

pub_sub_no_acl(Cfg) when is_list(Cfg) ->
    {ok, T1} = emqtt:start_link([{host, "localhost"},
                                 {clientid, <<"client1">>},
                                 {username, <<"testuser1">>},
                                 {password, <<"pass1">>}]),
    {ok, _} = emqtt:connect(T1),
    emqtt:publish(T1, <<"topic">>, <<"body">>, [{qos, 0}, {retain, true}]),
    timer:sleep(1000),
    {ok, T2} = emqtt:start_link([{host, "localhost"},
                                 {clientid, <<"client2">>},
                                 {username, <<"testuser2">>},
                                 {password, <<"pass2">>}]),
    {ok, _} = emqtt:connect(T2),
    emqtt:subscribe(T2, <<"topic">>),
    receive
        {publish, _Topic, Payload} ->
            ?assertEqual(<<"body">>, Payload)
        after 1000 -> false end,
    emqtt:disconnect(T1),
    emqtt:disconnect(T2).

t_pub_sub(Cfg) when is_list(Cfg) ->
    {ok, T1} = emqtt:start_link([{host, "localhost"},
                                 {clientid, <<"client1">>},
                                 {username, <<"testuser1">>},
                                 {password, <<"pass1">>}]),
    {ok, _} = emqtt:connect(T1),
    emqtt:publish(T1, <<"topic">>, <<"body">>, [{qos, 0}, {retain, true}]),
    timer:sleep(1000),
    {ok, T2} = emqtt:start_link([{host, "localhost"},
                                 {clientid, <<"client2">>},
                                 {username, <<"testuser2">>},
                                 {password, <<"pass2">>}]),
    {ok, _} = emqtt:connect(T2),
    emqtt:subscribe(T2, <<"topic">>),
    receive
        {publish, _Topic, Payload} ->
            ?assertEqual(<<"body">>, Payload)
        after 1000 -> false end,
    emqtt:disconnect(T1),
    emqtt:disconnect(T2).

no_hook_if_config_unset(Cfg) when is_list(Cfg) ->
    ?assert(is_hooked('client.authenticate')),
    ?assert(is_hooked('client.check_acl')),
    application:stop(?APP),
    [application:unset_env(?APP, Par) || Par <- [acl_req, auth_req]],
    application:start(?APP),
    ?assertEqual([], emqx_hooks:lookup('client.authenticate')),
    ?assertNot(is_hooked('client.authenticate')),
    ?assertNot(is_hooked('client.check_acl')).

is_hooked(HookName) ->
    Callbacks = emqx_hooks:lookup(HookName),
    F = fun(Callback) ->
                case emqx_hooks:callback_action(Callback) of
                    {emqx_auth_http, check, _} ->
                        'client.authenticate' = HookName, % assert
                        true;
                    {emqx_acl_http, check_acl, _} ->
                        'client.check_acl' = HookName, % assert
                        true;
                    _ ->
                        false
                end
        end,
    case lists:filter(F, Callbacks) of
        [_] -> true;
        [] -> false
    end.
