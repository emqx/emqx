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

-module(emqx_auth_http_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx_libs/include/emqx.hrl").
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
    [{group, http_inet},
     {group, http_inet6},
     {group, https_inet},
     {group, https_inet6}].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{Name, Cases} || Name <- [http_inet, http_inet6, https_inet, https_inet6]].

init_per_group(GrpName, Cfg) ->
    [Schema, Inet] = [list_to_atom(X) || X <- string:tokens(atom_to_list(GrpName), "_")],
    http_auth_server:start(Schema, Inet),
    Fun = fun(App) -> set_special_configs(App, Schema, Inet) end,
    emqx_ct_helpers:start_apps([emqx_auth_http], Fun),
    Cfg.

end_per_group(_GrpName, _Cfg) ->
    http_auth_server:stop(),
    emqx_ct_helpers:stop_apps([emqx_auth_http, emqx]).

set_special_configs(emqx, _Schmea, _Inet) ->
    application:set_env(emqx, allow_anonymous, true),
    application:set_env(emqx, enable_acl_cache, false),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(emqx_auth_http, Schema, Inet) ->
    AuthReq = maps:from_list(application:get_env(emqx_auth_http, auth_req, [])),
    SuprReq = maps:from_list(application:get_env(emqx_auth_http, super_req, [])),
    AclReq  = maps:from_list(application:get_env(emqx_auth_http, acl_req, [])),
    SvrAddr = http_server_host(Schema, Inet),

    AuthReq1 = AuthReq#{method := get, url := SvrAddr ++ "/mqtt/auth"},
    SuprReq1 = SuprReq#{method := post, content_type := 'x-www-form-urlencoded', url := SvrAddr ++ "/mqtt/superuser"},
    AclReq1  = AclReq #{method := post, content_type := json, url := SvrAddr ++ "/mqtt/acl"},

    Schema =:= https andalso set_https_client_opts(),

    application:set_env(emqx_auth_http, auth_req, maps:to_list(AuthReq1)),
    application:set_env(emqx_auth_http, super_req, maps:to_list(SuprReq1)),
    application:set_env(emqx_auth_http, acl_req, maps:to_list(AclReq1)).

%% @private
set_https_client_opts() ->
    HttpOpts = maps:from_list(application:get_env(emqx_auth_http, http_opts, [])),
    HttpOpts1 = HttpOpts#{ssl => emqx_ct_helpers:client_ssl_twoway()},
    application:set_env(emqx_auth_http, http_opts, maps:to_list(HttpOpts1)).

%% @private
http_server_host(http, inet) -> "http://127.0.0.1:8991";
http_server_host(http, inet6) -> "http://[::1]:8991";
http_server_host(https, inet) -> "https://127.0.0.1:8991";
http_server_host(https, inet6) -> "https://[::1]:8991".

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_check_acl(_) ->
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

t_check_auth(_) ->
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

t_sub_pub(_) ->
    ct:pal("start client"),
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

t_comment_config(_) ->
    AuthCount = length(emqx_hooks:lookup('client.authenticate')),
    AclCount = length(emqx_hooks:lookup('client.check_acl')),
    application:stop(?APP),
    [application:unset_env(?APP, Par) || Par <- [acl_req, auth_req]],
    application:start(?APP),
    ?assertEqual([], emqx_hooks:lookup('client.authenticate')),
    ?assertEqual(AuthCount - 1, length(emqx_hooks:lookup('client.authenticate'))),
    ?assertEqual(AclCount - 1, length(emqx_hooks:lookup('client.check_acl'))).

