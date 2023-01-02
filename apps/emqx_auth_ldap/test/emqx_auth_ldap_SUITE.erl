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

-module(emqx_auth_ldap_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PID, emqx_auth_ldap).

-define(APP, emqx_auth_ldap).

-define(DeviceDN, "ou=test_device,dc=emqx,dc=io").

-define(AuthDN, "ou=test_auth,dc=emqx,dc=io").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    [{group, nossl}, {group, ssl}].

groups() ->
    Cases = emqx_ct:all(?MODULE),
    [{nossl, Cases}, {ssl, Cases}].

init_per_group(GrpName, Cfg) ->
    Fun = fun(App) -> set_special_configs(GrpName, App) end,
    emqx_ct_helpers:start_apps([emqx_auth_ldap], Fun),
    Cfg.

end_per_group(_GrpName, _Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_auth_ldap]),
    %% clear the application envs to avoid cross-suite testcase failure
    application:unload(emqx_auth_ldap).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_check_auth(_) ->
    MqttUser1 = #{clientid => <<"mqttuser1">>,
                  username => <<"mqttuser0001">>,
                  password => <<"mqttuser0001">>,
                  zone => external},
    MqttUser2 = #{clientid => <<"mqttuser2">>,
                  username => <<"mqttuser0002">>,
                  password => <<"mqttuser0002">>,
                  zone => external},
    MqttUser3 = #{clientid => <<"mqttuser3">>,
                  username => <<"mqttuser0003">>,
                  password => <<"mqttuser0003">>,
                  zone => external},
    MqttUser4 = #{clientid => <<"mqttuser4">>,
                  username => <<"mqttuser0004">>,
                  password => <<"mqttuser0004">>,
                  zone => external},
    MqttUser5 = #{clientid => <<"mqttuser5">>,
                  username => <<"mqttuser0005">>,
                  password => <<"mqttuser0005">>,
                  zone => external},
    NonExistUser1 = #{clientid => <<"mqttuser6">>,
                      username => <<"mqttuser0006">>,
                      password => <<"mqttuser0006">>,
                      zone => external},
    NonExistUser2 = #{clientid => <<"mqttuser7">>,
                      username => <<"mqttuser0005">>,
                      password => <<"mqttuser0006">>,
                      zone => external},
    ct:log("MqttUser: ~p", [emqx_access_control:authenticate(MqttUser1)]),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser1)),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser2)),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser3)),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser4)),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser5)),
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(NonExistUser1)),
    ?assertEqual({error, bad_username_or_password}, emqx_access_control:authenticate(NonExistUser2)).

t_check_acl(_) ->
    MqttUser = #{clientid => <<"mqttuser1">>, username => <<"mqttuser0001">>, zone => external},
    NoMqttUser = #{clientid => <<"mqttuser2">>, username => <<"mqttuser0007">>, zone => external},
    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pub/1">>),
    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pub/+">>),
    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pub/#">>),

    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/sub/1">>),
    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/sub/+">>),
    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/sub/#">>),

    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pubsub/1">>),
    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pubsub/+">>),
    allow = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/pubsub/#">>),
    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/pubsub/1">>),
    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/pubsub/+">>),
    allow = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/pubsub/#">>),

    deny = emqx_access_control:check_acl(NoMqttUser, publish, <<"mqttuser0001/req/mqttuser0001/+">>),
    deny = emqx_access_control:check_acl(MqttUser, publish, <<"mqttuser0001/req/mqttuser0002/+">>),
    deny = emqx_access_control:check_acl(MqttUser, subscribe, <<"mqttuser0001/req/+/mqttuser0002">>),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

set_special_configs(_, emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, acl_nomatch, deny),
    AclFilePath = filename:join(["test", "emqx_SUITE_data", "acl.conf"]),
    application:set_env(emqx, acl_file,
		        emqx_ct_helpers:deps_path(emqx, AclFilePath)),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(Ssl, emqx_auth_ldap) ->
    case Ssl == ssl of
        true ->
            LdapOpts = application:get_env(emqx_auth_ldap, ldap, []),
            Path = emqx_ct_helpers:deps_path(emqx_auth_ldap, "test/certs/"),
            SslOpts = [{verify, verify_peer},
                       {fail_if_no_peer_cert, true},
                       {server_name_indication, disable},
                       {keyfile, Path ++ "/client-key.pem"},
                       {certfile, Path ++ "/client-cert.pem"},
                       {cacertfile, Path ++ "/cacert.pem"}],
            LdapOpts1 = lists:keystore(ssl, 1, LdapOpts, {ssl, true}),
            LdapOpts2 = lists:keystore(sslopts, 1, LdapOpts1, {sslopts, SslOpts}),
            LdapOpts3 = lists:keystore(port, 1, LdapOpts2, {port, 636}),
            application:set_env(emqx_auth_ldap, ldap, LdapOpts3);
        _ ->
            ok
    end,
    application:set_env(emqx_auth_ldap, device_dn, "ou=testdevice, dc=emqx, dc=io").

