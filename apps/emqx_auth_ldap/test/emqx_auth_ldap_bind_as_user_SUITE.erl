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

-module(emqx_auth_ldap_bind_as_user_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(PID, emqx_auth_ldap).

-define(APP, emqx_auth_ldap).

-define(DeviceDN, "ou=test_device,dc=emqx,dc=io").

-define(AuthDN, "ou=test_auth,dc=emqx,dc=io").

all() ->
    [check_auth,
     check_acl].

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_auth_ldap], fun set_special_configs/1),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_auth_ldap]),
    %% clear the application envs to avoid cross-suite testcase failure
    application:unload(emqx_auth_ldap).

check_auth(_) ->
    MqttUser1 = #{clientid => <<"mqttuser1">>,
                  username => <<"user1">>,
                  password => <<"mqttuser0001">>,
                  zone => external},
    MqttUser2 = #{clientid => <<"mqttuser2">>,
                  username => <<"user2">>,
                  password => <<"mqttuser0002">>,
                  zone => external},
    NonExistUser1 = #{clientid => <<"mqttuser3">>,
                      username => <<"user3">>,
                      password => <<"mqttuser0003">>,
                      zone => external},
    ct:log("MqttUser: ~p", [emqx_access_control:authenticate(MqttUser1)]),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser1)),
    ?assertMatch({ok, #{auth_result := success}}, emqx_access_control:authenticate(MqttUser2)),
    ?assertEqual({error, not_authorized}, emqx_access_control:authenticate(NonExistUser1)).

check_acl(_) ->
    MqttUser = #{clientid => <<"mqttuser1">>, username => <<"user1">>, zone => external},
    NoMqttUser = #{clientid => <<"mqttuser2">>, username => <<"user7">>, zone => external},
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

set_special_configs(emqx) ->
    application:set_env(emqx, allow_anonymous, false),
    application:set_env(emqx, enable_acl_cache, false),
    application:set_env(emqx, acl_nomatch, deny),
    AclFilePath = filename:join(["test", "emqx_SUITE_data", "acl.conf"]),
    application:set_env(emqx, acl_file,
		        emqx_ct_helpers:deps_path(emqx, AclFilePath)),
    LoadedPluginPath = filename:join(["test", "emqx_SUITE_data", "loaded_plugins"]),
    application:set_env(emqx, plugins_loaded_file,
                        emqx_ct_helpers:deps_path(emqx, LoadedPluginPath));

set_special_configs(emqx_auth_ldap) ->
    application:set_env(emqx_auth_ldap, bind_as_user, true),
    application:set_env(emqx_auth_ldap, device_dn, "ou=testdevice, dc=emqx, dc=io"),
    application:set_env(emqx_auth_ldap, custom_base_dn, "${device_dn}"),
    %% auth.ldap.filters.1.key = mqttAccountName
    %% auth.ldap.filters.1.value = ${user}
    %% auth.ldap.filters.1.op = and
    %% auth.ldap.filters.2.key = objectClass
    %% auth.ldap.filters.1.value = mqttUser
    application:set_env(emqx_auth_ldap, filters, [{"mqttAccountName", "${user}"},
                                                  "and",
                                                  {"objectClass", "mqttUser"}]);

set_special_configs(_App) ->
    ok.

