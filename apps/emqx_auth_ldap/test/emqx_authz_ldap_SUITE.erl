%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_authz_ldap_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_auth/include/emqx_authz.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_DEFAULT_PORT, 389).
-define(LDAP_RESOURCE, <<"emqx_authz_ldap_SUITE">>).

all() ->
    emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()).

groups() ->
    emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    ok = stop_apps([emqx_resource]),
    case emqx_common_test_helpers:is_tcp_server_available(?LDAP_HOST, ?LDAP_DEFAULT_PORT) of
        true ->
            ok = emqx_common_test_helpers:start_apps(
                [emqx_conf, emqx_auth, emqx_auth_ldap],
                fun set_special_configs/1
            ),
            ok = start_apps([emqx_resource]),
            ok = create_ldap_resource(),
            Config;
        false ->
            {skip, no_ldap}
    end.

end_per_suite(_Config) ->
    ok = emqx_authz_test_lib:restore_authorizers(),
    ok = emqx_resource:remove_local(?LDAP_RESOURCE),
    ok = stop_apps([emqx_resource]),
    ok = emqx_common_test_helpers:stop_apps([emqx_conf, emqx_auth, emqx_auth_ldap]).

init_per_group(Group, Config) ->
    [{test_case, emqx_authz_test_lib:get_case(Group, cases())} | Config].
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, true),
    ok.

set_special_configs(emqx_authz) ->
    ok = emqx_authz_test_lib:reset_authorizers();
set_special_configs(_) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_run_case(Config) ->
    Case = ?config(test_case, Config),
    ok = setup_authz_source(),
    ok = emqx_authz_test_lib:run_checks(Case).

t_create_invalid(_Config) ->
    ok = setup_authz_source(),
    BadConfig = maps:merge(
        raw_ldap_authz_config(),
        #{<<"server">> => <<"255.255.255.255:33333">>}
    ),
    {ok, _} = emqx_authz:update(?CMD_REPLACE, [BadConfig]),

    [_] = emqx_authz:lookup().

%%------------------------------------------------------------------------------
%% Case
%%------------------------------------------------------------------------------
cases() ->
    [
        #{
            name => simpe_publish,
            client_info => #{username => <<"mqttuser0001">>},
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/+">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/#">>}
            ]
        },
        #{
            name => simpe_subscribe,
            client_info => #{username => <<"mqttuser0001">>},
            checks => [
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/+">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/sub/#">>}
            ]
        },

        #{
            name => simpe_pubsub,
            client_info => #{username => <<"mqttuser0001">>},
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/+">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pubsub/#">>},

                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/1">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/+">>},
                {allow, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/pubsub/#">>}
            ]
        },

        #{
            name => simpe_unmatched,
            client_info => #{username => <<"mqttuser0001">>},
            checks => [
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0001/req/mqttuser0001/+">>},
                {deny, ?AUTHZ_PUBLISH, <<"mqttuser0001/req/mqttuser0002/+">>},
                {deny, ?AUTHZ_SUBSCRIBE, <<"mqttuser0001/req/+/mqttuser0002">>}
            ]
        }
    ].

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

setup_authz_source() ->
    setup_config(#{}).

raw_ldap_authz_config() ->
    #{
        <<"enable">> => <<"true">>,
        <<"type">> => <<"ldap">>,
        <<"server">> => ldap_server(),
        <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
        <<"username">> => <<"cn=root,dc=emqx,dc=io">>,
        <<"password">> => <<"public">>,
        <<"pool_size">> => 8
    }.

setup_config(SpecialParams) ->
    emqx_authz_test_lib:setup_config(
        raw_ldap_authz_config(),
        SpecialParams
    ).

ldap_server() ->
    iolist_to_binary(io_lib:format("~s:~B", [?LDAP_HOST, ?LDAP_DEFAULT_PORT])).

ldap_config() ->
    emqx_ldap_SUITE:ldap_config([]).

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).

create_ldap_resource() ->
    {ok, _} = emqx_resource:create_local(
        ?LDAP_RESOURCE,
        ?AUTHZ_RESOURCE_GROUP,
        emqx_ldap,
        ldap_config(),
        #{}
    ),
    ok.
