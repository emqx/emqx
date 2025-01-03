%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    emqx_authz_test_lib:all_with_table_case(?MODULE, t_run_case, cases()).

groups() ->
    emqx_authz_test_lib:table_groups(t_run_case, cases()).

init_per_suite(Config) ->
    case emqx_common_test_helpers:is_tcp_server_available(?LDAP_HOST, ?LDAP_DEFAULT_PORT) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_auth,
                    emqx_auth_ldap
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            [{apps, Apps} | Config];
        false ->
            {skip, no_ldap}
    end.

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(Group, Config) ->
    [{test_case, emqx_authz_test_lib:get_case(Group, cases())} | Config].
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    ok = emqx_authz_test_lib:reset_authorizers(),
    Config.
end_per_testcase(_TestCase, _Config) ->
    _ = emqx_authz:set_feature_available(rich_actions, true),
    ok = emqx_authz_test_lib:enable_node_cache(false),
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

t_node_cache(_Config) ->
    ClientInfo = #{username => <<"mqttuser0001">>, cert_common_name => <<"mqttUser">>},
    Case = #{
        name => cache_publish,
        client_info => ClientInfo,
        checks => []
    },
    setup_config(#{
        <<"base_dn">> => <<"uid=${username},ou=testdevice,dc=emqx,dc=io">>,
        %% This interpolation probably makes no sense,
        %% but we just test that the filter's vars are used for caching
        <<"filter">> => <<"(objectClass=${cert_common_name})">>
    }),
    ok = emqx_authz_test_lib:enable_node_cache(true),

    %% Subscribe to twice, should hit cache the second time
    emqx_authz_test_lib:run_checks(
        Case#{
            checks => [
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/1">>},
                {allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/+">>}
            ]
        }
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 1}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ),

    %% Change variables, should miss cache
    emqx_authz_test_lib:run_checks(
        Case#{
            checks => [{deny, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/1">>}],
            client_info => ClientInfo#{username => <<"username2">>}
        }
    ),
    emqx_authz_test_lib:run_checks(
        Case#{
            checks => [{allow, ?AUTHZ_PUBLISH, <<"mqttuser0001/pub/1">>}],
            client_info => ClientInfo#{cn => <<"mqttUser1">>}
        }
    ),
    ?assertMatch(
        #{hits := #{value := 1}, misses := #{value := 3}},
        emqx_auth_cache:metrics(?AUTHZ_CACHE)
    ).

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

start_apps(Apps) ->
    lists:foreach(fun application:ensure_all_started/1, Apps).

stop_apps(Apps) ->
    lists:foreach(fun application:stop/1, Apps).
