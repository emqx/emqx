% %%--------------------------------------------------------------------
% %% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
% %%
% %% Licensed under the Apache License, Version 2.0 (the "License");
% %% you may not use this file except in compliance with the License.
% %% You may obtain a copy of the License at
% %% http://www.apache.org/licenses/LICENSE-2.0
% %%
% %% Unless required by applicable law or agreed to in writing, software
% %% distributed under the License is distributed on an "AS IS" BASIS,
% %% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
% %% See the License for the specific language governing permissions and
% %% limitations under the License.
% %%--------------------------------------------------------------------

-module(emqx_ldap_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("eldap/include/eldap.hrl").

-define(LDAP_HOST, "ldap").
-define(LDAP_RESOURCE_MOD, emqx_ldap).

all() ->
    [
        {group, tcp},
        {group, ssl}
    ].

groups() ->
    Cases = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, Cases},
        {ssl, Cases}
    ].

init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(_, Config) ->
    proplists:delete(group, Config).

init_per_suite(Config) ->
    Port = port(tcp),
    case emqx_common_test_helpers:is_tcp_server_available(?LDAP_HOST, Port) of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            Config;
        false ->
            {skip, no_ldap}
    end.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

% %%------------------------------------------------------------------------------
% %% Testcases
% %%------------------------------------------------------------------------------

t_lifecycle(Config) ->
    perform_lifecycle_check(
        <<"emqx_ldap_SUITE">>,
        ldap_config(Config)
    ).

perform_lifecycle_check(ResourceId, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?LDAP_RESOURCE_MOD, InitialConfig),
    {ok, #{
        state := #{pool_name := PoolName} = State,
        status := InitialStatus
    }} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?LDAP_RESOURCE_MOD,
        CheckedConfig,
        #{}
    ),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    % % Perform query as further check that the resource is working as expected
    ?assertMatch(
        {ok, [#eldap_entry{attributes = [_, _ | _]}]},
        emqx_resource:query(ResourceId, test_query_no_attr())
    ),
    ?assertMatch(
        {ok, [#eldap_entry{attributes = [{"mqttAccountName", _}]}]},
        emqx_resource:query(ResourceId, test_query_with_attr())
    ),
    ?assertMatch(
        {ok, _},
        emqx_resource:query(
            ResourceId,
            test_query_with_attr_and_timeout()
        )
    ),
    ?assertMatch({ok, []}, emqx_resource:query(ResourceId, test_query_not_exists())),
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(ResourceId)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(ResourceId)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(ResourceId)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(ResourceId),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    ?assertMatch({ok, _}, emqx_resource:query(ResourceId, test_query_no_attr())),
    ?assertMatch({ok, _}, emqx_resource:query(ResourceId, test_query_with_attr())),
    ?assertMatch(
        {ok, _},
        emqx_resource:query(
            ResourceId,
            test_query_with_attr_and_timeout()
        )
    ),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceId)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceId)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------
ldap_config(Config) ->
    RawConfig = list_to_binary(
        io_lib:format(
            ""
            "\n"
            "    auto_reconnect = true\n"
            "    username= \"cn=root,dc=emqx,dc=io\"\n"
            "    password = public\n"
            "    pool_size = 8\n"
            "    server = \"~s:~b\"\n"
            "    base_object=\"uid=${username},ou=testdevice,dc=emqx,dc=io\"\n"
            "    filter =\"(objectClass=mqttUser)\"\n"
            "    ~ts\n"
            "",
            [?LDAP_HOST, port(Config), ssl(Config)]
        )
    ),

    {ok, LDConfig} = hocon:binary(RawConfig),
    #{<<"config">> => LDConfig}.

test_query_no_attr() ->
    {query, data()}.

test_query_with_attr() ->
    {query, data(), ["mqttAccountName"]}.

test_query_with_attr_and_timeout() ->
    {query, data(), ["mqttAccountName"], 5000}.

test_query_not_exists() ->
    {query, #{username => <<"not_exists">>}}.

data() ->
    #{username => <<"mqttuser0001">>}.

port(tcp) -> 389;
port(ssl) -> 636;
port(Config) -> port(proplists:get_value(group, Config)).

ssl(Config) ->
    case proplists:get_value(group, Config) of
        tcp ->
            "ssl.enable=false";
        ssl ->
            "ssl.enable=true\n"
            "ssl.cacertfile=\"etc/openldap/cacert.pem\""
    end.
