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

-module(ee_connector_clickhouse_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(CLICKHOUSE_HOST, "clickhouse").
-define(CLICKHOUSE_RESOURCE_MOD, emqx_ee_connector_clickhouse).

%% This test SUITE requires a running clickhouse instance. If you don't want to
%% bring up the whole CI infrastuctucture with the `scripts/ct/run.sh` script
%% you can create a clickhouse instance with the following command (execute it
%% from root of the EMQX directory.). You also need to set ?CLICKHOUSE_HOST and
%% ?CLICKHOUSE_PORT to appropriate values.
%%
%% docker run -d -p 18123:8123 -p19000:9000 --name some-clickhouse-server --ulimit nofile=262144:262144 -v "`pwd`/.ci/docker-compose-file/clickhouse/users.xml:/etc/clickhouse-server/users.xml" -v "`pwd`/.ci/docker-compose-file/clickhouse/config.xml:/etc/clickhouse-server/config.xml" clickhouse/clickhouse-server

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

clickhouse_url() ->
    erlang:iolist_to_binary([
        <<"http://">>,
        ?CLICKHOUSE_HOST,
        ":",
        erlang:integer_to_list(?CLICKHOUSE_DEFAULT_PORT)
    ]).

init_per_suite(Config) ->
    case
        emqx_common_test_helpers:is_tcp_server_available(?CLICKHOUSE_HOST, ?CLICKHOUSE_DEFAULT_PORT)
    of
        true ->
            ok = emqx_common_test_helpers:start_apps([emqx_conf]),
            ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
            {ok, _} = application:ensure_all_started(emqx_connector),
            {ok, _} = application:ensure_all_started(emqx_ee_connector),
            %% Create the db table
            {ok, Conn} =
                clickhouse:start_link([
                    {url, clickhouse_url()},
                    {user, <<"default">>},
                    {key, "public"},
                    {pool, tmp_pool}
                ]),
            {ok, _, _} = clickhouse:query(Conn, <<"CREATE DATABASE IF NOT EXISTS mqtt">>, #{}),
            clickhouse:stop(Conn),
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_clickhouse);
                _ ->
                    {skip, no_clickhouse}
            end
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

t_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_connector_clickhouse_SUITE">>,
        clickhouse_config()
    ).

show(X) ->
    erlang:display(X),
    X.

show(Label, What) ->
    erlang:display({Label, What}),
    What.

perform_lifecycle_check(PoolName, InitialConfig) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?CLICKHOUSE_RESOURCE_MOD, InitialConfig),
    {ok, #{
        state := #{poolname := ReturnedPoolName} = State,
        status := InitialStatus
    }} =
        emqx_resource:create_local(
            PoolName,
            ?CONNECTOR_RESOURCE_GROUP,
            ?CLICKHOUSE_RESOURCE_MOD,
            CheckedConfig,
            #{}
        ),
    ?assertEqual(InitialStatus, connected),
    % Instance should match the state and status of the just started resource
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := InitialStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    % % Perform query as further check that the resource is working as expected
    (fun() ->
        erlang:display({pool_name, PoolName}),
        QueryNoParamsResWrapper = emqx_resource:query(PoolName, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper),
        {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
        ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
    end)(),
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    % Resource will be listed still, but state will be changed and healthcheck will fail
    % as the worker no longer exists.
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{
        state := State,
        status := StoppedStatus
    }} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual(stopped, StoppedStatus),
    ?assertEqual({error, resource_is_stopped}, emqx_resource:health_check(PoolName)),
    % Resource healthcheck shortcuts things by checking ets. Go deeper by checking pool itself.
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
    % Can call stop/1 again on an already stopped instance
    ?assertEqual(ok, emqx_resource:stop(PoolName)),
    % Make sure it can be restarted and the healthchecks and queries work properly
    ?assertEqual(ok, emqx_resource:restart(PoolName)),
    % async restart, need to wait resource
    timer:sleep(500),
    {ok, ?CONNECTOR_RESOURCE_GROUP, #{status := InitialStatus}} =
        emqx_resource:get_instance(PoolName),
    ?assertEqual({ok, connected}, emqx_resource:health_check(PoolName)),
    (fun() ->
        QueryNoParamsResWrapper =
            emqx_resource:query(PoolName, test_query_no_params()),
        ?assertMatch({ok, _}, QueryNoParamsResWrapper),
        {_, QueryNoParamsRes} = QueryNoParamsResWrapper,
        ?assertMatch(<<"1">>, string:trim(QueryNoParamsRes))
    end)(),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(PoolName)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(ReturnedPoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(PoolName)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

clickhouse_config() ->
    Config =
        #{
            auto_reconnect => true,
            database => <<"mqtt">>,
            username => <<"default">>,
            password => <<"public">>,
            pool_size => 8,
            url => iolist_to_binary(
                io_lib:format(
                    "http://~s:~b",
                    [
                        ?CLICKHOUSE_HOST,
                        ?CLICKHOUSE_DEFAULT_PORT
                    ]
                )
            )
        },
    #{<<"config">> => Config}.

test_query_no_params() ->
    {query, <<"SELECT 1">>}.
