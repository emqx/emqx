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

-module(emqx_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REDIS_SINGLE_HOST, "redis").
-define(REDIS_SINGLE_PORT, 6379).
-define(REDIS_SENTINEL_HOST, "redis-sentinel").
-define(REDIS_SENTINEL_PORT, 26379).
-define(REDIS_CLUSTER_HOST, "redis-cluster-1").
-define(REDIS_CLUSTER_PORT, 6379).
-define(REDIS_RESOURCE_MOD, emqx_redis).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

init_per_suite(Config) ->
    Checks =
        case os:getenv("IS_CI") of
            "yes" -> 10;
            _ -> 1
        end,
    ok = wait_for_redis(Checks),
    ok = emqx_common_test_helpers:start_apps([emqx_conf]),
    ok = emqx_connector_test_helpers:start_apps([emqx_resource]),
    {ok, _} = application:ensure_all_started(emqx_connector),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps([emqx_resource]),
    _ = application:stop(emqx_connector).

init_per_testcase(_, Config) ->
    Config.

end_per_testcase(_, _Config) ->
    ok.

wait_for_redis(0) ->
    throw(timeout);
wait_for_redis(Checks) ->
    case
        emqx_common_test_helpers:is_all_tcp_servers_available(
            [
                {?REDIS_SINGLE_HOST, ?REDIS_SINGLE_PORT},
                {?REDIS_SENTINEL_HOST, ?REDIS_SENTINEL_PORT}
            ]
        )
    of
        true ->
            ok;
        false ->
            timer:sleep(1000),
            wait_for_redis(Checks - 1)
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_single_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_redis_SUITE_single">>,
        redis_config_single(),
        [<<"PING">>]
    ).

t_cluster_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_redis_SUITE_cluster">>,
        redis_config_cluster(),
        [<<"PING">>, <<"PONG">>]
    ).

t_sentinel_lifecycle(_Config) ->
    perform_lifecycle_check(
        <<"emqx_redis_SUITE_sentinel">>,
        redis_config_sentinel(),
        [<<"PING">>]
    ).

perform_lifecycle_check(ResourceId, InitialConfig, RedisCommand) ->
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?REDIS_RESOURCE_MOD, InitialConfig),
    {ok, #{
        state := #{pool_name := PoolName} = State,
        status := InitialStatus
    }} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?REDIS_RESOURCE_MOD,
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
    % Perform query as further check that the resource is working as expected
    ?assertEqual({ok, <<"PONG">>}, emqx_resource:query(ResourceId, {cmd, RedisCommand})),
    ?assertEqual(
        {ok, [{ok, <<"PONG">>}, {ok, <<"PONG">>}]},
        emqx_resource:query(ResourceId, {cmds, [RedisCommand, RedisCommand]})
    ),
    ?assertMatch(
        {error, {unrecoverable_error, [{ok, <<"PONG">>}, {error, _}]}},
        emqx_resource:query(
            ResourceId,
            {cmds, [RedisCommand, [<<"INVALID_COMMAND">>]]},
            #{timeout => 500}
        )
    ),
    % check authentication methods
    ?assertEqual(
        {ok, <<"OK">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "public"]})
    ),
    ?assertEqual(
        {error, <<"WRONGPASS invalid username-password pair or user is disabled.">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "test_passwd"]})
    ),
    ?assertEqual(
        {ok, <<"OK">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "test_user", "test_passwd"]})
    ),
    ?assertEqual(
        {error, <<"WRONGPASS invalid username-password pair or user is disabled.">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "test_user", "public"]})
    ),
    ?assertEqual(
        {error, <<"WRONGPASS invalid username-password pair or user is disabled.">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "wrong_user", "test_passwd"]})
    ),
    ?assertEqual(
        {error, <<"WRONGPASS invalid username-password pair or user is disabled.">>},
        emqx_resource:query(ResourceId, {cmd, ["AUTH", "wrong_user", "public"]})
    ),
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
    ?assertEqual({ok, <<"PONG">>}, emqx_resource:query(ResourceId, {cmd, RedisCommand})),
    % Stop and remove the resource in one go.
    ?assertEqual(ok, emqx_resource:remove_local(ResourceId)),
    ?assertEqual({error, not_found}, ecpool:stop_sup_pool(PoolName)),
    % Should not even be able to get the resource data out of ets now unlike just stopping.
    ?assertEqual({error, not_found}, emqx_resource:get_instance(ResourceId)).

% %%------------------------------------------------------------------------------
% %% Helpers
% %%------------------------------------------------------------------------------

redis_config_single() ->
    redis_config_base("single", "server").

redis_config_cluster() ->
    redis_config_base("cluster", "servers").

redis_config_sentinel() ->
    redis_config_base("sentinel", "servers").

-define(REDIS_CONFIG_BASE(MaybeSentinel, MaybeDatabase),
    "" ++
        "\n" ++
        "    auto_reconnect = true\n" ++
        "    pool_size = 8\n" ++
        "    redis_type = ~s\n" ++
        MaybeSentinel ++
        MaybeDatabase ++
        "    username = test_user\n" ++
        "    password = test_passwd\n" ++
        "    ~s = \"~s:~b\"\n" ++
        "    " ++
        ""
).

redis_config_base(Type, ServerKey) ->
    case Type of
        "sentinel" ->
            Host = ?REDIS_SENTINEL_HOST,
            Port = ?REDIS_SENTINEL_PORT,
            MaybeSentinel = "    sentinel = mymaster\n",
            MaybeDatabase = "    database = 1\n";
        "single" ->
            Host = ?REDIS_SINGLE_HOST,
            Port = ?REDIS_SINGLE_PORT,
            MaybeSentinel = "",
            MaybeDatabase = "    database = 1\n";
        "cluster" ->
            Host = ?REDIS_CLUSTER_HOST,
            Port = ?REDIS_CLUSTER_PORT,
            MaybeSentinel = "",
            MaybeDatabase = ""
    end,
    RawConfig = list_to_binary(
        io_lib:format(
            ?REDIS_CONFIG_BASE(MaybeSentinel, MaybeDatabase),
            [Type, ServerKey, Host, Port]
        )
    ),

    {ok, Config} = hocon:binary(RawConfig),
    #{<<"config">> => Config}.
