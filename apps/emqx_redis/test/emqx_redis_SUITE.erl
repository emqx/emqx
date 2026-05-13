%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_redis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../../emqx_connector/include/emqx_connector.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("stdlib/include/assert.hrl").

-define(REDIS_SINGLE_HOST, "redis").
-define(REDIS_SINGLE_PORT, 6379).
-define(REDIS_SENTINEL_HOST, "redis-sentinel").
-define(REDIS_SENTINEL_PORT, 26379).
-define(REDIS_SENTINEL_S_NO_AUTH_M_AUTH_HOST, "redis-sentinel-noauth-master-auth").
-define(REDIS_SENTINEL_S_AUTH_M_AUTH_HOST, "redis-sentinel-auth-master-auth").
-define(REDIS_SENTINEL_S_NO_AUTH_M_NO_AUTH_HOST, "redis-sentinel-noauth-master-noauth").
-define(REDIS_SENTINEL_S_AUTH_M_NO_AUTH_HOST, "redis-sentinel-auth-master-noauth").
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
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_connector,
            emqx_redis
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

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
                {?REDIS_SENTINEL_HOST, ?REDIS_SENTINEL_PORT},
                {?REDIS_SENTINEL_S_NO_AUTH_M_AUTH_HOST, ?REDIS_SENTINEL_PORT},
                {?REDIS_SENTINEL_S_AUTH_M_AUTH_HOST, ?REDIS_SENTINEL_PORT},
                {?REDIS_SENTINEL_S_NO_AUTH_M_NO_AUTH_HOST, ?REDIS_SENTINEL_PORT},
                {?REDIS_SENTINEL_S_AUTH_M_NO_AUTH_HOST, ?REDIS_SENTINEL_PORT}
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

t_sentinel_password_auth(_Config) ->
    ResourceId = <<"emqx_redis_SUITE_sentinel_password_auth">>,
    {ok, #{config := CheckedConfig}} =
        emqx_resource:check_config(?REDIS_RESOURCE_MOD, redis_config_sentinel()),
    {ok, #{status := connected}} = emqx_resource:create_local(
        ResourceId,
        ?CONNECTOR_RESOURCE_GROUP,
        ?REDIS_RESOURCE_MOD,
        CheckedConfig,
        #{}
    ),
    ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
    ?assertEqual({ok, <<"PONG">>}, emqx_resource:query(ResourceId, {cmd, [<<"PING">>]})),
    ?assertEqual(ok, emqx_resource:remove_local(ResourceId)).

t_sentinel_auth_master_noauth_sentinel_noauth(_Config) ->
    run_sentinel_auth_matrix_case(#{
        name => master_noauth_sentinel_noauth,
        host => ?REDIS_SENTINEL_S_NO_AUTH_M_NO_AUTH_HOST,
        redis_auth => none,
        sentinel_auth => none
    }).

t_sentinel_auth_master_auth_sentinel_noauth(_Config) ->
    run_sentinel_auth_matrix_case(#{
        name => master_auth_sentinel_noauth,
        host => ?REDIS_SENTINEL_S_NO_AUTH_M_AUTH_HOST,
        redis_auth => password,
        sentinel_auth => none
    }).

t_sentinel_auth_master_noauth_sentinel_auth(_Config) ->
    run_sentinel_auth_matrix_case(#{
        name => master_noauth_sentinel_auth,
        host => ?REDIS_SENTINEL_S_AUTH_M_NO_AUTH_HOST,
        redis_auth => none,
        sentinel_auth => password
    }).

t_sentinel_auth_master_auth_sentinel_auth(_Config) ->
    run_sentinel_auth_matrix_case(#{
        name => master_auth_sentinel_auth,
        host => ?REDIS_SENTINEL_S_AUTH_M_AUTH_HOST,
        redis_auth => password,
        sentinel_auth => password
    }).

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

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

redis_config_single() ->
    redis_config_base("single", "server").

redis_config_cluster() ->
    redis_config_base("cluster", "servers").

redis_config_sentinel() ->
    redis_config_base("sentinel", "servers").

redis_config_sentinel_auth_matrix(Host, RedisAuth, SentinelAuth) ->
    RawConfig = list_to_binary(
        io_lib:format(
            "" ++
                "\n" ++
                "    auto_reconnect = true\n" ++
                "    pool_size = 1\n" ++
                "    redis_type = sentinel\n" ++
                "    sentinel = mymaster\n" ++
                "    database = 0\n" ++
                "    servers = \"~s:~b\"\n" ++
                "~s" ++
                "~s",
            [
                Host,
                ?REDIS_SENTINEL_PORT,
                redis_auth_config(RedisAuth),
                sentinel_auth_config(SentinelAuth)
            ]
        )
    ),
    {ok, Config} = hocon:binary(RawConfig),
    #{<<"config">> => Config}.

redis_auth_config(none) ->
    "";
redis_auth_config(password) ->
    "" ++
        "    username = \"redis_user\"\n" ++
        "    password = \"redis-password\"\n".

sentinel_auth_config(none) ->
    "";
sentinel_auth_config(password) ->
    "" ++
        "    sentinel_username = \"sentinel_user\"\n" ++
        "    sentinel_password = \"sentinel-password\"\n".

run_sentinel_auth_matrix_case(#{
    name := Name,
    host := Host,
    redis_auth := RedisAuth,
    sentinel_auth := SentinelAuth
}) ->
    ResourceId = iolist_to_binary(["emqx_redis_SUITE_", atom_to_binary(Name)]),
    Config = redis_config_sentinel_auth_matrix(Host, RedisAuth, SentinelAuth),
    {ok, #{config := CheckedConfig}} = emqx_resource:check_config(?REDIS_RESOURCE_MOD, Config),
    try
        {ok, #{status := connected}} = emqx_resource:create_local(
            ResourceId,
            ?CONNECTOR_RESOURCE_GROUP,
            ?REDIS_RESOURCE_MOD,
            CheckedConfig,
            #{spawn_buffer_workers => true}
        ),
        Key = atom_to_binary(Name),
        Value = <<"ok">>,
        ?assertEqual({ok, connected}, emqx_resource:health_check(ResourceId)),
        ?assertEqual(
            {ok, <<"OK">>},
            emqx_resource:query(ResourceId, {cmd, [<<"SET">>, Key, Value]})
        ),
        ?assertEqual(
            {ok, Value},
            emqx_resource:query(ResourceId, {cmd, [<<"GET">>, Key]})
        )
    after
        catch emqx_resource:remove_local(ResourceId)
    end.

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
            MaybeSentinel =
                "    sentinel = mytcpmaster\n" ++
                    "    sentinel_username = test_user\n" ++
                    "    sentinel_password = test_passwd\n",
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
