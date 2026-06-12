%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_relup_eredis_upgrade_SUITE).

-compile([export_all, nowarn_export_all]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_Case, Config) ->
    ok = meck:new(emqx_resource, [non_strict, passthrough, no_link]),
    ok = meck:new(eredis_sentinel, [non_strict, passthrough, no_link]),
    ok = meck:expect(eredis_sentinel, stop, fun() -> ok end),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = catch emqx_relup_eredis_upgrade:start_redis_resources(),
    ok = meck:unload(eredis_sentinel),
    ok = meck:unload(emqx_resource),
    ok.

t_redis_resource_stop_start_keeps_stopped_resources_stopped(_Config) ->
    RedisRunning = <<"redis-running">>,
    RedisStopped = <<"redis-stopped">>,
    BridgeRunning = <<"redis-bridge-running">>,
    OfflineRunning = <<"redis-offline-running">>,
    ok = mock_redis_resource_instances(#{
        emqx_redis => [RedisRunning, RedisStopped],
        emqx_bridge_redis_connector => [BridgeRunning],
        emqx_offline_messages_redis_connector => [OfflineRunning]
    }),
    ok = mock_resource_statuses(#{
        RedisRunning => connected,
        RedisStopped => stopped,
        BridgeRunning => disconnected,
        OfflineRunning => connected
    }),
    ok = meck:expect(emqx_resource, stop, fun(_ResId) -> ok end),
    ok = meck:expect(emqx_resource, start, fun(_ResId) -> ok end),

    ok = emqx_relup_eredis_upgrade:stop_redis_resources(),
    ?assertEqual(1, meck:num_calls(emqx_resource, stop, [RedisRunning])),
    ?assertEqual(0, meck:num_calls(emqx_resource, stop, [RedisStopped])),
    ?assertEqual(1, meck:num_calls(emqx_resource, stop, [BridgeRunning])),
    ?assertEqual(1, meck:num_calls(emqx_resource, stop, [OfflineRunning])),
    ?assertEqual(1, meck:num_calls(eredis_sentinel, stop, [])),

    ok = emqx_relup_eredis_upgrade:start_redis_resources(),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [RedisRunning])),
    ?assertEqual(0, meck:num_calls(emqx_resource, start, [RedisStopped])),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [BridgeRunning])),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [OfflineRunning])),

    ok = emqx_relup_eredis_upgrade:start_redis_resources(),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [RedisRunning])),
    ?assertEqual(0, meck:num_calls(emqx_resource, start, [RedisStopped])),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [BridgeRunning])),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [OfflineRunning])).

t_redis_resource_stop_is_retryable_after_partial_failure(_Config) ->
    RedisStoppedOnce = <<"redis-stopped-before-failure">>,
    RedisFailsOnce = <<"redis-fails-once">>,
    ok = mock_redis_resource_instances(#{
        emqx_redis => [RedisStoppedOnce, RedisFailsOnce],
        emqx_bridge_redis_connector => []
    }),
    ok = mock_resource_statuses(#{
        RedisStoppedOnce => connected,
        RedisFailsOnce => connected
    }),
    ok = meck:expect(emqx_resource, stop, fun
        (ResId) when ResId =:= RedisStoppedOnce ->
            ok;
        (ResId) when ResId =:= RedisFailsOnce ->
            {error, stop_failed}
    end),
    ?assertError(
        {failed_to_stop_redis_resource, RedisFailsOnce, stop_failed},
        emqx_relup_eredis_upgrade:stop_redis_resources()
    ),

    ok = meck:expect(emqx_resource, stop, fun(_ResId) -> ok end),
    ok = meck:expect(emqx_resource, start, fun(_ResId) -> ok end),
    ok = emqx_relup_eredis_upgrade:stop_redis_resources(),
    ok = emqx_relup_eredis_upgrade:start_redis_resources(),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [RedisStoppedOnce])),
    ?assertEqual(1, meck:num_calls(emqx_resource, start, [RedisFailsOnce])).

mock_redis_resource_instances(TypeToIds) ->
    ok = meck:expect(emqx_resource, list_instances_by_type, fun(Type) ->
        maps:get(Type, TypeToIds, [])
    end).

mock_resource_statuses(IdToStatus) ->
    ok = meck:expect(emqx_resource, get_instance, fun(ResId) ->
        case maps:find(ResId, IdToStatus) of
            {ok, Status} ->
                {ok, <<"default">>, #{status => Status}};
            error ->
                {error, not_found}
        end
    end).
