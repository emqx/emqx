%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_resource_schema_tests).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

%%===========================================================================
%% Test cases
%%===========================================================================

health_check_interval_validator_test_() ->
    [
        ?_assertMatch(
            #{},
            http_bridge_health_check_hocon(<<"150s">>)
        ),
        ?_assertMatch(
            #{},
            http_bridge_health_check_hocon(<<"3600000ms">>)
        ),
        ?_assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := <<"Health Check Interval (3600001ms) is out of range", _/binary>>,
                    value := 3600001
                }
            ]},
            http_bridge_health_check_hocon(<<"3600001ms">>)
        ),
        {"bad parse: negative number",
            ?_assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := "Not a valid duration",
                        value := <<"-10ms">>
                    }
                ]},
                http_bridge_health_check_hocon(<<"-10ms">>)
            )},
        {"bad parse: underscores",
            ?_assertThrow(
                {_, [
                    #{
                        kind := validation_error,
                        reason := "Not a valid duration",
                        value := <<"3_600_000ms">>
                    }
                ]},
                http_bridge_health_check_hocon(<<"3_600_000ms">>)
            )},
        ?_assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := "timeout value too large" ++ _
                }
            ]},
            http_bridge_health_check_hocon(<<"150000000000000s">>)
        )
    ].

worker_pool_size_test_() ->
    Check = fun(WorkerPoolSize) ->
        #{<<"resource_opts">> := #{<<"worker_pool_size">> := WPS}} =
            emqx_bridge_schema_testlib:http_action_config(#{
                <<"connector">> => <<"my_connector">>,
                <<"resource_opts">> => #{<<"worker_pool_size">> => WorkerPoolSize}
            }),
        WPS
    end,
    AssertThrow = fun(WorkerPoolSize) ->
        ?assertThrow(
            {_, [
                #{
                    kind := validation_error,
                    reason := #{expected := _},
                    value := WorkerPoolSize
                }
            ]},
            Check(WorkerPoolSize)
        )
    end,
    [
        ?_assertEqual(1, Check(1)),
        ?_assertEqual(100, Check(100)),
        ?_assertEqual(1024, Check(1024)),
        ?_test(AssertThrow(0)),
        ?_test(AssertThrow(1025))
    ].

%%===========================================================================
%% Helper functions
%%===========================================================================

%%===========================================================================
%% Data section
%%===========================================================================

http_bridge_health_check_hocon(HealthCheckInterval) ->
    emqx_bridge_schema_testlib:http_connector_config(#{
        <<"resource_opts">> => #{<<"health_check_interval">> => HealthCheckInterval}
    }).
