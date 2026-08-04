%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_connector_tests).

-include_lib("eunit/include/eunit.hrl").

blank_database_test_() ->
    Base = #{
        server => <<"127.0.0.1:8361">>,
        pool_size => 8,
        ssl => #{enable => false},
        parameters => #{
            driver_type => influxdb_v1,
            enable_prepared => true
        }
    },
    [
        ?_assertMatch(
            {error, {bad_config, _}},
            emqx_bridge_datalayers_connector:on_start(
                <<"test:blank_database">>,
                Base
            )
        ),
        ?_assertMatch(
            {error, {bad_config, _}},
            emqx_bridge_datalayers_connector:on_start(
                <<"test:blank_database">>,
                Base#{
                    parameters =>
                        (maps:get(parameters, Base))#{database => <<>>}
                }
            )
        )
    ].

blank_database_arrow_flight_driver_test_() ->
    Base = #{
        server => <<"127.0.0.1:8360">>,
        pool_size => 8,
        ssl => #{enable => false},
        parameters => #{
            driver_type => arrow_flight,
            enable_prepared => true
        }
    },
    [
        ?_assertMatch(
            {error, {bad_config, _}},
            emqx_bridge_datalayers_connector:on_start(
                <<"test:blank_database_arrow">>,
                Base
            )
        ),
        ?_assertMatch(
            {error, {bad_config, _}},
            emqx_bridge_datalayers_connector:on_start(
                <<"test:blank_database_arrow">>,
                Base#{
                    parameters =>
                        (maps:get(parameters, Base))#{database => <<>>}
                }
            )
        )
    ].
