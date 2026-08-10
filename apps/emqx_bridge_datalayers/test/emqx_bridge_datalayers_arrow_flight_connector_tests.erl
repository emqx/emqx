%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_arrow_flight_connector_tests).

-include_lib("eunit/include/eunit.hrl").

blank_credentials_test_() ->
    {setup,
        fun() ->
            meck:new(emqx_resource_pool, [passthrough, no_link]),
            meck:expect(
                emqx_resource_pool,
                start,
                fun(_, _, _) -> {error, {start_pool_failed, noproc, mocked}} end
            ),
            ok
        end,
        fun(_) -> meck:unload(emqx_resource_pool) end, fun(_) ->
            Base = #{
                server => <<"127.0.0.1:8360">>,
                pool_size => 8,
                ssl => #{enable => false},
                parameters => #{
                    driver_type => arrow_flight,
                    database => <<"db">>,
                    enable_prepared => true
                }
            },
            [
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base#{
                            parameters := (maps:get(parameters, Base))#{username => <<"user">>}
                        }
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_credentials">>,
                        Base#{
                            parameters := (maps:get(parameters, Base))#{password => <<"pass">>}
                        }
                    )
                ),
                ?_assertMatch(
                    {error, _},
                    emqx_bridge_datalayers_arrow_flight_connector:on_start(
                        <<"test:blank_database">>,
                        Base#{
                            parameters := maps:remove(
                                database,
                                maps:get(parameters, Base)
                            )
                        }
                    )
                )
            ]
        end}.
