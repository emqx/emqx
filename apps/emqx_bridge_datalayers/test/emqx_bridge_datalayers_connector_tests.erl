%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_datalayers_connector_tests).

-include_lib("eunit/include/eunit.hrl").

default_influx_tls_config_test() ->
    SSL = checked_connector_tls(
        ~b"""
        connectors.datalayers.test {
          parameters { database = "db" }
          ssl { enable = true }
        }
        """
    ),
    ?assertEqual(verify_none, maps:get(<<"verify">>, SSL)),
    ?assertNot(maps:is_key(<<"cacertfile">>, SSL)).

arrow_flight_tls_verify_default_test() ->
    SSL = checked_connector_tls(
        ~b"""
        connectors.datalayers.test {
          parameters {
            driver_type = arrow_flight
            database = "db"
          }
          ssl { enable = true }
        }
        """
    ),
    ?assertEqual(verify_peer, maps:get(<<"verify">>, SSL)),
    ?assertNot(maps:is_key(<<"cacertfile">>, SSL)).

arrow_flight_null_verify_default_test() ->
    SSL = checked_connector_tls(
        ~b"""
        connectors.datalayers.test {
          parameters {
            driver_type = arrow_flight
            database = "db"
          }
          ssl {
            enable = true
            verify = null
          }
        }
        """
    ),
    ?assertEqual(verify_peer, maps:get(<<"verify">>, SSL)),
    ?assertNot(maps:is_key(<<"cacertfile">>, SSL)).

arrow_flight_explicit_verify_none_test() ->
    SSL = checked_connector_tls(
        ~b"""
        connectors.datalayers.test {
          parameters {
            driver_type = arrow_flight
            database = "db"
          }
          ssl {
            enable = true
            verify = verify_none
          }
        }
        """
    ),
    ?assertEqual(verify_none, maps:get(<<"verify">>, SSL)),
    ?assertNot(maps:is_key(<<"cacertfile">>, SSL)).

arrow_flight_string_enable_default_test() ->
    SSL = checked_connector_tls(
        ~b"""
        connectors.datalayers.test {
          parameters {
            driver_type = arrow_flight
            database = "db"
          }
          ssl { enable = "true" }
        }
        """
    ),
    ?assertEqual(verify_peer, maps:get(<<"verify">>, SSL)),
    ?assertNot(maps:is_key(<<"cacertfile">>, SSL)).

checked_connector_tls(Raw) ->
    {ok, Conf} = hocon:binary(Raw),
    Checked = hocon_tconf:check_plain(
        emqx_connector_schema, Conf, #{required => false, atom_key => false}
    ),
    #{<<"connectors">> := #{<<"datalayers">> := #{<<"test">> := #{<<"ssl">> := SSL}}}} =
        Checked,
    SSL.

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
