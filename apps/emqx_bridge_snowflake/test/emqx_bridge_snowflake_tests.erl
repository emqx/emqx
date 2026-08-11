%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_snowflake_tests).

-include_lib("eunit/include/eunit.hrl").
-include("src/emqx_bridge_snowflake.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

-define(CONNECTOR_NAME, <<"my_connector">>).

parse_and_check_connector(InnerConfig) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN,
        ?CONNECTOR_NAME,
        InnerConfig
    ).

connector_config(Overrides) ->
    Base = emqx_bridge_snowflake_SUITE:connector_config(
        ?CONNECTOR_NAME,
        <<"orgid-accountid">>,
        <<"orgid-accountid.snowflakecomputing.com">>,
        <<"odbcuser">>,
        <<"odbcpass">>
    ),
    emqx_utils_maps:deep_merge(Base, Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

validation_test_() ->
    [
        {"good config", ?_assertMatch(#{}, parse_and_check_connector(connector_config(#{})))},
        {"account must contain org id and account id",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"Account identifier must be of form ORGID-ACCOUNTNAME">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"account">> => <<"onlyaccid">>
                    })
                )
            )}
    ].

ssl_transport_opts_test_() ->
    TransportOpts = fun(SSL) ->
        emqx_bridge_snowflake_connector:http_pool_transport_opts(SSL)
    end,
    [
        {"untouched config keeps verify_none",
            ?_assertEqual(
                verify_none,
                proplists:get_value(verify, TransportOpts(#{}))
            )},
        {"configured verify is applied",
            ?_assertEqual(
                verify_peer,
                proplists:get_value(verify, TransportOpts(#{verify => verify_peer}))
            )},
        {"configured server_name_indication is applied",
            ?_assertEqual(
                "custom.sni.host",
                proplists:get_value(
                    server_name_indication,
                    TransportOpts(#{
                        verify => verify_peer,
                        server_name_indication => <<"custom.sni.host">>
                    })
                )
            )},
        {"TLS options are built even when ssl.enable is false",
            ?_assertMatch(
                [_ | _],
                TransportOpts(#{enable => false, verify => verify_none})
            )}
    ].
