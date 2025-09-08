%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_azure_event_hub_tests).

-include_lib("eunit/include/eunit.hrl").

%%===========================================================================
%% Data Section
%%===========================================================================

-define(CONNECTOR_TYPE_BIN, <<"azure_event_hub_producer">>).
-define(ACTION_TYPE_BIN, <<"azure_event_hub_producer">>).

%%===========================================================================
%% Helper functions
%%===========================================================================

check_connector(Overrides0) ->
    BaseConf = emqx_bridge_kafka_testlib:action_connector_config(#{}),
    DefaultOverrides = #{
        <<"authentication">> => #{<<"password">> => <<"Endpoint=...">>},
        <<"bootstrap_hosts">> => <<"emqx.servicebus.windows.net:9093">>,
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"server_name_indication">> => <<"auto">>
        }
    },
    Overrides = emqx_utils_maps:deep_merge(DefaultOverrides, Overrides0),
    Conf = emqx_utils_maps:deep_merge(BaseConf, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN, <<"x">>, Conf
    ).

%% To check defaults of hidden fields
check_connector_not_serializable(Overrides0) ->
    BaseConf = emqx_bridge_kafka_testlib:action_connector_config(#{}),
    DefaultOverrides = #{
        <<"authentication">> => #{<<"password">> => <<"Endpoint=...">>},
        <<"bootstrap_hosts">> => <<"emqx.servicebus.windows.net:9093">>,
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"server_name_indication">> => <<"auto">>
        }
    },
    Overrides = emqx_utils_maps:deep_merge(DefaultOverrides, Overrides0),
    Conf = emqx_utils_maps:deep_merge(BaseConf, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector_not_seriailzable(
        ?CONNECTOR_TYPE_BIN, <<"x">>, Conf
    ).

check_action(Overrides0) ->
    BaseConf0 = emqx_bridge_kafka_testlib:action_config(#{}),
    BaseConf = emqx_utils_maps:deep_remove(
        [<<"parameters">>, <<"message">>, <<"timestamp">>],
        BaseConf0
    ),
    DefaultOverrides = #{},
    Overrides = emqx_utils_maps:deep_merge(DefaultOverrides, Overrides0),
    Conf = emqx_utils_maps:deep_merge(BaseConf, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(
        action, ?ACTION_TYPE_BIN, <<"x">>, Conf
    ).

-define(connector_validation_error(Reason, Value),
    {emqx_connector_schema, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

-define(action_validation_error(Reason, Value),
    {emqx_bridge_v2_schema, [
        #{
            kind := validation_error,
            reason := Reason,
            value := Value
        }
    ]}
).

%%===========================================================================
%% Test cases
%%===========================================================================

aeh_producer_test_() ->
    [
        {"base config",
            ?_assertMatch(
                #{
                    <<"authentication">> := #{
                        <<"username">> := <<"$ConnectionString">>,
                        <<"mechanism">> := plain
                    },
                    <<"ssl">> := #{
                        <<"enable">> := true,
                        <<"server_name_indication">> := "servicebus.windows.net"
                    }
                },
                check_connector_not_serializable(#{})
            )},
        {"sni disabled",
            ?_assertMatch(
                #{<<"ssl">> := #{<<"server_name_indication">> := <<"disable">>}},
                check_connector(#{<<"ssl">> => #{<<"server_name_indication">> => <<"disable">>}})
            )},
        {"custom sni",
            ?_assertMatch(
                #{
                    <<"ssl">> := #{
                        <<"server_name_indication">> := <<"custom.servicebus.windows.net">>
                    }
                },
                check_connector(
                    #{
                        <<"ssl">> => #{
                            <<"server_name_indication">> => <<"custom.servicebus.windows.net">>
                        }
                    }
                )
            )},
        {"ssl disabled",
            ?_assertThrow(
                ?connector_validation_error("Expected: true" ++ _, <<"false">>),
                check_connector(#{<<"ssl">> => #{<<"enable">> => <<"false">>}})
            )},
        {"bad authn mechanism: scram sha256",
            ?_assertThrow(
                ?connector_validation_error("Expected: plain" ++ _, <<"scram_sha_256">>),
                check_connector(
                    #{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_256">>}}
                )
            )},
        {"bad authn mechanism: scram sha512",
            ?_assertThrow(
                ?connector_validation_error("Expected: plain" ++ _, <<"scram_sha_512">>),
                check_connector(
                    #{<<"authentication">> => #{<<"mechanism">> => <<"scram_sha_512">>}}
                )
            )},
        {"bad required acks: none",
            ?_assertThrow(
                ?action_validation_error(not_a_enum_symbol, none),
                check_action(#{<<"parameters">> => #{<<"required_acks">> => <<"none">>}})
            )},
        {"bad compression: snappy",
            ?_assertThrow(
                ?action_validation_error("Expected: no_compression" ++ _, <<"snappy">>),
                check_action(#{<<"parameters">> => #{<<"compression">> => <<"snappy">>}})
            )},
        {"bad compression: gzip",
            ?_assertThrow(
                ?action_validation_error("Expected: no_compression" ++ _, <<"gzip">>),
                check_action(#{<<"parameters">> => #{<<"compression">> => <<"gzip">>}})
            )}
    ].
