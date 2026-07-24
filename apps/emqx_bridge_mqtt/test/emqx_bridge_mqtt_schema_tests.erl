%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check_connector(InnerConfig) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"mqtt">>, <<"name">>, InnerConfig).

connector_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_connector_config(Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

schema_test_() ->
    [
        {"simple base config",
            ?_assertMatch(
                #{},
                parse_and_check_connector(
                    connector_config(#{})
                )
            )},
        {"static clientids : ok (old, just clientid format)",
            ?_assertMatch(
                #{
                    <<"static_clientids">> := [
                        #{
                            <<"node">> := <<"emqx@10.0.0.1">>,
                            <<"ids">> := [
                                #{<<"clientid">> := <<"1">>},
                                #{<<"clientid">> := <<"3">>}
                            ]
                        },
                        #{
                            <<"node">> := <<"emqx@10.0.0.2">>,
                            <<"ids">> := [
                                #{<<"clientid">> := <<"2">>}
                            ]
                        },
                        #{
                            <<"node">> := <<"emqx@10.0.0.3">>,
                            <<"ids">> := []
                        }
                    ]
                },
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [<<"1">>, <<"3">>]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => [<<"2">>]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => []
                            }
                        ]
                    })
                )
            )},
        {"static clientids : ok (new format with username and password)",
            ?_assertMatch(
                #{
                    <<"static_clientids">> := [
                        #{
                            <<"node">> := <<"emqx@10.0.0.1">>,
                            <<"ids">> := [
                                #{
                                    <<"clientid">> := <<"1">>,
                                    <<"username">> := <<"u1">>,
                                    <<"password">> := <<"p1">>
                                },
                                #{<<"clientid">> := <<"3">>}
                            ]
                        },
                        #{
                            <<"node">> := <<"emqx@10.0.0.2">>,
                            <<"ids">> := [
                                #{
                                    <<"clientid">> := <<"2">>,
                                    <<"username">> := <<"u2">>
                                }
                            ]
                        },
                        #{
                            <<"node">> := <<"emqx@10.0.0.3">>,
                            <<"ids">> := [#{<<"clientid">> := <<"4">>}]
                        }
                    ]
                },
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [
                                    #{
                                        <<"clientid">> => <<"1">>,
                                        <<"username">> => <<"u1">>,
                                        <<"password">> => <<"p1">>
                                    },
                                    #{<<"clientid">> => <<"3">>}
                                ]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => [
                                    #{
                                        <<"clientid">> => <<"2">>,
                                        <<"username">> => <<"u2">>
                                    }
                                ]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => [<<"4">>]
                            }
                        ]
                    })
                )
            )},
        {"static clientids : no clientids",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"must specify at least one static clientid">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => []
                            }
                        ]
                    })
                )
            )},
        {"static clientids : duplicated nodes",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason :=
                            <<"nodes must be unique; duplicated nodes: emqx@10.0.0.1, emqx@10.0.0.3">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => []
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => []
                            }
                        ]
                    })
                )
            )},
        {"static clientids : duplicated clientids",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"clientids must be unique; duplicated clientids: 1, 3">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [<<"1">>, <<"3">>, <<"1">>]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => [<<"3">>, <<"2">>]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => [<<"1">>, <<"3">>]
                            }
                        ]
                    })
                )
            )},
        {"static clientids : duplicated clientids (new format)",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"clientids must be unique; duplicated clientids: 1, 3">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [
                                    #{
                                        <<"clientid">> => <<"1">>,
                                        <<"username">> => <<"u1">>,
                                        <<"password">> => <<"p1">>
                                    },
                                    #{
                                        <<"clientid">> => <<"3">>,
                                        <<"username">> => <<"u3">>,
                                        <<"password">> => <<"p3">>
                                    },
                                    #{
                                        <<"clientid">> => <<"1">>,
                                        <<"username">> => <<"u11">>,
                                        <<"password">> => <<"p11">>
                                    }
                                ]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.2">>,
                                <<"ids">> => [<<"3">>, <<"2">>]
                            },
                            #{
                                <<"node">> => <<"emqx@10.0.0.3">>,
                                <<"ids">> => [<<"1">>, <<"3">>]
                            }
                        ]
                    })
                )
            )},
        {"static clientids : empty clientids",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"clientids must be non-empty">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [<<"1">>, <<"">>]
                            }
                        ]
                    })
                )
            )},
        {"static clientids : empty clientids (new format)",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := <<"clientids must be non-empty">>,
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{
                        <<"static_clientids">> => [
                            #{
                                <<"node">> => <<"emqx@10.0.0.1">>,
                                <<"ids">> => [<<"1">>, #{<<"clientid">> => <<"">>}]
                            }
                        ]
                    })
                )
            )},
        {"tcp_opts : parse and convert to proplist",
            ?_test(begin
                #{<<"tcp_opts">> := TcpOpts} = parse_and_check_connector(
                    connector_config(#{
                        <<"tcp_opts">> => #{
                            <<"active_n">> => 100,
                            <<"nodelay">> => true,
                            <<"sndbuf">> => <<"16KB">>,
                            <<"recbuf">> => <<"8KB">>,
                            <<"buffer">> => <<"32KB">>,
                            <<"keepalive">> => true,
                            <<"delay_send">> => true
                        }
                    })
                ),
                ?assertMatch(
                    #{
                        <<"active_n">> := 100,
                        <<"nodelay">> := true,
                        <<"sndbuf">> := <<"16KB">>,
                        <<"recbuf">> := <<"8KB">>,
                        <<"buffer">> := <<"32KB">>,
                        <<"keepalive">> := true,
                        <<"delay_send">> := true
                    },
                    TcpOpts
                ),
                Proplist = emqx_schema:client_tcp_opts_to_proplist(#{
                    active_n => 100,
                    nodelay => true,
                    sndbuf => 16384,
                    recbuf => 8192,
                    buffer => 32768,
                    keepalive => true,
                    delay_send => true
                }),
                ?assertEqual(100, proplists:get_value(active, Proplist)),
                ?assertEqual(true, proplists:get_value(nodelay, Proplist)),
                ?assertEqual(16384, proplists:get_value(sndbuf, Proplist)),
                ?assertEqual(8192, proplists:get_value(recbuf, Proplist)),
                ?assertEqual(32768, proplists:get_value(buffer, Proplist)),
                ?assertEqual(true, proplists:get_value(keepalive, Proplist)),
                ?assertEqual(true, proplists:get_value(delay_send, Proplist))
            end)},
        {"tcp_opts : empty/unset keys are not forwarded",
            ?_test(begin
                ?assertEqual([], emqx_schema:client_tcp_opts_to_proplist(#{})),
                ?assertEqual([], emqx_schema:client_tcp_opts_to_proplist(undefined)),
                Partial = emqx_schema:client_tcp_opts_to_proplist(#{nodelay => false}),
                ?assertEqual([{nodelay, false}], Partial)
            end)},
        %% The official MQTT URI schemes are `mqtt' (plain TCP) and `mqtts' (TLS).
        %% See https://github.com/mqtt/mqtt.org/wiki/URI-Scheme
        {"server : bare host:port (no scheme) is accepted",
            ?_assertMatch(
                #{<<"server">> := <<"127.0.0.1:1883">>},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"127.0.0.1:1883">>})
                )
            )},
        {"server : mqtt://host:port is accepted",
            ?_assertMatch(
                #{<<"server">> := <<"mqtt://broker.example:1883">>},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"mqtt://broker.example:1883">>})
                )
            )},
        {"server : mqtt://ip:port is accepted",
            ?_assertMatch(
                #{<<"server">> := <<"mqtt://127.0.0.1:1883">>},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"mqtt://127.0.0.1:1883">>})
                )
            )},
        {"server : mqtts://host:port is accepted",
            ?_assertMatch(
                #{<<"server">> := <<"mqtts://broker.example:8883">>},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"mqtts://broker.example:8883">>})
                )
            )},
        {"server : mqtt://[ipv6]:port is accepted",
            ?_assertMatch(
                #{<<"server">> := <<"mqtt://[::1]:1883">>},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"mqtt://[::1]:1883">>})
                )
            )},
        {"server : unsupported scheme is rejected",
            ?_assertThrow(
                {_SchemaMod, [
                    #{
                        reason := "unsupported_scheme",
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"tcp://broker.example:1883">>})
                )
            )}
    ].
