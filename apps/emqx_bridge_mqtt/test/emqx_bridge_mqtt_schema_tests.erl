%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_schema_tests).

-include_lib("eunit/include/eunit.hrl").

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check_connector(InnerConfig) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"mqtt">>, <<"name">>, InnerConfig).

connector_config(Overrides) ->
    emqx_bridge_mqtt_v2_publisher_SUITE:connector_config(Overrides).

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
            )}
    ].
