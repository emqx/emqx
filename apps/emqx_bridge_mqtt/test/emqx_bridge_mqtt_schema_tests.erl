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
        {"static clientids : ok",
            ?_assertMatch(
                #{},
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
            )}
    ].
