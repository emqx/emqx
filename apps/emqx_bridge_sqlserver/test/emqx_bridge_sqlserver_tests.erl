%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_sqlserver_tests).

-include_lib("eunit/include/eunit.hrl").

-include("emqx_bridge_sqlserver.hrl").

-define(CONNECTOR_TYPE_BIN, <<"sqlserver">>).
-define(CONNECTOR_NAME, <<"mysqlserver">>).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

parse_and_check_connector(InnerConfig) ->
    emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN,
        ?CONNECTOR_NAME,
        InnerConfig
    ).

connector_config(Overrides) ->
    Base = #{
        <<"database">> => <<"mqtt">>,
        <<"driver">> => <<"ms-sql">>,
        <<"enable">> => true,
        <<"password">> => <<"secretpass">>,
        <<"pool_size">> => 8,
        <<"resource_opts">> =>
            #{
                <<"health_check_interval">> => <<"15s">>,
                <<"start_after_created">> => true,
                <<"start_timeout">> => <<"5s">>
            },
        <<"server">> => <<"127.0.0.2:1433">>,
        <<"username">> => <<"root">>
    },
    emqx_utils_maps:deep_merge(Base, Overrides).

parse_server(Str) ->
    emqx_bridge_sqlserver_connector:parse_server(Str).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

server_schema_test_() ->
    [
        {"only server, no named instance, no port: IP",
            ?_assertMatch(
                #{},
                parse_and_check_connector(connector_config(#{<<"server">> => <<"127.0.0.2">>}))
            )},
        {"only server, no named instance, no port: name",
            ?_assertMatch(
                #{},
                parse_and_check_connector(connector_config(#{<<"server">> => <<"sqlserver">>}))
            )},
        {"server and port, no named instance: IP",
            ?_assertMatch(
                #{},
                parse_and_check_connector(connector_config(#{<<"server">> => <<"127.0.0.2:1434">>}))
            )},
        {"server and port, no named instance: name",
            ?_assertMatch(
                #{},
                parse_and_check_connector(connector_config(#{<<"server">> => <<"sqlserver:1434">>}))
            )},
        {"server and named instance, no port: IP",
            ?_assertThrow(
                {_, [
                    #{
                        reason := "must_explicitly_define_port_when_using_named_instances",
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"127.0.0.2\\NamedInstance">>})
                )
            )},
        {"server and named instance, no port: name",
            ?_assertThrow(
                {_, [
                    #{
                        reason := "must_explicitly_define_port_when_using_named_instances",
                        kind := validation_error
                    }
                ]},
                parse_and_check_connector(
                    connector_config(#{<<"server">> => <<"sqlserver\\NamedInstance">>})
                )
            )},
        {"server, named instance, port: IP",
            ?_assertMatch(
                #{},
                parse_and_check_connector(
                    connector_config(#{
                        <<"server">> =>
                            <<"127.0.0.2\\NamedInstance:52000">>
                    })
                )
            )},
        {"server, named instance, port: name",
            ?_assertMatch(
                #{},
                parse_and_check_connector(
                    connector_config(#{
                        <<"server">> =>
                            <<"sqlserver\\NamedInstance:52000">>
                    })
                )
            )}
    ].

server_parse_test_() ->
    [
        {"only server, no named instance, no port: IP",
            ?_assertMatch(
                #{hostname := "127.0.0.2", port := ?SQLSERVER_DEFAULT_PORT},
                parse_server(<<"127.0.0.2">>)
            )},
        {"only server, no named instance, no port: name",
            ?_assertMatch(
                #{hostname := "sqlserver", port := ?SQLSERVER_DEFAULT_PORT},
                parse_server(<<"sqlserver">>)
            )},
        {"server and port, no named instance: IP",
            ?_assertMatch(
                #{hostname := "127.0.0.2", port := 1434},
                parse_server(<<"127.0.0.2:1434">>)
            )},
        {"server and port, no named instance: name",
            ?_assertMatch(
                #{hostname := "sqlserver", port := 1434},
                parse_server(<<"sqlserver:1434">>)
            )},
        {"server and named instance, no port: IP",
            ?_assertThrow(
                "must_explicitly_define_port_when_using_named_instances",
                parse_server(<<"127.0.0.2\\NamedInstance">>)
            )},
        {"server and named instance, no port: name",
            ?_assertThrow(
                "must_explicitly_define_port_when_using_named_instances",
                parse_server(<<"sqlserver\\NamedInstance">>)
            )},
        {"server, named instance, port: IP",
            ?_assertMatch(
                #{hostname := "127.0.0.2", port := 52000, instance_name := "NamedInstance"},
                parse_server(<<"127.0.0.2\\NamedInstance:52000">>)
            )},
        {"server, named instance, port: name",
            ?_assertMatch(
                #{hostname := "sqlserver", port := 52000, instance_name := "NamedInstance"},
                parse_server(<<"sqlserver\\NamedInstance:52000">>)
            )}
    ].
