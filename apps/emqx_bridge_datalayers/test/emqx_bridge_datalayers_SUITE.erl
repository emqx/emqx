%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, with_batch},
        {group, without_batch}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {with_batch, [
            {group, sync_query},
            {group, async_query}
        ]},
        {without_batch, [
            {group, sync_query},
            {group, async_query}
        ]},
        {sync_query, [
            {group, apiv1_tcp},
            {group, apiv1_tls}
        ]},
        {async_query, [
            {group, apiv1_tcp},
            {group, apiv1_tls}
        ]},
        {apiv1_tcp, TCs},
        {apiv1_tls, TCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(DatalayersType, Config0) when
    DatalayersType =:= apiv1_tcp;
    DatalayersType =:= apiv1_tls
->
    #{
        host := DatalayersHost,
        port := DatalayersPort,
        use_tls := UseTLS,
        proxy_name := ProxyName
    } =
        case DatalayersType of
            apiv1_tcp ->
                #{
                    host => os:getenv("DATALAYERS_TCP_HOST", "toxiproxy"),
                    port => list_to_integer(os:getenv("DATALAYERS_TCP_PORT", "8361")),
                    use_tls => false,
                    proxy_name => "datalayers_tcp"
                };
            apiv1_tls ->
                #{
                    host => os:getenv("DATALAYERS_TLS_HOST", "toxiproxy"),
                    port => list_to_integer(os:getenv("DATALAYERS_TLS_PORT", "8362")),
                    use_tls => true,
                    proxy_name => "datalayers_tls"
                }
        end,
    case emqx_common_test_helpers:is_tcp_server_available(DatalayersHost, DatalayersPort) of
        true ->
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx_bridge_datalayers,
                    emqx_connector,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            Config = [{apps, Apps}, {use_tls, UseTLS} | Config0],
            {Name, ConnConfString, ConnConfMap, ActionConfString, ActionConfMap} = datalayers_config(
                DatalayersHost, DatalayersPort, Config
            ),
            EHttpcPoolNameBin = <<(atom_to_binary(?MODULE))/binary, "_apiv1">>,
            EHttpcPoolName = binary_to_atom(EHttpcPoolNameBin),
            {EHttpcTransport, EHttpcTransportOpts} =
                case UseTLS of
                    true -> {tls, [{verify, verify_none}]};
                    false -> {tcp, []}
                end,
            EHttpcPoolOpts = [
                {host, DatalayersHost},
                {port, DatalayersPort},
                {pool_size, 1},
                {transport, EHttpcTransport},
                {transport_opts, EHttpcTransportOpts}
            ],

            {ok, _} = ehttpc_sup:start_pool(EHttpcPoolName, EHttpcPoolOpts),
            NewConfig =
                [
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort},
                    {proxy_name, ProxyName},

                    {datalayers_host, DatalayersHost},
                    {datalayers_port, DatalayersPort},
                    {ehttpc_pool_name, EHttpcPoolName},

                    {bridge_type, datalayers},
                    {bridge_name, Name},
                    {bridge_config, ActionConfMap},
                    {bridge_config_string, ActionConfString},

                    {connector_name, Name},
                    {connector_type, datalayers},
                    {connector_config, ConnConfMap},
                    {connector_config_string, ConnConfString}
                    | Config
                ],
            ensure_database(NewConfig),
            NewConfig;
        false ->
            {skip, no_datalayers}
    end;
init_per_group(sync_query, Config) ->
    [{query_mode, sync} | Config];
init_per_group(async_query, Config) ->
    [{query_mode, async} | Config];
init_per_group(with_batch, Config) ->
    [{batch_size, 100} | Config];
init_per_group(without_batch, Config) ->
    [{batch_size, 1} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when
    Group =:= apiv1_tcp;
    Group =:= apiv1_tls
->
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    EHttpcPoolName = ?config(ehttpc_pool_name, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ehttpc_sup:stop_pool(EHttpcPoolName),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

example_write_syntax() ->
    %% N.B.: this single space character is relevant
    <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
        "${clientid}_int_value=${payload.int_key}i,",
        "uint_value=${payload.uint_key}u,"
        "float_value=${payload.float_key},", "undef_value=${payload.undef},",
        "${undef_key}=\"hard-coded-value\",", "bool=${payload.bool}">>.

datalayers_config(DatalayersHost, DatalayersPort, Config) ->
    Name = atom_to_binary(?MODULE),
    {ConnConfString, ConnConfMap} = datalayers_connector_config(
        Name, DatalayersHost, DatalayersPort, Config
    ),
    {ActionConfString, ActionConfMap} = datalayers_action_config(Name, Config),
    {Name, ConnConfString, ConnConfMap, ActionConfString, ActionConfMap}.

datalayers_connector_config(Name, DatalayersHost, DatalayersPort, Config) ->
    UseTLS = proplists:get_value(use_tls, Config, false),
    ConfigString =
        io_lib:format(
            "connectors.datalayers.~s {\n"
            "  enable = true\n"
            "  server = \"~s:~b\"\n"
            "  parameters {\n"
            "    driver_type = influxdb_v1\n"
            "    database = mqtt\n"
            "    username = admin\n"
            "    password = public\n"
            "  }\n"
            "  ssl {\n"
            "    enable = ~p\n"
            "    verify = verify_none\n"
            "  }\n"
            "}\n",
            [
                Name,
                DatalayersHost,
                DatalayersPort,
                UseTLS
            ]
        ),
    {ConfigString, parse_and_check_connector(ConfigString, Name)}.

datalayers_action_config(Name, Config) ->
    BatchSize = proplists:get_value(batch_size, Config, 100),
    QueryMode = proplists:get_value(query_mode, Config, sync),
    WriteSyntax = example_write_syntax(),
    ConfigString =
        io_lib:format(
            "actions.datalayers.~s {\n"
            "  enable = true\n"
            "  connector = ~s\n"
            "  parameters {\n"
            "    write_syntax = \"~s\"\n"
            "    precision = ns\n"
            "  }\n"
            "  resource_opts = {\n"
            "    request_ttl = 15s\n"
            "    query_mode = ~s\n"
            "    batch_size = ~b\n"
            "  }\n"
            "}\n",
            [
                Name,
                Name,
                WriteSyntax,
                QueryMode,
                BatchSize
            ]
        ),
    {ConfigString, parse_and_check_action(ConfigString, Name)}.

parse_and_check_connector(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_connector_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"connectors">> := #{<<"datalayers">> := #{Name := Config}}} = RawConf,
    Config.

parse_and_check_action(ConfigString, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_v2_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"actions">> := #{<<"datalayers">> := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_bridge(Config, Overrides).

delete_all_rules() ->
    lists:foreach(
        fun(#{id := RuleId}) ->
            ok = emqx_rule_engine:delete_rule(RuleId)
        end,
        emqx_rule_engine:get_rules()
    ).

create_rule_and_action_http(Config) ->
    create_rule_and_action_http(Config, _Overrides = #{}).

create_rule_and_action_http(Config, Overrides) ->
    Name = ?config(bridge_name, Config),
    Type = ?config(bridge_type, Config),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    Params0 = #{
        enable => true,
        sql => <<"SELECT * FROM \"t/topic\"">>,
        actions => [BridgeId]
    },
    Params = emqx_utils_maps:deep_merge(Params0, Overrides),
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Type = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    emqx_bridge_v2:send_message(Type, Name, Payload, #{}).

query_by_clientid(Table, ClientId, Config) ->
    SQL = <<"SELECT * FROM ", Table/binary, " WHERE clientid = '", ClientId/binary, "'">>,
    case exec_sql_via_http_api(SQL, Config) of
        %% empty response
        {200, <<>>} ->
            #{};
        {200, Resp} ->
            case emqx_utils_json:decode(Resp, [return_maps]) of
                [] -> error(no_data);
                [FirstRow | _] -> FirstRow
            end;
        {Code, Resp} ->
            error({bad_response, Code, Resp})
    end.

exec_sql_via_http_api(SQL, Config) ->
    DatalayersHost = ?config(datalayers_host, Config),
    DatalayersPort = ?config(datalayers_port, Config),
    EHttpcPoolName = ?config(ehttpc_pool_name, Config),
    UseTLS = proplists:get_bool(use_tls, Config),
    Path = <<"/api/v1/sql?db=mqtt">>,
    Scheme =
        case UseTLS of
            true -> <<"https://">>;
            false -> <<"http://">>
        end,
    URI = iolist_to_binary([
        Scheme,
        list_to_binary(DatalayersHost),
        ":",
        integer_to_binary(DatalayersPort),
        Path
    ]),
    Headers = [
        {"Authorization", "Basic " ++ base64:encode_to_string("admin:public")},
        {"Content-Type", "application/binary"}
    ],
    ct:pal("Try to execute SQL: ~s~n", [SQL]),
    Result = ehttpc:request(
        EHttpcPoolName, post, {URI, Headers, SQL}, _Timeout = 10_000, _Retry = 0
    ),
    case Result of
        {ok, 200, RespHeaders, RespBody} ->
            ct:pal(
                "Response:~n"
                "Code: ~w~n"
                "Headers: ~p~n"
                "Body: ~p~n",
                [200, RespHeaders, RespBody]
            ),
            {200, RespBody};
        {ok, RespCode, RespHeaders} ->
            ct:pal(
                "Response:~n"
                "Code: ~w~n"
                "Headers: ~p~n",
                [RespCode, RespHeaders]
            ),
            {RespCode, <<>>};
        {error, Reason} ->
            {error, Reason}
    end.

assert_persisted_data(ClientId, Expected, PersistedData) ->
    ClientIdIntKey = <<ClientId/binary, "_int_value">>,
    maps:foreach(
        fun
            (int_value, ExpectedValue) ->
                ?assertMatch(
                    ExpectedValue,
                    maps:get(ClientIdIntKey, PersistedData)
                );
            (Key, {ExpectedValue, ExpectedType}) ->
                ?assertMatch(
                    #{<<"_value">> := ExpectedValue, '_value_type' := ExpectedType},
                    maps:get(atom_to_binary(Key), PersistedData),
                    #{
                        key => Key,
                        expected_value => ExpectedValue,
                        expected_data_type => ExpectedType
                    }
                );
            (Key, ExpectedValue) ->
                ?assertMatch(
                    ExpectedValue,
                    maps:get(atom_to_binary(Key), PersistedData),
                    #{key => Key, expected_value => ExpectedValue}
                )
        end,
        Expected
    ),
    ok.

connector_id(Config) ->
    Name = ?config(connector_name, Config),
    emqx_connector_resource:resource_id(<<"datalayers">>, Name).

ensure_database(Config) ->
    SQL = <<"create database if not exists mqtt">>,
    _Resp = exec_sql_via_http_api(SQL, Config),
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_ok(Config) ->
    Table = atom_to_binary(?FUNCTION_NAME),
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        int_key => -123,
        bool => true,
        float_key => 24.5,
        uint_key => 123
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => Table,
        <<"payload">> => Payload,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    ?check_trace(
        begin
            case QueryMode of
                async ->
                    ?assertMatch(ok, send_message(Config, SentData));
                sync ->
                    ?assertMatch({ok, 204, _}, send_message(Config, SentData))
            end,
            ct:sleep(1500),
            PersistedData = query_by_clientid(Table, ClientId, Config),
            Expected = #{
                bool => true,
                int_value => -123,
                uint_value => 123,
                float_value => 24.5,
                payload => emqx_utils_json:encode(Payload)
            },
            assert_persisted_data(ClientId, Expected, PersistedData),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(influxdb_connector_send_query, Trace0),
            ?assertMatch([#{points := [_]}], Trace),
            [#{points := [Point]}] = Trace,
            ct:pal("sent point: ~p", [Point]),
            ?assertMatch(
                #{
                    fields := #{},
                    measurement := <<_/binary>>,
                    tags := #{},
                    timestamp := TS
                } when is_integer(TS),
                Point
            ),
            #{fields := Fields} = Point,
            ?assert(lists:all(fun is_binary/1, maps:keys(Fields))),
            ?assertNot(maps:is_key(<<"undefined">>, Fields)),
            ?assertNot(maps:is_key(<<"undef_value">>, Fields)),
            ok
        end
    ),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, influxdb_client_stopped),
    ok.

t_start_already_started(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),

    ConnectorId = connector_id(Config),
    NameAtom = binary_to_atom(?config(connector_name, Config)),
    {ok, ConnectorsConf} = emqx_hocon:check(
        emqx_connector_schema, ?config(connector_config_string, Config)
    ),
    ConnConfigMap = emqx_utils_maps:deep_get([connectors, datalayers, NameAtom], ConnectorsConf),

    ?check_trace(
        emqx_bridge_datalayers_connector:on_start(ConnectorId, ConnConfigMap),
        fun(Result, Trace) ->
            ?assertMatch({ok, _}, Result),
            ?assertMatch([_], ?of_kind(influxdb_connector_start_already_started, Trace)),
            ok
        end
    ),
    ok.

t_start_ok_timestamp_write_syntax(Config) ->
    DatalayersName = ?config(bridge_name, Config),
    DatalayersConfigString0 = ?config(bridge_config_string, Config),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic},clientid=${clientid}", " ", "payload=${payload},",
            "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    %% append this to override the config
    DatalayersConfigString1 =
        io_lib:format(
            "actions.datalayers.~s {\n"
            "  parameters {\n"
            "    write_syntax = \"~s\"\n"
            "  }\n"
            "}\n",
            [DatalayersName, WriteSyntax]
        ),
    DatalayersConfig1 = parse_and_check_action(
        DatalayersConfigString0 ++ DatalayersConfigString1,
        DatalayersName
    ),
    Config1 = lists:keyreplace(
        bridge_config,
        1,
        Config,
        {bridge_config, DatalayersConfig1}
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(Config1)
    ),
    ok.

t_start_ok_no_subject_tags_write_syntax(Config) ->
    DatalayersName = ?config(bridge_name, Config),
    DatalayersConfigString0 = ?config(bridge_config_string, Config),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic}", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "${timestamp}">>,
    %% append this to override the config
    DatalayersConfigString1 =
        io_lib:format(
            "actions.datalayers.~s {\n"
            "  parameters {\n"
            "    write_syntax = \"~s\"\n"
            "  }\n"
            "}\n",
            [DatalayersName, WriteSyntax]
        ),
    DatalayersConfig1 = parse_and_check_action(
        DatalayersConfigString0 ++ DatalayersConfigString1,
        DatalayersName
    ),
    Config1 = lists:keyreplace(
        bridge_config,
        1,
        Config,
        {bridge_config, DatalayersConfig1}
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(Config1)
    ),
    ok.

t_const_timestamp(Config) ->
    QueryMode = ?config(query_mode, Config),
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(
            Config,
            #{
                <<"parameters">> => #{
                    <<"write_syntax">> =>
                        <<
                            "mqtt,clientid=${clientid} "
                            "foo=${payload.foo}i,"
                            "foo1=${payload.foo},"
                            "foo2=\"${payload.foo}\","
                            "foo3=\"${payload.foo}somestr\","
                            "bar=5i,baz0=1.1,baz1=\"a\",baz2=\"ai\",baz3=\"au\",baz4=\"1u\" ",
                            ConstBin/binary
                        >>
                }
            }
        )
    ),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{<<"foo">> => 123},
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"payload">> => Payload,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    case QueryMode of
        async ->
            ?assertMatch(ok, send_message(Config, SentData));
        sync ->
            ?assertMatch({ok, 204, _}, send_message(Config, SentData))
    end,
    ct:sleep(1500),
    PersistedData = query_by_clientid(<<"mqtt">>, ClientId, Config),
    Expected = #{
        foo => 123,
        foo1 => 123.0,
        foo2 => <<"123">>,
        foo3 => <<"123somestr">>,
        bar => 5,
        baz0 => 1.1,
        baz1 => <<"a">>,
        baz2 => <<"ai">>,
        baz3 => <<"au">>,
        baz4 => <<"1u">>
    },
    assert_persisted_data(ClientId, Expected, PersistedData),
    TimeReturned0 = maps:get(<<"time">>, PersistedData),
    TimeReturned = pad_zero(TimeReturned0),
    ?assertEqual(TsStr, TimeReturned).

%% influxdb returns timestamps without trailing zeros such as
%% "2023-02-28T17:21:51.63678163Z"
%% while the standard should be
%% "2023-02-28T17:21:51.636781630Z"
pad_zero(BinTs) ->
    StrTs = binary_to_list(BinTs),
    [Nano | Rest] = lists:reverse(string:tokens(StrTs, ".")),
    [$Z | NanoNum] = lists:reverse(Nano),
    Padding = lists:duplicate(10 - length(Nano), $0),
    NewNano = lists:reverse(NanoNum) ++ Padding ++ "Z",
    iolist_to_binary(string:join(lists:reverse([NewNano | Rest]), ".")).

t_boolean_variants(Config) ->
    Table = atom_to_binary(?FUNCTION_NAME),
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    BoolVariants = #{
        true => true,
        false => false,
        <<"t">> => true,
        <<"f">> => false,
        <<"T">> => true,
        <<"F">> => false,
        <<"TRUE">> => true,
        <<"FALSE">> => false,
        <<"True">> => true,
        <<"False">> => false
    },
    maps:foreach(
        fun(BoolVariant, Translation) ->
            ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
            Payload = #{
                int_key => -123,
                bool => BoolVariant,
                uint_key => 123
            },
            SentData = #{
                <<"clientid">> => ClientId,
                <<"topic">> => Table,
                <<"timestamp">> => erlang:system_time(millisecond),
                <<"payload">> => Payload
            },
            case QueryMode of
                sync ->
                    ?assertMatch({ok, 204, _}, send_message(Config, SentData));
                async ->
                    ?assertMatch(ok, send_message(Config, SentData))
            end,
            ct:sleep(1500),
            PersistedData = query_by_clientid(Table, ClientId, Config),
            Expected = #{
                bool => Translation,
                int_value => -123,
                uint_value => 123,
                payload => emqx_utils_json:encode(Payload)
            },
            assert_persisted_data(ClientId, Expected, PersistedData),
            ok
        end,
        BoolVariants
    ),
    ok.

t_any_num_as_float(Config) ->
    QueryMode = ?config(query_mode, Config),
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(
            Config,
            #{
                <<"parameters">> => #{
                    <<"write_syntax">> =>
                        <<
                            "mqtt,clientid=${clientid}",
                            " ",
                            "float_no_dp=${payload.float_no_dp},"
                            "float_dp=${payload.float_dp},bar=5i ",
                            ConstBin/binary
                        >>
                }
            }
        )
    ),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        %% no decimal point
        float_no_dp => 123,
        %% with decimal point
        float_dp => 123.0
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"payload">> => Payload,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    case QueryMode of
        sync ->
            ?assertMatch({ok, 204, _}, send_message(Config, SentData)),
            ok;
        async ->
            ?assertMatch(ok, send_message(Config, SentData))
    end,
    %% sleep is still need even in sync mode, or we would get an empty result sometimes
    ct:sleep(1500),
    PersistedData = query_by_clientid(<<"mqtt">>, ClientId, Config),
    Expected = #{float_no_dp => 123.0, float_dp => 123.0},
    assert_persisted_data(ClientId, Expected, PersistedData),
    TimeReturned0 = maps:get(<<"time">>, PersistedData),
    TimeReturned = pad_zero(TimeReturned0),
    ?assertEqual(TsStr, TimeReturned).

t_tag_set_use_literal_value(Config) ->
    QueryMode = ?config(query_mode, Config),
    Const = erlang:system_time(nanosecond),
    ConstBin = integer_to_binary(Const),
    TsStr = iolist_to_binary(
        calendar:system_time_to_rfc3339(Const, [{unit, nanosecond}, {offset, "Z"}])
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(
            Config,
            #{
                <<"parameters">> => #{
                    <<"write_syntax">> =>
                        <<
                            "mqtt,clientid=${clientid},"
                            "tag_key1=100,tag_key2=123.4,"
                            "tag_key3=66i,tag_key4=${payload.float_dp}",
                            " ",
                            "field_key1=100.1,field_key2=100i,field_key3=${payload.float_dp},bar=5i",
                            " ",
                            ConstBin/binary
                        >>
                }
            }
        )
    ),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        %% with decimal point
        float_dp => 123.4
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"payload">> => Payload,
        <<"timestamp">> => erlang:system_time(millisecond)
    },
    case QueryMode of
        sync ->
            ?assertMatch({ok, 204, _}, send_message(Config, SentData)),
            ok;
        async ->
            ?assertMatch(ok, send_message(Config, SentData))
    end,
    %% sleep is still need even in sync mode, or we would get an empty result sometimes
    ct:sleep(1500),
    PersistedData = query_by_clientid(<<"mqtt">>, ClientId, Config),
    Expected = #{field_key1 => 100.1, field_key2 => 100, field_key3 => 123.4},
    assert_persisted_data(ClientId, Expected, PersistedData),
    TimeReturned0 = maps:get(<<"time">>, PersistedData),
    TimeReturned = pad_zero(TimeReturned0),
    ?assertEqual(TsStr, TimeReturned).

t_bad_timestamp(Config) ->
    ActionName = ?config(bridge_name, Config),
    QueryMode = ?config(query_mode, Config),
    BatchSize = ?config(batch_size, Config),
    ActionConfigString0 = ?config(bridge_config_string, Config),
    WriteSyntax =
        %% N.B.: this single space characters are relevant
        <<"${topic}", " ", "payload=${payload},", "${clientid}_int_value=${payload.int_key}i,",
            "uint_value=${payload.uint_key}u,"
            "bool=${payload.bool}", " ", "bad_timestamp">>,
    %% append this to override the config
    ActionConfigString1 =
        io_lib:format(
            "actions.datalayers.~s {\n"
            "  parameters {\n"
            "    write_syntax = \"~s\"\n"
            "  }\n"
            "}\n",
            [ActionName, WriteSyntax]
        ),
    ActionConfig1 = parse_and_check_action(
        ActionConfigString0 ++ ActionConfigString1,
        ActionName
    ),
    Config1 = lists:keystore(
        bridge_config,
        1,
        Config,
        {bridge_config, ActionConfig1}
    ),
    ?assertMatch(
        {ok, _},
        create_bridge(Config1)
    ),

    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        int_key => -123,
        bool => false,
        uint_key => 123
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"timestamp">> => erlang:system_time(millisecond),
        <<"payload">> => Payload
    },
    ?check_trace(
        ?wait_async_action(
            send_message(Config1, SentData),
            #{?snk_kind := influxdb_connector_send_query_error},
            10_000
        ),
        fun(Result, Trace) ->
            ?assertMatch({_, {ok, _}}, Result),
            {Return, {ok, _}} = Result,
            IsBatch = BatchSize > 1,
            case {QueryMode, IsBatch} of
                {async, true} ->
                    ?assertEqual(ok, Return),
                    ?assertMatch(
                        [#{error := points_trans_failed}],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                {async, false} ->
                    ?assertEqual(ok, Return),
                    ?assertMatch(
                        [
                            #{
                                error := [
                                    {error,
                                        {bad_timestamp,
                                            {non_integer_timestamp, <<"bad_timestamp">>}}}
                                ]
                            }
                        ],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                {sync, false} ->
                    ?assertEqual(
                        {error, [
                            {error, {bad_timestamp, {non_integer_timestamp, <<"bad_timestamp">>}}}
                        ]},
                        Return
                    );
                {sync, true} ->
                    ?assertEqual({error, {unrecoverable_error, points_trans_failed}}, Return)
            end,
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    {ok, _} = create_bridge(Config),
    ConnectorId = connector_id(Config),
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ConnectorId)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ConnectorId))
    end),
    ok.

t_create_disconnected(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    ?check_trace(
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            ?assertMatch({ok, _}, create_bridge(Config))
        end),
        fun(Trace) ->
            [#{error := influxdb_client_not_alive, reason := Reason}] =
                ?of_kind(influxdb_connector_start_failed, Trace),
            case Reason of
                econnrefused -> ok;
                closed -> ok;
                {closed, _} -> ok;
                {shutdown, closed} -> ok;
                _ -> ct:fail("influxdb_client_not_alive with wrong reason: ~p", [Reason])
            end,
            ok
        end
    ),
    ok.

t_start_error(Config) ->
    %% simulate client start error
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            influxdb,
            start_client,
            fun(_Config) -> {error, some_error} end,
            fun() ->
                ?wait_async_action(
                    ?assertMatch({ok, _}, create_bridge(Config)),
                    #{?snk_kind := influxdb_connector_start_failed},
                    10_000
                )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := some_error}],
                ?of_kind(influxdb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_start_exception(Config) ->
    %% simulate client start exception
    ?check_trace(
        emqx_common_test_helpers:with_mock(
            influxdb,
            start_client,
            fun(_Config) -> error(boom) end,
            fun() ->
                ?wait_async_action(
                    ?assertMatch({ok, _}, create_bridge(Config)),
                    #{?snk_kind := influxdb_connector_start_exception},
                    10_000
                )
            end
        ),
        fun(Trace) ->
            ?assertMatch(
                [#{error := {error, boom}}],
                ?of_kind(influxdb_connector_start_exception, Trace)
            ),
            ok
        end
    ),
    ok.

t_write_failure(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    QueryMode = ?config(query_mode, Config),
    {ok, _} = create_bridge(Config),
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        int_key => -123,
        bool => true,
        float_key => 24.5,
        uint_key => 123
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"timestamp">> => erlang:system_time(millisecond),
        <<"payload">> => Payload
    },
    ?check_trace(
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            case QueryMode of
                sync ->
                    {_, {ok, _}} =
                        ?wait_async_action(
                            ?assertMatch(
                                {error, {resource_error, #{reason := timeout}}},
                                send_message(Config, SentData)
                            ),
                            #{?snk_kind := handle_async_reply, action := nack},
                            1_000
                        );
                async ->
                    ?wait_async_action(
                        ?assertEqual(ok, send_message(Config, SentData)),
                        #{?snk_kind := handle_async_reply},
                        1_000
                    )
            end
        end),
        fun(Trace0) ->
            case QueryMode of
                sync ->
                    Trace = ?of_kind(handle_async_reply, Trace0),
                    ?assertMatch([_ | _], Trace),
                    [#{result := Result} | _] = Trace,
                    ?assert(
                        not emqx_bridge_influxdb_connector:is_unrecoverable_error(Result),
                        #{got => Result}
                    );
                async ->
                    Trace = ?of_kind(handle_async_reply, Trace0),
                    ?assertMatch([#{action := nack} | _], Trace),
                    [#{result := Result} | _] = Trace,
                    ?assert(
                        not emqx_bridge_influxdb_connector:is_unrecoverable_error(Result),
                        #{got => Result}
                    )
            end,
            ok
        end
    ),
    ok.

t_missing_field(Config) ->
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    {ok, _} =
        create_bridge(
            Config,
            #{
                <<"parameters">> => #{
                    <<"write_syntax">> => <<"${clientid} foo=${foo}i">>
                },
                <<"resource_opts">> => #{<<"worker_pool_size">> => 1}
            }
        ),
    %% note: we don't select foo here, but we interpolate it in the
    %% fields, so it'll become undefined.
    {ok, _} = create_rule_and_action_http(Config, #{sql => <<"select * from \"t/topic\"">>}),
    ClientId0 = emqx_guid:to_hexstr(emqx_guid:gen()),
    ClientId1 = emqx_guid:to_hexstr(emqx_guid:gen()),
    %% Message with the field that we "forgot" to select in the rule
    Msg0 = emqx_message:make(ClientId0, <<"t/topic">>, emqx_utils_json:encode(#{foo => 123})),
    %% Message without any fields
    Msg1 = emqx_message:make(ClientId1, <<"t/topic">>, emqx_utils_json:encode(#{})),
    ?check_trace(
        begin
            emqx:publish(Msg0),
            emqx:publish(Msg1),
            NEvents = 1,
            {ok, _} =
                snabbkaffe:block_until(
                    ?match_n_events(NEvents, #{
                        ?snk_kind := influxdb_connector_send_query_error
                    }),
                    _Timeout1 = 10_000
                ),
            ok
        end,
        fun(Trace) ->
            PersistedData0 = query_by_clientid(<<"mqtt">>, ClientId0, Config),
            PersistedData1 = query_by_clientid(<<"mqtt">>, ClientId1, Config),
            case IsBatch of
                true ->
                    ?assertMatch(
                        [#{error := points_trans_failed} | _],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    );
                false ->
                    ?assertMatch(
                        [#{error := [{error, no_fields}]} | _],
                        ?of_kind(influxdb_connector_send_query_error, Trace)
                    )
            end,
            %% nothing should have been persisted
            ?assertEqual(#{}, PersistedData0),
            ?assertEqual(#{}, PersistedData1),
            ok
        end
    ),
    ok.

t_authentication_error(Config0) ->
    DatalayersConfig0 = proplists:get_value(connector_config, Config0),
    DatalayersConfig = emqx_utils_maps:deep_merge(
        DatalayersConfig0,
        #{<<"parameters">> => #{<<"password">> => <<"wrong_password">>}}
    ),
    Config = lists:keyreplace(connector_config, 1, Config0, {connector_config, DatalayersConfig}),
    ?check_trace(
        begin
            ?wait_async_action(
                create_bridge(Config),
                #{?snk_kind := influxdb_connector_start_failed},
                10_000
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := auth_error} | _],
                ?of_kind(influxdb_connector_start_failed, Trace)
            ),
            ok
        end
    ),
    ok.

t_authentication_error_on_get_status(Config0) ->
    % Fake initialization to simulate credential update after bridge was created.
    ResourceId = emqx_common_test_helpers:with_mock(
        influxdb,
        check_auth,
        fun(_) ->
            ok
        end,
        fun() ->
            DatalayersConfig0 = proplists:get_value(connector_config, Config0),
            DatalayersConfig = emqx_utils_maps:deep_merge(
                DatalayersConfig0,
                #{<<"parameters">> => #{<<"password">> => <<"wrong_password">>}}
            ),
            Config = lists:keyreplace(
                connector_config, 1, Config0, {connector_config, DatalayersConfig}
            ),
            {ok, _} = create_bridge(Config),
            ResourceId = connector_id(Config0),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 10,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            ),
            ResourceId
        end
    ),
    % Now back to wrong credentials
    ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId)),
    ok.

t_authentication_error_on_send_message(Config0) ->
    QueryMode = proplists:get_value(query_mode, Config0, sync),
    DatalayersConfig0 = proplists:get_value(connector_config, Config0),
    DatalayersConfig = emqx_utils_maps:deep_merge(
        DatalayersConfig0,
        #{<<"parameters">> => #{<<"password">> => <<"wrong_password">>}}
    ),
    Config = lists:keyreplace(connector_config, 1, Config0, {connector_config, DatalayersConfig}),

    % Fake initialization to simulate credential update after bridge was created.
    emqx_common_test_helpers:with_mock(
        influxdb,
        check_auth,
        fun(_) ->
            ok
        end,
        fun() ->
            {ok, _} = create_bridge(Config),
            ResourceId = connector_id(Config),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 10,
                ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
            )
        end
    ),

    % Now back to wrong credentials
    ClientId = emqx_guid:to_hexstr(emqx_guid:gen()),
    Payload = #{
        int_key => -123,
        bool => true,
        float_key => 24.5,
        uint_key => 123
    },
    SentData = #{
        <<"clientid">> => ClientId,
        <<"topic">> => atom_to_binary(?FUNCTION_NAME),
        <<"timestamp">> => erlang:system_time(millisecond),
        <<"payload">> => Payload
    },
    case QueryMode of
        sync ->
            ?assertMatch(
                {error, {unrecoverable_error, <<"authorization failure">>}},
                send_message(Config, SentData)
            );
        async ->
            ?check_trace(
                begin
                    ?wait_async_action(
                        ?assertEqual(ok, send_message(Config, SentData)),
                        #{?snk_kind := handle_async_reply},
                        1_000
                    )
                end,
                fun(Trace) ->
                    ?assertMatch(
                        [#{error := <<"authorization failure">>} | _],
                        ?of_kind(influxdb_connector_do_query_failure, Trace)
                    ),
                    ok
                end
            )
    end,
    ok.
