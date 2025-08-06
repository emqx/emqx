%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_arrow_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-define(create_database, <<"CREATE DATABASE IF NOT EXISTS mqtt_arrow">>).

%% erlfmt-ignore
-define(create_table, <<"
    CREATE TABLE IF NOT EXISTS mqtt_arrow.common_test (
        ts TIMESTAMP(3) NOT NULL,
        msgid STRING NOT NULL,
        clientid STRING NOT NULL,
        topic STRING NOT NULL,
        qos INT8 NOT NULL,
        payload STRING NOT NULL,
        timestamp key(ts)
    )
    PARTITION BY HASH(msgid) PARTITIONS 8
    ENGINE=TimeSeries;
">>).

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
            {group, arrow_tcp},
            {group, arrow_tls}
        ]},
        {async_query, [
            {group, arrow_tcp},
            {group, arrow_tls}
        ]},
        {arrow_tcp, TCs},
        {arrow_tls, TCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(GroupName, Config0) when
    GroupName =:= arrow_tcp;
    GroupName =:= arrow_tls
->
    #{
        host := DatalayersHost,
        port := DatalayersPort,
        use_tls := UseTLS,
        proxy_name := ProxyName
    } =
        case GroupName of
            arrow_tcp ->
                #{
                    host => os:getenv("DATALAYERS_GRCP_HOST", "toxiproxy"),
                    port => list_to_integer(os:getenv("DATALAYERS_GRPC_PORT", "8360")),
                    use_tls => false,
                    proxy_name => "datalayers_gprc"
                };
            arrow_tls ->
                #{
                    host => os:getenv("DATALAYERS_GRPCS_HOST", "toxiproxy"),
                    port => list_to_integer(os:getenv("DATALAYERS_GRPCS_PORT", "8362")),
                    use_tls => true,
                    proxy_name => "datalayers_grpcs"
                }
        end,
    ct:pal(
        io_lib:format(
            "Running group ~s with Datalayers at ~s:~b, TLS enabled: ~p, Proxy name: ~s",
            [
                GroupName,
                DatalayersHost,
                DatalayersPort,
                UseTLS,
                ProxyName
            ]
        )
    ),
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
            NewConfig =
                [
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort},
                    {proxy_name, ProxyName},

                    {datalayers_host, DatalayersHost},
                    {datalayers_port, DatalayersPort},

                    {bridge_kind, action},
                    {bridge_type, datalayers},
                    {bridge_name, Name},
                    {bridge_config, ActionConfMap},
                    {bridge_config_string, ActionConfString},
                    {action_type, datalayers},
                    {action_name, Name},
                    {action_config, ActionConfMap},

                    {connector_name, Name},
                    {connector_type, datalayers},
                    {connector_config, ConnConfMap},
                    {connector_config_string, ConnConfString}
                    | Config
                ],
            ensure_database(NewConfig),
            ensure_table(NewConfig),
            NewConfig;
        false ->
            {skip, no_datalayers_arrow}
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

end_per_group(GroupName, Config) when
    GroupName =:= arrow_tcp;
    GroupName =:= arrow_tls
->
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(Testcase, Config) when
    Testcase =/= t_start_ok
->
    init_per_testcase(Config);
init_per_testcase(_Testcase, Config) ->
    init_per_testcase(Config).

init_per_testcase(Config) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

datalayers_config(DatalayersHost, DatalayersPort, Config) ->
    Name = atom_to_binary(?MODULE),
    {ConnConfString, ConnConfMap} = datalayers_connector_config(
        Name, DatalayersHost, DatalayersPort, Config
    ),
    {ActionConfString, ActionConfMap} = datalayers_action_config(Name, Config),
    {Name, ConnConfString, ConnConfMap, ActionConfString, ActionConfMap}.

datalayers_connector_config(Name, DatalayersHost, DatalayersPort, Config) ->
    UseTLS = proplists:get_value(use_tls, Config, false),
    Dir = code:lib_dir(emqx_bridge_datalayers),
    Cacertfile = filename:join([Dir, <<"test/data/certs">>, <<"ca.crt">>]),
    ConfigString =
        io_lib:format(
            "connectors.datalayers.~s {\n"
            "  enable = true\n"
            "  server = \"~s:~b\"\n"
            "  parameters {\n"
            "    driver_type = arrow_flight\n"
            "    database = mqtt\n"
            "    username = admin\n"
            "    password = public\n"
            "  }\n"
            "  ssl {\n"
            "    enable = ~p\n"
            "    verify = verify_peer\n"
            "    cacertfile = \"~s\"\n"
            "  }\n"
            "}\n",
            [
                Name,
                DatalayersHost,
                DatalayersPort,
                UseTLS,
                Cacertfile
            ]
        ),
    {ConfigString, parse_and_check_connector(ConfigString, Name)}.

datalayers_action_config(Name, Config) ->
    BatchSize = proplists:get_value(batch_size, Config, 100),
    QueryMode = proplists:get_value(query_mode, Config, sync),
    SQL = sql_template(),
    ConfigString =
        io_lib:format(
            "actions.datalayers.~s {\n"
            "  enable = true\n"
            "  connector = ~s\n"
            "  parameters {\n"
            "    sql = \"~s\"\n"
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
                SQL,
                QueryMode,
                BatchSize
            ]
        ),
    {ConfigString, parse_and_check_action(ConfigString, Name)}.

sql_template() ->
    <<
        "INSERT INTO mqtt_arrow.common_test(ts, msgid, clientid, topic, qos, payload) VALUES "
        "(${timestamp}, ${id}, ${clientid}, ${topic}, ${qos}, ${payload})"
    >>.

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
    emqx_bridge_v2_testlib:create_bridge_api(Config, Overrides).

query_resource(Config, Request) ->
    Type = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    BridgeV2Id = id(Type, Name),
    ConnectorResId = emqx_connector_resource:resource_id(Type, Name),
    emqx_resource:query(BridgeV2Id, Request, #{
        timeout => 1_000, connector_resource_id => ConnectorResId
    }).

query_resource_async(Config, Request) ->
    Type = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    Ref = alias([reply]),
    AsyncReplyFun = fun(#{result := Result}) -> Ref ! {result, Ref, Result} end,
    BridgeV2Id = id(Type, Name),
    ConnectorResId = emqx_connector_resource:resource_id(Type, Name),
    Return = emqx_resource:query(BridgeV2Id, Request, #{
        timeout => 500,
        async_reply_fun => {AsyncReplyFun, []},
        connector_resource_id => ConnectorResId,
        query_mode => async
    }),
    {Return, Ref}.

receive_result(Ref, Timeout) when is_reference(Ref) ->
    receive
        {result, Ref, Result} ->
            {ok, Result};
        {Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

query_by_clientid(ClientId, Config) ->
    SQL = io_lib:format("SELECT * FROM mqtt_arrow.common_test WHERE clientid = '~s'", [ClientId]),
    case exec_sql(bin(SQL), Config) of
        {ok, Rows} ->
            Rows;
        {error, Reason} ->
            error({bad_response, Reason})
    end.

assert_persisted_data(Expected, [PersistedData | _]) ->
    ?assertEqual(Expected, PersistedData).

connector_id(Config) ->
    Name = ?config(connector_name, Config),
    emqx_connector_resource:resource_id(<<"datalayers">>, Name).

ensure_database(Config) ->
    exec_sql(?create_database, Config).

ensure_table(Config) ->
    exec_sql(?create_table, Config).

id(Type, Name) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(#{
        kind => action,
        type => Type,
        name => Name
    }).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_ok(Config) ->
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ClientId = random_clientid_(),
    Topic = <<"t/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    Payload = #{msg => <<"Hello, Arrow Flight SQL!">>},
    #{id := Id, timestamp := TimeStamp} = Msg = make_message(ClientId, Topic, Payload),

    Request = {emqx_bridge_v2_testlib:bridge_id(Config), Msg},
    case QueryMode of
        sync ->
            query_resource(Config, Request);
        async ->
            {_, Ref} = query_resource_async(Config, Request),
            {ok, Res} = receive_result(Ref, 2_000),
            Res
    end,
    ct:sleep(1500),
    PersistedData = query_by_clientid(ClientId, Config),

    Expected = [
        _TsStr = iolist_to_binary(
            calendar:system_time_to_rfc3339(TimeStamp, [{unit, millisecond}, {offset, "Z"}])
        ),
        Id,
        ClientId,
        Topic,
        _Qos = <<"0">>,
        emqx_utils_json:encode(Payload)
    ],
    assert_persisted_data(Expected, PersistedData),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "arrow_flight_sql_client_stopped").

t_get_status(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    {ok, _} = create_bridge(Config),
    ConnectorId = emqx_connector_resource:resource_id(datalayers, ?config(connector_name, Config)),
    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ConnectorId)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ConnectorId))
    end),
    ok.

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

exec_sql(SQL, Config) ->
    Host = ?config(datalayers_host, Config),
    Port = ?config(datalayers_port, Config),
    ConnConfig0 = #{
        host => bin(Host),
        port => Port,
        username => <<"admin">>,
        password => <<"public">>
    },
    Dir = code:lib_dir(emqx_bridge_datalayers),
    Cacertfile = filename:join([Dir, <<"test/data/certs">>, <<"ca.crt">>]),
    ConnConfig =
        case ?config(use_tls, Config) of
            true ->
                ConnConfig0#{tls_cert => Cacertfile};
            false ->
                ConnConfig0
        end,
    {ok, Conn} = datalayers:connect(ConnConfig),
    Res = datalayers:execute(Conn, bin(SQL)),
    ok = datalayers:stop(Conn),
    Res.

bin(L) when is_list(L) ->
    list_to_binary(L);
bin(A) when is_atom(A) ->
    atom_to_binary(A);
bin(B) when is_binary(B) ->
    B.

random_clientid_() ->
    <<"mqtt_", (emqx_guid:to_hexstr(emqx_guid:gen()))/binary>>.

make_message(ClientId, Topic, Payload) ->
    Message = emqx_message:make(ClientId, Topic, Payload),
    Id = emqx_guid:to_hexstr(emqx_guid:gen()),
    From = emqx_message:from(Message),
    Msg = emqx_message:to_map(Message),
    Msg#{id => Id, clientid => From}.
