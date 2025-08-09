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
-include_lib("emqx/include/emqx_mqtt.hrl").

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

%% erlfmt-ignore
-define(connector_config,
"""
enable = true
server = "~s:~b"
parameters {
  driver_type = arrow_flight
  database = mqtt
  username = admin
  password = public
}
ssl {
  enable = ~p
  verify = verify_peer
  cacertfile = "~s"
}
"""
).

datalayers_connector_config(Name, DatalayersHost, DatalayersPort, Config) ->
    UseTLS = proplists:get_value(use_tls, Config, false),
    ConfigString =
        io_lib:format(
            ?connector_config,
            [
                DatalayersHost,
                DatalayersPort,
                UseTLS,
                cacert_file()
            ]
        ),
    {ok, RawConf} = hocon:binary(ConfigString),
    Checked = emqx_bridge_v2_testlib:parse_and_check_connector(
        <<"datalayers">>,
        Name,
        RawConf
    ),
    {ConfigString, Checked}.

%% erlfmt-ignore
-define(action_config,
"""
enable = true
connector = ~s
parameters {
  sql = "~s"
}
resource_opts = {
  request_ttl = 15s
  query_mode = ~s
  batch_size = ~b
}
"""
).

datalayers_action_config(Name, Config) ->
    BatchSize = proplists:get_value(batch_size, Config, 100),
    QueryMode = proplists:get_value(query_mode, Config, sync),
    SQL = sql_template(),
    ConfigString =
        io_lib:format(
            ?action_config,
            [
                Name,
                SQL,
                QueryMode,
                BatchSize
            ]
        ),
    {ok, RawConf} = hocon:binary(ConfigString),
    Checked = emqx_bridge_v2_testlib:parse_and_check(
        <<"datalayers">>,
        Name,
        RawConf
    ),
    {ConfigString, Checked}.

sql_template() ->
    <<
        "INSERT INTO mqtt_arrow.common_test(ts, msgid, clientid, topic, qos, payload) VALUES "
        "(${timestamp}, ${id}, ${clientid}, ${topic}, ${qos}, ${payload})"
    >>.

query_by_clientid(ClientId, Config) ->
    SQL = io_lib:format("SELECT * FROM mqtt_arrow.common_test WHERE clientid = '~s'", [ClientId]),
    case exec_sql(bin(SQL), Config) of
        {ok, Rows} ->
            Rows;
        {error, Reason} ->
            error({bad_response, Reason})
    end.

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

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "arrow_flight_sql_client_stopped").

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config),
    ok.

t_start_action_or_source_with_disabled_connector(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(Config),
    ok.

t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, _}, Res) end,
        datalayers_arrow_flight_connector_on_query
    ),
    ok.

t_async_query(Config) ->
    case ?config(query_mode, Config) of
        async ->
            TracePoint =
                case ?config(batch_size, Config) of
                    1 -> datalayers_arrow_flight_connector_on_query_async;
                    _ -> datalayers_arrow_flight_connector_on_batch_query_async
                end,
            ok = emqx_bridge_v2_testlib:t_async_query(
                Config,
                fun make_message/0,
                fun(Res) -> ?assertMatch({ok, _}, Res) end,
                TracePoint
            );
        sync ->
            ok
    end,
    ok.

t_rule_action(Config) ->
    RandomClientId = random_clientid_(),
    PyloadFn = fun() -> emqx_guid:to_hexstr(emqx_guid:gen()) end,
    RandomTopic = <<"/", (atom_to_binary(?FUNCTION_NAME))/binary, (random_topic_suffix())/binary>>,
    RandomQoS = random_qos(),
    PublishFn =
        fun(#{rule_topic := RuleTopic, payload_fn := RandomPayloadFnIn} = Context) ->
            RandomPayload = RandomPayloadFnIn(),
            {ok, C} = emqtt:start_link(#{clean_start => true, clientid => RandomClientId}),
            {ok, _} = emqtt:connect(C),
            case RandomQoS of
                0 ->
                    ?assertMatch(
                        ok, emqtt:publish(C, RuleTopic, RandomPayload, [{qos, RandomQoS}])
                    );
                _ ->
                    ?assertMatch(
                        {ok, _}, emqtt:publish(C, RuleTopic, RandomPayload, [{qos, RandomQoS}])
                    )
            end,

            ok = emqtt:stop(C),
            Context#{payload => RandomPayload}
        end,

    PostPublishFn = fun(#{payload := Payload, rule_topic := Topic} = _Context) ->
        RandomClientIdIn = RandomClientId,
        QoSBin = integer_to_binary(RandomQoS),
        PayloadIn = Payload,
        ?assertMatch(
            [
                [
                    %% timestamp and msg are generated by emqx rule engine
                    _TsStr,
                    _Id,
                    RandomClientIdIn,
                    Topic,
                    QoSBin,
                    PayloadIn
                ]
            ],
            query_by_clientid(RandomClientId, Config)
        )
    end,
    Opts = #{
        rule_topic => RandomTopic,
        payload_fn => PyloadFn,
        publish_fn => PublishFn,
        post_publish_fn => PostPublishFn
    },

    emqx_bridge_v2_testlib:t_rule_action(Config, Opts),
    ok.

exec_sql(SQL, Config) ->
    Host = ?config(datalayers_host, Config),
    Port = ?config(datalayers_port, Config),
    ConnConfig0 = #{
        host => bin(Host),
        port => Port,
        username => <<"admin">>,
        password => <<"public">>
    },
    ConnConfig =
        case ?config(use_tls, Config) of
            true ->
                ConnConfig0#{tls_cert => cacert_file()};
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

random_topic_suffix() ->
    emqx_guid:to_hexstr(emqx_guid:gen()).

random_qos() ->
    %% rand:uniform(N) -> X generated random integer `X`, which: 1 =< X =< N
    rand:uniform(?QOS_2 + 1) - 1.

make_message() ->
    ClientId = random_clientid_(),
    Topic = <<"t/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    Payload = #{msg => <<"Hello, Arrow Flight SQL!">>},
    make_message(ClientId, Topic, Payload).

make_message(ClientId, Topic, Payload) ->
    Message = emqx_message:make(ClientId, Topic, Payload),
    Id = emqx_guid:to_hexstr(emqx_guid:gen()),
    From = emqx_message:from(Message),
    Msg = emqx_message:to_map(Message),
    Msg#{id => Id, clientid => From}.

cacert_file() ->
    %% XXX:
    %% in CI, same as `.ci/docker-compose-file/certs/ca.crt`
    Dir = code:lib_dir(emqx_bridge_datalayers),
    filename:join([Dir, <<"test/emqx_bridge_datalayers_SUITE_data">>, <<"ca.crt">>]).
