%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_datalayers_arrow_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
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

-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
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
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(GroupName, TCConfig) when
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
    [
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ProxyName},

        {use_tls, UseTLS},
        {datalayers_host, DatalayersHost},
        {datalayers_port, DatalayersPort}
        | TCConfig
    ];
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

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_TestCase, TCConfig0) ->
    reset_proxy(),
    DatalayersHost = get_config(datalayers_host, TCConfig0, "toxiproxy"),
    DatalayersPort = get_config(datalayers_port, TCConfig0, 8360),
    {Name, ConnConfString, ConnConfMap, ActionConfString, ActionConfMap} = datalayers_config(
        DatalayersHost, DatalayersPort, TCConfig0
    ),
    TCConfig =
        [
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
            | TCConfig0
        ],
    {ok, _} = ensure_database(TCConfig),
    {ok, _} = ensure_table(TCConfig),
    TCConfig.

end_per_testcase(_TestCase, TCConfig) ->
    ok = snabbkaffe:stop(),
    reset_proxy(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    clear_table(TCConfig),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

fmt(Fmt, Ctx) -> emqx_bridge_v2_testlib:fmt(Fmt, Ctx).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

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
  database = mqtt_arrow
  username = admin
  password = public
}
ssl {
  enable = ~p
  verify = verify_peer
  server_name_indication = "datalayers_tls"
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

clear_table(TCConfig) ->
    exec_sql(~"delete from mqtt_arrow.common_test", TCConfig).

id(Type, Name) ->
    emqx_bridge_v2_testlib:lookup_chan_id_in_conf(#{
        kind => action,
        type => Type,
        name => Name
    }).

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

connector_resource_id(TCConfig) ->
    emqx_bridge_v2_testlib:connector_resource_id(TCConfig).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

delete_action_api(TCConfig) ->
    #{kind := Kind, type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:delete_kind_api(Kind, Type, Name).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

exec_sql(SQL, Config) ->
    Host = get_config(datalayers_host, Config, "toxiproxy"),
    Port = get_config(datalayers_port, Config, 8360),
    ConnConfig0 = #{
        host => bin(Host),
        port => Port,
        username => <<"admin">>,
        password => <<"public">>
    },
    ConnConfig = tls_conn_opts(Config, ConnConfig0),
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

tls_conn_opts(Config, ConnConfig) ->
    case get_config(use_tls, Config, false) of
        true ->
            ConnConfig#{
                tls_enable => true,
                tls_cacertfile => bin(cacert_file()),
                verify => verify_peer,
                tls_server_name => <<"datalayers_tls">>
            };
        false ->
            ConnConfig
    end.

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

%% The set the docker-compose TLS services use, generated by
%% `scripts/ct/gen-compose-certs.sh' before the services start.
%% N.B.: in dev-70, this must point to `.ci/docker-compose-file/certs/cacert.pem`
cacert_file() ->
    %% A binary: the datalayers NIF rejects a string here.
    Dir = code:lib_dir(emqx_bridge_datalayers),
    iolist_to_binary(
        filename:join([Dir, <<"test/emqx_bridge_datalayers_SUITE_data">>, <<"ca.crt">>])
    ).

full_matrix() ->
    [
        [Transport, Sync, Batch]
     || Transport <- [arrow_tcp, arrow_tls],
        Sync <- [sync_query, async_query],
        Batch <- [with_batch, without_batch]
    ].

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    full_matrix();
t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, "arrow_flight_sql_client_stopped").

t_create_via_http() ->
    [{matrix, true}].
t_create_via_http(matrix) ->
    full_matrix();
t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    full_matrix();
t_on_get_status(Config) ->
    %% Shorten health_check_interval so recovery fits the 20s retry budget
    %% on the slow arrow_tls variant (TLS handshake + arrow-flight setup).
    ResourceOpts = #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}},
    Config1 = [
        {connector_overrides, ResourceOpts},
        {action_overrides, ResourceOpts}
        | Config
    ],
    emqx_bridge_v2_testlib:t_on_get_status(Config1),
    ok.

t_reconnect_and_write() ->
    [{matrix, true}].
t_reconnect_and_write(matrix) ->
    full_matrix();
t_reconnect_and_write(Config) ->
    ResourceOpts = #{<<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}},
    Config1 = [
        {connector_overrides, ResourceOpts},
        {action_overrides, ResourceOpts}
        | Config
    ],
    ConnOverrides = proplists:get_value(connector_overrides, Config1, #{}),
    KindOverrides = proplists:get_value(action_overrides, Config1, #{}),
    ProxyHost = ?config(proxy_host, Config1),
    ProxyPort = ?config(proxy_port, Config1),
    ProxyName = ?config(proxy_name, Config1),
    ?assertMatch({201, _}, emqx_bridge_v2_testlib:create_connector_api2(Config1, ConnOverrides)),
    ?assertMatch({201, _}, emqx_bridge_v2_testlib:create_action_api2(Config1, KindOverrides)),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config1),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(Config1),
    ?retry(
        _Sleep1 = 200,
        _Attempts1 = 100,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    {ok, _} = emqx_resource:simple_sync_query(ResourceId, {BridgeId, make_message()}),
    %% Simulate Datalayers restart
    emqx_common_test_helpers:enable_failure(down, ProxyName, ProxyHost, ProxyPort),
    ?retry(
        _Sleep2 = 100,
        _Attempts2 = 20,
        ?assertEqual({ok, disconnected}, emqx_resource_manager:health_check(ResourceId))
    ),
    emqx_common_test_helpers:heal_failure(down, ProxyName, ProxyHost, ProxyPort),
    ?retry(
        _Sleep3 = 200,
        _Attempts3 = 100,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    {ok, _} = emqx_resource:simple_sync_query(ResourceId, {BridgeId, make_message()}),
    ok.

t_start_action_or_source_with_disabled_connector() ->
    [{matrix, true}].
t_start_action_or_source_with_disabled_connector(matrix) ->
    full_matrix();
t_start_action_or_source_with_disabled_connector(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(Config),
    ok.

t_rule_test_trace() ->
    [{matrix, true}].
t_rule_test_trace(matrix) ->
    full_matrix();
t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

t_sync_query() ->
    [{matrix, true}].
t_sync_query(matrix) ->
    [
        [Transport, sync_query, Batch]
     || Transport <- [arrow_tcp, arrow_tls],
        Batch <- [with_batch, without_batch]
    ];
t_sync_query(Config) ->
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config,
        fun make_message/0,
        fun(Res) -> ?assertMatch({ok, _}, Res) end,
        datalayers_arrow_flight_connector_on_query
    ),
    ok.

t_async_query() ->
    [{matrix, true}].
t_async_query(matrix) ->
    [
        [Transport, async_query, Batch]
     || Transport <- [arrow_tcp, arrow_tls],
        Batch <- [with_batch, without_batch]
    ];
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

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    full_matrix();
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
        ?retry(
            200,
            10,
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

t_prepare_auto_rebuild() ->
    [{matrix, true}].
t_prepare_auto_rebuild(matrix) ->
    full_matrix();
t_prepare_auto_rebuild(Config) ->
    Host = ?config(datalayers_host, Config),
    Port = ?config(datalayers_port, Config),
    ConnConfig0 = #{
        host => bin(Host),
        port => Port,
        username => <<"admin">>,
        password => <<"public">>
    },
    ConnConfig = tls_conn_opts(Config, ConnConfig0),
    {ok, Client} = datalayers:connect(ConnConfig),

    _ = datalayers:execute(Client, <<"DROP TABLE IF EXISTS mqtt_arrow.auto_rebuild_test">>),
    {ok, _} = datalayers:execute(Client, <<
        "CREATE TABLE mqtt_arrow.auto_rebuild_test ("
        "ts TIMESTAMP(3) NOT NULL,"
        "msgid STRING NOT NULL,"
        "clientid STRING NOT NULL,"
        "topic STRING NOT NULL,"
        "qos INT8 NOT NULL,"
        "payload STRING NOT NULL,"
        "timestamp key(ts))"
        " PARTITION BY HASH(msgid) PARTITIONS 8 ENGINE=TimeSeries"
    >>),

    InsertSql = <<
        "INSERT INTO mqtt_arrow.auto_rebuild_test"
        "(ts, msgid, clientid, topic, qos, payload) VALUES (?, ?, ?, ?, ?, ?)"
    >>,
    {ok, Stmt} = datalayers:prepare(Client, InsertSql, #{auto_rebuild => true}),

    Ts1 = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, Stmt, [
        [Ts1, <<"id1">>, <<"c1">>, <<"t/1">>, 1, <<"hello1">>]
    ]),
    {ok, _} = datalayers:close_prepared(Client, Stmt),

    Ts2 = erlang:system_time(millisecond),
    {ok, _} = datalayers:execute_prepare(Client, Stmt, [
        [Ts2, <<"id2">>, <<"c2">>, <<"t/2">>, 1, <<"hello2">>]
    ]),

    {ok, Rows} = datalayers:execute(
        Client,
        <<"SELECT clientid, payload FROM mqtt_arrow.auto_rebuild_test ORDER BY ts">>
    ),
    ?assertMatch(
        [
            [<<"c1">>, <<"hello1">>],
            [<<"c2">>, <<"hello2">>]
        ],
        Rows
    ),

    _ = datalayers:execute(Client, <<"DROP TABLE IF EXISTS mqtt_arrow.auto_rebuild_test">>),
    datalayers:stop(Client),
    ok.

t_ecpool_on_reconnect_callback_consistency(TCConfig) ->
    emqx_bridge_v2_testlib:t_ecpool_on_reconnect_callback_consistency(TCConfig, #{
        callback_mod => emqx_bridge_datalayers_arrow_flight_connector,
        reconnect_callback_fns => [prepare_sql_to_conn]
    }).

-doc """
Reproduces the issue described in https://github.com/emqx/emqx/issues/19965

Previously, client pids were written to the connector's state.  So, when such clients
restarted, attempting to call those "hardcoded" pids would fail and fail the channel
removal.
""".
t_client_dead_recreate_action(TCConfig) ->
    OtherTable = atom_to_binary(?FUNCTION_NAME),
    on_exit(fun() ->
        exec_sql(
            fmt(~"drop table ${t}", #{t => OtherTable}),
            TCConfig
        )
    end),
    OtherTableSQL =
        fmt(
            ~"""
            CREATE TABLE IF NOT EXISTS mqtt_arrow.${t} (
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
          """,
            #{t => OtherTable}
        ),
    exec_sql(OtherTableSQL, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{
        ~"resource_opts" => #{~"health_check_interval" => ~"1s"}
    }),
    OtherSQL =
        fmt(
            <<
                "INSERT INTO mqtt_arrow.${t}(ts, msgid, clientid, topic, qos, payload) VALUES "
                "(${$}{timestamp}, ${$}{id}, ${$}{clientid}, ${$}{topic}, ${$}{qos}, ${$}{payload})"
            >>,
            #{t => OtherTable}
        ),
    {201, #{~"status" := ~"connected"}} = create_action_api(TCConfig, #{
        ~"parameters" => #{~"sql" => OtherSQL}
    }),
    ConnResId = connector_resource_id(TCConfig),
    Workers = [_ | _] = ecpool:workers(ConnResId),
    lists:foreach(
        fun({_, Pid}) ->
            {ok, ClientPid} = ecpool_worker:client(Pid),
            MRef = monitor(process, ClientPid),
            exit(ClientPid, kill),
            ?assertReceive({'DOWN', MRef, process, ClientPid, _})
        end,
        Workers
    ),
    {204, _} = delete_action_api(TCConfig),
    %% immediately recreating the action might fail because the ecpool workers haven't had
    %% time to restart their clients.
    {201, _} = create_action_api(TCConfig, #{
        ~"resource_opts" => #{~"health_check_interval" => ~"1s"}
    }),
    ?retry(
        1_000,
        7,
        ?assertMatch(
            {200, #{~"status" := ~"connected"}},
            get_action_api(TCConfig)
        )
    ),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    C = start_client(#{clientid => ClientId}),
    emqtt:publish(C, RuleTopic, ~"hey", [{qos, 1}]),
    %% wrote to the new configured table
    ?retry(
        200,
        10,
        ?assertMatch(
            [_],
            query_by_clientid(ClientId, TCConfig)
        )
    ),
    emqtt:stop(C),
    ok.

t_prune_dead_refs(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    ConnResId = connector_resource_id(TCConfig),
    {ok, _, #{state := #{client_state_tab := Tid} = ConnState}} =
        emqx_resource:get_instance(ConnResId),
    TabBefore = ets:tab2list(Tid),
    Workers = [_ | _] = ecpool:workers(ConnResId),
    lists:foreach(
        fun({_, Pid}) ->
            {ok, ClientPid} = ecpool_worker:client(Pid),
            MRef = monitor(process, ClientPid),
            exit(ClientPid, kill),
            ?assertReceive({'DOWN', MRef, process, ClientPid, _})
        end,
        Workers
    ),
    ok = emqx_bridge_datalayers_arrow_flight_connector:prune_dead_refs(ConnState),
    TabAfter = ets:tab2list(Tid),
    ?assertNotEqual(TabBefore, TabAfter, #{before => TabBefore, 'after' => TabAfter}),
    ok.
