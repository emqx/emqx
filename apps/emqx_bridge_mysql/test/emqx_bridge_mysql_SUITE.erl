%%--------------------------------------------------------------------
% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% SQL definitions
-define(SQL_BRIDGE,
    "INSERT INTO mqtt_test(payload, arrived) "
    "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
).
-define(SQL_CREATE_TABLE,
    "CREATE TABLE IF NOT EXISTS mqtt_test (payload blob, arrived datetime NOT NULL) "
    "DEFAULT CHARSET=utf8MB4;"
).
-define(SQL_DROP_TABLE, "DROP TABLE mqtt_test").
-define(SQL_SELECT, "SELECT payload FROM mqtt_test").

% DB defaults
-define(MYSQL_DATABASE, "mqtt").
-define(MYSQL_USERNAME, "root").
-define(MYSQL_PASSWORD, "public").
-define(MYSQL_POOL_SIZE, 4).

-define(WORKER_POOL_SIZE, 1).

-define(ACTION_TYPE, mysql).

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    NonBatchCases = [
        t_write_timeout,
        t_uninitialized_prepared_statement,
        t_non_batch_update_is_allowed,
        t_delete_with_undefined_field_in_sql,
        t_undefined_field_in_sql
    ],
    OnlyBatchCases = [
        t_batch_update_is_forbidden
    ],
    BatchingGroups = [
        {group, with_batch},
        {group, without_batch}
    ],
    QueryModeGroups = [{group, async}, {group, sync}],
    [
        {tcp, QueryModeGroups},
        {tls, QueryModeGroups},
        {async, BatchingGroups},
        {sync, BatchingGroups},
        {with_batch, TCs -- NonBatchCases},
        {without_batch, TCs -- OnlyBatchCases}
    ].

init_per_group(tcp, Config) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, false},
        {proxy_name, "mysql_tcp"}
        | Config
    ];
init_per_group(tls, Config) ->
    MysqlHost = os:getenv("MYSQL_TLS_HOST", "toxiproxy"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TLS_PORT", "3307")),
    [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, true},
        {proxy_name, "mysql_tls"}
        | Config
    ];
init_per_group(async, Config) ->
    [{query_mode, async} | Config];
init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(with_batch, Config0) ->
    Config = [{batch_size, 100} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{batch_size, 1} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    Apps = ?config(apps, Config),
    connect_and_drop_table(Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_rule_engine, emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    connect_and_drop_table(Config),
    connect_and_create_table(Config),
    delete_bridge(Config),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(Config0) ->
    BridgeType = <<"mysql">>,
    MysqlHost = ?config(mysql_host, Config0),
    MysqlPort = ?config(mysql_port, Config0),
    case emqx_common_test_helpers:is_tcp_server_available(MysqlHost, MysqlPort) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_mysql,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            % Connect to mysql directly and create the table
            connect_and_create_table(Config0),
            {Name, MysqlConfig} = mysql_config(BridgeType, Config0),
            Config =
                [
                    {apps, Apps},
                    {mysql_config, MysqlConfig},
                    {mysql_bridge_type, BridgeType},
                    {mysql_name, Name},
                    {bridge_type, BridgeType},
                    {bridge_name, Name},
                    {bridge_config, MysqlConfig},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_mysql);
                _ ->
                    {skip, no_mysql}
            end
    end.

mysql_config(BridgeType, Config) ->
    MysqlPort = integer_to_list(?config(mysql_port, Config)),
    Server = ?config(mysql_host, Config) ++ ":" ++ MysqlPort,
    Name = atom_to_binary(?MODULE),
    BatchSize = ?config(batch_size, Config),
    QueryMode = ?config(query_mode, Config),
    TlsEnabled = ?config(enable_tls, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  database = ~p\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "  pool_size = ~b\n"
            "  sql = ~p\n"
            "  resource_opts = {\n"
            "    request_ttl = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "    worker_pool_size = ~b\n"
            "    health_check_interval = 15s\n"
            "    start_timeout = 5s\n"
            "    inflight_window = 100\n"
            "    max_buffer_bytes = 256MB\n"
            "    buffer_mode = memory_only\n"
            "    batch_time = 100ms\n"
            "    metrics_flush_interval = 5s\n"
            "    buffer_seg_bytes = 10MB\n"
            "    start_after_created = true\n"
            "  }\n"
            "  ssl = {\n"
            "    enable = ~w\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?MYSQL_DATABASE,
                ?MYSQL_USERNAME,
                ?MYSQL_PASSWORD,
                ?MYSQL_POOL_SIZE,
                ?SQL_BRIDGE,
                BatchSize,
                QueryMode,
                ?WORKER_POOL_SIZE,
                TlsEnabled
            ]
        ),
    {Name, parse_and_check(ConfigString, BridgeType, Name)}.

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    MysqlConfig0 = ?config(mysql_config, Config),
    MysqlConfig = emqx_utils_maps:deep_merge(MysqlConfig0, Overrides),
    emqx_bridge:create(BridgeType, Name, MysqlConfig).

delete_bridge(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Request) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    emqx_bridge_v2:query(BridgeType, Name, Request, #{timeout => 500}).

sync_query_resource(Config, Request) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_v2:id(BridgeType, Name),
    emqx_resource_buffer_worker:simple_sync_query(ResourceID, Request).

query_resource_async(Config, Request) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    Ref = alias([reply]),
    AsyncReplyFun = fun(Result) -> Ref ! {result, Ref, Result} end,
    Return = emqx_bridge_v2:query(BridgeType, Name, Request, #{
        timeout => 500, async_reply_fun => {AsyncReplyFun, []}
    }),
    {Return, Ref}.

receive_result(Ref, Timeout) ->
    receive
        {result, Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

unprepare(Config, Key) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    {ok, _, #{state := #{connector_state := #{pool_name := PoolName}}}} = emqx_resource:get_instance(
        ResourceID
    ),
    [
        begin
            {ok, Conn} = ecpool_worker:client(Worker),
            ok = mysql:unprepare(Conn, Key)
        end
     || {_Name, Worker} <- ecpool:workers(PoolName)
    ].

% We need to create and drop the test table outside of using bridges
% since a bridge expects the table to exist when enabling it. We
% therefore call the mysql module directly, in addition to using it
% for querying the DB directly.
connect_direct_mysql(Config) ->
    Opts = [
        {host, ?config(mysql_host, Config)},
        {port, ?config(mysql_port, Config)},
        {user, ?MYSQL_USERNAME},
        {password, ?MYSQL_PASSWORD},
        {database, ?MYSQL_DATABASE}
    ],
    SslOpts =
        case ?config(enable_tls, Config) of
            true ->
                [{ssl, emqx_tls_lib:to_client_opts(#{enable => true})}];
            false ->
                []
        end,
    {ok, Pid} = mysql:start_link(Opts ++ SslOpts),
    Pid.

query_direct_mysql(Config, Query) ->
    Pid = connect_direct_mysql(Config),
    try
        mysql:query(Pid, Query)
    after
        mysql:stop(Pid)
    end.

% These funs connect and then stop the mysql connection
connect_and_create_table(Config) ->
    query_direct_mysql(Config, ?SQL_CREATE_TABLE).

connect_and_drop_table(Config) ->
    query_direct_mysql(Config, ?SQL_DROP_TABLE).

connect_and_get_payload(Config) ->
    query_direct_mysql(Config, ?SQL_SELECT).

create_rule_and_action_http(Config) ->
    Name = ?config(mysql_name, Config),
    Type = ?config(mysql_bridge_type, Config),
    BridgeId = emqx_bridge_resource:bridge_id(Type, Name),
    Params = #{
        enable => true,
        sql => <<"SELECT * FROM \"t/topic\"">>,
        actions => [BridgeId]
    },
    Path = emqx_mgmt_api_test_util:api_path(["rules"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res0} ->
            Res = #{<<"id">> := RuleId} = emqx_utils_json:decode(Res0, [return_maps]),
            on_exit(fun() -> ok = emqx_rule_engine:delete_rule(RuleId) end),
            {ok, Res};
        Error ->
            Error
    end.

request_api_status(BridgeId) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges", BridgeId]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(get, Path, "", AuthHeader) of
        {ok, Res0} ->
            #{<<"status">> := Status} = _Res = emqx_utils_json:decode(Res0, [return_maps]),
            {ok, binary_to_existing_atom(Status)};
        Error ->
            Error
    end.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := mysql_connector_query_return},
                10_000
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[Val]]},
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_undefined_vars_as_null(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config, #{<<"undefined_vars_as_null">> => true})
    ),
    SentData = #{payload => undefined, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := mysql_connector_query_return},
                10_000
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[null]]},
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    MysqlConfig0 = ?config(mysql_config, Config),
    MysqlConfig = MysqlConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(MysqlConfig)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := mysql_connector_query_return},
                10_000
            ),
            ?assertMatch(
                {ok, [<<"payload">>], [[Val]]},
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(mysql_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_get_status(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),

    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            {ok, Status} when Status =:= disconnected orelse Status =:= connecting,
            emqx_resource_manager:health_check(ResourceID)
        )
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
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(mysql_connector_start_failed, Trace)
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
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            %% for some unknown reason, `?wait_async_action' and `subscribe'
            %% hang and timeout if called inside `with_failure', but the event
            %% happens and is emitted after the test pid dies!?
            {ok, SRef} = snabbkaffe:subscribe(
                ?match_event(#{?snk_kind := buffer_worker_flush_nack}),
                2_000
            ),
            emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
                case QueryMode of
                    sync ->
                        ?assertMatch(
                            {error, {resource_error, #{reason := timeout}}},
                            send_message(Config, SentData)
                        );
                    async ->
                        send_message(Config, SentData)
                end,
                ?assertMatch({ok, [#{result := {error, _}}]}, snabbkaffe:receive_events(SRef)),
                ok
            end),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(buffer_worker_flush_nack, Trace0),
            ?assertMatch([#{result := {error, _}} | _], Trace),
            [#{result := {error, Error}} | _] = Trace,
            case Error of
                {resource_error, _} ->
                    ok;
                {recoverable_error, disconnected} ->
                    ok;
                _ ->
                    ct:fail("unexpected error: ~p", [Error])
            end
        end
    ),
    ok.

% This test doesn't work with batch enabled since it is not possible
% to set the timeout directly for batch queries
t_write_timeout(Config) ->
    ProxyName = ?config(proxy_name, Config),
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    QueryMode = ?config(query_mode, Config),
    {ok, _} = create_bridge(Config),
    connect_and_create_table(Config),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    Timeout = 1000,
    %% for some unknown reason, `?wait_async_action' and `subscribe'
    %% hang and timeout if called inside `with_failure', but the event
    %% happens and is emitted after the test pid dies!?
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := buffer_worker_flush_nack}),
        2 * Timeout
    ),
    emqx_common_test_helpers:with_failure(timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
        Name = ?config(mysql_name, Config),
        BridgeType = ?config(mysql_bridge_type, Config),
        ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

        case QueryMode of
            sync ->
                ?assertMatch(
                    {error, {resource_error, #{reason := timeout}}},
                    query_resource(Config, {ResourceID, SentData, [], Timeout})
                );
            async ->
                query_resource(Config, {ResourceID, SentData, [], Timeout}),
                ok
        end,
        ok
    end),
    ?assertMatch({ok, [#{result := {error, _}}]}, snabbkaffe:receive_events(SRef)),
    ok.

t_simple_sql_query(Config) ->
    QueryMode = ?config(query_mode, Config),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"SELECT count(1) AS T">>},
    Result =
        case QueryMode of
            sync ->
                query_resource(Config, Request);
            async ->
                {_, Ref} = query_resource_async(Config, Request),
                {ok, Res} = receive_result(Ref, 2_000),
                Res
        end,
    case IsBatch of
        true -> ?assertEqual({error, {unrecoverable_error, batch_select_not_implemented}}, Result);
        false -> ?assertEqual({ok, [<<"T">>], [[1]]}, Result)
    end,
    ok.

t_missing_data(Config) ->
    BatchSize = ?config(batch_size, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := buffer_worker_flush_ack}),
        2_000
    ),
    send_message(Config, #{}),
    {ok, [Event]} = snabbkaffe:receive_events(SRef),
    case BatchSize of
        N when N > 1 ->
            ?assertMatch(
                #{
                    result :=
                        {error,
                            {unrecoverable_error,
                                {1292, _, <<"Truncated incorrect DOUBLE value: 'undefined'">>}}}
                },
                Event
            );
        1 ->
            ?assertMatch(
                #{
                    result :=
                        {error,
                            {unrecoverable_error, {1048, _, <<"Column 'arrived' cannot be null">>}}}
                },
                Event
            )
    end,
    ok.

t_bad_sql_parameter(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"">>, [bad_parameter]},
    {_, {ok, Event}} =
        ?wait_async_action(
            query_resource(Config, Request),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertMatch(#{result := {error, {unrecoverable_error, invalid_request}}}, Event);
        false ->
            ?assertMatch(
                #{result := {error, {unrecoverable_error, {invalid_params, [bad_parameter]}}}},
                Event
            )
    end,
    ok.

t_nasty_sql_string(Config) ->
    ?assertMatch({ok, _}, create_bridge(Config)),
    Payload = list_to_binary(lists:seq(0, 255)),
    Message = #{payload => Payload, timestamp => erlang:system_time(millisecond)},
    {Result, {ok, _}} =
        ?wait_async_action(
            send_message(Config, Message),
            #{?snk_kind := mysql_connector_query_return},
            1_000
        ),
    ?assertEqual(ok, Result),
    ?assertMatch(
        {ok, [<<"payload">>], [[Payload]]},
        connect_and_get_payload(Config)
    ).

t_workload_fits_prepared_statement_limit(Config) ->
    N = 50,
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Results = lists:append(
        emqx_utils:pmap(
            fun(_) ->
                [
                    begin
                        Payload = integer_to_binary(erlang:unique_integer()),
                        Timestamp = erlang:system_time(millisecond),
                        send_message(Config, #{payload => Payload, timestamp => Timestamp})
                    end
                 || _ <- lists:seq(1, N)
                ]
            end,
            lists:seq(1, ?WORKER_POOL_SIZE * ?MYSQL_POOL_SIZE),
            _Timeout = 10_000
        )
    ),
    ?assertEqual(
        [],
        [R || R <- Results, R /= ok]
    ),
    {ok, _, [[_Var, Count]]} =
        query_direct_mysql(Config, "SHOW GLOBAL STATUS LIKE 'Prepared_stmt_count'"),
    ?assertEqual(
        ?MYSQL_POOL_SIZE,
        binary_to_integer(Count)
    ).

t_unprepared_statement_query(Config) ->
    ok = connect_and_create_table(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {prepared_query, unprepared_query, []},
    {_, {ok, Event}} =
        ?wait_async_action(
            query_resource(Config, Request),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    BatchSize = ?config(batch_size, Config),
    IsBatch = BatchSize > 1,
    case IsBatch of
        true ->
            ?assertMatch(#{result := {error, {unrecoverable_error, invalid_request}}}, Event);
        false ->
            ?assertMatch(
                #{result := {error, {unrecoverable_error, prepared_statement_invalid}}},
                Event
            )
    end,
    ok.

%% Test doesn't work with batch enabled since batch doesn't use
%% prepared statements as such; it has its own query generation process
t_uninitialized_prepared_statement(Config) ->
    connect_and_create_table(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_v2:id(BridgeType, Name),
    unprepare(Config, ResourceID),
    ?check_trace(
        begin
            {Res, {ok, _}} =
                ?wait_async_action(
                    send_message(Config, SentData),
                    #{?snk_kind := mysql_connector_query_return},
                    2_000
                ),
            ?assertEqual(ok, Res),
            ok
        end,
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := mysql_connector_prepare_query_failed, error := not_prepared},
                    #{
                        ?snk_kind := mysql_connector_on_query_prepared_sql,
                        type_or_key := ResourceID
                    },
                    Trace
                )
            ),
            SendQueryTrace = ?of_kind(mysql_connector_send_query, Trace),
            ?assertMatch([#{data := [Val, _]}, #{data := [Val, _]}], SendQueryTrace),
            ReturnTrace = ?of_kind(mysql_connector_query_return, Trace),
            ?assertMatch([#{result := ok}], ReturnTrace),
            ok
        end
    ),
    ok.

t_missing_table(Config) ->
    QueryMode = ?config(query_mode, Config),
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),

    ?check_trace(
        begin
            connect_and_drop_table(Config),
            ?assertMatch({ok, _}, create_bridge(Config)),
            BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    {ok, Status} when Status == connecting orelse Status == disconnected,
                    request_api_status(BridgeID)
                )
            ),
            Val = integer_to_binary(erlang:unique_integer()),
            SentData = #{payload => Val, timestamp => 1668602148000},
            ResourceID = emqx_bridge_v2:id(BridgeType, Name),
            Request = {ResourceID, SentData},
            Result =
                case QueryMode of
                    sync ->
                        query_resource(Config, Request);
                    async ->
                        {Res, _Ref} = query_resource_async(Config, Request),
                        Res
                end,
            ?assertMatch(
                {error, {resource_error, #{reason := unhealthy_target}}},
                Result
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(mysql_undefined_table, Trace)),
            ok
        end
    ).

t_table_removed(Config) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    connect_and_create_table(Config),
    ?assertMatch({ok, _}, create_bridge(Config)),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID))
    ),
    connect_and_drop_table(Config),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    Timeout = 1000,
    ActionID = emqx_bridge_v2:id(BridgeType, Name),
    ?assertMatch(
        {error,
            {unrecoverable_error, {1146, <<"42S02">>, <<"Table 'mqtt.mqtt_test' doesn't exist">>}}},
        sync_query_resource(Config, {ActionID, SentData, [], Timeout})
    ),
    ok.

init_nested_payload_template(SQL, Config) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    Value = integer_to_binary(erlang:unique_integer()),
    {ok, _} = create_bridge(Config, #{<<"sql">> => SQL}),
    {ok, #{<<"from">> := [Topic]}} = create_rule_and_action_http(Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceID))
    ),
    %% send message via rule action
    Payload = emqx_utils_json:encode(#{value => Value}),
    Message = emqx_message:make(Topic, Payload),
    {Value, Message}.

t_nested_payload_template_1(Config) ->
    SQL = <<
        "INSERT INTO mqtt_test(payload, arrived) "
        "VALUES (${payload.value}, FROM_UNIXTIME(${timestamp}/1000))"
    >>,
    {Value, Message} = init_nested_payload_template(SQL, Config),
    {_, {ok, _}} =
        ?wait_async_action(
            emqx:publish(Message),
            #{?snk_kind := mysql_connector_query_return},
            10_000
        ),
    SelectResult = connect_and_get_payload(Config),
    ?assertEqual({ok, [<<"payload">>], [[Value]]}, SelectResult),
    ok.

t_nested_payload_template_2(Config) ->
    SQL = <<
        "INSERT INTO mqtt_test(payload, arrived) "
        "VALUES (${payload.value}, FROM_UNIXTIME(${timestamp}/1000)) "
        "ON DUPLICATE KEY UPDATE arrived=NOW()"
    >>,
    MsgCnt = 20,
    {Value, Message} = init_nested_payload_template(SQL, Config),
    {_, {ok, _}} =
        ?wait_async_action(
            [emqx:publish(Message) || _ <- lists:seq(1, MsgCnt)],
            #{?snk_kind := mysql_connector_query_return},
            10_000
        ),
    timer:sleep(1000),
    {ok, [<<"payload">>], Results} = connect_and_get_payload(Config),
    ct:pal("-----value: ~p, results: ~p", [Value, Results]),
    ?assert(length(Results) >= MsgCnt),
    ?assert(lists:all(fun([V]) -> V == Value end, Results)),
    ok.

t_batch_update_is_forbidden(Config) ->
    ?check_trace(
        begin
            Overrides = #{
                <<"sql">> =>
                    <<
                        "UPDATE mqtt_test "
                        "SET arrived = FROM_UNIXTIME(${timestamp}/1000) "
                        "WHERE payload = ${payload.value}"
                    >>
            },
            ProbeRes = emqx_bridge_testlib:probe_bridge_api(Config, Overrides),
            ?assertMatch({error, {{_, 400, _}, _, _Body}}, ProbeRes),
            {error, {{_, 400, _}, _, ProbeBodyRaw}} = ProbeRes,
            ?assertEqual(
                match,
                re:run(
                    ProbeBodyRaw,
                    <<"UPDATE statements are not supported for batch operations">>,
                    [global, {capture, none}]
                )
            ),
            CreateRes = emqx_bridge_testlib:create_bridge_api(Config, Overrides),
            ?assertMatch(
                {ok, {{_, 201, _}, _, #{<<"status">> := <<"disconnected">>}}},
                CreateRes
            ),
            {ok, {{_, 201, _}, _, #{<<"status_reason">> := Reason}}} = CreateRes,
            ?assertEqual(
                match,
                re:run(
                    Reason,
                    <<"UPDATE statements are not supported for batch operations">>,
                    [global, {capture, none}]
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_non_batch_update_is_allowed(Config) ->
    ?check_trace(
        begin
            BridgeName = ?config(bridge_name, Config),
            Overrides = #{
                <<"resource_opts">> => #{<<"metrics_flush_interval">> => <<"500ms">>},
                <<"sql">> =>
                    <<
                        "UPDATE mqtt_test "
                        "SET arrived = FROM_UNIXTIME(${timestamp}/1000) "
                        "WHERE payload = ${payload.value}"
                    >>
            },
            ProbeRes = emqx_bridge_testlib:probe_bridge_api(Config, Overrides),
            ?assertMatch({ok, {{_, 204, _}, _, _Body}}, ProbeRes),
            ?assertMatch(
                {ok, {{_, 201, _}, _, #{<<"status">> := <<"connected">>}}},
                emqx_bridge_testlib:create_bridge_api(Config, Overrides)
            ),
            {ok, #{
                <<"id">> := RuleId,
                <<"from">> := [Topic]
            }} = create_rule_and_action_http(Config),
            Payload = emqx_utils_json:encode(#{value => <<"aaaa">>}),
            Message = emqx_message:make(Topic, Payload),
            {_, {ok, _}} =
                ?wait_async_action(
                    emqx:publish(Message),
                    #{?snk_kind := mysql_connector_query_return},
                    10_000
                ),
            ActionId = emqx_bridge_v2:id(?ACTION_TYPE, BridgeName),
            ?assertEqual(1, emqx_resource_metrics:matched_get(ActionId)),
            ?retry(
                _Sleep0 = 200,
                _Attempts0 = 10,
                ?assertEqual(1, emqx_resource_metrics:success_get(ActionId))
            ),

            ?assertEqual(1, emqx_metrics_worker:get(rule_metrics, RuleId, 'actions.success')),
            ok
        end,
        []
    ),
    ok.

t_undefined_field_in_sql(Config) ->
    ?check_trace(
        begin
            Overrides = #{
                <<"sql">> =>
                    <<
                        "INSERT INTO mqtt_test(wrong_column, arrived) "
                        "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
                    >>
            },
            ProbeRes = emqx_bridge_testlib:probe_bridge_api(Config, Overrides),
            ?assertMatch({error, {{_, 400, _}, _, _BodyRaw}}, ProbeRes),
            {error, {{_, 400, _}, _, BodyRaw}} = ProbeRes,
            ?assertEqual(
                match,
                re:run(
                    BodyRaw,
                    <<"Unknown column 'wrong_column' in 'field list'">>,
                    [{capture, none}]
                ),
                #{body => BodyRaw}
            ),
            ok
        end,
        []
    ),
    ok.

t_delete_with_undefined_field_in_sql(Config) ->
    ?check_trace(
        begin
            Name = ?config(bridge_name, Config),
            Type = ?config(bridge_type, Config),
            Overrides = #{
                <<"sql">> =>
                    <<
                        "INSERT INTO mqtt_test(wrong_column, arrived) "
                        "VALUES (${payload}, FROM_UNIXTIME(${timestamp}/1000))"
                    >>
            },
            ?assertMatch(
                {ok, {{_, 201, _}, _, #{<<"status">> := Status}}} when
                    Status =:= <<"connecting">> orelse Status =:= <<"disconnected">>,
                emqx_bridge_testlib:create_bridge_api(Config, Overrides)
            ),
            ?assertMatch(
                {ok, {{_, 204, _}, _, _}},
                emqx_bridge_testlib:delete_bridge_http_api_v1(#{type => Type, name => Name})
            ),
            ok
        end,
        []
    ),
    ok.
