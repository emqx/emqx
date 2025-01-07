%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_pgsql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../../emqx_resource/include/emqx_resource_errors.hrl").

% SQL definitions
-define(SQL_BRIDGE,
    "INSERT INTO mqtt_test(payload, arrived) "
    "VALUES (${payload}, TO_TIMESTAMP((${timestamp} :: bigint)/1000))"
).
-define(SQL_CREATE_TABLE,
    "CREATE TABLE IF NOT EXISTS mqtt_test (payload text, arrived timestamp NOT NULL) "
).
-define(SQL_DROP_TABLE, "DROP TABLE mqtt_test").
-define(SQL_DELETE, "DELETE from mqtt_test").
-define(SQL_SELECT, "SELECT payload FROM mqtt_test").

% DB defaults
-define(PGSQL_DATABASE, "mqtt").
-define(PGSQL_USERNAME, "root").
-define(PGSQL_PASSWORD, "public").
-define(BATCH_SIZE, 10).

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
    NonBatchCases = [t_write_timeout],
    BatchVariantGroups = [
        {group, with_batch},
        {group, without_batch},
        {group, matrix},
        {group, timescale}
    ],
    QueryModeGroups = [{async, BatchVariantGroups}, {sync, BatchVariantGroups}],
    [
        {tcp, QueryModeGroups},
        {tls, QueryModeGroups},
        {async, BatchVariantGroups},
        {sync, BatchVariantGroups},
        {with_batch, TCs -- NonBatchCases},
        {without_batch, TCs},
        {matrix, [t_setup_via_config_and_publish, t_setup_via_http_api_and_publish]},
        {timescale, [t_setup_via_config_and_publish, t_setup_via_http_api_and_publish]}
    ].

init_per_group(tcp, Config) ->
    Host = os:getenv("PGSQL_TCP_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("PGSQL_TCP_PORT", "5432")),
    [
        {pgsql_host, Host},
        {pgsql_port, Port},
        {enable_tls, false},
        {proxy_name, "pgsql_tcp"}
        | Config
    ];
init_per_group(tls, Config) ->
    Host = os:getenv("PGSQL_TLS_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("PGSQL_TLS_PORT", "5433")),
    [
        {pgsql_host, Host},
        {pgsql_port, Port},
        {enable_tls, true},
        {proxy_name, "pgsql_tls"}
        | Config
    ];
init_per_group(async, Config) ->
    [{query_mode, async} | Config];
init_per_group(sync, Config) ->
    [{query_mode, sync} | Config];
init_per_group(with_batch, Config0) ->
    Config = [{enable_batch, true} | Config0],
    common_init(Config);
init_per_group(without_batch, Config0) ->
    Config = [{enable_batch, false} | Config0],
    common_init(Config);
init_per_group(matrix, Config0) ->
    Config = [{bridge_type, <<"matrix">>}, {enable_batch, true} | Config0],
    common_init(Config);
init_per_group(timescale, Config0) ->
    Config = [{bridge_type, <<"timescale">>}, {enable_batch, true} | Config0],
    common_init(Config);
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when
    Group =:= with_batch;
    Group =:= without_batch;
    Group =:= matrix;
    Group =:= timescale
->
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    connect_and_drop_table(Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok = emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    connect_and_clear_table(Config),
    delete_bridge(Config),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    connect_and_clear_table(Config),
    ok = snabbkaffe:stop(),
    delete_bridge(Config),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(Config0) ->
    BridgeType = proplists:get_value(bridge_type, Config0, <<"pgsql">>),
    Host = ?config(pgsql_host, Config0),
    Port = ?config(pgsql_port, Config0),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            % Ensure enterprise bridge module is loaded
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_connector,
                    emqx_bridge,
                    emqx_bridge_pgsql,
                    emqx_rule_engine,
                    emqx_management,
                    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            {ok, _Api} = emqx_common_test_http:create_default_app(),
            % Connect to pgsql directly and create the table
            connect_and_create_table(Config0),
            {Name, PGConf} = pgsql_config(BridgeType, Config0),
            Config =
                [
                    {apps, Apps},
                    {pgsql_config, PGConf},
                    {pgsql_bridge_type, BridgeType},
                    {pgsql_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_pgsql);
                _ ->
                    {skip, no_pgsql}
            end
    end.

pgsql_config(BridgeType, Config) ->
    Port = integer_to_list(?config(pgsql_port, Config)),
    Server = ?config(pgsql_host, Config) ++ ":" ++ Port,
    Name = atom_to_binary(?MODULE),
    BatchSize =
        case ?config(enable_batch, Config) of
            true -> ?BATCH_SIZE;
            false -> 1
        end,
    QueryMode = ?config(query_mode, Config),
    TlsEnabled = ?config(enable_tls, Config),
    %% NOTE: supplying password through a file here, to verify that it works.
    Password = create_passfile(BridgeType, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {"
            "\n   enable = true"
            "\n   server = ~p"
            "\n   database = ~p"
            "\n   username = ~p"
            "\n   password = ~p"
            "\n   sql = ~p"
            "\n   resource_opts = {"
            "\n     request_ttl = 500ms"
            "\n     batch_size = ~b"
            "\n     query_mode = ~s"
            "\n     worker_pool_size = 1"
            "\n     health_check_interval = 15s"
            "\n     start_after_created = true"
            "\n     start_timeout = 5s"
            "\n     inflight_window = 100"
            "\n     max_buffer_bytes = 256MB"
            "\n     buffer_seg_bytes = 10MB"
            "\n     buffer_mode = memory_only"
            "\n     metrics_flush_interval = 5s"
            "\n     resume_interval = 15s"
            "\n   }"
            "\n   ssl = {"
            "\n     enable = ~w"
            "\n   }"
            "\n }",
            [
                BridgeType,
                Name,
                Server,
                ?PGSQL_DATABASE,
                ?PGSQL_USERNAME,
                Password,
                ?SQL_BRIDGE,
                BatchSize,
                QueryMode,
                TlsEnabled
            ]
        ),
    {Name, parse_and_check(ConfigString, BridgeType, Name)}.

default_sql() ->
    ?SQL_BRIDGE.

create_passfile(BridgeType, Config) ->
    Filename = binary_to_list(BridgeType) ++ ".passfile",
    Filepath = filename:join(?config(priv_dir, Config), Filename),
    ok = file:write_file(Filepath, ?PGSQL_PASSWORD),
    "file://" ++ Filepath.

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := Config}}} = RawConf,
    Config.

create_bridge(Config) ->
    create_bridge(Config, _Overrides = #{}).

create_bridge(Config, Overrides) ->
    BridgeType = ?config(pgsql_bridge_type, Config),
    Name = ?config(pgsql_name, Config),
    PGConfig0 = ?config(pgsql_config, Config),
    PGConfig = emqx_utils_maps:deep_merge(PGConfig0, Overrides),
    emqx_bridge:create(BridgeType, Name, PGConfig).

delete_bridge(Config) ->
    BridgeType = ?config(pgsql_bridge_type, Config),
    Name = ?config(pgsql_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Msg = _Request) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    emqx_bridge_v2:query(BridgeType, Name, Msg, #{timeout => 1_000}).

query_resource_sync(Config, Request) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    ActionId = emqx_bridge_v2:id(BridgeType, Name),
    emqx_resource_buffer_worker:simple_sync_query(ActionId, Request).

query_resource_async(Config, Request) ->
    query_resource_async(Config, Request, _Opts = #{}).

query_resource_async(Config, Request, Opts) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    Ref = alias([reply]),
    AsyncReplyFun = fun(Result) -> Ref ! {result, Ref, Result} end,
    Timeout = maps:get(timeout, Opts, 500),
    Return = emqx_bridge_v2:query(BridgeType, Name, Request, #{
        timeout => Timeout,
        async_reply_fun => {AsyncReplyFun, []}
    }),
    {Return, Ref}.

receive_result(Ref, Timeout) ->
    receive
        {result, Ref, Result} ->
            {ok, Result};
        {Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

connect_direct_pgsql(Config) ->
    Opts = #{
        host => ?config(pgsql_host, Config),
        port => ?config(pgsql_port, Config),
        username => ?PGSQL_USERNAME,
        password => ?PGSQL_PASSWORD,
        database => ?PGSQL_DATABASE
    },

    SslOpts =
        case ?config(enable_tls, Config) of
            true ->
                Opts#{
                    ssl => true,
                    ssl_opts => emqx_tls_lib:to_client_opts(#{enable => true})
                };
            false ->
                Opts
        end,
    {ok, Con} = epgsql:connect(SslOpts),
    Con.

% These funs connect and then stop the pgsql connection
connect_and_create_table(Config) ->
    Con = connect_direct_pgsql(Config),
    {ok, _, _} = epgsql:squery(Con, ?SQL_CREATE_TABLE),
    ok = epgsql:close(Con).

connect_and_drop_table(Config) ->
    Con = connect_direct_pgsql(Config),
    {ok, _, _} = epgsql:squery(Con, ?SQL_DROP_TABLE),
    ok = epgsql:close(Con).

connect_and_clear_table(Config) ->
    Con = connect_direct_pgsql(Config),
    _ = epgsql:squery(Con, ?SQL_CREATE_TABLE),
    {ok, _} = epgsql:squery(Con, ?SQL_DELETE),
    ok = epgsql:close(Con).

connect_and_get_payload(Config) ->
    Con = connect_direct_pgsql(Config),
    {ok, _, [{Result}]} = epgsql:squery(Con, ?SQL_SELECT),
    ok = epgsql:close(Con),
    Result.

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
            {_, {ok, _}} =
                ?wait_async_action(
                    send_message(Config, SentData),
                    #{?snk_kind := pgsql_connector_query_return},
                    10_000
                ),
            ?assertMatch(
                Val,
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(pgsql_connector_query_return, Trace0),
            case ?config(enable_batch, Config) of
                true ->
                    ?assertMatch([#{result := {_, [{ok, 1}]}}], Trace);
                false ->
                    ?assertMatch([#{result := {ok, 1}}], Trace)
            end,
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(pgsql_bridge_type, Config),
    Name = ?config(pgsql_name, Config),
    PgsqlConfig0 = ?config(pgsql_config, Config),
    QueryMode = ?config(query_mode, Config),
    PgsqlConfig = PgsqlConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType,
        %% NOTE: using literal passwords with HTTP API requests.
        <<"password">> => <<?PGSQL_PASSWORD>>
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(PgsqlConfig)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            {Res, {ok, _}} =
                ?wait_async_action(
                    send_message(Config, SentData),
                    #{?snk_kind := pgsql_connector_query_return},
                    10_000
                ),
            case QueryMode of
                async ->
                    ok;
                sync ->
                    ?assertEqual({ok, 1}, Res)
            end,
            ?assertMatch(
                Val,
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(pgsql_connector_query_return, Trace0),
            case ?config(enable_batch, Config) of
                true ->
                    ?assertMatch([#{result := {_, [{ok, 1}]}}], Trace);
                false ->
                    ?assertMatch([#{result := {ok, 1}}], Trace)
            end,
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

    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),

    ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name)),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            #{status := Status} when Status =:= disconnected orelse Status =:= connecting,
            emqx_bridge_v2:health_check(BridgeType, Name)
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
                ?of_kind(pgsql_connector_start_failed, Trace)
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
        emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
            {_, {ok, _}} =
                ?wait_async_action(
                    case QueryMode of
                        sync ->
                            ?assertMatch({error, _}, send_message(Config, SentData));
                        async ->
                            send_message(Config, SentData)
                    end,
                    #{?snk_kind := buffer_worker_flush_nack},
                    15_000
                )
        end),
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
    {ok, _} = create_bridge(
        Config,
        #{
            <<"resource_opts">> => #{
                <<"resume_interval">> => <<"100ms">>,
                <<"health_check_interval">> => <<"100ms">>
            }
        }
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := call_query_enter}),
        2_000
    ),
    Res0 =
        emqx_common_test_helpers:with_failure(timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
            Res1 =
                case QueryMode of
                    async ->
                        query_resource_async(Config, {send_message, SentData}, #{timeout => 60_000});
                    sync ->
                        query_resource(Config, {send_message, SentData})
                end,
            ?assertMatch({ok, [_]}, snabbkaffe:receive_events(SRef)),
            Res1
        end),
    case Res0 of
        {_, Ref} when is_reference(Ref) ->
            case receive_result(Ref, 15_000) of
                {ok, Res} ->
                    %% we may receive a successful result depending on
                    %% timing, if the request is retried after the
                    %% failure is healed.
                    case Res of
                        {error, {unrecoverable_error, _}} ->
                            ok;
                        {ok, _} ->
                            ok;
                        _ ->
                            ct:fail("unexpected result: ~p", [Res])
                    end;
                timeout ->
                    ct:pal("mailbox:\n  ~p", [process_info(self(), messages)]),
                    ct:fail("no response received")
            end;
        _ ->
            ?assertMatch({error, {resource_error, #{reason := timeout}}}, Res0)
    end,
    ok.

t_simple_sql_query(Config) ->
    EnableBatch = ?config(enable_batch, Config),
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {query, <<"SELECT count(1) AS T">>},
    Result =
        case QueryMode of
            sync ->
                query_resource(Config, Request);
            async ->
                {_, Ref} = query_resource_async(Config, Request),
                {ok, Res} = receive_result(Ref, 2_000),
                Res
        end,
    case EnableBatch of
        true ->
            ?assertEqual({error, {unrecoverable_error, batch_prepare_not_implemented}}, Result);
        false ->
            ?assertMatch({ok, _, [{1}]}, Result)
    end,
    ok.

t_missing_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    {_, {ok, Event}} =
        ?wait_async_action(
            send_message(Config, #{}),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),
    ?assertMatch(
        #{
            result :=
                {error,
                    {unrecoverable_error, #{
                        error_code := <<"23502">>,
                        error_codename := not_null_violation,
                        severity := error
                    }}}
        },
        Event
    ),
    ok.

t_bad_sql_parameter(Config) ->
    QueryMode = ?config(query_mode, Config),
    EnableBatch = ?config(enable_batch, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {query, <<"">>, [bad_parameter]},
    Result =
        case QueryMode of
            sync ->
                query_resource(Config, Request);
            async ->
                {_, Ref} = query_resource_async(Config, Request),
                {ok, Res} = receive_result(Ref, 2_000),
                Res
        end,
    case EnableBatch of
        true ->
            ?assertEqual({error, {unrecoverable_error, invalid_request}}, Result);
        false ->
            ?assertMatch(
                {error, {unrecoverable_error, _}}, Result
            )
    end,
    ok.

t_nasty_sql_string(Config) ->
    ?assertMatch({ok, _}, create_bridge(Config)),
    Payload = list_to_binary(lists:seq(1, 127)),
    Message = #{payload => Payload, timestamp => erlang:system_time(millisecond)},
    {_, {ok, _}} =
        ?wait_async_action(
            send_message(Config, Message),
            #{?snk_kind := pgsql_connector_query_return},
            1_000
        ),
    ?assertEqual(Payload, connect_and_get_payload(Config)).

t_missing_table(Config) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    % ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),

    ?check_trace(
        begin
            connect_and_drop_table(Config),
            ?assertMatch({ok, _}, create_bridge(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    #{status := Status} when Status == connecting orelse Status == disconnected,
                    emqx_bridge_v2:health_check(BridgeType, Name)
                )
            ),
            Val = integer_to_binary(erlang:unique_integer()),
            SentData = #{payload => Val, timestamp => 1668602148000},
            ?assertMatch(
                {error, {resource_error, #{reason := unhealthy_target}}},
                query_resource(Config, {send_message, SentData})
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_undefined_table, Trace)),
            ok
        end
    ),
    connect_and_create_table(Config),
    ok.

%% We test that we can handle when the prepared statement with the channel
%% name already exists in the connection instance when we try to make a new
%% prepared statement. It is unknown in which scenario this can happen but it
%% has been observed in a production log file.
%% See:
%% https://emqx.atlassian.net/browse/EEC-1036
t_prepared_statement_exists(Config) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    emqx_common_test_helpers:on_exit(fun() ->
        meck:unload()
    end),
    MeckOpts = [passthrough, no_link, no_history],
    meck:new(epgsql, MeckOpts),
    InsertPrepStatementDupAndThenRemoveMeck =
        fun(Conn, Key, SQL, List) ->
            meck:passthrough([Conn, Key, SQL, List]),
            meck:delete(
                epgsql,
                parse2,
                4
            ),
            meck:passthrough([Conn, Key, SQL, List])
        end,
    meck:expect(
        epgsql,
        parse2,
        InsertPrepStatementDupAndThenRemoveMeck
    ),
    %% We should recover if the prepared statement name already exists in the
    %% driver
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    #{status := Status} when Status == connected,
                    emqx_bridge_v2:health_check(BridgeType, Name)
                )
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_prepared_statement_exists, Trace)),
            ok
        end
    ),
    InsertPrepStatementDup =
        fun(Conn, Key, SQL, List) ->
            meck:passthrough([Conn, Key, SQL, List]),
            meck:passthrough([Conn, Key, SQL, List])
        end,
    meck:expect(
        epgsql,
        parse2,
        InsertPrepStatementDup
    ),
    %% We should get status disconnected if removing already existing statment don't help
    ?check_trace(
        begin
            ?assertMatch({ok, _}, create_bridge(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(
                    #{status := Status} when Status == disconnected,
                    emqx_bridge_v2:health_check(BridgeType, Name)
                )
            ),
            snabbkaffe_nemesis:cleanup(),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(pgsql_prepared_statement_exists, Trace)),
            ok
        end
    ),
    meck:unload(),
    ok.

t_table_removed(Config) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    ?check_trace(
        begin
            ct:pal("creating table"),
            connect_and_create_table(Config),
            ct:pal("creating bridge"),
            ?assertMatch(
                {ok, _},
                create_bridge(Config, #{
                    <<"resource_opts">> => #{
                        <<"health_check_interval">> => <<"1s">>
                    }
                })
            ),
            ct:pal("checking bridge health"),
            ?retry(
                _Sleep = 100,
                _Attempts = 200,
                ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name))
            ),
            ct:pal("dropping table"),
            connect_and_drop_table(Config),
            Val = integer_to_binary(erlang:unique_integer()),
            SentData = #{payload => Val, timestamp => 1668602148000},
            ActionId = emqx_bridge_v2:id(BridgeType, Name),
            ?retry(
                _Sleep = 100,
                _Attempts = 200,
                ?assertMatch(
                    #{error := {unhealthy_target, _}, status := disconnected},
                    emqx_bridge_v2:health_check(BridgeType, Name)
                )
            ),
            ct:pal("sending query"),
            case query_resource_sync(Config, {ActionId, SentData}) of
                {error, {unrecoverable_error, _}} ->
                    ok;
                ?RESOURCE_ERROR_M(not_connected, _) ->
                    ok;
                Res ->
                    ct:fail("unexpected result: ~p", [Res])
            end,
            ok
        end,
        []
    ),
    connect_and_create_table(Config),
    ok.

t_concurrent_health_checks(Config) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    ?check_trace(
        begin
            connect_and_create_table(Config),
            ?assertMatch({ok, _}, create_bridge(Config)),
            ?retry(
                _Sleep = 1_000,
                _Attempts = 20,
                ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name))
            ),
            emqx_utils:pmap(
                fun(_) ->
                    ?assertMatch(
                        #{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name)
                    )
                end,
                lists:seq(1, 20)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(postgres_connector_bad_parse2, Trace)),
            ok
        end
    ),
    ok.
