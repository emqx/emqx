%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_pgsql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    [
        {tcp, [
            {group, with_batch},
            {group, without_batch},
            {group, matrix},
            {group, timescale}
        ]},
        {tls, [
            {group, with_batch},
            {group, without_batch},
            {group, matrix},
            {group, timescale}
        ]},
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
        {query_mode, sync},
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
        {query_mode, sync},
        {proxy_name, "pgsql_tls"}
        | Config
    ];
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

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    connect_and_drop_table(Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    connect_and_clear_table(Config),
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    connect_and_clear_table(Config),
    ok = snabbkaffe:stop(),
    delete_bridge(Config),
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
            % Ensure EE bridge module is loaded
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            % Connect to pgsql directly and create the table
            connect_and_create_table(Config0),
            {Name, PGConf} = pgsql_config(BridgeType, Config0),
            Config =
                [
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
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  database = ~p\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "  sql = ~p\n"
            "  resource_opts = {\n"
            "    request_timeout = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "  }\n"
            "  ssl = {\n"
            "    enable = ~w\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?PGSQL_DATABASE,
                ?PGSQL_USERNAME,
                ?PGSQL_PASSWORD,
                ?SQL_BRIDGE,
                BatchSize,
                QueryMode,
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
    BridgeType = ?config(pgsql_bridge_type, Config),
    Name = ?config(pgsql_name, Config),
    PGConfig = ?config(pgsql_config, Config),
    emqx_bridge:create(BridgeType, Name, PGConfig).

delete_bridge(Config) ->
    BridgeType = ?config(pgsql_bridge_type, Config),
    Name = ?config(pgsql_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    emqx_bridge:send_message(BridgeID, Payload).

query_resource(Config, Request) ->
    Name = ?config(pgsql_name, Config),
    BridgeType = ?config(pgsql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ResourceID, Request, #{timeout => 1_000}).

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
            ?wait_async_action(
                ?assertEqual({ok, 1}, send_message(Config, SentData)),
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
    PgsqlConfig = PgsqlConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(PgsqlConfig)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual({ok, 1}, send_message(Config, SentData)),
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
                    1_000
                )
        end),
        fun(Trace0) ->
            ct:pal("trace: ~p", [Trace0]),
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
    {ok, _} = create_bridge(Config),
    Val = integer_to_binary(erlang:unique_integer()),
    SentData = #{payload => Val, timestamp => 1668602148000},
    Timeout = 1000,
    emqx_common_test_helpers:with_failure(timeout, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch(
            {error, {resource_error, #{reason := timeout}}},
            query_resource(Config, {send_message, SentData, [], Timeout})
        )
    end),
    ok.

t_simple_sql_query(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"SELECT count(1) AS T">>},
    Result = query_resource(Config, Request),
    case ?config(enable_batch, Config) of
        true -> ?assertEqual({error, {unrecoverable_error, batch_prepare_not_implemented}}, Result);
        false -> ?assertMatch({ok, _, [{1}]}, Result)
    end,
    ok.

t_missing_data(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    ?assertMatch(
        {error, {unrecoverable_error, {error, error, <<"23502">>, not_null_violation, _, _}}},
        Result
    ),
    ok.

t_bad_sql_parameter(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Request = {sql, <<"">>, [bad_parameter]},
    Result = query_resource(Config, Request),
    case ?config(enable_batch, Config) of
        true ->
            ?assertEqual({error, {unrecoverable_error, invalid_request}}, Result);
        false ->
            ?assertMatch(
                {error, {unrecoverable_error, _}}, Result
            )
    end,
    ok.
