%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_mysql_SUITE).

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
-define(SQL_DELETE, "DELETE from mqtt_test").
-define(SQL_SELECT, "SELECT payload FROM mqtt_test").

% DB defaults
-define(MYSQL_DATABASE, "mqtt").
-define(MYSQL_USERNAME, "root").
-define(MYSQL_PASSWORD, "public").

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
            {group, tcp},
            {group, tls}
        ]},
        {async_query, [
            {group, tcp},
            {group, tls}
        ]},
        {tcp, TCs},
        {tls, TCs}
    ].

init_per_group(tcp, Config0) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "mysql"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    Config = [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, false}
        | Config0
    ],
    common_init(Config);
init_per_group(tls, Config0) ->
    MysqlHost = os:getenv("MYSQL_TLS_HOST", "mysql-tls"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TLS_PORT", "3306")),
    Config = [
        {mysql_host, MysqlHost},
        {mysql_port, MysqlPort},
        {enable_tls, true}
        | Config0
    ],
    common_init(Config);
init_per_group(sync_query, Config) ->
    [{query_mode, sync} | Config];
init_per_group(async_query, Config) ->
    [{query_mode, async} | Config];
init_per_group(with_batch, Config) ->
    [{enable_batch, true} | Config];
init_per_group(without_batch, Config) ->
    [{enable_batch, false} | Config];
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= tcp; Group =:= tls ->
    connect_and_drop_table(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config0) ->
    Config = [{mysql_direct_pid, connect_direct_mysql(Config0)} | Config0],
    catch clear_table(Config),
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    catch clear_table(Config),
    DirectPid = ?config(mysql_direct_pid, Config),
    mysql:stop(DirectPid),
    delete_bridge(Config),
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
            % Ensure EE bridge module is loaded
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            % Connect to mysql directly and create the table
            connect_and_create_table(Config0),
            {Name, MysqlConfig} = mysql_config(BridgeType, Config0),
            Config =
                [
                    {mysql_config, MysqlConfig},
                    {mysql_bridge_type, BridgeType},
                    {mysql_name, Name}
                    | Config0
                ],
            Config;
        false ->
            {skip, no_mysql}
    end.

mysql_config(BridgeType, Config) ->
    MysqlPort = integer_to_list(?config(mysql_port, Config)),
    Server = ?config(mysql_host, Config) ++ ":" ++ MysqlPort,
    Name = atom_to_binary(?MODULE),
    EnableBatch = ?config(enable_batch, Config),
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
            "    enable_batch = ~p\n"
            "    query_mode = ~s\n"
            "  }\n"
            "  ssl = {\n"
            "                 enable = ~w\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?MYSQL_DATABASE,
                ?MYSQL_USERNAME,
                ?MYSQL_PASSWORD,
                ?SQL_BRIDGE,
                EnableBatch,
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
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    MysqlConfig = ?config(mysql_config, Config),
    emqx_bridge:create(BridgeType, Name, MysqlConfig).

delete_bridge(Config) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    BridgeID = emqx_bridge_resource:bridge_id(BridgeType, Name),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?assertMatch({ok, connected}, emqx_resource:health_check(ResourceID)),
    emqx_bridge:send_message(BridgeID, Payload).

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

% These funs connect and then stop the mysql connection
connect_and_create_table(Config) ->
    DirectPid = connect_direct_mysql(Config),
    ok = mysql:query(DirectPid, ?SQL_CREATE_TABLE),
    mysql:stop(DirectPid).

connect_and_drop_table(Config) ->
    DirectPid = connect_direct_mysql(Config),
    ok = mysql:query(DirectPid, ?SQL_DROP_TABLE),
    mysql:stop(DirectPid).

% These funs expects a connection to already exist
clear_table(Config) ->
    DirectPid = ?config(mysql_direct_pid, Config),
    ok = mysql:query(DirectPid, ?SQL_DELETE).

get_payload(Config) ->
    DirectPid = ?config(mysql_direct_pid, Config),
    mysql:query(DirectPid, ?SQL_SELECT).

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
                get_payload(Config)
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
                get_payload(Config)
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
