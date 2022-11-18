%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_ee_bridge_mysql_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
        {group, tcp},
        {group, tls}
        | (emqx_common_test_helpers:all(?MODULE) -- group_tests())
    ].

group_tests() ->
    [
        t_setup_via_config_and_publish,
        t_setup_via_http_api_and_publish
    ].

groups() ->
    [
        {tcp, group_tests()},
        {tls, group_tests()}
    ].

init_per_group(GroupType = tcp, Config) ->
    MysqlHost = os:getenv("MYSQL_TCP_HOST", "mysql"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TCP_PORT", "3306")),
    common_init(GroupType, Config, MysqlHost, MysqlPort);
init_per_group(GroupType = tls, Config) ->
    MysqlHost = os:getenv("MYSQL_TLS_HOST", "mysql-tls"),
    MysqlPort = list_to_integer(os:getenv("MYSQL_TLS_PORT", "3306")),
    common_init(GroupType, Config, MysqlHost, MysqlPort).

end_per_group(GroupType, Config) ->
    drop_table_raw(GroupType, Config),
    ok.

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_common_test_helpers:stop_apps([emqx_bridge, emqx_conf]),
    ok.

init_per_testcase(_Testcase, Config) ->
    catch clear_table(Config),
    delete_bridge(Config),
    Config.

end_per_testcase(_Testcase, Config) ->
    catch clear_table(Config),
    delete_bridge(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(GroupType, Config0, MysqlHost, MysqlPort) ->
    BridgeType = <<"mysql">>,
    case emqx_common_test_helpers:is_tcp_server_available(MysqlHost, MysqlPort) of
        true ->
            % Ensure EE bridge module is loaded
            _ = application:load(emqx_ee_bridge),
            _ = emqx_ee_bridge:module_info(),
            ok = emqx_common_test_helpers:start_apps([emqx_conf, emqx_bridge]),
            emqx_mgmt_api_test_util:init_suite(),
            {Name, MysqlConfig} = mysql_config(MysqlHost, MysqlPort, GroupType, BridgeType),
            Config =
                [
                    {mysql_host, MysqlHost},
                    {mysql_port, MysqlPort},
                    {mysql_config, MysqlConfig},
                    {mysql_bridge_type, BridgeType},
                    {mysql_name, Name}
                    | Config0
                ],
            create_table_raw(GroupType, Config),
            Config;
        false ->
            {skip, no_mysql}
    end.

mysql_config(MysqlHost, MysqlPort0, GroupType, BridgeType) ->
    MysqlPort = integer_to_list(MysqlPort0),
    Server = MysqlHost ++ ":" ++ MysqlPort,
    Name = iolist_to_binary(io_lib:format("~s-~s", [?MODULE, GroupType])),
    SslEnabled =
        case GroupType of
            tcp -> "false";
            tls -> "true"
        end,
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  database = ~p\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "  sql = ~p\n"
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
                SslEnabled
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

query(Config, SqlQuery) ->
    BridgeType = ?config(mysql_bridge_type, Config),
    Name = ?config(mysql_name, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?assertMatch({ok, connected}, emqx_resource:health_check(ResourceID)),
    emqx_resource:simple_sync_query(ResourceID, {sql, SqlQuery}).

send_message(Config, Payload) ->
    Name = ?config(mysql_name, Config),
    BridgeType = ?config(mysql_bridge_type, Config),
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    ?assertMatch({ok, connected}, emqx_resource:health_check(ResourceID)),
    % TODO: Check why we can't use send_message directly!
    % BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
    % emqx_bridge:send_message(BridgeID, Payload).
    emqx_resource:simple_sync_query(ResourceID, {send_message, Payload}).

clear_table(Config) ->
    query(Config, ?SQL_DELETE).

get_payload(Config) ->
    query(Config, ?SQL_SELECT).

% We need to create and drop the test table outside of using bridges
% since a bridge expects the table to exist when enabling it. We
% therefore call the mysql module directly.
connect_raw_and_run_sql(GroupType, Config, Sql) ->
    Opts = [
        {host, ?config(mysql_host, Config)},
        {port, ?config(mysql_port, Config)},
        {user, ?MYSQL_USERNAME},
        {password, ?MYSQL_PASSWORD},
        {database, ?MYSQL_DATABASE}
    ],
    SslOpts =
        case GroupType of
            tls ->
                [{ssl, emqx_tls_lib:to_client_opts(#{enable => true})}];
            tcp ->
                []
        end,
    {ok, Pid} = mysql:start_link(Opts ++ SslOpts),
    ok = mysql:query(Pid, Sql),
    mysql:stop(Pid).

create_table_raw(GroupType, Config) ->
    connect_raw_and_run_sql(GroupType, Config, ?SQL_CREATE_TABLE).

drop_table_raw(GroupType, Config) ->
    connect_raw_and_run_sql(GroupType, Config, ?SQL_DROP_TABLE).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = integer_to_binary(erlang:unique_integer()),
    ?assertMatch(ok, send_message(Config, #{payload => Val, timestamp => 1668602148000})),
    ?assertMatch(
        {ok, [<<"payload">>], [[Val]]},
        get_payload(Config)
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
    ?assertMatch(ok, send_message(Config, #{payload => Val, timestamp => 1668602148000})),
    ?assertMatch(
        {ok, [<<"payload">>], [[Val]]},
        get_payload(Config)
    ),
    ok.
