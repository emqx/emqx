%%--------------------------------------------------------------------
% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_sqlserver_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("../include/emqx_bridge_sqlserver.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% SQL definitions
-define(SQL_BRIDGE,
    "insert into t_mqtt_msg(msgid, topic, qos, payload) values ( ${id}, ${topic}, ${qos}, ${payload})"
).
-define(SQL_SERVER_DRIVER, "ms-sql").

-define(SQL_CREATE_DATABASE_IF_NOT_EXISTS,
    " IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'mqtt')"
    " BEGIN"
    " CREATE DATABASE mqtt;"
    " END"
).

-define(SQL_CREATE_TABLE_IN_DB_MQTT,
    " CREATE TABLE mqtt.dbo.t_mqtt_msg"
    " (id int PRIMARY KEY IDENTITY(1000000001,1) NOT NULL,"
    " msgid   VARCHAR(64) NULL,"
    " topic   VARCHAR(100) NULL,"
    " qos     tinyint NOT NULL DEFAULT 0,"
    %% use VARCHAR to use utf8 encoding
    %% for default, sqlserver use utf16 encoding NVARCHAR()
    " payload VARCHAR(100) NULL,"
    " arrived DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)"
).

-define(SQL_DROP_DB_MQTT, "DROP DATABASE mqtt").
-define(SQL_DROP_TABLE, "DROP TABLE mqtt.dbo.t_mqtt_msg").
-define(SQL_DELETE, "DELETE from mqtt.dbo.t_mqtt_msg").
-define(SQL_SELECT, "SELECT payload FROM mqtt.dbo.t_mqtt_msg").
-define(SQL_SELECT_COUNT, "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg").
% DB defaults
-define(SQL_SERVER_DATABASE, "mqtt").
-define(SQL_SERVER_USERNAME, "sa").
-define(SQL_SERVER_PASSWORD, "mqtt_public1").
-define(BATCH_SIZE, 10).
-define(REQUEST_TIMEOUT_MS, 2_000).

-define(WORKER_POOL_SIZE, 4).

-define(WITH_CON(Process),
    Con = connect_direct_sqlserver(Config),
    Process,
    ok = disconnect(Con)
).

%% How to run it locally (all commands are run in $PROJ_ROOT dir):
%%   A: run ct on host
%%     1. Start all deps services
%%       ```bash
%%       sudo docker compose -f .ci/docker-compose-file/docker-compose.yaml \
%%                           -f .ci/docker-compose-file/docker-compose-sqlserver.yaml \
%%                           -f .ci/docker-compose-file/docker-compose-toxiproxy.yaml \
%%                           up --build
%%       ```
%%
%%     2. Run use cases with special environment variables
%%       11433 is toxiproxy exported port.
%%       Local:
%%       ```bash
%%       SQLSERVER_HOST=toxiproxy SQLSERVER_PORT=11433 \
%%           PROXY_HOST=toxiproxy PROXY_PORT=1433 \
%%           ./rebar3 as test ct -c -v --readable true --name ct@127.0.0.1 \
%%                               --suite apps/emqx_bridge_sqlserver/test/emqx_bridge_sqlserver_SUITE.erl
%%       ```
%%
%%   B: run ct in docker container
%%     run script:
%%     ```bash
%%     ./scripts/ct/run.sh --ci --app apps/emqx_bridge_sqlserver/ -- \
%%                         --name 'test@127.0.0.1' -c -v --readable true \
%%                         --suite apps/emqx_bridge_sqlserver/test/emqx_bridge_sqlserver_SUITE.erl
%%     ````

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, async},
        {group, sync}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    NonBatchCases = [t_write_timeout],
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {async, BatchingGroups},
        {sync, BatchingGroups},
        {with_batch, TCs -- NonBatchCases},
        {without_batch, TCs}
    ].

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
init_per_group(_Group, Config) ->
    Config.

end_per_group(Group, Config) when Group =:= with_batch; Group =:= without_batch ->
    connect_and_drop_table(Config),
    connect_and_drop_db(Config),
    Apps = ?config(apps, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Passfile = filename:join(?config(priv_dir, Config), "passfile"),
    ok = file:write_file(Passfile, <<?SQL_SERVER_PASSWORD>>),
    [{sqlserver_passfile, Passfile} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(_Testcase, Config) ->
    %% drop database and table
    %% connect_and_clear_table(Config),
    %% create a new one
    %% TODO: create a new database for each test case
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
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_setup_via_config_and_publish(Config) ->
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Val = str(erlang:unique_integer()),
    SentData = sent_data(Val),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertMatch(
                [{Val}],
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(sqlserver_connector_query_return, Trace0),
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
    SentData = maps:put(payload, undefined, sent_data("tmp")),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertMatch(
                [{null}],
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(sqlserver_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_setup_via_http_api_and_publish(Config) ->
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    SQLServerConfig0 = ?config(sqlserver_config, Config),
    SQLServerConfig = SQLServerConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType,
        %% NOTE: using literal password with HTTP API requests.
        <<"password">> => <<?SQL_SERVER_PASSWORD>>
    },
    ?assertMatch(
        {ok, _},
        create_bridge_http(SQLServerConfig)
    ),
    Val = str(erlang:unique_integer()),
    SentData = sent_data(Val),
    ?check_trace(
        begin
            ?wait_async_action(
                ?assertEqual(ok, send_message(Config, SentData)),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertMatch(
                [{Val}],
                connect_and_get_payload(Config)
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(sqlserver_connector_query_return, Trace0),
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

    health_check_resource_ok(Config),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        health_check_resource_down(Config)
    end),
    ok.

t_create_disconnected(Config) ->
    ProxyPort = ?config(proxy_port, Config),
    ProxyHost = ?config(proxy_host, Config),
    ProxyName = ?config(proxy_name, Config),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        ?assertMatch({ok, _}, create_bridge(Config)),
        health_check_resource_down(Config)
    end),
    timer:sleep(10_000),
    health_check_resource_ok(Config),
    ok.

t_create_with_invalid_password(Config) ->
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    SQLServerConfig0 = ?config(sqlserver_config, Config),
    SQLServerConfig = SQLServerConfig0#{
        <<"name">> => Name,
        <<"type">> => BridgeType,
        <<"password">> => <<"wrong_password">>
    },
    ?check_trace(
        begin
            ?assertMatch(
                {ok, _},
                create_bridge_http(SQLServerConfig)
            )
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := {start_pool_failed, _, _}}],
                ?of_kind(sqlserver_connector_start_failed, Trace)
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
    Val = str(erlang:unique_integer()),
    SentData = sent_data(Val),
    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            create_bridge(Config),
            #{?snk_kind := resource_connected_enter},
            20_000
        ),
    emqx_common_test_helpers:with_failure(down, ProxyName, ProxyHost, ProxyPort, fun() ->
        case QueryMode of
            sync ->
                ?assertMatch(
                    {error, {resource_error, #{reason := timeout}}},
                    send_message(Config, SentData)
                );
            async ->
                ?assertMatch(
                    ok, send_message(Config, SentData)
                )
        end
    end),
    ok.

t_write_timeout(_Config) ->
    %% msodbc driver handled all connection exceptions
    %% the case is same as t_write_failure/1
    ok.

t_simple_query(Config) ->
    BatchSize = batch_size(Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),

    {Requests, Vals} = gen_batch_req(Config, BatchSize),
    ?check_trace(
        begin
            ?wait_async_action(
                begin
                    [?assertEqual(ok, query_resource(Config, Request)) || Request <- Requests]
                end,
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            %% just assert the data count is correct
            ?assertMatch(
                BatchSize,
                connect_and_get_count(Config)
            ),
            %% assert the data order is correct
            ?assertMatch(
                Vals,
                connect_and_get_payload(Config)
            )
        end,
        fun(Trace0) ->
            Trace = ?of_kind(sqlserver_connector_query_return, Trace0),
            case BatchSize of
                1 ->
                    ?assertMatch([#{result := ok}], Trace);
                _ ->
                    [?assertMatch(#{result := ok}, Trace1) || Trace1 <- Trace]
            end,
            ok
        end
    ),
    ok.

-define(MISSING_TINYINT_ERROR,
    "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
    "Conversion failed when converting the varchar value 'undefined' to data type tinyint. SQLSTATE IS: 22018"
).

t_missing_data(Config) ->
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    case QueryMode of
        sync ->
            ?assertMatch(
                {error, {unrecoverable_error, {invalid_request, ?MISSING_TINYINT_ERROR}}},
                Result
            );
        async ->
            ?assertMatch(
                ok, send_message(Config, #{})
            )
    end,
    ok.

t_bad_parameter(Config) ->
    QueryMode = ?config(query_mode, Config),
    ?assertMatch(
        {ok, _},
        create_bridge(Config)
    ),
    Result = send_message(Config, #{}),
    case QueryMode of
        sync ->
            ?assertMatch(
                {error, {unrecoverable_error, {invalid_request, ?MISSING_TINYINT_ERROR}}},
                Result
            );
        async ->
            ?assertMatch(
                ok, send_message(Config, #{})
            )
    end,
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    Host = os:getenv("SQLSERVER_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("SQLSERVER_PORT", str(?SQLSERVER_DEFAULT_PORT))),

    Config0 = [
        {sqlserver_host, Host},
        {sqlserver_port, Port},
        %% see also for `proxy_name` : $PROJ_ROOT/.ci/docker-compose-file/toxiproxy.json
        {proxy_name, "sqlserver"},
        {batch_size, batch_size(ConfigT)}
        | ConfigT
    ],

    BridgeType = proplists:get_value(bridge_type, Config0, <<"sqlserver">>),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            % Setup toxiproxy
            ProxyHost = os:getenv("PROXY_HOST", "toxiproxy"),
            ProxyPort = list_to_integer(os:getenv("PROXY_PORT", "8474")),
            emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
            Apps = emqx_cth_suite:start(
                [
                    emqx_conf,
                    emqx_bridge_sqlserver,
                    emqx_bridge,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config0)}
            ),
            % Connect to sqlserver directly
            % drop old db and table, and then create new ones
            connect_and_create_db_and_table(Config0),
            {Name, SQLServerConf} = sqlserver_config(BridgeType, Config0),
            Config =
                [
                    {apps, Apps},
                    {sqlserver_config, SQLServerConf},
                    {sqlserver_bridge_type, BridgeType},
                    {sqlserver_name, Name},
                    {proxy_host, ProxyHost},
                    {proxy_port, ProxyPort}
                    | Config0
                ],
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_sqlserver);
                _ ->
                    {skip, no_sqlserver}
            end
    end.

sqlserver_config(BridgeType, Config) ->
    Port = integer_to_list(?config(sqlserver_port, Config)),
    Server = ?config(sqlserver_host, Config) ++ ":" ++ Port,
    Name = atom_to_binary(?MODULE),
    BatchSize = batch_size(Config),
    QueryMode = ?config(query_mode, Config),
    Passfile = ?config(sqlserver_passfile, Config),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  server = ~p\n"
            "  database = ~p\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "  sql = ~p\n"
            "  driver = ~p\n"
            "  resource_opts = {\n"
            "    request_ttl = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "    worker_pool_size = ~b\n"
            "  }\n"
            "}",
            [
                BridgeType,
                Name,
                Server,
                ?SQL_SERVER_DATABASE,
                ?SQL_SERVER_USERNAME,
                "file://" ++ Passfile,
                ?SQL_BRIDGE,
                ?SQL_SERVER_DRIVER,
                BatchSize,
                QueryMode,
                ?WORKER_POOL_SIZE
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
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    SSConfig0 = ?config(sqlserver_config, Config),
    SSConfig = emqx_utils_maps:deep_merge(SSConfig0, Overrides),
    emqx_bridge:create(BridgeType, Name, SSConfig).

delete_bridge(Config) ->
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    emqx_bridge:remove(BridgeType, Name).

create_bridge_http(Params) ->
    Path = emqx_mgmt_api_test_util:api_path(["bridges"]),
    AuthHeader = emqx_mgmt_api_test_util:auth_header_(),
    case emqx_mgmt_api_test_util:request_api(post, Path, "", AuthHeader, Params) of
        {ok, Res} -> {ok, emqx_utils_json:decode(Res, [return_maps])};
        Error -> Error
    end.

send_message(Config, Payload) ->
    Name = ?config(sqlserver_name, Config),
    BridgeType = ?config(sqlserver_bridge_type, Config),
    ActionId = emqx_bridge_v2:id(BridgeType, Name),
    emqx_bridge_v2:query(BridgeType, Name, {ActionId, Payload}, #{}).

query_resource(Config, Request) ->
    Name = ?config(sqlserver_name, Config),
    BridgeType = ?config(sqlserver_bridge_type, Config),
    ID = emqx_bridge_v2:id(BridgeType, Name),
    ResID = emqx_connector_resource:resource_id(BridgeType, Name),
    emqx_resource:query(ID, Request, #{timeout => 1_000, connector_resource_id => ResID}).

query_resource_async(Config, Request) ->
    Name = ?config(sqlserver_name, Config),
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Ref = alias([reply]),
    AsyncReplyFun = fun(Result) -> Ref ! {result, Ref, Result} end,
    ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name),
    Return = emqx_resource:query(ResourceID, Request, #{
        timeout => 500, async_reply_fun => {AsyncReplyFun, []}
    }),
    {Return, Ref}.

resource_id(Config) ->
    Name = ?config(sqlserver_name, Config),
    BridgeType = ?config(sqlserver_bridge_type, Config),
    _ResourceID = emqx_bridge_resource:resource_id(BridgeType, Name).

health_check_resource_ok(Config) ->
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    % Wait for reconnection.
    ?retry(
        _Sleep = 1_000,
        _Attempts = 10,
        begin
            ?assertEqual({ok, connected}, emqx_resource_manager:health_check(resource_id(Config))),
            ?assertMatch(#{status := connected}, emqx_bridge_v2:health_check(BridgeType, Name))
        end
    ).

health_check_resource_down(Config) ->
    case emqx_resource_manager:health_check(resource_id(Config)) of
        {ok, Status} when Status =:= disconnected orelse Status =:= connecting ->
            ok;
        {error, timeout} ->
            ok;
        Other ->
            ?assert(
                false, lists:flatten(io_lib:format("invalid health check result:~p~n", [Other]))
            )
    end.

receive_result(Ref, Timeout) ->
    receive
        {result, Ref, Result} ->
            {ok, Result};
        {Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

connect_direct_sqlserver(Config) ->
    Opts = [
        {host, ?config(sqlserver_host, Config)},
        {port, ?config(sqlserver_port, Config)},
        {username, ?SQL_SERVER_USERNAME},
        {password, ?SQL_SERVER_PASSWORD},
        {driver, ?SQL_SERVER_DRIVER},
        {pool_size, 8}
    ],
    {ok, Con} = connect(Opts),
    Con.

connect(Options) ->
    ConnectStr = lists:concat(conn_str(Options, [])),
    Opts = proplists:get_value(options, Options, []),
    odbc:connect(ConnectStr, Opts).

disconnect(Ref) ->
    odbc:disconnect(Ref).

% These funs connect and then stop the sqlserver connection
connect_and_create_db_and_table(Config) ->
    ?WITH_CON(begin
        {updated, undefined} = directly_query(Con, ?SQL_CREATE_DATABASE_IF_NOT_EXISTS),
        {updated, undefined} = directly_query(Con, ?SQL_CREATE_TABLE_IN_DB_MQTT)
    end).

connect_and_drop_db(Config) ->
    ?WITH_CON({updated, undefined} = directly_query(Con, ?SQL_DROP_DB_MQTT)).

connect_and_drop_table(Config) ->
    ?WITH_CON({updated, undefined} = directly_query(Con, ?SQL_DROP_TABLE)).

connect_and_clear_table(Config) ->
    ?WITH_CON({updated, _} = directly_query(Con, ?SQL_DELETE)).

connect_and_get_payload(Config) ->
    ?WITH_CON(
        {selected, ["payload"], Rows} = directly_query(Con, ?SQL_SELECT)
    ),
    Rows.

connect_and_get_count(Config) ->
    ?WITH_CON(
        {selected, [[]], [{Count}]} = directly_query(Con, ?SQL_SELECT_COUNT)
    ),
    Count.

directly_query(Con, Query) ->
    directly_query(Con, Query, ?REQUEST_TIMEOUT_MS).

directly_query(Con, Query, Timeout) ->
    odbc:sql_query(Con, Query, Timeout).

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

batch_size(Config) ->
    case ?config(enable_batch, Config) of
        true -> ?BATCH_SIZE;
        false -> 1
    end.

conn_str([], Acc) ->
    lists:join(";", ["Encrypt=YES", "TrustServerCertificate=YES" | Acc]);
conn_str([{driver, Driver} | Opts], Acc) ->
    conn_str(Opts, ["Driver=" ++ str(Driver) | Acc]);
conn_str([{host, Host} | Opts], Acc) ->
    Port = proplists:get_value(port, Opts, str(?SQLSERVER_DEFAULT_PORT)),
    NOpts = proplists:delete(port, Opts),
    conn_str(NOpts, ["Server=" ++ str(Host) ++ "," ++ str(Port) | Acc]);
conn_str([{port, Port} | Opts], Acc) ->
    Host = proplists:get_value(host, Opts, "localhost"),
    NOpts = proplists:delete(host, Opts),
    conn_str(NOpts, ["Server=" ++ str(Host) ++ "," ++ str(Port) | Acc]);
conn_str([{database, Database} | Opts], Acc) ->
    conn_str(Opts, ["Database=" ++ str(Database) | Acc]);
conn_str([{username, Username} | Opts], Acc) ->
    conn_str(Opts, ["UID=" ++ str(Username) | Acc]);
conn_str([{password, Password} | Opts], Acc) ->
    conn_str(Opts, ["PWD=" ++ str(Password) | Acc]);
conn_str([{_, _} | Opts], Acc) ->
    conn_str(Opts, Acc).

sent_data(Payload) ->
    #{
        payload => to_bin(Payload),
        id => <<"0005F8F84FFFAFB9F44200000D810002">>,
        topic => <<"test/topic">>,
        qos => 0
    }.

gen_batch_req(Config, Count) when
    is_integer(Count) andalso Count > 0
->
    BridgeType = ?config(sqlserver_bridge_type, Config),
    Name = ?config(sqlserver_name, Config),
    ActionId = emqx_bridge_v2:id(BridgeType, Name),
    Vals = [{str(erlang:unique_integer())} || _Seq <- lists:seq(1, Count)],
    Requests = [{ActionId, sent_data(Payload)} || {Payload} <- Vals],
    {Requests, Vals};
gen_batch_req(_Config, Count) ->
    ct:pal("Gen batch requests failed with unexpected Count: ~p", [Count]).

str(List) when is_list(List) ->
    unicode:characters_to_list(List, utf8);
str(Bin) when is_binary(Bin) ->
    unicode:characters_to_list(Bin, utf8);
str(Num) when is_number(Num) ->
    number_to_list(Num).

number_to_list(Int) when is_integer(Int) ->
    integer_to_list(Int);
number_to_list(Float) when is_float(Float) ->
    float_to_list(Float, [{decimals, 10}, compact]).

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Bin) when is_binary(Bin) ->
    Bin.
