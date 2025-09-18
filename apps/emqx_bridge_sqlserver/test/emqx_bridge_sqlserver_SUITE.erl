%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_sqlserver_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, sqlserver).
-define(CONNECTOR_TYPE_BIN, <<"sqlserver">>).
-define(ACTION_TYPE, sqlserver).
-define(ACTION_TYPE_BIN, <<"sqlserver">>).

-define(PROXY_NAME, "sqlserver").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(async, async).
-define(sync, sync).
-define(with_batch, with_batch).
-define(without_batch, without_batch).

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
            emqx,
            emqx_conf,
            emqx_bridge_sqlserver,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    %% Connect to sqlserver directly
    %% drop old db and table, and then create new ones
    try
        connect_and_drop_table(),
        connect_and_drop_db()
    catch
        _:_ ->
            ok
    end,
    connect_and_create_db_and_table(),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?async, TCConfig) ->
    [{query_mode, async} | TCConfig];
init_per_group(?sync, TCConfig) ->
    [{query_mode, sync} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
    connect_and_clear_table(),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    reset_proxy(),
    connect_and_clear_table(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"server">> => <<"toxiproxy:1433">>,
        <<"database">> => <<"mqtt">>,
        <<"username">> => <<"sa">>,
        <<"password">> => <<"mqtt_public1">>,
        <<"pool_size">> => 1,
        <<"driver">> => <<"ms-sql">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"sql">> =>
                iolist_to_binary([
                    <<"insert into t_mqtt_msg(msgid, topic, qos, payload)">>,
                    <<" values ( ${id}, ${topic}, ${qos}, ${payload})">>
                ]),
            <<"undefined_vars_as_null">> => false
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).

connect_direct_sqlserver() ->
    Opts = [
        {host, "sqlserver"},
        {port, 1433},
        {username, "sa"},
        {password, "mqtt_public1"},
        {driver, "ms-sql"},
        {pool_size, 1}
    ],
    {ok, Con} = connect(Opts),
    Con.

connect(Options) ->
    ConnectStr = lists:concat(conn_str(Options, [])),
    Opts = proplists:get_value(options, Options, []),
    odbc:connect(ConnectStr, Opts).

conn_str([], Acc) ->
    lists:join(";", ["Encrypt=YES", "TrustServerCertificate=YES" | Acc]);
conn_str([{driver, Driver} | Opts], Acc) ->
    conn_str(Opts, ["Driver=" ++ str(Driver) | Acc]);
conn_str([{host, Host} | Opts], Acc) ->
    Port = proplists:get_value(port, Opts, "1433"),
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

disconnect(Ref) ->
    odbc:disconnect(Ref).

with_conn(Fn) ->
    Conn = connect_direct_sqlserver(),
    try
        Fn(Conn)
    after
        ok = tdengine:stop(Conn)
    end.

connect_and_create_db_and_table() ->
    SQL1 =
        " IF NOT EXISTS(SELECT name FROM sys.databases WHERE name = 'mqtt')"
        " BEGIN"
        " CREATE DATABASE mqtt;"
        " END",
    SQL2 =
        " CREATE TABLE mqtt.dbo.t_mqtt_msg"
        " (id int PRIMARY KEY IDENTITY(1000000001,1) NOT NULL,"
        " msgid   VARCHAR(64) NULL,"
        " topic   VARCHAR(100) NULL,"
        " qos     tinyint NOT NULL DEFAULT 0,"
        %% use VARCHAR to use utf8 encoding
        %% for default, sqlserver use utf16 encoding NVARCHAR()
        " payload VARCHAR(100) NULL,"
        " arrived DATETIME NOT NULL DEFAULT CURRENT_TIMESTAMP)",
    with_conn(fun(Conn) ->
        {updated, undefined} = directly_query(Conn, SQL1),
        {updated, undefined} = directly_query(Conn, SQL2)
    end).

connect_and_drop_db() ->
    with_conn(fun(Conn) ->
        {updated, undefined} = directly_query(Conn, "DROP DATABASE mqtt")
    end).

connect_and_drop_table() ->
    with_conn(fun(Conn) ->
        {updated, undefined} = directly_query(Conn, "DROP TABLE mqtt.dbo.t_mqtt_msg")
    end).

connect_and_clear_table() ->
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(Conn, "DELETE from mqtt.dbo.t_mqtt_msg")
    end).

connect_and_get_payload() ->
    with_conn(fun(Conn) ->
        {selected, ["payload"], Rows} = directly_query(
            Conn, "SELECT payload FROM mqtt.dbo.t_mqtt_msg"
        ),
        Rows
    end).

connect_and_get_count() ->
    SQL = "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg",
    with_conn(fun(Conn) ->
        {selected, [[]], [{Count}]} = directly_query(Conn, SQL),
        Count
    end).

directly_query(Con, Query) ->
    directly_query(Con, Query, _Timeout = 2_000).

directly_query(Con, Query, Timeout) ->
    odbc:sql_query(Con, Query, Timeout).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_connector_api(TCConfig) ->
    #{connector_type := ConnectorType, connector_name := ConnectorName} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(
        <<
            "select *,"
            " 1668602148000 as timestamp from \"${t}\" "
        >>,
        TCConfig
    ).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    {ok, C} = emqtt:start_link(),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, sqlserver_connector_on_stop).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [Sync, Batch]
     || Sync <- [?sync, ?async],
        Batch <- [?without_batch, ?with_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        PayloadStr = str(Payload),
        ?retry(
            200,
            10,
            ?assertMatch(
                [{PayloadStr}],
                connect_and_get_payload(),
                #{payload => Payload}
            )
        )
    end,
    Opts = #{
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_undefined_vars_as_null(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"select id, qos, timestamp from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertMatch(
                [{null}],
                connect_and_get_payload()
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([#{result := ok}], ?of_kind(sqlserver_connector_query_return, Trace)),
            ok
        end
    ),
    ok.

t_create_disconnected(TCConfig) ->
    with_failure(down, fun() ->
        {201, #{<<"status">> := <<"connecting">>}} = create_connector_api(TCConfig, #{}),
        {201, #{<<"status">> := <<"disconnected">>}} = create_action_api(TCConfig, #{})
    end),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_action_api(TCConfig)
        )
    ),
    ok.

t_create_with_invalid_password(TCConfig) ->
    ?check_trace(
        begin
            ?assertMatch(
                {201, _},
                create_connector_api(TCConfig, #{<<"password">> => <<"wrong_password">>})
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

t_batch_write() ->
    [{matrix, true}].
t_batch_write(matrix) ->
    [[?sync, ?with_batch], [?async, ?with_batch]];
t_batch_write(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    Payloads = [unique_payload() || _ <- lists:seq(1, BatchSize)],
    ?check_trace(
        begin
            ?wait_async_action(
                lists:foreach(
                    fun(Payload) -> emqx:publish(emqx_message:make(Topic, Payload)) end,
                    Payloads
                ),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            %% just assert the data count is correct
            ?assertMatch(
                BatchSize,
                connect_and_get_count()
            ),
            %% assert the data order is correct
            ?assertEqual(
                Payloads,
                [list_to_binary(P) || {P} <- connect_and_get_payload()]
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

t_health_check_return_error(TCConfig) ->
    %% use an ets table for mocked health-check fault injection
    Ets = ets:new(test, [public]),
    true = ets:insert(Ets, {result, {selected, [[]], [{1}]}}),
    meck:new(odbc, [passthrough, no_link, no_history]),
    meck:expect(odbc, sql_query, fun(Conn, SQL) ->
        %% insert the latest odbc connection used in the health check calls
        %% so we can inspect if a new connection is established after
        %% health-check failure
        case SQL of
            "SELECT 1" ->
                true = ets:insert(Ets, {conn, Conn}),
                [{result, R}] = ets:lookup(Ets, result),
                R;
            _ ->
                meck:passthrough([Conn, SQL])
        end
    end),
    try
        {201, _} = create_connector_api(TCConfig, #{}),
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        ),
        [{conn, Conn1}] = ets:lookup(Ets, conn),
        true = ets:insert(Ets, {result, {error, <<"injected failure">>}}),
        ?retry(
            200,
            10,
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                get_connector_api(TCConfig)
            )
        ),
        %% remove injected failure
        true = ets:insert(Ets, {result, {selected, [[]], [{1}]}}),
        ?retry(
            200,
            10,
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_connector_api(TCConfig)
            )
        ),
        [{conn, Conn2}] = ets:lookup(Ets, conn),
        ?assertNotEqual(Conn1, Conn2)
    after
        meck:unload(odbc),
        ets:delete(Ets)
    end,
    ok.

t_missing_data(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<"select missing_stuff from \"${t}\" ">>,
        TCConfig
    ),
    C = start_client(),
    {_, {ok, #{error := Error}}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"hey">>),
            #{?snk_kind := sqlserver_connector_query_return, error := _},
            10_000
        ),
    ExpectedError =
        "[Microsoft][ODBC Driver 18 for SQL Server][SQL Server]"
        "Conversion failed when converting the varchar value 'undefined' to"
        " data type tinyint. SQLSTATE IS: 22018",
    ?assertMatch(
        {unrecoverable_error, {invalid_request, ExpectedError}},
        Error
    ),
    ok.

%% Note: it's not possible to setup a true named instance in linux/docker; we only verify
%% here that the notation for named instances work.  When explicitly defining the port,
%% the ODBC driver will connect to whatever instance is running on said port, regardless
%% of what's given as instance name.
t_named_instance_mismatch(TCConfig) ->
    %% Note: we connect to the default instance here, despite explicitly definining an
    %% instance name, because we're using the default instance port.
    ServerWithInstanceName = iolist_to_binary([
        <<"toxiproxy">>,
        $\\,
        <<"NamedInstance">>,
        $:,
        <<"1433">>
    ]),
    {201, #{<<"status">> := <<"disconnected">>, <<"status_reason">> := Msg}} =
        create_connector_api(TCConfig, #{<<"server">> => ServerWithInstanceName}),
    ?assertMatch(<<"{unhealthy_target,", _/binary>>, Msg),
    ?assertEqual(
        match,
        re:run(
            Msg,
            <<"connected instance does not match desired instance name">>,
            [global, {capture, none}]
        )
    ),
    ok.

t_table_not_found(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{<<"sql">> => <<"insert into i_don_exist(id) values (${id})">>}
        }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {_, {ok, #{error := Error}}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"hey">>),
            #{?snk_kind := sqlserver_connector_query_return, error := _},
            10_000
        ),
    ?assertEqual(
        {unrecoverable_error, {invalid_request, <<"table_or_view_not_found">>}},
        Error
    ),
    ok.

%% Checks that we report the connector as `?status_disconnected` when `ecpool` supervision
%% tree is unhealthy for any reason.
t_ecpool_workers_crash(TCConfig) ->
    ok = emqx_bridge_v2_testlib:t_ecpool_workers_crash(TCConfig),
    ok.
