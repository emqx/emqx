%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
init_per_group(null_values, TCConfig) ->
    [{undefined_vars_as_null, true} | TCConfig];
init_per_group(undefined_values, TCConfig) ->
    [{undefined_vars_as_null, false} | TCConfig];
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

-doc "Preserve INSERT templates with identifiers that are also UPDATE keywords.".
t_insert_keyword_identifiers() ->
    [{matrix, true}].
t_insert_keyword_identifiers(matrix) ->
    [[Sync, Batch] || Sync <- [?sync, ?async], Batch <- [?without_batch, ?with_batch]];
t_insert_keyword_identifiers(TCConfig) ->
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(Conn, """
        CREATE TABLE mqtt.dbo.[where] ([set] VARCHAR(100), [update] VARCHAR(256))
        """)
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.[where]") end)
    end),
    BatchSize = min(2, get_config(batch_size, TCConfig)),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => BatchSize},
        <<"parameters">> => #{
            <<"sql">> =>
                <<"INSERT INTO mqtt.dbo.where(set, update) VALUES (${payload}, ${topic})">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    lists:foreach(fun(_) -> emqtt:publish(C, Topic, <<"online">>) end, lists:seq(1, BatchSize)),
    Expected = lists:duplicate(BatchSize, {"online", str(Topic)}),
    ?retry(
        200,
        20,
        ?assertEqual(
            Expected,
            with_conn(fun(Conn) ->
                {selected, _, Rows} = directly_query(
                    Conn, "SELECT [set], [update] FROM mqtt.dbo.[where]"
                ),
                Rows
            end)
        )
    ),
    ok.

-doc "Update existing rows through a rule in sync and async modes with both missing-value policies.".
t_update() ->
    [{matrix, true}].
t_update(matrix) ->
    [[Sync, Nulls] || Sync <- [?sync, ?async], Nulls <- [null_values, undefined_values]];
t_update(TCConfig) ->
    Nulls = get_config(undefined_vars_as_null, TCConfig),
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(Conn, """
        CREATE TABLE mqtt.dbo.t_update_points (
            device NVARCHAR(100), point INT, status NVARCHAR(100), updates INT);
        """)
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_update_points") end)
    end),
    with_conn(fun(Conn) ->
        {updated, 3} = directly_query(Conn, """
        INSERT INTO mqtt.dbo.t_update_points VALUES
            (N'device''1', 1, N'old', 0),
            (N'device''1', 2, N'old', 0),
            (N'device2', 1, N'old', 0)
        """)
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    Overrides = #{
        <<"resource_opts">> => #{<<"batch_size">> => 1},
        <<"parameters">> => #{
            <<"undefined_vars_as_null">> => Nulls,
            <<"sql">> => <<"""
            UPDATE mqtt.dbo.t_update_points
            SET [status] = CASE WHEN ${payload.value} IS NULL THEN NULL ELSE N'${payload.value}' END,
                [updates] = [updates] + 1
            WHERE [device] = ${payload.key} AND [point] = ${payload.point} AND [updates] < 10
            """>>
        }
    },
    ?assertMatch({204, _}, emqx_bridge_v2_testlib:probe_bridge_api_simple(TCConfig, Overrides)),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, Overrides),
    #{topic := Topic} = simple_create_rule_api(
        <<"""
        SELECT json_decode(payload) AS payload FROM "${t}"
        """>>,
        TCConfig
    ),
    C = start_client(),
    MissingPredicate =
        case Nulls of
            true -> <<"status IS NULL">>;
            false -> <<"status = N'undefined'">>
        end,
    UnicodeValue = <<"在线😀'); DROP TABLE t_update_points; --"/utf8>>,
    %% Keep the assertion independent of ODBC's encoding of SQL text.
    UnicodeHex = binary:encode_hex(
        unicode:characters_to_binary(UnicodeValue, utf8, {utf16, little})
    ),
    UnicodePredicate = <<"status = CONVERT(NVARCHAR(MAX), 0x", UnicodeHex/binary, ")">>,
    Cases = [
        {#{value => <<"online">>}, <<"status = N'online'">>},
        {#{value => UnicodeValue}, UnicodePredicate},
        {#{value => null}, <<"status IS NULL">>},
        {#{}, MissingPredicate}
    ],
    lists:foldl(
        fun({Values, Predicate}, Count) ->
            Payload = emqx_utils_json:encode(Values#{key => <<"device'1">>, point => 1}),
            emqtt:publish(C, Topic, Payload),
            CheckSQL = iolist_to_binary([
                "SELECT COUNT(*) FROM mqtt.dbo.t_update_points WHERE device = N'device''1' ",
                "AND point = 1 AND updates = ",
                integer_to_binary(Count),
                " AND ",
                Predicate
            ]),
            ?retry(200, 20, ?assertEqual(1, update_count(CheckSQL))),
            ?assertEqual(
                2,
                update_count(
                    "SELECT COUNT(*) FROM mqtt.dbo.t_update_points WHERE status = N'old' AND updates = 0"
                )
            ),
            Count + 1
        end,
        1,
        Cases
    ),
    NoMatchKeys = [#{key => <<"anything' OR 1 = 1 --">>}, #{key => null}, #{}],
    lists:foreach(
        fun(Keys) ->
            Payload = emqx_utils_json:encode(Keys#{value => <<"unexpected">>, point => 1}),
            ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := sqlserver_connector_query_return, result := ok},
                10_000
            ),
            assert_update_rows_unchanged()
        end,
        NoMatchKeys
    ),
    NulCases = [
        {#{key => <<"device'1">>, value => <<"new", 0>>}, "payload.value"},
        {#{key => <<"device'1", 0>>, value => <<"new">>}, "payload.key"}
    ],
    lists:foreach(
        fun({Values, Placeholder}) ->
            Payload = emqx_utils_json:encode(Values#{point => 1}),
            {_, {ok, #{error := Error}}} = ?wait_async_action(
                emqtt:publish(C, Topic, Payload),
                #{?snk_kind := sqlserver_connector_query_return, error := _},
                10_000
            ),
            ?assertMatch(
                {unrecoverable_error,
                    {invalid_sql_template_value, #{
                        placeholder := Placeholder, reason := {error, nul_character_not_allowed}
                    }}},
                Error
            ),
            assert_update_rows_unchanged()
        end,
        NulCases
    ),
    ok.

assert_update_rows_unchanged() ->
    ?assertEqual(
        1,
        update_count(
            "SELECT COUNT(*) FROM mqtt.dbo.t_update_points WHERE updates = 4"
        )
    ),
    ?assertEqual(
        2,
        update_count(
            "SELECT COUNT(*) FROM mqtt.dbo.t_update_points WHERE status = N'old' AND updates = 0"
        )
    ).

update_count(SQL) ->
    with_conn(fun(Conn) ->
        {selected, _, [{Count}]} = directly_query(Conn, str(SQL)),
        Count
    end).

-doc "Preserve Unicode values and keys in UPDATE and single/batch INSERT, including long SQL.".
t_unicode_queries() ->
    [{matrix, true}].
t_unicode_queries(matrix) ->
    [[?sync], [?async]];
t_unicode_queries(TCConfig) ->
    Device = <<"设备😀'1"/utf8>>,
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(Conn, """
        CREATE TABLE mqtt.dbo.t_unicode_points (device NVARCHAR(100) PRIMARY KEY, status NVARCHAR(MAX))
        """),
        {updated, 2} = directly_query(
            Conn,
            str(
                iolist_to_binary([
                    "INSERT mqtt.dbo.t_unicode_points VALUES (",
                    unicode_sql_literal(Device),
                    ",N'old'),(N'other',N'old')"
                ])
            )
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_unicode_points") end)
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 1},
        <<"parameters">> => #{
            <<"sql">> =>
                <<"UPDATE t_unicode_points SET status=${payload.value} WHERE device=${payload.device}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"""
        SELECT json_decode(payload) AS payload FROM "${t}"
        """>>,
        TCConfig
    ),
    C = start_client(),
    Values = [
        <<"在线😀'ready"/utf8>>, binary:copy(<<"界"/utf8>>, 4001), binary:copy(<<"😀"/utf8>>, 4000)
    ],
    lists:foreach(
        fun(Value) ->
            emqtt:publish(C, Topic, emqx_utils_json:encode(#{device => Device, value => Value})),
            ?retry(200, 30, ?assertEqual(1, unicode_point_count(Device, Value)))
        end,
        Values
    ),
    emqtt:publish(C, Topic, emqx_utils_json:encode(#{device => Device, value => null})),
    NullSQL = iolist_to_binary([
        "SELECT COUNT(*) FROM mqtt.dbo.t_unicode_points WHERE device=",
        unicode_sql_literal(Device),
        " AND status IS NULL"
    ]),
    ?retry(200, 30, ?assertEqual(1, update_count(NullSQL))),
    InsertSQL =
        <<"INSERT INTO t_unicode_points(device,status) VALUES (${payload.device},${payload.value})">>,
    {200, #{<<"status">> := <<"connected">>}} = emqx_bridge_v2_testlib:update_bridge_api2(
        TCConfig, #{
            <<"resource_opts">> => #{<<"batch_size">> => 1},
            <<"parameters">> => #{<<"sql">> => InsertSQL}
        }
    ),
    InsertDevice = <<"新设备😀"/utf8>>,
    InsertValue = <<"新状态😀'"/utf8>>,
    emqtt:publish(
        C, Topic, emqx_utils_json:encode(#{device => InsertDevice, value => InsertValue})
    ),
    ?retry(200, 30, ?assertEqual(1, unicode_point_count(InsertDevice, InsertValue))),
    {_, {ok, #{error := DuplicateError}}} = ?wait_async_action(
        emqtt:publish(
            C, Topic, emqx_utils_json:encode(#{device => InsertDevice, value => InsertValue})
        ),
        #{?snk_kind := sqlserver_connector_query_return, error := _},
        10_000
    ),
    ?assertMatch({unrecoverable_error, {invalid_request, _}}, DuplicateError),
    ?assertEqual(1, unicode_point_count(InsertDevice, InsertValue)),
    {200, #{<<"status">> := <<"connected">>}} = emqx_bridge_v2_testlib:update_bridge_api2(
        TCConfig, #{
            <<"resource_opts">> => #{<<"batch_size">> => 2, <<"batch_time">> => <<"1s">>},
            <<"parameters">> => #{<<"sql">> => InsertSQL}
        }
    ),
    BatchDevices = [<<"批量😀A"/utf8>>, <<"批量😀B"/utf8>>],
    lists:foreach(
        fun(Key) ->
            emqtt:publish(C, Topic, emqx_utils_json:encode(#{device => Key, value => InsertValue}))
        end,
        BatchDevices
    ),
    lists:foreach(
        fun(Key) ->
            ?retry(200, 30, ?assertEqual(1, unicode_point_count(Key, InsertValue)))
        end,
        BatchDevices
    ),
    ?assertEqual(1, unicode_point_count(<<"other">>, <<"old">>)),
    ok.

-doc "Reject invalid UTF-8 strings before executing single statements or mixed batches.".
t_invalid_utf8_queries() ->
    [{matrix, true}].
t_invalid_utf8_queries(matrix) ->
    [[?sync], [?async]];
t_invalid_utf8_queries(TCConfig) ->
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(
            Conn,
            "CREATE TABLE mqtt.dbo.t_utf8_guard (id INT, status NVARCHAR(100))"
        ),
        {updated, 1} = directly_query(Conn, "INSERT mqtt.dbo.t_utf8_guard VALUES (0,N'old')"),
        ?assertEqual(
            {error,
                {unrecoverable_error,
                    {invalid_request, <<"SQL statement contains invalid UTF-8">>}}},
            emqx_bridge_sqlserver_connector:worker_do_insert(
                Conn,
                <<"UPDATE mqtt.dbo.t_utf8_guard SET status='", 16#FF, "' WHERE id=0">>,
                #{resource_opts => #{}, pool_name => ?FUNCTION_NAME}
            )
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_utf8_guard") end)
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 1, <<"worker_pool_size">> => 1},
        <<"parameters">> => #{<<"sql">> => <<"INSERT INTO t_utf8_guard VALUES (1,${payload})">>}
    }),
    #{topic := Topic} = simple_create_rule_api(<<"SELECT payload FROM \"${t}\"">>, TCConfig),
    C = start_client(),
    C2 = start_client(),
    ActionId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    Expressions = [<<"${payload}">>, <<"'pre${payload}post'">>, <<"N'pre${payload}post'">>],
    lists:foreach(
        fun(Expression) ->
            lists:foreach(
                fun(SQL) ->
                    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
                        <<"parameters">> => #{<<"sql">> => SQL}
                    }),
                    lists:foreach(
                        fun(Value) ->
                            {_, {ok, #{error := Error}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, Value),
                                #{?snk_kind := sqlserver_connector_query_return, error := _},
                                10_000
                            ),
                            ?assertMatch(
                                {unrecoverable_error,
                                    {invalid_sql_template_value, #{
                                        reason := {error, invalid_utf8}
                                    }}},
                                Error
                            ),
                            assert_utf8_guard_unchanged()
                        end,
                        [<<16#FF>>, <<16#C3>>]
                    )
                end,
                [
                    <<"INSERT INTO t_utf8_guard VALUES (1,", Expression/binary, ")">>,
                    <<"UPDATE t_utf8_guard SET status=", Expression/binary, " WHERE id=0">>
                ]
            )
        end,
        Expressions
    ),
    lists:foreach(
        fun(Expression) ->
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
                <<"resource_opts">> => #{<<"batch_size">> => 2, <<"batch_time">> => <<"5s">>},
                <<"parameters">> => #{
                    <<"sql">> => <<"INSERT INTO t_utf8_guard VALUES (1,", Expression/binary, ")">>
                }
            }),
            FailedBefore = emqx_resource_metrics:failed_get(ActionId),
            lists:foreach(
                fun({Values, BadIndex}) ->
                    {_, {ok, #{error := Error}}} = ?wait_async_action(
                        publish_utf8_pair(C, C2, Topic, Values, ActionId),
                        #{?snk_kind := sqlserver_connector_query_return, error := _},
                        10_000
                    ),
                    ?assertMatch(
                        {unrecoverable_error,
                            {sqlserver_template_render_failed, #{
                                batch_index := BadIndex,
                                reason :=
                                    {invalid_sql_template_value, #{reason := {error, invalid_utf8}}}
                            }}},
                        Error
                    ),
                    assert_utf8_guard_unchanged()
                end,
                [
                    {[<<"中文😀"/utf8>>, <<16#FF>>], 2},
                    {[<<16#FF>>, <<"中文😀"/utf8>>], 1}
                ]
            ),
            ?retry(
                200, 20, ?assertEqual(FailedBefore + 4, emqx_resource_metrics:failed_get(ActionId))
            ),
            ?assertEqual(0, emqx_resource_metrics:success_get(ActionId))
        end,
        Expressions
    ),
    %% A rejected batch must not prevent subsequent valid Unicode batches from executing.
    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
        <<"parameters">> => #{<<"sql">> => <<"INSERT INTO t_utf8_guard VALUES (1,${payload})">>}
    }),
    publish_utf8_pair(C, C2, Topic, lists:duplicate(2, <<"中文😀"/utf8>>), ActionId),
    SQL = iolist_to_binary([
        "SELECT COUNT(*) FROM mqtt.dbo.t_utf8_guard WHERE id=1 AND status=",
        unicode_sql_literal(<<"中文😀"/utf8>>)
    ]),
    ?retry(200, 30, ?assertEqual(2, update_count(SQL))),
    ?assertEqual(3, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_utf8_guard")),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ActionId)),
    ?retry(200, 20, ?assertEqual(2, emqx_resource_metrics:success_get(ActionId))),
    ok.

publish_utf8_pair(C1, C2, Topic, [First, Second], ActionId) ->
    %% Separate connections let a sync action enqueue both messages before its batch timer expires.
    {_, {ok, _}} = ?wait_async_action(
        emqtt:publish(C1, Topic, First),
        #{?snk_kind := buffer_worker_appended_to_queue, id := ActionId},
        2_000
    ),
    emqtt:publish(C2, Topic, Second).

assert_utf8_guard_unchanged() ->
    ?assertEqual(1, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_utf8_guard")),
    ?assertEqual(
        1, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_utf8_guard WHERE id=0 AND status=N'old'")
    ).

-doc "Unicode UPDATE with no matching row is successful.".
t_unicode_zero_rows() ->
    [{matrix, true}].
t_unicode_zero_rows(matrix) ->
    [[?sync], [?async]];
t_unicode_zero_rows(TCConfig) ->
    create_result_fixture("t_zero_rows"),
    {C, Topic} = create_result_action(
        TCConfig,
        <<"UPDATE t_zero_rows SET status=${payload} WHERE id=9">>
    ),
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        emqtt:publish(C, Topic, <<"中文😀"/utf8>>),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    ?assertEqual(1, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_zero_rows WHERE status=N'old'")),
    ActionId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    ?retry(200, 20, ?assertEqual(1, emqx_resource_metrics:success_get(ActionId))),
    ?assertEqual(0, emqx_resource_metrics:failed_get(ActionId)).

-doc "Consume all trigger results and report late errors for INSERT and UPDATE.".
t_trigger_results() ->
    [{matrix, true}].
t_trigger_results(matrix) ->
    [[?sync], [?async]];
t_trigger_results(TCConfig) ->
    create_result_fixture("t_trigger_results"),
    {C, Topic} = create_result_action(
        TCConfig,
        <<"UPDATE t_trigger_results SET status=${payload} WHERE id=0">>
    ),
    ActionId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    lists:foreach(
        fun(NoCount) ->
            with_conn(fun(Conn) ->
                directly_query(Conn, "USE mqtt"),
                {updated, _} = directly_query(Conn, "DROP TRIGGER IF EXISTS dbo.result_guard"),
                {updated, _} = directly_query(
                    Conn,
                    lists:concat([
                        "CREATE TRIGGER dbo.result_guard ON dbo.t_trigger_results AFTER INSERT,UPDATE AS BEGIN ",
                        "SET NOCOUNT ",
                        NoCount,
                        "; SELECT 1 AS debug_result; SELECT 2 AS another_result; ",
                        "IF EXISTS(SELECT 1 FROM inserted WHERE status LIKE N'reject%') ",
                        "THROW 51000, 'result guard rejected the write', 1; END"
                    ])
                )
            end),
            lists:foreach(
                fun(SQL) ->
                    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
                        <<"resource_opts">> => #{
                            <<"batch_size">> => 1, <<"batch_time">> => <<"0ms">>
                        },
                        <<"parameters">> => #{<<"sql">> => SQL}
                    }),
                    lists:foreach(
                        fun(Value) ->
                            Before = update_count(
                                "SELECT COUNT(*) FROM mqtt.dbo.t_trigger_results"
                            ),
                            FailedBefore = emqx_resource_metrics:failed_get(ActionId),
                            SuccessBefore = emqx_resource_metrics:success_get(ActionId),
                            {_, {ok, #{error := Error}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, Value),
                                #{?snk_kind := sqlserver_connector_query_return},
                                10_000
                            ),
                            ?assertMatch({unrecoverable_error, {invalid_request, _}}, Error),
                            {unrecoverable_error, {invalid_request, Reason}} = Error,
                            ?assertMatch(
                                {match, _}, re:run(Reason, "result guard rejected the write")
                            ),
                            ?assertEqual(
                                Before,
                                update_count("SELECT COUNT(*) FROM mqtt.dbo.t_trigger_results")
                            ),
                            ?assertEqual(0, exact_value_count("t_trigger_results", Value)),
                            ?retry(
                                200,
                                20,
                                ?assertEqual(
                                    FailedBefore + 1, emqx_resource_metrics:failed_get(ActionId)
                                )
                            ),
                            ?assertEqual(SuccessBefore, emqx_resource_metrics:success_get(ActionId))
                        end,
                        [<<"reject😀"/utf8>>, <<"reject-ascii">>]
                    ),
                    lists:foreach(
                        fun(Value) ->
                            {_, {ok, #{result := ok}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, Value),
                                #{?snk_kind := sqlserver_connector_query_return},
                                10_000
                            ),
                            ?assert(exact_value_count("t_trigger_results", Value) > 0)
                        end,
                        [<<"accept😀"/utf8>>, <<"accept-ascii">>]
                    )
                end,
                [
                    <<"UPDATE t_trigger_results SET status=${payload} WHERE id=0">>,
                    <<"INSERT INTO t_trigger_results VALUES (1,${payload})">>
                ]
            )
        end,
        ["ON", "OFF"]
    ),
    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 2, <<"batch_time">> => <<"5s">>},
        <<"parameters">> => #{
            <<"sql">> => <<"INSERT INTO t_trigger_results VALUES (1,${payload})">>
        }
    }),
    C2 = start_client(),
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        publish_utf8_pair(C, C2, Topic, [<<"batch😀"/utf8>>, <<"batch-ascii">>], ActionId),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    ?assertEqual(1, exact_value_count("t_trigger_results", <<"batch😀"/utf8>>)),
    ?assertEqual(1, exact_value_count("t_trigger_results", <<"batch-ascii">>)),
    Before = update_count("SELECT COUNT(*) FROM mqtt.dbo.t_trigger_results"),
    {_, {ok, #{error := {unrecoverable_error, {invalid_request, _}}}}} = ?wait_async_action(
        publish_utf8_pair(C, C2, Topic, [<<"reject-batch😀"/utf8>>, <<"rolled-back">>], ActionId),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    ?assertEqual(Before, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_trigger_results")),
    ?retry(200, 20, ?assertEqual(2, emqx_resource_metrics:success_get(ActionId))),
    ?retry(200, 20, ?assertEqual(2, emqx_resource_metrics:failed_get(ActionId))).

-doc "Preserve backslashes and line endings in string values byte for byte.".
t_string_line_endings() ->
    [{matrix, true}].
t_string_line_endings(matrix) ->
    [[?sync], [?async]];
t_string_line_endings(TCConfig) ->
    create_result_fixture("t_line_endings"),
    with_conn(fun(Conn) ->
        directly_query(Conn, "INSERT INTO mqtt.dbo.t_line_endings VALUES (99,N'protected')")
    end),
    {C, Topic} = create_result_action(
        TCConfig,
        <<"UPDATE t_line_endings SET status=${payload} WHERE id=0">>
    ),
    lists:foreach(
        fun({Expression, Prefix, Suffix, Unicode}) ->
            lists:foreach(
                fun(SQL) ->
                    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
                        <<"parameters">> => #{<<"sql">> => SQL}
                    }),
                    Values0 = [
                        <<"a\\\nb">>,
                        <<"a\\\r\nb">>,
                        <<"a\\\rb">>,
                        <<"a\\\\\nb">>,
                        <<"a\nb">>,
                        <<"a\r\nb">>,
                        <<"'\\\n'); DELETE FROM t_line_endings; --">>
                    ],
                    Values =
                        case Unicode of
                            true ->
                                [
                                    <<"中文😀\\\n'"/utf8>>,
                                    binary:copy(<<"界\\\r\n"/utf8>>, 4001)
                                    | Values0
                                ];
                            false ->
                                Values0
                        end,
                    lists:foreach(
                        fun(Value) ->
                            with_conn(fun(Conn) ->
                                directly_query(
                                    Conn, "DELETE FROM mqtt.dbo.t_line_endings WHERE id=1"
                                ),
                                directly_query(
                                    Conn,
                                    "UPDATE mqtt.dbo.t_line_endings SET status=N'old' WHERE id=0"
                                )
                            end),
                            {_, {ok, #{result := ok}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, Value),
                                #{?snk_kind := sqlserver_connector_query_return},
                                10_000
                            ),
                            Expected = <<Prefix/binary, Value/binary, Suffix/binary>>,
                            ?assertEqual(1, exact_value_count("t_line_endings", Expected)),
                            ?assertEqual(
                                1,
                                update_count(
                                    "SELECT COUNT(*) FROM mqtt.dbo.t_line_endings WHERE id=99 AND status=N'protected'"
                                )
                            )
                        end,
                        Values
                    )
                end,
                [
                    <<"UPDATE t_line_endings SET status=", Expression/binary, " WHERE id=0">>,
                    <<"INSERT INTO t_line_endings VALUES (1,", Expression/binary, ")">>
                ]
            )
        end,
        [
            {<<"${payload}">>, <<>>, <<>>, true},
            {<<"'pre${payload}post'">>, <<"pre">>, <<"post">>, false},
            {<<"N'pre${payload}post'">>, <<"pre">>, <<"post">>, true}
        ]
    ),
    with_conn(fun(Conn) ->
        directly_query(Conn, "DELETE FROM mqtt.dbo.t_line_endings WHERE id=1"),
        directly_query(Conn, "UPDATE mqtt.dbo.t_line_endings SET status=N'old' WHERE id=0")
    end),
    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 2, <<"batch_time">> => <<"5s">>},
        <<"parameters">> => #{<<"sql">> => <<"INSERT INTO t_line_endings VALUES (1,${payload})">>}
    }),
    Values = [<<"batch\\\nvalue">>, <<"批量😀\\\r\n"/utf8>>],
    ActionId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    C2 = start_client(),
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        publish_utf8_pair(C, C2, Topic, Values, ActionId),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    [?assertEqual(1, exact_value_count("t_line_endings", V)) || V <- Values],
    lists:foreach(
        fun({SQL, Value, Expected}) ->
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, #{
                <<"resource_opts">> => #{<<"batch_size">> => 1, <<"batch_time">> => <<"0ms">>},
                <<"parameters">> => #{<<"sql">> => SQL}
            }),
            {_, {ok, #{result := ok}}} = ?wait_async_action(
                emqtt:publish(C, Topic, Value),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertEqual(1, exact_value_count("t_line_endings", Expected))
        end,
        [
            {<<"UPDATE t_line_endings SET status=N'${payload}\nb' WHERE id=0">>, <<"a\\">>,
                <<"a\\\nb">>},
            {<<"UPDATE t_line_endings SET status=N'a\\\nb' WHERE id=0">>, <<"ignored">>, <<"ab">>}
        ]
    ).

create_result_fixture(Table) ->
    with_conn(fun(Conn) ->
        {updated, _} = directly_query(
            Conn,
            "CREATE TABLE mqtt.dbo." ++ Table ++ " (id INT, status NVARCHAR(MAX))"
        ),
        {updated, 1} = directly_query(
            Conn, "INSERT INTO mqtt.dbo." ++ Table ++ " VALUES (0,N'old')"
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo." ++ Table) end)
    end).

create_result_action(TCConfig, SQL) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 1, <<"worker_pool_size">> => 1},
        <<"parameters">> => #{<<"sql">> => SQL}
    }),
    #{topic := Topic} = simple_create_rule_api(<<"SELECT payload FROM \"${t}\"">>, TCConfig),
    {start_client(), Topic}.

exact_value_count(Table, Value) ->
    Hex = binary:encode_hex(unicode:characters_to_binary(Value, utf8, {utf16, little})),
    update_count(
        iolist_to_binary([
            "SELECT COUNT(*) FROM mqtt.dbo.", Table, " WHERE CONVERT(VARBINARY(MAX),status)=0x", Hex
        ])
    ).

-doc "Verify typed values across INSERT/UPDATE, string styles, batching and NULL policies.".
t_typed_value_matrix() -> [{matrix, true}].
t_typed_value_matrix(matrix) ->
    [[?sync], [?async]];
t_typed_value_matrix(TCConfig) ->
    try
        typed_matrix(TCConfig, get_config(query_mode, TCConfig))
    after
        catch typed_sql("DROP TABLE IF EXISTS mqtt.dbo.deep_matrix"),
        case erase(typed_matrix_connection) of
            undefined -> ok;
            Conn -> odbc:disconnect(Conn)
        end
    end.

typed_matrix(C, Mode) ->
    typed_sql("CREATE TABLE mqtt.dbo.deep_matrix(id INT PRIMARY KEY,v NVARCHAR(MAX))"),
    typed_sql("INSERT mqtt.dbo.deep_matrix VALUES (999999,N'protected')"),
    {201, _} = emqx_bridge_sqlserver_SUITE:create_connector_api(C, #{}),
    {201, _} = emqx_bridge_sqlserver_SUITE:create_action_api(C, #{
        <<"parameters">> => #{
            <<"sql">> => <<"INSERT INTO deep_matrix VALUES (${payload.id},${payload.value})">>
        },
        <<"resource_opts">> => #{
            <<"batch_size">> => 1,
            <<"worker_pool_size">> => 1,
            <<"metrics_flush_interval">> => <<"100ms">>
        }
    }),
    #{topic := Topic} = emqx_bridge_sqlserver_SUITE:simple_create_rule_api(
        <<"SELECT json_decode(payload) AS payload FROM \"${t}\"">>, C
    ),
    C1 = emqx_bridge_sqlserver_SUITE:start_client(),
    C2 = emqx_bridge_sqlserver_SUITE:start_client(),
    A = emqx_bridge_v2_testlib:resource_id(C),
    Phases = [
        {Op, Mode, Batch, Style, Nulls}
     || Op <- [insert, update],
        Batch <- [1, 2],
        (Op =:= insert orelse Batch =:= 1),
        Style <- [bare, varchar, nvarchar],
        Nulls <- [true, false]
    ],
    {_, Results} = lists:foldl(
        fun(Phase, {PhaseIndex, Acc}) ->
            ct:pal("PHASE ~p ~p", [PhaseIndex, Phase]),
            {Op, Mode, Batch, Style, Nulls} = Phase,
            Expr =
                case Style of
                    bare -> <<"${payload.value}">>;
                    varchar -> <<"'pre${payload.value}post'">>;
                    nvarchar -> <<"N'pre${payload.value}post'">>
                end,
            Q =
                case Op of
                    insert ->
                        iolist_to_binary([
                            "INSERT INTO deep_matrix VALUES (${payload.id},", Expr, ")"
                        ]);
                    update ->
                        iolist_to_binary([
                            "UPDATE deep_matrix SET v=", Expr, " WHERE id=${payload.id}"
                        ])
                end,
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(C, #{
                <<"parameters">> => #{<<"sql">> => Q, <<"undefined_vars_as_null">> => Nulls},
                <<"resource_opts">> => #{
                    <<"batch_size">> => Batch,
                    <<"batch_time">> => <<"2s">>,
                    <<"worker_pool_size">> => 1,
                    <<"metrics_flush_interval">> => <<"10ms">>,
                    <<"query_mode">> => atom_to_binary(Mode)
                }
            }),
            {_, Rows} = lists:foldl(
                fun({Label, V, Kind}, {Index, Rs}) ->
                    ID = PhaseIndex * 10000 + Index * 10,
                    IDs = lists:seq(ID, ID + Batch - 1),
                    case Op of
                        update ->
                            lists:foreach(
                                fun(I) ->
                                    typed_sql([
                                        "INSERT mqtt.dbo.deep_matrix VALUES (",
                                        integer_to_list(I),
                                        ",N'old')"
                                    ])
                                end,
                                IDs
                            );
                        _ ->
                            ok
                    end,
                    OldS = emqx_resource_metrics:success_get(A),
                    OldF = emqx_resource_metrics:failed_get(A),
                    lists:foreach(
                        fun({I, Client}) ->
                            P0 = #{id => I},
                            P =
                                case V of
                                    missing -> P0;
                                    _ -> P0#{value => V}
                                end,
                            emqtt:publish(Client, Topic, emqx_utils_json:encode(P))
                        end,
                        lists:zip(IDs, lists:sublist([C1, C2], Batch))
                    ),
                    Done = typed_wait_metrics(A, OldS + OldF + Batch, 60),
                    S = emqx_resource_metrics:success_get(A) - OldS,
                    F = emqx_resource_metrics:failed_get(A) - OldF,
                    ExpectError = Kind =:= nul orelse (Kind =:= overflow andalso Style =:= bare),
                    Expected = typed_expected(V, Kind, Style, Nulls),
                    Exact =
                        case ExpectError of
                            true ->
                                case Op of
                                    insert ->
                                        typed_count(["id=", integer_to_list(ID)]) =:= 0;
                                    update ->
                                        typed_count(["id=", integer_to_list(ID), " AND v=N'old'"]) =:=
                                            1
                                end;
                            false ->
                                lists:all(
                                    fun(I) ->
                                        typed_count(["id=", integer_to_list(I), " AND ", Expected]) =:=
                                            1
                                    end,
                                    IDs
                                )
                        end,
                    Protected = typed_count("id=999999 AND v=N'protected'") =:= 1,
                    Passed =
                        Done andalso Exact andalso Protected andalso
                            case ExpectError of
                                true -> F =:= Batch andalso S =:= 0;
                                false -> S =:= Batch andalso F =:= 0
                            end,
                    R = #{
                        phase => Phase,
                        sample => Label,
                        passed => Passed,
                        exact => Exact,
                        protected => Protected,
                        success => S,
                        failed => F,
                        done => Done
                    },
                    case Passed of
                        true -> ok;
                        false -> ct:fail(R)
                    end,
                    {Index + 1, [R | Rs]}
                end,
                {1, []},
                typed_samples()
            ),
            {PhaseIndex + 1, Rows ++ Acc}
        end,
        {1, []},
        Phases
    ),
    ?assertEqual([], [R || #{passed := false} = R <- Results]),
    typed_sql("DROP TABLE mqtt.dbo.deep_matrix"),
    ok.
typed_samples() ->
    [
        {ascii, <<"ordinary">>, text},
        {empty, <<>>, text},
        {unicode, <<"中文😀"/utf8>>, text},
        {hex_unicode, <<"0x中文😀"/utf8>>, text},
        {hex_upper_unicode, <<"0X中文😀"/utf8>>, text},
        {bad_hex_unicode, <<"0xg中文😀"/utf8>>, text},
        {hex_unicode_line, <<"0x中文😀\\\n"/utf8>>, text},
        {hex_lf, <<"0xFF\n">>, text},
        {hex_crlf, <<"0xFF\r\n">>, text},
        {hex_valid, <<"0xFF">>, hex},
        {hex_odd, <<"0xF">>, hex},
        {hex_empty, <<"0x">>, text},
        {injection, <<"中文😀'); DELETE FROM deep_matrix; --\\\n"/utf8>>, text},
        {line, <<"a\\\r\nb">>, text},
        {nul, <<"a", 0, "b">>, nul},
        {null, null, text},
        {missing, missing, text},
        {boolean_true, true, text},
        {boolean_false, false, text},
        {integer, 12345, text},
        {negative, -12345, text},
        {big_integer, 18446744073709551616, text},
        {fraction, 0.125, text},
        {tiny_float, 1.0e-12, tiny},
        {large_float, 1.0e40, large},
        {object, #{x => <<"中文😀"/utf8>>}, object},
        {array, [<<"中文😀"/utf8>>, 1, true, null], array},
        {charlist_ascii, [65, 66], chars},
        {charlist_latin1, [233], chars},
        {charlist_unicode, [20013, 128512], chars}
    ].
typed_expected(V, Kind, Style, Nulls) ->
    Text =
        case V of
            missing ->
                case Nulls of
                    true -> <<"null">>;
                    false -> <<"undefined">>
                end;
            null ->
                <<"null">>;
            true ->
                <<"true">>;
            false ->
                <<"false">>;
            N when is_integer(N) -> integer_to_binary(N);
            N when is_float(N) ->
                case Kind of
                    tiny -> <<"0.000000000001">>;
                    large -> <<"10000000000000000000000000000000000000000.0">>;
                    _ -> <<"0.125">>
                end;
            B when is_binary(B) -> B;
            L when Kind =:= chars -> unicode:characters_to_binary(L);
            _ ->
                <<>>
        end,
    case {Style, V, Kind, Nulls} of
        {bare, missing, _, true} ->
            "v IS NULL";
        {bare, null, _, _} ->
            "v IS NULL";
        {bare, _, hex, _} ->
            ["CONVERT(VARBINARY(MAX),v)=CONVERT(VARBINARY(MAX),CONVERT(NVARCHAR(MAX),", V, "))"];
        {_, _, K, _} when K =:= tiny; K =:= large ->
            View =
                case Style of
                    bare -> "v";
                    _ -> "SUBSTRING(v,4,LEN(v)-7)"
                end,
            Number =
                case K of
                    tiny -> "1.0e-12";
                    large -> "1.0e40"
                end,
            ["TRY_CONVERT(FLOAT,", View, ")=CONVERT(FLOAT,", Number, ")"];
        {_, _, K, _} when K =:= object; K =:= array ->
            View =
                case Style of
                    bare -> "v";
                    _ -> "SUBSTRING(v,4,LEN(v)-7)"
                end,
            Path =
                case K of
                    object -> "$.x";
                    array -> "$[0]"
                end,
            [
                "CONVERT(VARBINARY(MAX),JSON_VALUE(",
                View,
                ",'",
                Path,
                "'))=CONVERT(VARBINARY(MAX),",
                typed_reference(<<"中文😀"/utf8>>, Style),
                ")"
            ];
        _ ->
            Full =
                case Style of
                    bare -> Text;
                    _ -> <<"pre", Text/binary, "post">>
                end,
            ["CONVERT(VARBINARY(MAX),v)=CONVERT(VARBINARY(MAX),", typed_reference(Full, Style), ")"]
    end.
typed_reference(Text, varchar) ->
    ["CONVERT(NVARCHAR(MAX),CONVERT(VARCHAR(MAX),", typed_literal(Text), "))"];
typed_reference(Text, _) ->
    typed_literal(Text).
typed_literal(Text) ->
    [
        "CONVERT(NVARCHAR(MAX),0x",
        binary:encode_hex(unicode:characters_to_binary(Text, utf8, {utf16, little})),
        ")"
    ].
typed_count(Where) ->
    {selected, _, [{N}]} = typed_sql(["SELECT COUNT(*) FROM mqtt.dbo.deep_matrix WHERE ", Where]),
    N.
typed_sql(Q) ->
    C =
        case get(typed_matrix_connection) of
            undefined ->
                Conn = emqx_bridge_sqlserver_SUITE:connect_direct_sqlserver(),
                put(typed_matrix_connection, Conn),
                Conn;
            Conn ->
                Conn
        end,
    emqx_bridge_sqlserver_SUITE:directly_query(C, binary_to_list(iolist_to_binary(Q)), 10_000).
typed_wait_metrics(_, _, 0) ->
    false;
typed_wait_metrics(A, N, Tries) ->
    case emqx_resource_metrics:success_get(A) + emqx_resource_metrics:failed_get(A) >= N of
        true ->
            true;
        false ->
            timer:sleep(50),
            typed_wait_metrics(A, N, Tries - 1)
    end.

-doc "Keep Unicode identity keys separate from their lossy VARCHAR counterparts.".
t_unicode_key_isolation() -> [{matrix, true}].
t_unicode_key_isolation(matrix) ->
    [[?sync], [?async]];
t_unicode_key_isolation(Cfg) ->
    with_conn(fun(Conn) ->
        directly_query(
            Conn,
            "CREATE TABLE mqtt.dbo.t_key_isolation(k NVARCHAR(100) PRIMARY KEY,v NVARCHAR(100))"
        ),
        Keys = [<<"0x中文😀"/utf8>>, <<"0xz中文😀"/utf8>>, <<"0X中文😀"/utf8>>, <<"0x????">>, <<"0xz????">>],
        [
            directly_query(
                Conn,
                str(
                    iolist_to_binary([
                        "INSERT mqtt.dbo.t_key_isolation VALUES (",
                        unicode_sql_literal(K),
                        ",N'old')"
                    ])
                )
            )
         || K <- Keys
        ]
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_key_isolation") end)
    end),
    {C, Topic} = create_json_result_action(
        Cfg, <<"UPDATE t_key_isolation SET v=${payload.value} WHERE k=${payload.key}">>
    ),
    lists:foreach(
        fun(Key) ->
            with_conn(fun(Conn) ->
                directly_query(Conn, "UPDATE mqtt.dbo.t_key_isolation SET v=N'old'")
            end),
            {_, {ok, #{result := ok}}} = ?wait_async_action(
                emqtt:publish(
                    C, Topic, emqx_utils_json:encode(#{key => Key, value => <<"changed">>})
                ),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertEqual(
                1,
                update_count(
                    iolist_to_binary([
                        "SELECT COUNT(*) FROM mqtt.dbo.t_key_isolation WHERE k=",
                        unicode_sql_literal(Key),
                        " AND v=N'changed'"
                    ])
                )
            ),
            ?assertEqual(
                1, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_key_isolation WHERE v=N'changed'")
            )
        end,
        [<<"0x中文😀"/utf8>>, <<"0xz中文😀"/utf8>>, <<"0X中文😀"/utf8>>]
    ),
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        emqtt:publish(
            C,
            Topic,
            emqx_utils_json:encode(#{key => <<"0x中文😀' OR 1=1 --"/utf8>>, value => <<"injected">>})
        ),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    ?assertEqual(
        0, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_key_isolation WHERE v=N'injected'")
    ),
    ok.

-doc "Short encoded strings remain compatible with SQL_VARIANT in INSERT and UPDATE.".
t_variant_strings() -> [{matrix, true}].
t_variant_strings(matrix) ->
    [[?sync], [?async]];
t_variant_strings(Cfg) ->
    with_conn(fun(Conn) ->
        directly_query(
            Conn, "CREATE TABLE mqtt.dbo.t_variant_strings(id INT PRIMARY KEY,v SQL_VARIANT)"
        ),
        directly_query(Conn, "INSERT mqtt.dbo.t_variant_strings VALUES (0,N'old')"),
        ?assertEqual(
            {ok, [{123}]},
            emqx_bridge_sqlserver_connector:worker_do_insert(Conn, "SELECT 123 AS n", #{
                pool_name => variant_test, resource_opts => #{}
            })
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_variant_strings") end)
    end),
    {C, Topic} = create_json_result_action(
        Cfg, <<"UPDATE t_variant_strings SET v=${payload.value} WHERE id=0">>
    ),
    lists:foreach(
        fun({Expr, Style}) ->
            lists:foreach(
                fun(SQL) ->
                    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
                        <<"parameters">> => #{<<"sql">> => SQL}
                    }),
                    lists:foreach(
                        fun(V) ->
                            with_conn(fun(Conn) ->
                                directly_query(
                                    Conn, "DELETE mqtt.dbo.t_variant_strings WHERE id=1"
                                ),
                                directly_query(
                                    Conn,
                                    "UPDATE mqtt.dbo.t_variant_strings SET v=N'old' WHERE id=0"
                                )
                            end),
                            {_, {ok, #{result := ok}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, emqx_utils_json:encode(#{value => V})),
                                #{?snk_kind := sqlserver_connector_query_return},
                                10_000
                            ),
                            Expected = typed_reference(V, Style),
                            ?assertEqual(
                                1,
                                update_count(
                                    iolist_to_binary([
                                        "SELECT COUNT(*) FROM mqtt.dbo.t_variant_strings WHERE ",
                                        "CONVERT(VARBINARY(MAX),CONVERT(NVARCHAR(MAX),v))=",
                                        "CONVERT(VARBINARY(MAX),",
                                        Expected,
                                        ")"
                                    ])
                                )
                            )
                        end,
                        [<<"plain">>, <<"中文😀"/utf8>>, <<"a\\\nb">>, <<"中文😀\\\r\n"/utf8>>]
                    )
                end,
                [
                    <<"UPDATE t_variant_strings SET v=", Expr/binary, " WHERE id=0">>,
                    <<"INSERT INTO t_variant_strings VALUES (1,", Expr/binary, ")">>
                ]
            )
        end,
        [
            {<<"${payload.value}">>, bare},
            {<<"'${payload.value}'">>, varchar},
            {<<"N'${payload.value}'">>, nvarchar}
        ]
    ),
    ok.

-doc "Bounded string types preserve exact values and reject oversized SQL_VARIANT writes.".
t_string_type_boundaries() -> [{matrix, true}].
t_string_type_boundaries(matrix) ->
    [[?sync], [?async]];
t_string_type_boundaries(Cfg) ->
    with_conn(fun(Conn) ->
        directly_query(
            Conn, "CREATE TABLE mqtt.dbo.t_string_bounds(id INT PRIMARY KEY,v SQL_VARIANT)"
        ),
        directly_query(Conn, "INSERT mqtt.dbo.t_string_bounds VALUES(0,N'old')")
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_string_bounds") end)
    end),
    {C, Topic} = create_json_result_action(
        Cfg, <<"UPDATE t_string_bounds SET v=${payload.value} WHERE id=0">>
    ),
    Unicode4000 = <<(binary:copy(<<"😀"/utf8>>, 1999))/binary, "\\\n">>,
    Unicode4001 = <<"x", Unicode4000/binary>>,
    Ascii4000 = <<(binary:copy(<<"a">>, 3998))/binary, "\\\n">>,
    Ascii8000 = <<(binary:copy(<<"a">>, 7998))/binary, "\\\n">>,
    lists:foreach(
        fun({Expr, Value, Success, Type}) ->
            with_conn(fun(Conn) ->
                directly_query(Conn, "UPDATE mqtt.dbo.t_string_bounds SET v=N'old' WHERE id=0")
            end),
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
                <<"parameters">> => #{
                    <<"sql">> => <<"UPDATE t_string_bounds SET v=", Expr/binary, " WHERE id=0">>
                }
            }),
            {_, {ok, Event}} = ?wait_async_action(
                emqtt:publish(C, Topic, emqx_utils_json:encode(#{value => Value})),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            {Expected, ExpectedType} =
                case Success of
                    true ->
                        ?assertMatch(#{result := ok}, Event),
                        {Value, Type};
                    false ->
                        ?assertMatch(#{error := {unrecoverable_error, _}}, Event),
                        {<<"old">>, nvarchar}
                end,
            Bytes =
                case ExpectedType of
                    varchar -> Expected;
                    nvarchar -> unicode:characters_to_binary(Expected, utf8, {utf16, little})
                end,
            ?assertEqual(
                1,
                update_count(
                    iolist_to_binary([
                        "SELECT COUNT(*) FROM mqtt.dbo.t_string_bounds WHERE id=0 AND ",
                        "CONVERT(VARBINARY(MAX),v)=0x",
                        binary:encode_hex(Bytes),
                        " AND CONVERT(VARCHAR(20),SQL_VARIANT_PROPERTY(v,'BaseType'))='",
                        atom_to_binary(ExpectedType),
                        "'"
                    ])
                )
            )
        end,
        [
            {<<"N'${payload.value}'">>, Unicode4000, true, nvarchar},
            {<<"N'${payload.value}'">>, Unicode4001, false, nvarchar},
            {<<"N'${payload.value}'">>, Ascii4000, true, nvarchar},
            {<<"N'${payload.value}'">>, <<"x", Ascii4000/binary>>, false, nvarchar},
            {<<"'${payload.value}'">>, Ascii8000, true, varchar},
            {<<"'${payload.value}'">>, <<"x", Ascii8000/binary>>, false, varchar},
            {<<"${payload.value}">>, Unicode4000, true, nvarchar},
            {<<"${payload.value}">>, Unicode4001, false, nvarchar},
            {<<"${payload.value}">>, <<"中文😀  "/utf8>>, true, nvarchar},
            {<<"${payload.value}">>, <<"a", 16#FFFF/utf8, "b">>, true, nvarchar},
            {<<"${payload.value}">>, <<"a'\\b">>, true, varchar},
            {<<"${payload.value}">>, <<"0xFF", 0>>, false, varchar}
        ]
    ),
    ok.

-doc "Data encoding does not change trigger nesting or bypass existing audit guards.".
t_trigger_context() -> [{matrix, true}].
t_trigger_context(matrix) ->
    [[?sync], [?async]];
t_trigger_context(Cfg) ->
    create_result_fixture("t_context_values"),
    with_conn(fun(Conn) ->
        directly_query(Conn, "CREATE TABLE mqtt.dbo.t_context_audit(v NVARCHAR(MAX),depth INT)")
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_context_audit") end)
    end),
    {C, Topic} = create_json_result_action(
        Cfg, <<"UPDATE t_context_values SET status=${payload.value} WHERE id=0">>
    ),
    lists:foreach(
        fun(Guard) ->
            with_conn(fun(Conn) ->
                directly_query(Conn, "USE mqtt"),
                directly_query(Conn, "DROP TRIGGER IF EXISTS dbo.context_guard"),
                directly_query(
                    Conn,
                    lists:concat([
                        "CREATE TRIGGER dbo.context_guard ON dbo.t_context_values AFTER UPDATE ",
                        "AS BEGIN SET NOCOUNT ON; ",
                        Guard,
                        " INSERT dbo.t_context_audit SELECT status,@@NESTLEVEL FROM inserted; END"
                    ])
                )
            end),
            lists:foreach(
                fun(Expr) ->
                    SQL = <<"UPDATE t_context_values SET status=", Expr/binary, " WHERE id=0">>,
                    {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
                        <<"parameters">> => #{<<"sql">> => SQL}
                    }),
                    with_conn(fun(Conn) ->
                        directly_query(Conn, "DELETE mqtt.dbo.t_context_audit")
                    end),
                    Values = [<<"ascii">>, <<"中文😀"/utf8>>, <<"0x中文😀"/utf8>>, <<"a\\\nb">>],
                    [
                        begin
                            {_, {ok, #{result := ok}}} = ?wait_async_action(
                                emqtt:publish(C, Topic, emqx_utils_json:encode(#{value => V})),
                                #{?snk_kind := sqlserver_connector_query_return},
                                10_000
                            )
                        end
                     || V <- Values
                    ],
                    ?assertEqual(
                        4,
                        update_count("SELECT COUNT(*) FROM mqtt.dbo.t_context_audit WHERE depth=1")
                    ),
                    ?assertEqual(4, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_context_audit"))
                end,
                [<<"${payload.value}">>, <<"'${payload.value}'">>, <<"N'${payload.value}'">>]
            )
        end,
        ["", "IF @@NESTLEVEL > 1 RETURN;", "IF TRIGGER_NESTLEVEL() > 1 RETURN;"]
    ),
    ok.

create_json_result_action(Cfg, SQL) ->
    {201, _} = create_connector_api(Cfg, #{}),
    {201, _} = create_action_api(Cfg, #{
        <<"parameters">> => #{<<"sql">> => SQL, <<"undefined_vars_as_null">> => true},
        <<"resource_opts">> => #{<<"batch_size">> => 1, <<"worker_pool_size">> => 1}
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"SELECT json_decode(payload) AS payload FROM \"${t}\"">>, Cfg
    ),
    {start_client(), Topic}.

-doc "Retained plans preserve static continuations, Unicode constants and dynamic strings.".
t_legacy_plan_upgrade(Cfg) ->
    Mod = emqx_bridge_sqlserver_sql,
    {Mod, NewBin, NewFile} = code:get_object_code(Mod),
    Source = filename:join(?config(data_dir, Cfg), "legacy_sqlserver_sql.erl.txt"),
    Temp = filename:join(?config(priv_dir, Cfg), "emqx_bridge_sqlserver_sql.erl"),
    {ok, _} = file:copy(Source, Temp),
    {ok, Mod, OldBin} = compile:file(Temp, [binary]),
    with_conn(fun(Conn) ->
        directly_query(
            Conn,
            "CREATE TABLE mqtt.dbo.t_legacy_plan(id INT PRIMARY KEY,v NVARCHAR(MAX),s NVARCHAR(MAX))"
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) -> directly_query(Conn, "DROP TABLE mqtt.dbo.t_legacy_plan") end)
    end),
    {201, _} = create_connector_api(Cfg, #{}),
    SQL =
        <<
            "INSERT INTO t_legacy_plan VALUES (${payload.id},",
            "N'pre\\\\\n${payload.value}post',",
            "N'静态${$}{literal}\\\\\nconstant')"/utf8
        >>,
    {module, Mod} = code:load_binary(Mod, Temp, OldBin),
    try
        {201, _} = create_action_api(Cfg, #{
            <<"parameters">> => #{<<"sql">> => SQL},
            <<"resource_opts">> => #{<<"batch_size">> => 1, <<"worker_pool_size">> => 1}
        }),
        {module, Mod} = code:load_binary(Mod, NewFile, NewBin),
        #{topic := Topic} = simple_create_rule_api(
            <<"SELECT json_decode(payload) AS payload FROM \"${t}\"">>, Cfg
        ),
        C1 = start_client(),
        C2 = start_client(),
        check_legacy_publish(C1, Topic, 1, <<"v">>),
        code:purge(Mod),
        {module, Mod} = code:load_binary(Mod, Temp, OldBin),
        {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
            <<"parameters">> => #{<<"sql">> => SQL},
            <<"resource_opts">> => #{
                <<"batch_size">> => 2, <<"batch_time">> => <<"2s">>, <<"worker_pool_size">> => 1
            }
        }),
        code:purge(Mod),
        {module, Mod} = code:load_binary(Mod, NewFile, NewBin),
        emqtt:publish(C1, Topic, emqx_utils_json:encode(#{id => 2, value => <<"😀'\\\n"/utf8>>})),
        emqtt:publish(C2, Topic, emqx_utils_json:encode(#{id => 3, value => <<"new">>})),
        ?retry(
            100, 50, ?assertEqual(3, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_legacy_plan"))
        ),
        assert_legacy_value(2, <<"😀'\\\n"/utf8>>),
        assert_legacy_value(3, <<"new">>)
    after
        code:purge(Mod),
        {module, Mod} = code:load_binary(Mod, NewFile, NewBin)
    end.
check_legacy_publish(C, Topic, ID, Value) ->
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        emqtt:publish(C, Topic, emqx_utils_json:encode(#{id => ID, value => Value})),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    assert_legacy_value(ID, Value).
assert_legacy_value(ID, Value) ->
    Expected = <<"pre\\", Value/binary, "post">>,
    VHex = binary:encode_hex(unicode:characters_to_binary(Expected, utf8, {utf16, little})),
    SHex = binary:encode_hex(
        unicode:characters_to_binary(<<"静态${literal}\\constant"/utf8>>, utf8, {utf16, little})
    ),
    ?assertEqual(
        1,
        update_count(
            iolist_to_binary([
                "SELECT COUNT(*) FROM mqtt.dbo.t_legacy_plan WHERE id=",
                integer_to_binary(ID),
                " AND CONVERT(VARBINARY(MAX),v)=0x",
                VHex,
                " AND CONVERT(VARBINARY(MAX),s)=0x",
                SHex
            ])
        )
    ).

-doc "Database code pages select safe bounded types and refresh when the connector is changed.".
t_varchar_codepages(Cfg) ->
    Profiles = [
        {"sqlserver_cp_latin", "SQL_Latin1_General_CP1_CI_AS", 1},
        {"sqlserver_cp_dbcs", "Chinese_PRC_CI_AS", 2},
        {"sqlserver_cp_utf8", "Latin1_General_100_CI_AS_SC_UTF8", 3}
    ],
    on_exit(fun() ->
        emqx_bridge_v2_testlib:delete_all_rules(),
        emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
        with_conn(fun(Conn) ->
            [directly_query(Conn, "DROP DATABASE IF EXISTS " ++ DB) || {DB, _, _} <- Profiles]
        end)
    end),
    with_conn(fun(Conn) ->
        [
            begin
                {updated, _} = directly_query(
                    Conn, "CREATE DATABASE " ++ DB ++ " COLLATE " ++ Collation
                ),
                {updated, _} = directly_query(
                    Conn, "CREATE TABLE " ++ DB ++ ".dbo.points(v VARCHAR(MAX))"
                ),
                {updated, _} = directly_query(
                    Conn, "CREATE TABLE " ++ DB ++ ".dbo.variants(v SQL_VARIANT)"
                )
            end
         || {DB, Collation, _} <- Profiles
        ]
    end),
    [{DB0, _, _} | _] = Profiles,
    {201, _} = create_connector_api(Cfg, #{<<"database">> => list_to_binary(DB0)}),
    {201, _} = create_action_api(Cfg, #{
        <<"parameters">> => #{<<"sql">> => <<"INSERT INTO points VALUES ('${payload}')">>},
        <<"resource_opts">> => #{<<"batch_size">> => 1}
    }),
    #{topic := Topic} = simple_create_rule_api(<<"SELECT payload FROM \"${t}\"">>, Cfg),
    C = start_client(),
    Value = <<(binary:copy(<<"中"/utf8>>, 3000))/binary, "\\\n">>,
    lists:foreach(
        fun({DB, _, Width}) ->
            {200, _} = emqx_bridge_v2_testlib:update_connector_api_simple(Cfg, #{
                <<"database">> => list_to_binary(DB)
            }),
            {_, {ok, #{result := ok}}} = ?wait_async_action(
                emqtt:publish(C, Topic, Value),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            ?assertEqual(
                1,
                update_count(
                    "SELECT COUNT(*) FROM " ++ DB ++ ".dbo.points WHERE DATALENGTH(v)=" ++
                        integer_to_list(3000 * Width + 2)
                )
            ),
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
                <<"parameters">> => #{<<"sql">> => <<"INSERT INTO variants VALUES ('${payload}')">>}
            }),
            {_, {ok, Event}} = ?wait_async_action(
                emqtt:publish(C, Topic, Value),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            ),
            case Width of
                3 ->
                    ?assertMatch(#{error := {unrecoverable_error, {invalid_request, _}}}, Event);
                _ ->
                    ?assertMatch(#{result := ok}, Event),
                    Expected =
                        case Width of
                            1 -> <<(binary:copy(<<"?">>, 3000))/binary, "\\\n">>;
                            2 -> Value
                        end,
                    ?assertEqual(
                        1,
                        update_count(
                            iolist_to_binary([
                                "SELECT COUNT(*) FROM ",
                                DB,
                                ".dbo.variants WHERE ",
                                "CONVERT(VARBINARY(MAX),CONVERT(NVARCHAR(MAX),v))=",
                                "CONVERT(VARBINARY(MAX),",
                                unicode_sql_literal(Expected),
                                ")"
                            ])
                        )
                    )
            end,
            {200, _} = emqx_bridge_v2_testlib:update_bridge_api2(Cfg, #{
                <<"parameters">> => #{<<"sql">> => <<"INSERT INTO points VALUES ('${payload}')">>}
            })
        end,
        Profiles
    ),
    ok.

-doc "Unicode identifiers use a stable dynamic context, independent of the value's characters.".
t_unicode_identifier_context(Cfg) ->
    with_conn(fun(Conn) ->
        unicode_ddl(
            Conn, <<"CREATE TABLE mqtt.dbo.[设备](id INT PRIMARY KEY,[状态] NVARCHAR(100))"/utf8>>
        ),
        unicode_ddl(Conn, <<"INSERT mqtt.dbo.[设备] VALUES(1,N'old')"/utf8>>),
        directly_query(Conn, "CREATE TABLE mqtt.dbo.t_identifier_audit(depth INT)"),
        directly_query(Conn, "USE mqtt"),
        unicode_ddl(
            Conn,
            [
                <<"CREATE TRIGGER dbo.identifier_guard ON dbo.[设备] AFTER UPDATE "/utf8>>,
                "AS BEGIN SET NOCOUNT ON; IF TRIGGER_NESTLEVEL()>1 RETURN; ",
                "INSERT dbo.t_identifier_audit VALUES(@@NESTLEVEL); END"
            ]
        )
    end),
    on_exit(fun() ->
        with_conn(fun(Conn) ->
            unicode_ddl(Conn, <<"DROP TABLE mqtt.dbo.[设备]"/utf8>>),
            directly_query(Conn, "DROP TABLE mqtt.dbo.t_identifier_audit")
        end)
    end),
    {C, Topic} = create_json_result_action(
        Cfg, <<"UPDATE [设备] SET [状态]=${payload.value} WHERE id=1"/utf8>>
    ),
    [
        begin
            {_, {ok, #{result := ok}}} = ?wait_async_action(
                emqtt:publish(C, Topic, emqx_utils_json:encode(#{value => V})),
                #{?snk_kind := sqlserver_connector_query_return},
                10_000
            )
        end
     || V <- [<<"ascii">>, <<"中文😀"/utf8>>]
    ],
    ?assertEqual(2, update_count("SELECT COUNT(*) FROM mqtt.dbo.t_identifier_audit WHERE depth=3")),
    ok.

unicode_ddl(Conn, SQL) ->
    Query = iolist_to_binary([
        "DECLARE @s NVARCHAR(MAX)=", unicode_sql_literal(SQL), ";EXEC sys.sp_executesql @s"
    ]),
    case directly_query(Conn, str(Query)) of
        {error, Reason} -> ct:fail(Reason);
        Result -> Result
    end.

-doc "Value encoding and code-page discovery work with only SELECT/UPDATE table permissions.".
t_restricted_value_encoding(Cfg) ->
    on_exit(fun() ->
        emqx_bridge_v2_testlib:delete_all_rules(),
        emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
        with_conn(fun(Conn) ->
            directly_query(Conn, "USE mqtt"),
            directly_query(Conn, "DROP USER IF EXISTS sqlserver_value_user"),
            directly_query(Conn, "DROP LOGIN sqlserver_value_user"),
            directly_query(Conn, "DROP TABLE dbo.t_restricted_values")
        end)
    end),
    with_conn(fun(Conn) ->
        directly_query(Conn, "USE mqtt"),
        directly_query(
            Conn, "CREATE TABLE dbo.t_restricted_values(id INT PRIMARY KEY,v SQL_VARIANT)"
        ),
        directly_query(Conn, "INSERT dbo.t_restricted_values VALUES(1,N'old')"),
        directly_query(
            Conn,
            "CREATE LOGIN sqlserver_value_user WITH PASSWORD='Sql_value_test_7!',CHECK_POLICY=OFF"
        ),
        directly_query(Conn, "CREATE USER sqlserver_value_user FOR LOGIN sqlserver_value_user"),
        directly_query(
            Conn, "GRANT SELECT,UPDATE ON dbo.t_restricted_values TO sqlserver_value_user"
        )
    end),
    {201, _} = create_connector_api(Cfg, #{
        <<"username">> => <<"sqlserver_value_user">>, <<"password">> => <<"Sql_value_test_7!">>
    }),
    {201, _} = create_action_api(Cfg, #{
        <<"resource_opts">> => #{<<"batch_size">> => 1},
        <<"parameters">> => #{
            <<"sql">> => <<"UPDATE t_restricted_values SET v='${payload}' WHERE id=1">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(<<"SELECT payload FROM \"${t}\"">>, Cfg),
    C = start_client(),
    Value = <<(binary:copy(<<"中"/utf8>>, 3000))/binary, "\\\n">>,
    {_, {ok, #{result := ok}}} = ?wait_async_action(
        emqtt:publish(C, Topic, Value),
        #{?snk_kind := sqlserver_connector_query_return},
        10_000
    ),
    ?assertEqual(
        1,
        update_count(
            "SELECT COUNT(*) FROM mqtt.dbo.t_restricted_values WHERE id=1 AND DATALENGTH(CONVERT(VARCHAR(8000),v))=3002"
        )
    ),
    ok.

unicode_sql_literal(Value) ->
    Hex = binary:encode_hex(unicode:characters_to_binary(Value, utf8, {utf16, little})),
    <<"CONVERT(NVARCHAR(MAX), 0x", Hex/binary, ")">>.

unicode_point_count(Device, Value) ->
    SQL = iolist_to_binary([
        "SELECT COUNT(*) FROM mqtt.dbo.t_unicode_points WHERE device=",
        unicode_sql_literal(Device),
        " AND status=",
        unicode_sql_literal(Value)
    ]),
    update_count(SQL).

-doc "Recover an UPDATE after a real connection failure and execute bare SET placeholders.".
t_update_reconnect() ->
    [{matrix, true}].
t_update_reconnect(matrix) ->
    [[?sync], [?async]];
t_update_reconnect(TCConfig) ->
    with_conn(fun(Conn) ->
        {updated, 2} = directly_query(Conn, """
        INSERT INTO mqtt.dbo.t_mqtt_msg(topic, payload) VALUES ('device', 'old'), ('other', 'old')
        """)
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{<<"batch_size">> => 1, <<"request_ttl">> => <<"30s">>},
        <<"parameters">> => #{
            <<"sql">> =>
                <<"UPDATE t_mqtt_msg SET payload = ${payload} WHERE topic = ${topic}">>
        }
    }),
    #{topic := Topic} = simple_create_rule_api(
        <<"""
        SELECT payload, 'device' AS topic FROM "${t}"
        """>>,
        TCConfig
    ),
    C = start_client(),
    with_failure(down, fun() ->
        ?wait_async_action(
            emqtt:publish(C, Topic, <<"online'1">>),
            #{?snk_kind := buffer_worker_retry_inflight},
            10_000
        ),
        ?assertEqual(
            2,
            update_count(
                "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg WHERE payload = 'old'"
            )
        )
    end),
    ?retry(
        200,
        100,
        ?assertEqual(
            1,
            update_count(
                "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg WHERE topic = 'device' AND payload = 'online''1'"
            )
        )
    ),
    ?assertEqual(
        1,
        update_count(
            "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg WHERE topic = 'other' AND payload = 'old'"
        )
    ),
    ActionId = emqx_bridge_v2_testlib:resource_id(TCConfig),
    ?retry(200, 20, ?assertEqual(1, emqx_resource_metrics:success_get(ActionId))),
    ok.

-doc "Reject unsupported UPDATE syntax through the probe API without changing database rows.".
t_update_invalid_template(TCConfig) ->
    with_conn(fun(Conn) ->
        {updated, 1} = directly_query(
            Conn,
            "INSERT INTO mqtt.dbo.t_mqtt_msg(topic, payload) VALUES ('device', 'old')"
        )
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    InvalidSQLs = [
        <<"UPDATE ${table} SET payload = ${payload} WHERE topic = ${topic}">>,
        <<"UPDATE t_mqtt_msg SET [${column}] = ${payload} WHERE topic = ${topic}">>,
        <<"UPDATE t_mqtt_msg SET payload = ${payload}; DELETE FROM t_mqtt_msg">>,
        <<"UPDATE t_mqtt_msg SET payload = ${payload} -- WHERE topic = ${topic}">>,
        <<"UPDATE t_mqtt_msg SET payload = ${payload} /* comment */ WHERE topic = ${topic}">>,
        <<"UPDATE t_mqtt_msg SET payload = ${payload} WHERE [${column}] = ${topic}">>,
        <<"UPDATE t_mqtt_msg SET payload = 'old", 0, "' WHERE topic = ${topic}">>
    ],
    lists:foreach(
        fun(SQL) ->
            ?assertMatch(
                {400, _},
                emqx_bridge_v2_testlib:probe_bridge_api_simple(TCConfig, #{
                    <<"resource_opts">> => #{<<"batch_size">> => 1},
                    <<"parameters">> => #{<<"sql">> => SQL}
                })
            ),
            ?assertEqual(
                1,
                update_count(
                    "SELECT COUNT(*) FROM mqtt.dbo.t_mqtt_msg WHERE topic = 'device' AND payload = 'old'"
                )
            )
        end,
        InvalidSQLs
    ),
    ok.

-doc "Reject batched UPDATE on probe, create, and update, and recover when batching is disabled.".
t_batch_update(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    Overrides = #{
        <<"resource_opts">> => #{<<"batch_size">> => 100},
        <<"parameters">> => #{
            <<"sql">> =>
                <<"UPDATE t_mqtt_msg SET payload = ${payload} WHERE topic = ${topic}">>
        }
    },
    {400, _} = emqx_bridge_v2_testlib:probe_bridge_api_simple(TCConfig, Overrides),
    {201, #{<<"status">> := <<"disconnected">>, <<"status_reason">> := Reason}} =
        create_action_api(TCConfig, Overrides),
    ?assertNotEqual(
        nomatch,
        binary:match(Reason, <<"UPDATE statements are not supported for batch operations">>)
    ),
    {200, #{<<"status">> := <<"connected">>}} = emqx_bridge_v2_testlib:update_bridge_api2(
        TCConfig,
        emqx_utils_maps:deep_merge(Overrides, #{<<"resource_opts">> => #{<<"batch_size">> => 1}})
    ),
    {200, #{<<"status">> := <<"disconnected">>, <<"status_reason">> := Reason}} =
        emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, Overrides),
    ok.

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
                    fun(Payload) -> emqx:publish2(emqx_message:make(Topic, Payload)) end,
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
