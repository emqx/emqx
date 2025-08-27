%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_clickhouse_SUITE).

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

-define(CONNECTOR_TYPE, clickhouse).
-define(CONNECTOR_TYPE_BIN, <<"clickhouse">>).
-define(ACTION_TYPE, clickhouse).
-define(ACTION_TYPE_BIN, <<"clickhouse">>).

-define(PROXY_NAME, "clickhouse").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

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
            emqx_bridge_clickhouse,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    Conn = start_clickhouse_connection(),
    {ok, _, _} = clickhouse:query(Conn, sql_create_database(), #{}),
    {ok, _, _} = clickhouse:query(Conn, sql_create_table(), []),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME},
        {clickhouse_connection, Conn}
        | TCConfig
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1} | TCConfig0];
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
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    reset_table(TCConfig),
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

end_per_testcase(_TestCase, TCConfig) ->
    reset_proxy(),
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    reset_table(TCConfig),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"url">> => <<"toxiproxy:8123">>,
        <<"connect_timeout">> => <<"1s">>,
        <<"database">> => <<"mqtt">>,
        <<"pool_size">> => 2,
        <<"username">> => <<"default">>,
        <<"password">> => <<"public">>,
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
            <<"sql">> => bin(sql_insert_template_for_bridge()),
            <<"undefined_vars_as_null">> => false,
            <<"batch_value_separator">> => <<", ">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
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

is_batching(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(
        TCConfig, [?without_batch, ?with_batch], ?without_batch
    ).

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

get_connector_api(Config) ->
    ConnectorType = ?config(connector_type, Config),
    ConnectorName = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
    ).

simple_create_rule_api(TCConfig) ->
    simple_create_rule_api(
        <<
            "select payload,"
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

sql_insert_template_for_bridge() ->
    "INSERT INTO mqtt_test(key, data, arrived) VALUES "
    "(${payload.key}, '${payload.data}', ${timestamp})".

sql_insert_template_for_bridge_json() ->
    "INSERT INTO mqtt_test(key, data, arrived) FORMAT JSONCompactEachRow "
    "[${payload.key}, \"${payload.data}\", ${timestamp}]".

sql_create_table() ->
    "CREATE TABLE IF NOT EXISTS mqtt.mqtt_test "
    "(key BIGINT, data String, arrived BIGINT) ENGINE = Memory".

sql_find_key(Key) ->
    io_lib:format("SELECT key FROM mqtt.mqtt_test WHERE key = ~p", [Key]).

sql_find_all_keys() ->
    "SELECT key FROM mqtt.mqtt_test".

sql_drop_table() ->
    "DROP TABLE IF EXISTS mqtt.mqtt_test".

sql_create_database() ->
    "CREATE DATABASE IF NOT EXISTS mqtt".

parse_insert(SQL) ->
    emqx_bridge_clickhouse_connector:split_clickhouse_insert_sql(SQL).

start_clickhouse_connection() ->
    %% Start clickhouse connector in sub process so that it does not go
    %% down with the process that is calling init_per_suite
    InitPerSuiteProcess = self(),
    erlang:spawn(
        fun() ->
            {ok, Conn} =
                clickhouse:start_link([
                    {url, <<"toxiproxy:8123">>},
                    {user, <<"default">>},
                    {key, "public"},
                    {pool, tmp_pool}
                ]),
            InitPerSuiteProcess ! {clickhouse_connection, Conn},
            Ref = erlang:monitor(process, Conn),
            receive
                {'DOWN', Ref, process, _, _} ->
                    erlang:display(helper_down),
                    ok
            end
        end
    ),
    receive
        {clickhouse_connection, C} -> C
    end.

reset_table(TCConfig) ->
    ClickhouseConnection = get_config(clickhouse_connection, TCConfig),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_drop_table(), []),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_create_table(), []),
    ok.

check_key_in_clickhouse(AttempsLeft, Key, TCConfig) ->
    ClickhouseConnection = get_config(clickhouse_connection, TCConfig),
    check_key_in_clickhouse(AttempsLeft, Key, none, ClickhouseConnection).

check_key_in_clickhouse(Key, TCConfig) ->
    ClickhouseConnection = get_config(clickhouse_connection, TCConfig),
    check_key_in_clickhouse(30, Key, none, ClickhouseConnection).

check_key_in_clickhouse(0, Key, PrevResult, _) ->
    ct:fail("Expected ~p in database but got ~s", [Key, PrevResult]);
check_key_in_clickhouse(AttempsLeft, Key, _, ClickhouseConnection) ->
    {ok, 200, ResultString} = clickhouse:query(ClickhouseConnection, sql_find_key(Key), []),
    Expected = erlang:integer_to_binary(Key),
    case iolist_to_binary(string:trim(ResultString)) of
        Expected ->
            ok;
        SomethingElse ->
            timer:sleep(100),
            check_key_in_clickhouse(AttempsLeft - 1, Key, SomethingElse, ClickhouseConnection)
    end.

json_encode(X) ->
    emqx_utils_json:encode(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "clickhouse_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?without_batch], [?with_batch]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    Key = 42,
    PayloadFn = fun() ->
        emqx_utils_json:encode(#{
            <<"key">> => Key,
            <<"data">> => <<"hey">>
        })
    end,
    PostPublishFn = fun(_Context) ->
        check_key_in_clickhouse(Key, TCConfig)
    end,
    Opts = #{
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_parse_insert_sql_template(_Config) ->
    ?assertEqual(
        <<"(${tagvalues},${date})"/utf8>>,
        parse_insert(
            <<"insert into tag_VALUES(tag_values,Timestamp) values (${tagvalues},${date})"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${id}, 'Иван', 25)"/utf8>>,
        parse_insert(
            <<"INSERT INTO Values_таблица (идентификатор, имя, возраст)   VALUES \t (${id}, 'Иван', 25)  "/utf8>>
        )
    ),
    %% with `;` suffix, bug-to-bug compatibility
    ?assertEqual(
        <<"(${id}, 'Иван', 25)"/utf8>>,
        parse_insert(
            <<"INSERT INTO Values_таблица (идентификатор, имя, возраст)   VALUES \t (${id}, 'Иван', 25);  "/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${id},'李四', 35)"/utf8>>,
        parse_insert(
            <<"  inSErt into 表格(标识,名字,年龄)values(${id},'李四', 35) ; "/utf8>>
        )
    ),

    %% `values` in column name
    ?assertEqual(
        <<"(${tagvalues},${date}  )"/utf8>>,
        parse_insert(
            <<"insert into PI.dbo.tags(tag_values,Timestamp) values (${tagvalues},${date}  )"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${payload}, FROM_UNIXTIME((${timestamp}/1000)))">>,
        parse_insert(
            <<"INSERT INTO mqtt_test(payload, arrived) VALUES (${payload}, FROM_UNIXTIME((${timestamp}/1000)))"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${id},'Алексей',30)"/utf8>>,
        parse_insert(
            <<"insert into таблица (идентификатор,имя,возраст) VALUES(${id},'Алексей',30)"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${id}, '张三', 22)"/utf8>>,
        parse_insert(
            <<"INSERT into 表格 (标识, 名字, 年龄) VALUES (${id}, '张三', 22)"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${id},'李四', 35)"/utf8>>,
        parse_insert(
            <<"  inSErt into 表格(标识,名字,年龄)values(${id},'李四', 35)"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(   ${tagvalues},   ${date} )"/utf8>>,
        parse_insert(
            <<"insert into PI.dbo.tags( tag_value,Timestamp)  VALUES\t\t(   ${tagvalues},   ${date} )"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${tagvalues},${date})"/utf8>>,
        parse_insert(
            <<"insert into PI.dbo.tags(tag_value , Timestamp )vALues(${tagvalues},${date})"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${one}, ${two},${three})"/utf8>>,
        parse_insert(
            <<"inSErt  INTO  table75 (column1, column2, column3) values (${one}, ${two},${three})"/utf8>>
        )
    ),
    ?assertEqual(
        <<"(${tag1},   ${tag2}  )">>,
        parse_insert(
            <<"INSERT Into some_table      values\t(${tag1},   ${tag2}  )">>
        )
    ),
    ?assertEqual(
        <<"(2, 2)">>,
        parse_insert(
            <<"INSERT INTO insert_select_testtable (* EXCEPT(b)) Values (2, 2)">>
        )
    ),
    ?assertEqual(
        <<"(2, 2), (3, ${five})">>,
        parse_insert(
            <<"INSERT INTO insert_select_testtable (* EXCEPT(b))Values(2, 2), (3, ${five})">>
        )
    ),

    %% `format`
    ?assertEqual(
        <<"[(${key}, \"${data}\", ${timestamp})]">>,
        parse_insert(
            <<"INSERT INTO mqtt_test(key, data, arrived)",
                " FORMAT JSONCompactEachRow [(${key}, \"${data}\", ${timestamp})]">>
        )
    ),
    ?assertEqual(
        <<"(v11, v12, v13), (v21, v22, v23)">>,
        parse_insert(
            <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values (v11, v12, v13), (v21, v22, v23)">>
        )
    ),

    ?assertEqual(
        <<"👋    .."/utf8>>,
        %% Only check if FORMAT_DATA existed after `FORMAT FORMAT_NAME`
        parse_insert(
            <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT AnyFORMAT  👋    .."/utf8>>
        )
    ),

    ErrMsg = <<"The SQL template should be an SQL INSERT statement but it is something else.">>,
    %% No `FORMAT_DATA`
    ?assertError(
        ErrMsg,
        parse_insert(
            <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values">>
        )
    ),
    ?assertError(
        ErrMsg,
        parse_insert(
            <<"INSERT INTO   mqtt_test(key, data, arrived) FORMAT Values  ">>
        )
    ).

t_undefined_vars_as_null(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Key = 42,
    Payload = json_encode(#{key => Key, data => null}),
    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    %% Check that the data got to the database
    check_key_in_clickhouse(Key, TCConfig),
    ClickhouseConnection = get_config(clickhouse_connection, TCConfig),
    SQL = io_lib:format("SELECT data FROM mqtt.mqtt_test WHERE key = ~p", [Key]),
    {ok, 200, ResultString} = clickhouse:query(ClickhouseConnection, SQL, []),
    ?assertMatch(<<"null">>, iolist_to_binary(string:trim(ResultString))),
    ok.

t_send_simple_batch_alternative_format(TCConfig) when is_list(TCConfig) ->
    Key = 42,
    PayloadFn = fun() ->
        emqx_utils_json:encode(#{
            <<"key">> => Key,
            <<"data">> => <<"hey">>
        })
    end,
    PostPublishFn = fun(_Context) ->
        check_key_in_clickhouse(Key, TCConfig)
    end,
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"sql">> => bin(sql_insert_template_for_bridge_json()),
            <<"batch_value_separator">> => <<"">>
        }
    },
    Opts = #{
        action_overrides => ActionOverrides,
        payload_fn => PayloadFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

%% Checks that we handle timeouts during health checks
t_connector_health_check_timeout(TCConfig) ->
    %% Create the connector without problems
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{}),
    %% Now wait until a health check fails
    with_failure(timeout, fun() ->
        ?retry(
            1_000,
            5,
            ?assertMatch(
                {200, #{
                    <<"status">> := <<"connecting">>,
                    <<"status_reason">> := <<"health check timeout">>
                }},
                get_connector_api(TCConfig)
            )
        )
    end),
    ok.

t_heavy_batching(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"resource_opts">> => #{
            <<"batch_size">> => 743,
            <<"batch_time">> => <<"50ms">>
        }
    }),
    do_t_heavy_batching(TCConfig).

t_heavy_batching_alternative_format(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"sql">> => bin(sql_insert_template_for_bridge_json()),
            <<"batch_value_separator">> => <<"">>
        },
        <<"resource_opts">> => #{
            <<"batch_size">> => 743,
            <<"batch_time">> => <<"50ms">>
        }
    }),
    do_t_heavy_batching(TCConfig).

do_t_heavy_batching(TCConfig) ->
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    NumberOfMessages = 10_000,
    emqx_utils:pforeach(
        fun(Key) ->
            Payload = json_encode(#{
                <<"key">> => Key,
                <<"data">> => <<"hey">>
            }),
            emqx:publish(emqx_message:make(Topic, Payload))
        end,
        lists:seq(1, NumberOfMessages)
    ),
    % Wait until the last message is in clickhouse
    %% The delay between attempts is 100ms so 150 attempts means 15 seconds
    check_key_in_clickhouse(_AttemptsToFindKey = 150, NumberOfMessages, TCConfig),
    %% In case the messages are not sent in order (could happend with multiple buffer workers)
    ct:sleep(1_000),
    ClickhouseConnection = get_config(clickhouse_connection, TCConfig),
    {ok, 200, ResultString1} = clickhouse:query(ClickhouseConnection, sql_find_all_keys(), []),
    ResultString2 = iolist_to_binary(string:trim(ResultString1)),
    KeyStrings = string:lexemes(ResultString2, "\n"),
    Keys = [erlang:binary_to_integer(iolist_to_binary(K)) || K <- KeyStrings],
    KeySet = maps:from_keys(Keys, true),
    ?assertEqual(NumberOfMessages, maps:size(KeySet)),
    CheckKey = fun(Key) -> maps:get(Key, KeySet, false) end,
    true = lists:all(CheckKey, lists:seq(1, NumberOfMessages)),
    ok.
