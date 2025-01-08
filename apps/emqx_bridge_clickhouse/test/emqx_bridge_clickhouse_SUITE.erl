%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_clickhouse_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-define(CLICKHOUSE_HOST, "clickhouse").
-define(CLICKHOUSE_PORT, "8123").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_connector/include/emqx_connector.hrl").

%% See comment in
%% apps/emqx_bridge_clickhouse/test/emqx_bridge_clickhouse_connector_SUITE.erl for how to
%% run this without bringing up the whole CI infrastucture

%%------------------------------------------------------------------------------
%% Common Test Setup, Teardown and Testcase List
%%------------------------------------------------------------------------------

init_per_suite(Config) ->
    Host = clickhouse_host(),
    Port = list_to_integer(clickhouse_port()),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Apps = emqx_cth_suite:start(
                [
                    emqx,
                    emqx_conf,
                    emqx_bridge_clickhouse,
                    emqx_connector,
                    emqx_bridge,
                    emqx_rule_engine,
                    emqx_management,
                    emqx_mgmt_api_test_util:emqx_dashboard()
                ],
                #{work_dir => emqx_cth_suite:work_dir(Config)}
            ),
            %% Create the db table
            Conn = start_clickhouse_connection(),
            % erlang:monitor,sb
            {ok, _, _} = clickhouse:query(Conn, sql_create_database(), #{}),
            {ok, _, _} = clickhouse:query(Conn, sql_create_table(), []),
            clickhouse:query(Conn, sql_find_key(42), []),
            [{apps, Apps}, {clickhouse_connection, Conn} | Config];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_clickhouse);
                _ ->
                    {skip, no_clickhouse}
            end
    end.

start_clickhouse_connection() ->
    %% Start clickhouse connector in sub process so that it does not go
    %% down with the process that is calling init_per_suite
    InitPerSuiteProcess = self(),
    erlang:spawn(
        fun() ->
            {ok, Conn} =
                clickhouse:start_link([
                    {url, clickhouse_url()},
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

end_per_suite(Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    clickhouse:stop(ClickhouseConnection),
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(_, Config) ->
    reset_table(Config),
    Config.

end_per_testcase(_, Config) ->
    reset_table(Config),
    ok.

all() ->
    emqx_common_test_helpers:all(?MODULE).

%%------------------------------------------------------------------------------
%% Helper functions for test cases
%%------------------------------------------------------------------------------

sql_insert_template_for_bridge() ->
    "INSERT INTO mqtt_test(key, data, arrived) VALUES "
    "(${key}, '${data}', ${timestamp})".

sql_insert_template_for_bridge_json() ->
    "INSERT INTO mqtt_test(key, data, arrived) FORMAT JSONCompactEachRow "
    "[${key}, \\\"${data}\\\", ${timestamp}]".

sql_create_table() ->
    "CREATE TABLE IF NOT EXISTS mqtt.mqtt_test (key BIGINT, data String, arrived BIGINT) ENGINE = Memory".

sql_find_key(Key) ->
    io_lib:format("SELECT key FROM mqtt.mqtt_test WHERE key = ~p", [Key]).

sql_find_all_keys() ->
    "SELECT key FROM mqtt.mqtt_test".

sql_drop_table() ->
    "DROP TABLE IF EXISTS mqtt.mqtt_test".

sql_create_database() ->
    "CREATE DATABASE IF NOT EXISTS mqtt".

clickhouse_host() ->
    os:getenv("CLICKHOUSE_HOST", ?CLICKHOUSE_HOST).
clickhouse_port() ->
    os:getenv("CLICKHOUSE_PORT", ?CLICKHOUSE_PORT).

clickhouse_url() ->
    Host = clickhouse_host(),
    Port = clickhouse_port(),
    erlang:iolist_to_binary(["http://", Host, ":", Port]).

parse_insert(SQL) ->
    emqx_bridge_clickhouse_connector:split_clickhouse_insert_sql(SQL).

clickhouse_config(Config) ->
    SQL = maps:get(sql, Config, sql_insert_template_for_bridge()),
    BatchSeparator = maps:get(batch_value_separator, Config, <<", ">>),
    BatchSize = maps:get(batch_size, Config, 1),
    BatchTime = maps:get(batch_time_ms, Config, 0),
    EnableBatch = maps:get(enable_batch, Config, true),
    Name = atom_to_binary(?MODULE),
    URL = clickhouse_url(),
    ConfigString =
        io_lib:format(
            "bridges.clickhouse.~s {\n"
            "  enable = true\n"
            "  url = \"~s\"\n"
            "  database = \"mqtt\"\n"
            "  sql = \"~s\"\n"
            "  batch_value_separator = \"~s\""
            "  resource_opts = {\n"
            "    enable_batch = ~w\n"
            "    batch_size = ~b\n"
            "    batch_time = ~bms\n"
            "  }\n"
            "}\n",
            [
                Name,
                URL,
                SQL,
                BatchSeparator,
                EnableBatch,
                BatchSize,
                BatchTime
            ]
        ),
    ct:pal(ConfigString),
    parse_and_check(ConfigString, <<"clickhouse">>, Name).

parse_and_check(ConfigString, BridgeType, Name) ->
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(emqx_bridge_schema, RawConf, #{required => false, atom_key => false}),
    #{<<"bridges">> := #{BridgeType := #{Name := RetConfig}}} = RawConf,
    RetConfig.

make_bridge(Config) ->
    make_bridge(Config, #{}).

make_bridge(Config, Overrides) ->
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    BridgeConfig = maps:merge(clickhouse_config(Config), Overrides),
    {ok, _} = emqx_bridge:create(
        Type,
        Name,
        BridgeConfig
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

delete_bridge() ->
    Type = <<"clickhouse">>,
    Name = atom_to_binary(?MODULE),
    ok = emqx_bridge:remove(Type, Name).

reset_table(Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_drop_table(), []),
    {ok, _, _} = clickhouse:query(ClickhouseConnection, sql_create_table(), []),
    ok.

check_key_in_clickhouse(AttempsLeft, Key, Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    check_key_in_clickhouse(AttempsLeft, Key, none, ClickhouseConnection).

check_key_in_clickhouse(Key, Config) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
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

%%------------------------------------------------------------------------------
%% Test Cases
%%------------------------------------------------------------------------------

t_make_delete_bridge(_Config) ->
    make_bridge(#{}),
    %% Check that the new brige is in the list of bridges
    Bridges = emqx_bridge:list(),
    Name = atom_to_binary(?MODULE),
    IsRightName =
        fun
            (#{name := BName}) when BName =:= Name ->
                true;
            (_) ->
                false
        end,
    true = lists:any(IsRightName, Bridges),
    delete_bridge(),
    BridgesAfterDelete = emqx_bridge:list(),
    false = lists:any(IsRightName, BridgesAfterDelete),
    ok.

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

t_send_message_query(Config) ->
    BridgeID = make_bridge(#{enable_batch => false}),
    Key = 42,
    Payload = #{key => Key, data => <<"clickhouse_data">>, timestamp => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    check_key_in_clickhouse(Key, Config),
    delete_bridge(),
    ok.

t_undefined_vars_as_null(Config) ->
    BridgeID = make_bridge(#{enable_batch => false}, #{<<"undefined_vars_as_null">> => true}),
    Key = 42,
    Payload = #{key => Key, data => undefined, timestamp => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    %% Check that the data got to the database
    check_key_in_clickhouse(Key, Config),
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    SQL = io_lib:format("SELECT data FROM mqtt.mqtt_test WHERE key = ~p", [Key]),
    {ok, 200, ResultString} = clickhouse:query(ClickhouseConnection, SQL, []),
    ?assertMatch(<<"null">>, iolist_to_binary(string:trim(ResultString))),
    delete_bridge(),
    ok.

t_send_simple_batch(Config) ->
    send_simple_batch_helper(Config, #{}).

t_send_simple_batch_alternative_format(Config) ->
    send_simple_batch_helper(
        Config,
        #{
            sql => sql_insert_template_for_bridge_json(),
            batch_value_separator => <<"">>
        }
    ).

send_simple_batch_helper(Config, BridgeConfigExt) ->
    BridgeConf = maps:merge(
        #{
            batch_size => 100,
            enable_batch => true
        },
        BridgeConfigExt
    ),
    BridgeID = make_bridge(BridgeConf),
    Key = 42,
    Payload = #{key => Key, data => <<"clickhouse_data">>, timestamp => 10000},
    %% This will use the SQL template included in the bridge
    emqx_bridge:send_message(BridgeID, Payload),
    check_key_in_clickhouse(Key, Config),
    delete_bridge(),
    ok.

t_heavy_batching(Config) ->
    heavy_batching_helper(Config, #{}).

t_heavy_batching_alternative_format(Config) ->
    heavy_batching_helper(
        Config,
        #{
            sql => sql_insert_template_for_bridge_json(),
            batch_value_separator => <<"">>
        }
    ).

heavy_batching_helper(Config, BridgeConfigExt) ->
    ClickhouseConnection = proplists:get_value(clickhouse_connection, Config),
    NumberOfMessages = 10000,
    BridgeConf = maps:merge(
        #{
            batch_size => 743,
            batch_time_ms => 50,
            enable_batch => true
        },
        BridgeConfigExt
    ),
    BridgeID = make_bridge(BridgeConf),
    SendMessageKey = fun(Key) ->
        Payload = #{
            key => Key,
            data => <<"clickhouse_data">>,
            timestamp => 10000
        },
        emqx_bridge:send_message(BridgeID, Payload)
    end,
    [SendMessageKey(Key) || Key <- lists:seq(1, NumberOfMessages)],
    % Wait until the last message is in clickhouse
    %% The delay between attempts is 100ms so 150 attempts means 15 seconds
    check_key_in_clickhouse(_AttemptsToFindKey = 150, NumberOfMessages, Config),
    %% In case the messages are not sent in order (could happend with multiple buffer workers)
    timer:sleep(1000),
    {ok, 200, ResultString1} = clickhouse:query(ClickhouseConnection, sql_find_all_keys(), []),
    ResultString2 = iolist_to_binary(string:trim(ResultString1)),
    KeyStrings = string:lexemes(ResultString2, "\n"),
    Keys = [erlang:binary_to_integer(iolist_to_binary(K)) || K <- KeyStrings],
    KeySet = maps:from_keys(Keys, true),
    NumberOfMessages = maps:size(KeySet),
    CheckKey = fun(Key) -> maps:get(Key, KeySet, false) end,
    true = lists:all(CheckKey, lists:seq(1, NumberOfMessages)),
    delete_bridge(),
    ok.
