%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_tdengine_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

% SQL definitions
-define(SQL_BRIDGE,
    "insert into t_mqtt_msg(ts, payload) values (${timestamp}, '${payload}')"
    "t_mqtt_msg(ts, payload) values (${second_ts}, '${payload}')"
).

-define(SQL_CREATE_DATABASE, "CREATE DATABASE IF NOT EXISTS mqtt; USE mqtt;").
-define(SQL_CREATE_TABLE,
    "CREATE TABLE t_mqtt_msg (\n"
    "  ts timestamp,\n"
    "  payload BINARY(1024)\n"
    ");"
).
-define(SQL_DROP_TABLE, "DROP TABLE t_mqtt_msg").
-define(SQL_DROP_STABLE, "DROP STABLE s_tab").
-define(SQL_DELETE, "DELETE FROM t_mqtt_msg").

-define(AUTO_CREATE_BRIDGE,
    "insert into ${clientid} USING s_tab TAGS ('${clientid}') values (${timestamp}, '${payload}')"
    "test_${clientid} USING s_tab TAGS ('${clientid}') values (${second_ts}, '${payload}')"
).

-define(SQL_CREATE_STABLE,
    "CREATE STABLE s_tab (\n"
    "  ts timestamp,\n"
    "  payload BINARY(1024)\n"
    ") TAGS (clientid BINARY(128));"
).

% DB defaults
-define(TD_DATABASE, "mqtt").
-define(TD_USERNAME, "root").
-define(TD_PASSWORD, "taosdata").
-define(BATCH_SIZE, 10).
-define(PAYLOAD, <<"HELLO">>).

-define(WITH_CON(Process),
    Con = connect_direct_tdengine(Config),
    Process,
    ok = tdengine:stop(Con)
).

-define(BRIDGE_TYPE_BIN, <<"tdengine">>).

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
    MustBatchCases = [t_batch_insert, t_auto_create_batch_insert],
    BatchingGroups = [{group, with_batch}, {group, without_batch}],
    [
        {async, BatchingGroups},
        {sync, BatchingGroups},
        {with_batch, TCs},
        {without_batch, TCs -- MustBatchCases}
    ].

-define(APPS, [
    emqx,
    emqx_conf,
    emqx_bridge_tdengine,
    emqx_connector,
    emqx_bridge,
    emqx_rule_engine,
    emqx_management
]).

init_per_suite(Config) ->
    emqx_bridge_v2_testlib:init_per_suite(
        Config, ?APPS ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
    ).

end_per_suite(Config) ->
    emqx_bridge_v2_testlib:end_per_suite([{apps, ?APPS} | Config]).

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

end_per_group(Group, Config) when
    Group =:= with_batch;
    Group =:= without_batch
->
    emqx_bridge_v2_testlib:end_per_group(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) ->
    connect_and_clear_table(Config0),
    Type = ?config(bridge_type, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    {_ConfigString, ConnectorConfig} = connector_config(Name, Config0),
    {_, ActionConfig} = action_config(TestCase, Name, Config0),
    Config = [
        {connector_type, Type},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, Type},
        {bridge_name, Name},
        {bridge_config, ActionConfig}
        | Config0
    ],
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_bridge_v2_testlib:end_per_testcase(TestCase, Config),
    connect_and_clear_table(Config),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

common_init(ConfigT) ->
    Host = os:getenv("TDENGINE_HOST", "toxiproxy"),
    Port = list_to_integer(os:getenv("TDENGINE_PORT", "6041")),

    Config0 = [
        {td_host, Host},
        {td_port, Port},
        {proxy_name, "tdengine_restful"}
        | ConfigT
    ],

    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(default, ?BRIDGE_TYPE_BIN, Config0),
            connect_and_create_table(Config),
            Config;
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_tdengine);
                _ ->
                    {skip, no_tdengine}
            end
    end.

action_config(TestCase, Name, Config) ->
    Type = ?config(bridge_type, Config),
    BatchSize =
        case ?config(enable_batch, Config) of
            true -> ?BATCH_SIZE;
            false -> 1
        end,
    QueryMode = ?config(query_mode, Config),
    ConfigString =
        io_lib:format(
            "actions.~s.~s {\n"
            "  enable = true\n"
            "  connector = \"~s\"\n"
            "  parameters = {\n"
            "    database = ~p\n"
            "    sql = ~p\n"
            "  }\n"
            "  resource_opts = {\n"
            "    request_ttl = 500ms\n"
            "    batch_size = ~b\n"
            "    query_mode = ~s\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                Name,
                ?TD_DATABASE,
                case TestCase of
                    Auto when
                        Auto =:= t_auto_create_simple_insert; Auto =:= t_auto_create_batch_insert
                    ->
                        ?AUTO_CREATE_BRIDGE;
                    _ ->
                        ?SQL_BRIDGE
                end,
                BatchSize,
                QueryMode
            ]
        ),
    ct:pal("ActionConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_action_and_check(ConfigString, Type, Name)}.

connector_config(Name, Config) ->
    Host = ?config(td_host, Config),
    Port = ?config(td_port, Config),
    Type = ?config(bridge_type, Config),
    Server = Host ++ ":" ++ integer_to_list(Port),
    ConfigString =
        io_lib:format(
            "connectors.~s.~s {\n"
            "  enable = true\n"
            "  server = \"~s\"\n"
            "  username = ~p\n"
            "  password = ~p\n"
            "}\n",
            [
                Type,
                Name,
                Server,
                ?TD_USERNAME,
                ?TD_PASSWORD
            ]
        ),
    ct:pal("ConnectorConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_connector_and_check(ConfigString, Type, Name)}.

parse_action_and_check(ConfigString, BridgeType, Name) ->
    parse_and_check(ConfigString, emqx_bridge_schema, <<"actions">>, BridgeType, Name).

parse_connector_and_check(ConfigString, ConnectorType, Name) ->
    parse_and_check(
        ConfigString, emqx_connector_schema, <<"connectors">>, ConnectorType, Name
    ).

parse_and_check(ConfigString, SchemaMod, RootKey, Type0, Name) ->
    Type = to_bin(Type0),
    {ok, RawConf} = hocon:binary(ConfigString, #{format => map}),
    hocon_tconf:check_plain(SchemaMod, RawConf, #{required => false, atom_key => false}),
    #{RootKey := #{Type := #{Name := Config}}} = RawConf,
    Config.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom);
to_bin(Bin) when is_binary(Bin) ->
    Bin.

send_message(Config, Payload) ->
    BridgeType = ?config(bridge_type, Config),
    Name = ?config(bridge_name, Config),
    ct:print(">>> Name:~p~n BridgeType:~p~n", [Name, BridgeType]),
    emqx_bridge_v2:send_message(BridgeType, Name, Payload, #{}).

receive_result(Ref, Timeout) ->
    receive
        {result, Ref, Result} ->
            {ok, Result};
        {Ref, Result} ->
            {ok, Result}
    after Timeout ->
        timeout
    end.

connect_direct_tdengine(Config) ->
    Opts = [
        {host, to_bin(?config(td_host, Config))},
        {port, ?config(td_port, Config)},
        {username, to_bin(?TD_USERNAME)},
        {password, to_bin(?TD_PASSWORD)},
        {pool_size, 8}
    ],

    {ok, Con} = tdengine:start_link(Opts),
    Con.

% These funs connect and then stop the tdengine connection
connect_and_create_table(Config) ->
    ?WITH_CON(begin
        _ = directly_query(Con, ?SQL_DROP_TABLE),
        _ = directly_query(Con, ?SQL_DROP_STABLE),
        {ok, _} = directly_query(Con, ?SQL_CREATE_DATABASE, []),
        {ok, _} = directly_query(Con, ?SQL_CREATE_TABLE),
        {ok, _} = directly_query(Con, ?SQL_CREATE_STABLE)
    end).

connect_and_clear_table(Config) ->
    ?WITH_CON({ok, _} = directly_query(Con, ?SQL_DELETE)).

connect_and_get_payload(Config) ->
    connect_and_get_column(Config, "SELECT payload FROM t_mqtt_msg").

connect_and_get_column(Config, Select) ->
    ?WITH_CON(
        {ok, #{<<"code">> := 0, <<"data">> := Result}} = directly_query(Con, Select)
    ),
    Result.

connect_and_exec(Config, SQL) ->
    ?WITH_CON({ok, _} = directly_query(Con, SQL)).

connect_and_query(Config, SQL) ->
    ?WITH_CON(
        {ok, #{<<"code">> := 0, <<"data">> := Data}} = directly_query(Con, SQL)
    ),
    Data.

directly_query(Con, Query) ->
    directly_query(Con, Query, [{db_name, ?TD_DATABASE}]).

directly_query(Con, Query, QueryOpts) ->
    tdengine:insert(Con, Query, QueryOpts).

is_success_check(Result) ->
    ?assertMatch({ok, #{<<"code">> := 0}}, Result).

to_str(Atom) when is_atom(Atom) ->
    erlang:atom_to_list(Atom).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config, #{failure_status => connecting}).

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, tdengine_connector_stop).

t_invalid_data(Config) ->
    MakeMessageFun = fun() -> #{} end,
    IsSuccessCheck = fun(Result) ->
        ?assertMatch(
            {error, #{
                <<"code">> := 534,
                <<"desc">> := _
            }},
            Result
        )
    end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsSuccessCheck, tdengine_connector_query_return
    ),

    ok.

t_simple_insert(Config) ->
    connect_and_clear_table(Config),

    MakeMessageFun = fun() ->
        #{payload => ?PAYLOAD, timestamp => 1668602148000, second_ts => 1668602148010}
    end,

    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, tdengine_connector_query_return
    ),

    ?assertMatch(
        [[?PAYLOAD], [?PAYLOAD]],
        connect_and_get_payload(Config)
    ).

t_simple_insert_undefined(Config) ->
    connect_and_clear_table(Config),

    MakeMessageFun = fun() ->
        #{payload => undefined, timestamp => 1668602148000, second_ts => 1668602148010}
    end,

    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, tdengine_connector_query_return
    ),

    ?assertMatch(
        %% the old behavior without undefined_vars_as_null
        [[<<"undefined">>], [<<"undefined">>]],
        connect_and_get_payload(Config)
    ).

t_undefined_vars_as_null(Config0) ->
    Config = patch_bridge_config(Config0, #{
        <<"parameters">> => #{<<"undefined_vars_as_null">> => true}
    }),
    connect_and_clear_table(Config),

    MakeMessageFun = fun() ->
        #{payload => undefined, timestamp => 1668602148000, second_ts => 1668602148010}
    end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, tdengine_connector_query_return
    ),

    ?assertMatch(
        [[<<"null">>], [<<"null">>]],
        connect_and_get_payload(Config)
    ).

t_batch_insert(Config) ->
    connect_and_clear_table(Config),
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),

    Size = 5,
    Ts = erlang:system_time(millisecond),
    {_, {ok, #{result := _Result}}} =
        ?wait_async_action(
            lists:foreach(
                fun(Idx) ->
                    SentData = #{
                        payload => ?PAYLOAD, timestamp => Ts + Idx, second_ts => Ts + Idx + 5000
                    },
                    send_message(Config, SentData)
                end,
                lists:seq(1, Size)
            ),

            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),

    DoubleSize = Size * 2,
    ?retry(
        _Sleep = 50,
        _Attempts = 30,
        ?assertMatch(
            [[DoubleSize]],
            connect_and_query(Config, "SELECT COUNT(1) FROM t_mqtt_msg")
        )
    ).

t_auto_create_simple_insert(Config) ->
    ClientId = to_str(?FUNCTION_NAME),

    MakeMessageFun = fun() ->
        #{
            payload => ?PAYLOAD,
            timestamp => 1668602148000,
            second_ts => 1668602148000 + 100,
            clientid => ClientId
        }
    end,

    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, tdengine_connector_query_return
    ),

    ?assertMatch(
        [[?PAYLOAD]],
        connect_and_query(Config, "SELECT payload FROM " ++ ClientId)
    ),

    ?assertMatch(
        [[?PAYLOAD]],
        connect_and_query(Config, "SELECT payload FROM test_" ++ ClientId)
    ),

    ?assertMatch(
        [[0]],
        connect_and_query(Config, "DROP TABLE " ++ ClientId)
    ),

    ?assertMatch(
        [[0]],
        connect_and_query(Config, "DROP TABLE test_" ++ ClientId)
    ).

t_auto_create_batch_insert(Config) ->
    ClientId1 = "client1",
    ClientId2 = "client2",
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),

    Size1 = 2,
    Size2 = 3,

    Ts = erlang:system_time(millisecond),
    {_, {ok, #{result := _Result}}} =
        ?wait_async_action(
            lists:foreach(
                fun({Offset, ClientId, Size}) ->
                    lists:foreach(
                        fun(Idx) ->
                            SentData = #{
                                payload => ?PAYLOAD,
                                timestamp => Ts + Idx + Offset,
                                second_ts => Ts + Idx + Offset + 5000,
                                clientid => ClientId
                            },
                            send_message(Config, SentData)
                        end,
                        lists:seq(1, Size)
                    )
                end,
                [{0, ClientId1, Size1}, {100, ClientId2, Size2}]
            ),
            #{?snk_kind := buffer_worker_flush_ack},
            2_000
        ),

    ?retry(
        _Sleep = 50,
        _Attempts = 30,

        lists:foreach(
            fun({Table, Size}) ->
                ?assertMatch(
                    [[Size]],
                    connect_and_query(Config, "SELECT COUNT(1) FROM " ++ Table)
                )
            end,
            lists:zip(
                [ClientId1, "test_" ++ ClientId1, ClientId2, "test_" ++ ClientId2],
                [Size1, Size1, Size2, Size2]
            )
        )
    ),

    lists:foreach(
        fun(E) ->
            ?assertMatch(
                [[0]],
                connect_and_query(Config, "DROP TABLE " ++ E)
            )
        end,
        [ClientId1, ClientId2, "test_" ++ ClientId1, "test_" ++ ClientId2]
    ).

patch_bridge_config(Config, Overrides) ->
    BridgeConfig0 = ?config(bridge_config, Config),
    BridgeConfig1 = emqx_utils_maps:deep_merge(BridgeConfig0, Overrides),
    [{bridge_config, BridgeConfig1} | proplists:delete(bridge_config, Config)].
