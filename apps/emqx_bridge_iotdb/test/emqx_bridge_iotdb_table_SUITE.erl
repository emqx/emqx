%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_table_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_bridge_iotdb.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE_BIN, <<"iotdb">>).
-define(DATABASE, <<"test_db">>).
-define(TABLE, <<"test_table">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, restapi},
        {group, thrift}
    ].

groups() ->
    RestAPIOnlyTCs = [t_table_model_only_supports_iotdb_2_0_x_and_later],
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {restapi, AllTCs},
        {thrift, (AllTCs -- [t_async_query]) -- RestAPIOnlyTCs}
    ].

init_per_suite(Config) ->
    emqx_bridge_v2_testlib:init_per_suite(
        Config,
        [
            emqx,
            emqx_conf,
            emqx_bridge_iotdb,
            emqx_connector,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ]
    ).

end_per_suite(Config) ->
    emqx_bridge_v2_testlib:end_per_suite(Config).

init_per_group(restapi = Type, Config0) ->
    Host = os:getenv("IOTDB_PLAIN_HOST", "toxiproxy.emqx.net"),
    IotDbVersion = ?VSN_2_0_X,
    DefaultPort = "58080",
    Port = list_to_integer(os:getenv("IOTDB_PLAIN_PORT", DefaultPort)),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {rest_port, Port},
                {proxy_name, <<"iotdb205_rest">>},
                {iotdb_version, IotDbVersion},
                {iotdb_rest_prefix, <<"/rest/table/v1/">>}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_iotdb);
                _ ->
                    {skip, no_iotdb}
            end
    end;
init_per_group(thrift = Type, Config0) ->
    Host = os:getenv("IOTDB_THRIFT_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("IOTDB_THRIFT_PORT", "56667")),
    DefaultPort = "58080",
    RestPort = list_to_integer(os:getenv("IOTDB_PLAIN_PORT", DefaultPort)),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {rest_port, RestPort},
                {proxy_name, <<"iotdb205_thrift">>},
                {iotdb_version, ?PROTOCOL_V3},
                {iotdb_rest_prefix, <<"/rest/table/v1/">>}
                | Config
            ];
        false ->
            case os:getenv("IS_CI") of
                "yes" ->
                    throw(no_iotdb);
                _ ->
                    {skip, no_iotdb}
            end
    end;
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, Config) ->
    emqx_bridge_v2_testlib:end_per_group(Config),
    ok.

init_per_testcase(TestCase, Config0) ->
    Type = ?BRIDGE_TYPE_BIN,
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    {_ConfigString, ConnectorConfig} = connector_config(TestCase, Name, Config0),

    {_, ActionConfig} = action_config(TestCase, Name, Config0),
    Config = [
        {bridge_kind, action},
        {connector_type, Type},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, Type},
        {action_name, Name},
        {action_config, ActionConfig}
        | Config0
    ],
    ok = iotdb_create_db(Config),
    ok = iotdb_create_table(Config),
    iotdb_reset(Config),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_bridge_v2_testlib:end_per_testcase(TestCase, Config).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

iotdb_server_url(Host, Port) ->
    iolist_to_binary([
        "http://",
        Host,
        ":",
        integer_to_binary(Port)
    ]).

make_iotdb_payload(Measurement, Type, Value) ->
    #{
        measurement => s_to_b(Measurement),
        data_type => s_to_b(Type),
        value => s_to_b(Value),
        is_aligned => true
    }.

make_iotdb_payload(Measurement, Type, Value, Timestamp) ->
    Payload = make_iotdb_payload(Measurement, Type, Value),
    Payload#{timestamp => Timestamp}.

s_to_b(S) when is_list(S) -> list_to_binary(S);
s_to_b(V) -> V.

make_message_fun(Topic, Payload) ->
    fun() ->
        MsgId = erlang:unique_integer([positive]),
        #{
            topic => Topic,
            id => MsgId,
            payload => emqx_utils_json:encode(Payload),
            retain => true
        }
    end.

iotdb_topic(Config) ->
    ?config(mqtt_topic, Config).

iotdb_device(Config) ->
    Topic = iotdb_topic(Config),
    topic_to_iotdb_device(Topic).

topic_to_iotdb_device(Topic) ->
    Device = re:replace(Topic, "/", ".", [global, {return, binary}]),
    <<"root.", Device/binary>>.

iotdb_request(Config, Path, Body) ->
    iotdb_request(Config, Path, Body, #{}).

iotdb_request(Config, Path, Body, Opts) ->
    BridgeConfig = ?config(connector_config, Config),
    Host = ?config(bridge_host, Config),
    Port = ?config(rest_port, Config),
    Username = <<"root">>,
    Password = <<"root">>,
    BaseURL = iotdb_server_url(Host, Port),

    ct:pal("bridge config: ~p", [BridgeConfig]),
    URL = <<BaseURL/binary, Path/binary>>,
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    Headers = [
        {"Content-type", "application/json"},
        {"Authorization", binary_to_list(BasicToken)}
    ],
    emqx_mgmt_api_test_util:request_api(post, URL, "", Headers, Body, Opts).

iotdb_create_db(Config) ->
    Prefix = ?config(iotdb_rest_prefix, Config),
    Body = #{sql => <<"create database if not exists ", ?DATABASE/binary>>},
    {ok, _} = iotdb_request(Config, <<Prefix/binary, "nonQuery">>, Body),
    ok.

iotdb_create_table(Config) ->
    Prefix = ?config(iotdb_rest_prefix, Config),
    Sql =
        <<"create table if not exists ", ?TABLE/binary, " (", "time TIMESTAMP TIME,",
            "value INT32"
            ")">>,
    Body = #{sql => Sql, database => ?DATABASE},
    {ok, _} = iotdb_request(Config, <<Prefix/binary, "nonQuery">>, Body),
    ok.

iotdb_reset(Config) ->
    Prefix = ?config(iotdb_rest_prefix, Config),
    Body = #{sql => <<"delete from ", ?TABLE/binary>>, database => ?DATABASE},
    {ok, _} = iotdb_request(Config, <<Prefix/binary, "nonQuery">>, Body).

iotdb_query(Config, Query) ->
    Prefix = ?config(iotdb_rest_prefix, Config),
    Path = <<Prefix/binary, "query">>,
    Opts = #{return_all => true},
    Body = #{sql => Query, database => ?DATABASE},
    iotdb_request(Config, Path, Body, Opts).

is_success_check({ok, 200, _, Body}) ->
    ?assert(is_code(200, emqx_utils_json:decode(Body)));
is_success_check({ok, _}) ->
    ok;
is_success_check(Other) ->
    throw(Other).

is_code(Code, #{<<"code">> := Code}) -> true;
is_code(_, _) -> false.

is_error_check(Reason) ->
    fun(Result) ->
        ?assertEqual({error, Reason}, Result)
    end.

action_config(TestCase, ConnectorName, _Config) ->
    Type = ?BRIDGE_TYPE_BIN,
    QueryMode = query_mode(TestCase),
    DataTemplate = default_data_template(TestCase),
    ConfigString =
        io_lib:format(
            "enable = true\n"
            "connector = \"~s\"\n"
            "parameters = {\n"
            "    write_to_table = true\n"
            "    table = \"~s\"\n"
            "   data = [~s]\n"
            "}\n"
            "resource_opts = {\n"
            "   query_mode = \"~s\"\n"
            "}\n",
            [
                ConnectorName,
                ?TABLE,
                DataTemplate,
                QueryMode
            ]
        ),
    ct:pal("ActionConfig:~ts~n", [ConfigString]),
    {ok, ConfMap} = hocon:binary(ConfigString),
    {ConfigString, emqx_bridge_v2_testlib:parse_and_check(Type, <<"x">>, ConfMap)}.

connector_config(TestCase, Name, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Type = ?BRIDGE_TYPE_BIN,
    Version = ?config(iotdb_version, Config),
    ServerURL = iotdb_server_url(Host, Port),
    ConfigString =
        case ?config(test_group, Config) of
            thrift ->
                Server = make_thrift_server(TestCase, Config),
                io_lib:format(
                    "enable = true\n"
                    "driver = \"thrift\"\n"
                    "server = \"~s\"\n"
                    "protocol_version = \"~p\"\n"
                    "username = \"root\"\n"
                    "password = \"root\"\n"
                    "zoneId = \"Asia/Shanghai\"\n"
                    "ssl.enable = false\n"
                    "sql = {\n"
                    "   dialect = \"table\"\n"
                    "   database = \"~s\"\n"
                    "}\n",
                    [
                        Server,
                        Version,
                        ?DATABASE
                    ]
                );
            _ ->
                io_lib:format(
                    "enable = true\n"
                    "base_url = \"~s\"\n"
                    "max_inactive = 10s\n"
                    "iotdb_version = \"~s\"\n"
                    "authentication = {\n"
                    "  username = \"root\"\n"
                    "  password = \"root\"\n"
                    "}\n"
                    "sql = {\n"
                    "   dialect = \"table\"\n"
                    "   database = \"~s\"\n"
                    "}\n",
                    [
                        ServerURL,
                        Version,
                        ?DATABASE
                    ]
                )
        end,
    ct:pal("ConnectorConfig:~ts~n", [ConfigString]),
    {ok, ConfMap} = hocon:binary(ConfigString),
    {ConfigString, emqx_bridge_v2_testlib:parse_and_check_connector(Type, Name, ConfMap)}.

to_bin(List) when is_list(List) ->
    unicode:characters_to_binary(List, utf8);
to_bin(Atom) when is_atom(Atom) ->
    erlang:atom_to_binary(Atom);
to_bin(Bin) when is_binary(Bin) ->
    Bin.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query_simple(Config) ->
    Payload = make_iotdb_payload("temp", "int32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    Query = <<"select temp from ", ?TABLE/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_async_query(Config) ->
    Payload = make_iotdb_payload("temp", "int32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_async_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query_async
    ),
    Query = <<"select temp from ", ?TABLE/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_sync_query_fail(Config) ->
    Payload = make_iotdb_payload("temp", "int32", "Anton"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(error, element(1, Result))
        end,
    emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsSuccessCheck, iotdb_bridge_on_query
    ).

t_create_via_http(Config) ->
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, #{})
    ),
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, #{})
    ),
    ok.

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, iotdb_bridge_stopped).

t_probe_connector_api(Config) ->
    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:probe_connector_api(Config)
    ).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_template(Config) ->
    %% Create without data configured
    ?assertMatch(
        {error, #{reason := empty_array_not_allowed}},
        emqx_bridge_v2_testlib:create_bridge(
            Config,
            #{<<"parameters">> => #{<<"data">> => []}}
        )
    ),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),

    %% reconfigure with data template
    {ok, _} =
        emqx_bridge_v2_testlib:create_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"table">> => ?TABLE,
                <<"data">> => [
                    #{
                        <<"measurement">> => <<"${payload.measurement}">>,
                        <<"data_type">> => <<"text">>,
                        <<"column_category">> => <<"field">>,
                        <<"value">> => <<"${payload.value}">>
                    }
                ]
            }
        }),

    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    Topic = <<"some/random/topic">>,
    Payload1 = make_iotdb_payload("test", "text", "hi, there"),
    MessageF1 = make_message_fun(Topic, Payload1),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

    {ok, {{_, 200, _}, _, Res2_2}} = iotdb_query(
        Config, <<"select test from ", ?TABLE/binary>>
    ),

    ?assertMatch(#{<<"values">> := [[<<"hi, there">>]]}, emqx_utils_json:decode(Res2_2)),
    ok.

t_sync_query_case(Config) ->
    Payload = make_iotdb_payload("temp", "InT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    Query = <<"select temp from ", ?TABLE/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_sync_query_unmatched_type(Config) ->
    Payload = make_iotdb_payload("temp", "boolean", "not boolean"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ).

t_sync_query_with_lowercase(Config) ->
    Payload = make_iotdb_payload("temp", "int32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    Query = <<"select temp from ", ?TABLE/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_sync_query_plain_text(Config) ->
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload = <<"this is a text">>,
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ).

t_sync_query_invalid_json(Config) ->
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload2 = <<"{\"msg\":}">>,
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload2),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ).

t_rule_test_trace(Config) ->
    PayloadFn = fun() ->
        Msg = make_iotdb_payload("temp", "int32", "36"),
        emqx_utils_json:encode(Msg)
    end,
    Opts = #{payload_fn => PayloadFn},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).

t_sql_dialect_database_required(Config) ->
    ConnectorConfig = ?config(connector_config, Config),
    BadConnectorConfig = ConnectorConfig#{
        <<"sql">> => #{<<"dialect">> => <<"table">>, <<"database">> => <<>>}
    },
    Config1 = lists:keyreplace(connector_config, 1, Config, {connector_config, BadConnectorConfig}),
    {error, {_StatusCode, _Headers, Body}} = emqx_bridge_v2_testlib:create_connector_api(Config1),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{
                <<"reason">> := <<"empty_string_not_allowed">>,
                <<"value">> := <<>>
            }
        },
        Body
    ),
    ok.

t_table_name_configuration_validation(Config) ->
    ActionConfig = ?config(action_config, Config),
    Parameters = maps:get(<<"parameters">>, ActionConfig),
    BadActionConfig1 = ActionConfig#{<<"parameters">> => Parameters#{<<"table">> => <<>>}},
    BadConfig1 = lists:keyreplace(action_config, 1, Config, {action_config, BadActionConfig1}),
    {error, {_, _, Body1}} = emqx_bridge_v2_testlib:create_bridge_api(BadConfig1),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{
                <<"path">> := <<"root.parameters.table">>,
                <<"value">> := <<>>
            }
        },
        Body1
    ),
    ok.

t_table_model_only_supports_iotdb_2_0_x_and_later(Config) ->
    ConnectorConfig = ?config(connector_config, Config),
    BadConnectorConfig = ConnectorConfig#{
        <<"iotdb_version">> => atom_to_binary(?VSN_1_3_X)
    },
    Config1 = lists:keyreplace(connector_config, 1, Config, {connector_config, BadConnectorConfig}),
    {error, {_StatusCode, _Headers, Body}} = emqx_bridge_v2_testlib:create_connector_api(Config1),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{
                <<"reason">> := <<"Table model only supports IoTDB 2.0.x and later">>
            }
        },
        Body
    ),
    ok.

is_empty(null) -> true;
is_empty([]) -> true;
is_empty([[]]) -> true;
is_empty(_) -> false.

make_thrift_server(_, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    lists:flatten(io_lib:format("~s:~p", [Host, Port])).

query_mode(TestCase) ->
    Name = erlang:atom_to_list(TestCase),
    Tokens = string:tokens(Name, "_"),
    case lists:member("async", Tokens) of
        true ->
            async;
        _ ->
            sync
    end.

default_data_template(_TestCase) ->
    emqx_utils_json:encode(
        #{
            <<"measurement">> => <<"${payload.measurement}">>,
            <<"data_type">> => <<"int32">>,
            <<"column_category">> => <<"field">>,
            <<"value">> => <<"${payload.value}">>
        }
    ).
