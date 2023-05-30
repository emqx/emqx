%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_impl_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BRIDGE_TYPE_BIN, <<"iotdb">>).
-define(APPS, [emqx_bridge, emqx_resource, emqx_rule_engine, emqx_bridge_iotdb]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, plain}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {plain, AllTCs}
    ].

init_per_suite(Config) ->
    emqx_bridge_testlib:init_per_suite(Config, ?APPS).

end_per_suite(Config) ->
    emqx_bridge_testlib:end_per_suite(Config).

init_per_group(plain = Type, Config0) ->
    Host = os:getenv("IOTDB_PLAIN_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("IOTDB_PLAIN_PORT", "18080")),
    ProxyName = "iotdb",
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {proxy_name, ProxyName}
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

end_per_group(Group, Config) when
    Group =:= plain
->
    emqx_bridge_testlib:end_per_group(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) ->
    Config = emqx_bridge_testlib:init_per_testcase(TestCase, Config0, fun bridge_config/3),
    iotdb_reset(Config),
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_bridge_testlib:end_per_testcase(TestCase, Config).

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

bridge_config(TestCase, _TestGroup, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    ServerURL = iotdb_server_url(Host, Port),
    ConfigString =
        io_lib:format(
            "bridges.iotdb.~s {\n"
            "  enable = true\n"
            "  base_url = \"~s\"\n"
            "  authentication = {\n"
            "     username = \"root\"\n"
            "     password = \"root\"\n"
            "  }\n"
            "  pool_size = 1\n"
            "  resource_opts = {\n"
            "     auto_restart_interval = 5000\n"
            "     request_timeout = 30000\n"
            "     query_mode = \"async\"\n"
            "     worker_pool_size = 1\n"
            "  }\n"
            "}\n",
            [
                Name,
                ServerURL
            ]
        ),
    {Name, ConfigString, emqx_bridge_testlib:parse_and_check(Config, ConfigString, Name)}.

make_iotdb_payload(DeviceId, Measurement, Type, Value) ->
    #{
        measurement => s_to_b(Measurement),
        data_type => s_to_b(Type),
        value => s_to_b(Value),
        device_id => DeviceId,
        is_aligned => true
    }.

make_iotdb_payload(DeviceId, Measurement, Type, Value, Timestamp) ->
    Payload = make_iotdb_payload(DeviceId, Measurement, Type, Value),
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
    _BridgeConfig =
        #{
            <<"base_url">> := BaseURL,
            <<"authentication">> := #{
                <<"username">> := Username,
                <<"password">> := Password
            }
        } =
        ?config(bridge_config, Config),
    ct:pal("bridge config: ~p", [_BridgeConfig]),
    URL = <<BaseURL/binary, Path/binary>>,
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    Headers = [
        {"Content-type", "application/json"},
        {"Authorization", binary_to_list(BasicToken)}
    ],
    emqx_mgmt_api_test_util:request_api(post, URL, "", Headers, Body, Opts).

iotdb_reset(Config) ->
    Device = iotdb_device(Config),
    iotdb_reset(Config, Device).

iotdb_reset(Config, Device) ->
    Body = #{sql => <<"delete from ", Device/binary, ".*">>},
    {ok, _} = iotdb_request(Config, <<"/rest/v2/nonQuery">>, Body).

iotdb_query(Config, Query) ->
    Path = <<"/rest/v2/query">>,
    Opts = #{return_all => true},
    Body = #{sql => Query},
    iotdb_request(Config, Path, Body, Opts).

is_success_check({ok, 200, _, Body}) ->
    ?assert(is_code(200, emqx_utils_json:decode(Body))).

is_code(Code, #{<<"code">> := Code}) -> true;
is_code(_, _) -> false.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query_simple(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "INT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    Query = <<"select temp from ", DeviceId/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_async_query(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "INT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_testlib:t_async_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query_async
    ),
    Query = <<"select temp from ", DeviceId/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_sync_query_aggregated(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = [
        make_iotdb_payload(DeviceId, "temp", "INT32", "36", 1685112026290),
        make_iotdb_payload(DeviceId, "temp", "INT32", 37, 1685112026291),
        make_iotdb_payload(DeviceId, "temp", "INT32", 38.7, 1685112026292),
        make_iotdb_payload(DeviceId, "temp", "INT32", "39", <<"1685112026293">>),
        make_iotdb_payload(DeviceId, "temp", "INT64", "36", 1685112026294),
        make_iotdb_payload(DeviceId, "temp", "INT64", 36, 1685112026295),
        make_iotdb_payload(DeviceId, "temp", "INT64", 36.7, 1685112026296),
        %% implicit 'now()' timestamp
        make_iotdb_payload(DeviceId, "temp", "INT32", "40"),
        %% [FIXME] neither nanoseconds nor microseconds don't seem to be supported by IoTDB
        (make_iotdb_payload(DeviceId, "temp", "INT32", "41"))#{timestamp => <<"now_us">>},
        (make_iotdb_payload(DeviceId, "temp", "INT32", "42"))#{timestamp => <<"now_ns">>},

        make_iotdb_payload(DeviceId, "weight", "FLOAT", "87.3", 1685112026290),
        make_iotdb_payload(DeviceId, "weight", "FLOAT", 87.3, 1685112026291),
        make_iotdb_payload(DeviceId, "weight", "FLOAT", 87, 1685112026292),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", "87.3", 1685112026293),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", 87.3, 1685112026294),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", 87, 1685112026295),

        make_iotdb_payload(DeviceId, "charged", "BOOLEAN", "1", 1685112026300),
        make_iotdb_payload(DeviceId, "floated", "BOOLEAN", 1, 1685112026300),
        make_iotdb_payload(DeviceId, "started", "BOOLEAN", true, 1685112026300),
        make_iotdb_payload(DeviceId, "stoked", "BOOLEAN", "true", 1685112026300),
        make_iotdb_payload(DeviceId, "enriched", "BOOLEAN", "TRUE", 1685112026300),
        make_iotdb_payload(DeviceId, "gutted", "BOOLEAN", "True", 1685112026300),
        make_iotdb_payload(DeviceId, "drained", "BOOLEAN", "0", 1685112026300),
        make_iotdb_payload(DeviceId, "toasted", "BOOLEAN", 0, 1685112026300),
        make_iotdb_payload(DeviceId, "uncharted", "BOOLEAN", false, 1685112026300),
        make_iotdb_payload(DeviceId, "dazzled", "BOOLEAN", "false", 1685112026300),
        make_iotdb_payload(DeviceId, "unplugged", "BOOLEAN", "FALSE", 1685112026300),
        make_iotdb_payload(DeviceId, "unraveled", "BOOLEAN", "False", 1685112026300),
        make_iotdb_payload(DeviceId, "undecided", "BOOLEAN", null, 1685112026300),

        make_iotdb_payload(DeviceId, "foo", "TEXT", "bar", 1685112026300)
    ],
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),

    %% check temp
    QueryTemp = <<"select temp from ", DeviceId/binary>>,
    {ok, {{_, 200, _}, _, ResultTemp}} = iotdb_query(Config, QueryTemp),
    ?assertMatch(
        #{<<"values">> := [[36, 37, 38, 39, 36, 36, 36, 40, 41, 42]]},
        emqx_utils_json:decode(ResultTemp)
    ),
    %% check weight
    QueryWeight = <<"select weight from ", DeviceId/binary>>,
    {ok, {{_, 200, _}, _, ResultWeight}} = iotdb_query(Config, QueryWeight),
    ?assertMatch(
        #{<<"values">> := [[87.3, 87.3, 87.0, 87.3, 87.3, 87.0]]},
        emqx_utils_json:decode(ResultWeight)
    ),
    %% check rest ts = 1685112026300
    QueryRest = <<"select * from ", DeviceId/binary, " where time = 1685112026300">>,
    {ok, {{_, 200, _}, _, ResultRest}} = iotdb_query(Config, QueryRest),
    #{<<"values">> := Values, <<"expressions">> := Expressions} = emqx_utils_json:decode(
        ResultRest
    ),
    Results = maps:from_list(lists:zipwith(fun(K, [V]) -> {K, V} end, Expressions, Values)),
    Exp = #{
        exp(DeviceId, "charged") => true,
        exp(DeviceId, "floated") => true,
        exp(DeviceId, "started") => true,
        exp(DeviceId, "stoked") => true,
        exp(DeviceId, "enriched") => true,
        exp(DeviceId, "gutted") => true,
        exp(DeviceId, "drained") => false,
        exp(DeviceId, "toasted") => false,
        exp(DeviceId, "uncharted") => false,
        exp(DeviceId, "dazzled") => false,
        exp(DeviceId, "unplugged") => false,
        exp(DeviceId, "unraveled") => false,
        exp(DeviceId, "undecided") => null,
        exp(DeviceId, "foo") => <<"bar">>,
        exp(DeviceId, "temp") => null,
        exp(DeviceId, "weight") => null
    },
    ?assertEqual(Exp, Results),

    ok.

exp(Dev, M0) ->
    M = s_to_b(M0),
    <<Dev/binary, ".", M/binary>>.

t_sync_query_fail(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "INT32", "Anton"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(error, element(1, Result))
        end,
    emqx_bridge_testlib:t_sync_query(Config, MakeMessageFun, IsSuccessCheck, iotdb_bridge_on_query).

t_sync_query_badpayload(Config) ->
    BadPayload = #{foo => bar},
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual({error, invalid_data}, Result)
        end,
    emqx_bridge_testlib:t_sync_query(
        Config,
        make_message_fun(iotdb_topic(Config), BadPayload),
        IsSuccessCheck,
        iotdb_bridge_on_query
    ),
    ok.

t_async_query_badpayload(Config) ->
    BadPayload = #{foo => bar},
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual({error, invalid_data}, Result)
        end,
    emqx_bridge_testlib:t_async_query(
        Config,
        make_message_fun(iotdb_topic(Config), BadPayload),
        IsSuccessCheck,
        iotdb_bridge_on_query_async
    ),
    ok.

t_create_via_http(Config) ->
    emqx_bridge_testlib:t_create_via_http(Config).

t_start_stop(Config) ->
    emqx_bridge_testlib:t_start_stop(Config, iotdb_bridge_stopped).

t_on_get_status(Config) ->
    emqx_bridge_testlib:t_on_get_status(Config).

t_device_id(Config) ->
    ResourceId = emqx_bridge_testlib:resource_id(Config),
    %% Create without device_id configured
    ?assertMatch({ok, _}, emqx_bridge_testlib:create_bridge(Config)),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ConfiguredDevice = <<"root.someOtherDevice234">>,
    DeviceId = <<"root.deviceFooBar123">>,
    Topic = <<"some/random/topic">>,
    TopicDevice = topic_to_iotdb_device(Topic),
    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TopicDevice),
    iotdb_reset(Config, ConfiguredDevice),
    Payload1 = make_iotdb_payload(DeviceId, "test", "BOOLEAN", true),
    MessageF1 = make_message_fun(Topic, Payload1),
    ?assertNotEqual(DeviceId, TopicDevice),
    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {send_message, MessageF1()})
    ),
    {ok, {{_, 200, _}, _, Res1_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    #{<<"values">> := Values1_1} = emqx_utils_json:decode(Res1_1),
    ?assertNotEqual([], Values1_1),
    {ok, {{_, 200, _}, _, Res1_2}} = iotdb_query(Config, <<"select * from ", TopicDevice/binary>>),
    #{<<"values">> := Values1_2} = emqx_utils_json:decode(Res1_2),
    ?assertEqual([], Values1_2),

    %% test without device_id in message, taking it from topic
    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TopicDevice),
    iotdb_reset(Config, ConfiguredDevice),
    Payload2 = maps:remove(device_id, make_iotdb_payload(DeviceId, "root", "BOOLEAN", true)),
    MessageF2 = make_message_fun(Topic, Payload2),
    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {send_message, MessageF2()})
    ),
    {ok, {{_, 200, _}, _, Res2_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    #{<<"values">> := Values2_1} = emqx_utils_json:decode(Res2_1),
    ?assertEqual([], Values2_1),
    {ok, {{_, 200, _}, _, Res2_2}} = iotdb_query(Config, <<"select * from ", TopicDevice/binary>>),
    #{<<"values">> := Values2_2} = emqx_utils_json:decode(Res2_2),
    ?assertNotEqual([], Values2_2),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TopicDevice),
    iotdb_reset(Config, ConfiguredDevice),

    %% reconfigure bridge with device_id
    {ok, _} =
        emqx_bridge_testlib:update_bridge_api(Config, #{<<"device_id">> => ConfiguredDevice}),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {send_message, MessageF1()})
    ),

    %% even though we had a device_id in the message it's not being used
    {ok, {{_, 200, _}, _, Res3_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    #{<<"values">> := Values3_1} = emqx_utils_json:decode(Res3_1),
    ?assertEqual([], Values3_1),
    {ok, {{_, 200, _}, _, Res3_2}} = iotdb_query(Config, <<"select * from ", TopicDevice/binary>>),
    #{<<"values">> := Values3_2} = emqx_utils_json:decode(Res3_2),
    ?assertEqual([], Values3_2),
    {ok, {{_, 200, _}, _, Res3_3}} = iotdb_query(
        Config, <<"select * from ", ConfiguredDevice/binary>>
    ),
    #{<<"values">> := Values3_3} = emqx_utils_json:decode(Res3_3),
    ?assertNotEqual([], Values3_3),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TopicDevice),
    iotdb_reset(Config, ConfiguredDevice),
    ok.
