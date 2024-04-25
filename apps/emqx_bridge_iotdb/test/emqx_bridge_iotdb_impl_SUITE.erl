%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_impl_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_bridge_iotdb.hrl").
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
        {group, iotdb110},
        {group, iotdb130},
        {group, legacy}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {iotdb110, AllTCs},
        {iotdb130, AllTCs},
        {legacy, AllTCs}
    ].

init_per_suite(Config) ->
    emqx_bridge_v2_testlib:init_per_suite(Config, ?APPS).

end_per_suite(Config) ->
    emqx_bridge_v2_testlib:end_per_suite(Config).

init_per_group(Type, Config0) when Type =:= iotdb110 orelse Type =:= iotdb130 ->
    Host = os:getenv("IOTDB_PLAIN_HOST", "toxiproxy.emqx.net"),
    ProxyName = atom_to_list(Type),
    {IotDbVersion, DefaultPort} =
        case Type of
            iotdb110 -> {?VSN_1_1_X, "18080"};
            iotdb130 -> {?VSN_1_3_X, "28080"}
        end,
    Port = list_to_integer(os:getenv("IOTDB_PLAIN_PORT", DefaultPort)),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {proxy_name, ProxyName},
                {iotdb_version, IotDbVersion},
                {iotdb_rest_prefix, <<"/rest/v2/">>}
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
init_per_group(legacy = Type, Config0) ->
    Host = os:getenv("IOTDB_LEGACY_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("IOTDB_LEGACY_PORT", "38080")),
    ProxyName = "iotdb013",
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {proxy_name, ProxyName},
                {iotdb_version, ?VSN_0_13_X},
                {iotdb_rest_prefix, <<"/rest/v1/">>}
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
    Group =:= iotdb110;
    Group =:= iotdb130;
    Group =:= legacy
->
    emqx_bridge_v2_testlib:end_per_group(Config),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config0) ->
    Type = ?config(bridge_type, Config0),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    {_ConfigString, ConnectorConfig} = connector_config(Name, Config0),
    {_, ActionConfig} = action_config(Name, Config0),
    Config = [
        {connector_type, Type},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {bridge_type, Type},
        {bridge_name, Name},
        {bridge_config, ActionConfig}
        | Config0
    ],
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

bridge_config(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Version = ?config(iotdb_version, Config),
    Type = ?config(bridge_type, Config),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    ServerURL = iotdb_server_url(Host, Port),
    ConfigString =
        io_lib:format(
            "bridges.~s.~s {\n"
            "  enable = true\n"
            "  base_url = \"~s\"\n"
            "  authentication = {\n"
            "     username = \"root\"\n"
            "     password = \"root\"\n"
            "  }\n"
            "  iotdb_version = \"~s\"\n"
            "  pool_size = 1\n"
            "  resource_opts = {\n"
            "     health_check_interval = \"1s\"\n"
            "     request_ttl = 30s\n"
            "     query_mode = \"async\"\n"
            "     worker_pool_size = 1\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                ServerURL,
                Version
            ]
        ),
    {ok, InnerConfigMap} = hocon:binary(ConfigString),
    {Name, ConfigString, emqx_bridge_v2_testlib:parse_and_check(Type, Name, InnerConfigMap)}.

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
        ?config(connector_config, Config),
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
    Prefix = ?config(iotdb_rest_prefix, Config),
    Body = #{sql => <<"delete from ", Device/binary, ".*">>},
    {ok, _} = iotdb_request(Config, <<Prefix/binary, "nonQuery">>, Body).

iotdb_query(Config, Query) ->
    Prefix = ?config(iotdb_rest_prefix, Config),
    Path = <<Prefix/binary, "query">>,
    Opts = #{return_all => true},
    Body = #{sql => Query},
    iotdb_request(Config, Path, Body, Opts).

is_success_check({ok, 200, _, Body}) ->
    ?assert(is_code(200, emqx_utils_json:decode(Body)));
is_success_check(Other) ->
    throw(Other).

is_code(Code, #{<<"code">> := Code}) -> true;
is_code(_, _) -> false.

is_error_check(Reason) ->
    fun(Result) ->
        ?assertEqual({error, Reason}, Result)
    end.

action_config(Name, Config) ->
    Type = ?config(bridge_type, Config),
    ConfigString =
        io_lib:format(
            "actions.~s.~s {\n"
            "  enable = true\n"
            "  connector = \"~s\"\n"
            "  parameters = {\n"
            "     data = []\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                Name
            ]
        ),
    ct:pal("ActionConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_action_and_check(ConfigString, Type, Name)}.

connector_config(Name, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Type = ?config(bridge_type, Config),
    Version = ?config(iotdb_version, Config),
    ServerURL = iotdb_server_url(Host, Port),
    ConfigString =
        io_lib:format(
            "connectors.~s.~s {\n"
            "  enable = true\n"
            "  base_url = \"~s\"\n"
            "  iotdb_version = \"~s\"\n"
            "  authentication = {\n"
            "     username = \"root\"\n"
            "     password = \"root\"\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                ServerURL,
                Version
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
%%    emqx_utils_maps:safe_atom_key_map(Config).

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

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query_simple(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "INT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
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
    ok = emqx_bridge_v2_testlib:t_async_query(
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
    MS = erlang:system_time(millisecond) - 5000,
    Payload = [
        make_iotdb_payload(DeviceId, "temp", "INT32", "36", MS - 7000),
        make_iotdb_payload(DeviceId, "temp", "INT32", 37, MS - 6000),
        make_iotdb_payload(DeviceId, "temp", "INT64", 38.7, MS - 5000),
        make_iotdb_payload(DeviceId, "temp", "INT64", "39", integer_to_binary(MS - 4000)),
        make_iotdb_payload(DeviceId, "temp", "INT64", "34", MS - 3000),
        make_iotdb_payload(DeviceId, "temp", "INT32", 33.7, MS - 2000),
        make_iotdb_payload(DeviceId, "temp", "INT32", 32, MS - 1000),
        %% [FIXME] neither nanoseconds nor microseconds don't seem to be supported by IoTDB
        (make_iotdb_payload(DeviceId, "temp", "INT32", "41"))#{timestamp => <<"now_us">>},

        make_iotdb_payload(DeviceId, "weight", "FLOAT", "87.3", MS - 6000),
        make_iotdb_payload(DeviceId, "weight", "FLOAT", 87.3, MS - 5000),
        make_iotdb_payload(DeviceId, "weight", "FLOAT", 87, MS - 4000),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", "87.3", MS - 3000),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", 87.3, MS - 2000),
        make_iotdb_payload(DeviceId, "weight", "DOUBLE", 87, MS - 1000),

        make_iotdb_payload(DeviceId, "charged", "BOOLEAN", "1", MS + 1000),
        make_iotdb_payload(DeviceId, "floated", "BOOLEAN", 1, MS + 1000),
        make_iotdb_payload(DeviceId, "started", "BOOLEAN", true, MS + 1000),
        make_iotdb_payload(DeviceId, "stoked", "BOOLEAN", "true", MS + 1000),
        make_iotdb_payload(DeviceId, "enriched", "BOOLEAN", "TRUE", MS + 1000),
        make_iotdb_payload(DeviceId, "gutted", "BOOLEAN", "True", MS + 1000),
        make_iotdb_payload(DeviceId, "drained", "BOOLEAN", "0", MS + 1000),
        make_iotdb_payload(DeviceId, "toasted", "BOOLEAN", 0, MS + 1000),
        make_iotdb_payload(DeviceId, "uncharted", "BOOLEAN", false, MS + 1000),
        make_iotdb_payload(DeviceId, "dazzled", "BOOLEAN", "false", MS + 1000),
        make_iotdb_payload(DeviceId, "unplugged", "BOOLEAN", "FALSE", MS + 1000),
        make_iotdb_payload(DeviceId, "unraveled", "BOOLEAN", "False", MS + 1000),
        make_iotdb_payload(DeviceId, "undecided", "BOOLEAN", null, MS + 1000),

        make_iotdb_payload(DeviceId, "foo", "TEXT", "bar", MS + 1000)
    ],
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),

    Time = integer_to_binary(MS - 20000),
    %% check weight
    QueryWeight = <<"select weight from ", DeviceId/binary, " where time > ", Time/binary>>,
    {ok, {{_, 200, _}, _, ResultWeight}} = iotdb_query(Config, QueryWeight),
    ?assertMatch(
        #{<<"values">> := [[87.3, 87.3, 87.0, 87.3, 87.3, 87.0]]},
        emqx_utils_json:decode(ResultWeight)
    ),
    %% [FIXME] https://github.com/apache/iotdb/issues/12375
    %% null don't seem to be supported by IoTDB insertTablet when 1.3.0
    case ?config(iotdb_version, Config) of
        ?VSN_1_3_X ->
            skip;
        _ ->
            %% check rest ts = MS + 1000
            CheckTime = integer_to_binary(MS + 1000),
            QueryRest = <<"select * from ", DeviceId/binary, " where time = ", CheckTime/binary>>,
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

            %% check temp
            QueryTemp = <<"select temp from ", DeviceId/binary, " where time > ", Time/binary>>,
            {ok, {{_, 200, _}, _, ResultTemp}} = iotdb_query(Config, QueryTemp),
            ?assertMatch(
                #{<<"values">> := [[36, 37, 38, 39, 34, 33, 32, 41]]},
                emqx_utils_json:decode(ResultTemp)
            )
    end,
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
    emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsSuccessCheck, iotdb_bridge_on_query
    ).

t_sync_device_id_missing(Config) ->
    emqx_bridge_v2_testlib:t_sync_query(
        Config,
        make_message_fun(iotdb_topic(Config), #{foo => bar}),
        is_error_check(device_id_missing),
        iotdb_bridge_on_query
    ).

t_extract_device_id_from_rule_engine_message(Config) ->
    BridgeType = ?config(bridge_type, Config),
    RuleTopic = <<"t/iotdb">>,
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "INT32", "12"),
    Message = emqx_message:make(RuleTopic, emqx_utils_json:encode(Payload)),
    ?check_trace(
        begin
            {ok, _} = emqx_bridge_v2_testlib:create_bridge(Config),
            SQL = <<
                "SELECT\n"
                "  payload.measurement, payload.data_type, payload.value, payload.device_id\n"
                "FROM\n"
                "  \"",
                RuleTopic/binary,
                "\""
            >>,
            Opts = #{sql => SQL},
            {ok, _} = emqx_bridge_v2_testlib:create_rule_and_action_http(
                BridgeType, RuleTopic, Config, Opts
            ),
            emqx:publish(Message),
            ?block_until(handle_async_reply, 5_000),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{action := ack, result := {ok, 200, _, _}}],
                ?of_kind(handle_async_reply, Trace)
            ),
            ok
        end
    ),
    ok.

t_sync_invalid_template(Config) ->
    emqx_bridge_v2_testlib:t_sync_query(
        Config,
        make_message_fun(iotdb_topic(Config), #{foo => bar, device_id => <<"root.sg27">>}),
        is_error_check(invalid_template),
        iotdb_bridge_on_query
    ).

t_async_device_id_missing(Config) ->
    emqx_bridge_v2_testlib:t_async_query(
        Config,
        make_message_fun(iotdb_topic(Config), #{foo => bar}),
        is_error_check(device_id_missing),
        iotdb_bridge_on_query_async
    ).

t_async_invalid_template(Config) ->
    emqx_bridge_v2_testlib:t_async_query(
        Config,
        make_message_fun(iotdb_topic(Config), #{foo => bar, device_id => <<"root.sg27">>}),
        is_error_check(invalid_template),
        iotdb_bridge_on_query_async
    ).

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(Config).

t_start_stop(Config) ->
    emqx_bridge_v2_testlib:t_start_stop(Config, iotdb_bridge_stopped).

t_on_get_status(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_device_id(Config) ->
    %% Create without device_id configured
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    ConfiguredDevice = <<"root.someOtherDevice234">>,
    DeviceId = <<"root.deviceFooBar123">>,
    Topic = <<"some/random/topic">>,
    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, ConfiguredDevice),
    Payload1 = make_iotdb_payload(DeviceId, "test", "BOOLEAN", true),
    MessageF1 = make_message_fun(Topic, Payload1),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

    {ok, {{_, 200, _}, _, Res1_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    ct:pal("device_id result: ~p", [emqx_utils_json:decode(Res1_1)]),
    #{<<"values">> := Values1_1} = emqx_utils_json:decode(Res1_1),
    ?assertNot(is_empty(Values1_1)),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, ConfiguredDevice),

    %% reconfigure bridge with device_id
    {ok, _} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{<<"device_id">> => ConfiguredDevice}
        }),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

    %% even though we had a device_id in the message it's not being used
    {ok, {{_, 200, _}, _, Res2_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    #{<<"values">> := Values2_1} = emqx_utils_json:decode(Res2_1),
    ?assert(is_empty(Values2_1)),
    {ok, {{_, 200, _}, _, Res2_2}} = iotdb_query(
        Config, <<"select * from ", ConfiguredDevice/binary>>
    ),
    #{<<"values">> := Values2_2} = emqx_utils_json:decode(Res2_2),
    ?assertNot(is_empty(Values2_2)),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, ConfiguredDevice),
    ok.

t_template(Config) ->
    %% Create without data  configured
    ?assertMatch({ok, _}, emqx_bridge_v2_testlib:create_bridge(Config)),
    ResourceId = emqx_bridge_v2_testlib:resource_id(Config),
    BridgeId = emqx_bridge_v2_testlib:bridge_id(Config),
    ?retry(
        _Sleep = 1_000,
        _Attempts = 20,
        ?assertEqual({ok, connected}, emqx_resource_manager:health_check(ResourceId))
    ),
    TemplateDeviceId = <<"root.deviceWithTemplate">>,
    DeviceId = <<"root.deviceWithoutTemplate">>,
    Topic = <<"some/random/topic">>,
    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TemplateDeviceId),
    Payload1 = make_iotdb_payload(DeviceId, "test", "BOOLEAN", true),
    MessageF1 = make_message_fun(Topic, Payload1),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

    {ok, {{_, 200, _}, _, Res1_1}} = iotdb_query(Config, <<"select * from ", DeviceId/binary>>),
    ?assertMatch(#{<<"values">> := [[true]]}, emqx_utils_json:decode(Res1_1)),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TemplateDeviceId),

    %% reconfigure with data template
    {ok, _} =
        emqx_bridge_v2_testlib:update_bridge_api(Config, #{
            <<"parameters">> => #{
                <<"device_id">> => TemplateDeviceId,
                <<"data">> => [
                    #{
                        <<"measurement">> => <<"${payload.measurement}">>,
                        <<"data_type">> => "TEXT",
                        <<"value">> => <<"${payload.device_id}">>
                    }
                ]
            }
        }),

    is_success_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

    {ok, {{_, 200, _}, _, Res2_2}} = iotdb_query(
        Config, <<"select * from ", TemplateDeviceId/binary>>
    ),

    ?assertMatch(#{<<"values">> := [[<<DeviceId/binary>>]]}, emqx_utils_json:decode(Res2_2)),

    iotdb_reset(Config, DeviceId),
    iotdb_reset(Config, TemplateDeviceId),
    ok.

t_sync_query_case(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "InT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    Query = <<"select temp from ", DeviceId/binary>>,
    {ok, {{_, 200, _}, _, IoTDBResult}} = iotdb_query(Config, Query),
    ?assertMatch(
        #{<<"values">> := [[36]]},
        emqx_utils_json:decode(IoTDBResult)
    ).

t_sync_query_invalid_type(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "IxT32", "36"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsInvalidType = fun(Result) -> ?assertMatch({error, #{reason := invalid_type}}, Result) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidType, iotdb_bridge_on_query
    ).

t_sync_query_unmatched_type(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "BOOLEAN", "not boolean"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsInvalidType = fun(Result) -> ?assertMatch({error, invalid_data}, Result) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidType, iotdb_bridge_on_query
    ).

is_empty(null) -> true;
is_empty([]) -> true;
is_empty([[]]) -> true;
is_empty(_) -> false.
