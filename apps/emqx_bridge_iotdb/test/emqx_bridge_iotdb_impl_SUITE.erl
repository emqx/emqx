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

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, iotdb130},
        {group, thrift}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE) -- [t_thrift_auto_recon],
    Async = [
        t_async_device_id_missing,
        t_async_query,
        t_extract_device_id_from_rule_engine_message
    ],
    [
        {iotdb130, AllTCs},
        {thrift, (AllTCs -- Async) ++ [t_thrift_auto_recon]}
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

init_per_group(iotdb130 = Type, Config0) ->
    Host = os:getenv("IOTDB_PLAIN_HOST", "toxiproxy.emqx.net"),
    ProxyName = atom_to_list(Type),
    IotDbVersion = ?VSN_1_3_X,
    DefaultPort = "28080",
    Port = list_to_integer(os:getenv("IOTDB_PLAIN_PORT", DefaultPort)),
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {rest_port, Port},
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
init_per_group(thrift = Type, Config0) ->
    Host = os:getenv("IOTDB_THRIFT_HOST", "toxiproxy.emqx.net"),
    Port = list_to_integer(os:getenv("IOTDB_THRIFT_PORT", "46667")),
    ProxyName = "iotdb_thrift",
    case emqx_common_test_helpers:is_tcp_server_available(Host, Port) of
        true ->
            Config = emqx_bridge_v2_testlib:init_per_group(Type, ?BRIDGE_TYPE_BIN, Config0),
            [
                {bridge_host, Host},
                {bridge_port, Port},
                {rest_port, 48080},
                {proxy_name, ProxyName},
                {iotdb_version, ?PROTOCOL_V3},
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
init_per_group(_Group, Config) ->
    Config.

end_per_group(iotdb130, Config) ->
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
    {_ConfigString, ConnectorConfig} = connector_config(TestCase, Name, Config0),

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
    QueryMode = query_mode(TestCase),
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
            "     query_mode = \"~s\"\n"
            "     worker_pool_size = 1\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                ServerURL,
                Version,
                QueryMode
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

action_config(TestCase, Name, Config) ->
    Type = ?config(bridge_type, Config),
    QueryMode = query_mode(TestCase),
    DataTemplate =
        case TestCase of
            t_template ->
                <<"">>;
            _ ->
                emqx_utils_json:encode(
                    #{
                        <<"measurement">> => <<"${payload.measurement}">>,
                        <<"data_type">> => test_case_data_type(TestCase),
                        <<"value">> => <<"${payload.value}">>
                    }
                )
        end,
    ConfigString =
        io_lib:format(
            "actions.~s.~s {\n"
            "  enable = true\n"
            "  connector = \"~s\"\n"
            "  parameters = {\n"
            "     data = [~s]\n"
            "  }\n"
            "  resource_opts = {\n"
            "     query_mode = \"~s\"\n"
            "  }\n"
            "}\n",
            [
                Type,
                Name,
                Name,
                DataTemplate,
                QueryMode
            ]
        ),
    ct:pal("ActionConfig:~ts~n", [ConfigString]),
    {ConfigString, parse_action_and_check(ConfigString, Type, Name)}.

connector_config(TestCase, Name, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Type = ?config(bridge_type, Config),
    Version = ?config(iotdb_version, Config),
    ServerURL = iotdb_server_url(Host, Port),
    ConfigString =
        case ?config(test_group, Config) of
            thrift ->
                Server = make_thrift_server(TestCase, Config),
                io_lib:format(
                    "connectors.~s.~s {\n"
                    "  enable = true\n"
                    "  driver = \"thrift\"\n"
                    "  server = \"~s\"\n"
                    "  protocol_version = \"~p\"\n"
                    "  username = \"root\"\n"
                    "  password = \"root\"\n"
                    "  zoneId = \"Asia/Shanghai\"\n"
                    "  ssl.enable = false\n"
                    "}\n",
                    [
                        Type,
                        Name,
                        Server,
                        Version
                    ]
                );
            _ ->
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
                )
        end,
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

t_async_device_id_missing(Config) ->
    emqx_bridge_v2_testlib:t_async_query(
        Config,
        make_message_fun(iotdb_topic(Config), #{foo => bar}),
        is_error_check(device_id_missing),
        iotdb_bridge_on_query_async
    ).

t_create_via_http(Config) ->
    emqx_bridge_v2_testlib:t_create_via_http(
        Config,
        thrift =:= ?config(test_group, Config)
    ).

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

    is_error_check(
        emqx_resource:simple_sync_query(ResourceId, {BridgeId, MessageF1()})
    ),

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

t_sync_query_unmatched_type(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "BOOLEAN", "not boolean"),
    MakeMessageFun = make_message_fun(iotdb_topic(Config), Payload),
    IsInvalidType = fun(Result) -> ?assertMatch({error, invalid_data}, Result) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        Config, MakeMessageFun, IsInvalidType, iotdb_bridge_on_query
    ).

t_thrift_auto_recon(Config) ->
    emqx_bridge_v2_testlib:t_on_get_status(Config).

t_sync_query_with_lowercase(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36"),
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

is_empty(null) -> true;
is_empty([]) -> true;
is_empty([[]]) -> true;
is_empty(_) -> false.

make_thrift_server(t_thrift_auto_recon, Config) ->
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    lists:flatten(io_lib:format("127.0.0.1:9999,~s:~p", [Host, Port]));
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

exp(Dev, M0) ->
    M = s_to_b(M0),
    <<Dev/binary, ".", M/binary>>.

test_case_data_type(t_device_id) ->
    <<"BOOLEAN">>;
test_case_data_type(t_sync_query_unmatched_type) ->
    <<"BOOLEAN">>;
test_case_data_type(t_sync_query_with_lowercase) ->
    <<"int32">>;
test_case_data_type(_) ->
    <<"INT32">>.
