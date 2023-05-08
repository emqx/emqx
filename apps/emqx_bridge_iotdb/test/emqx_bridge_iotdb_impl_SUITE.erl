%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_impl_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    reset_service(Config),
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_bridge_testlib:end_per_testcase(TestCase, Config).

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

bridge_config(TestCase, _TestGroup, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Host = ?config(bridge_host, Config),
    Port = ?config(bridge_port, Config),
    Name = <<
        (atom_to_binary(TestCase))/binary, UniqueNum/binary
    >>,
    ServerURL = iolist_to_binary([
        "http://",
        Host,
        ":",
        integer_to_binary(Port)
    ]),
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

reset_service(Config) ->
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
    Path = <<BaseURL/binary, "/rest/v2/nonQuery">>,
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    Headers = [
        {"Content-type", "application/json"},
        {"Authorization", binary_to_list(BasicToken)}
    ],
    Device = iotdb_device(Config),
    Body = #{sql => <<"delete from ", Device/binary, ".*">>},
    {ok, _} = emqx_mgmt_api_test_util:request_api(post, Path, "", Headers, Body, #{}).

make_iotdb_payload(DeviceId) ->
    make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "36").

make_iotdb_payload(DeviceId, Measurement, Type, Value) ->
    #{
        measurement => Measurement,
        data_type => Type,
        value => Value,
        device_id => DeviceId,
        is_aligned => false
    }.

make_message_fun(Topic, Payload) ->
    fun() ->
        MsgId = erlang:unique_integer([positive]),
        #{
            topic => Topic,
            id => MsgId,
            payload => Payload,
            retain => true
        }
    end.

iotdb_device(Config) ->
    MQTTTopic = ?config(mqtt_topic, Config),
    Device = re:replace(MQTTTopic, "/", ".dev", [global, {return, binary}]),
    <<"root.", Device/binary>>.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_sync_query_simple(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "36"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(ok, element(1, Result))
        end,
    emqx_bridge_testlib:t_sync_query(Config, MakeMessageFun, IsSuccessCheck).

t_async_query(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "36"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(ok, element(1, Result))
        end,
    emqx_bridge_testlib:t_async_query(Config, MakeMessageFun, IsSuccessCheck).

t_sync_query_aggregated(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = [
        make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "36"),
        (make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "37"))#{timestamp => <<"mow_us">>},
        (make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "38"))#{timestamp => <<"mow_ns">>},
        make_iotdb_payload(DeviceId, "charged", <<"BOOLEAN">>, "1"),
        make_iotdb_payload(DeviceId, "stoked", <<"BOOLEAN">>, "true"),
        make_iotdb_payload(DeviceId, "enriched", <<"BOOLEAN">>, <<"TRUE">>),
        make_iotdb_payload(DeviceId, "drained", <<"BOOLEAN">>, "0"),
        make_iotdb_payload(DeviceId, "dazzled", <<"BOOLEAN">>, "false"),
        make_iotdb_payload(DeviceId, "unplugged", <<"BOOLEAN">>, <<"FALSE">>),
        make_iotdb_payload(DeviceId, "weight", <<"FLOAT">>, "87.3"),
        make_iotdb_payload(DeviceId, "foo", <<"TEXT">>, <<"bar">>)
    ],
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(ok, element(1, Result))
        end,
    emqx_bridge_testlib:t_sync_query(Config, MakeMessageFun, IsSuccessCheck).

t_sync_query_fail(Config) ->
    DeviceId = iotdb_device(Config),
    Payload = make_iotdb_payload(DeviceId, "temp", <<"INT32">>, "Anton"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(error, element(1, Result))
        end,
    emqx_bridge_testlib:t_sync_query(Config, MakeMessageFun, IsSuccessCheck).

t_create_via_http(Config) ->
    emqx_bridge_testlib:t_create_via_http(Config).

t_start_stop(Config) ->
    emqx_bridge_testlib:t_start_stop(Config, iotdb_bridge_stopped).

t_on_get_status(Config) ->
    emqx_bridge_testlib:t_on_get_status(Config).
