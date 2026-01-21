%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_iotdb_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include("emqx_bridge_iotdb.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, iotdb).
-define(CONNECTOR_TYPE_BIN, <<"iotdb">>).
-define(ACTION_TYPE, iotdb).
-define(ACTION_TYPE_BIN, <<"iotdb">>).

-define(PROXY_NAME_IOTDB130, "iotdb130").
-define(PROXY_NAME_THRIFT, "iotdb_thrift").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(iotdb130, iotdb130).
-define(thrift, thrift).

-define(rest_prefix, <<"/rest/v2">>).

-define(device_id(), <<"root.", (atom_to_binary(?FUNCTION_NAME))/binary>>).

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
            emqx_bridge_iotdb,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?iotdb130, TCConfig) ->
    [
        {driver, ?iotdb130},
        {proxy_name, ?PROXY_NAME_IOTDB130},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {server_url, <<"http://toxiproxy:28080">>}
        | TCConfig
    ];
init_per_group(?thrift, TCConfig) ->
    [
        {driver, ?thrift},
        {proxy_name, ?PROXY_NAME_THRIFT},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {server_url, <<"http://toxiproxy:48080">>}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnSpecificOpts =
        case get_config(driver, TCConfig, ?iotdb130) of
            ?iotdb130 ->
                connector_config_iotdb130();
            ?thrift ->
                connector_config_thrift()
        end,
    ConnectorConfig = connector_config(ConnSpecificOpts),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
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
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

connector_config_thrift() ->
    #{
        <<"driver">> => <<"thrift">>,
        <<"server">> => <<"toxiproxy:46667">>,
        <<"protocol_version">> => <<"protocol_v3">>,
        <<"username">> => <<"root">>,
        <<"password">> => <<"root">>,
        <<"zoneId">> => <<"Asia/Shanghai">>
    }.

connector_config_iotdb130() ->
    #{
        <<"base_url">> => <<"http://toxiproxy:28080">>,
        <<"iotdb_version">> => bin(?VSN_1_3_X),
        <<"authentication">> => #{
            <<"username">> => <<"root">>,
            <<"password">> => <<"root">>
        },
        <<"max_inactive">> => <<"10s">>
    }.

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"data">> => [default_data_template()]
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

default_data_template() ->
    #{
        <<"measurement">> => <<"${payload.measurement}">>,
        <<"data_type">> => <<"int32">>,
        <<"value">> => <<"${payload.value}">>
    }.

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bin(X) -> emqx_utils_conv:bin(X).
str(X) -> emqx_utils_conv:str(X).
fmt(Fmt, Context) -> emqx_bridge_v2_testlib:fmt(Fmt, Context).

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

%% with_failure(FailureType, Fn) ->
%%     emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

iotdb_request(TCConfig, Path, Body) ->
    iotdb_request(TCConfig, Path, Body, #{}).

iotdb_request(TCConfig, Path, Body, _Opts) ->
    Username = <<"root">>,
    Password = <<"root">>,
    BaseURL = get_config(server_url, TCConfig, <<"http://toxiproxy:28080">>),
    URL0 = <<BaseURL/binary, Path/binary>>,
    URL = str(URL0),
    BasicToken = base64:encode(<<Username/binary, ":", Password/binary>>),
    AuthHeader = {"Authorization", binary_to_list(BasicToken)},
    emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => URL,
        body => Body,
        auth_header => AuthHeader
    }).

iotdb_reset(DeviceId, TCConfig) ->
    Path = iolist_to_binary([?rest_prefix, "/nonQuery"]),
    Body = #{sql => <<"delete from ", DeviceId/binary, ".*">>},
    {200, #{<<"code">> := 200}} =
        iotdb_request(TCConfig, Path, Body),
    ?retry(200, 10, ?assertMatch({200, #{<<"values">> := []}}, scan_table(DeviceId, TCConfig))),
    ok.

scan_table(DeviceId, TCConfig) ->
    iotdb_query(<<"select * from ${did}">>, #{did => DeviceId}, TCConfig).

iotdb_query(Query, TCConfig) ->
    iotdb_query(Query, _FmtContext = #{}, TCConfig).

iotdb_query(Query0, FmtContext, TCConfig) ->
    Path = iolist_to_binary([?rest_prefix, "/query"]),
    Opts = #{return_all => true},
    Query = fmt(Query0, FmtContext),
    Body = #{sql => Query},
    iotdb_request(TCConfig, Path, Body, Opts).

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

is_success_check({ok, 200, _, Body}) ->
    ?assert(is_code(200, emqx_utils_json:decode(Body)));
is_success_check({ok, _}) ->
    ok;
is_success_check(Other) ->
    throw(Other).

is_code(Code, #{<<"code">> := Code}) -> true;
is_code(_, _) -> false.

make_iotdb_payload(DeviceId, Measurement, Type, Value) ->
    #{
        measurement => bin(Measurement),
        data_type => bin(Type),
        value => bin(Value),
        device_id => DeviceId,
        is_aligned => true
    }.

make_iotdb_payload(DeviceId, Measurement, Type, Value, Timestamp) ->
    Payload = make_iotdb_payload(DeviceId, Measurement, Type, Value),
    Payload#{timestamp => Timestamp}.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

json_encode(X) ->
    emqx_utils_json:encode(X).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?iotdb130], [?thrift]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, iotdb_bridge_stopped).

t_probe_connector_api() ->
    [{matrix, true}].
t_probe_connector_api(matrix) ->
    [[?iotdb130], [?thrift]];
t_probe_connector_api(TCConfig) when is_list(TCConfig) ->
    ?assertMatch(
        {ok, _},
        emqx_bridge_v2_testlib:probe_connector_api(TCConfig)
    ).

t_on_get_status() ->
    [{matrix, true}].
t_on_get_status(matrix) ->
    [[?iotdb130], [?thrift]];
t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?iotdb130], [?thrift]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    DeviceId = ?device_id(),
    PayloadFn = fun() ->
        json_encode(#{
            <<"measurement">> => <<"temp">>,
            <<"data_type">> => <<"int32">>,
            <<"value">> => <<"36">>,
            <<"device_id">> => DeviceId,
            <<"is_aligned">> => true
        })
    end,
    PrePublishFn = fun(Context) ->
        iotdb_reset(DeviceId, TCConfig),
        Context
    end,
    PostPublishFn = fun(_Context) ->
        ?retry(
            200,
            20,
            ?assertMatch(
                {200, #{<<"values">> := [_]}},
                scan_table(DeviceId, TCConfig)
            )
        ),
        ok
    end,
    Opts = #{
        payload_fn => PayloadFn,
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_async_device_id_missing() ->
    [{matrix, true}].
t_async_device_id_missing(matrix) ->
    [[?iotdb130]];
t_async_device_id_missing(TCConfig) ->
    DeviceId = ?device_id(),
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    emqx_bridge_v2_testlib:t_async_query(
        TCConfig,
        make_message_fun(DeviceId, #{foo => bar}),
        IsInvalidData,
        iotdb_bridge_on_query_async
    ).

t_async_query() ->
    [{matrix, true}].
t_async_query(matrix) ->
    [[?iotdb130]];
t_async_query(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    ok = emqx_bridge_v2_testlib:t_async_query(
        TCConfig, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query_async
    ),
    Query = <<"select temp from ", DeviceId/binary>>,
    ?assertMatch(
        {200, #{<<"values">> := [[36]]}},
        iotdb_query(Query, TCConfig)
    ).

t_extract_device_id_from_rule_engine_message() ->
    [{matrix, true}].
t_extract_device_id_from_rule_engine_message(matrix) ->
    [[?iotdb130]];
t_extract_device_id_from_rule_engine_message(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    Payload = json_encode(make_iotdb_payload(DeviceId, "temp", "int32", "12")),
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{}),
            SQL = <<
                "SELECT"
                " payload.measurement, payload.data_type, payload.value, payload.device_id "
                " FROM "
                " \"${t}\""
            >>,
            #{topic := Topic} = simple_create_rule_api(SQL, TCConfig),
            C = start_client(),
            emqtt:publish(C, Topic, Payload, [{qos, 1}]),
            ?retry(
                200,
                10,
                ?assertMatch(
                    {200, #{<<"values">> := [[12]]}},
                    scan_table(DeviceId, TCConfig)
                )
            ),
            ok
        end,
        []
    ),
    ok.

t_sync_device_id_missing() ->
    [{matrix, true}].
t_sync_device_id_missing(matrix) ->
    [[?iotdb130], [?thrift]];
t_sync_device_id_missing(TCConfig) ->
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    emqx_bridge_v2_testlib:t_sync_query(
        TCConfig,
        make_message_fun(?device_id(), #{foo => bar}),
        IsInvalidData,
        iotdb_bridge_on_query
    ).

t_sync_query_fail() ->
    [{matrix, true}].
t_sync_query_fail(matrix) ->
    [[?iotdb130], [?thrift]];
t_sync_query_fail(TCConfig) ->
    DeviceId = ?device_id(),
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "Anton"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsSuccessCheck =
        fun(Result) ->
            ?assertEqual(error, element(1, Result))
        end,
    emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsSuccessCheck, iotdb_bridge_on_query
    ).

t_device_id() ->
    [{matrix, true}].
t_device_id(matrix) ->
    [[?iotdb130], [?thrift]];
t_device_id(TCConfig) ->
    %% Create without device_id configured
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ConfiguredDevice = <<"root.someOtherDevice234">>,
    DeviceId = <<"root.deviceFooBar123">>,
    iotdb_reset(DeviceId, TCConfig),
    iotdb_reset(ConfiguredDevice, TCConfig),
    Payload1 = json_encode(make_iotdb_payload(DeviceId, "test", "int32", "33")),
    emqtt:publish(C, Topic, Payload1, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"values">> := [_ | _]}},
            scan_table(DeviceId, TCConfig)
        )
    ),
    iotdb_reset(DeviceId, TCConfig),
    iotdb_reset(ConfiguredDevice, TCConfig),
    %% reconfigure bridge with device_id
    {200, _} = update_action_api(TCConfig, #{
        <<"parameters">> => #{<<"device_id">> => ConfiguredDevice}
    }),
    emqtt:publish(C, Topic, Payload1, [{qos, 1}]),
    %% even though we had a device_id in the message it's not being used
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"values">> := []}},
            scan_table(DeviceId, TCConfig)
        )
    ),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"values">> := [[33]]}},
            scan_table(ConfiguredDevice, TCConfig)
        )
    ),
    iotdb_reset(DeviceId, TCConfig),
    iotdb_reset(ConfiguredDevice, TCConfig),
    ok.

t_template(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    %% Create without data configured
    ?assertMatch(
        {400, #{<<"message">> := #{<<"reason">> := <<"empty_array_not_allowed">>}}},
        create_action_api(TCConfig, #{<<"parameters">> => #{<<"data">> => []}})
    ),
    TemplateDeviceId = <<"root.deviceWithTemplate">>,
    DeviceId = <<"root.deviceWithoutTemplate">>,
    iotdb_reset(DeviceId, TCConfig),
    iotdb_reset(TemplateDeviceId, TCConfig),
    %% create with data template
    {201, _} =
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"device_id">> => TemplateDeviceId,
                <<"data">> => [
                    #{
                        <<"measurement">> => <<"${payload.measurement}">>,
                        <<"data_type">> => <<"text">>,
                        <<"value">> => <<"${payload.device_id}">>
                    }
                ]
            }
        }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    Payload = json_encode(make_iotdb_payload(DeviceId, "test", "boolean", true)),
    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    ?retry(
        200,
        10,
        ?assertMatch(
            {200, #{<<"values">> := [[DeviceId]]}},
            scan_table(TemplateDeviceId, TCConfig)
        )
    ),
    iotdb_reset(DeviceId, TCConfig),
    iotdb_reset(TemplateDeviceId, TCConfig),
    ok.

t_sync_query_unmatched_type(TCConfig) ->
    DeviceId = ?device_id(),
    Payload = make_iotdb_payload(DeviceId, "temp", "boolean", "not boolean"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ),
    ok.

t_thrift_auto_recon() ->
    [{matrix, true}].
t_thrift_auto_recon(matrix) ->
    [[?thrift]];
t_thrift_auto_recon(TCConfig) ->
    ConnectorOverrides = #{<<"server">> => <<"127.0.0.1:9999,toxiproxy:46667">>},
    Opts = #{connector_overrides => ConnectorOverrides},
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, Opts).

t_thrift_protocol_version_1_or_2_is_deprecated() ->
    [{matrix, true}].
t_thrift_protocol_version_1_or_2_is_deprecated(matrix) ->
    [[?thrift]];
t_thrift_protocol_version_1_or_2_is_deprecated(TCConfig) ->
    ConnectorConfig = ?config(connector_config, TCConfig),
    BadConnectorConfig = ConnectorConfig#{
        <<"protocol_version">> => atom_to_binary(?PROTOCOL_V1)
    },
    TCConfig1 = lists:keyreplace(
        connector_config, 1, TCConfig, {connector_config, BadConnectorConfig}
    ),
    {error, {_StatusCode, _Headers, Body}} = emqx_bridge_v2_testlib:create_connector_api(TCConfig1),
    ?assertMatch(
        #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := #{<<"reason">> := <<"Thrift protocol version 1 or 2 is deprecated">>}
        },
        Body
    ),
    ok.

t_sync_query_with_lowercase(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query
    ),
    ?assertMatch(
        {200, #{<<"values">> := [[36]]}},
        scan_table(DeviceId, TCConfig)
    ),
    ok.

t_sync_query_plain_text(TCConfig) ->
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload = <<"this is a text">>,
    MakeMessageFun = make_message_fun(?device_id(), Payload),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ).

t_sync_query_invalid_json(TCConfig) ->
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload2 = <<"{\"msg\":}">>,
    MakeMessageFun = make_message_fun(?device_id(), Payload2),
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query
    ).

t_sync_query_invalid_timestamp(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36", <<"this is a string">>),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"timestamp">> => <<"${payload.timestamp}">>,
                    <<"measurement">> => <<"${payload.measurement}">>,
                    <<"data_type">> => <<"int32">>,
                    <<"value">> => <<"${payload.value}">>
                }
            ]
        }
    },
    Opts = #{action_overrides => ActionOverrides},
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query, Opts
    ).

t_sync_query_missing_timestamp(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    IsInvalidData = fun(Result) -> ?assertMatch({error, {invalid_data, _}}, Result) end,
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36"),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"timestamp">> => <<"${payload.timestamp}">>,
                    <<"measurement">> => <<"${payload.measurement}">>,
                    <<"data_type">> => <<"int32">>,
                    <<"value">> => <<"${payload.value}">>
                }
            ]
        }
    },
    Opts = #{action_overrides => ActionOverrides},
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, IsInvalidData, iotdb_bridge_on_query, Opts
    ).

t_sync_query_templated_timestamp(TCConfig) ->
    Ts = erlang:system_time(millisecond) - rand:uniform(864000),
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    Payload = make_iotdb_payload(DeviceId, "temp", "int32", "36", Ts),
    MakeMessageFun = make_message_fun(DeviceId, Payload),
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"data">> => [
                #{
                    <<"timestamp">> => <<"${payload.timestamp}">>,
                    <<"measurement">> => <<"${payload.measurement}">>,
                    <<"data_type">> => <<"int32">>,
                    <<"value">> => <<"${payload.value}">>
                }
            ]
        }
    },
    Opts = #{action_overrides => ActionOverrides},
    ok = emqx_bridge_v2_testlib:t_sync_query(
        TCConfig, MakeMessageFun, fun is_success_check/1, iotdb_bridge_on_query, Opts
    ),
    ?assertMatch(
        {200, #{<<"timestamps">> := [Ts]}},
        scan_table(DeviceId, TCConfig)
    ).

t_rule_test_trace() ->
    [{matrix, true}].
t_rule_test_trace(matrix) ->
    [[?iotdb130], [?thrift]];
t_rule_test_trace(TCConfig) ->
    DeviceId = ?device_id(),
    iotdb_reset(DeviceId, TCConfig),
    PayloadFn = fun() ->
        Msg = make_iotdb_payload(DeviceId, "temp", "int32", "36"),
        emqx_utils_json:encode(Msg)
    end,
    Opts = #{payload_fn => PayloadFn},
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, Opts).

t_bad_password() ->
    [{matrix, true}].
t_bad_password(matrix) ->
    [[?iotdb130], [?thrift]];
t_bad_password(TCConfig) ->
    BadAuth = #{<<"password">> => <<"wrong password">>},
    Driver = get_config(driver, TCConfig),
    Overrides =
        case Driver of
            ?thrift -> BadAuth;
            ?iotdb130 -> #{<<"authentication">> => BadAuth}
        end,
    {201, #{
        <<"status">> := <<"disconnected">>,
        <<"status_reason">> := Msg
    }} = create_connector_api(TCConfig, Overrides),
    case Driver of
        ?thrift ->
            ?assertMatch(match, re:run(Msg, <<"Authentication failed">>, [{capture, none}]));
        ?iotdb130 ->
            ?assertMatch(match, re:run(Msg, <<"WRONG_LOGIN_PASSWORD">>, [{capture, none}]))
    end,
    ok.
