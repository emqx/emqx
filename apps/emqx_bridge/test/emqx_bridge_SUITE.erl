%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_http,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine
        ],
        #{work_dir => ?config(priv_dir, Config)}
    ),
    emqx_mgmt_api_test_util:init_suite(),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_mgmt_api_test_util:end_suite(),
    ok = emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(t_get_basic_usage_info_1, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    setup_fake_telemetry_data(),
    Config;
init_per_testcase(t_update_ssl_conf, Config) ->
    Path = [bridges, <<"mqtt">>, <<"ssl_update_test">>],
    [{config_path, Path} | Config];
init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(t_get_basic_usage_info_1, _Config) ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok = emqx_config:delete_override_conf_files(),
    ok = emqx_config:put([bridges], #{}),
    ok = emqx_config:put_raw([bridges], #{}),
    ok;
end_per_testcase(t_update_ssl_conf, Config) ->
    Path = proplists:get_value(config_path, Config),
    emqx:remove_config(Path);
end_per_testcase(_TestCase, _Config) ->
    ok.

t_get_basic_usage_info_0(_Config) ->
    ?assertEqual(
        #{
            num_bridges => 0,
            count_by_type => #{}
        },
        emqx_bridge:get_basic_usage_info()
    ).

t_get_basic_usage_info_1(_Config) ->
    BasicUsageInfo = emqx_bridge:get_basic_usage_info(),
    ?assertEqual(
        #{
            num_bridges => 3,
            count_by_type => #{
                http => 1,
                mqtt => 2
            }
        },
        BasicUsageInfo
    ).

setup_fake_telemetry_data() ->
    HTTPTCConfig1 = [
        {bridge_kind, action},
        {connector_type, <<"http">>},
        {connector_name, <<"basic_usage_info_webhook">>},
        {connector_config,
            emqx_bridge_schema_testlib:http_connector_config(#{
                <<"url">> => <<"http://localhost:9901/">>
            })},
        {action_type, <<"http">>},
        {action_name, <<"basic_usage_info_webhook">>},
        {action_config,
            emqx_bridge_schema_testlib:http_action_config(#{
                <<"connector">> => <<"basic_usage_info_webhook">>
            })}
    ],
    {201, _} = emqx_bridge_v2_testlib:create_connector_api2(HTTPTCConfig1, #{}),
    {201, _} = emqx_bridge_v2_testlib:create_action_api2(HTTPTCConfig1, #{}),
    HTTPTCConfig2 = [
        {connector_name, <<"basic_usage_info_webhook_disabled">>},
        {action_name, <<"basic_usage_info_webhook_disabled">>}
        | HTTPTCConfig1
    ],
    {201, _} = emqx_bridge_v2_testlib:create_connector_api2(HTTPTCConfig2, #{
        <<"enable">> => false
    }),
    {201, _} = emqx_bridge_v2_testlib:create_action_api2(HTTPTCConfig2, #{
        <<"enable">> => false
    }),
    MQTTTCConfig1 = [
        {bridge_kind, action},
        {connector_type, <<"mqtt">>},
        {connector_name, <<"basic_usage_info_mqtt">>},
        {connector_config, emqx_bridge_schema_testlib:mqtt_connector_config(#{})},
        {action_type, <<"mqtt">>},
        {action_name, <<"basic_usage_info_mqtt">>},
        {action_config,
            emqx_bridge_schema_testlib:mqtt_action_config(#{
                <<"connector">> => <<"basic_usage_info_mqtt">>
            })}
    ],
    {201, _} = emqx_bridge_v2_testlib:create_connector_api2(MQTTTCConfig1, #{}),
    {201, _} = emqx_bridge_v2_testlib:create_action_api2(MQTTTCConfig1, #{}),
    MQTTTCConfig2 = [
        {bridge_kind, source},
        {connector_type, <<"mqtt">>},
        {connector_name, <<"basic_usage_info_mqtt">>},
        {connector_config, emqx_bridge_schema_testlib:mqtt_connector_config(#{})},
        {source_type, <<"mqtt">>},
        {source_name, <<"basic_usage_info_mqtt_from_select">>},
        {source_config,
            emqx_bridge_schema_testlib:mqtt_source_config(#{
                <<"connector">> => <<"basic_usage_info_mqtt">>
            })}
    ],
    {201, _} = emqx_bridge_v2_testlib:create_source_api(MQTTTCConfig2, #{}),
    ok.

t_create_with_bad_name(_Config) ->
    Path = [bridges, mqtt, 'test_哈哈'],
    Conf = #{
        <<"bridge_mode">> => false,
        <<"clean_start">> => true,
        <<"keepalive">> => <<"60s">>,
        <<"proto_ver">> => <<"v4">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"ssl">> =>
            #{
                %% needed to trigger pre_config_update
                <<"certfile">> => cert_file("certfile"),
                <<"enable">> => true
            }
    },
    ?assertMatch(
        {error,
            {pre_config_update, emqx_bridge_app, #{
                reason := <<"Invalid name format.", _/binary>>,
                kind := validation_error
            }}},
        emqx:update_config(Path, Conf)
    ),
    ok.

t_create_with_bad_name_root(_Config) ->
    BadBridgeName = <<"test_哈哈"/utf8>>,
    BridgeConf = #{
        <<"bridge_mode">> => false,
        <<"clean_start">> => true,
        <<"keepalive">> => <<"60s">>,
        <<"proto_ver">> => <<"v4">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"ssl">> =>
            #{
                %% needed to trigger pre_config_update
                <<"certfile">> => cert_file("certfile"),
                <<"enable">> => true
            }
    },
    Conf = #{<<"mqtt">> => #{BadBridgeName => BridgeConf}},
    Path = [bridges],
    ?assertMatch(
        {error,
            {pre_config_update, _ConfigHandlerMod, #{
                kind := validation_error,
                reason := bad_bridge_names,
                bad_bridges := [#{type := <<"mqtt">>, name := BadBridgeName}]
            }}},
        emqx:update_config(Path, Conf)
    ),
    ok.

data_file(Name) ->
    Dir = code:lib_dir(emqx_bridge),
    {ok, Bin} = file:read_file(filename:join([Dir, "test", "data", Name])),
    Bin.

cert_file(Name) ->
    data_file(filename:join(["certs", Name])).
