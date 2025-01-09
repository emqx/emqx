%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
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
    lists:foreach(
        fun({BridgeType, BridgeName}) ->
            ok = emqx_bridge:remove(BridgeType, BridgeName)
        end,
        [
            %% Keep using the old bridge names to avoid breaking the tests
            {webhook, <<"basic_usage_info_webhook">>},
            {webhook, <<"basic_usage_info_webhook_disabled">>},
            {mqtt, <<"basic_usage_info_mqtt">>}
        ]
    ),
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
    MQTTConfig1 = #{
        server => "127.0.0.1:1883",
        enable => true,
        ingress => #{
            remote => #{
                topic => <<"aws/#">>,
                qos => 1
            }
        }
    },
    MQTTConfig2 = #{
        server => "127.0.0.1:1884",
        enable => true,
        ingress => #{
            remote => #{
                topic => <<"$bridges/mqtt:some_bridge_in">>,
                qos => 1
            }
        }
    },
    HTTPConfig = #{
        url => <<"http://localhost:9901/messages/${topic}">>,
        enable => true,
        local_topic => "emqx_http/#",
        method => post,
        body => <<"${payload}">>,
        headers => #{},
        request_timeout => "15s"
    },
    %% Keep use the old bridge names to test the backward compatibility
    {ok, _} = emqx_bridge_testlib:create_bridge_api(
        <<"webhook">>,
        <<"basic_usage_info_webhook">>,
        HTTPConfig
    ),
    {ok, _} = emqx_bridge_testlib:create_bridge_api(
        <<"webhook">>,
        <<"basic_usage_info_webhook_disabled">>,
        HTTPConfig#{enable => false}
    ),
    {ok, _} = emqx_bridge_testlib:create_bridge_api(
        <<"mqtt">>,
        <<"basic_usage_info_mqtt">>,
        MQTTConfig1
    ),
    {ok, _} = emqx_bridge_testlib:create_bridge_api(
        <<"mqtt">>,
        <<"basic_usage_info_mqtt_from_select">>,
        MQTTConfig2
    ),
    ok.

t_update_ssl_conf(Config) ->
    [_Root, Type, Name] = proplists:get_value(config_path, Config),
    CertDir = filename:join([emqx:mutable_certs_dir(), connectors, Type, Name]),
    EnableSSLConf = #{
        <<"bridge_mode">> => false,
        <<"clean_start">> => true,
        <<"keepalive">> => <<"60s">>,
        <<"proto_ver">> => <<"v4">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"egress">> => #{
            <<"local">> => #{<<"topic">> => <<"t">>},
            <<"remote">> => #{<<"topic">> => <<"remote/t">>}
        },
        <<"ssl">> =>
            #{
                <<"cacertfile">> => cert_file("cafile"),
                <<"certfile">> => cert_file("certfile"),
                <<"enable">> => true,
                <<"keyfile">> => cert_file("keyfile"),
                <<"verify">> => <<"verify_peer">>
            }
    },
    CreateCfg = [
        {bridge_name, Name},
        {bridge_type, Type},
        {bridge_config, #{}}
    ],
    {ok, _} = emqx_bridge_testlib:create_bridge_api(CreateCfg, EnableSSLConf),
    ?assertMatch({ok, [_, _, _]}, file:list_dir(CertDir)),
    NoSSLConf = EnableSSLConf#{<<"ssl">> := #{<<"enable">> => false}},
    {ok, _} = emqx_bridge_testlib:update_bridge_api(CreateCfg, NoSSLConf),
    {ok, _} = emqx_tls_certfile_gc:force(),
    ?assertMatch({error, enoent}, file:list_dir(CertDir)),
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
