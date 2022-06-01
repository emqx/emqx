%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    _ = application:load(emqx_conf),
    %% to avoid inter-suite dependencies
    application:stop(emqx_connector),
    ok = emqx_common_test_helpers:start_apps([emqx, emqx_bridge]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx,
        emqx_bridge,
        emqx_resource,
        emqx_connector
    ]).

init_per_testcase(t_get_basic_usage_info_1, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    setup_fake_telemetry_data(),
    Config;
init_per_testcase(_TestCase, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(t_get_basic_usage_info_1, _Config) ->
    lists:foreach(
        fun({BridgeType, BridgeName}) ->
            {ok, _} = emqx_bridge:remove(BridgeType, BridgeName)
        end,
        [
            {webhook, <<"basic_usage_info_webhook">>},
            {webhook, <<"basic_usage_info_webhook_disabled">>},
            {mqtt, <<"basic_usage_info_mqtt">>}
        ]
    ),
    ok = emqx_config:delete_override_conf_files(),
    ok = emqx_config:put([bridges], #{}),
    ok = emqx_config:put_raw([bridges], #{}),
    ok;
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
                webhook => 1,
                mqtt => 2
            }
        },
        BasicUsageInfo
    ).

setup_fake_telemetry_data() ->
    ConnectorConf =
        #{
            <<"connectors">> =>
                #{
                    <<"mqtt">> => #{
                        <<"my_mqtt_connector">> =>
                            #{server => "127.0.0.1:1883"},
                        <<"my_mqtt_connector2">> =>
                            #{server => "127.0.0.1:1884"}
                    }
                }
        },
    MQTTConfig1 = #{
        connector => <<"mqtt:my_mqtt_connector">>,
        enable => true,
        direction => ingress,
        remote_topic => <<"aws/#">>,
        remote_qos => 1
    },
    MQTTConfig2 = #{
        connector => <<"mqtt:my_mqtt_connector2">>,
        enable => true,
        direction => ingress,
        remote_topic => <<"$bridges/mqtt:some_bridge_in">>,
        remote_qos => 1
    },
    HTTPConfig = #{
        url => <<"http://localhost:9901/messages/${topic}">>,
        enable => true,
        direction => egress,
        local_topic => "emqx_webhook/#",
        method => post,
        body => <<"${payload}">>,
        headers => #{},
        request_timeout => "15s"
    },
    Conf =
        #{
            <<"bridges">> =>
                #{
                    <<"webhook">> =>
                        #{
                            <<"basic_usage_info_webhook">> => HTTPConfig,
                            <<"basic_usage_info_webhook_disabled">> =>
                                HTTPConfig#{enable => false}
                        },
                    <<"mqtt">> =>
                        #{
                            <<"basic_usage_info_mqtt">> => MQTTConfig1,
                            <<"basic_usage_info_mqtt_from_select">> => MQTTConfig2
                        }
                }
        },
    Opts = #{raw_with_default => true},
    ok = emqx_common_test_helpers:load_config(emqx_connector_schema, ConnectorConf, Opts),
    ok = emqx_common_test_helpers:load_config(emqx_bridge_schema, Conf, Opts),

    ok = snabbkaffe:start_trace(),
    Predicate = fun(#{?snk_kind := K}) -> K =:= emqx_bridge_loaded end,
    NEvents = 3,
    BackInTime = 0,
    Timeout = 11_000,
    {ok, Sub} = snabbkaffe_collector:subscribe(Predicate, NEvents, Timeout, BackInTime),
    ok = emqx_bridge:load(),
    {ok, _} = snabbkaffe_collector:receive_events(Sub),
    ok = snabbkaffe:stop(),
    ok.
