%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_bridge_mqtt_v2_subscriber_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, Api} = emqx_common_test_http:create_default_app(),
    [
        {apps, Apps},
        {api, Api}
        | Config
    ].

end_per_suite(Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    ConnectorConfig = connector_config(),
    SourceConfig = source_config(#{connector => Name}),
    [
        {bridge_kind, source},
        {source_type, mqtt},
        {source_name, Name},
        {source_config, SourceConfig},
        {connector_type, mqtt},
        {connector_name, Name},
        {connector_config, ConnectorConfig}
        | Config
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config() ->
    %% !!!!!!!!!!!! FIXME!!!!!! add more fields ("server_configs")
    #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    }.

source_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"remote">> =>
                        #{
                            <<"topic">> => <<"remote/topic">>,
                            <<"qos">> => 2
                        },
                    <<"local">> =>
                        #{
                            <<"topic">> => <<"local/topic">>,
                            <<"qos">> => 2,
                            <<"retain">> => false,
                            <<"payload">> => <<"${payload}">>
                        }
                },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"buffer_mode">> => <<"memory_only">>,
                <<"buffer_seg_bytes">> => <<"10MB">>,
                <<"health_check_interval">> => <<"15s">>,
                <<"inflight_window">> => 100,
                <<"max_buffer_bytes">> => <<"256MB">>,
                <<"metrics_flush_interval">> => <<"1s">>,
                <<"query_mode">> => <<"sync">>,
                <<"request_ttl">> => <<"45s">>,
                <<"resume_interval">> => <<"15s">>,
                <<"worker_pool_size">> => <<"1">>
            }
        },
    maps:merge(CommonConfig, Overrides).

replace(Key, Value, Proplist) ->
    lists:keyreplace(Key, 1, Proplist, {Key, Value}).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    ConnectorName = ?config(connector_name, Config),
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{
                    <<"enable">> := true,
                    <<"status">> := <<"connected">>
                }
            ]}},
        emqx_bridge_v2_testlib:list_bridges_http_api_v1()
    ),
    NewSourceName = <<"my_other_source">>,
    {ok, {{_, 201, _}, _, _}} =
        emqx_bridge_v2_testlib:create_kind_api(
            replace(source_name, NewSourceName, Config)
        ),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{<<"connector">> := ConnectorName},
                #{<<"connector">> := ConnectorName}
            ]}},
        emqx_bridge_v2_testlib:list_sources_http_api()
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, []}},
        emqx_bridge_v2_testlib:list_bridges_http_api_v1()
    ),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, mqtt_connector_stopped),
    ok.
