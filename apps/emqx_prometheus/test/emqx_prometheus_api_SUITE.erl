%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_prometheus_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-define(CLUSTER_RPC_SHARD, emqx_cluster_rpc_shard).

-define(LOGT(Format, Args), ct:pal("TEST_SUITE: " ++ Format, Args)).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    [
        {group, new_config},
        {group, legacy_config}
    ].

groups() ->
    [
        {new_config, [sequence], [t_stats_auth_api, t_stats_no_auth_api, t_prometheus_api]},
        {legacy_config, [sequence], [t_stats_no_auth_api, t_legacy_prometheus_api]}
    ].

init_per_suite(Config) ->
    emqx_prometheus_SUITE:init_group(),
    emqx_mgmt_api_test_util:init_suite([emqx_conf]),
    Config.
end_per_suite(Config) ->
    emqx_prometheus_SUITE:end_group(),
    emqx_mgmt_api_test_util:end_suite([emqx_conf]),
    Config.

init_per_group(new_config, Config) ->
    emqx_common_test_helpers:start_apps(
        [emqx_prometheus],
        fun(App) -> set_special_configs(App, new_config) end
    ),
    Config;
init_per_group(legacy_config, Config) ->
    emqx_common_test_helpers:start_apps(
        [emqx_prometheus],
        fun(App) -> set_special_configs(App, legacy_config) end
    ),
    Config.

end_per_group(_Group, Config) ->
    _ = application:stop(emqx_prometheus),
    Config.

set_special_configs(emqx_dashboard, _) ->
    emqx_dashboard_api_test_helpers:set_default_config();
set_special_configs(emqx_prometheus, new_config) ->
    emqx_prometheus_SUITE:load_config(),
    ok;
set_special_configs(emqx_prometheus, legacy_config) ->
    emqx_prometheus_SUITE:load_legacy_config(),
    ok;
set_special_configs(_App, _) ->
    ok.

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
t_legacy_prometheus_api(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["prometheus"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),

    Conf = emqx_utils_json:decode(Response, [return_maps]),
    ?assertMatch(
        #{
            <<"push_gateway_server">> := _,
            <<"interval">> := _,
            <<"enable">> := _,
            <<"vm_statistics_collector">> := _,
            <<"vm_system_info_collector">> := _,
            <<"vm_memory_collector">> := _,
            <<"vm_msacc_collector">> := _,
            <<"headers">> := _
        },
        Conf
    ),
    #{<<"enable">> := Enable} = Conf,
    ?assertEqual(Enable, undefined =/= erlang:whereis(emqx_prometheus)),

    NewConf = Conf#{
        <<"interval">> => <<"2s">>,
        <<"vm_statistics_collector">> => <<"enabled">>,
        <<"headers">> => #{
            <<"test-str1">> => <<"test-value">>,
            <<"test-str2">> => <<"42">>
        }
    },
    {ok, Response2} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf),

    Conf2 = emqx_utils_json:decode(Response2, [return_maps]),
    ?assertMatch(NewConf, Conf2),

    EnvCollectors = application:get_env(prometheus, collectors, []),
    PromCollectors = prometheus_registry:collectors(default),
    ?assertEqual(lists:sort(EnvCollectors), lists:sort(PromCollectors)),
    ?assert(lists:member(prometheus_vm_statistics_collector, EnvCollectors), EnvCollectors),

    lists:foreach(
        fun({C, Enabled}) ->
            ?assertEqual(Enabled, lists:member(C, EnvCollectors), EnvCollectors)
        end,
        [
            {prometheus_vm_dist_collector, false},
            {prometheus_vm_system_info_collector, false},
            {prometheus_vm_memory_collector, false},
            {prometheus_mnesia_collector, false},
            {prometheus_vm_msacc_collector, false},
            {prometheus_vm_statistics_collector, true}
        ]
    ),

    ?assertMatch(
        #{
            <<"headers">> := #{
                <<"test-str1">> := <<"test-value">>,
                <<"test-str2">> := <<"42">>
            }
        },
        emqx_config:get_raw([prometheus])
    ),
    ?assertMatch(
        #{
            headers := [
                {"test-str2", "42"},
                {"test-str1", "test-value"}
            ]
        },
        emqx_config:get([prometheus])
    ),

    NewConf1 = Conf#{<<"enable">> => (not Enable)},
    {ok, _Response3} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf1),
    ?assertEqual((not Enable), undefined =/= erlang:whereis(emqx_prometheus)),

    ConfWithoutScheme = Conf#{<<"push_gateway_server">> => "127.0.0.1:8081"},
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, ConfWithoutScheme)
    ),
    ok.

t_prometheus_api(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["prometheus"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),

    Conf = emqx_utils_json:decode(Response, [return_maps]),
    ?assertMatch(
        #{
            <<"push_gateway">> := #{},
            <<"collectors">> := _,
            <<"enable_basic_auth">> := _
        },
        Conf
    ),
    #{
        <<"push_gateway">> :=
            #{<<"url">> := Url} = PushGateway,
        <<"collectors">> := Collector
    } = Conf,
    Pid = erlang:whereis(emqx_prometheus),
    ?assertEqual(Url =/= "", undefined =/= Pid, {Url, Pid}),

    NewConf = Conf#{
        <<"push_gateway">> => PushGateway#{
            <<"interval">> => <<"2s">>,
            <<"headers">> => #{
                <<"test-str1">> => <<"test-value">>,
                <<"test-str2">> => <<"42">>
            }
        },
        <<"collectors">> => Collector#{
            <<"vm_dist">> => <<"enabled">>,
            <<"vm_system_info">> => <<"enabled">>,
            <<"vm_memory">> => <<"enabled">>,
            <<"vm_msacc">> => <<"enabled">>,
            <<"mnesia">> => <<"enabled">>,
            <<"vm_statistics">> => <<"enabled">>
        }
    },
    {ok, Response2} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf),

    Conf2 = emqx_utils_json:decode(Response2, [return_maps]),
    ?assertMatch(NewConf, Conf2),

    EnvCollectors = application:get_env(prometheus, collectors, []),
    PromCollectors = prometheus_registry:collectors(default),
    ?assertEqual(lists:sort(EnvCollectors), lists:sort(PromCollectors)),
    ?assert(lists:member(prometheus_vm_statistics_collector, EnvCollectors), EnvCollectors),

    lists:foreach(
        fun({C, Enabled}) ->
            ?assertEqual(Enabled, lists:member(C, EnvCollectors), EnvCollectors)
        end,
        [
            {prometheus_vm_dist_collector, true},
            {prometheus_vm_system_info_collector, true},
            {prometheus_vm_memory_collector, true},
            {prometheus_mnesia_collector, true},
            {prometheus_vm_msacc_collector, true},
            {prometheus_vm_statistics_collector, true}
        ]
    ),

    ?assertMatch(
        #{
            <<"push_gateway">> := #{
                <<"headers">> := #{
                    <<"test-str1">> := <<"test-value">>,
                    <<"test-str2">> := <<"42">>
                }
            }
        },
        emqx_config:get_raw([prometheus])
    ),
    ?assertMatch(
        #{
            push_gateway := #{
                headers := [
                    {"test-str2", "42"},
                    {"test-str1", "test-value"}
                ]
            }
        },
        emqx_config:get([prometheus])
    ),

    NewConf1 = Conf#{<<"push_gateway">> => PushGateway#{<<"url">> => <<"">>}},
    {ok, _Response3} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf1),
    ?assertEqual(undefined, erlang:whereis(emqx_prometheus)),

    ConfWithoutScheme = Conf#{
        <<"push_gateway">> => PushGateway#{<<"url">> => <<"127.0.0.1:8081">>}
    },
    ?assertMatch(
        {error, {"HTTP/1.1", 400, _}},
        emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, ConfWithoutScheme)
    ),
    ok.

t_stats_no_auth_api(_) ->
    %% undefined is legacy prometheus
    case emqx:get_config([prometheus, enable_basic_auth], undefined) of
        true ->
            {ok, _} = emqx:update_config([prometheus, enable_basic_auth], false),
            emqx_dashboard_listener:regenerate_minirest_dispatch();
        _ ->
            ok
    end,
    emqx_dashboard_listener:regenerate_minirest_dispatch(),
    Json = [{"accept", "application/json"}],
    request_stats(Json, []).

t_stats_auth_api(_) ->
    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], true),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    JsonAuth = [{"accept", "application/json"}, Auth],
    request_stats(JsonAuth, Auth),
    ok.

request_stats(JsonAuth, Auth) ->
    Path = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", JsonAuth),
    Data = emqx_utils_json:decode(Response, [return_maps]),
    ?assertMatch(#{<<"client">> := _, <<"delivery">> := _}, Data),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),
    ok = meck:expect(mria_rlog, backend, fun() -> rlog end),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
