%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_prometheus/include/emqx_prometheus.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    [
        {group, new_config},
        {group, legacy_config}
    ].

groups() ->
    LegacyTCs = legacy_config_test_cases(),
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {new_config, [sequence], AllTCs -- LegacyTCs},
        {legacy_config, [sequence], LegacyTCs}
    ].

legacy_config_test_cases() ->
    [t_stats_no_auth_api, t_legacy_prometheus_api].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        lists:flatten([
            emqx,
            emqx_conf,
            emqx_management,
            {emqx_prometheus, #{start => false}},
            {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"},
            [
                {emqx_license, "license.key = default"}
             || emqx_release:edition() == ee
            ]
        ]),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    {ok, _} = emqx_common_test_http:create_default_app(),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(new_config, Config) ->
    Apps = emqx_cth_suite:start_app(
        emqx_prometheus,
        #{config => emqx_prometheus_SUITE:config(default)}
    ),
    [{group_apps, Apps} | Config];
init_per_group(legacy_config, Config) ->
    Apps = emqx_cth_suite:start_app(
        emqx_prometheus,
        #{config => emqx_prometheus_SUITE:config(legacy)}
    ),
    [{group_apps, Apps} | Config].

end_per_group(_Group, Config) ->
    ok = emqx_cth_suite:stop_apps(?config(group_apps, Config)).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------
%% we return recommend config for prometheus even if prometheus is legacy.
t_legacy_prometheus_api(_) ->
    Path = emqx_mgmt_api_test_util:api_path(["prometheus"]),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),

    OldConf = emqx:get_raw_config([prometheus]),
    Conf = emqx_utils_json:decode(Response, [return_maps]),
    %% Always return new config.
    ?assertMatch(
        #{
            <<"collectors">> :=
                #{
                    <<"mnesia">> := <<"disabled">>,
                    <<"vm_dist">> := <<"disabled">>,
                    <<"vm_memory">> := <<"disabled">>,
                    <<"vm_msacc">> := <<"disabled">>,
                    <<"vm_statistics">> := <<"disabled">>,
                    <<"vm_system_info">> := <<"disabled">>
                },
            <<"enable_basic_auth">> := false,
            <<"push_gateway">> :=
                #{
                    <<"enable">> := true,
                    <<"headers">> := #{<<"Authorization">> := <<"some-authz-tokens">>},
                    <<"interval">> := <<"1s">>,
                    <<"job_name">> := <<"${name}~${host}">>,
                    <<"url">> := <<"http://127.0.0.1:9091">>
                }
        },
        Conf
    ),
    #{<<"push_gateway">> := #{<<"enable">> := Enable}} = Conf,
    ?assertEqual(Enable, undefined =/= erlang:whereis(emqx_prometheus)),

    NewConf = OldConf#{
        <<"interval">> => <<"2s">>,
        <<"vm_statistics_collector">> => <<"enabled">>,
        <<"headers">> => #{
            <<"test-str1">> => <<"test-value">>,
            <<"test-str2">> => <<"42">>
        }
    },
    {ok, Response2} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf),

    Conf2 = emqx_utils_json:decode(Response2, [return_maps]),
    ?assertEqual(NewConf, Conf2),

    EnvCollectors = env_collectors(),
    PromCollectors = all_collectors(),
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

    NewConf1 = OldConf#{<<"enable">> => (not Enable)},
    {ok, _Response3} = emqx_mgmt_api_test_util:request_api(put, Path, "", Auth, NewConf1),
    ?assertEqual((not Enable), undefined =/= erlang:whereis(emqx_prometheus)),

    ConfWithoutScheme = OldConf#{<<"push_gateway_server">> => "127.0.0.1:8081"},
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
            #{<<"url">> := Url, <<"enable">> := Enable} = PushGateway,
        <<"collectors">> := Collector
    } = Conf,
    Pid = erlang:whereis(emqx_prometheus),
    ?assertEqual(Enable, undefined =/= Pid, {Url, Pid}),

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

    EnvCollectors = env_collectors(),
    PromCollectors = all_collectors(),
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

    NewConf1 = Conf#{<<"push_gateway">> => PushGateway#{<<"enable">> => false}},
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
            {ok, _} = emqx:update_config([prometheus, enable_basic_auth], false);
        _ ->
            ok
    end,
    emqx_dashboard_listener:regenerate_minirest_dispatch(),
    Headers = accept_json_header(),
    request_stats(Headers, []).

t_stats_auth_api(_) ->
    {ok, _} = emqx:update_config([prometheus, enable_basic_auth], true),
    emqx_dashboard_listener:regenerate_minirest_dispatch(),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    Headers = [Auth | accept_json_header()],
    request_stats(Headers, Auth),
    ok.

%% Simple smoke test for verifying reason code labels in `emqx_client_disconnected_reason'
%% counter metric.
t_listener_shutdown_count(_Config) ->
    ClientId1 = <<"shutdown_count_test">>,
    {ok, C1} = emqtt:start_link(#{clientid => ClientId1}),
    {ok, _} = emqtt:connect(C1),
    %% Takeover
    unlink(C1),
    {ok, C2} = emqtt:start_link(#{clientid => ClientId1}),
    {ok, _} = emqtt:connect(C2),
    %% Kick
    unlink(C2),
    ok = emqx_cm:kick_session(ClientId1),
    %% Normal disconnect
    ClientId2 = <<"shutdown_count_test2">>,
    {ok, C3} = emqtt:start_link(#{clientid => ClientId2}),
    {ok, _} = emqtt:connect(C3),
    ok = emqtt:stop(C3),
    %% Disconnect with reason code
    {ok, C4} = emqtt:start_link(#{clientid => ClientId2}),
    {ok, _} = emqtt:connect(C4),
    ok = emqtt:disconnect(C4, ?RC_IMPLEMENTATION_SPECIFIC_ERROR),
    OnlyDisconnectStats = fun(Stats0) ->
        Stats = lists:filter(
            fun
                (#{<<"emqx_client_disconnected_reason">> := _}) ->
                    true;
                (_) ->
                    false
            end,
            Stats0
        ),
        lists:sort(Stats)
    end,
    #{<<"client">> := JSONClientStatsNode} = get_stats(json, ?PROM_DATA_MODE__NODE),
    ?assertEqual(
        [
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"discarded">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"kicked">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"tcp_closed">>
            }
        ],
        OnlyDisconnectStats(JSONClientStatsNode)
    ),
    #{<<"client">> := JSONClientStatsAgg} = get_stats(json, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED),
    ?assertEqual(
        [
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"discarded">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"kicked">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"tcp_closed">>
            }
        ],
        OnlyDisconnectStats(JSONClientStatsAgg)
    ),
    #{<<"client">> := JSONClientStatsUnagg} = get_stats(
        json, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED
    ),
    NodeBin = atom_to_binary(node()),
    ?assertEqual(
        [
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"node">> => NodeBin,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"discarded">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"node">> => NodeBin,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"kicked">>
            },
            #{
                <<"emqx_client_disconnected_reason">> => 1,
                <<"node">> => NodeBin,
                <<"listener_type">> => <<"tcp">>,
                <<"listener_name">> => <<"default">>,
                <<"reason">> => <<"tcp_closed">>
            }
        ],
        OnlyDisconnectStats(JSONClientStatsUnagg)
    ),
    AssertExpectedLines = fun(ExpectedLines, Output) ->
        lists:foreach(
            fun(ExpectedLine) ->
                ?assertEqual(
                    match,
                    re:run(Output, ExpectedLine, [global, {capture, none}]),
                    #{
                        expected => ExpectedLine,
                        output => string:split(Output, <<"\n">>, all)
                    }
                )
            end,
            ExpectedLines
        )
    end,
    PromClientStatsNode = get_stats(prometheus, ?PROM_DATA_MODE__NODE),
    ExpectedLines1 = [
        iolist_to_binary(
            io_lib:format(
                "emqx_client_disconnected_reason{"
                "listener_type=\"tcp\",listener_name=\"default\","
                "reason=\"~s\"} ~b",
                [Reason, N]
            )
        )
     || {Reason, N} <- [
            {"discarded", 1},
            {"kicked", 1},
            {"tcp_closed", 1}
        ]
    ],
    AssertExpectedLines(ExpectedLines1, PromClientStatsNode),
    PromClientStatsAgg = get_stats(prometheus, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED),
    AssertExpectedLines(ExpectedLines1, PromClientStatsAgg),
    PromClientStatsUnagg = get_stats(prometheus, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED),
    ExpectedLines2 = [
        iolist_to_binary(
            io_lib:format(
                "emqx_client_disconnected_reason{"
                "node=\"~s\",listener_type=\"tcp\",listener_name=\"default\","
                "reason=\"~s\"} ~b",
                [NodeBin, Reason, N]
            )
        )
     || {Reason, N} <- [
            {"discarded", 1},
            {"kicked", 1},
            {"tcp_closed", 1}
        ]
    ],
    AssertExpectedLines(ExpectedLines2, PromClientStatsUnagg),
    ok.

accept_json_header() ->
    [{"accept", "application/json"}].

request_stats(Headers, Auth) ->
    Path = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, "", Headers),
    Data = emqx_utils_json:decode(Response, [return_maps]),
    ?assertMatch(#{<<"client">> := _, <<"delivery">> := _}, Data),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth),
    ok = meck:expect(mria_rlog, backend, fun() -> rlog end),
    {ok, _} = emqx_mgmt_api_test_util:request_api(get, Path, "", Auth).

%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%
%%% Internal Functions
%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%%

env_collectors() ->
    do_env_collectors(application:get_env(prometheus, collectors, []), []).

do_env_collectors([], Acc) ->
    lists:reverse(Acc);
do_env_collectors([{_Registry, Collector} | Rest], Acc) when is_atom(Collector) ->
    do_env_collectors(Rest, [Collector | Acc]);
do_env_collectors([Collector | Rest], Acc) when is_atom(Collector) ->
    do_env_collectors(Rest, [Collector | Acc]).

all_collectors() ->
    emqx_prometheus_config:all_collectors().

get_stats(Format, Mode) ->
    Headers =
        case Format of
            json -> accept_json_header();
            prometheus -> []
        end,
    QueryString = uri_string:compose_query([{"mode", atom_to_binary(Mode)}]),
    Path = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {ok, Response} = emqx_mgmt_api_test_util:request_api(get, Path, QueryString, Headers),
    case Format of
        json ->
            emqx_utils_json:decode(Response, [return_maps]);
        prometheus ->
            Response
    end.
