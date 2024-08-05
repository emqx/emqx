%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(API_PATH, emqx_mgmt_api_test_util:api_path(["cluster", "links"])).
-define(CONF_PATH, [cluster, links]).

-define(CACERT, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV\n"
    "BAYTAkNOMREwDwYDVQQIDAhoYW5nemhvdTEMMAoGA1UECgwDRU1RMQ8wDQYDVQQD\n"
    "DAZSb290Q0EwHhcNMjAwNTA4MDgwNjUyWhcNMzAwNTA2MDgwNjUyWjA/MQswCQYD\n"
    "VQQGEwJDTjERMA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UE\n"
    "AwwGUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzcgVLex1\n"
    "EZ9ON64EX8v+wcSjzOZpiEOsAOuSXOEN3wb8FKUxCdsGrsJYB7a5VM/Jot25Mod2\n"
    "juS3OBMg6r85k2TWjdxUoUs+HiUB/pP/ARaaW6VntpAEokpij/przWMPgJnBF3Ur\n"
    "MjtbLayH9hGmpQrI5c2vmHQ2reRZnSFbY+2b8SXZ+3lZZgz9+BaQYWdQWfaUWEHZ\n"
    "uDaNiViVO0OT8DRjCuiDp3yYDj3iLWbTA/gDL6Tf5XuHuEwcOQUrd+h0hyIphO8D\n"
    "tsrsHZ14j4AWYLk1CPA6pq1HIUvEl2rANx2lVUNv+nt64K/Mr3RnVQd9s8bK+TXQ\n"
    "KGHd2Lv/PALYuwIDAQABo1AwTjAdBgNVHQ4EFgQUGBmW+iDzxctWAWxmhgdlE8Pj\n"
    "EbQwHwYDVR0jBBgwFoAUGBmW+iDzxctWAWxmhgdlE8PjEbQwDAYDVR0TBAUwAwEB\n"
    "/zANBgkqhkiG9w0BAQsFAAOCAQEAGbhRUjpIred4cFAFJ7bbYD9hKu/yzWPWkMRa\n"
    "ErlCKHmuYsYk+5d16JQhJaFy6MGXfLgo3KV2itl0d+OWNH0U9ULXcglTxy6+njo5\n"
    "CFqdUBPwN1jxhzo9yteDMKF4+AHIxbvCAJa17qcwUKR5MKNvv09C6pvQDJLzid7y\n"
    "E2dkgSuggik3oa0427KvctFf8uhOV94RvEDyqvT5+pgNYZ2Yfga9pD/jjpoHEUlo\n"
    "88IGU8/wJCx3Ds2yc8+oBg/ynxG8f/HmCC1ET6EHHoe2jlo8FpU/SgGtghS1YL30\n"
    "IWxNsPrUP+XsZpBJy/mvOhE5QXo6Y35zDqqj8tI7AGmAWu22jg==\n"
    "-----END CERTIFICATE-----"
>>).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    OtherTCs = AllTCs -- cluster_test_cases(),
    [
        {group, cluster}
        | OtherTCs
    ].

groups() ->
    [{cluster, cluster_test_cases()}].

cluster_test_cases() ->
    [
        t_status,
        t_metrics
    ].

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_cluster_link
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Auth = auth_header(),
    [{suite_apps, Apps}, {auth, Auth} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

init_per_group(cluster = Group, Config) ->
    ok = emqx_cth_suite:stop_apps([emqx_dashboard]),
    SourceClusterSpec = emqx_cluster_link_SUITE:mk_source_cluster(Group, Config),
    TargetClusterSpec = emqx_cluster_link_SUITE:mk_target_cluster(Group, Config),
    SourceNodes = [SN1 | _] = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = [TN1 | _] = emqx_cth_cluster:start(TargetClusterSpec),
    emqx_cluster_link_SUITE:start_cluster_link(SourceNodes ++ TargetNodes, Config),
    erpc:call(SN1, emqx_cth_suite, start_apps, [
        [emqx_management, emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ]),
    erpc:call(TN1, emqx_cth_suite, start_apps, [
        [
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http { enable = true, bind = 28083 }"
            )
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ]),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

end_per_group(cluster, Config) ->
    SourceNodes = ?config(source_nodes, Config),
    TargetNodes = ?config(target_nodes, Config),
    ok = emqx_cth_cluster:stop(SourceNodes),
    ok = emqx_cth_cluster:stop(TargetNodes),
    _ = emqx_cth_suite:start_apps(
        [emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    ok;
end_per_group(_Group, _Config) ->
    ok.

auth_header() ->
    emqx_mgmt_api_test_util:auth_header_().

init_per_testcase(_TC, Config) ->
    {ok, _} = emqx_cluster_link_config:update([]),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_TC, _Config) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

api_root() ->
    <<"cluster/links">>.

list() ->
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "").

get_link(Name) ->
    get_link(source, Name).

get_link(SourceOrTargetCluster, Name) ->
    Host = host(SourceOrTargetCluster),
    Path = emqx_mgmt_api_test_util:api_path(Host, [api_root(), "link", Name]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "").

delete_link(Name) ->
    Path = emqx_mgmt_api_test_util:api_path([api_root(), "link", Name]),
    emqx_mgmt_api_test_util:simple_request(delete, Path, _Params = "").

update_link(Name, Params) ->
    update_link(source, Name, Params).

update_link(SourceOrTargetCluster, Name, Params) ->
    Host = host(SourceOrTargetCluster),
    Path = emqx_mgmt_api_test_util:api_path(Host, [api_root(), "link", Name]),
    emqx_mgmt_api_test_util:simple_request(put, Path, Params).

create_link(Name, Params0) ->
    Params = Params0#{<<"name">> => Name},
    Path = emqx_mgmt_api_test_util:api_path([api_root()]),
    emqx_mgmt_api_test_util:simple_request(post, Path, Params).

get_metrics(Name) ->
    get_metrics(source, Name).

get_metrics(SourceOrTargetCluster, Name) ->
    Host = host(SourceOrTargetCluster),
    Path = emqx_mgmt_api_test_util:api_path(Host, [api_root(), "link", Name, "metrics"]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = []).

host(source) -> "http://127.0.0.1:18083";
host(target) -> "http://127.0.0.1:28083".

link_params() ->
    link_params(_Overrides = #{}).

link_params(Overrides) ->
    Default = #{
        <<"clientid">> => <<"linkclientid">>,
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_put_get_valid(_Config) ->
    ?assertMatch({200, []}, list()),

    Name1 = <<"emqcl_1">>,
    Link1 = link_params(#{
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"name">> => Name1
    }),
    Name2 = <<"emqcl_2">>,
    Link2 = link_params(#{
        <<"server">> => <<"emqxcl_2.nohost:41883">>,
        <<"name">> => Name2
    }),
    ?assertMatch({201, _}, create_link(Name1, Link1)),
    ?assertMatch({201, _}, create_link(Name2, Link2)),
    ?assertMatch({200, [_, _]}, list()),

    DisabledLink1 = Link1#{<<"enable">> => false},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, DisabledLink1))),
    ?assertMatch({200, #{<<"enable">> := false}}, get_link(Name1)),
    ?assertMatch({200, #{<<"enable">> := true}}, get_link(Name2)),

    SSL = #{<<"enable">> => true, <<"cacertfile">> => ?CACERT},
    SSLLink1 = Link1#{<<"ssl">> => SSL},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, SSLLink1))),
    ?assertMatch(
        {200, #{<<"ssl">> := #{<<"enable">> := true, <<"cacertfile">> := _Path}}},
        get_link(Name1)
    ),
    ok.

t_put_invalid(_Config) ->
    Name = <<"l1">>,
    {201, _} = create_link(Name, link_params()),
    ?assertMatch(
        {400, _},
        update_link(Name, maps:remove(<<"server">>, link_params()))
    ).

%% Tests a sequence of CRUD operations and their expected responses, for common use cases
%% and configuration states.
t_crud(_Config) ->
    %% No links initially.
    ?assertMatch({200, []}, list()),
    NameA = <<"a">>,
    ?assertMatch({404, _}, get_link(NameA)),
    ?assertMatch({404, _}, delete_link(NameA)),
    ?assertMatch({404, _}, update_link(NameA, link_params())),
    ?assertMatch({404, _}, get_metrics(NameA)),

    Params1 = link_params(),
    ?assertMatch(
        {201, #{
            <<"name">> := NameA,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        create_link(NameA, Params1)
    ),
    ?assertMatch({400, #{<<"code">> := <<"ALREADY_EXISTS">>}}, create_link(NameA, Params1)),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := NameA,
                <<"status">> := _,
                <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
            }
        ]},
        list()
    ),
    ?assertMatch(
        {200, #{
            <<"name">> := NameA,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        get_link(NameA)
    ),
    ?assertMatch({200, _}, get_metrics(NameA)),

    Params2 = Params1#{<<"pool_size">> := 2},
    ?assertMatch(
        {200, #{
            <<"name">> := NameA,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        update_link(NameA, Params2)
    ),

    ?assertMatch({204, _}, delete_link(NameA)),
    ?assertMatch({404, _}, delete_link(NameA)),
    ?assertMatch({404, _}, get_link(NameA)),
    ?assertMatch({404, _}, update_link(NameA, Params1)),
    ?assertMatch({404, _}, get_metrics(NameA)),
    ?assertMatch({200, []}, list()),

    ok.

%% Verifies the behavior of reported status under different conditions when listing all
%% links and when fetching a specific link.
t_status(Config) ->
    [SN1 | _] = ?config(source_nodes, Config),
    Name = <<"cl.target">>,
    ?retry(
        100,
        10,
        ?assertMatch(
            {200, [
                #{
                    <<"status">> := <<"connected">>,
                    <<"node_status">> := [
                        #{
                            <<"node">> := _,
                            <<"status">> := <<"connected">>
                        },
                        #{
                            <<"node">> := _,
                            <<"status">> := <<"connected">>
                        }
                    ]
                }
            ]},
            list()
        )
    ),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"connected">>,
            <<"node_status">> := [
                #{
                    <<"node">> := _,
                    <<"status">> := <<"connected">>
                },
                #{
                    <<"node">> := _,
                    <<"status">> := <<"connected">>
                }
            ]
        }},
        get_link(Name)
    ),

    %% If one of the nodes reports a different status, the cluster is inconsistent.
    ProtoMod = emqx_cluster_link_proto_v1,
    ?ON(SN1, begin
        ok = meck:new(ProtoMod, [no_link, passthrough, no_history]),
        meck:expect(ProtoMod, get_all_resources, fun(Nodes) ->
            [Res1, {ok, Res2A} | Rest] = meck:passthrough([Nodes]),
            %% Res2A :: #{cluster_name() => emqx_resource:resource_data()}
            Res2B = maps:map(fun(_, Data) -> Data#{status := disconnected} end, Res2A),
            [Res1, {ok, Res2B} | Rest]
        end),
        meck:expect(ProtoMod, get_resource, fun(Nodes, LinkName) ->
            [Res1, {ok, {ok, Res2A}} | Rest] = meck:passthrough([Nodes, LinkName]),
            Res2B = Res2A#{status := disconnected},
            [Res1, {ok, {ok, Res2B}} | Rest]
        end)
    end),
    on_exit(fun() -> catch ?ON(SN1, meck:unload()) end),
    ?assertMatch(
        {200, [
            #{
                <<"status">> := <<"inconsistent">>,
                <<"node_status">> := [
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"connected">>
                    },
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"disconnected">>
                    }
                ]
            }
        ]},
        list()
    ),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"inconsistent">>,
            <<"node_status">> := [
                #{
                    <<"node">> := _,
                    <<"status">> := <<"connected">>
                },
                #{
                    <<"node">> := _,
                    <<"status">> := <<"disconnected">>
                }
            ]
        }},
        get_link(Name)
    ),

    %% Simulating erpc failures
    ?ON(SN1, begin
        meck:expect(ProtoMod, get_all_resources, fun(Nodes) ->
            [Res1, _ | Rest] = meck:passthrough([Nodes]),
            [Res1, {error, {erpc, noconnection}} | Rest]
        end),
        meck:expect(ProtoMod, get_resource, fun(Nodes, LinkName) ->
            [Res1, _ | Rest] = meck:passthrough([Nodes, LinkName]),
            [Res1, {error, {erpc, noconnection}} | Rest]
        end)
    end),
    ?assertMatch(
        {200, [
            #{
                <<"status">> := <<"inconsistent">>,
                <<"node_status">> := [
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"connected">>
                    },
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"inconsistent">>,
                        <<"reason">> := _
                    }
                ]
            }
        ]},
        list()
    ),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"inconsistent">>,
            <<"node_status">> := [
                #{
                    <<"node">> := _,
                    <<"status">> := <<"connected">>
                },
                #{
                    <<"node">> := _,
                    <<"status">> := <<"inconsistent">>,
                    <<"reason">> := _
                }
            ]
        }},
        get_link(Name)
    ),
    %% Simulate another inconsistency
    ?ON(SN1, begin
        meck:expect(ProtoMod, get_resource, fun(Nodes, LinkName) ->
            [Res1, _ | Rest] = meck:passthrough([Nodes, LinkName]),
            [Res1, {ok, {error, not_found}} | Rest]
        end)
    end),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"inconsistent">>,
            <<"node_status">> := [
                #{
                    <<"node">> := _,
                    <<"status">> := <<"connected">>
                },
                #{
                    <<"node">> := _,
                    <<"status">> := <<"disconnected">>
                }
            ]
        }},
        get_link(Name)
    ),

    ok.

t_metrics(Config) ->
    ct:timetrap({seconds, 10}),
    [SN1, SN2] = ?config(source_nodes, Config),
    [TN1, TN2] = ?config(target_nodes, Config),
    %% N.B. Link names on each cluster, so they are switched.
    SourceName = <<"cl.target">>,
    TargetName = <<"cl.source">>,

    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"router">> := #{
                    <<"routes">> := 0
                },
                <<"forwarding">> := #{
                    <<"matched">> := _,
                    <<"success">> := _,
                    <<"failed">> := _,
                    <<"dropped">> := _,
                    <<"retried">> := _,
                    <<"received">> := _,
                    <<"queuing">> := _,
                    <<"inflight">> := _,
                    <<"rate">> := _,
                    <<"rate_last5m">> := _,
                    <<"rate_max">> := _
                }
            },
            <<"node_metrics">> := [
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{
                        <<"router">> := #{
                            <<"routes">> := 0
                        },
                        <<"forwarding">> := #{
                            <<"matched">> := _,
                            <<"success">> := _,
                            <<"failed">> := _,
                            <<"dropped">> := _,
                            <<"retried">> := _,
                            <<"received">> := _,
                            <<"queuing">> := _,
                            <<"inflight">> := _,
                            <<"rate">> := _,
                            <<"rate_last5m">> := _,
                            <<"rate_max">> := _
                        }
                    }
                },
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{
                        <<"router">> := #{
                            <<"routes">> := 0
                        },
                        <<"forwarding">> := #{
                            <<"matched">> := _,
                            <<"success">> := _,
                            <<"failed">> := _,
                            <<"dropped">> := _,
                            <<"retried">> := _,
                            <<"received">> := _,
                            <<"queuing">> := _,
                            <<"inflight">> := _,
                            <<"rate">> := _,
                            <<"rate_last5m">> := _,
                            <<"rate_max">> := _
                        }
                    }
                }
            ]
        }},
        get_metrics(source, SourceName)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
            <<"node_metrics">> := [
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                },
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                }
            ]
        }},
        get_metrics(target, TargetName)
    ),

    SourceC1 = emqx_cluster_link_SUITE:start_client(<<"sc1">>, SN1),
    SourceC2 = emqx_cluster_link_SUITE:start_client(<<"sc2">>, SN2),
    {ok, _, _} = emqtt:subscribe(SourceC1, <<"t/sc1">>),
    {ok, _, _} = emqtt:subscribe(SourceC2, <<"t/sc2">>),

    %% Still no routes, as routes in the source cluster are replicated to the target
    %% cluster.
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
            <<"node_metrics">> := [
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                },
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                }
            ]
        }},
        get_metrics(source, SourceName)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
            <<"node_metrics">> := [
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                },
                #{
                    <<"node">> := _,
                    <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}}
                }
            ]
        }},
        get_metrics(target, TargetName)
    ),

    TargetC1 = emqx_cluster_link_SUITE:start_client(<<"tc1">>, TN1),
    TargetC2 = emqx_cluster_link_SUITE:start_client(<<"tc2">>, TN2),
    {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/tc1">>),
    {ok, _, _} = emqtt:subscribe(TargetC2, <<"t/tc2">>),
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/tc1">>),
                {ok, _, _} = emqtt:subscribe(TargetC2, <<"t/tc2">>)
            end,
            #{?snk_kind := clink_route_sync_complete}
        ),

    %% Routes = 2 in source cluster, because the target cluster has some topic filters
    %% configured and subscribers to them, which were replicated to the source cluster.
    %% This metric is global (cluster-wide).
    ?retry(
        300,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"router">> := #{<<"routes">> := 2}},
                <<"node_metrics">> := [
                    #{<<"metrics">> := #{<<"router">> := #{<<"routes">> := 2}}},
                    #{<<"metrics">> := #{<<"router">> := #{<<"routes">> := 2}}}
                ]
            }},
            get_metrics(source, SourceName)
        )
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
            <<"node_metrics">> := _
        }},
        get_metrics(target, TargetName)
    ),

    %% Unsubscribe and remove route.
    ct:pal("unsubscribing"),
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                {ok, _, _} = emqtt:unsubscribe(TargetC1, <<"t/tc1">>)
            end,
            #{?snk_kind := clink_route_sync_complete}
        ),

    ?retry(
        300,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"router">> := #{<<"routes">> := 1}},
                <<"node_metrics">> := [
                    #{<<"metrics">> := #{<<"router">> := #{<<"routes">> := 1}}},
                    #{<<"metrics">> := #{<<"router">> := #{<<"routes">> := 1}}}
                ]
            }},
            get_metrics(source, SourceName)
        )
    ),

    %% Disabling the link should remove the routes.
    ct:pal("disabling"),
    {200, TargetLink0} = get_link(target, TargetName),
    TargetLink1 = maps:without([<<"status">>, <<"node_status">>], TargetLink0),
    TargetLink2 = TargetLink1#{<<"enable">> := false},
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                {200, _} = update_link(target, TargetName, TargetLink2),
                %% Note that only when the GC runs and collects the stopped actor it'll actually
                %% remove the routes
                NowMS = erlang:system_time(millisecond),
                TTL = emqx_cluster_link_config:actor_ttl(),
                ct:pal("gc"),
                %% 2 Actors: one for normal routes, one for PS routes
                1 = ?ON(SN1, emqx_cluster_link_extrouter:actor_gc(#{timestamp => NowMS + TTL * 3})),
                1 = ?ON(SN1, emqx_cluster_link_extrouter:actor_gc(#{timestamp => NowMS + TTL * 3})),
                ct:pal("gc done"),
                ok
            end,
            #{?snk_kind := "cluster_link_extrouter_route_deleted"}
        ),

    ?retry(
        300,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
                <<"node_metrics">> := _
            }},
            get_metrics(source, SourceName)
        )
    ),

    %% Enabling again
    TargetLink3 = TargetLink2#{<<"enable">> := true},
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                {200, _} = update_link(target, TargetName, TargetLink3)
            end,
            #{?snk_kind := "cluster_link_extrouter_route_added"}
        ),

    ?retry(
        300,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"router">> := #{<<"routes">> := 1}},
                <<"node_metrics">> := _
            }},
            get_metrics(source, SourceName)
        )
    ),

    ok.
