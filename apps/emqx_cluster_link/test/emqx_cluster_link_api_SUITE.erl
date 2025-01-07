%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

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
-define(REDACTED, <<"******">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [{group, cluster}, {group, local}].

groups() ->
    [
        {cluster, cluster_test_cases()},
        {local, local_test_cases()}
    ].

local_test_cases() ->
    emqx_common_test_helpers:all(?MODULE) -- cluster_test_cases().

cluster_test_cases() ->
    [
        t_status,
        t_metrics,
        t_disable_reenable
    ].

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(local = _Group, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard(),
            emqx_cluster_link
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    Auth = emqx_mgmt_api_test_util:auth_header_(),
    [{suite_apps, Apps}, {auth, Auth} | Config];
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
    Auth = ?ON(SN1, emqx_mgmt_api_test_util:auth_header_()),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes},
        {auth, Auth}
        | Config
    ].

end_per_group(local, Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config));
end_per_group(cluster, Config) ->
    SourceNodes = ?config(source_nodes, Config),
    TargetNodes = ?config(target_nodes, Config),
    ok = emqx_cth_cluster:stop(SourceNodes),
    ok = emqx_cth_cluster:stop(TargetNodes).

init_per_testcase(TC, Config) ->
    [Group] = [G || {G, TCs} <- groups(), lists:member(TC, TCs)],
    snabbkaffe:start_trace(),
    init_per_testcase(TC, Group, Config).

init_per_testcase(_TC, local, Config) ->
    {ok, _} = emqx_cluster_link_config:update([]),
    Config;
init_per_testcase(_TC, cluster, Config) ->
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

api_path(ReqPath) ->
    emqx_mgmt_api_test_util:api_path([api_root() | ReqPath]).

api_path(Host, ReqPath) ->
    emqx_mgmt_api_test_util:api_path(Host, [api_root() | ReqPath]).

api_auth(Config) ->
    ?config(auth, Config).

list(Config) ->
    Path = api_path([]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "", api_auth(Config)).

get_link(Name, Config) ->
    get_link(source, Name, Config).

get_link(SourceOrTargetCluster, Name, Config) ->
    Path = api_path(host(SourceOrTargetCluster), ["link", Name]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = "", api_auth(Config)).

delete_link(Name, Config) ->
    Path = api_path(["link", Name]),
    emqx_mgmt_api_test_util:simple_request(delete, Path, _Params = "", api_auth(Config)).

update_link(Name, Params, Config) ->
    update_link(source, Name, Params, Config).

update_link(SourceOrTargetCluster, Name, Params, Config) ->
    Path = api_path(host(SourceOrTargetCluster), ["link", Name]),
    emqx_mgmt_api_test_util:simple_request(put, Path, Params, api_auth(Config)).

create_link(Name, Params0, Config) ->
    Path = api_path([]),
    Params = Params0#{<<"name">> => Name},
    emqx_mgmt_api_test_util:simple_request(post, Path, Params, api_auth(Config)).

get_metrics(Name, Config) ->
    get_metrics(source, Name, Config).

get_metrics(SourceOrTargetCluster, Name, Config) ->
    Path = api_path(host(SourceOrTargetCluster), ["link", Name, "metrics"]),
    emqx_mgmt_api_test_util:simple_request(get, Path, _Params = [], api_auth(Config)).

reset_metrics(SourceOrTargetCluster, Name, Config) ->
    Path = api_path(host(SourceOrTargetCluster), ["link", Name, "metrics", "reset"]),
    emqx_mgmt_api_test_util:simple_request(put, Path, _Params = [], api_auth(Config)).

host(source) -> "http://127.0.0.1:18083";
host(target) -> "http://127.0.0.1:28083".

link_params() ->
    link_params(_Overrides = #{}).

link_params(Overrides) ->
    Default = #{
        <<"clientid">> => <<"linkclientid">>,
        <<"password">> => <<"my secret password">>,
        <<"pool_size">> => 1,
        <<"server">> => <<"emqxcl_2.nohost:31883">>,
        <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]
    },
    emqx_utils_maps:deep_merge(Default, Overrides).

remove_api_virtual_fields(Response) ->
    maps:without([<<"name">>, <<"node_status">>, <<"status">>], Response).

%% Node
disable_and_force_gc(TargetOrSource, Name, Params, TCConfig, Opts) ->
    NExpectedDeletions = maps:get(expected_num_route_deletions, Opts),
    {ok, SRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_link_extrouter_route_deleted"}),
        NExpectedDeletions,
        infinity
    ),
    Nodes =
        case TargetOrSource of
            target -> ?config(source_nodes, TCConfig);
            source -> ?config(target_nodes, TCConfig)
        end,
    {200, _} = update_link(TargetOrSource, Name, Params#{<<"enable">> := false}, TCConfig),
    %% Note that only when the GC runs and collects the stopped actor it'll actually
    %% remove the routes
    NowMS = erlang:system_time(millisecond),
    TTL = emqx_cluster_link_config:actor_ttl(),
    ct:pal("gc"),
    Timestamp = NowMS + TTL * 3,
    lists:foreach(fun(N) -> ok = do_actor_gc(N, Timestamp) end, Nodes),
    {ok, _} = snabbkaffe:receive_events(SRef),
    ct:pal("gc done"),
    ok.

do_actor_gc(Node, Timestamp) ->
    %% 2 Actors: one for normal routes, one for PS routes
    ?ON(Node, emqx_cluster_link_extrouter_gc:force(Timestamp)).

wait_for_routes([Node | Nodes], ExpectedTopics) ->
    Topics = ?ON(Node, emqx_cluster_link_extrouter:topics()),
    case lists:sort(ExpectedTopics) == lists:sort(Topics) of
        true ->
            wait_for_routes(Nodes, ExpectedTopics);
        false ->
            timer:sleep(100),
            wait_for_routes([Node | Nodes], ExpectedTopics)
    end;
wait_for_routes([], _ExpectedTopics) ->
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_put_get_valid(Config) ->
    ?assertMatch({200, []}, list(Config)),

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
    ?assertMatch({201, _}, create_link(Name1, Link1, Config)),
    ?assertMatch({201, _}, create_link(Name2, Link2, Config)),
    ?assertMatch({200, [_, _]}, list(Config)),

    DisabledLink1 = Link1#{<<"enable">> => false},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, DisabledLink1), Config)),
    ?assertMatch({200, #{<<"enable">> := false}}, get_link(Name1, Config)),
    ?assertMatch({200, #{<<"enable">> := true}}, get_link(Name2, Config)),

    SSL = #{<<"enable">> => true, <<"cacertfile">> => ?CACERT},
    SSLLink1 = Link1#{<<"ssl">> => SSL},
    ?assertMatch({200, _}, update_link(Name1, maps:remove(<<"name">>, SSLLink1), Config)),
    ?assertMatch(
        {200, #{<<"ssl">> := #{<<"enable">> := true, <<"cacertfile">> := _Path}}},
        get_link(Name1, Config)
    ),
    ok.

t_put_invalid(Config) ->
    Name = <<"l1">>,
    {201, _} = create_link(Name, link_params(), Config),
    ?assertMatch(
        {400, _},
        update_link(Name, maps:remove(<<"server">>, link_params()), Config)
    ).

%% Tests a sequence of CRUD operations and their expected responses, for common use cases
%% and configuration states.
t_crud(Config) ->
    %% No links initially.
    ?assertMatch({200, []}, list(Config)),
    NameA = <<"a">>,
    ?assertMatch({404, _}, get_link(NameA, Config)),
    ?assertMatch({404, _}, delete_link(NameA, Config)),
    ?assertMatch({404, _}, update_link(NameA, link_params(), Config)),
    ?assertMatch({404, _}, get_metrics(NameA, Config)),

    Params1 = link_params(),
    ?assertMatch(
        {201, #{
            <<"name">> := NameA,
            <<"password">> := ?REDACTED,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        create_link(NameA, Params1, Config)
    ),
    ?assertMatch(
        {400, #{<<"code">> := <<"ALREADY_EXISTS">>}},
        create_link(NameA, Params1, Config)
    ),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := NameA,
                <<"password">> := ?REDACTED,
                <<"status">> := _,
                <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
            }
        ]},
        list(Config)
    ),
    ?assertMatch(
        {200, #{
            <<"name">> := NameA,
            <<"password">> := ?REDACTED,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        get_link(NameA, Config)
    ),
    ?assertMatch({200, _}, get_metrics(NameA, Config)),

    Params2 = Params1#{<<"pool_size">> := 2},
    ?assertMatch(
        {200, #{
            <<"name">> := NameA,
            <<"password">> := ?REDACTED,
            <<"status">> := _,
            <<"node_status">> := [#{<<"node">> := _, <<"status">> := _} | _]
        }},
        update_link(NameA, Params2, Config)
    ),

    ?assertMatch({204, _}, delete_link(NameA, Config)),
    ?assertMatch({404, _}, delete_link(NameA, Config)),
    ?assertMatch({404, _}, get_link(NameA, Config)),
    ?assertMatch({404, _}, update_link(NameA, Params1, Config)),
    ?assertMatch({404, _}, get_metrics(NameA, Config)),
    ?assertMatch({200, []}, list(Config)),

    ok.

t_create_invalid(Config) ->
    Params = link_params(),
    EmptyName = <<>>,
    {400, #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := Message1}} =
        create_link(EmptyName, Params, Config),
    ?assertMatch(
        #{<<"kind">> := <<"validation_error">>, <<"reason">> := <<"Name cannot be empty string">>},
        Message1
    ),
    LongName = binary:copy(<<$a>>, 256),
    {400, #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := Message2}} =
        create_link(LongName, Params, Config),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"Name length must be less than 255">>
        },
        Message2
    ),
    BadName = <<"~!@#$%^&*()_+{}:'<>?|">>,
    {400, #{<<"code">> := <<"BAD_REQUEST">>, <<"message">> := Message3}} =
        create_link(BadName, Params, Config),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"Invalid name format", _/binary>>
        },
        Message3
    ).

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
            list(Config)
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
        get_link(Name, Config)
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
        list(Config)
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
        get_link(Name, Config)
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
        list(Config)
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
        get_link(Name, Config)
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
        get_link(Name, Config)
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
        get_metrics(source, SourceName, Config)
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
        get_metrics(target, TargetName, Config)
    ),

    SourceC1 = emqx_cluster_link_cth:connect_client(<<"sc1">>, SN1),
    SourceC2 = emqx_cluster_link_cth:connect_client(<<"sc2">>, SN2),
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
        get_metrics(source, SourceName, Config)
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
        get_metrics(target, TargetName, Config)
    ),

    TargetC1 = emqx_cluster_link_cth:connect_client(<<"tc1">>, TN1),
    TargetC2 = emqx_cluster_link_cth:connect_client(<<"tc2">>, TN2),
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
            get_metrics(source, SourceName, Config)
        )
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
            <<"node_metrics">> := _
        }},
        get_metrics(target, TargetName, Config)
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
            get_metrics(source, SourceName, Config)
        )
    ),

    %% Disabling the link should remove the routes.
    ct:pal("disabling"),
    {200, TargetLink0} = get_link(target, TargetName, Config),
    TargetLink1 = remove_api_virtual_fields(TargetLink0),
    ok = disable_and_force_gc(target, TargetName, TargetLink1, Config, #{
        expected_num_route_deletions => 1
    }),

    ?retry(
        300,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"router">> := #{<<"routes">> := 0}},
                <<"node_metrics">> := _
            }},
            get_metrics(source, SourceName, Config)
        )
    ),

    %% Enabling again
    TargetLink2 = TargetLink1#{<<"enable">> := true},
    {_, {ok, _}} =
        ?wait_async_action(
            begin
                {200, _} = update_link(target, TargetName, TargetLink2, Config)
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
            get_metrics(source, SourceName, Config)
        )
    ),

    %% Reset metrics
    ?assertMatch({204, _}, reset_metrics(source, SourceName, Config)),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"router">> := #{<<"routes">> := 0},
                <<"forwarding">> := #{<<"matched">> := 0}
            },
            <<"node_metrics">> := [
                #{
                    <<"metrics">> :=
                        #{
                            <<"router">> := #{<<"routes">> := 0},
                            <<"forwarding">> := #{<<"matched">> := 0}
                        }
                },
                #{
                    <<"metrics">> :=
                        #{
                            <<"router">> := #{<<"routes">> := 0},
                            <<"forwarding">> := #{<<"matched">> := 0}
                        }
                }
            ]
        }},
        get_metrics(source, SourceName, Config)
    ),

    ok = lists:foreach(
        fun emqx_cluster_link_cth:disconnect_client/1,
        [SourceC1, SourceC2, TargetC1, TargetC2]
    ).

%% Checks that we can update a link via the API in the same fashion as the frontend does,
%% by sending secrets as `******', and the secret is not mangled.
t_update_password(Config) ->
    ?check_trace(
        begin
            Name = atom_to_binary(?FUNCTION_NAME),
            Password = <<"my secret password">>,
            Params1 = link_params(#{<<"password">> => Password}),
            {201, Response1} = create_link(Name, Params1, Config),
            [#{name := Name, password := WrappedPassword0}] = emqx_config:get([cluster, links]),
            ?assertEqual(Password, emqx_secret:unwrap(WrappedPassword0)),
            Params2A = remove_api_virtual_fields(Response1),
            Params2 = Params2A#{<<"pool_size">> := 2},
            ?assertEqual(?REDACTED, maps:get(<<"password">>, Params2)),
            ?assertMatch({200, _}, update_link(Name, Params2, Config)),
            [#{name := Name, password := WrappedPassword}] = emqx_config:get([cluster, links]),
            ?assertEqual(Password, emqx_secret:unwrap(WrappedPassword)),
            ok
        end,
        []
    ),
    ok.

%% Checks that we forbid duplicate topic filters.
t_duplicate_topic_filters(Config) ->
    ?check_trace(
        begin
            Name = atom_to_binary(?FUNCTION_NAME),
            Params1 = link_params(#{<<"topics">> => [<<"t">>, <<"t">>]}),
            ?assertMatch(
                {400, #{
                    <<"message">> := #{
                        <<"reason">> := #{
                            <<"reason">> := <<"redundant_topics">>,
                            <<"topics">> := [<<"t">>]
                        }
                    }
                }},
                create_link(Name, Params1, Config)
            ),
            ok
        end,
        []
    ),
    ok.

%% Verifies that some fields are not required when updating a link, such as:
%%  - clientid
t_optional_fields_update(Config) ->
    Name = <<"mylink">>,
    Params0 = maps:without([<<"clientid">>], link_params()),
    {201, _} = create_link(Name, Params0, Config),
    ?assertMatch({200, _}, update_link(Name, Params0, Config)),
    ok.

%% Verifies that, if we disable a link and then re-enable it, it should keep working.
t_disable_reenable(Config) ->
    ct:timetrap({seconds, 20}),
    [SN1, _SN2] = SourceNodes = ?config(source_nodes, Config),
    [TN1, TN2] = ?config(target_nodes, Config),
    SourceName = <<"cl.target">>,

    SourceC1 = emqx_cluster_link_cth:connect_client(<<"sc1">>, SN1),
    TargetC1 = emqx_cluster_link_cth:connect_client(<<"tc1">>, TN1),
    TargetC2 = emqx_cluster_link_cth:connect_client(<<"tc2">>, TN2),
    Topic1 = <<"t/tc1">>,
    Topic2 = <<"t/tc2">>,
    {ok, _, _} = emqtt:subscribe(TargetC1, Topic1),
    {ok, _, _} = emqtt:subscribe(TargetC2, Topic2),
    %% fixme: use snabbkaffe subscription
    ?block_until(#{?snk_kind := clink_route_sync_complete, ?snk_meta := #{node := TN1}}),
    ?block_until(#{?snk_kind := clink_route_sync_complete, ?snk_meta := #{node := TN2}}),
    {ok, _} = emqtt:publish(SourceC1, Topic1, <<"1">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(SourceC1, Topic2, <<"2">>, [{qos, 1}]),
    %% Sanity check: link is working, initially.
    ?assertReceive({publish, #{topic := Topic1, payload := <<"1">>}}),
    ?assertReceive({publish, #{topic := Topic2, payload := <<"2">>}}),

    %% Now we just disable and re-enable it in the link in the source cluster.
    {200, #{<<"enable">> := true} = SourceLink0} = get_link(source, SourceName, Config),
    SourceLink1 = remove_api_virtual_fields(SourceLink0),
    %% We force GC to simulate that we left the link disable for enough time that the GC
    %% kicks in.
    ?assertMatch(
        {200, #{<<"enable">> := false}},
        update_link(source, SourceName, SourceLink1#{<<"enable">> := false}, Config)
    ),
    %% In the original issue, GC deleted the state of target cluster's agent in source
    %% cluster.  After the fix, there's no longer GC, so we ignore timeouts here.
    _ = ?block_until(
        #{?snk_kind := "clink_extrouter_gc_ran", result := NumDeleted} when
            NumDeleted > 0,
        emqx_cluster_link_config:actor_ttl() + 1_000
    ),
    ?assertMatch(
        {200, #{<<"enable">> := true}},
        update_link(source, SourceName, SourceLink1, Config)
    ),

    Topic3 = <<"t/tc3">>,
    Topic4 = <<"t/tc4">>,
    {ok, _, _} = emqtt:subscribe(TargetC1, Topic3),
    {ok, _, _} = emqtt:subscribe(TargetC2, Topic4),
    ct:pal("waiting for routes to be synced..."),
    ExpectedTopics = [Topic1, Topic2, Topic3, Topic4],
    wait_for_routes(SourceNodes, ExpectedTopics),

    {ok, _} = emqtt:publish(SourceC1, Topic1, <<"3">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(SourceC1, Topic2, <<"4">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(SourceC1, Topic3, <<"5">>, [{qos, 1}]),
    {ok, _} = emqtt:publish(SourceC1, Topic4, <<"6">>, [{qos, 1}]),
    ?assertReceive({publish, #{topic := Topic1, payload := <<"3">>}}),
    ?assertReceive({publish, #{topic := Topic2, payload := <<"4">>}}),
    ?assertReceive({publish, #{topic := Topic3, payload := <<"5">>}}),
    ?assertReceive({publish, #{topic := Topic4, payload := <<"6">>}}),

    ok = lists:foreach(
        fun emqx_cluster_link_cth:disconnect_client/1,
        [SourceC1, TargetC1, TargetC2]
    ).
