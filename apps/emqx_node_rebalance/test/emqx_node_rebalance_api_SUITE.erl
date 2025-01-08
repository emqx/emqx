%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request_api/3,
        request/2,
        request/3,
        uri/1
    ]
).

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect_many/2, stop_many/1, case_specific_node_name/3]
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx, emqx_node_rebalance], #{
        work_dir => ?config(priv_dir, Config)
    }),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    DonorNode = case_specific_node_name(?MODULE, Case, '_donor'),
    RecipientNode = case_specific_node_name(?MODULE, Case, '_recipient'),
    Spec = #{
        role => core,
        join_to => emqx_cth_cluster:node_name(DonorNode),
        listeners => true,
        apps => app_specs()
    },
    Cluster = [{Node, Spec} || Node <- [DonorNode, RecipientNode]],
    ClusterNodes =
        [Node1 | _] = emqx_cth_cluster:start(
            Cluster,
            #{work_dir => ?config(priv_dir, Config)}
        ),
    ok = rpc:call(Node1, emqx_mgmt_api_test_util, init_suite, []),
    ok = take_auth_header_from(Node1),
    [{cluster_nodes, ClusterNodes} | Config].
end_per_testcase(_Case, Config) ->
    Nodes = ?config(cluster_nodes, Config),
    _ = emqx_cth_cluster:stop(Nodes),
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_start_evacuation_validation(Config) ->
    [DonorNode, RecipientNode] = ?config(cluster_nodes, Config),
    BadOpts = [
        #{conn_evict_rate => <<"conn">>},
        #{sess_evict_rate => <<"sess">>},
        #{wait_takeover => <<"wait">>},
        #{wait_health_check => <<"wait">>},
        #{migrate_to => []},
        #{migrate_to => <<"migrate_to">>},
        #{migrate_to => [<<"bad_node">>]},
        #{migrate_to => [<<"bad_node">>, atom_to_binary(DonorNode)]},
        #{unknown => <<"Value">>}
    ],
    lists:foreach(
        fun(Opts) ->
            ?assertMatch(
                {ok, 400, #{}},
                api_post(
                    ["load_rebalance", atom_to_list(DonorNode), "evacuation", "start"],
                    Opts
                ),
                Opts
            )
        end,
        BadOpts
    ),
    ?assertMatch(
        {ok, 404, #{}},
        api_post(
            ["load_rebalance", "bad@node", "evacuation", "start"],
            #{}
        )
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "evacuation", "start"],
            #{
                conn_evict_rate => 10,
                sess_evict_rate => 10,
                wait_takeover => <<"10s">>,
                wait_health_check => <<"10s">>,
                redirect_to => <<"srv">>,
                migrate_to => [atom_to_binary(RecipientNode)]
            }
        )
    ),

    DonorNodeBin = atom_to_binary(DonorNode),
    ?assertMatch(
        {ok, 200, #{<<"evacuations">> := [#{<<"node">> := DonorNodeBin}]}},
        api_get(["load_rebalance", "global_status"])
    ).

%% TODO: uncomment after we officially release the feature.
skipped_t_start_purge_validation(Config) ->
    [Node1 | _] = ?config(cluster_nodes, Config),
    Port1 = get_mqtt_port(Node1, tcp),
    BadOpts = [
        #{purge_rate => <<"conn">>},
        #{purge_rate => 0},
        #{purge_rate => -1},
        #{purge_rate => 1.1},
        #{unknown => <<"Value">>}
    ],
    lists:foreach(
        fun(Opts) ->
            ?assertMatch(
                {ok, 400, #{}},
                api_post(
                    ["load_rebalance", atom_to_list(Node1), "purge", "start"],
                    Opts
                ),
                Opts
            )
        end,
        BadOpts
    ),
    ?assertMatch(
        {ok, 404, #{}},
        api_post(
            ["load_rebalance", "bad@node", "purge", "start"],
            #{}
        )
    ),

    process_flag(trap_exit, true),
    Conns = emqtt_connect_many(Port1, 100),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(Node1), "purge", "start"],
            #{purge_rate => 10}
        )
    ),

    Node1Bin = atom_to_binary(Node1),
    ?assertMatch(
        {ok, 200, #{<<"purges">> := [#{<<"node">> := Node1Bin}]}},
        api_get(["load_rebalance", "global_status"])
    ),

    ?assertMatch(
        {ok, 200, #{
            <<"process">> := <<"purge">>,
            <<"purge_rate">> := 10,
            <<"session_goal">> := 0,
            <<"state">> := <<"purging">>,
            <<"stats">> :=
                #{
                    <<"current_sessions">> := _,
                    <<"initial_sessions">> := 100
                }
        }},
        api_get(["load_rebalance", "status"])
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(Node1), "purge", "stop"],
            #{}
        )
    ),

    ok = stop_many(Conns),

    ok.

t_start_rebalance_validation(Config) ->
    process_flag(trap_exit, true),

    [DonorNode, RecipientNode] = ?config(cluster_nodes, Config),
    DonorPort = get_mqtt_port(DonorNode, tcp),

    BadOpts = [
        #{conn_evict_rate => <<"conn">>},
        #{sess_evict_rate => <<"sess">>},
        #{conn_evict_rpc_timeout => <<"conn">>},
        #{sess_evict_rpc_timeout => <<"sess">>},
        #{abs_conn_threshold => <<"act">>},
        #{rel_conn_threshold => <<"rct">>},
        #{abs_sess_threshold => <<"act">>},
        #{rel_sess_threshold => <<"rct">>},
        #{wait_takeover => <<"wait">>},
        #{wait_health_check => <<"wait">>},
        #{nodes => <<"nodes">>},
        #{nodes => []},
        #{nodes => [<<"bad_node">>]},
        #{nodes => [<<"bad_node">>, atom_to_binary(DonorNode)]},
        #{unknown => <<"Value">>}
    ],
    lists:foreach(
        fun(Opts) ->
            ?assertMatch(
                {ok, 400, #{}},
                api_post(
                    ["load_rebalance", atom_to_list(DonorNode), "start"],
                    Opts
                )
            )
        end,
        BadOpts
    ),
    ?assertMatch(
        {ok, 404, #{}},
        api_post(
            ["load_rebalance", "bad@node", "start"],
            #{}
        )
    ),

    Conns = emqtt_connect_many(DonorPort, 50),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "start"],
            #{
                conn_evict_rate => 10,
                conn_evict_rpc_timeout => <<"10s">>,
                sess_evict_rate => 10,
                sess_evict_rpc_timeout => <<"10s">>,
                wait_takeover => <<"10s">>,
                wait_health_check => <<"10s">>,
                abs_conn_threshold => 10,
                rel_conn_threshold => 1.001,
                abs_sess_threshold => 10,
                rel_sess_threshold => 1.001,
                nodes => [
                    atom_to_binary(DonorNode),
                    atom_to_binary(RecipientNode)
                ]
            }
        )
    ),

    DonorNodeBin = atom_to_binary(DonorNode),
    ?assertMatch(
        {ok, 200, #{<<"rebalances">> := [#{<<"node">> := DonorNodeBin}]}},
        api_get(["load_rebalance", "global_status"])
    ),

    ok = stop_many(Conns).

t_start_stop_evacuation(Config) ->
    [DonorNode, RecipientNode] = ?config(cluster_nodes, Config),

    StartOpts = maps:merge(
        maps:get(evacuation, emqx_node_rebalance_api:rebalance_evacuation_example()),
        #{migrate_to => [atom_to_binary(RecipientNode)]}
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "evacuation", "start"],
            StartOpts
        )
    ),

    StatusResponse = api_get(["load_rebalance", "status"]),

    ?assertMatch(
        {ok, 200, _},
        StatusResponse
    ),

    {ok, 200, Status} = StatusResponse,

    ?assertMatch(
        #{
            process := evacuation,
            connection_eviction_rate := 100,
            session_eviction_rate := 100,
            connection_goal := 0,
            session_goal := 0,
            stats := #{
                initial_connected := _,
                current_connected := _,
                initial_sessions := _,
                current_sessions := _
            }
        },
        emqx_node_rebalance_api:translate(local_status_enabled, Status)
    ),

    DonorNodeBin = atom_to_binary(DonorNode),

    GlobalStatusResponse = api_get(["load_rebalance", "global_status"]),

    ?assertMatch(
        {ok, 200, _},
        GlobalStatusResponse
    ),

    {ok, 200, GlobalStatus} = GlobalStatusResponse,

    ?assertMatch(
        #{
            rebalances := [],
            evacuations := [
                #{
                    node := DonorNodeBin,
                    connection_eviction_rate := 100,
                    session_eviction_rate := 100,
                    connection_goal := 0,
                    session_goal := 0,
                    stats := #{
                        initial_connected := _,
                        current_connected := _,
                        initial_sessions := _,
                        current_sessions := _
                    }
                }
            ]
        },
        emqx_node_rebalance_api:translate(global_status, GlobalStatus)
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "evacuation", "stop"],
            #{}
        )
    ),

    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])
    ),

    ?assertMatch(
        {ok, 200, #{<<"evacuations">> := [], <<"rebalances">> := []}},
        api_get(["load_rebalance", "global_status"])
    ).

t_start_stop_rebalance(Config) ->
    process_flag(trap_exit, true),

    [DonorNode, RecipientNode] = ?config(cluster_nodes, Config),
    DonorPort = get_mqtt_port(DonorNode, tcp),

    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])
    ),

    Conns = emqtt_connect_many(DonorPort, 100),

    StartOpts = maps:without(
        [nodes],
        maps:get(rebalance, emqx_node_rebalance_api:rebalance_example())
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "start"],
            StartOpts
        )
    ),

    StatusResponse = api_get(["load_rebalance", "status"]),

    ?assertMatch(
        {ok, 200, _},
        StatusResponse
    ),

    {ok, 200, Status} = StatusResponse,

    ?assertMatch(
        #{process := rebalance, connection_eviction_rate := 10, session_eviction_rate := 20},
        emqx_node_rebalance_api:translate(local_status_enabled, Status)
    ),

    DonorNodeBin = atom_to_binary(DonorNode),
    RecipientNodeBin = atom_to_binary(RecipientNode),

    GlobalStatusResponse = api_get(["load_rebalance", "global_status"]),

    ?assertMatch(
        {ok, 200, _},
        GlobalStatusResponse
    ),

    {ok, 200, GlobalStatus} = GlobalStatusResponse,

    ?assertMatch(
        {ok, 200, #{
            <<"evacuations">> := [],
            <<"rebalances">> :=
                [
                    #{
                        <<"state">> := _,
                        <<"node">> := DonorNodeBin,
                        <<"coordinator_node">> := _,
                        <<"connection_eviction_rate">> := 10,
                        <<"connection_eviction_rpc_timeout">> := 15000,
                        <<"session_eviction_rate">> := 20,
                        <<"session_eviction_rpc_timeout">> := 15000,
                        <<"donors">> := [DonorNodeBin],
                        <<"recipients">> := [RecipientNodeBin]
                    }
                ]
        }},
        GlobalStatusResponse
    ),

    ?assertMatch(
        #{
            evacuations := [],
            rebalances := [
                #{
                    state := _,
                    node := DonorNodeBin,
                    coordinator_node := _,
                    connection_eviction_rate := 10,
                    connection_eviction_rpc_timeout := 15000,
                    session_eviction_rate := 20,
                    session_eviction_rpc_timeout := 15000,
                    donors := [DonorNodeBin],
                    recipients := [RecipientNodeBin]
                }
            ]
        },
        emqx_node_rebalance_api:translate(global_status, GlobalStatus)
    ),

    ?assertMatch(
        {ok, 200, #{}},
        api_post(
            ["load_rebalance", atom_to_list(DonorNode), "stop"],
            #{}
        )
    ),

    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])
    ),

    ?assertMatch(
        {ok, 200, #{<<"evacuations">> := [], <<"rebalances">> := []}},
        api_get(["load_rebalance", "global_status"])
    ),

    ok = stop_many(Conns).

t_availability_check(Config) ->
    [DonorNode | _] = ?config(cluster_nodes, Config),
    ?assertMatch(
        {ok, _},
        api_get_noauth(["load_rebalance", "availability_check"])
    ),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [#{}]),

    ?assertMatch(
        {error, {_, 503, _}},
        api_get_noauth(["load_rebalance", "availability_check"])
    ),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, stop, []),

    ?assertMatch(
        {ok, _},
        api_get_noauth(["load_rebalance", "availability_check"])
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_get_noauth(Path) ->
    request_api(get, uri(Path), emqx_common_test_http:auth_header("invalid", "password")).

api_get(Path) ->
    case request(get, uri(Path)) of
        {ok, Code, ResponseBody} ->
            {ok, Code, jiffy:decode(ResponseBody, [return_maps])};
        {error, _} = Error ->
            Error
    end.

api_post(Path, Data) ->
    case request(post, uri(Path), Data) of
        {ok, Code, ResponseBody} ->
            Res =
                case emqx_utils_json:safe_decode(ResponseBody, [return_maps]) of
                    {ok, Decoded} -> Decoded;
                    {error, _} -> ResponseBody
                end,
            {ok, Code, Res};
        {error, _} = Error ->
            Error
    end.

take_auth_header_from(Node) ->
    meck:new(emqx_common_test_http, [passthrough]),
    meck:expect(
        emqx_common_test_http,
        default_auth_header,
        fun() -> rpc:call(Node, emqx_common_test_http, default_auth_header, []) end
    ),
    ok.

case_specific_data_dir(Case, Config) ->
    case ?config(priv_dir, Config) of
        undefined -> undefined;
        PrivDir -> filename:join(PrivDir, atom_to_list(Case))
    end.

app_specs() ->
    [
        {emqx, #{
            before_start => fun() ->
                emqx_app:set_config_loader(?MODULE)
            end,
            override_env => [{boot_modules, [broker, listeners]}]
        }},
        {emqx_retainer, #{
            config =>
                #{
                    retainer =>
                        #{enable => true}
                }
        }},
        emqx_node_rebalance
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.
