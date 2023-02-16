%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(
    emqx_mgmt_api_test_util,
    [
        request/2,
        request/3,
        uri/1
    ]
).

-import(
    emqx_eviction_agent_test_helpers,
    [emqtt_connect_many/2, stop_many/1, case_specific_node_name/3]
).

-define(START_APPS, [emqx_eviction_agent, emqx_node_rebalance]).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    ok = emqx_common_test_helpers:start_apps(?START_APPS),
    Config.

end_per_suite(_Config) ->
    ok = emqx_common_test_helpers:stop_apps(?START_APPS),
    ok.

init_per_testcase(Case, Config) ->
    [{DonorNode, _} | _] =
        ClusterNodes = emqx_eviction_agent_test_helpers:start_cluster(
            [
                {case_specific_node_name(?MODULE, Case, '_donor'), 2883},
                {case_specific_node_name(?MODULE, Case, '_recipient'), 3883}
            ],
            ?START_APPS,
            [{emqx, data_dir, case_specific_data_dir(Case, Config)}]
        ),

    ok = rpc:call(DonorNode, emqx_mgmt_api_test_util, init_suite, []),
    ok = take_auth_header_from(DonorNode),

    [{cluster_nodes, ClusterNodes} | Config].
end_per_testcase(_Case, Config) ->
    _ = emqx_eviction_agent_test_helpers:stop_cluster(
        ?config(cluster_nodes, Config),
        ?START_APPS
    ).

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_start_evacuation_validation(Config) ->
    [{DonorNode, _}, {RecipientNode, _}] = ?config(cluster_nodes, Config),
    BadOpts = [
        #{conn_evict_rate => <<"conn">>},
        #{sess_evict_rate => <<"sess">>},
        #{redirect_to => 123},
        #{wait_takeover => <<"wait">>},
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
                )
            )
        end,
        BadOpts
    ),
    ?assertMatch(
        {ok, 400, #{}},
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
                wait_takeover => 10,
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

t_start_rebalance_validation(Config) ->
    process_flag(trap_exit, true),

    [{DonorNode, DonorPort}, {RecipientNode, _}] = ?config(cluster_nodes, Config),

    BadOpts = [
        #{conn_evict_rate => <<"conn">>},
        #{sess_evict_rate => <<"sess">>},
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
        {ok, 400, #{}},
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
                sess_evict_rate => 10,
                wait_takeover => 10,
                wait_health_check => 10,
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
    [{DonorNode, _}, {RecipientNode, _}] = ?config(cluster_nodes, Config),

    StartOpts = maps:merge(
        emqx_node_rebalance_api:rebalance_evacuation_example(),
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

    [{DonorNode, DonorPort}, {RecipientNode, _}] = ?config(cluster_nodes, Config),

    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])
    ),

    Conns = emqtt_connect_many(DonorPort, 100),

    StartOpts = maps:without(
        [nodes],
        emqx_node_rebalance_api:rebalance_example()
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
                        <<"session_eviction_rate">> := 20,
                        <<"donors">> := [DonorNodeBin],
                        <<"recipients">> := [RecipientNodeBin]
                    }
                ]
        }},
        api_get(["load_rebalance", "global_status"])
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
                    session_eviction_rate := 20,
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
    [{DonorNode, _} | _] = ?config(cluster_nodes, Config),
    ?assertMatch(
        {ok, 200, #{}},
        api_get(["load_rebalance", "availability_check"])
    ),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, start, [#{}]),

    ?assertMatch(
        {ok, 503, _},
        api_get(["load_rebalance", "availability_check"])
    ),

    ok = rpc:call(DonorNode, emqx_node_rebalance_evacuation, stop, []),

    ?assertMatch(
        {ok, 200, #{}},
        api_get(["load_rebalance", "availability_check"])
    ).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

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
            {ok, Code, jiffy:decode(ResponseBody, [return_maps])};
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
