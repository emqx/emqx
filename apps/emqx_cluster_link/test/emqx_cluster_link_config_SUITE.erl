%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_config_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    [{work_dir, emqx_cth_suite:work_dir(Config)} | Config].

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, [{tc_name, TCName} | Config]).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

t_config_update_cli('init', Config) ->
    t_config_update('init', Config);
t_config_update_cli('end', Config) ->
    t_config_update('end', Config).

t_config_update_cli(Config) ->
    test_config_update(cli, Config).

t_config_update('init', Config) ->
    TCName = ?config(tc_name, Config),
    NameA = fmt("~s_~s", [TCName, "a"]),
    NameB = fmt("~s_~s", [TCName, "b"]),
    ClusterA = emqx_cth_cluster:start(mk_cluster(1, NameA, 2, Config)),
    ClusterB = emqx_cth_cluster:start(mk_cluster(2, NameB, 2, Config)),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB}
        | Config
    ];
t_config_update('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_config_update(Config) ->
    test_config_update(api, Config).

test_config_update(Via, Config) ->
    ClusterA = [NodeA | _] = ?config(cluster_a, Config),
    ClusterB = [NodeB | _] = ?config(cluster_b, Config),

    ClientA = start_client("t_config_a", NodeA),
    ClientB = start_client("t_config_b", NodeB),

    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    %% add link
    LinkConfA = mk_link_conf_to(ClusterB, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),
    LinkConfB = mk_link_conf_to(ClusterA, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),

    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"}),
        %% Num nodes = num actors (durable storage is disabled)
        length(ClusterA) + length(ClusterB),
        %% Expected to often take more than 5s.
        %% ClusterB is likely to reject first ClusterA attempt because it doesn't yet
        %% know about ClusterA.
        30_000
    ),
    ?assertMatch({ok, _}, update(Via, NodeA, [LinkConfA], Config)),
    ?assertMatch({ok, _}, update(Via, NodeB, [LinkConfB], Config)),

    ?assertMatch(
        {ok, [#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"} | _]},
        snabbkaffe:receive_events(SubRef)
    ),

    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"hello-from-a">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"hello-from-b">>, qos1),

    ?assertReceive(
        {publish, #{
            topic := <<"t/test-topic">>, payload := <<"hello-from-a">>, client_pid := ClientB
        }},
        5_000
    ),
    ?assertReceive(
        {publish, #{
            topic := <<"t/test/1/1">>, payload := <<"hello-from-b">>, client_pid := ClientA
        }},
        5_000
    ),
    %% no more messages expected
    ?assertNotReceive({publish, _Message = #{}}),

    {ok, SubRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"}),
        length(ClusterA),
        30_000
    ),

    %% update link
    LinkConfA1 = LinkConfA#{<<"pool_size">> => 2, <<"topics">> => [<<"t/new/+">>]},
    ?assertMatch({ok, _}, update(Via, NodeA, [LinkConfA1], Config)),

    ?assertMatch(
        {ok, [#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"} | _]},
        snabbkaffe:receive_events(SubRef1)
    ),

    %% wait for route sync on ClientA node
    {{ok, _, _}, {ok, _}} = ?wait_async_action(
        emqtt:subscribe(ClientA, <<"t/new/1">>, qos1),
        #{?snk_kind := "cluster_link_route_sync_complete", ?snk_meta := #{node := NodeA}},
        10_000
    ),

    %% not expected to be received anymore
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"not-expected-hello-from-b">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"hello-from-b-1">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/new/1">>, payload := <<"hello-from-b-1">>, client_pid := ClientA}},
        5_000
    ),
    ?assertNotReceive({publish, _Message = #{}}),

    %% disable link
    LinkConfA2 = LinkConfA1#{<<"enable">> => false},
    ?assertMatch({ok, _}, update(Via, NodeA, [LinkConfA2], Config)),
    %% must be already blocked by the receiving cluster even if external routing state is not
    %% updated yet
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"not-expected-hello-from-b-1">>, qos1),

    LinkConfB1 = LinkConfB#{<<"enable">> => false},
    ?assertMatch({ok, _}, update(Via, NodeB, [LinkConfB1], Config)),
    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"not-expected-hello-from-a">>, qos1),

    ?assertNotReceive({publish, _Message = #{}}, 3000),

    %% delete links
    ?assertMatch({ok, _}, update(Via, NodeA, [], Config)),
    ?assertMatch({ok, _}, update(Via, NodeB, [], Config)),

    ok = emqtt:stop(ClientA),
    ok = emqtt:stop(ClientB).

update(Via, Node, Links, Config) ->
    case Via of
        api -> update_links_from_api(Node, Links, Config);
        cli -> update_links_from_cli(Node, Links, Config)
    end.

t_config_validations('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    ClusterA = emqx_cth_cluster:start(mk_cluster(1, NameA, 1, Config)),
    ClusterB = emqx_cth_cluster:start(mk_cluster(2, NameB, 1, Config)),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB}
        | Config
    ];
t_config_validations('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_config_validations(Config) ->
    _ClusterA = [NodeA] = ?config(cluster_a, Config),
    ClusterB = ?config(cluster_b, Config),

    LinkConfA = mk_link_conf_to(ClusterB, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),
    DuplicatedLinks = [
        LinkConfA,
        LinkConfA#{<<"enable">> => false, <<"pool_size">> => 2}
    ],
    ?assertMatch(
        {error, #{reason := #{reason := duplicated_cluster_links, duplicates := _}}},
        ?ON(NodeA, emqx_cluster_link_config:update(DuplicatedLinks))
    ),

    InvalidTopics = [<<"t/test/#">>, <<"$LINK/cluster/test/#">>],
    InvalidTopics1 = [<<"t/+/#/+">>, <<>>],
    ?assertMatch(
        {error, #{reason := #{reason := invalid_topics, topics := _}}},
        ?ON(
            NodeA,
            emqx_cluster_link_config:update([LinkConfA#{<<"topics">> => InvalidTopics}])
        )
    ),
    ?assertMatch(
        {error, #{reason := #{reason := invalid_topics, topics := _}}},
        ?ON(
            NodeA,
            emqx_cluster_link_config:update([LinkConfA#{<<"topics">> => InvalidTopics1}])
        )
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        ?ON(NodeA, emqx_cluster_link_config:update([maps:remove(<<"name">>, LinkConfA)]))
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        ?ON(NodeA, emqx_cluster_link_config:update([maps:remove(<<"server">>, LinkConfA)]))
    ),
    ?assertMatch(
        {error, #{reason := required_field}},
        ?ON(NodeA, emqx_cluster_link_config:update([maps:remove(<<"topics">>, LinkConfA)]))
    ),

    %% Some valid changes to cover different update scenarios (msg resource changed, actor changed, both changed)
    ?assertMatch(
        {ok, _},
        ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA]))
    ),
    LinkConfUnknown = LinkConfA#{
        <<"name">> => <<"no-cluster">>,
        <<"server">> => <<"no-cluster.emqx:31883">>
    },
    ?assertMatch(
        {ok, _},
        ?ON(
            NodeA,
            emqx_cluster_link_config:update([LinkConfA#{<<"pool_size">> => 5}, LinkConfUnknown])
        )
    ),

    ?assertMatch(
        {ok, _},
        ?ON(
            NodeA,
            emqx_cluster_link_config:update([LinkConfA, LinkConfUnknown#{<<"topics">> => []}])
        )
    ),

    ?assertMatch(
        {ok, _},
        ?ON(
            NodeA,
            emqx_cluster_link_config:update([
                LinkConfA#{
                    <<"clientid">> => <<"new-client">>,
                    <<"username">> => <<"user">>
                },
                LinkConfUnknown#{
                    <<"clientid">> => <<"new-client">>,
                    <<"username">> => <<"user">>
                }
            ])
        )
    ).

t_config_update_ds('init', _Config) ->
    %% Conf = combine([conf_log(), conf_ds()]),
    %% NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    %% NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    %% NodesA = mk_cluster(1, NameA, [#{role => replicant}, #{role => core}], Conf, Config),
    %% NodesB = mk_cluster(2, NameB, [#{role => replicant}, #{role => core}], Conf, Config),
    %% ClusterA = emqx_cth_cluster:start(NodesA),
    %% ClusterB = emqx_cth_cluster:start(NodesB),
    %% ok = snabbkaffe:start_trace(),
    %% [
    %%     {cluster_a, ClusterA},
    %%     {cluster_b, ClusterB}
    %%     | Config
    %% ];
    %%
    %% At the time of writing (2025-08-21), this test is broken in multiple due to recent
    %% changes in how DS works.
    {skip, broken_test};
t_config_update_ds('end', _Config) ->
    %% ok = snabbkaffe:stop(),
    %% ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    %% ok = emqx_cth_cluster:stop(?config(cluster_b, Config)),
    %% %% @NOTE: Clean work_dir for this TC to avoid running out of disk space
    %% %% causing other test run flaky. Uncomment it if you need to preserve the
    %% %% work_dir for troubleshooting
    %% emqx_cth_suite:clean_work_dir(?config(work_dir, Config)).
    ok.

t_config_update_ds(Config) ->
    %% @NOTE: for troubleshooting this TC,
    %% take a look in end_per_testcase/2 to preserve the work dir
    ClusterA = [NodeA | _] = ?config(cluster_a, Config),
    ClusterB = [NodeB | _] = ?config(cluster_b, Config),

    ClientA = start_client("t_config_a", NodeA, false),
    ClientB = start_client("t_config_b", NodeB, false),
    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    LinkConfA = mk_link_conf_to(ClusterB, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),
    LinkConfB = mk_link_conf_to(ClusterA, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),

    {ok, SubRef} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"}),
        %% 2 cores = 4 actors (durable storage enabled) + 2 replicants = 2 more actors
        6,
        30_000
    ),
    ?assertMatch({ok, _}, ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA]))),
    ?assertMatch({ok, _}, ?ON(NodeB, emqx_cluster_link_config:update([LinkConfB]))),

    ?assertMatch(
        [#{ps_actor_incarnation := 0}], ?ON(NodeA, emqx:get_config([cluster, links]))
    ),
    ?assertMatch(
        [#{ps_actor_incarnation := 0}], ?ON(NodeB, emqx:get_config([cluster, links]))
    ),

    ?assertMatch({ok, [_ | _]}, snabbkaffe:receive_events(SubRef)),

    {ok, _} = emqtt:publish(ClientA, <<"t/test-topic">>, <<"hello-from-a">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"hello-from-b">>, qos1),

    ?assertReceive(
        {publish, #{
            topic := <<"t/test-topic">>, payload := <<"hello-from-a">>, client_pid := ClientB
        }},
        10_000
    ),
    ?assertReceive(
        {publish, #{
            topic := <<"t/test/1/1">>, payload := <<"hello-from-b">>, client_pid := ClientA
        }},
        10_000
    ),
    %% no more messages expected
    ?assertNotReceive({publish, _Message = #{}}),
    {ok, SubRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "cluster_link_routerepl_bootstrap_complete"}),
        %% 2 nodes (1 replicant) in cluster a (3 actors including ds)
        3,
        30_000
    ),

    %% update link
    LinkConfA1 = LinkConfA#{<<"pool_size">> => 2, <<"topics">> => [<<"t/new/+">>]},
    ?assertMatch({ok, _}, ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA1]))),
    ?assertMatch({ok, [_ | _]}, snabbkaffe:receive_events(SubRef1)),

    %% wait for route sync on ClientA node
    {{ok, _, _}, {ok, _}} = ?wait_async_action(
        emqtt:subscribe(ClientA, <<"t/new/1">>, qos1),
        #{
            ?snk_kind := "cluster_link_route_sync_complete",
            ?snk_meta := #{node := NodeA},
            actor := <<"ps-routes-v1">>,
            incarnation := 1
        },
        10_000
    ),
    %% not expected to be received anymore
    {ok, _} = emqtt:publish(ClientB, <<"t/test/1/1">>, <<"not-expected-hello-from-b">>, qos1),
    {ok, _} = emqtt:publish(ClientB, <<"t/new/1">>, <<"hello-from-b-1">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/new/1">>, payload := <<"hello-from-b-1">>, client_pid := ClientA}},
        10_000
    ),
    ?assertNotReceive({publish, _Message = #{}}),

    %% incarnation of ps-actors was updated
    ?assertMatch(
        [#{ps_actor_incarnation := 1}], ?ON(NodeA, emqx:get_config([cluster, links]))
    ),
    ?assertMatch(
        [#{ps_actor_incarnation := 1}], ?ON(NodeA, emqx:get_config([cluster, links]))
    ),

    ok = emqtt:stop(ClientA),
    ok = emqtt:stop(ClientB).

t_misconfigured_links('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    NodesA = mk_cluster(1, NameA, [#{role => replicant}, #{role => core}], Config),
    NodesB = mk_cluster(2, NameB, [#{role => core}], Config),
    ClusterA = emqx_cth_cluster:start(NodesA),
    ClusterB = emqx_cth_cluster:start(NodesB),
    ok = snabbkaffe:start_trace(),
    [
        {cluster_a, ClusterA},
        {cluster_b, ClusterB}
        | Config
    ];
t_misconfigured_links('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

t_misconfigured_links(Config) ->
    ClusterA = [NodeA | _] = ?config(cluster_a, Config),
    ClusterB = [NodeB | _] = ?config(cluster_b, Config),

    ClientA = start_client("t_config_a", NodeA),
    ClientB = start_client("t_config_b", NodeB),
    on_exit(fun() -> catch emqtt:stop(ClientA) end),
    on_exit(fun() -> catch emqtt:stop(ClientB) end),

    {ok, _, _} = emqtt:subscribe(ClientA, <<"t/test/1/+">>, qos1),
    {ok, _, _} = emqtt:subscribe(ClientB, <<"t/test-topic">>, qos1),

    LinkConfA = mk_link_conf_to(ClusterB, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),
    LinkConfB = mk_link_conf_to(ClusterA, #{<<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]}),

    ?assertMatch({ok, _}, ?ON(NodeB, emqx_cluster_link_config:update([LinkConfB]))),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA#{<<"name">> => <<"bad-b-name">>}])),
        #{
            ?snk_kind := "cluster_link_routerepl_handshake_rejected",
            reason := <<"bad_remote_cluster_link_name">>,
            ?snk_meta := #{node := NodeA}
        },
        10_000
    ),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA])),
        #{
            ?snk_kind := "cluster_link_routerepl_bootstrap_complete",
            ?snk_meta := #{node := NodeA}
        },
        10_000
    ),

    ?assertMatch(
        {ok, _},
        ?ON(
            NodeB,
            emqx_cluster_link_config:update([LinkConfB#{<<"enable">> => false}])
        )
    ),

    ?assertMatch({ok, _}, erpc:call(NodeA, emqx_cluster_link_config, update, [[]])),
    {{ok, _}, {ok, _}} = ?wait_async_action(
        ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA])),
        #{
            ?snk_kind := "cluster_link_routerepl_handshake_rejected",
            reason := <<"cluster_link_disabled">>,
            ?snk_meta := #{node := NodeA}
        },
        10_000
    ),

    ?assertMatch(
        {ok, _},
        ?ON(NodeB, emqx_cluster_link_config:update([LinkConfB#{<<"name">> => <<"bad-a-name">>}]))
    ),
    ?assertMatch(
        {ok, _},
        ?ON(NodeA, emqx_cluster_link_config:update([]))
    ),

    {{ok, _}, {ok, _}} = ?wait_async_action(
        ?ON(NodeA, emqx_cluster_link_config:update([LinkConfA])),
        #{
            ?snk_kind := "cluster_link_routerepl_handshake_rejected",
            reason := <<"unknown_cluster">>,
            ?snk_meta := #{node := NodeA}
        },
        10_000
    ).

t_multi_server('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    NameB = fmt("~s_~s", [?FUNCTION_NAME, "b"]),
    ClusterA = emqx_cth_cluster:start(mk_cluster(1, NameA, 1, Config)),
    ClusterB = emqx_cth_cluster:start(mk_cluster(2, NameB, 2, Config)),
    ok = snabbkaffe:start_trace(),
    [{cluster_a, ClusterA}, {cluster_b, ClusterB} | Config];
t_multi_server('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)),
    ok = emqx_cth_cluster:stop(?config(cluster_b, Config)).

-doc """
A link whose `server` lists several addresses uses all of them: a dead address
is skipped in favour of a live one (the route replication client and the first
forwarding client both start with the dead address), and the forwarding clients
spread over the live addresses (the third forwarding client prefers the second
node of the remote cluster).
""".
t_multi_server(Config) ->
    ClusterA = [NodeA] = ?config(cluster_a, Config),
    ClusterB = [NodeB1, NodeB2] = ?config(cluster_b, Config),
    DeadPort = emqx_common_test_helpers:select_free_port(tcp),
    PortB1 = emqx_cluster_link_cth:tcp_port(NodeB1, clink),
    PortB2 = emqx_cluster_link_cth:tcp_port(NodeB2),
    Server = fmt("localhost:~b,localhost:~b,localhost:~b", [DeadPort, PortB1, PortB2]),
    LinkConfA = mk_link_conf_to(ClusterB, #{
        <<"server">> => Server,
        <<"topics">> => [<<"t/#">>],
        <<"pool_size">> => 3
    }),
    LinkConfB = mk_link_conf_to(ClusterA, #{<<"topics">> => [<<"t/#">>]}),
    ?assertMatch({ok, _}, update(api, NodeB1, [LinkConfB], Config)),
    {{ok, _}, {ok, _}} = ?wait_async_action(
        update(api, NodeA, [LinkConfA], Config),
        #{?snk_kind := "cluster_link_routerepl_bootstrap_complete", ?snk_meta := #{node := NodeA}},
        30_000
    ),
    #{<<"name">> := NameB} = LinkConfA,
    ResId = emqx_cluster_link_mqtt:resource_id(NameB),
    ?retry(
        500,
        40,
        ?assertMatch({ok, _, #{status := connected}}, ?ON(NodeA, emqx_resource:get_instance(ResId)))
    ),
    %% Assert both cluster B nodes have at least one connection.
    ?assertMatch([_ | _], ?ON(NodeB1, emqx_cm:all_client_ids())),
    ?assertMatch([_ | _], ?ON(NodeB2, emqx_cm:all_client_ids())),
    %% Cluster B's actors connect back once cluster A knows cluster B.
    lists:foreach(
        fun(NodeB) ->
            ?assertMatch(
                {ok, _},
                ?block_until(
                    #{
                        ?snk_kind := "cluster_link_routerepl_bootstrap_complete",
                        ?snk_meta := #{node := NodeB}
                    },
                    30_000
                )
            )
        end,
        ClusterB
    ),
    ClientA = start_client("t_multi_server_a", NodeA),
    ClientB = start_client("t_multi_server_b", NodeB1),
    on_exit(fun() -> catch emqtt:stop(ClientA) end),
    on_exit(fun() -> catch emqtt:stop(ClientB) end),
    {{ok, _, _}, {ok, _}} = ?wait_async_action(
        emqtt:subscribe(ClientB, <<"t/multi">>, qos1),
        #{?snk_kind := "cluster_link_route_sync_complete", ?snk_meta := #{node := NodeB1}},
        10_000
    ),
    ok = emqx_cth_cluster:sync_routes(ClusterB),
    {ok, _} = emqtt:publish(ClientA, <<"t/multi">>, <<"hello-1">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/multi">>, payload := <<"hello-1">>, client_pid := ClientB}},
        5_000
    ),
    %% The link keeps working when the addresses are reordered.
    Server1 = fmt("localhost:~b,localhost:~b", [PortB2, DeadPort]),
    {{ok, _}, {ok, _}} = ?wait_async_action(
        update(api, NodeA, [LinkConfA#{<<"server">> => Server1}], Config),
        #{?snk_kind := "cluster_link_routerepl_bootstrap_complete", ?snk_meta := #{node := NodeA}},
        30_000
    ),
    ?retry(
        500,
        40,
        ?assertMatch({ok, _, #{status := connected}}, ?ON(NodeA, emqx_resource:get_instance(ResId)))
    ),
    ClientB2 = start_client("t_multi_server_b2", NodeB2),
    on_exit(fun() -> catch emqtt:stop(ClientB2) end),
    {ok, _, _} = emqtt:subscribe(ClientB2, <<"t/multi">>, qos1),
    %% Wait until the route tables inside cluster B converge, so that one
    %% publish reaches the subscribers on both nodes.
    ok = emqx_cth_cluster:sync_routes(ClusterB),
    {ok, _} = emqtt:publish(ClientA, <<"t/multi">>, <<"hello-2">>, qos1),
    ?assertReceive(
        {publish, #{topic := <<"t/multi">>, payload := <<"hello-2">>, client_pid := ClientB}},
        5_000
    ),
    ?assertReceive(
        {publish, #{topic := <<"t/multi">>, payload := <<"hello-2">>, client_pid := ClientB2}},
        5_000
    ).

t_broken_link_crud('init', Config) ->
    NameA = fmt("~s_~s", [?FUNCTION_NAME, "a"]),
    %% All addresses are dead: the link can never connect.
    Server = fmt("localhost:~b,localhost:~b", [
        emqx_common_test_helpers:select_free_port(tcp),
        emqx_common_test_helpers:select_free_port(tcp)
    ]),
    BrokenLink = #{
        <<"name">> => <<"broken">>,
        <<"server">> => Server,
        <<"topics">> => [<<"t/#">>]
    },
    Spec = #{apps => [{emqx_conf, #{config => #{cluster => #{links => [BrokenLink]}}}}]},
    ok = snabbkaffe:start_trace(),
    ClusterA = emqx_cth_cluster:start(mk_cluster(1, NameA, [Spec], Config)),
    [{cluster_a, ClusterA}, {broken_link, BrokenLink} | Config];
t_broken_link_crud('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(cluster_a, Config)).

-doc """
A node starts with a stored link that cannot connect to any of its addresses.
The application starts, the config handler stays registered, and the broken
link can coexist with a new link, be updated, and be deleted.
""".
t_broken_link_crud(Config) ->
    [NodeA] = ?config(cluster_a, Config),
    BrokenLink = #{<<"name">> := Name} = ?config(broken_link, Config),
    ?assertMatch(
        {emqx_cluster_link, _, _},
        lists:keyfind(emqx_cluster_link, 1, ?ON(NodeA, application:which_applications()))
    ),
    ?assertMatch(
        #{handlers := #{cluster := #{links := #{'$mod' := emqx_cluster_link_config}}}},
        ?ON(NodeA, emqx_config_handler:info())
    ),
    %% The route replication actor keeps retrying instead of crashing.
    ?assertMatch(
        {ok, _},
        ?block_until(
            #{
                ?snk_kind := "cluster_link_routerepl_connection_failed",
                ?snk_meta := #{node := NodeA}
            },
            10_000
        )
    ),
    %% A new link can be created while the broken one is present.
    Other = BrokenLink#{<<"name">> => <<"other">>},
    ?assertMatch({ok, _}, ?ON(NodeA, emqx_cluster_link_config:create_link(Other))),
    %% The broken link can be updated and deleted.
    ?assertMatch(
        {ok, _},
        ?ON(NodeA, emqx_cluster_link_config:update_link(BrokenLink#{<<"pool_size">> => 2}))
    ),
    ?assertEqual(ok, ?ON(NodeA, emqx_cluster_link_config:delete_link(Name))),
    ?assertEqual(ok, ?ON(NodeA, emqx_cluster_link_config:delete_link(<<"other">>))),
    ?assertEqual([], ?ON(NodeA, emqx_cluster_link_config:get_links())).

start_client(ClientId, Node) ->
    start_client(ClientId, Node, true).

start_client(ClientId, Node, CleanStart) ->
    Port = emqx_cluster_link_cth:tcp_port(Node),
    {ok, Client} = emqtt:start_link(
        [
            {proto_ver, v5},
            {clientid, ClientId},
            {port, Port},
            {clean_start, CleanStart}
            | [{properties, #{'Session-Expiry-Interval' => 300}} || CleanStart =:= false]
        ]
    ),
    {ok, _} = emqtt:connect(Client),
    Client.

conf_log() ->
    "log.file { enable = true, level = info, path = node.log }".

conf_ds() ->
    "durable_sessions { enable = true } \n"
    "durable_storage.messages.n_shards = 2".

update_links_from_api(Node, Links, _Config) ->
    erpc:call(Node, emqx_cluster_link_config, update, [Links]).

update_links_from_cli(Node, Links, Config) ->
    ConfBin = hocon_pp:do(#{<<"cluster">> => #{<<"links">> => Links}}, #{}),
    ConfFile = prepare_conf_file(?FUNCTION_NAME, ConfBin, Config),
    case erpc:call(Node, emqx_conf_cli, conf, [["load", ConfFile]]) of
        ok -> {ok, Links};
        Error -> Error
    end.

prepare_conf_file(Name, Content, CTConfig) ->
    Filename = tc_conf_file(Name, CTConfig),
    filelib:ensure_dir(Filename),
    ok = file:write_file(Filename, Content),
    Filename.

tc_conf_file(TC, Config) ->
    DataDir = ?config(data_dir, Config),
    filename:join([DataDir, TC, 'emqx.conf']).

%%

mk_cluster(N, ClusterName, Size, CTConfig) ->
    emqx_cluster_link_cth:mk_cluster(N, ClusterName, Size, CTConfig).

mk_link_conf_to(Cluster, Overrides) ->
    emqx_cluster_link_cth:mk_link_conf_to(Cluster, Overrides).

fmt(Fmt, Args) ->
    emqx_utils:format(Fmt, Args).
