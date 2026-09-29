%%--------------------------------------------------------------------
%% Copyright (c) 2024-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%

suite() -> [{timetrap, {minutes, 1}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

%%

mk_interlinked_clusters(BaseName, NSource, NTarget, Config) ->
    mk_interlinked_clusters(BaseName, NSource, NTarget, #{}, Config).

mk_interlinked_clusters(BaseName, NSource, NTarget, LinkConf, Config) ->
    SourceName = fmt("~p_~p", [BaseName, s]),
    TargetName = fmt("~p_~p", [BaseName, t]),
    SourceLinkConf = LinkConf#{<<"topics">> => []},
    SourceLink = emqx_cluster_link_cth:mk_link_conf(2, TargetName, SourceLinkConf),
    TargetLinkConf = LinkConf#{<<"topics">> => [<<"#">>]},
    TargetLink = emqx_cluster_link_cth:mk_link_conf(1, SourceName, TargetLinkConf),
    SourceSpec = #{
        apps => [
            {emqx_conf, merge_conf(#{cluster => #{links => [SourceLink]}}, conf_log())}
        ]
    },
    TargetSpec = #{
        apps => [
            {emqx_conf, merge_conf(#{cluster => #{links => [TargetLink]}}, conf_log())}
        ]
    },
    {
        emqx_cluster_link_cth:mk_cluster(1, SourceName, {NSource, SourceSpec}, Config),
        emqx_cluster_link_cth:mk_cluster(2, TargetName, {NTarget, TargetSpec}, Config)
    }.

merge_conf(C1, C2) ->
    emqx_cth_suite:merge_config(C1, C2).

conf_log() ->
    "log.file { enable = true, level = info, path = node.log }".

%% Link configuration for the cases that exercise a sustained outage: a single pool worker
%% with a long `request_ttl', so that everything published while the link is unusable has to
%% be buffered instead of being dropped.
outage_link_conf() ->
    #{
        <<"pool_size">> => 1,
        <<"resource_opts">> => #{
            <<"worker_pool_size">> => 1,
            <<"inflight_window">> => 10,
            <<"max_buffer_bytes">> => <<"10MB">>,
            <<"request_ttl">> => <<"2m">>,
            <<"resume_interval">> => <<"1s">>,
            <<"health_check_interval">> => <<"1s">>
        }
    }.

wait_link_online_on(Nodes) ->
    wait_link_online_on(Nodes, 5000).

wait_link_online_on(Nodes, Timeout) ->
    lists:foreach(
        fun(N) ->
            {ok, _} = ?block_until(
                #{?snk_kind := "cluster_link_routerepl_online", ?snk_meta := #{node := N}},
                Timeout
            )
        end,
        Nodes
    ).

%%

nodes_all(Config) ->
    nodes_source(Config) ++ nodes_target(Config).

nodes_source(Config) ->
    ?config(source_nodes, Config).

nodes_target(Config) ->
    ?config(target_nodes, Config).

%%

%% Verifies that basic message forwarding across cluster links works.
t_message_forwarding('init', Config) ->
    test_message_forwarding_init(?FUNCTION_NAME, Config);
t_message_forwarding('end', Config) ->
    test_message_forwarding_end(?FUNCTION_NAME, Config).

t_message_forwarding(Config) ->
    test_message_forwarding(regular, Config).

%% Verifies that basic message forwarding across cluster links works with "random"
%% dispatch strategy.
t_message_forwarding_random('init', Config) ->
    ok = snabbkaffe:start_trace(),
    LinkConf = #{
        <<"resource_opts">> => #{
            <<"dispatch_strategy">> => <<"random">>
        }
    },
    {SourceClusterSpec, TargetClusterSpec} =
        mk_interlinked_clusters(?FUNCTION_NAME, 2, 2, LinkConf, Config),
    SourceNodes = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = emqx_cth_cluster:start(TargetClusterSpec),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_message_forwarding_random('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_message_forwarding_random(Config) ->
    test_message_forwarding(regular, Config).

%% Verifies that basic message forwarding across cluster links works for shared
%% subscribers.
t_message_forwarding_sharesub('init', Config) ->
    test_message_forwarding_init(?FUNCTION_NAME, Config);
t_message_forwarding_sharesub('end', Config) ->
    test_message_forwarding_end(?FUNCTION_NAME, Config).

t_message_forwarding_sharesub(Config) ->
    test_message_forwarding(shared, Config).

test_message_forwarding_init(TCName, Config) ->
    ok = snabbkaffe:start_trace(),
    {SourceClusterSpec, TargetClusterSpec} = mk_interlinked_clusters(TCName, 2, 2, Config),
    SourceNodes = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = emqx_cth_cluster:start(TargetClusterSpec),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ].

test_message_forwarding_end(_, Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

test_message_forwarding(SubType, Config) ->
    [SourceNode1 | _] = SourceNodes = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = TargetNodes = nodes_target(Config),
    %% Connect client to the target cluster.
    TargetC1 = emqx_cluster_link_cth:connect_client("t_message_forwarding1", TargetNode1),
    TargetC2 = emqx_cluster_link_cth:connect_client("t_message_forwarding2", TargetNode2),
    %% Connect a client to the source cluster.
    SourceC1 = emqx_cluster_link_cth:connect_client("t_message_forwarding", SourceNode1),
    ?check_trace(
        #{timetrap => 30_000},
        try
            %% Subscribe both to different topics.
            T11 = mk_topic(SubType, <<"t1/+">>),
            T12 = mk_topic(SubType, <<"t1/#">>),
            {ok, _, _} = emqtt:subscribe(TargetC1, T11, qos1),
            {ok, _, _} = emqtt:subscribe(TargetC2, T12, qos1),
            emqx_cth_cluster:sync_routes(SourceNodes, 10_000),
            emqx_cth_cluster:sync_routes(TargetNodes, 10_000),
            %% Start cluster link, existing routes should be replicated.
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete",
                ?snk_meta := #{node := TargetNode1}
            }),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete",
                ?snk_meta := #{node := TargetNode2}
            }),
            %% Publish a message to the source cluster.
            {ok, _} = emqtt:publish(SourceC1, <<"t1/42">>, <<"hello">>, qos1),
            ?assertReceive(
                {publish, #{topic := <<"t1/42">>, payload := <<"hello">>, client_pid := TargetC1}}
            ),
            ?assertReceive(
                {publish, #{topic := <<"t1/42">>, payload := <<"hello">>, client_pid := TargetC2}}
            ),
            ?assertNotReceive(
                {publish, #{}}
            ),
            %% Subscribe both clients to another pair of topics while cluster link is active.
            T21 = mk_topic(SubType, <<"t2/#">>),
            T22 = mk_topic(SubType, <<"t2/+/+">>),
            ?wait_async_action(
                begin
                    {ok, _, _} = emqtt:subscribe(TargetC1, T21, qos1),
                    {ok, _, _} = emqtt:subscribe(TargetC2, T22, qos1)
                end,
                #{?snk_kind := "cluster_link_route_sync_complete"}
            ),
            emqx_cth_cluster:sync_routes(SourceNodes),
            emqx_cth_cluster:sync_routes(TargetNodes),
            %% Publish another message.
            {ok, _} = emqtt:publish(SourceC1, <<"t2/3/4">>, <<"heh">>, qos1),
            ?assertReceive(
                {publish, #{topic := <<"t2/3/4">>, payload := <<"heh">>, client_pid := TargetC1}}
            ),
            ?assertReceive(
                {publish, #{topic := <<"t2/3/4">>, payload := <<"heh">>, client_pid := TargetC2}}
            ),
            ?assertNotReceive(
                {publish, #{}}
            )
        after
            %% Stop clients.
            ok = emqtt:stop(SourceC1),
            ok = emqtt:stop(TargetC1),
            ok = emqtt:stop(TargetC2)
        end,
        []
    ).

t_message_forwarding_ordering('init', Config) ->
    ok = snabbkaffe:start_trace(),
    {SourceClusterSpec, TargetClusterSpec} = mk_interlinked_clusters(?FUNCTION_NAME, 1, 1, Config),
    SourceNodes = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = emqx_cth_cluster:start(TargetClusterSpec),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_message_forwarding_ordering('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

%% Verifies message forwarding with default "clientid" dispatch strategy preserves per-client
%% publishing order.
t_message_forwarding_ordering(Config) ->
    NClients = 10,
    NMessages = 10,
    [SourceNode | _] = nodes_source(Config),
    [TargetNode | _] = nodes_target(Config),
    %% Connect client to the target cluster.
    TargetC1 = emqx_cluster_link_cth:connect_client("t_message_forwarding1", TargetNode),
    %% Connect clients to the source cluster.
    CNs = lists:seq(1, NClients),
    SourceClients = [
        emqx_cluster_link_cth:connect_client("t_message_forwarding" ++ CN, SourceNode)
     || CN <- lists:map(fun integer_to_list/1, CNs)
    ],
    ?check_trace(
        #{timetrap => 30_000},
        try
            %% Subscribe to a wildcard topic.
            {ok, _, _} = emqtt:subscribe(TargetC1, <<"t/#">>, qos1),
            %% Wait while existing routes are replicated.
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete",
                ?snk_meta := #{node := TargetNode}
            }),
            %% Publish bunch a message to the source cluster concurrently.
            %% * Use unique topic for each message.
            %% * Keep publishing order reflected in the payload.
            SourcePublishes = [
                [
                    {Topic, {CN, MN}}
                 || MN <- lists:seq(1, NMessages),
                    Topic <- [emqx_topic:join(["t", integer_to_list(CN), integer_to_list(MN)])]
                ]
             || CN <- CNs
            ],
            WorkerFn = fun
                WorkerFn(SourceClient, [{Topic, Marker} | Rest]) ->
                    {ok, _} = emqtt:publish(SourceClient, Topic, term_to_binary(Marker), ?QOS_1),
                    WorkerFn(SourceClient, Rest);
                WorkerFn(_, []) ->
                    ok
            end,
            _Workers = [
                erlang:spawn(fun() -> WorkerFn(Client, Publishes) end)
             || {Client, Publishes} <- lists:zip(SourceClients, SourcePublishes)
            ],
            %% Receive publishes from the target cluster.
            TargetPublishes = [
                {Topic, binary_to_term(Payload)}
             || {publish, #{topic := Topic, payload := Payload}} <- ?drainMailbox(2_500)
            ],
            %% Verify per-client publishing order is preserved.
            ?assertEqual(
                SourcePublishes,
                [[TP || TP = {_Topic, {CN0, _MN}} <- TargetPublishes, CN =:= CN0] || CN <- CNs]
            )
        after
            %% Stop clients.
            lists:foreach(fun emqtt:stop/1, [TargetC1 | SourceClients])
        end,
        []
    ).

%% Allow enough time for a sustained network outage and for the buffered
%% messages to drain afterwards.
t_message_forwarding_adverse_network_conditions() ->
    [{timetrap, {minutes, 2}}].

t_message_forwarding_adverse_network_conditions('init', Config) ->
    ok = snabbkaffe:start_trace(),
    LinkConf = outage_link_conf(),
    {SourceClusterSpec, TargetClusterSpec} =
        mk_interlinked_clusters(?FUNCTION_NAME, 1, 1, LinkConf, Config),
    SourceNodes = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = emqx_cth_cluster:start(TargetClusterSpec),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_message_forwarding_adverse_network_conditions('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

%% Verifies that the message forwarding resource buffers messages during a
%% network outage and delivers every buffered message after recovery.
t_message_forwarding_adverse_network_conditions(Config) ->
    [SourceNode] = nodes_source(Config),
    [TargetNode] = nodes_target(Config),
    TargetName = atom_to_binary(emqx_cluster_link_cth:cluster_name(TargetNode)),
    ResourceId = emqx_cluster_link_mqtt:resource_id(TargetName),
    Topic = <<"t/adverse-network">>,
    TargetClient = emqx_cluster_link_cth:connect_client(
        "t_message_forwarding_adverse_network_conditions_target",
        TargetNode
    ),
    ?check_trace(
        #{timetrap => 90_000},
        try
            %% 1. Subscribe to the topic on the target cluser, wait for route replication.
            {ok, _, _} = emqtt:subscribe(TargetClient, Topic, qos1),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete",
                ?snk_meta := #{node := TargetNode}
            }),
            %% 2. Start async publisher to the source cluster.
            Publisher = start_async_publisher(SourceNode, Topic, 10),
            %% 3. Keep the link running smoothly for 2.5 seconds.
            ct:sleep(2_500),
            %% 4. Sever the link connection, wait the source cluster notices.
            ok = ?ON(TargetNode, emqx_listeners:stop_listener('tcp:clink')),
            wait_forwarding_resource_disconnected(SourceNode, ResourceId),
            %% 5. Keep the link down for 10 seconds:
            ct:sleep(10_000),
            %% 6. Restore the connectivity, wait the source cluster notices.
            ok = ?ON(TargetNode, emqx_listeners:start_listener('tcp:clink')),
            wait_forwarding_resource_connected(SourceNode, ResourceId),
            %% 7. Keep the link running smoothly for 2.5 seconds more.
            ct:sleep(2_500),
            %% 8. Stop async publisher, verify each published message was delivered to the
            %%    target cluster.
            NPublished = stop_async_publisher(Publisher),
            Publishes = [
                M
             || {publish, M} <- ?drainMailbox(5_000),
                maps:get(client_pid, M) =:= TargetClient,
                maps:get(topic, M) =:= Topic
            ],
            Received = [binary_to_integer(Payload) || #{payload := Payload} <- Publishes],
            Expected = lists:seq(1, NPublished),
            ?assertEqual([], Expected -- Received, #{received => Received}),
            ?assertEqual([], Received -- Expected, #{received => Received}),
            %% 9. Verify metrics reflect full recovery:
            ?assertMatch(
                #{
                    counters := #{
                        dropped := 0,
                        failed := 0,
                        retried := Retried,
                        success := NPublished
                    },
                    gauges := #{
                        inflight := 0,
                        queuing := 0
                    }
                } when Retried > 0,
                ?ON(SourceNode, emqx_resource:get_metrics(ResourceId))
            )
        after
            ok = emqtt:stop(TargetClient)
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(buffer_worker_appended_to_queue, Trace)),
            ?assertMatch([_ | _], ?of_kind(buffer_worker_retry_inflight_succeeded, Trace))
        end
    ).

wait_forwarding_resource_disconnected(SourceNode, ResourceId) ->
    ?retry(
        500,
        20,
        ?assertMatch(
            {ok, _, #{status := Status}} when Status =/= connected,
            ?ON(SourceNode, emqx_resource:get_instance(ResourceId))
        )
    ).

wait_forwarding_resource_connected(SourceNode, ResourceId) ->
    ?retry(
        500,
        20,
        ?assertMatch(
            {ok, _, #{status := connected}},
            ?ON(SourceNode, emqx_resource:get_instance(ResourceId))
        )
    ).

%% Verifies that the message forwarding health check probes the remote broker.  The
%% link is routed through a relay that the test freezes: the TCP connection stays open
%% but nothing gets through it anymore, which is what a black-holed network looks like.
%% The resource must stop being reported as connected within a couple of health check
%% intervals, raise the alarm, and keep the messages published meanwhile until the
%% remote answers again.
t_message_forwarding_health_check_silent_remote() ->
    [{timetrap, {minutes, 2}}].

t_message_forwarding_health_check_silent_remote('init', Config) ->
    ok = snabbkaffe:start_trace(),
    SourceName = fmt("~p_s", [?FUNCTION_NAME]),
    TargetName = fmt("~p_t", [?FUNCTION_NAME]),
    LinkConf = outage_link_conf(),
    %% Target cluster: replicates its routes to the source cluster so that the source
    %% knows where to forward, and does so directly (the relay only sits in front of the
    %% other direction).
    TargetLink = emqx_cluster_link_cth:mk_link_conf(1, SourceName, LinkConf#{
        <<"topics">> => [<<"#">>]
    }),
    TargetSpec = #{
        apps => [
            {emqx_conf, merge_conf(#{cluster => #{links => [TargetLink]}}, conf_log())}
        ]
    },
    TargetNodes = emqx_cth_cluster:start(
        emqx_cluster_link_cth:mk_cluster(2, TargetName, {1, TargetSpec}, Config)
    ),
    [TargetNode] = TargetNodes,
    TargetPort = emqx_cluster_link_cth:tcp_port(TargetNode, clink, undefined),
    {RelayPort, Relay} = emqx_cluster_link_cth:start_relay(TargetPort),
    %% Source cluster: its link to the target goes through the relay.
    SourceLink = emqx_cluster_link_cth:mk_link_conf(2, TargetName, LinkConf#{
        <<"topics">> => [],
        <<"server">> => fmt("localhost:~b", [RelayPort])
    }),
    SourceSpec = #{
        apps => [
            {emqx_conf, merge_conf(#{cluster => #{links => [SourceLink]}}, conf_log())}
        ]
    },
    SourceNodes = emqx_cth_cluster:start(
        emqx_cluster_link_cth:mk_cluster(1, SourceName, {1, SourceSpec}, Config)
    ),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes, 30_000),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes},
        {relay, Relay}
        | Config
    ];
t_message_forwarding_health_check_silent_remote('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cluster_link_cth:stop_relay(?config(relay, Config)),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_message_forwarding_health_check_silent_remote(Config) ->
    [SourceNode] = nodes_source(Config),
    [TargetNode] = nodes_target(Config),
    Relay = ?config(relay, Config),
    TargetName = atom_to_binary(emqx_cluster_link_cth:cluster_name(TargetNode)),
    ResourceId = emqx_cluster_link_mqtt:resource_id(TargetName),
    Topic = <<"t/silent-remote">>,
    TargetClient = emqx_cluster_link_cth:connect_client(
        "t_silent_remote_target", TargetNode
    ),
    SourceClient = emqx_cluster_link_cth:connect_client(
        "t_silent_remote_source", SourceNode
    ),
    try
        %% 1. A link that answers the probe is reported as connected, and messages flow.
        {ok, _, _} = emqtt:subscribe(TargetClient, Topic, qos1),
        {ok, _} = ?block_until(
            #{?snk_kind := "cluster_link_route_sync_complete", ?snk_meta := #{node := TargetNode}},
            10_000
        ),
        ?retry(
            200, 25, ?assertEqual(connected, forwarding_resource_status(SourceNode, ResourceId))
        ),
        {ok, _} = emqtt:publish(SourceClient, Topic, <<"before">>, qos1),
        ?assertReceive(
            {publish, #{topic := Topic, payload := <<"before">>, client_pid := TargetClient}}
        ),

        %% 2. The remote end stops forwarding traffic in both directions, keeping the
        %%    connection open: the probe must notice within a couple of intervals.
        ok = emqx_cluster_link_cth:freeze_relay(Relay),
        ?retry(
            200,
            25,
            ?assertEqual(connecting, forwarding_resource_status(SourceNode, ResourceId))
        ),
        ?retry(200, 25, ?assert(is_forwarding_resource_down(SourceNode, ResourceId))),

        %% 3. Messages published while the link is unusable must stay queued: they may
        %%    not be counted as failed nor dropped, and the target must not see them.
        {ok, _} = emqtt:publish(SourceClient, Topic, <<"during_hang">>, qos1),
        ?retry(
            200,
            15,
            ?assertMatch(
                #{counters := #{dropped := 0, failed := 0}},
                ?ON(SourceNode, emqx_resource:get_metrics(ResourceId))
            )
        ),
        ct:sleep(2_000),
        ?assertNotReceive({publish, #{topic := Topic, payload := <<"during_hang">>}}),

        %% 4. Once the remote answers again the link must recover, and the message queued
        %%    while it was silent must be delivered rather than dropped.
        ok = emqx_cluster_link_cth:thaw_relay(Relay),
        ?retry(
            200,
            25,
            ?assertEqual(connected, forwarding_resource_status(SourceNode, ResourceId))
        ),
        ?assertReceive(
            {publish, #{
                topic := Topic, payload := <<"during_hang">>, client_pid := TargetClient
            }},
            15_000
        ),
        ?retry(
            200,
            25,
            %% Both messages made it through, and the worker holds nothing back anymore.
            %% How a send is accounted for is not the point here: a query that went
            %% through the buffer retry path is counted as a retried success.
            ?assertMatch(
                #{
                    counters := #{dropped := 0, failed := 0},
                    gauges := #{inflight := 0, queuing := 0}
                },
                ?ON(SourceNode, emqx_resource:get_metrics(ResourceId))
            )
        )
    after
        ok = emqx_cluster_link_cth:thaw_relay(Relay),
        ok = emqtt:stop(TargetClient),
        ok = emqtt:stop(SourceClient)
    end.

forwarding_resource_status(Node, ResourceId) ->
    {ok, _, #{status := Status}} = ?ON(Node, emqx_resource:get_instance(ResourceId)),
    Status.

is_forwarding_resource_down(Node, ResourceId) ->
    lists:any(
        fun
            (#{details := #{resource_id := RId, reason := resource_down}}) ->
                RId =:= ResourceId;
            (_) ->
                false
        end,
        ?ON(Node, emqx_alarm:get_alarms(activated))
    ).

start_async_publisher(SourceNode, Topic, Interval) ->
    spawn_link(SourceNode, ?MODULE, async_publisher_loop, [Topic, 1, Interval, self()]).

stop_async_publisher(Publisher) ->
    Publisher ! {self(), stop},
    receive
        {Publisher, NPublished} ->
            NPublished
    after 5_000 ->
        ct:fail("publisher ~p did not stop", [Publisher])
    end.

async_publisher_loop(Topic, N, Interval, Controller) ->
    Message = emqx_message:make(?MODULE, ?QOS_1, Topic, integer_to_binary(N)),
    _ = emqx:publish(Message),
    receive
        {Controller, stop} ->
            Controller ! {self(), N}
    after Interval ->
        async_publisher_loop(Topic, N + 1, Interval, Controller)
    end.

t_message_forwarding_disconnect_reason_reported('init', Config) ->
    ok = snabbkaffe:start_trace(),
    SourceName = fmt("~p_s", [?FUNCTION_NAME]),
    TargetName = fmt("~p_t", [?FUNCTION_NAME]),
    TargetSpec = emqx_cluster_link_cth:mk_cluster(2, TargetName, 1, Config),
    TargetNodes = emqx_cth_cluster:start(TargetSpec),
    SourceLink = emqx_cluster_link_cth:mk_link_conf_to(TargetNodes, #{
        <<"topics">> => [],
        <<"resource_opts">> => #{<<"health_check_interval">> => 100}
    }),
    SourceNodespec = #{
        apps => [
            {emqx_conf, merge_conf(#{cluster => #{links => [SourceLink]}}, conf_log())}
        ]
    },
    SourceSpec = emqx_cluster_link_cth:mk_cluster(1, SourceName, [SourceNodespec], Config),
    SourceNodes = emqx_cth_cluster:start(SourceSpec),
    [
        {target_name, TargetName},
        {source_name, SourceName},
        {target_nodes, TargetNodes},
        {source_nodes, SourceNodes}
        | Config
    ];
t_message_forwarding_disconnect_reason_reported('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_message_forwarding_disconnect_reason_reported(Config) ->
    TargetName = ?config(target_name, Config),
    [TargetNode] = ?config(target_nodes, Config),
    [SourceNode] = ?config(source_nodes, Config),
    %% 1. Wait and verify message forwarding resource is connected.
    ResourceId = emqx_cluster_link_mqtt:resource_id(TargetName),
    ?retry(
        500,
        10,
        ?assertMatch(
            {ok, _, #{status := connected}},
            ?ON(SourceNode, emqx_resource:get_instance(ResourceId))
        )
    ),
    %% 2. Repeatedly kick clients after the MQTT resource is up.
    %% Disconnect reason should be reported through the emqtt disconnect handler.
    _Kicker = ?ON(
        TargetNode,
        erlang:spawn(fun Loop() ->
            lists:foreach(fun emqx_cm:kick_session/1, emqx_cm:all_client_ids()),
            timer:sleep(100),
            Loop()
        end)
    ),
    %% 3. Verify reason is propagated to the resource status.
    ?retry(
        500,
        5,
        ?assertMatch(
            {ok, _, #{
                status := disconnected,
                error := #{
                    cause := broker_disconnect,
                    reason := administrative_action,
                    reason_code := ?RC_ADMINISTRATIVE_ACTION
                }
            }},
            ?ON(SourceNode, emqx_resource:get_instance(ResourceId))
        )
    ),
    %% 4. Verify reason is propagated to the alarm details.
    ?retry(
        500,
        5,
        begin
            Alarms = ?ON(SourceNode, emqx_alarm:get_alarms(activated)),
            ResourceAlarms = [
                Alarm
             || Alarm = #{details := #{resource_id := RId, reason := resource_down}} <- Alarms,
                RId =:= ResourceId
            ],
            ?assertEqual(
                [
                    Alarm
                 || Alarm = #{message := Message} <- ResourceAlarms,
                    binary:match(Message, <<"broker_disconnect">>) =/= nomatch,
                    binary:match(Message, <<"administrative_action">>) =/= nomatch
                ],
                ResourceAlarms
            )
        end
    ).

t_target_extrouting_gc('init', Config) ->
    ok = snabbkaffe:start_trace(),
    {SourceClusterSpec, TargetClusterSpec} = mk_interlinked_clusters(?FUNCTION_NAME, 1, 2, Config),
    SourceNodes = emqx_cth_cluster:start(SourceClusterSpec),
    TargetNodes = emqx_cth_cluster:start(TargetClusterSpec),
    ok = wait_link_online_on(SourceNodes ++ TargetNodes),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_target_extrouting_gc('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_target_extrouting_gc(Config) ->
    [SourceNode1 | _] = SourceNodes = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = TargetNodes = nodes_target(Config),
    TargetName = atom_to_binary(emqx_cluster_link_cth:cluster_name(TargetNode1)),
    SourceC1 = emqx_cluster_link_cth:connect_client("t_target_extrouting_gc", SourceNode1),
    TargetC1 = emqx_cluster_link_cth:connect_client_unlink("t_target_extrouting_gc1", TargetNode1),
    TargetC2 = emqx_cluster_link_cth:connect_client_unlink("t_target_extrouting_gc2", TargetNode2),
    ?check_trace(
        #{timetrap => 30_000},
        begin
            TopicFilter1 = <<"t/+">>,
            TopicFilter2 = <<"t/#">>,
            {ok, _, _} = emqtt:subscribe(TargetC1, TopicFilter1, qos1),
            {ok, _, _} = emqtt:subscribe(TargetC2, TopicFilter2, qos1),
            emqx_cth_cluster:sync_routes(SourceNodes, 10_000),
            emqx_cth_cluster:sync_routes(TargetNodes, 10_000),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete", ?snk_meta := #{node := TargetNode1}
            }),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_route_sync_complete", ?snk_meta := #{node := TargetNode2}
            }),
            {ok, _} = emqtt:publish(SourceC1, <<"t/1">>, <<"HELLO1">>, qos1),
            {ok, _} = emqtt:publish(SourceC1, <<"t/2/ext">>, <<"HELLO2">>, qos1),
            {ok, _} = emqtt:publish(SourceC1, <<"t/3/ext">>, <<"HELLO3">>, qos1),
            Pubs1 = [M || {publish, M} <- ?drainMailbox(1_000)],
            %% We switch off `TargetNode2' first.  Since `TargetNode1' is the sole endpoint
            %% configured in Target Cluster, the link will keep working (i.e., CL MQTT ecpool
            %% workers will stay connected).  If we turned `TargetNode1' first, then the link
            %% would stay down and stop replicating messages.
            {ok, _} = ?wait_async_action(
                emqx_cth_cluster:stop_node(TargetNode2),
                #{?snk_kind := "cluster_link_extrouter_actor_cleaned", cluster := TargetName}
            ),
            {ok, _} = emqtt:publish(SourceC1, <<"t/4/ext">>, <<"HELLO4">>, qos1),
            {ok, _} = emqtt:publish(SourceC1, <<"t/5">>, <<"HELLO5">>, qos1),
            Pubs2 = [M || {publish, M} <- ?drainMailbox(1_000)],
            {ok, _} = ?wait_async_action(
                emqx_cth_cluster:stop_node(TargetNode1),
                #{?snk_kind := "cluster_link_extrouter_actor_cleaned", cluster := TargetName}
            ),
            ok = emqtt:stop(SourceC1),
            %% Verify that extrouter table eventually becomes empty.
            ?assertEqual(
                [],
                erpc:call(SourceNode1, emqx_cluster_link_extrouter, topics, []),
                {
                    erpc:call(SourceNode1, ets, tab2list, [emqx_external_router_actor]),
                    erpc:call(SourceNode1, ets, tab2list, [emqx_external_router_route])
                }
            ),
            %% Verify all relevant messages were forwarded.
            ?assertMatch(
                [
                    #{topic := <<"t/1">>, payload := <<"HELLO1">>, client_pid := _C1},
                    #{topic := <<"t/1">>, payload := <<"HELLO1">>, client_pid := _C2},
                    #{topic := <<"t/2/ext">>, payload := <<"HELLO2">>},
                    #{topic := <<"t/3/ext">>, payload := <<"HELLO3">>},
                    %% We expect only `HELLO5' and not `HELLO4' to be here because the former was
                    %% published while only `TargetNode1' was alive, and this node held only the
                    %% `t/+' subscription at that time.
                    #{topic := <<"t/5">>, payload := <<"HELLO5">>}
                ],
                lists:sort(emqx_utils_maps:key_comparer(topic), Pubs1 ++ Pubs2)
            )
        end,
        fun(Trace) ->
            %% Verify there was no unnecessary forwarding.
            ?assertMatch(
                [
                    #{message := #message{topic = <<"t/1">>, payload = <<"HELLO1">>}},
                    #{message := #message{topic = <<"t/2/ext">>, payload = <<"HELLO2">>}},
                    #{message := #message{topic = <<"t/3/ext">>, payload = <<"HELLO3">>}},
                    #{message := #message{topic = <<"t/5">>, payload = <<"HELLO5">>}}
                ],
                lists:sort(
                    emqx_utils_maps:key_comparer(topic),
                    ?of_kind("cluster_link_message_forwarded", Trace)
                )
            )
        end
    ).

%%

mk_topic(regular, Topic) ->
    Topic;
mk_topic(shared, Topic) ->
    <<"$share/test-group/", Topic/binary>>.

fmt(Fmt, Args) ->
    emqx_utils:format(Fmt, Args).
