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
    SourceName = fmt("~p_~p", [BaseName, s]),
    TargetName = fmt("~p_~p", [BaseName, t]),
    SourceLink = emqx_cluster_link_cth:mk_link_conf(2, TargetName, #{<<"topics">> => []}),
    TargetLink = emqx_cluster_link_cth:mk_link_conf(1, SourceName, #{<<"topics">> => [<<"#">>]}),
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

wait_link_online_on(Nodes) ->
    lists:foreach(
        fun(N) ->
            {ok, _} = ?block_until(
                #{?snk_kind := "cluster_link_routerepl_online", ?snk_meta := #{node := N}},
                5000
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

t_message_forwarding('init', Config) ->
    test_message_forwarding_init(?FUNCTION_NAME, Config);
t_message_forwarding('end', Config) ->
    test_message_forwarding_end(?FUNCTION_NAME, Config).

t_message_forwarding(Config) ->
    test_message_forwarding(regular, Config).

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
    [SourceNode1 | _] = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = nodes_target(Config),
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
