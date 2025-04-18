%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_cluster_link_SUITE).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, DO), erpc:call(NODE, fun() -> DO end)).

%%

suite() -> [{timetrap, {minutes, 1}}].

all() ->
    [
        {group, shared_subs},
        {group, non_shared_subs}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    [
        {shared_subs, AllTCs},
        {non_shared_subs, AllTCs}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(shared_subs, Config) ->
    [{is_shared_sub, true} | Config];
init_per_group(non_shared_subs, Config) ->
    [{is_shared_sub, false} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TCName, Config).

end_per_testcase(TCName, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TCName, Config).

%%

mk_source_cluster(BaseName, Config) ->
    SourceConf =
        "cluster {"
        "\n name = cl.source"
        "\n links = ["
        "\n   { enable = true"
        "\n     name = cl.target"
        "\n     server = \"localhost:31883\""
        "\n     clientid = client.source"
        "\n     topics = []"
        "\n   }"
        "\n ]}",
    ExtraConf = proplists:get_value(extra_conf, Config, ""),
    SourceApps1 = [
        {emqx_conf, combine([conf_log(), SourceConf])},
        {emqx, ExtraConf},
        {emqx_auth, #{}}
    ],
    SourceApps2 = [
        {emqx_conf, combine([conf_log(), SourceConf])},
        {emqx, combine([conf_mqtt_listener(41883), ExtraConf])},
        {emqx_auth, #{}}
    ],
    emqx_cth_cluster:mk_nodespecs(
        [
            {mk_nodename(BaseName, s1), #{apps => SourceApps1}},
            {mk_nodename(BaseName, s2), #{apps => SourceApps2}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

mk_target_cluster(BaseName, Config) ->
    TargetConf =
        "cluster {"
        "\n name = cl.target"
        "\n links = ["
        "\n   { enable = true"
        "\n     name = cl.source"
        "\n     server = \"localhost:41883\""
        "\n     clientid = client.target"
        "\n     topics = [\"#\"]"
        "\n   }"
        "\n ]}",
    ExtraConf = proplists:get_value(extra_conf, Config, ""),
    TargetApps1 = [
        {emqx_conf, combine([conf_log(), TargetConf])},
        {emqx, combine([conf_mqtt_listener(31883), ExtraConf])},
        {emqx_auth, #{}}
    ],
    TargetApps2 = [
        {emqx_conf, combine([conf_log(), TargetConf])},
        {emqx, ExtraConf},
        {emqx_auth, #{}}
    ],
    emqx_cth_cluster:mk_nodespecs(
        [
            {mk_nodename(BaseName, t1), #{apps => TargetApps1, base_port => 20100}},
            {mk_nodename(BaseName, t2), #{apps => TargetApps2, base_port => 20200}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ).

mk_nodename(BaseName, Suffix) ->
    binary_to_atom(fmt("emqx_clink_~s_~s", [BaseName, Suffix])).

conf_mqtt_listener(LPort) when is_integer(LPort) ->
    fmt("listeners.tcp.clink { bind = ~p }", [LPort]);
conf_mqtt_listener(_) ->
    "".

conf_log() ->
    "log.file { enable = true, level = info, path = node.log, supervisor_reports = progress }".

combine([Entry | Rest]) ->
    lists:foldl(fun emqx_cth_suite:merge_config/2, Entry, Rest).

start_cluster_link(Nodes, Config) ->
    Results = lists:usort(
        erpc:multicall(Nodes, emqx_cth_suite, start_apps, [
            [emqx_cluster_link],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ])
    ),
    lists:flatmap(fun({ok, Apps}) -> Apps end, Results).

stop_cluster_link(Config) ->
    Apps = ?config(tc_apps, Config),
    Nodes = nodes_all(Config),
    [{ok, ok}] = lists:usort(
        erpc:multicall(Nodes, emqx_cth_suite, stop_apps, [Apps])
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
    SourceNodes = emqx_cth_cluster:start(mk_source_cluster(?FUNCTION_NAME, Config)),
    TargetNodes = emqx_cth_cluster:start(mk_target_cluster(?FUNCTION_NAME, Config)),
    ok = snabbkaffe:start_trace(),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_message_forwarding('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_message_forwarding(Config) ->
    IsShared = ?config(is_shared_sub, Config),
    SourceNodes = [SourceNode1 | _] = nodes_source(Config),
    TargetNodes = [TargetNode1, TargetNode2 | _] = nodes_target(Config),
    %% Connect client to the target cluster.
    TargetC1 = emqx_cluster_link_cth:connect_client("t_message_forwarding1", TargetNode1),
    TargetC2 = emqx_cluster_link_cth:connect_client("t_message_forwarding2", TargetNode2),
    %% Connect a client to the source cluster.
    SourceC1 = emqx_cluster_link_cth:connect_client("t_message_forwarding", SourceNode1),
    ?check_trace(
        try
            %% Subscribe both to different topics.
            T11 = maybe_shared_topic(IsShared, <<"t1/+">>),
            T12 = maybe_shared_topic(IsShared, <<"t1/#">>),
            {ok, _, _} = emqtt:subscribe(TargetC1, T11, qos1),
            {ok, _, _} = emqtt:subscribe(TargetC2, T12, qos1),
            %% Start cluster link, existing routes should be replicated.
            _Apps = start_cluster_link(SourceNodes ++ TargetNodes, Config),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_bootstrap_complete",
                ?snk_meta := #{node := TargetNode1}
            }),
            {ok, _} = ?block_until(#{
                ?snk_kind := "cluster_link_bootstrap_complete",
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
            T21 = maybe_shared_topic(IsShared, <<"t2/#">>),
            T22 = maybe_shared_topic(IsShared, <<"t2/+/+">>),
            {ok, _, _} = emqtt:subscribe(TargetC1, T21, qos1),
            {ok, _, _} = emqtt:subscribe(TargetC2, T22, qos1),
            {ok, _} = ?block_until(#{?snk_kind := "cluster_link_route_sync_complete"}),
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
        fun(_) -> ok end
    ).

t_target_extrouting_gc('init', Config) ->
    SourceCluster = mk_source_cluster(?FUNCTION_NAME, Config),
    SourceNodes = emqx_cth_cluster:start(SourceCluster),
    TargetCluster = mk_target_cluster(?FUNCTION_NAME, Config),
    TargetNodes = emqx_cth_cluster:start(TargetCluster),
    _Apps = start_cluster_link(SourceNodes ++ TargetNodes, Config),
    ok = snabbkaffe:start_trace(),
    [
        {source_cluster, SourceCluster},
        {source_nodes, SourceNodes},
        {target_cluster, TargetCluster},
        {target_nodes, TargetNodes}
        | Config
    ];
t_target_extrouting_gc('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).

t_target_extrouting_gc(Config) ->
    [SourceNode1 | _] = nodes_source(Config),
    [TargetNode1, TargetNode2 | _] = nodes_target(Config),
    SourceC1 = emqx_cluster_link_cth:connect_client("t_target_extrouting_gc", SourceNode1),
    TargetC1 = emqx_cluster_link_cth:connect_client_unlink("t_target_extrouting_gc1", TargetNode1),
    TargetC2 = emqx_cluster_link_cth:connect_client_unlink("t_target_extrouting_gc2", TargetNode2),
    ?check_trace(
        begin
            IsShared = ?config(is_shared_sub, Config),
            TopicFilter1 = maybe_shared_topic(IsShared, <<"t/+">>),
            TopicFilter2 = maybe_shared_topic(IsShared, <<"t/#">>),
            {ok, _, _} = emqtt:subscribe(TargetC1, TopicFilter1, qos1),
            {ok, _, _} = emqtt:subscribe(TargetC2, TopicFilter2, qos1),
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
                #{?snk_kind := clink_extrouter_actor_cleaned, cluster := <<"cl.target">>}
            ),
            {ok, _} = emqtt:publish(SourceC1, <<"t/4/ext">>, <<"HELLO4">>, qos1),
            {ok, _} = emqtt:publish(SourceC1, <<"t/5">>, <<"HELLO5">>, qos1),
            Pubs2 = [M || {publish, M} <- ?drainMailbox(1_000)],
            {ok, _} = ?wait_async_action(
                emqx_cth_cluster:stop_node(TargetNode1),
                #{?snk_kind := clink_extrouter_actor_cleaned, cluster := <<"cl.target">>}
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
                ?of_kind(clink_message_forwarded, Trace)
            )
        end
    ).

%% Checks that, if an exception occurs while handling a route op message, we disconnect
%% the upstream agent client so it restarts.
t_disconnect_on_errors('init', Config) ->
    SourceNodes = emqx_cth_cluster:start(mk_source_cluster(?FUNCTION_NAME, Config)),
    [TargetNodeSpec | _] = mk_target_cluster(?FUNCTION_NAME, Config),
    TargetNodes = emqx_cth_cluster:start([TargetNodeSpec]),
    _Apps = start_cluster_link(SourceNodes ++ TargetNodes, Config),
    ok = snabbkaffe:start_trace(),
    [
        {source_nodes, SourceNodes},
        {target_nodes, TargetNodes}
        | Config
    ];
t_disconnect_on_errors('end', Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)).
t_disconnect_on_errors(Config) ->
    ct:timetrap({seconds, 10}),
    [SN1 | _] = nodes_source(Config),
    [TargetNode] = nodes_target(Config),
    SC1 = emqx_cluster_link_cth:connect_client("t_disconnect_on_errors", SN1),
    ok = ?ON(SN1, meck:new(emqx_cluster_link, [passthrough, no_link, no_history])),
    ?assertMatch(
        {_, {ok, _}},
        ?wait_async_action(
            begin
                ok = ?ON(
                    TargetNode,
                    meck:expect(
                        emqx_cluster_link,
                        do_handle_route_op_msg,
                        fun(_Msg) ->
                            meck:exception(error, {unexpected, error})
                        end
                    )
                ),
                emqtt:subscribe(SC1, <<"t/u/v">>, 1)
            end,
            #{?snk_kind := "cluster_link_connection_down"}
        )
    ),
    _ = ?ON(TargetNode, meck:unload()),
    ok = emqtt:stop(SC1),
    ok.

%% Checks that if a timeout occurs during actor state initialization, we close the
%% (potentially unhealthy) connection and start anew.
t_restart_connection_on_actor_init_timeout('init', Config0) ->
    ExtraConf = "authorization.no_match = deny",
    SourceNodesSpec = mk_source_cluster(?FUNCTION_NAME, [{extra_conf, ExtraConf} | Config0]),
    TargetNodesSpec = mk_target_cluster(?FUNCTION_NAME, Config0),
    ok = snabbkaffe:start_trace(),
    SourceNodes = emqx_cth_cluster:start(SourceNodesSpec),
    TargetNodes = emqx_cth_cluster:start(TargetNodesSpec),
    [
        {source_nodes_spec, SourceNodesSpec},
        {source_nodes, SourceNodes},
        {target_nodes_spec, TargetNodesSpec},
        {target_nodes, TargetNodes}
        | Config0
    ];
t_restart_connection_on_actor_init_timeout('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(target_nodes, Config)),
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.
t_restart_connection_on_actor_init_timeout(Config) ->
    SourceNodes = [SN | _] = ?config(source_nodes, Config),
    TargetNodes = ?config(target_nodes, Config),

    %% Simulate a poorly configured node that'll reject the actor init ack
    %% message, making the initialization time out.
    ok = ?ON(
        SN,
        emqx_authz_test_lib:setup_config(
            #{
                <<"type">> => <<"file">>,
                <<"enable">> => true,
                <<"rules">> =>
                    <<
                        "{deny, all, subscribe, [\"#\"]}.\n"
                        "{allow, all, publish, [\"$LINK/#\", \"#\"]}."
                    >>
            },
            #{}
        )
    ),

    ?check_trace(
        #{timetrap => 30_000},
        begin
            ct:pal("starting cluster link"),
            ?wait_async_action(
                start_cluster_link(SourceNodes ++ TargetNodes, Config),
                #{?snk_kind := "cluster_link_handshake_timeout"}
            ),

            %% Fix the authorization config, it should reconnect.
            ct:pal("fixing config"),
            ok = timer:sleep(100),
            ?wait_async_action(
                begin
                    ok = ?ON(
                        SN,
                        emqx_authz_test_lib:setup_config(
                            #{
                                <<"type">> => <<"file">>,
                                <<"enable">> => true,
                                <<"rules">> => <<"{allow, all}.">>
                            },
                            #{}
                        )
                    ),
                    {ok, _} = ?ON(
                        SN,
                        emqx_conf:update([authorization, no_match], allow, #{override_to => cluster})
                    )
                end,
                #{?snk_kind := "cluster_link_bootstrap_complete"}
            ),

            ok
        end,
        fun(Trace) ->
            ?assert(
                ?strict_causality(
                    #{?snk_kind := "cluster_link_handshake_timeout", ?snk_meta := #{node := _N1}},
                    #{?snk_kind := "cluster_link_stop_link_client", ?snk_meta := #{node := _N2}},
                    _N1 =:= _N2,
                    Trace
                )
            ),
            ok
        end
    ).

%% Checks that connect / subscribe errors during routerepl actor initialization are
%% handled gracefully.
t_graceful_retry_on_actor_error('init', Config0) ->
    ExtraConf = "listeners.tcp.clink.enable = false",
    SourceNodesSpec = mk_source_cluster(?FUNCTION_NAME, Config0),
    TargetNodesSpec = mk_target_cluster(?FUNCTION_NAME, [{extra_conf, ExtraConf} | Config0]),
    ok = snabbkaffe:start_trace(),
    SourceNodes = emqx_cth_cluster:start(SourceNodesSpec),
    [
        {source_nodes_spec, SourceNodesSpec},
        {source_nodes, SourceNodes},
        {target_nodes_spec, TargetNodesSpec}
        | Config0
    ];
t_graceful_retry_on_actor_error('end', Config) ->
    ok = emqx_cth_cluster:stop(?config(source_nodes, Config)),
    ok = snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.
t_graceful_retry_on_actor_error(Config) ->
    SourceNodes = [SN | _] = ?config(source_nodes, Config),
    TargetNodeSpecs = ?config(target_nodes_spec, Config),

    ?check_trace(
        #{timetrap => 30_000},
        begin
            %% Target cluster is not started yet.
            %% Connection failure should be tolerated.
            ?wait_async_action(
                start_cluster_link(SourceNodes, Config),
                #{
                    ?snk_kind := "cluster_link_connection_failed",
                    ?snk_meta := #{node := SN}
                }
            ),

            %% Start Target cluster.
            TargetNodes = [TN | _] = emqx_cth_cluster:start(TargetNodeSpecs),
            on_exit(fun() -> emqx_cth_cluster:stop(TargetNodes) end),

            %% Setup strict authz, incompatible with what routerepl actor expects.
            ok = ?ON(
                TN,
                emqx_authz_test_lib:setup_config(
                    #{
                        <<"type">> => <<"file">>,
                        <<"enable">> => true,
                        <<"rules">> => <<"{deny, all, subscribe, [\"$LINK/#\"]}.">>
                    },
                    #{}
                )
            ),
            %% Also make sure that connection is forcefully disconnected on authz failures.
            {ok, _} = ?ON(
                TN,
                emqx_conf:update([authorization, deny_action], disconnect, #{override_to => cluster})
            ),

            %% Enable dedicated listener.
            ?wait_async_action(
                {ok, _} = ?ON(
                    TN,
                    emqx_mgmt_listeners_conf:update(tcp, clink, #{<<"enable">> => true})
                ),
                %% Disconnect during SUBSCRIBE should be tolerated.
                #{
                    ?snk_kind := "cluster_link_connection_failed",
                    ?snk_meta := #{node := SN},
                    reason := {{shutdown, {disconnected, ?RC_NOT_AUTHORIZED, _}}, _}
                }
            ),

            _ = start_cluster_link(TargetNodes, Config),

            %% Fix the authorization config, it should reconnect.
            ?wait_async_action(
                begin
                    ok = ?ON(
                        TN,
                        emqx_authz_test_lib:setup_config(
                            #{
                                <<"type">> => <<"file">>,
                                <<"enable">> => true,
                                <<"rules">> => <<"{allow, all}.">>
                            },
                            #{}
                        )
                    )
                end,
                #{
                    ?snk_kind := "cluster_link_bootstrap_complete",
                    ?snk_meta := #{node := SN}
                }
            )
        end,
        fun(Trace) ->
            %% Exactly 4 actors started, no actor should have restarted.
            ?assertMatch(
                [_SN1, _SN2, _TN1, _TN2],
                ?of_kind("cluster_link_actor_init", Trace)
            )
        end
    ).

%%

maybe_shared_topic(true = _IsShared, Topic) ->
    <<"$share/test-group/", Topic/binary>>;
maybe_shared_topic(false = _IsShared, Topic) ->
    Topic.

fmt(Fmt, Args) ->
    emqx_utils:format(Fmt, Args).
