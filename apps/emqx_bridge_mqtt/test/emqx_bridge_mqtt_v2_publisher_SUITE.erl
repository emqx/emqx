%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_v2_publisher_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(NS, <<"some_namespace">>).

-define(local, local).
-define(cluster, cluster).
-define(global_namespace, global_namespace).
-define(namespaced, namespaced).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, ?cluster},
        {group, ?local}
    ].

groups() ->
    AllTCs0 = emqx_common_test_helpers:all_with_matrix(?MODULE),
    AllTCs = lists:filter(
        fun
            ({group, _}) -> false;
            (_) -> true
        end,
        AllTCs0
    ),
    CustomMatrix = emqx_common_test_helpers:groups_with_matrix(?MODULE),
    LocalTCs = merge_custom_groups(?local, AllTCs, CustomMatrix),
    ClusterTCs = merge_custom_groups(?cluster, cluster_testcases(), CustomMatrix),
    [
        {?cluster, ClusterTCs},
        {?local, LocalTCs}
    ].

merge_custom_groups(RootGroup, GroupTCs, CustomMatrix0) ->
    CustomMatrix =
        lists:flatmap(
            fun
                ({G, _, SubGroup}) when G == RootGroup ->
                    SubGroup;
                (_) ->
                    []
            end,
            CustomMatrix0
        ),
    CustomMatrix ++ GroupTCs.

cluster_testcases() ->
    Key = ?cluster,
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false);
            (TestCase) ->
                emqx_common_test_helpers:get_tc_prop(?MODULE, TestCase, Key, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(?cluster = Group, Config) ->
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge_mqtt,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ],
    Nodes = emqx_cth_cluster:start(
        [
            {bridge_mqtt_pub1, #{
                role => core,
                apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {bridge_mqtt_pub2, #{
                role => core,
                apps => AppSpecs
            }},
            {bridge_mqtt_pub3, #{
                role => core,
                apps => AppSpecs
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, Config)}
    ),
    [{nodes, Nodes} | Config];
init_per_group(?local, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_connector,
            emqx_bridge_mqtt,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config];
init_per_group(?namespaced, Config) ->
    Opts = #{namespace => ?NS},
    AuthHeader = emqx_bridge_v2_testlib:ensure_namespaced_api_key(Opts),
    emqx_bridge_v2_testlib:set_auth_header_getter(fun() -> AuthHeader end),
    [
        {auth_header, AuthHeader},
        {resource_namespace, ?NS}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

end_per_group(?cluster, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(?local, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    ConnectorConfig = connector_config(),
    ActionConfig = action_config(#{connector => Name}),
    case ?config(nodes, Config) of
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun);
        _ ->
            ok
    end,
    [
        {bridge_kind, action},
        {action_type, mqtt},
        {action_name, Name},
        {action_config, ActionConfig},
        {connector_type, mqtt},
        {connector_name, Name},
        {connector_config, ConnectorConfig}
        | Config
    ].

end_per_testcase(_TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    case Nodes of
        undefined ->
            emqx_bridge_v2_testlib:delete_all_rules(),
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors();
        _ ->
            emqx_utils:pmap(
                fun(N) ->
                    ?ON(N, emqx_bridge_v2_testlib:delete_all_rules()),
                    ?ON(N, emqx_bridge_v2_testlib:delete_all_bridges_and_connectors())
                end,
                Nodes
            )
    end,
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"clean_start">> => true,
        <<"connect_timeout">> => <<"5s">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => <<"remote/topic">>,
                    <<"qos">> => 2
                },
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"1s">>,
                <<"metrics_flush_interval">> => <<"500ms">>,
                <<"resume_interval">> => <<"1s">>
            }
        },
    maps:merge(CommonConfig, Overrides).

replace(Key, Value, Proplist) ->
    lists:keyreplace(Key, 1, Proplist, {Key, Value}).

bridge_id(Config) ->
    Type = ?config(action_type, Config),
    Name = ?config(action_name, Config),
    emqx_bridge_resource:bridge_id(Type, Name).

connector_resource_id(Config) ->
    emqx_bridge_v2_testlib:connector_resource_id(Config).

get_tcp_mqtt_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

get_connector_api(Config) ->
    Type = ?config(connector_type, Config),
    Name = ?config(connector_name, Config),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

get_action_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            Config
        )
    ).

get_source_api(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(
            Config
        )
    ).

get_action_metrics_api(Config) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(
        Config
    ).

create_rule_and_action_http(Config, RuleTopic, Opts) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(
        ?config(action_type, Config), RuleTopic, Config, Opts
    ).

connect_client(Node) ->
    Port = get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link(#{port => Port, proto_ver => v5}),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

get_stats_http() ->
    {200, Res} = emqx_bridge_v2_testlib:get_stats_http(),
    lists:foldl(
        fun(#{<<"node">> := N} = R, Acc) -> Acc#{N => R} end,
        #{},
        Res
    ).

get_emqtt_clients(PoolName) ->
    lists:filtermap(
        fun({_Id, Worker}) ->
            case ecpool_worker:client(Worker) of
                {ok, Client} -> {true, Client};
                _ -> false
            end
        end,
        ecpool:workers(PoolName)
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, mqtt_connector_stopped),
    ok.

t_static_clientids() ->
    [{matrix, true}].
t_static_clientids(matrix) ->
    [[?cluster]];
t_static_clientids(Config) ->
    [N1, N2, N3] = Nodes = ?config(nodes, Config),
    [N1Bin, N2Bin, N3Bin] = lists:map(fun atom_to_binary/1, Nodes),
    Port = get_tcp_mqtt_port(N1),
    ct:pal("creating connector"),
    {201, _} = create_connector_api(Config, #{
        <<"server">> => <<"127.0.0.1:", (integer_to_binary(Port))/binary>>,
        <<"static_clientids">> =>
            [
                #{<<"node">> => N1Bin, <<"ids">> => []},
                #{<<"node">> => N2Bin, <<"ids">> => [<<"1">>, <<"3">>]},
                #{<<"node">> => N3Bin, <<"ids">> => [<<"2">>]}
            ]
    }),

    %% Nodes without any workers should report as disconnected.
    ct:pal("checking connector health"),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"status">> := <<"inconsistent">>,
                <<"node_status">> := [
                    #{
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
                    },
                    #{<<"status">> := <<"connected">>},
                    #{<<"status">> := <<"connected">>}
                ]
            }},
            get_connector_api(Config)
        )
    ),
    ConnResId = connector_resource_id(Config),
    GetWorkerClientids = fun() ->
        Clientids = lists:map(
            fun({_WorkerId, WorkerPid}) ->
                {ok, Client} = ecpool_worker:client(WorkerPid),
                Info = emqtt:info(Client),
                proplists:get_value(clientid, Info)
            end,
            ecpool:workers(ConnResId)
        ),
        lists:sort(Clientids)
    end,
    ?assertEqual([], ?ON(N1, GetWorkerClientids())),
    ?assertEqual([<<"1">>, <<"3">>], ?ON(N2, GetWorkerClientids())),
    ?assertEqual([<<"2">>], ?ON(N3, GetWorkerClientids())),

    %% Actions using this connector should be created just fine as well
    ct:pal("creating action"),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_action_api(Config, #{
            <<"parameters">> => #{
                <<"payload">> => <<"${.payload}">>
            }
        }),
    ct:pal("checking action health"),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"status">> := <<"inconsistent">>,
                <<"node_status">> := [
                    #{<<"status">> := <<"disconnected">>},
                    #{<<"status">> := <<"connected">>},
                    #{<<"status">> := <<"connected">>}
                ]
            }},
            get_action_api(Config)
        )
    ),

    RuleTopic = <<"local/t">>,
    ct:pal("creating rule"),
    {ok, #{<<"id">> := RuleId}} = create_rule_and_action_http(Config, RuleTopic, #{
        sql => iolist_to_binary(
            io_lib:format(
                "select payload from \"~s\"",
                [RuleTopic]
            )
        )
    }),

    C0 = connect_client(N1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C0, RemoteTopic, ?QOS_1),
    Clients = lists:map(fun connect_client/1, Nodes),

    %% Messages from the client connected to the node without clientids will be lost.
    ct:pal("publishing messages"),
    lists:foreach(
        fun({N, C}) ->
            {ok, _} = emqtt:publish(C, RuleTopic, integer_to_binary(N), ?QOS_1)
        end,
        lists:enumerate(Clients)
    ),
    ?assertNotReceive({publish, #{payload := <<"1">>}}),
    ?assertReceive({publish, #{payload := <<"2">>}}),
    ?assertReceive({publish, #{payload := <<"3">>}}),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 3,
                    <<"success">> := 2,
                    <<"failed">> := 0,
                    <<"dropped">> := 1
                },
                <<"node_metrics">> := [
                    #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"success">> := 0,
                            <<"failed">> := 0,
                            <<"dropped">> := 1
                        }
                    },
                    #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"success">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0
                        }
                    },
                    #{
                        <<"metrics">> := #{
                            <<"matched">> := 1,
                            <<"success">> := 1,
                            <<"failed">> := 0,
                            <<"dropped">> := 0
                        }
                    }
                ]
            }},
            get_action_metrics_api(Config)
        )
    ),
    ?assertMatch(
        #{
            N1 := #{
                counters := #{
                    'matched' := 1,
                    'passed' := 1,
                    'actions.failed' := 1,
                    'actions.success' := 0
                }
            },
            N2 := #{
                counters := #{
                    'matched' := 1,
                    'passed' := 1,
                    'actions.failed' := 0,
                    'actions.success' := 1
                }
            },
            N3 := #{
                counters := #{
                    'matched' := 1,
                    'passed' := 1,
                    'actions.failed' := 0,
                    'actions.success' := 1
                }
            }
        },
        maps:from_list([{N, ?ON(N, emqx_bridge_v2_testlib:get_rule_metrics(RuleId))} || N <- Nodes])
    ),

    ok.

%% Checks that we can forward original MQTT user properties via this action.  Also
%% verifies that extra properties may be added via templates.
t_forward_user_properties(Config) ->
    {201, _} = create_connector_api(Config, _Overrides = #{}),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_action_api(
            Config,
            #{}
        ),
    RuleTopic = <<"t/forward/ups">>,
    {ok, _} = create_rule_and_action_http(
        Config,
        RuleTopic,
        _Opts = #{
            sql => iolist_to_binary(
                io_lib:format(
                    "select *,"
                    " map_put(concat('User-Property.', payload.extra_key), "
                    "payload.extra_value, pub_props) as pub_props from \"~s\"",
                    [RuleTopic]
                )
            )
        }
    ),
    {ok, C} = emqtt:start_link(#{proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C, RemoteTopic, 1),
    Payload1 = emqx_utils_json:encode(#{
        <<"extra_key">> => <<"k2">>,
        <<"extra_value">> => <<"v2">>
    }),
    {ok, _} = emqtt:publish(
        C,
        RuleTopic,
        #{'User-Property' => [{<<"k1">>, <<"v1">>}]},
        Payload1,
        [{qos, ?QOS_1}]
    ),
    {publish, #{properties := #{'User-Property' := UserProps1}}} =
        ?assertReceive({publish, #{properties := #{'User-Property' := _}}}),
    ?assertMatch(
        [{<<"k1">>, <<"v1">>}, {<<"k2">>, <<"v2">>}],
        UserProps1
    ),
    ok.

%% Verifies that `emqtt' clients automatically try to reconnect once disconnected.
t_reconnect(Config) ->
    PoolSize = 3,
    ?check_trace(
        begin
            {201, _} = create_connector_api(Config, #{
                <<"pool_size">> => PoolSize
            }),
            NBin = atom_to_binary(node()),
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{NBin := #{<<"live_connections.count">> := PoolSize}},
                    get_stats_http()
                )
            ),
            ClientIds0 = emqx_cm:all_client_ids(),
            ClientIds = lists:droplast(ClientIds0),
            ChanPids0 = emqx_cm:all_channels(),
            ChanPids = lists:droplast(ChanPids0),
            lists:foreach(fun(Pid) -> monitor(process, Pid) end, ChanPids),
            ct:pal("kicking ~p (leaving 1 client alive)", [ClientIds]),
            {204, _} = emqx_bridge_v2_testlib:kick_clients_http(ClientIds),
            DownPids = emqx_utils:drain_down(PoolSize - 1),
            ?assertEqual(lists:sort(ChanPids), lists:sort(DownPids)),
            %% Recovery
            ct:pal("clients kicked; waiting for recovery..."),
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{NBin := #{<<"live_connections.count">> := PoolSize}},
                    get_stats_http()
                )
            ),
            ConnResId = emqx_bridge_v2_testlib:connector_resource_id(Config),
            ?assertEqual(PoolSize, length(ecpool:workers(ConnResId))),
            ?retry(500, 40, ?assertEqual(PoolSize, length(get_emqtt_clients(ConnResId)))),
            ok
        end,
        []
    ),
    ok.

%% Verifies we validate duplicate clientids amongst different connectors as well, to avoid
%% dumb mistakes like reusing the same clientid multiple times.
t_duplicate_static_clientids_different_connectors(Config) ->
    NodeBin = atom_to_binary(node()),
    {201, _} = create_connector_api(Config, #{
        <<"static_clientids">> => [#{<<"node">> => NodeBin, <<"ids">> => [<<"1">>]}]
    }),
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"reason">> :=
                    <<
                        "distinct mqtt connectors must not use the same static clientids;"
                        " connectors with duplicate static clientids: ",
                        _/binary
                    >>
            }
        }},
        create_connector_api([{connector_name, <<"another">>} | Config], #{
            <<"static_clientids">> => [#{<<"node">> => NodeBin, <<"ids">> => [<<"1">>]}]
        })
    ),
    %% Using a different host is fine (different IPs and hostnames may still lead to the
    %% same cluster, it's a best effort check)
    ?assertMatch(
        {201, _},
        create_connector_api([{connector_name, <<"another">>} | Config], #{
            <<"server">> => <<"a-different-host:1883">>,
            <<"static_clientids">> => [#{<<"node">> => NodeBin, <<"ids">> => [<<"1">>]}]
        })
    ),
    ok.

%% Checks the race condition where the action is deemed healthy, but the MQTT client has
%% concurrently been disconnected (and, due to a race, `emqtt' gets either a `{error,
%% closed}' or `{error, tcp_closed}' error message).  We should retry.
t_publish_while_tcp_closed_concurrently(Config) ->
    PoolSize = 3,
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := call_query_enter},
                #{?snk_kind := "kill_clients", ?snk_span := start}
            ),
            ?force_ordering(
                #{?snk_kind := "kill_clients", ?snk_span := {complete, _}},
                #{?snk_kind := "mqtt_action_about_to_publish"}
            ),

            TestClientid = <<"testclientid">>,

            spawn_link(fun() ->
                ?tp_span(
                    "kill_clients",
                    #{},
                    begin
                        ClientIds0 = [_, _ | _] = emqx_cm:all_client_ids(),
                        ChanPids0 = [_, _ | _] = emqx_cm:all_channels(),
                        ClientidsChanPids0 = lists:zip(ClientIds0, ChanPids0),
                        ClientidsChanPids = lists:filter(
                            fun({CId, _Pid}) ->
                                CId /= TestClientid
                            end,
                            ClientidsChanPids0
                        ),
                        {ClientidsToKick, ChanPids} = lists:unzip(ClientidsChanPids),
                        lists:foreach(fun(Pid) -> monitor(process, Pid) end, ChanPids),
                        ct:pal("kicking ~p", [ClientidsToKick]),
                        %% Forcefully kill connection processes, so we get the ellusive `tcp_closed'
                        %% error more easily from `emqtt'.
                        lists:foreach(fun(ChanPid) -> ChanPid ! die_if_test end, ChanPids),
                        _DownPids = emqx_utils:drain_down(PoolSize),

                        ok
                    end
                )
            end),

            {201, _} = create_connector_api(Config, #{
                %% Should usually be `true', but we allow `false', despite the potential
                %% issues with duplication of messages and loss of client state.
                <<"clean_start">> => false,
                <<"pool_size">> => PoolSize,
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
            }),
            {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
                create_action_api(
                    Config,
                    #{
                        <<"parameters">> => #{<<"qos">> => ?QOS_2},
                        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
                    }
                ),

            RuleTopic = <<"rule/republish">>,
            {ok, _} = create_rule_and_action_http(Config, RuleTopic, #{}),

            {ok, C} = emqtt:start_link(#{proto_ver => v5, clientid => TestClientid}),
            {ok, _} = emqtt:connect(C),
            on_exit(fun() -> emqtt:stop(C) end),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(C, RemoteTopic, [{qos, ?QOS_2}]),

            {ok, _} = emqtt:publish(C, RuleTopic, <<"hey">>, [{qos, ?QOS_2}]),

            ?assertReceive({publish, #{topic := RemoteTopic}}, 5_000),

            ok
        end,
        fun(Trace) ->
            %% Connection being closed forcefully should induce a retry
            ?assertMatch([_ | _], ?of_kind(buffer_worker_retry_inflight, Trace)),
            ok
        end
    ),
    ok.

%% Smoke integration test to check that fallback action are triggered.  This Action is
%% chosen for this test because it uses the conventional builtin buffer.
t_fallback_actions(Config) ->
    {201, _} = create_connector_api(Config, _Overrides = #{}),
    RepublishTopic = <<"republish/fallback">>,
    RepublishArgs = #{
        <<"topic">> => RepublishTopic,
        <<"qos">> => 1,
        <<"retain">> => false,
        <<"payload">> => <<"${payload}">>,
        <<"mqtt_properties">> => #{},
        <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
        <<"direct_dispatch">> => false
    },
    {201, _} =
        create_action_api(
            Config,
            #{
                <<"fallback_actions">> => [
                    #{
                        <<"kind">> => <<"republish">>,
                        <<"args">> => RepublishArgs
                    }
                ],
                %% Simple way to make the requests fail: make the buffer overflow
                <<"resource_opts">> => #{
                    <<"max_buffer_bytes">> => <<"0B">>,
                    <<"buffer_seg_bytes">> => <<"0B">>
                }
            }
        ),
    RuleTopic = <<"fallback/actions">>,
    {ok, _} = create_rule_and_action_http(
        Config,
        RuleTopic,
        #{}
    ),
    {ok, C} = emqtt:start_link(#{proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C, RepublishTopic, 1),
    Payload = emqx_utils_json:encode(#{
        <<"k">> => <<"aaaaaaaaaaaaaaaaa">>
    }),
    {ok, _} = emqtt:publish(C, RuleTopic, Payload, [{qos, ?QOS_1}]),
    ?assertReceive({publish, #{topic := RepublishTopic, payload := Payload}}),
    ok.

%% Checks that we can start a node with a config with a republish action (essentially,
%% `emqx_config:init_load' accepts it).g
t_fallback_actions_load_config(Config) ->
    ConnectorType = ?config(connector_type, Config),
    ConnectorName = ?config(connector_name, Config),
    ConnectorConfig = ?config(connector_config, Config),
    ActionType = ?config(action_type, Config),
    ActionName = ?config(action_name, Config),
    ActionConfig0 = ?config(action_config, Config),
    RepublishTopic = <<"republish/fallback">>,
    RepublishArgs = #{
        <<"topic">> => RepublishTopic,
        <<"qos">> => 1,
        <<"retain">> => false,
        <<"payload">> => <<"${payload}">>,
        <<"mqtt_properties">> => #{},
        <<"user_properties">> => <<"${pub_props.'User-Property'}">>,
        <<"direct_dispatch">> => false
    },
    ActionConfig = emqx_utils_maps:deep_merge(
        ActionConfig0,
        #{
            <<"fallback_actions">> => [
                #{
                    <<"kind">> => <<"republish">>,
                    <<"args">> => RepublishArgs
                }
            ],
            %% Simple way to make the requests fail: make the buffer overflow
            <<"resource_opts">> => #{
                <<"max_buffer_bytes">> => <<"0B">>,
                <<"buffer_seg_bytes">> => <<"0B">>
            }
        }
    ),
    Hocon = #{
        <<"connectors">> => #{ConnectorType => #{ConnectorName => ConnectorConfig}},
        <<"actions">> => #{ActionType => #{ActionName => ActionConfig}}
    },
    AppSpecs = [
        emqx,
        emqx_conf,
        emqx_connector,
        emqx_bridge_mqtt,
        {emqx_bridge, #{config => Hocon}},
        emqx_rule_engine,
        emqx_management
    ],
    %% Simply successfully starting the nodes is enough.
    Nodes = emqx_cth_cluster:start(
        [{bridge_fallback_actions, #{role => core, apps => AppSpecs}}],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    ok = emqx_cth_cluster:stop(Nodes),
    ok.

t_rule_test_trace() ->
    [{matrix, true}].
t_rule_test_trace(matrix) ->
    [
        [?local, ?global_namespace],
        [?local, ?namespaced]
    ];
t_rule_test_trace(Config) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(Config, Opts).
