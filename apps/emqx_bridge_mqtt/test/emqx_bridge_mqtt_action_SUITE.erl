%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_action_SUITE).

-moduledoc """
This suite holds test cases for the `mqtt` action.

For cases where a single connector has both actions and sources, see
`emqx_bridge_mqtt_hybrid_SUITE`.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, mqtt).
-define(CONNECTOR_TYPE_BIN, <<"mqtt">>).
-define(ACTION_TYPE, mqtt).
-define(ACTION_TYPE_BIN, <<"mqtt">>).

-define(local, local).
-define(cluster, cluster).
-define(global_namespace, global_namespace).
-define(namespaced, namespaced).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(NS, <<"some_namespace">>).

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

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?cluster = Group, TCConfig) ->
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
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    [{nodes, Nodes} | TCConfig];
init_per_group(?local, TCConfig) ->
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
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig];
init_per_group(?namespaced, TCConfig) ->
    Opts = #{namespace => ?NS},
    AuthHeader = emqx_bridge_v2_testlib:ensure_namespaced_api_key(Opts),
    emqx_bridge_v2_testlib:set_auth_header_getter(fun() -> AuthHeader end),
    [
        {auth_header, AuthHeader},
        {resource_namespace, ?NS}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?cluster, TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName
    }),
    setup_auth_header(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_connector_config(Overrides).

action_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_action_config(Overrides).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

get_tcp_mqtt_port(Node) ->
    {_Host, Port} = ?ON(Node, emqx_config:get([listeners, tcp, default, bind])),
    Port.

setup_auth_header(TCConfig) ->
    case get_config(nodes, TCConfig, undefined) of
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun);
        _ ->
            ok
    end.

connector_resource_id(TCConfig) ->
    emqx_bridge_v2_testlib:connector_resource_id(TCConfig).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

update_connector_api(TCConfig, Overrides) ->
    #{
        connector_type := Type,
        connector_name := Name,
        connector_config := Cfg0
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_action_api(
            TCConfig
        )
    ).

get_source_api(TCConfig) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(
            TCConfig
        )
    ).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(
        TCConfig
    ).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

get_stats_api() ->
    {200, Res} = emqx_bridge_v2_testlib:get_stats_http(),
    lists:foldl(
        fun(#{<<"node">> := N} = R, Acc) -> Acc#{N => R} end,
        #{},
        Res
    ).

start_client(TCConfigOrNode) ->
    start_client(TCConfigOrNode, _Opts = #{}).

start_client(TCConfig, Opts) when is_list(TCConfig) ->
    case get_config(nodes, TCConfig, undefined) of
        [N | _] ->
            start_client(N, Opts);
        _ ->
            start_client(node(), Opts)
    end;
start_client(Node, Opts) when is_atom(Node) ->
    Port = get_tcp_mqtt_port(Node),
    {ok, C} = emqtt:start_link(Opts#{port => Port, proto_ver => v5}),
    on_exit(fun() -> catch emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

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

start_publisher(Topic, Interval, CtrlPid) ->
    spawn_link(fun() -> publisher(Topic, 1, Interval, CtrlPid) end).

stop_publisher(Pid) ->
    _ = Pid ! {self(), stop},
    receive
        {Pid, N} -> N
    after 1_000 -> ct:fail("publisher ~p did not stop", [Pid])
    end.

publisher(Topic, N, Delay, CtrlPid) ->
    _ = emqx:publish(emqx_message:make(Topic, integer_to_binary(N))),
    receive
        {CtrlPid, stop} ->
            CtrlPid ! {self(), N}
    after Delay ->
        publisher(Topic, N + 1, Delay, CtrlPid)
    end.

hook_authenticate() ->
    on_exit(fun unhook_authenticate/0),
    emqx_hooks:add('client.authenticate', {?MODULE, authenticate, [self()]}, ?HP_HIGHEST).

unhook_authenticate() ->
    emqx_hooks:del('client.authenticate', {?MODULE, authenticate}).

authenticate(Credential, _, TestRunnerPid) ->
    _ = TestRunnerPid ! {authenticate, Credential},
    ignore.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, mqtt_connector_stopped).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action(TCConfig) when is_list(TCConfig) ->
    #{<<"parameters">> := #{<<"topic">> := Topic}} =
        get_config(action_config, TCConfig),
    C = start_client(TCConfig),
    PrePublishFn = fun(Context) ->
        {ok, _, [_]} = emqtt:subscribe(C, Topic, [{qos, 2}]),
        Context
    end,
    PostPublishFn = fun(Context) ->
        #{payload := Payload} = Context,
        ?assertReceive({publish, #{payload := Payload}})
    end,
    ActionOverrides = #{<<"parameters">> => #{<<"payload">> => <<"${.payload}">>}},
    Opts = #{
        action_overrides => ActionOverrides,
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn,
        rule_topic => <<"rule/topic">>,
        sql => <<"select payload from \"rule/topic\" ">>
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_static_clientids() ->
    [{matrix, true}].
t_static_clientids(matrix) ->
    [[?cluster]];
t_static_clientids(TCConfig) ->
    [N1, N2, N3] = Nodes = get_config(nodes, TCConfig),
    [N1Bin, N2Bin, N3Bin] = lists:map(fun atom_to_binary/1, Nodes),
    Port = get_tcp_mqtt_port(N1),
    ct:pal("creating connector"),
    {201, _} = create_connector_api(TCConfig, #{
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
            get_connector_api(TCConfig)
        )
    ),
    ConnResId = connector_resource_id(TCConfig),
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
        create_action_api(TCConfig, #{
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
            get_action_api(TCConfig)
        )
    ),

    ct:pal("creating rule"),
    #{id := RuleId, topic := RuleTopic} = simple_create_rule_api(
        <<"select payload from \"${t}\" ">>,
        TCConfig
    ),

    C0 = start_client(N1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C0, RemoteTopic, ?QOS_1),
    Clients = lists:map(fun start_client/1, Nodes),

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
            get_action_metrics_api(TCConfig)
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

t_rule_test_trace() ->
    [{matrix, true}].
t_rule_test_trace(matrix) ->
    [
        [?local, ?global_namespace],
        [?local, ?namespaced]
    ];
t_rule_test_trace(TCConfig) ->
    Opts = #{},
    emqx_bridge_v2_testlib:t_rule_test_trace(TCConfig, Opts).

%% Checks that we can forward original MQTT user properties via this action.  Also
%% verifies that extra properties may be added via templates.
t_forward_user_properties(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, _Overrides = #{}),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_action_api(
            TCConfig,
            #{}
        ),
    #{topic := RuleTopic} = simple_create_rule_api(
        <<
            "select *,"
            " map_put(concat('User-Property.', payload.extra_key), "
            "payload.extra_value, pub_props) as pub_props from \"${t}\""
        >>,
        TCConfig
    ),
    C = start_client(TCConfig),
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
t_reconnect(TCConfig) ->
    PoolSize = 3,
    ?check_trace(
        begin
            {201, _} = create_connector_api(TCConfig, #{
                <<"pool_size">> => PoolSize
            }),
            NBin = atom_to_binary(node()),
            ?retry(
                500,
                20,
                ?assertMatch(
                    #{NBin := #{<<"live_connections.count">> := PoolSize}},
                    get_stats_api()
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
                    get_stats_api()
                )
            ),
            ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
            ?assertEqual(PoolSize, length(ecpool:workers(ConnResId))),
            ?retry(500, 40, ?assertEqual(PoolSize, length(get_emqtt_clients(ConnResId)))),
            ok
        end,
        []
    ),
    ok.

%% Verifies we validate duplicate clientids amongst different connectors as well, to avoid
%% dumb mistakes like reusing the same clientid multiple times.
t_duplicate_static_clientids_different_connectors(TCConfig) ->
    NodeBin = atom_to_binary(node()),
    {201, _} = create_connector_api(TCConfig, #{
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
        create_connector_api([{connector_name, <<"another">>} | TCConfig], #{
            <<"static_clientids">> => [#{<<"node">> => NodeBin, <<"ids">> => [<<"1">>]}]
        })
    ),
    %% Using a different host is fine (different IPs and hostnames may still lead to the
    %% same cluster, it's a best effort check)
    ?assertMatch(
        {201, _},
        create_connector_api([{connector_name, <<"another">>} | TCConfig], #{
            <<"server">> => <<"a-different-host:1883">>,
            <<"static_clientids">> => [#{<<"node">> => NodeBin, <<"ids">> => [<<"1">>]}]
        })
    ),
    ok.

%% Checks the race condition where the action is deemed healthy, but the MQTT client has
%% concurrently been disconnected (and, due to a race, `emqtt' gets either a `{error,
%% closed}' or `{error, tcp_closed}' error message).  We should retry.
t_publish_while_tcp_closed_concurrently(TCConfig) ->
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

            {201, _} = create_connector_api(TCConfig, #{
                %% Should usually be `true', but we allow `false', despite the potential
                %% issues with duplication of messages and loss of client state.
                <<"clean_start">> => false,
                <<"pool_size">> => PoolSize,
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
            }),
            {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
                create_action_api(
                    TCConfig,
                    #{
                        <<"parameters">> => #{<<"qos">> => ?QOS_2},
                        <<"resource_opts">> => #{<<"health_check_interval">> => <<"15s">>}
                    }
                ),

            #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
            C = start_client(TCConfig, #{clientid => TestClientid}),
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

t_fallback_actions(TCConfig) ->
    emqx_bridge_v2_testlib:t_fallback_actions(TCConfig).

%% Checks that we can start a node with a config with a republish action (essentially,
%% `emqx_config:init_load' accepts it).g
t_fallback_actions_load_config(TCConfig) ->
    #{
        type := ActionType,
        name := ActionName,
        config := ActionConfig0,
        connector_type := ConnectorType,
        connector_name := ConnectorName,
        connector_config := ConnectorConfig
    } =
        emqx_bridge_v2_testlib:get_common_values_with_configs(TCConfig),
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
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig)}
    ),
    ok = emqx_cth_cluster:stop(Nodes),
    ok.

t_conf_no_resource_leak(TCConfig) ->
    FindTopicIndexTab = fun() ->
        lists:filter(
            fun(T) -> ets:info(T, name) =:= emqx_topic_index end,
            ets:all()
        )
    end,
    Tabs1 = FindTopicIndexTab(),
    {201, _} = create_connector_api(TCConfig, #{
        %% invalid server
        <<"server">> => <<"127.0.0.1:6060">>,
        <<"pool_size">> => 1
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    ?assertEqual(Tabs1, FindTopicIndexTab()).

t_conf_bridge_authn_anonymous(TCConfig) ->
    ok = hook_authenticate(),
    {201, _} = create_connector_api(TCConfig, #{<<"pool_size">> => 1}),
    {201, _} = create_action_api(TCConfig, #{}),
    ?assertReceive(
        {authenticate, #{username := undefined, password := undefined}}
    ).

t_conf_bridge_authn_password(TCConfig) ->
    Username = <<"user1">>,
    Password = <<"from-here">>,
    ok = hook_authenticate(),
    {201, _} = create_connector_api(TCConfig, #{
        <<"username">> => Username,
        <<"password">> => Password,
        <<"pool_size">> => 1
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    ?assertReceive(
        {authenticate, #{username := Username, password := Password}}
    ).

t_conf_bridge_authn_passfile(TCConfig) ->
    DataDir = get_config(data_dir, TCConfig),
    Username = <<"user2">>,
    PasswordFilename = filename:join(DataDir, "password"),
    ok = filelib:ensure_dir(PasswordFilename),
    Password = <<"from-there">>,
    ok = file:write_file(PasswordFilename, Password),
    PasswordURI = iolist_to_binary(["file://", PasswordFilename]),
    ok = hook_authenticate(),
    {201, _} = create_connector_api(TCConfig, #{
        <<"username">> => Username,
        <<"password">> => PasswordURI,
        <<"pool_size">> => 1
    }),
    {201, _} = create_action_api(TCConfig, #{}),
    ?assertReceive(
        {authenticate, #{username := Username, password := Password}}
    ),
    {200, #{
        <<"status">> := <<"disconnected">>,
        <<"status_reason">> := Reason
    }} = update_connector_api(TCConfig, #{
        <<"username">> => <<"">>,
        <<"password">> => <<"file://im/pretty/sure/theres/no/such/file">>
    }),
    ?assertMatch({match, _}, re:run(Reason, <<"failed_to_read_secret_file">>)).

t_egress_short_clientid(TCConfig) ->
    %% Name is short, expect the actual client ID in use is hashed from
    %% <name><nodename-hash>:<pool_worker_id>
    Name = <<"abc01234">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = iolist_to_binary([BaseId, $:, "1"]),
    ?assertMatch(<<"abc01234", _/binary>>, ExpectedClientId),
    test_egress_clientid(Name, _Prefix = <<>>, ExpectedClientId, TCConfig).

t_egress_long_clientid(TCConfig) ->
    %% Expect the actual client ID in use is hashed from
    %% <name><nodename-hash>:<pool_worker_id>
    Name = <<"abc012345678901234567890">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = emqx_bridge_mqtt_lib:bytes23(BaseId, 1),
    test_egress_clientid(Name, _Prefix = <<>>, ExpectedClientId, TCConfig).

t_egress_with_short_prefix(TCConfig) ->
    %% Expect the actual client ID in use is hashed from
    %% <prefix>head(sha1(<name><nodename-hash>:<pool_worker_id>), 16)
    Prefix = <<"012-">>,
    Name = <<"345">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = emqx_bridge_mqtt_lib:bytes23_with_prefix(Prefix, BaseId, 1),
    ?assertMatch(<<"012-", _/binary>>, ExpectedClientId),
    test_egress_clientid(Name, Prefix, ExpectedClientId, TCConfig).

t_egress_with_long_prefix(TCConfig) ->
    %% Expect the actual client ID in use is hashed from
    %% <prefix><name><nodename-hash>:<pool_worker_id>
    Prefix = <<"0123456789abcdef01234-">>,
    Name = <<"345">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = iolist_to_binary([Prefix, BaseId, <<":1">>]),
    test_egress_clientid(Name, Prefix, ExpectedClientId, TCConfig).

test_egress_clientid(Name, ClientIdPrefix, ExpectedClientId, TCConfig0) ->
    TCConfig = [{connector_name, Name} | TCConfig0],
    {201, _} = create_connector_api(TCConfig, #{
        <<"pool_size">> => 1,
        <<"clientid_prefix">> => ClientIdPrefix
    }),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_action_api(TCConfig, #{<<"connector">> => Name}),
    #{topic := LocalTopic} = simple_create_rule_api(TCConfig),
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    emqx:publish(emqx_message:make(LocalTopic, Payload)),
    {deliver, _, #message{from = From}} = ?assertReceive({deliver, _, _}),
    ?assertEqual(ExpectedClientId, From),
    ok.

t_mqtt_conn_bridge_egress_reconnect(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        <<"resource_opts">> => #{
            %% to make it check the healthy and reconnect quickly
            <<"health_check_interval">> => <<"500ms">>
        }
    }),
    RemotePrefix = <<"remote">>,
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{
            <<"topic">> => <<RemotePrefix/binary, "/${topic}">>,
            <<"payload">> => <<"${payload}">>,
            <<"qos">> => <<"${qos}">>,
            <<"retain">> => <<"${retain}">>
        },
        <<"resource_opts">> => #{
            %% to make it check the healthy and reconnect quickly
            <<"health_check_interval">> => <<"500ms">>,
            %% using a long time so we can test recovery
            <<"request_ttl">> => <<"15s">>,
            <<"query_mode">> => <<"sync">>,
            <<"resume_interval">> => <<"500ms">>
        }
    }),
    #{topic := LocalTopic} = simple_create_rule_api(TCConfig),
    %% we now test if the bridge works as expected
    RemoteTopic = emqx_topic:join([RemotePrefix, LocalTopic]),
    Payload0 = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload0)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    ?assertReceive({deliver, RemoteTopic, #message{payload = Payload0}}),

    %% verify the metrics of the bridge
    ?retry(
        300,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),

    %% stop the listener 1883 to make the bridge disconnected
    ok = emqx_listeners:stop_listener('tcp:default'),
    ct:sleep(1500),

    %% PUBLISH 2 messages to the 'local' broker, the messages should
    %% be enqueued and the resource will block
    {ok, SRef} =
        snabbkaffe:subscribe(
            fun
                (#{?snk_kind := buffer_worker_retry_inflight_failed}) ->
                    true;
                (#{?snk_kind := buffer_worker_flush_nack}) ->
                    true;
                (_) ->
                    false
            end,
            _NEvents = 2,
            _Timeout = 1_000
        ),
    Payload1 = <<"hello2">>,
    Payload2 = <<"hello3">>,
    %% We need to do it in other processes because it'll block due to
    %% the long timeout
    spawn(fun() -> emqx:publish(emqx_message:make(LocalTopic, Payload1)) end),
    spawn(fun() -> emqx:publish(emqx_message:make(LocalTopic, Payload2)) end),
    {ok, _} = snabbkaffe:receive_events(SRef),

    %% verify the metrics of the bridge, the message should be queued
    ?assertMatch(
        {200, #{<<"status">> := Status}} when
            Status == <<"connecting">> orelse Status == <<"disconnected">>,
        get_connector_api(TCConfig)
    ),
    %% matched >= 3 because of possible retries.
    ?retry(
        300,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := Matched,
                    <<"success">> := 1,
                    <<"failed">> := 0,
                    <<"queuing">> := Queuing,
                    <<"inflight">> := Inflight
                }
            }} when Matched >= 3 andalso Inflight + Queuing == 2,
            get_action_metrics_api(TCConfig)
        )
    ),

    %% start the listener 1883 to make the bridge reconnected
    ok = emqx_listeners:start_listener('tcp:default'),
    %% verify the metrics of the bridge, the 2 queued messages should have been sent
    ?retry(
        500,
        20,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    %% matched >= 3 because of possible retries.
    ?retry(
        300,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := Matched,
                    <<"success">> := 3,
                    <<"failed">> := 0,
                    <<"queuing">> := 0,
                    <<"retried">> := _
                }
            }} when Matched >= 3,
            get_action_metrics_api(TCConfig)
        )
    ),
    %% also verify the 2 messages have been sent to the remote broker
    ?assertReceive({deliver, RemoteTopic, #message{payload = Payload1}}),
    ?assertReceive({deliver, RemoteTopic, #message{payload = Payload2}}),
    ok.

t_mqtt_conn_bridge_egress_async_reconnect(TCConfig) ->
    Self = self(),

    ?check_trace(
        begin
            %% stop the listener 1883 to make the bridge disconnected
            ok = emqx_listeners:stop_listener('tcp:default'),

            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{
                    %% to make it check the healthy and reconnect quickly
                    <<"health_check_interval">> => <<"500ms">>
                }
            }),
            RemotePrefix = <<"remote">>,
            {201, _} = create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"topic">> => <<RemotePrefix/binary, "/${topic}">>,
                    <<"payload">> => <<"${payload}">>,
                    <<"qos">> => <<"${qos}">>,
                    <<"retain">> => <<"${retain}">>
                },
                <<"resource_opts">> => #{
                    %% to make it check the healthy and reconnect quickly
                    <<"health_check_interval">> => <<"500ms">>,
                    %% using a long time so we can test recovery
                    <<"request_ttl">> => <<"15s">>,
                    <<"query_mode">> => <<"async">>,
                    <<"resume_interval">> => <<"500ms">>
                }
            }),
            #{topic := LocalTopic} = simple_create_rule_api(TCConfig),
            %% we now test if the bridge works as expected
            RemoteTopic = emqx_topic:join([RemotePrefix, LocalTopic]),

            emqx:subscribe(RemoteTopic),

            Publisher = start_publisher(LocalTopic, 200, Self),
            ct:sleep(1000),

            ?retry(
                500,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := Status}} when
                        Status == <<"connecting">> orelse Status == <<"disconnected">>,
                    get_connector_api(TCConfig)
                )
            ),

            %% start the listener 1883 to make the bridge reconnected
            ok = emqx_listeners:start_listener('tcp:default'),
            ?retry(
                500,
                20,
                ?assertMatch(
                    {200, #{<<"status">> := <<"connected">>}},
                    get_connector_api(TCConfig)
                )
            ),

            N = stop_publisher(Publisher),

            %% all those messages should eventually be delivered
            lists:foreach(
                fun(I) ->
                    Payload = integer_to_binary(I),
                    ?assertReceive({deliver, RemoteTopic, #message{payload = Payload}})
                end,
                lists:seq(1, N)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_bridge_mqtt_connector_econnrefused_error, Trace)),
            ok
        end
    ),
    ok.

t_egress_mqtt_bridge_with_dummy_rule(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} = create_action_api(TCConfig, #{}),
    #{topic := LocalTopic} = simple_create_rule_api(<<"select x from \"${t}\"">>, TCConfig),
    %% PUBLISH a message to the rule.
    Payload = <<"hi">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    emqx:publish(emqx_message:make(LocalTopic, Payload)),
    %% we should receive a message on the "remote" broker, with specified topic
    ?assertReceive({deliver, RemoteTopic, _}),
    ok.

t_mqtt_egress_bridge_warns_clean_start(TCConfig) ->
    Action = fun() ->
        {201, _} = create_connector_api(TCConfig, #{<<"clean_start">> => false}),
        {201, _} = create_action_api(TCConfig, #{})
    end,
    {_, {ok, _}} =
        ?wait_async_action(
            Action(),
            #{?snk_kind := mqtt_clean_start_egress_action_warning},
            10_000
        ),
    ok.

t_mqtt_conn_bridge_egress_no_payload_template(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    %% `payload` template must not be specified here (empty string is not the same thing).
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} = create_action_api(TCConfig, #{}),
    #{topic := LocalTopic} = simple_create_rule_api(TCConfig),
    %% we now test if the bridge works as expected
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),
    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),
    %% we should receive a message on the "remote" broker, with specified topic
    {deliver, RemoteTopic, #message{payload = PayloadBin}} = ?assertReceive({deliver, _, _}),
    ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(PayloadBin)),
    %% verify the metrics of the bridge
    ?retry(
        300,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"matched">> := 1,
                    <<"success">> := 1,
                    <<"failed">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.
