%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_source_SUITE).

-moduledoc """
This suite holds test cases for the `mqtt` source.

For cases where a single connector has both actions and sources, see
`emqx_bridge_mqtt_hybrid_SUITE`.
""".

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, mqtt).
-define(CONNECTOR_TYPE_BIN, <<"mqtt">>).
-define(SOURCE_TYPE, mqtt).
-define(SOURCE_TYPE_BIN, <<"mqtt">>).

-define(local, local).
-define(cluster, cluster).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

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
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
    }),
    setup_auth_header(TCConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig}
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

source_config(Overrides) ->
    emqx_bridge_schema_testlib:mqtt_source_config(Overrides).

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

connector_resource_id(Config) ->
    emqx_bridge_v2_testlib:connector_resource_id(Config).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

update_source_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(Config, Overrides).

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

create_source_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(Config, Overrides).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

get_source_metrics_api(Config) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(
        Config
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

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

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, mqtt_connector_stopped).

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_consume(TCConfig) when is_list(TCConfig) ->
    #{<<"parameters">> := #{<<"topic">> := RemoteTopic}} =
        get_config(source_config, TCConfig),
    Payload = <<"hello">>,
    ProduceFn = fun() ->
        emqx:publish(emqx_message:make(RemoteTopic, Payload))
    end,
    CheckFn = fun(Message) ->
        ?assertMatch(#{topic := RemoteTopic, payload := Payload}, Message)
    end,
    Opts = #{
        produce_fn => ProduceFn,
        check_fn => CheckFn,
        produce_tracepoint => ?match_event(#{?snk_kind := "mqtt_ingress_processed_message"})
    },
    emqx_bridge_v2_testlib:t_consume(TCConfig, Opts).

t_mqtt_conn_bridge_ingress_full_context(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_source_api(TCConfig, #{}),
    #{id := RuleId, topic := RepublishTopic} = simple_create_rule_api(TCConfig),
    C = start_client(TCConfig),
    {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),
    emqtt:publish(C, RemoteTopic, #{'User-Property' => [{<<"a">>, <<"b">>}]}, <<"hello">>, [
        {qos, 1}
    ]),
    {publish, #{payload := PayloadBin}} = ?assertReceive({publish, _}),
    ?assertMatch(
        #{
            <<"dup">> := false,
            <<"id">> := _,
            <<"message_received_at">> := _,
            <<"payload">> := <<"hello">>,
            <<"pub_props">> := #{<<"User-Property">> := #{<<"a">> := <<"b">>}},
            <<"qos">> := 1,
            <<"retain">> := false,
            <<"server">> := <<"127.0.0.1:1883">>,
            <<"topic">> := RemoteTopic
        },
        emqx_utils_json:decode(PayloadBin)
    ),
    ?assertMatch(
        {200, #{
            <<"metrics">> := #{
                <<"matched">> := 0,
                <<"received">> := 1,
                <<"failed">> := 0,
                <<"success">> := 0
            }
        }},
        get_source_metrics_api(TCConfig)
    ),
    ?assertMatch(
        #{
            counters := #{
                'matched' := 1,
                'passed' := 1,
                'failed' := 0,
                'failed.exception' := 0,
                'failed.no_result' := 0,
                'actions.total' := 1,
                'actions.success' := 1,
                'actions.failed' := 0,
                'actions.failed.out_of_service' := 0,
                'actions.failed.unknown' := 0
            }
        },
        emqx_bridge_v2_testlib:get_rule_metrics(RuleId)
    ),
    ok.

t_mqtt_conn_bridge_ingress_shared_subscription(TCConfig) ->
    PoolSize = 4,
    {201, _} = create_connector_api(TCConfig, #{
        <<"pool_size">> => PoolSize
    }),
    RemoteTopic = <<"remote/topic">>,
    {201, _} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{
                <<"topic">> => iolist_to_binary(["$share/ingress/", RemoteTopic, "/#"]),
                <<"qos">> => 1
            }
        }),
    #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
    C = start_client(TCConfig),
    {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),

    Ns = lists:seq(1, 10),
    emqx_utils:pforeach(
        fun emqx:publish/1,
        [emqx_message:make(RemoteTopic, <<>>) || _ <- Ns]
    ),
    lists:foreach(fun(_) -> ?assertReceive({publish, _}) end, Ns),
    ?assertEqual(
        PoolSize,
        length(emqx_shared_sub:subscribers(<<"ingress">>, <<RemoteTopic/binary, "/#">>))
    ),
    ok.

t_mqtt_conn_bridge_ingress_downgrades_qos_2(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_source_api(TCConfig, #{<<"parameters">> => #{<<"qos">> => 2}}),
    #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
    C = start_client(TCConfig),
    {ok, _, [_]} = emqtt:subscribe(C, RepublishTopic, [{qos, 2}]),
    emqtt:publish(C, RemoteTopic, <<"hey">>, [{qos, 2}]),
    {publish, #{payload := PayloadBin}} = ?assertReceive({publish, _}),
    ?assertMatch(#{<<"qos">> := 1}, emqx_utils_json:decode(PayloadBin)),
    ok.

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

    %% Sources using this connector should be created just fine as well
    ct:pal("creating source"),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} = create_source_api(TCConfig, #{}),
    ct:pal("checking source health"),
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
            get_source_api(TCConfig)
        )
    ),

    ct:pal("creating rule"),
    #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
    C0 = start_client(N1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C0, RepublishTopic, ?QOS_1),
    Clients = lists:map(fun start_client/1, Nodes),

    ct:pal("publishing messages"),
    lists:foreach(
        fun({N, C}) ->
            {ok, _} = emqtt:publish(C, RemoteTopic, integer_to_binary(N), ?QOS_1)
        end,
        lists:enumerate(Clients)
    ),
    %% We've sent 3 messages (one for each node), and each node with a static clientid
    %% forwards the messages it receives to the control subscriber `C0'.
    NumClients = length(Nodes),
    NodesWithClientids = 2,
    ExpectedPublishes = NumClients * NodesWithClientids,
    Publishes0 = emqx_common_test_helpers:wait_publishes(ExpectedPublishes, 1_000),
    Publishes =
        maps:groups_from_list(
            fun(#{<<"node">> := N}) -> N end,
            fun(#{<<"payload">> := P}) -> P end,
            lists:map(
                fun(#{payload := P}) -> emqx_utils_json:decode(P) end,
                Publishes0
            )
        ),
    ?assertMatch(
        #{
            N2Bin := [<<"1">>, <<"2">>, <<"3">>],
            N3Bin := [<<"1">>, <<"2">>, <<"3">>]
        },
        Publishes
    ),
    ?assertEqual([], emqx_common_test_helpers:wait_publishes(10, 100)),
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{<<"received">> := 6},
                <<"node_metrics">> := [
                    #{<<"metrics">> := #{<<"received">> := 0}},
                    #{<<"metrics">> := #{<<"received">> := 3}},
                    #{<<"metrics">> := #{<<"received">> := 3}}
                ]
            }},
            get_source_metrics_api(TCConfig)
        )
    ),

    ok.

t_connect_with_more_clients_than_the_broker_accepts(TCConfig) ->
    OrgConf = emqx_mgmt_listeners_conf:get_raw(tcp, default),
    on_exit(fun() ->
        emqx_mgmt_listeners_conf:update(tcp, default, OrgConf)
    end),
    NewConf = OrgConf#{<<"max_connections">> => 3},
    {ok, _} = emqx_mgmt_listeners_conf:update(tcp, default, NewConf),
    ?check_trace(
        #{timetrap => 10_000},
        begin
            ?assertMatch(
                {201, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"Your MQTT connection attempt was unsuccessful", _/binary>>
                }},
                create_connector_api(
                    TCConfig,
                    #{<<"pool_size">> => 100}
                )
            ),
            ?block_until(#{?snk_kind := emqx_bridge_mqtt_connector_tcp_closed}),
            ?assertMatch(
                {200, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"Your MQTT connection attempt was unsuccessful", _/binary>>
                }},
                get_connector_api(TCConfig)
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_bridge_mqtt_connector_tcp_closed, Trace)),
            ok
        end
    ),
    ok.

t_shared_subscription(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{<<"pool_size">> => 1}),
    SharedTopic1 = <<"$share/t/test/#">>,
    {201, _} =
        create_source_api(TCConfig, #{
            <<"parameters">> => #{
                <<"topic">> => SharedTopic1
            }
        }),
    #{topic := RepublishTopic} = simple_create_rule_api(TCConfig),
    Client = start_client(TCConfig),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, RepublishTopic, [{qos, 1}]),

    %% Check that it's working as intended.
    PublishTopic = <<"test/1">>,
    {ok, _} = emqtt:publish(Client, PublishTopic, <<"1">>, [{qos, 1}]),
    {publish, #{payload := PayloadBin1}} = ?assertReceive({publish, _}),
    ?assertMatch(#{<<"payload">> := <<"1">>}, emqx_utils_json:decode(PayloadBin1)),
    ?assertNotReceive({publish, _}),

    %% Update the shared subscription; should still republish only one message.
    SharedTopic2 = <<"$share/t/test/1">>,
    {200, _} = update_source_api(TCConfig, #{
        <<"parameters">> => #{
            <<"topic">> => SharedTopic2
        }
    }),

    {ok, _} = emqtt:publish(Client, PublishTopic, <<"2">>, [{qos, 1}]),
    {publish, #{payload := PayloadBin2}} = ?assertReceive({publish, _}),
    ?assertMatch(#{<<"payload">> := <<"2">>}, emqx_utils_json:decode(PayloadBin2)),
    ?assertNotReceive({publish, _}),

    ok.
