%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_v2_subscriber_SUITE).

-feature(maybe_expr, enable).
-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("stdlib/include/assert.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

-define(assertReceivePublish(EXPR, GUARD, EXTRA, TIMEOUT),
    (fun() ->
        lists:foreach(
            fun(Pub0) ->
                Pub = maps:update_with(
                    payload,
                    fun emqx_utils_json:decode/1,
                    Pub0
                ),
                self() ! {decoded, Pub}
            end,
            drain_publishes([])
        ),
        {decoded, __X} = ?assertReceive({decoded, EXPR} when ((GUARD)), TIMEOUT, EXTRA),
        __X
    end)()
).
-define(assertReceivePublish(EXPR), ?assertReceivePublish(EXPR, true, #{}, 1_000)).
-define(assertReceivePublish(EXPR, EXTRA), ?assertReceivePublish(EXPR, true, EXTRA, 1_000)).
-define(assertReceivePublish(EXPR, GUARD, EXTRA), ?assertReceivePublish(EXPR, GUARD, EXTRA, 1_000)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, cluster},
        {group, local}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    ClusterTCs = cluster_testcases(),
    [
        {cluster, ClusterTCs},
        {local, AllTCs -- ClusterTCs}
    ].

cluster_testcases() ->
    [t_static_clientids, t_resubscribe_on_fast_failure].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Group, Config) ->
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
init_per_group(local, Config) ->
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
    [{apps, Apps} | Config].

end_per_group(cluster, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_group(local, Config) ->
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, Config) ->
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = iolist_to_binary([atom_to_binary(TestCase), UniqueNum]),
    ConnectorConfig = connector_config(),
    SourceConfig = source_config(#{connector => Name}),
    case ?config(nodes, Config) of
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun);
        _ ->
            ok
    end,
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {source_type, mqtt},
        {source_name, Name},
        {source_config, SourceConfig},
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
            emqx_bridge_v2_testlib:delete_all_bridges_and_connectors();
        _ ->
            emqx_utils:pmap(
                fun(N) ->
                    ?ON(N, emqx_bridge_v2_testlib:delete_all_bridges_and_connectors())
                end,
                Nodes
            )
    end,
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

drain_publishes(Acc) ->
    receive
        {publish, Msg} ->
            drain_publishes([Msg | Acc])
    after 200 ->
        lists:reverse(Acc)
    end.

connector_config() ->
    #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"connect_timeout">> => <<"5s">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    }.

source_config(Overrides0) ->
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
                <<"health_check_interval">> => <<"15s">>,
                <<"resume_interval">> => <<"15s">>
            }
        },
    maps:merge(CommonConfig, Overrides).

replace(Key, Value, Proplist) ->
    lists:keyreplace(Key, 1, Proplist, {Key, Value}).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

bridge_id(Config) ->
    Type = ?config(source_type, Config),
    Name = ?config(source_name, Config),
    emqx_bridge_resource:bridge_id(Type, Name).

hookpoint(Config) ->
    BridgeId = bridge_id(Config),
    emqx_bridge_resource:bridge_hookpoint(BridgeId).

simplify_result(Res) ->
    emqx_bridge_v2_testlib:simplify_result(Res).

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
    emqx_bridge_mqtt_v2_publisher_SUITE:get_tcp_mqtt_port(Node).

create_connector_api(Config, Overrides) ->
    emqx_bridge_mqtt_v2_publisher_SUITE:create_connector_api(Config, Overrides).

get_connector_api(Config) ->
    emqx_bridge_mqtt_v2_publisher_SUITE:get_connector_api(Config).

connector_resource_id(Config) ->
    emqx_bridge_mqtt_v2_publisher_SUITE:connector_resource_id(Config).

create_source_api(Config) ->
    create_source_api(Config, _Overrides = #{}).

create_source_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(Config, Overrides).

delete_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    Opts = #{query_params => #{<<"also_delete_dep_actions">> => <<"true">>}},
    emqx_bridge_v2_testlib:delete_kind_api(source, Type, Name, Opts).

get_source_api(Config) ->
    #{
        type := Type,
        name := Name
    } = emqx_bridge_v2_testlib:get_common_values(Config),
    simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

get_source_metrics_api(Config) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(
        Config
    ).

connect_client(Node) ->
    emqx_bridge_mqtt_v2_publisher_SUITE:connect_client(Node).

create_rule_and_action_http(Config, Opts) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(
        ?config(source_type, Config), <<"">>, Config, Opts
    ).

update_source_api(Config, Overrides) ->
    simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(Config, Overrides)
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_create_via_http(Config) ->
    ConnectorName = ?config(connector_name, Config),
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{
                    <<"enable">> := true,
                    <<"status">> := <<"connected">>
                }
            ]}},
        emqx_bridge_v2_testlib:list_bridges_http_api_v1()
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, [#{<<"enable">> := true}]}},
        emqx_bridge_v2_testlib:list_connectors_http_api()
    ),

    NewSourceName = <<"my_other_source">>,
    {ok, {{_, 201, _}, _, _}} =
        emqx_bridge_v2_testlib:create_kind_api(
            replace(source_name, NewSourceName, Config)
        ),
    ?assertMatch(
        {ok,
            {{_, 200, _}, _, [
                #{<<"connector">> := ConnectorName},
                #{<<"connector">> := ConnectorName}
            ]}},
        emqx_bridge_v2_testlib:list_sources_http_api()
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, []}},
        emqx_bridge_v2_testlib:list_bridges_http_api_v1()
    ),
    ok.

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, mqtt_connector_stopped),
    ok.

t_receive_via_rule(Config) ->
    SourceConfig = ?config(source_config, Config),
    ?check_trace(
        begin
            {ok, {{_, 201, _}, _, _}} = emqx_bridge_v2_testlib:create_connector_api(Config),
            {ok, {{_, 201, _}, _, _}} = emqx_bridge_v2_testlib:create_kind_api(Config),
            Hookpoint = hookpoint(Config),
            RepublishTopic = <<"rep/t">>,
            RemoteTopic = emqx_utils_maps:deep_get(
                [<<"parameters">>, <<"topic">>],
                SourceConfig
            ),
            RuleOpts = #{
                sql => <<"select * from \"", Hookpoint/binary, "\"">>,
                actions => [
                    %% #{function => console},
                    #{
                        function => republish,
                        args => #{
                            topic => RepublishTopic,
                            payload => <<"${.}">>,
                            qos => 0,
                            retain => false,
                            user_properties => <<"${.pub_props.'User-Property'}">>
                        }
                    }
                ]
            },
            {ok, {{_, 201, _}, _, #{<<"id">> := RuleId}}} =
                emqx_bridge_v2_testlib:create_rule_api(RuleOpts),
            on_exit(fun() -> emqx_rule_engine:delete_rule(RuleId) end),
            {ok, Client} = emqtt:start_link([{proto_ver, v5}]),
            {ok, _} = emqtt:connect(Client),
            {ok, _, [?RC_GRANTED_QOS_0]} = emqtt:subscribe(Client, RepublishTopic),
            ok = emqtt:publish(
                Client,
                RemoteTopic,
                #{'User-Property' => [{<<"key">>, <<"value">>}]},
                <<"mypayload">>,
                _Opts = []
            ),
            {publish, Msg} =
                ?assertReceive(
                    {publish, #{
                        topic := RepublishTopic,
                        retain := false,
                        qos := 0,
                        properties := #{'User-Property' := [{<<"key">>, <<"value">>}]}
                    }}
                ),
            Payload = emqx_utils_json:decode(maps:get(payload, Msg)),
            ?assertMatch(
                #{
                    <<"event">> := Hookpoint,
                    <<"payload">> := <<"mypayload">>
                },
                Payload
            ),
            emqtt:stop(Client),
            ok
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind("action_references_nonexistent_bridges", Trace)),
            %% We don't have the hidden, legacy `local' config set, so we shouldn't
            %% attempt to publish directly.
            ?assertEqual([], ?of_kind(mqtt_ingress_publish_local, Trace)),
            ok
        end
    ),
    ok.

t_connect_with_more_clients_than_the_broker_accepts(Config) ->
    Name = ?config(connector_name, Config),
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
                simplify_result(
                    emqx_bridge_v2_testlib:create_connector_api(
                        Config,
                        #{<<"pool_size">> => 100}
                    )
                )
            ),
            ?block_until(#{?snk_kind := emqx_bridge_mqtt_connector_tcp_closed}),
            ?assertMatch(
                {200, #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> :=
                        <<"Your MQTT connection attempt was unsuccessful", _/binary>>
                }},
                simplify_result(emqx_bridge_v2_testlib:get_connector_api(mqtt, Name))
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_bridge_mqtt_connector_tcp_closed, Trace)),
            ok
        end
    ),

    ok.

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

    %% Sources using this connector should be created just fine as well
    ct:pal("creating source"),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} = create_source_api(Config),
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
            get_source_api(Config)
        )
    ),

    RepublishTopic = <<"rep/t">>,
    ct:pal("creating rule"),
    {ok, _} = create_rule_and_action_http(Config, #{
        sql => iolist_to_binary(
            io_lib:format(
                "select * from \"~s\"",
                [hookpoint(Config)]
            )
        ),
        overrides => #{
            actions => [
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> =>
                        #{
                            <<"topic">> => RepublishTopic,
                            <<"payload">> => <<>>,
                            <<"qos">> => 1,
                            <<"retain">> => false
                        }
                }
            ]
        }
    }),

    C0 = connect_client(N1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(C0, RepublishTopic, ?QOS_1),
    Clients = lists:map(fun connect_client/1, Nodes),

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
    #{N2Bin := Msgs2, N3Bin := Msgs3} = Publishes,
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>], lists:sort(Msgs2)),
    ?assertEqual([<<"1">>, <<"2">>, <<"3">>], lists:sort(Msgs3)),
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
            get_source_metrics_api(Config)
        )
    ),

    ok.

%% Checks that we're able to set the no-local `nl' flag when subscribing.
t_no_local(Config) ->
    ConnectorName = ?config(connector_name, Config),
    %% Only 1 worker to avoid the other workers multiplying the message.
    {201, _} = create_connector_api(Config, #{
        <<"pool_size">> => 1
    }),
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        create_source_api(Config, #{
            <<"parameters">> => #{
                <<"no_local">> => true
            }
        }),
    %% Must be the same topic
    ActionParams = [
        {action_name, <<"t_no_local">>},
        {action_type, <<"mqtt">>},
        {action_config,
            emqx_bridge_mqtt_v2_publisher_SUITE:action_config(#{
                <<"connector">> => ConnectorName
            })}
    ],
    {201, #{<<"parameters">> := #{<<"topic">> := RemoteTopic}}} =
        emqx_bridge_mqtt_v2_publisher_SUITE:create_action_api(ActionParams, _Overrides = #{}),

    RuleTopicPublisher = <<"publisher/t">>,
    {ok, _} = emqx_bridge_mqtt_v2_publisher_SUITE:create_rule_and_action_http(
        ActionParams, RuleTopicPublisher, #{}
    ),
    RuleTopicSubscriber = <<"subscriber/t">>,
    {ok, _} = create_rule_and_action_http(Config, #{
        sql => iolist_to_binary(
            io_lib:format(
                "select * from \"~s\"",
                [hookpoint(Config)]
            )
        ),
        overrides => #{
            actions => [
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> =>
                        #{
                            <<"topic">> => RuleTopicSubscriber,
                            <<"payload">> => <<>>,
                            <<"qos">> => 1,
                            <<"retain">> => false
                        }
                }
            ]
        }
    }),
    %% Should not receive own messages echoed back.
    ok = emqx:subscribe(RuleTopicSubscriber, #{qos => ?QOS_1, nl => 1}),
    %% Should receive only 1 message copy.
    ok = emqx:subscribe(RemoteTopic),
    emqx:publish(emqx_message:make(<<"external_client">>, ?QOS_1, RuleTopicPublisher, <<"hey">>)),
    ?assertReceive({deliver, RemoteTopic, _}),
    ?assertNotReceive({deliver, RemoteTopic, _}),
    ?assertNotReceive({deliver, RuleTopicSubscriber, _}),
    ok.

t_shared_subscription(Config) ->
    {201, _} = create_connector_api(Config, #{<<"pool_size">> => 1}),
    SharedTopic1 = <<"$share/t/test/#">>,
    {201, _} =
        create_source_api(Config, #{
            <<"parameters">> => #{
                <<"topic">> => SharedTopic1
            }
        }),
    RepublishTopic = <<"share/output">>,
    {ok, _} = create_rule_and_action_http(Config, #{
        sql => iolist_to_binary(
            io_lib:format(
                "select * from \"~s\"",
                [hookpoint(Config)]
            )
        ),
        overrides => #{
            actions => [
                #{
                    <<"function">> => <<"republish">>,
                    <<"args">> =>
                        #{
                            <<"topic">> => RepublishTopic,
                            <<"payload">> => <<"republish_${.payload}">>,
                            <<"qos">> => 1,
                            <<"retain">> => false
                        }
                }
            ]
        }
    }),

    {ok, Client} = emqtt:start_link([{proto_ver, v5}]),
    {ok, _} = emqtt:connect(Client),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client, RepublishTopic, [{qos, 1}]),

    %% Check that it's working as intended.
    PublishTopic = <<"test/1">>,
    {ok, _} = emqtt:publish(Client, PublishTopic, <<"1">>, [{qos, 1}]),
    ?assertReceive({publish, #{payload := <<"republish_1">>}}),
    ?assertNotReceive({publish, _}),

    %% Update the shared subscription; should still republish only one message.
    SharedTopic2 = <<"$share/t/test/1">>,
    {200, _} = update_source_api(Config, #{
        <<"parameters">> => #{
            <<"topic">> => SharedTopic2
        }
    }),

    {ok, _} = emqtt:publish(Client, PublishTopic, <<"2">>, [{qos, 1}]),
    ?assertReceive({publish, #{payload := <<"republish_2">>}}),
    ?assertNotReceive({publish, _}),

    ok.

%% Verifies that we re-subscribe source topics when reconnecting after a connection
%% failure that happens before connector health check can catch it.
t_resubscribe_on_fast_failure(TCConfig) when is_list(TCConfig) ->
    Nodes = get_config(nodes, TCConfig),
    lists:foreach(
        fun({CleanStart, ProtoVer}) ->
            do_t_resubscribe_on_fast_failure(CleanStart, ProtoVer, TCConfig),
            ?ON_ALL(Nodes, emqx_bridge_v2_testlib:delete_all_rules()),
            ?ON_ALL(Nodes, emqx_bridge_v2_testlib:delete_all_bridges_and_connectors()),
            emqx_common_test_helpers:call_janitor()
        end,
        [
            {CleanStart, ProtoVer}
         || CleanStart <- [true, false],
            ProtoVer <- [<<"v3">>, <<"v5">>]
        ]
    ).

do_t_resubscribe_on_fast_failure(CleanStart, ProtoVer, TCConfig) ->
    ct:pal("testing clean start = ~p and proto ver = ~p", [CleanStart, ProtoVer]),
    #{<<"pool_size">> := PoolSize} = get_config(connector_config, TCConfig),
    [N1 | _] = Nodes = get_config(nodes, TCConfig),
    ?ON(N1, emqx_logger:set_log_level(debug)),
    NumNodes = length(Nodes),
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, TCConfig),
    on_exit(fun() -> emqx_cth_suite:clean_work_dir(WorkDir) end),
    SourceNSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {source1, #{
                role => core,
                apps => [
                    emqx,
                    {emqx_conf,
                        "log.console.level = debug\n"
                        "log.console.enable = true"},
                    emqx_connector,
                    emqx_bridge_mqtt,
                    emqx_bridge
                ],
                base_port => 20100
            }}
        ],
        #{work_dir => WorkDir}
    ),
    [Source] = emqx_cth_cluster:start(SourceNSpecs),
    on_exit(fun() -> emqx_cth_cluster:stop([Source]) end),

    Port = get_tcp_mqtt_port(Source),
    {201, #{<<"status">> := <<"connected">>}} = create_connector_api(TCConfig, #{
        <<"server">> => <<"127.0.0.1:", (integer_to_binary(Port))/binary>>,
        <<"proto_ver">> => ProtoVer,
        <<"clean_start">> => CleanStart,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"10m">>}
    }),
    %% Let's create two sources; their reconnect should be independent.
    SourceNameA = <<"a">>,
    SourceNameB = <<"b">>,
    TCConfigA = lists:keyreplace(source_name, 1, TCConfig, {source_name, SourceNameA}),
    TCConfigB = lists:keyreplace(source_name, 1, TCConfig, {source_name, SourceNameB}),
    {201, #{<<"status">> := <<"connected">>}} =
        create_source_api(TCConfigA, #{
            <<"parameters">> => #{
                <<"topic">> => iolist_to_binary(["$queue/t/#"]),
                <<"qos">> => 1
            },
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"10m">>}
        }),
    {201, #{<<"status">> := <<"connected">>}} =
        create_source_api(TCConfigB, #{
            <<"parameters">> => #{
                <<"topic">> => <<"u/#">>,
                <<"qos">> => 1
            },
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"10m">>}
        }),
    #{topic := RepublishTopicA} = simple_create_rule_api(TCConfigA),
    #{topic := RepublishTopicB} = simple_create_rule_api(TCConfigB),
    Sub = start_client(N1, #{clientid => <<"subscriber">>}),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, RepublishTopicA, ?QOS_1),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Sub, RepublishTopicB, ?QOS_1),
    Pub1 = start_client(Source, #{clientid => <<"publisher">>}),
    emqtt:publish(Pub1, <<"t/a">>, <<"1">>),
    emqtt:publish(Pub1, <<"u/a">>, <<"2">>),
    %% Sanity check: sources should be working.
    ?assertReceivePublish(
        #{
            payload := #{<<"payload">> := <<"1">>},
            topic := RepublishTopicA
        }
    ),
    %% We receive this multiple times because it's an ordinary subscription, so each node
    %% receives and forwards it.
    lists:foreach(
        fun(_) ->
            ?assertReceivePublish(
                #{
                    payload := #{<<"payload">> := <<"2">>},
                    topic := RepublishTopicB
                }
            )
        end,
        lists:seq(1, NumNodes)
    ),
    emqtt:stop(Pub1),

    %% Now, we restart the source's node.  The health check interval is large enough so
    %% that it doesn't "notice" that the connection went down.
    ct:pal("Restarting source node (1)"),
    {ok, SRef0} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "mqtt_source_reconnected"}),
        %% 2 sources with 3 workers each
        2 * PoolSize,
        %% Timeout
        5_000
    ),
    [Source] = emqx_cth_cluster:restart(SourceNSpecs),

    %% Source should recover.
    {ok, _} = snabbkaffe:receive_events(SRef0),
    ct:pal("Restarted source node (1)"),

    Pub2 = start_client(Source),
    emqtt:publish(Pub2, <<"t/a">>, <<"3">>),
    emqtt:publish(Pub2, <<"u/a">>, <<"4">>),
    emqtt:stop(Pub2),

    ?assertReceivePublish(
        #{
            payload := #{<<"payload">> := <<"3">>},
            topic := RepublishTopicA
        },
        true,
        #{},
        3_000
    ),
    lists:foreach(
        fun(N) ->
            ct:pal("expecting B message ~b", [N]),
            ?assertReceivePublish(
                #{
                    payload := #{<<"payload">> := <<"4">>},
                    topic := RepublishTopicB
                }
            )
        end,
        lists:seq(1, NumNodes)
    ),

    %% Remove one of the sources and restart again.  Removing the reconnect callback of
    %% one shouldn't affect the other.
    ct:pal("Deleting source"),
    {204, _} = delete_source_api(TCConfigB),

    ct:pal("Restarting source node (2)"),
    {ok, SRef1} = snabbkaffe:subscribe(
        ?match_event(#{?snk_kind := "mqtt_source_reconnected"}),
        %% 1 source with 3 workers each
        1 * PoolSize,
        %% Timeout
        5_000
    ),
    [Source] = emqx_cth_cluster:restart(SourceNSpecs),
    {ok, _} = snabbkaffe:receive_events(SRef1),
    ct:pal("Restarted source node (2)"),

    Pub3 = start_client(Source),
    emqtt:publish(Pub3, <<"t/a">>, <<"5">>),
    emqtt:publish(Pub3, <<"u/a">>, <<"6">>),
    emqtt:stop(Pub3),

    ?assertReceivePublish(
        #{
            payload := #{<<"payload">> := <<"5">>},
            topic := RepublishTopicA
        },
        true,
        #{},
        3_000
    ),
    ?assertNotReceive({publish, _}),

    ok.
