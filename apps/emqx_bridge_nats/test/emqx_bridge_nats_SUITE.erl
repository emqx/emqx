%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_nats_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("../src/emqx_bridge_nats.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(NATS_HOST, "nats-bridge-js").
-define(NATS_PORT, 4222).
-define(NATS_CA_CERT, "/emqx/.ci/docker-compose-file/certs/cacert.pem").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).
-define(RECONNECT_PROXY, "nats_bridge_reconnect").
-define(JETSTREAM_PROXY, "nats_bridge_js").

all() -> [{group, local}, {group, cluster}].

suite() -> [{timetrap, {seconds, 60}}].

groups() ->
    emqx_bridge_v2_testlib:local_and_cluster_groups(?MODULE, local, cluster).

init_per_suite(Config) ->
    wait_for_port(?NATS_HOST, ?NATS_PORT),
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(local, Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_nats,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config];
init_per_group(cluster, Config) ->
    emqx_cth_suite:load_apps([emqx_bridge_nats]),
    Nodes = emqx_cth_cluster:start(
        [
            {nats_credentials_1, #{
                apps => cluster_app_specs() ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }},
            {nats_credentials_2, #{apps => cluster_app_specs()}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{cluster_nodes, Nodes} | Config].

end_per_group(local, Config) ->
    emqx_cth_suite:stop(?config(apps, Config)),
    ok;
end_per_group(cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster_nodes, Config)).

init_per_testcase(TestCase, Config) ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT),
    case ?config(cluster_nodes, Config) of
        undefined ->
            on_exit(fun emqx_bridge_v2_testlib:delete_all_bridges_and_connectors/0);
        [Node | _] ->
            emqx_bridge_v2_testlib:set_auth_header_getter(fun() ->
                ?ON(Node, emqx_mgmt_api_test_util:auth_header_())
            end),
            on_exit(fun() ->
                ?ON(Node, emqx_bridge_v2_testlib:delete_all_bridges_and_connectors())
            end)
    end,
    Name = atom_to_binary(TestCase),
    ConnectorConfig = emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN,
        Name,
        #{
            <<"enable">> => true,
            <<"servers">> => <<"nats-bridge-js:4222">>,
            <<"pool_size">> => 1,
            <<"connect_timeout">> => <<"2s">>,
            <<"authentication">> => <<"none">>,
            <<"ssl">> => #{<<"enable">> => false},
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"1s">>}
        }
    ),
    ActionConfig = emqx_bridge_v2_testlib:parse_and_check(
        action,
        ?ACTION_TYPE,
        Name,
        #{
            <<"enable">> => true,
            <<"connector">> => Name,
            <<"parameters">> => #{
                <<"subject">> => <<"emqx.events">>,
                <<"payload_template">> => <<"${.payload}">>,
                <<"headers">> => []
            },
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"sync">>,
                <<"batch_size">> => 10,
                <<"batch_time">> => <<"100ms">>,
                <<"request_ttl">> => <<"60s">>
            }
        }
    ),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, Name},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, Name},
        {action_config, ActionConfig}
        | Config
    ].

end_per_testcase(_TestCase, Config) ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT),
    emqx_common_test_helpers:call_janitor(),
    emqx_bridge_v2_testlib:clear_auth_header_getter(),
    lists:foreach(
        fun(Node) ->
            ?assertEqual([], ?ON(Node, emqx_bridge_v2:list(?global_ns, actions))),
            ?assertEqual([], ?ON(Node, emqx_connector:list(?global_ns)))
        end,
        proplists:get_value(cluster_nodes, Config, [])
    ),
    ok.

%%--------------------------------------------------------------------
%% Core NATS publishing
%%--------------------------------------------------------------------

t_ssl_enabled_by_default(Config) ->
    RawConfig = maps:remove(<<"ssl">>, ?config(connector_config, Config)),
    CheckedConfig = emqx_bridge_v2_testlib:parse_and_check_connector(
        ?CONNECTOR_TYPE_BIN, ?config(connector_name, Config), RawConfig
    ),
    ?assertMatch(#{<<"ssl">> := #{<<"enable">> := true}}, CheckedConfig).

t_core_publish(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.sensor/1/data">>, #{}),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.${.topic}">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"headers">> => [
                #{<<"key">> => <<"x-topic">>, <<"value">> => <<"${.topic}">>}
            ]
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-core">>),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-core">>),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-core">>),
    lists:foreach(
        fun(_) ->
            ?assertMatch(
                {enats_client, Client,
                    {message, #{
                        subject := <<"emqx.sensor/1/data">>,
                        payload := <<"hello-core">>,
                        headers := [{<<"x-topic">>, <<"sensor/1/data">>}]
                    }}},
                receive_message()
            )
        end,
        lists:seq(1, 3)
    ),
    ok = enats_client:stop(Client).

t_core_concurrent_publish(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.concurrent">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"subject">> => <<"emqx.concurrent">>},
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"worker_pool_size">> => 16
            }
        }
    ),
    {ok, _} = create_rule(Config, <<"nats/concurrent">>),
    Publisher = mqtt_client(),
    Payloads = [integer_to_binary(N) || N <- lists:seq(1, 32)],
    publish_mqtt_concurrently(Publisher, <<"nats/concurrent">>, Payloads),
    ?assertEqual(lists:sort(Payloads), lists:sort(receive_payloads(length(Payloads), []))),
    ok = enats_client:stop(Client).

t_core_batch_callback_mode(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.async">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"subject">> => <<"emqx.async">>},
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"async">>,
                <<"batch_size">> => 3,
                <<"batch_time">> => <<"1s">>,
                <<"worker_pool_size">> => 2
            }
        }
    ),
    {ok, _} = create_rule(Config, <<"sensor/+/async">>),
    ok = snabbkaffe:start_trace(),
    try
        Publisher = mqtt_client(),
        lists:foreach(
            fun(N) ->
                publish_mqtt(Publisher, <<"sensor/1/async">>, integer_to_binary(N))
            end,
            lists:seq(1, 3)
        ),
        ?assertEqual(
            lists:sort([<<"1">>, <<"2">>, <<"3">>]),
            lists:sort(receive_payloads(3, []))
        ),
        Trace = snabbkaffe:collect_trace(),
        ?assert(
            lists:any(
                fun
                    (#{?snk_kind := call_batch_query}) -> true;
                    (_) -> false
                end,
                Trace
            )
        ),
        ?assertEqual([], ?of_kind(call_batch_query_async, Trace))
    after
        snabbkaffe:stop(),
        ok = enats_client:stop(Client)
    end.

t_invalid_connector_config(Config) ->
    {400, Response} = create_connector(
        Config,
        #{<<"authentication">> => #{<<"mechanism">> => <<"unsupported">>}}
    ),
    ?assertMatch(
        #{<<"message">> := #{<<"field_name">> := <<"mechanism">>}},
        Response
    ).

t_invalid_authentication_is_redacted(Config) ->
    Secret = <<"nats-authentication-must-not-leak">>,
    {400, Response} = create_connector(
        Config,
        #{<<"authentication">> => #{<<"password">> => Secret}}
    ),
    ?assertEqual(nomatch, binary:match(emqx_utils_json:encode(Response), Secret)).

t_core_batch_template_failure(Config) ->
    {201, _} = create_connector(Config),
    {201, _} = create_action(Config, #{
        <<"parameters">> => #{<<"subject">> => <<"${.missing}">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"batch_size">> => 2,
            <<"batch_time">> => <<"1s">>,
            <<"worker_pool_size">> => 1
        }
    }),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt_concurrently(Publisher, <<"sensor/1/data">>, [<<"one">>, <<"two">>]),
        {ok, #{result := Results}} = ?block_until(
            #{?snk_kind := nats_connector_query_return, batch := true, batch_size := 2}, 5000
        ),
        ?assertMatch(
            [
                {error, {unrecoverable_error, {template_error, _}}},
                {error, {unrecoverable_error, {template_error, _}}}
            ],
            Results
        )
    after
        snabbkaffe:stop()
    end.

t_jetstream_batch_partial_failure(Config) ->
    {ok, Client} = nats_client(Config),
    on_exit(fun() -> enats_client:stop(Client) end),
    ok = create_stream(Client),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(Config, #{
        <<"parameters">> => #{
            <<"subject">> => <<"${.topic}">>, <<"delivery_mode">> => <<"jetstream">>
        },
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"batch_size">> => 3,
            <<"batch_time">> => <<"1s">>,
            <<"worker_pool_size">> => 1
        }
    }),
    {ok, _} = create_rule(Config, <<"#">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt_concurrently(Publisher, [
            {<<"emqx.events">>, <<"one">>},
            {<<"bad subject">>, <<"bad">>},
            {<<"emqx.events">>, <<"two">>}
        ]),
        ?assertEqual([<<"one">>, <<"two">>], lists:sort(receive_payloads(2, []))),
        {ok, #{result := Results}} = ?block_until(
            #{?snk_kind := nats_connector_query_return, batch := true, batch_size := 3}, 5000
        ),
        ?assertEqual(2, length([ok || ok <- Results])),
        ?assertEqual(1, length([Error || Error <- Results, is_invalid_subject_result(Error)])),
        ?assertEqual({ok, 2}, stream_last_sequence(Client))
    after
        snabbkaffe:stop()
    end.

t_jetstream_batch_puback_timeout(Config) ->
    {ok, Client} = nats_client(Config),
    on_exit(fun() -> enats_client:stop(Client) end),
    ok = create_stream(Client),
    {201, _} = create_connector(Config, #{
        <<"servers">> => <<"toxiproxy:14223">>,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"60s">>}
    }),
    {201, _} = create_action(Config, #{
        <<"parameters">> => #{<<"delivery_mode">> => <<"jetstream">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"batch_size">> => 2,
            <<"batch_time">> => <<"1s">>,
            <<"worker_pool_size">> => 1,
            <<"request_ttl">> => <<"5s">>
        }
    }),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        emqx_common_test_helpers:with_failure(
            timeout_downstream,
            ?JETSTREAM_PROXY,
            ?PROXY_HOST,
            ?PROXY_PORT,
            fun() ->
                publish_mqtt_concurrently(Publisher, <<"sensor/1/data">>, [<<"one">>, <<"two">>]),
                {ok, _} = ?block_until(
                    #{
                        ?snk_kind := nats_connector_query_return,
                        batch := true,
                        batch_size := 2,
                        result := {error, {recoverable_error, #{reason := timeout}}}
                    },
                    10000
                ),
                {ok, Count} = stream_last_sequence(Client),
                ?assert(Count >= 1)
            end
        )
    after
        snabbkaffe:stop()
    end.

t_pool_reconnect_error_preserved(Config) ->
    {ok, Subscriber} = nats_client_on_host("nats-noauth", 4222),
    on_exit(fun() -> enats_client:stop(Subscriber) end),
    {ok, _} = enats_client:subscribe(Subscriber, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config, #{
        <<"servers">> => <<"toxiproxy:14222">>,
        <<"connect_timeout">> => <<"200ms">>,
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"60s">>}
    }),
    {201, _} = create_action(Config, #{
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"batch_size">> => 1,
            <<"batch_time">> => <<"0ms">>,
            <<"resume_interval">> => <<"1s">>,
            <<"request_ttl">> => <<"15s">>
        }
    }),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    [{_, Worker}] = ecpool:workers(ResourceId),
    Client = pool_client(ResourceId),
    emqx_common_test_helpers:enable_failure(down, ?RECONNECT_PROXY, ?PROXY_HOST, ?PROXY_PORT),
    exit(Client, kill),
    ?retry(
        10,
        50,
        ?assertMatch(
            {error, {disconnected, #{reason := killed}}}, ecpool_worker:client(Worker)
        )
    ),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt(Publisher, <<"sensor/1/data">>, <<"client-killed">>),
        {ok, _} = ?block_until(
            #{
                ?snk_kind := nats_connector_query_return,
                result :=
                    {error,
                        {recoverable_error,
                            {disconnected, #{
                                reason := killed, time_since_observed_ms := _
                            }}}}
            },
            2000
        ),
        ?retry(
            100,
            50,
            ?assertMatch(
                {error, {disconnected, #{reason := #{reason := connection_failed}}}},
                ecpool_worker:client(Worker)
            )
        ),
        publish_mqtt(Publisher, <<"sensor/1/data">>, <<"pool-recovered">>),
        {ok, _} = ?block_until(
            #{
                ?snk_kind := nats_connector_query_return,
                result :=
                    {error,
                        {recoverable_error,
                            {disconnected, #{
                                reason := #{reason := connection_failed},
                                time_since_observed_ms := _
                            }}}}
            },
            7000
        ),
        emqx_common_test_helpers:heal_failure(down, ?RECONNECT_PROXY, ?PROXY_HOST, ?PROXY_PORT),
        ?assertEqual(
            [<<"client-killed">>, <<"pool-recovered">>],
            lists:sort(receive_payloads(2, []))
        )
    after
        emqx_common_test_helpers:heal_failure(down, ?RECONNECT_PROXY, ?PROXY_HOST, ?PROXY_PORT),
        snabbkaffe:stop()
    end.

t_core_batch_partial_failure(Config) ->
    {ok, Client} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.>">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{
                <<"subject">> => <<"${.topic}">>,
                <<"payload_template">> => <<"${.payload}">>
            },
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"async">>,
                <<"batch_size">> => 3,
                <<"batch_time">> => <<"1s">>,
                <<"worker_pool_size">> => 1
            }
        }
    ),
    {ok, _} = create_rule(Config, <<"#">>),
    Publisher = mqtt_client(),
    Messages = [
        {<<"emqx.batch.one">>, <<"batch-one">>},
        {<<"bad subject">>, <<"batch-bad">>},
        {<<"emqx.batch.two">>, <<"batch-two">>}
    ],
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt_concurrently(Publisher, Messages),
        ?assertEqual(
            lists:sort([<<"batch-one">>, <<"batch-two">>]),
            lists:sort(receive_payloads(2, []))
        ),
        {ok, _} = ?block_until(
            #{?snk_kind := nats_connector_query_return, batch := true, batch_size := 3},
            5_000
        ),
        Trace = snabbkaffe:collect_trace(),
        ?assert(
            lists:any(
                fun
                    (#{result := Results}) when is_list(Results) ->
                        lists:any(fun is_invalid_subject_result/1, Results);
                    (_) ->
                        false
                end,
                ?of_kind(nats_connector_query_return, Trace)
            )
        )
    after
        snabbkaffe:stop(),
        ok = enats_client:stop(Client)
    end.

%%--------------------------------------------------------------------
%% JetStream publishing
%%--------------------------------------------------------------------

t_jetstream_publish(Config) ->
    {ok, Client} = nats_client(Config),
    ok = create_stream(Client),
    {ok, InitialCount} = stream_last_sequence(Client),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.events">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"delivery_mode">> => <<"jetstream">>,
            <<"msg_id_template">> => <<"fixed-test-id">>,
            <<"headers">> => []
        },
        <<"resource_opts">> => #{
            <<"batch_size">> => 1,
            <<"batch_time">> => <<"0ms">>
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-js">>),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-js">>),
    ?retry(50, 100, ?assertEqual({ok, InitialCount + 1}, stream_last_sequence(Client))),
    ok = enats_client:stop(Client).

t_jetstream_batch_publish_all(Config) ->
    {ok, Client} = nats_client(Config),
    ok = create_stream(Client),
    {ok, InitialCount} = stream_last_sequence(Client),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    Action = #{
        <<"parameters">> => #{
            <<"subject">> => <<"emqx.events">>,
            <<"payload_template">> => <<"${.payload}">>,
            <<"delivery_mode">> => <<"jetstream">>,
            <<"msg_id_template">> => <<>>,
            <<"headers">> => []
        },
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"batch_size">> => 3,
            <<"batch_time">> => <<"1s">>,
            <<"worker_pool_size">> => 1
        }
    },
    {201, _} = create_action(Config, Action),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt_concurrently(
            Publisher,
            <<"sensor/1/data">>,
            [<<"js-batch-1">>, <<"js-batch-2">>, <<"js-batch-3">>]
        ),
        ?retry(50, 100, ?assertEqual({ok, InitialCount + 3}, stream_last_sequence(Client))),
        ?assertEqual(
            lists:sort([<<"js-batch-1">>, <<"js-batch-2">>, <<"js-batch-3">>]),
            lists:sort(receive_payloads(3, []))
        ),
        Trace = snabbkaffe:collect_trace(),
        ?assert(
            lists:any(
                fun(#{batch := Batch}) -> length(Batch) =:= 3 end,
                ?of_kind(call_batch_query, Trace) ++ ?of_kind(call_batch_query_async, Trace)
            )
        )
    after
        snabbkaffe:stop(),
        ok = enats_client:stop(Client)
    end.

%%--------------------------------------------------------------------
%% Error and recovery handling
%%--------------------------------------------------------------------

t_jetstream_no_responders(Config) ->
    wait_for_port("nats-noauth", 4222),
    ConnectorOverrides = #{<<"servers">> => <<"nats-noauth:4222">>},
    {201, _} = create_connector(Config, ConnectorOverrides),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{
                <<"delivery_mode">> => <<"jetstream">>,
                <<"msg_id_template">> => <<"stable-id">>
            },
            <<"resource_opts">> => #{
                <<"batch_size">> => 1,
                <<"batch_time">> => <<"0ms">>,
                <<"request_ttl">> => <<"5s">>
            }
        }
    ),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt(Publisher, <<"sensor/1/data">>, <<"no-js">>),
        {ok, _} = ?block_until(
            #{
                ?snk_kind := nats_connector_query_return,
                result :=
                    {error,
                        {unrecoverable_error, #{
                            reason := server_error,
                            details := #{source := jetstream, code := unavailable, status := 503}
                        }}}
            },
            10_000
        )
    after
        snabbkaffe:stop()
    end.

t_template_error_details(Config) ->
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{
            <<"parameters">> => #{<<"payload_template">> => <<"${.missing.payload}">>},
            <<"resource_opts">> => #{<<"batch_size">> => 1}
        }
    ),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello">>),
        {ok, _} = ?block_until(
            #{
                ?snk_kind := nats_connector_query_return,
                result :=
                    {error, {unrecoverable_error, {template_error, #{class := error, reason := _}}}}
            },
            5_000
        )
    after
        snabbkaffe:stop()
    end.

t_publish_error_preserves_classification(Config) ->
    {201, _} = create_connector(Config),
    {201, _} = create_action(
        Config,
        #{<<"parameters">> => #{<<"subject">> => <<"bad subject">>}}
    ),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ok = snabbkaffe:start_trace(),
    try
        publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello">>),
        {ok, #{result := Result}} = ?block_until(
            #{?snk_kind := nats_connector_query_return},
            5_000
        ),
        ?assert(has_invalid_subject_result(Result))
    after
        snabbkaffe:stop()
    end.

t_reconnect(Config) ->
    {ok, Client} = nats_client_on_host("toxiproxy", 14222),
    on_exit(fun() -> enats_client:stop(Client) end),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config, #{<<"servers">> => <<"toxiproxy:14222">>}),
    {201, _} = create_action(Config),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    emqx_common_test_helpers:enable_failure(down, ?RECONNECT_PROXY, ?PROXY_HOST, ?PROXY_PORT),
    receive
        {enats_client, Client, disconnected, _Reason} -> ok
    after 2000 -> ct:fail(nats_disconnect_not_observed)
    end,
    Parent = self(),
    Reenable = spawn(fun() ->
        timer:sleep(200),
        emqx_common_test_helpers:heal_failure(down, ?RECONNECT_PROXY, ?PROXY_HOST, ?PROXY_PORT),
        Parent ! nats_proxy_reenabled
    end),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-during-outage">>),
    receive
        nats_proxy_reenabled -> ok
    after 10000 -> ct:fail({nats_proxy_not_reenabled, Reenable})
    end,
    ?assertMatch(
        {enats_client, Client, {message, #{payload := <<"hello-during-outage">>}}},
        receive_message(5000)
    ).

t_worker_client_restart(Config) ->
    {ok, Subscriber} = nats_client(Config),
    {ok, _} = enats_client:subscribe(Subscriber, <<"emqx.events">>, #{}),
    {201, _} = create_connector(Config),
    {201, _} = create_action(Config),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    OldClient = pool_client(ResourceId),
    exit(OldClient, kill),
    ?retry(
        500,
        20,
        begin
            NewClient = pool_client(ResourceId),
            ?assertNotEqual(OldClient, NewClient),
            ?assert(is_process_alive(NewClient))
        end
    ),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-after-client-restart">>),
    ?assertMatch(
        {enats_client, Subscriber, {message, #{payload := <<"hello-after-client-restart">>}}},
        receive_message(5000)
    ),
    ok = enats_client:stop(Subscriber).

t_health_check_timeout(Config) ->
    {201, _} = create_connector(
        Config,
        #{
            <<"resource_opts">> => #{
                <<"health_check_interval">> => <<"100ms">>,
                <<"health_check_timeout">> => <<"100ms">>
            }
        }
    ),
    ResourceId = emqx_bridge_v2_testlib:connector_resource_id(Config),
    Client = pool_client(ResourceId),
    ok = sys:suspend(Client),
    on_exit(fun() ->
        _ = catch sys:resume(Client),
        ok
    end),
    ?retry(
        100,
        20,
        ?assertMatch(
            {200, #{<<"status">> := <<"disconnected">>}},
            get_connector(Config)
        )
    ),
    _ = catch sys:resume(Client),
    ok.

%%--------------------------------------------------------------------
%% Authentication and TLS
%%--------------------------------------------------------------------

t_auth_user_password(Config) ->
    auth_publish_case(
        Config,
        {"nats", 4222},
        #{
            <<"mechanism">> => <<"user_password">>,
            <<"username">> => <<"test_user">>,
            <<"password">> => <<"password">>
        },
        #{
            mechanism => user_password,
            username => <<"test_user">>,
            password => fun() -> <<"password">> end
        }
    ).

t_auth_token(Config) ->
    auth_publish_case(
        Config,
        {"nats-token", 4222},
        #{
            <<"mechanism">> => <<"token">>,
            <<"token">> => <<"nats_token">>
        },
        #{mechanism => token, token => fun() -> <<"nats_token">> end}
    ).

t_auth_nkey(Config) ->
    PrivateKey = nkey_private_key(),
    {PublicKey, _} = crypto:generate_key(eddsa, ed25519, PrivateKey),
    PublicNKey = enats_auth:encode_nkey_public(PublicKey),
    Seed = encode_seed(PrivateKey),
    auth_publish_case(
        Config,
        {"nats", 4222},
        #{
            <<"mechanism">> => <<"nkey">>,
            <<"nkey_seed">> => Seed
        },
        #{
            mechanism => nkey,
            public_key => PublicNKey,
            sign_fun => enats_auth:nkey_signer(PublicKey, PrivateKey)
        },
        #{},
        #{},
        fun(ConnectorBody) ->
            ?assertEqual(nomatch, binary:match(ConnectorBody, Seed)),
            ok
        end
    ).

t_tls(Config) ->
    auth_publish_case(
        Config,
        {"nats-tls-noauth", 4422},
        none,
        none,
        #{tls => true, ssl_opts => [{verify, verify_none}]},
        #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => ?NATS_CA_CERT
            }
        }
    ).

t_tls_first(Config) ->
    auth_publish_case(
        Config,
        {"nats-bridge-tls-first", 4222},
        none,
        none,
        #{tls => true, tls_handshake => first, ssl_opts => [{verify, verify_none}]},
        #{
            <<"ssl">> => #{
                <<"enable">> => true,
                <<"verify">> => <<"verify_peer">>,
                <<"cacertfile">> => ?NATS_CA_CERT
            },
            <<"tls_handshake">> => <<"first">>
        }
    ).

%%--------------------------------------------------------------------
%% Inline credentials content
%%--------------------------------------------------------------------

t_credentials_content_validation(_Config) ->
    Seed = encode_seed(<<1:256>>),
    Contents = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        Seed,
        "\n------END USER NKEY SEED------\n"
    ]),
    ?assertEqual(ok, enats_auth:validate_credentials(Contents)),
    InvalidContents = <<
        "-----BEGIN NATS USER JWT-----\njwt\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\ninvalid\n------END USER NKEY SEED------\n"
    >>,
    ?assertMatch({error, _}, enats_auth:validate_credentials(InvalidContents)).

t_cluster_credentials_content() ->
    [{cluster, true}].
t_cluster_credentials_content(Config) ->
    {Fixture, Credentials} = jwt_credentials_fixture(),
    #{user_public := UserPublic, user_private := UserPrivate, user_jwt := UserJWT} = Fixture,
    {ok, Subscriber} = connect_client(
        enats_client:start_link(#{
            host => "nats-jwt",
            port => 4222,
            owner => self(),
            auth => #{
                mechanism => jwt,
                public_key => UserPublic,
                jwt => fun() -> UserJWT end,
                sign_fun => enats_auth:nkey_signer(UserPublic, UserPrivate)
            }
        })
    ),
    on_exit(fun() -> enats_client:stop(Subscriber) end),
    {ok, _} = enats_client:subscribe(Subscriber, <<"emqx.events">>, #{}),
    ok = enats_client:flush(Subscriber, 2000),
    Name = ?config(connector_name, Config),
    ConnectorOverrides = #{
        <<"servers">> => <<"nats-jwt:4222">>,
        <<"authentication">> => #{
            <<"mechanism">> => <<"jwt">>,
            <<"credentials_file_content">> => Credentials
        }
    },
    Nodes = ?config(cluster_nodes, Config),
    [N1, N2] = Nodes,
    {201, Connector} = create_connector(Config, ConnectorOverrides),
    assert_credentials_not_exposed(emqx_utils_json:encode(Connector)),
    assert_cluster_credentials_content(N1, Name),
    assert_cluster_credentials_content(N2, Name),
    {201, _} = create_action(Config),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    ?retry(
        100,
        100,
        begin
            {200, #{<<"node_status">> := NodeStatuses}} = get_connector(Config),
            ?assertEqual(2, length(NodeStatuses)),
            ?assert(
                lists:all(
                    fun(#{<<"status">> := Status}) -> Status =:= <<"connected">> end,
                    NodeStatuses
                )
            )
        end
    ),
    lists:foreach(fun(Node) -> cluster_credentials_publish(Node, Subscriber) end, Nodes),
    ok.

t_auth_jwt_creds(Config) ->
    {Fixture, Credentials} = jwt_credentials_fixture(),
    #{user_public := UserPublic, user_private := UserPrivate, user_jwt := UserJWT} = Fixture,
    auth_publish_case(
        Config,
        {"nats-jwt", 4222},
        #{
            <<"mechanism">> => <<"jwt">>,
            <<"credentials_file_content">> => Credentials
        },
        #{
            mechanism => jwt,
            public_key => UserPublic,
            jwt => fun() -> UserJWT end,
            sign_fun => enats_auth:nkey_signer(UserPublic, UserPrivate)
        },
        #{},
        #{},
        fun(ConnectorBody) ->
            assert_credentials_not_exposed(ConnectorBody),
            assert_credentials_content_config(Config)
        end
    ).

%%--------------------------------------------------------------------
%% Test helpers
%%--------------------------------------------------------------------

auth_publish_case(Config, Server, Authentication, ClientAuth) ->
    auth_publish_case(Config, Server, Authentication, ClientAuth, #{}, #{}, fun(_Body) -> ok end).

auth_publish_case(
    Config,
    Server,
    Authentication,
    ClientAuth,
    ClientOptions,
    ConnectorOverrides0
) ->
    auth_publish_case(
        Config,
        Server,
        Authentication,
        ClientAuth,
        ClientOptions,
        ConnectorOverrides0,
        fun(_Body) -> ok end
    ).

auth_publish_case(
    Config,
    {Host, Port},
    Authentication,
    ClientAuth,
    ClientOptions,
    ConnectorOverrides0,
    AfterConnector
) ->
    wait_for_port(Host, Port),
    {ok, Client} = connect_client(
        enats_client:start_link(
            maps:merge(
                #{
                    host => Host,
                    port => Port,
                    auth => ClientAuth,
                    owner => self()
                },
                ClientOptions
            )
        )
    ),
    on_exit(fun() -> enats_client:stop(Client) end),
    ConnectorOverrides = emqx_utils_maps:deep_merge(
        #{
            <<"servers">> => nats_server(Host, Port),
            <<"authentication">> => Authentication
        },
        ConnectorOverrides0
    ),
    {201, #{<<"status">> := <<"connected">>} = Connector} = create_connector(
        Config, ConnectorOverrides
    ),
    ok = AfterConnector(emqx_utils_json:encode(Connector)),
    {201, _} = create_action(Config),
    {ok, _} = enats_client:subscribe(Client, <<"emqx.events">>, #{}),
    {ok, _} = create_rule(Config, <<"sensor/+/data">>),
    Publisher = mqtt_client(),
    publish_mqtt(Publisher, <<"sensor/1/data">>, <<"hello-auth">>),
    ?assertMatch(
        {enats_client, Client, {message, #{payload := <<"hello-auth">>}}},
        receive_message(5000)
    ),
    ok = enats_client:stop(Client).

assert_credentials_not_exposed(ConnectorBody) ->
    ?assertEqual(nomatch, binary:match(ConnectorBody, <<"BEGIN NATS USER JWT">>)),
    ?assertEqual(nomatch, binary:match(ConnectorBody, <<"BEGIN USER NKEY SEED">>)),
    ok.

assert_credentials_content_config(Config) ->
    Name = proplists:get_value(connector_name, Config),
    Authentication = emqx:get_raw_config(
        [connectors, nats, Name, authentication], #{}
    ),
    ?assertEqual(undefined, maps:get(credentials_file, Authentication, undefined)),
    ?assert(
        maps:is_key(credentials_file_content, Authentication) orelse
            maps:is_key(<<"credentials_file_content">>, Authentication)
    ),
    ok.

assert_cluster_credentials_content(Node, Name) ->
    Authentication = ?ON(
        Node,
        emqx:get_raw_config([connectors, nats, Name, authentication], #{})
    ),
    ?assertEqual(undefined, maps:get(credentials_file, Authentication, undefined)),
    ?assert(
        maps:is_key(credentials_file_content, Authentication) orelse
            maps:is_key(<<"credentials_file_content">>, Authentication)
    ),
    ok.

cluster_app_specs() ->
    [
        {emqx, #{before_start => fun cluster_emqx_before_start/2}},
        emqx_conf,
        emqx_auth,
        emqx_connector,
        emqx_bridge_nats,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ].

cluster_emqx_before_start(App, AppConfig) ->
    emqx_config:init_load(emqx_connector_schema, <<>>),
    emqx_config:add_allowed_namespaced_config_root(<<"connectors">>),
    emqx_cth_suite:inhibit_config_loader(App, AppConfig).

cluster_credentials_publish(Node, Subscriber) ->
    Port = emqx_common_test_helpers:listener_port(
        ?ON(Node, emqx_config:get([listeners, tcp, default, bind]))
    ),
    {ok, Publisher} = emqtt:start_link(#{port => Port}),
    try
        {ok, _} = emqtt:connect(Publisher),
        Payload = atom_to_binary(Node),
        publish_mqtt(Publisher, <<"sensor/1/data">>, Payload),
        ?assertMatch(
            {enats_client, Subscriber, {message, #{payload := Payload}}}, receive_message(5000)
        )
    after
        emqtt:stop(Publisher)
    end.

create_connector(Config) ->
    create_connector(Config, #{}).

create_connector(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_action(Config) -> create_action(Config, #{}).
create_action(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api(Config, Overrides)
    ).

create_rule(Config, Topic) ->
    emqx_bridge_v2_testlib:create_rule_and_action_http(?ACTION_TYPE_BIN, Topic, Config, #{}).

get_connector(Config) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            proplists:get_value(connector_type, Config),
            proplists:get_value(connector_name, Config)
        )
    ).

mqtt_client() ->
    {ok, Client} = emqtt:start_link(),
    {ok, _} = emqtt:connect(Client),
    on_exit(fun() -> emqtt:stop(Client) end),
    Client.

publish_mqtt(Client, Topic, Payload) ->
    {ok, _} = emqtt:publish(Client, Topic, Payload, [{qos, 1}]),
    ok.

publish_mqtt_concurrently(Client, Topic, Payloads) ->
    publish_mqtt_concurrently(Client, [{Topic, Payload} || Payload <- Payloads]).

publish_mqtt_concurrently(Client, Messages) ->
    Results = emqx_utils:pmap(
        fun({Topic, Payload}) ->
            {Topic, Payload, emqtt:publish(Client, Topic, Payload, [{qos, 1}])}
        end,
        Messages,
        10_000
    ),
    ?assertEqual(
        [],
        [
            {Topic, Payload, Result}
         || {Topic, Payload, Result} <- Results, not is_mqtt_publish_success(Result)
        ]
    ),
    ok.

is_mqtt_publish_success({ok, _PacketId}) ->
    true;
is_mqtt_publish_success(_) ->
    false.

is_invalid_subject_result(
    {error,
        {unrecoverable_error, #{
            reason := badarg, details := #{field := subject, code := bad_value}
        }}}
) ->
    true;
is_invalid_subject_result(_) ->
    false.

has_invalid_subject_result(Results) when is_list(Results) ->
    lists:any(fun is_invalid_subject_result/1, Results);
has_invalid_subject_result(Result) ->
    is_invalid_subject_result(Result).

nats_client(_Config) ->
    nats_client_on_host(?NATS_HOST, ?NATS_PORT).

nats_client_on_host(Host, Port) ->
    connect_client(
        enats_client:start_link(#{
            host => Host,
            port => Port,
            owner => self(),
            reconnect => true,
            reconnect_delay => 100
        })
    ).

nats_server(Host, Port) ->
    iolist_to_binary([Host, ":", integer_to_list(Port)]).

pool_client(ResourceId) ->
    [{_, Worker}] = ecpool:workers(ResourceId),
    {ok, Client} = ecpool_worker:client(Worker),
    Client.

connect_client({ok, Client}) ->
    ok = enats_client:connect(Client),
    {ok, Client}.

create_stream(Client) ->
    {ok, _} = enats_client:request(Client, <<"$JS.API.STREAM.DELETE.EMQX">>, <<>>, #{
        timeout => 2000
    }),
    Body = jsx:encode(#{<<"name">> => <<"EMQX">>, <<"subjects">> => [<<"emqx.events">>]}),
    {ok, _} = enats_client:request(Client, <<"$JS.API.STREAM.CREATE.EMQX">>, Body, #{
        timeout => 2000
    }),
    ok.

stream_last_sequence(Client) ->
    {ok, #{payload := Payload}} = enats_client:request(
        Client, <<"$JS.API.STREAM.INFO.EMQX">>, <<>>, #{timeout => 2000}
    ),
    #{<<"state">> := #{<<"messages">> := Count}} = jsx:decode(Payload, [return_maps]),
    {ok, Count}.

receive_message() -> receive_message(2000).
receive_message(Timeout) ->
    receive
        {enats_client, Client, {message, _} = Message} -> {enats_client, Client, Message}
    after Timeout -> ct:fail(nats_message_timeout)
    end.

receive_payloads(0, Acc) ->
    Acc;
receive_payloads(N, Acc) ->
    {enats_client, _Client, {message, #{payload := Payload}}} = receive_message(5000),
    receive_payloads(N - 1, [Payload | Acc]).

encode_seed(PrivateSeed) ->
    Prefix = <<(16#90 bor (16#A0 bsr 5)), ((16#A0 band 31) bsl 3), PrivateSeed/binary>>,
    encode_base32(<<Prefix/binary, (test_crc16(Prefix)):16/little>>).

test_crc16(Bin) ->
    test_crc16(Bin, 0).
test_crc16(<<>>, Crc) ->
    Crc;
test_crc16(<<Byte, Rest/binary>>, Crc0) ->
    Crc1 = Crc0 bxor (Byte bsl 8),
    test_crc16(Rest, test_crc_byte(Crc1, 8)).

test_crc_byte(Crc, 0) ->
    Crc band 16#FFFF;
test_crc_byte(Crc, N) when Crc band 16#8000 =/= 0 ->
    test_crc_byte(((Crc bsl 1) bxor 16#1021) band 16#FFFF, N - 1);
test_crc_byte(Crc, N) ->
    test_crc_byte((Crc bsl 1) band 16#FFFF, N - 1).

encode_base32(Bits) ->
    encode_base32(Bits, []).

encode_base32(<<Value:5, Rest/bitstring>>, Acc) ->
    encode_base32(Rest, [lists:nth(Value + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(Bits, Acc) when bit_size(Bits) > 0 ->
    Size = bit_size(Bits),
    <<Value:Size>> = Bits,
    Padded = Value bsl (5 - Size),
    encode_base32(<<>>, [lists:nth(Padded + 1, "ABCDEFGHIJKLMNOPQRSTUVWXYZ234567") | Acc]);
encode_base32(<<>>, Acc) ->
    list_to_binary(lists:reverse(Acc)).

nkey_private_key() ->
    <<205, 42, 56, 73, 83, 88, 159, 152, 35, 244, 15, 34, 196, 39, 226, 60, 111, 109, 0, 79, 72,
        148, 60, 239, 181, 139, 118, 231, 215, 12, 158, 116>>.

jwt_credentials_fixture() ->
    UserPrivate = nats_jwt_private_key(),
    {PublicKey, _} = crypto:generate_key(eddsa, ed25519, UserPrivate),
    UserPublic = enats_auth:encode_nkey_public(PublicKey),
    UserJWT = nats_jwt_token(),
    Credentials = iolist_to_binary([
        "-----BEGIN NATS USER JWT-----\n",
        UserJWT,
        "\n------END NATS USER JWT------\n",
        "-----BEGIN USER NKEY SEED-----\n",
        encode_seed(UserPrivate),
        "\n------END USER NKEY SEED------\n"
    ]),
    {#{user_public => UserPublic, user_private => UserPrivate, user_jwt => UserJWT}, Credentials}.

nats_jwt_token() ->
    <<
        "eyJ0eXAiOiJKV1QiLCJhbGciOiJlZDI1NTE5LW5rZXkifQ."
        "eyJqdGkiOiJPMjU3WlA3NDdUQ1g3VUo2RkFVS0xHSzNJQTVGRFRXV01BTERaSEJNTUtQRlo1NTNPNlJRIiwia"
        "WF0IjoxNzcwOTU0MjMyLCJpc3MiOiJBQURBQk5FRktMWVdaRENGQTVTUlJKMlRaWkFaTUNZNVdMR0FUTlg3V"
        "1ZCQlJaRU9UWFZCTFI0TSIsIm5hbWUiOiJ0ZXN0Iiwic3ViIjoiVUNDNEdGUlJYVVVNS1ROUDY3VlFUQUJDT"
        "ExPRFROQ05PQklVTlVIVUFNRUZQM09FRkgzUUQ3WUIiLCJuYXRzIjp7InB1YiI6e30sInN1YiI6e30sInN1Y"
        "nMiOi0xLCJkYXRhIjotMSwicGF5bG9hZCI6LTEsInR5cGUiOiJ1c2VyIiwidmVyc2lvbiI6Mn19."
        "-aoi_dRV83R-LmpKbUCTYpvHvBiuOlx_HdhDyD89ZV2ocTxtyFf4KCco5F0lUA7GsLQZo1kmX1Df9sLv4wIZDA"
    >>.

nats_jwt_private_key() ->
    <<153, 4, 163, 231, 183, 138, 62, 8, 137, 201, 217, 217, 31, 222, 119, 53, 165, 160, 35, 110,
        172, 49, 225, 23, 186, 170, 182, 203, 170, 119, 70, 83>>.

wait_for_port(Host, Port) ->
    wait_for_port(Host, Port, 100).

wait_for_port(_Host, _Port, 0) ->
    ct:fail(nats_port_not_ready);
wait_for_port(Host, Port, Attempts) ->
    case gen_tcp:connect(Host, Port, [], 100) of
        {ok, Socket} ->
            gen_tcp:close(Socket),
            ok;
        {error, _} ->
            timer:sleep(50),
            wait_for_port(Host, Port, Attempts - 1)
    end.
