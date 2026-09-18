%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rocketmq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, rocketmq).
-define(CONNECTOR_TYPE_BIN, <<"rocketmq">>).
-define(ACTION_TYPE, rocketmq).
-define(ACTION_TYPE_BIN, <<"rocketmq">>).

-define(PROXY_NAME, "rocketmq").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(ACCESS_KEY, <<"RocketMQ">>).
-define(SECRET_KEY, <<"12345678">>).
-define(TOPIC, <<"TopicTest">>).
-define(PAYLOAD, <<"hello from emqx">>).

-define(tcp, tcp).
-define(tls, tls).
-define(sync, sync).
-define(async, async).
-define(without_batch, without_batch).
-define(with_batch, with_batch).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_rocketmq,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps},
        {proxy_host, ?PROXY_HOST},
        {proxy_port, ?PROXY_PORT},
        {proxy_name, ?PROXY_NAME}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {servers, <<"toxiproxy:9876">>},
        {enable_tls, false}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {servers, <<"rocketmq_namesrv_ssl:9876">>},
        {enable_tls, true}
        | TCConfig
    ];
init_per_group(?sync, TCConfig) ->
    [{query_mode, ?sync} | TCConfig];
init_per_group(?async, TCConfig) ->
    [{query_mode, ?async} | TCConfig];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{
        <<"servers">> => get_config(servers, TCConfig, <<"toxiproxy:9876">>),
        <<"ssl">> => #{
            <<"enable">> => get_config(enable_tls, TCConfig, false),
            <<"verify">> => <<"verify_none">>
        }
    }),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>),
            <<"query_mode">> => get_config(query_mode, TCConfig, <<"sync">>)
        }
    }),
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
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"servers">> => <<"toxiproxy:9876">>,
        <<"access_key">> => <<"RocketMQ">>,
        <<"secret_key">> => <<"12345678">>,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"description">> => <<"my action">>,
        <<"tags">> => [<<"some">>, <<"tags">>],
        <<"parameters">> => #{
            <<"topic">> => <<"TopicTest">>
        },
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_action_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check(action, ?ACTION_TYPE_BIN, <<"x">>, InnerConfigMap).

get_config(K, TCConfig) -> emqx_bridge_v2_testlib:get_value(K, TCConfig).
get_config(K, TCConfig, Default) -> proplists:get_value(K, TCConfig, Default).

group_path(TCConfig, Default) ->
    case emqx_common_test_helpers:group_path(TCConfig) of
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

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

with_failure(FailureType, Fn) ->
    emqx_common_test_helpers:with_failure(FailureType, ?PROXY_NAME, ?PROXY_HOST, ?PROXY_PORT, Fn).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

%% Make the name server answer every route request with "no route", as it does for a topic
%% that does not exist on a broker without auto-create. The CI broker auto-creates topics,
%% so this cannot be provoked with a real topic name.
mock_topic_not_found() ->
    ok = meck:new(rocketmq_client, [passthrough, no_link, no_history]),
    on_exit(fun() -> catch meck:unload(rocketmq_client) end),
    ok = meck:expect(rocketmq_client, get_routeinfo_by_topic, fun(_Pid, Topic) ->
        Header = #{
            <<"code">> => 17,
            <<"remark">> => <<"No topic route info in name server for the topic: ", Topic/binary>>
        },
        {ok, {Header, undefined}}
    end).

unmock_topic_not_found() ->
    ok = meck:unload(rocketmq_client).

get_action_metrics(TCConfig) ->
    {200, #{<<"metrics">> := Metrics}} = emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig),
    Metrics.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?tcp], [?tls]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "rocketmq_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    %% Once the proxy is down, the client reconnects inline after the passive close and
    %% fails, so the connector is `disconnected'. The resource manager then restarts the
    %% connector, whose first connect attempt reports `connecting'.
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig, #{
        failure_status => [?status_connecting, ?status_disconnected]
    }).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [
        [?tcp, ?sync, ?without_batch],
        [?tcp, ?sync, ?with_batch],
        [?tcp, ?async, ?without_batch],
        [?tcp, ?async, ?with_batch],
        [?tls, ?sync, ?without_batch]
    ];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    TraceChecker = fun(Trace) ->
        ?assertMatch([#{result := ok}], ?of_kind(rocketmq_connector_query_return, Trace)),
        ok
    end,
    PostPublishFn = fun(_Context) ->
        {ok, _} = ?block_until(#{?snk_kind := rocketmq_connector_query_return}, 10_000)
    end,
    Opts = #{
        trace_checkers => [TraceChecker],
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

%% Check that we can not connect to the SSL only RocketMQ instance
%% with incorrect SSL options
t_setup_via_config_ssl_host_bad_ssl_opts() ->
    [{matrix, true}].
t_setup_via_config_ssl_host_bad_ssl_opts(matrix) ->
    [[?tls]];
t_setup_via_config_ssl_host_bad_ssl_opts(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"disconnected">>}},
        create_connector_api(TCConfig, #{
            <<"ssl">> => #{<<"verify">> => <<"verify_peer">>}
        })
    ),
    ok.

t_setup_two_actions_via_http_api_and_publish(TCConfig) ->
    ActionName1 = <<"action1">>,
    ActionName2 = <<"action2">>,
    TCConfigAction1 = [{action_name, ActionName1} | TCConfig],
    TCConfigAction2 = [{action_name, ActionName2} | TCConfig],
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfigAction1, #{}),
    {201, _} = create_action_api(TCConfigAction2, #{
        <<"parameters">> => #{<<"topic">> => <<"Topic2">>}
    }),
    #{topic := Topic1} = simple_create_rule_api(TCConfigAction1),
    #{topic := Topic2} = simple_create_rule_api(TCConfigAction2),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic1, <<"hey">>),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic2, <<"hey">>),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace0) ->
            Trace = ?of_kind(rocketmq_connector_query_return, Trace0),
            ?assertMatch([#{result := ok}], Trace),
            ok
        end
    ),
    ok.

t_async_producer_cleans_completed_requests() ->
    [{matrix, true}].
t_async_producer_cleans_completed_requests(matrix) ->
    [[?tcp, ?async, ?with_batch]];
t_async_producer_cleans_completed_requests(TCConfig) when is_list(TCConfig) ->
    Servers = parse_servers(get_config(servers, TCConfig, <<"toxiproxy:9876">>)),
    Parent = self(),
    ClientId = unique_atom("rocketmq_async_client"),
    ProducerName = unique_atom("rocketmq_async_producer"),
    ProducerGroup = iolist_to_binary([atom_to_binary(ClientId, utf8), <<"_">>, ?TOPIC]),
    ACLInfo = #{
        access_key => ?ACCESS_KEY,
        secret_key => ?SECRET_KEY
    },
    ProducerOpts = #{
        batch_size => get_config(batch_size, TCConfig, 100),
        callback => fun(Result, CallbackTopic, BatchLen) ->
            Parent ! {rocketmq_async_callback, Result, CallbackTopic, BatchLen}
        end,
        name => ProducerName,
        ref_topic_route_interval => 3000,
        acl_info => ACLInfo
    },
    {ok, _ClientPid} = rocketmq:ensure_supervised_client(ClientId, Servers, #{
        acl_info => ACLInfo
    }),
    try
        {ok, Producers} =
            rocketmq:ensure_supervised_producers(ClientId, ProducerGroup, ?TOPIC, ProducerOpts),
        try
            ok = rocketmq:send(Producers, ?PAYLOAD),
            receive
                {rocketmq_async_callback, ok, ?TOPIC, 1} ->
                    ok;
                {rocketmq_async_callback, Result, CallbackTopic, BatchLen} ->
                    ct:fail(#{
                        reason => unexpected_async_callback,
                        result => Result,
                        topic => CallbackTopic,
                        batch_len => BatchLen
                    })
            after 10000 ->
                ct:fail(async_callback_timeout)
            end,
            wait_until_request_count(Producers, 0, 50)
        after
            _ = rocketmq:stop_and_delete_supervised_producers(Producers)
        end
    after
        _ = rocketmq:stop_and_delete_supervised_client(ClientId)
    end.

t_acl_deny(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"topic">> => <<"DENY_TOPIC">>}
    }),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{error := #{<<"code">> := 1}}],
                ?of_kind(rocketmq_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ok.

-doc """
Smoke test for templating key and tag values.
""".
t_key_tag_templates() ->
    [{matrix, true}].
t_key_tag_templates(matrix) ->
    [[?tcp, ?without_batch], [?tcp, ?with_batch]];
t_key_tag_templates(TCConfig) when is_list(TCConfig) ->
    BatchSize = get_config(batch_size, TCConfig),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"key">> => <<"${.mykey}">>,
                <<"tag">> => <<"${.mytag}">>,
                <<"template">> => <<"${.payload}">>
            },
            <<"resource_opts">> => #{<<"batch_size">> => BatchSize}
        })
    ),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "select *, payload.tag as mytag, payload.key as mykey"
            " from \"${t}\" "
        >>,
        TCConfig
    ),
    C = start_client(),
    PayloadMap = #{<<"key">> => <<"k1">>, <<"tag">> => <<"t1">>},
    Payload = emqx_utils_json:encode(PayloadMap),
    ct:timetrap({seconds, 10}),
    ok = snabbkaffe:start_trace(),
    {{ok, _}, {ok, #{data := Data}}} =
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload, [{qos, 2}]),
            #{?snk_kind := "rocketmq_rendered_data"}
        ),
    case BatchSize of
        1 ->
            ?assertMatch(
                {_Payload, #{
                    key := <<"k1">>,
                    tag := <<"t1">>
                }},
                Data
            );
        _ ->
            ?assertMatch(
                [
                    {_Payload, #{
                        key := <<"k1">>,
                        tag := <<"t1">>
                    }}
                    | _
                ],
                Data
            )
    end,
    PayloadOut =
        case BatchSize of
            1 ->
                {Payload0, _} = Data,
                Payload0;
            _ ->
                [{Payload0, _} | _] = Data,
                Payload0
        end,
    ?assertEqual(PayloadMap, emqx_utils_json:decode(PayloadOut)),
    ok.

-doc """
Checks that we require `key` to be set if `key_dispatch` strategy is used.
""".
t_key_template_required(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"must provide a key template if strategy is key dispatch">>
        }},
        create_action_api(TCConfig, #{
            <<"parameters">> => #{
                <<"strategy">> => <<"key_dispatch">>
            }
        })
    ),
    ok.

-doc """
Checks that we emit a warning about using deprecated strategy which used templates in that
config key to imply key dispatch.
""".
t_deprecated_templated_strategy(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    ct:timetrap({seconds, 5}),
    ok = snabbkaffe:start_trace(),
    ?assertMatch(
        {{201, #{<<"status">> := <<"connected">>}}, {ok, _}},
        ?wait_async_action(
            create_action_api(TCConfig, #{
                <<"parameters">> => #{
                    <<"strategy">> => <<"${some_template}">>
                }
            }),
            #{?snk_kind := "rocketmq_deprecated_placeholder_strategy"}
        )
    ),
    ok.

parse_servers(Servers) ->
    lists:map(
        fun(Server) ->
            [Host, Port] = binary:split(Server, <<":">>),
            {binary_to_list(Host), binary_to_integer(Port)}
        end,
        binary:split(Servers, <<",">>, [global])
    ).

unique_atom(Prefix) ->
    list_to_atom(Prefix ++ "_" ++ integer_to_list(erlang:unique_integer([positive]))).

wait_until_request_count(Producers, Expected, Attempts) ->
    case producer_request_count(Producers) of
        Expected ->
            ok;
        _Count when Attempts > 0 ->
            timer:sleep(100),
            wait_until_request_count(Producers, Expected, Attempts - 1);
        Count ->
            ?assertEqual(Expected, Count)
    end.

producer_request_count(#{workers := WorkersTab}) ->
    lists:sum([
        producer_request_count(ProducerPid)
     || {_Index, _BrokerName, _QueueSeqNum, ProducerPid, _BrokerAddrs} <- ets:tab2list(WorkersTab),
        is_pid(ProducerPid)
    ]);
producer_request_count(ProducerPid) ->
    {_StateName, ProducerState} = sys:get_state(ProducerPid),
    Requests = element(14, ProducerState),
    map_size(Requests).

-doc """
`namespace' was renamed to `rocketmq_namespace'.  The old name collided with the
EMQX namespace reported by the connector API: both encoded to the JSON key
"namespace", the response carried it twice, and clients keep the last
occurrence.  `namespace' is kept as an alias.

Checks that a config using the old name is still accepted and is normalised to
the new one, and that the response reports the two namespaces separately.
""".
t_rocketmq_namespace_alias(TCConfig) ->
    ConnectorName = get_config(connector_name, TCConfig),
    OwnNamespace = <<"rmq-cn-fzh4bmq240a">>,
    %% Created with the legacy key.
    {201, _} = create_connector_api(TCConfig, #{<<"namespace">> => OwnNamespace}),
    %% Stored under the canonical name, with the alias dropped.
    RawConf = emqx:get_raw_config([connectors, ?CONNECTOR_TYPE, ConnectorName], #{}),
    ?assertMatch(#{<<"rocketmq_namespace">> := OwnNamespace}, RawConf),
    ?assertNot(maps:is_key(<<"namespace">>, RawConf)),
    %% The response keeps the two namespaces apart: `namespace' is the EMQX
    %% namespace, `null' outside a managed namespace.
    {200, Got} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(?CONNECTOR_TYPE, ConnectorName)
    ),
    ?assertMatch(
        #{<<"namespace">> := null, <<"rocketmq_namespace">> := OwnNamespace},
        Got
    ),
    %% The value still reaches the connector runtime state.
    ConnResId = emqx_connector_resource:resource_id(
        ?global_ns, ?CONNECTOR_TYPE, ConnectorName
    ),
    ?assertMatch(
        {ok, _, #{state := #{namespace := OwnNamespace}}},
        emqx_resource:get_instance(ConnResId)
    ),
    ok.

-doc """
When the action topic has no placeholders and the name server has no route for it, the action
reports `disconnected` with a message that names the topic, and a message routed to it is held
in the buffer rather than dropped. Once the topic exists, the next health check reconnects the
action and the held message is delivered.
""".
t_topic_not_found_static_topic(TCConfig) ->
    ok = mock_topic_not_found(),
    {201, _} = create_connector_api(TCConfig, #{}),
    %% Async, so the publisher does not block on the held message; a long TTL, so the
    %% message is still held when the topic appears.
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"topic">> => <<"Topic2">>},
        <<"resource_opts">> => #{
            <<"query_mode">> => <<"async">>,
            <<"request_ttl">> => <<"60s">>
        }
    }),
    ?retry(
        200,
        50,
        ?assertMatch(
            #{
                status := ?status_disconnected,
                error := <<"Topic \"Topic2\" not found", _/binary>>
            },
            emqx_bridge_v2_testlib:health_check_channel(TCConfig)
        )
    ),
    ?assertMatch(
        {200, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"Topic \"Topic2\" not found", _/binary>>
        }},
        emqx_bridge_v2_testlib:simplify_result(emqx_bridge_v2_testlib:get_action_api(TCConfig))
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, _} = emqtt:publish(C, Topic, <<"hey">>, [{qos, 1}]),
    %% The message is held, not dropped or failed.
    ?retry(
        200,
        50,
        ?assertMatch(
            #{<<"matched">> := 1, <<"dropped">> := 0, <<"failed">> := 0, <<"success">> := 0},
            get_action_metrics(TCConfig)
        )
    ),
    %% The topic now exists (the broker auto-creates it): the action recovers and the held
    %% message is delivered.
    ok = unmock_topic_not_found(),
    ?retry(
        200,
        50,
        ?assertMatch(
            #{status := ?status_connected},
            emqx_bridge_v2_testlib:health_check_channel(TCConfig)
        )
    ),
    ?retry(
        200,
        100,
        ?assertMatch(
            #{<<"matched">> := 1, <<"dropped">> := 0, <<"failed">> := 0, <<"success">> := 1},
            get_action_metrics(TCConfig)
        )
    ),
    ok.

-doc """
When the action topic is a template, a rendered topic that has no route cannot be checked
ahead of time. The message is dropped with an `unrecoverable_error` that names the topic, the
error is not a `case_clause`, and the action stays connected.
""".
t_topic_not_found_templated_topic(TCConfig) ->
    ok = mock_topic_not_found(),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{
        <<"parameters">> => #{<<"topic">> => <<"Missing${payload}">>}
    }),
    ?retry(
        200,
        50,
        ?assertMatch(
            #{status := ?status_connected},
            emqx_bridge_v2_testlib:health_check_channel(TCConfig)
        )
    ),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ?check_trace(
        begin
            ?wait_async_action(
                emqtt:publish(C, Topic, <<"T1">>, [{qos, 1}]),
                #{?snk_kind := rocketmq_connector_query_return},
                10_000
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [
                    #{
                        error :=
                            {unrecoverable_error,
                                {topic_not_found, #{
                                    topic := <<"MissingT1">>, remark := <<_/binary>>
                                }}}
                    }
                ],
                ?of_kind(rocketmq_connector_query_return, Trace)
            ),
            ok
        end
    ),
    ?retry(
        200,
        50,
        ?assertMatch(
            #{<<"matched">> := 1, <<"failed">> := 1, <<"success">> := 0},
            get_action_metrics(TCConfig)
        )
    ),
    ?assertMatch(
        #{status := ?status_connected},
        emqx_bridge_v2_testlib:health_check_channel(TCConfig)
    ),
    ok.
