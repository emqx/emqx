%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_rabbitmq_source_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("amqp_client/include/amqp_client.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, rabbitmq).
-define(CONNECTOR_TYPE_BIN, <<"rabbitmq">>).
-define(SOURCE_TYPE, rabbitmq).
-define(SOURCE_TYPE_BIN, <<"rabbitmq">>).

-define(USER, <<"guest">>).
-define(PASSWORD, <<"guest">>).
-define(EXCHANGE, <<"messages">>).
-define(QUEUE, <<"test_queue">>).
-define(ROUTING_KEY, <<"test_routing_key">>).

-define(tcp, tcp).
-define(tls, tls).

-define(assertReceivePublish(EXPR, TIMEOUT, EXTRA),
    (fun() ->
        lists:foreach(
            fun(Pub0___) ->
                Pub___ = maps:update_with(
                    payload,
                    fun emqx_utils_json:decode/1,
                    Pub0___
                ),
                self() ! {decoded, Pub___}
            end,
            drain_publishes([], TIMEOUT)
        ),
        {decoded, __X} = ?assertReceive({decoded, EXPR}, 200, EXTRA),
        __X
    end)()
).
-define(assertReceivePublish(EXPR), ?assertReceivePublish(EXPR, 1_000, #{})).
-define(assertReceivePublish(EXPR, TIMEOUT), ?assertReceivePublish(EXPR, TIMEOUT, #{})).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_rabbitmq,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_group(?tcp, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5672},
        {enable_tls, false}
        | TCConfig
    ];
init_per_group(?tls, TCConfig) ->
    [
        {host, <<"rabbitmq">>},
        {port, 5671},
        {enable_tls, true}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    SSL =
        case get_config(enable_tls, TCConfig, false) of
            true ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(true)
                );
            false ->
                emqx_utils_maps:binary_key_map(
                    emqx_bridge_rabbitmq_testlib:ssl_options(false)
                )
        end,
    ConnectorConfig = connector_config(#{
        <<"server">> => get_config(host, TCConfig, <<"rabbitmq">>),
        <<"port">> => get_config(port, TCConfig, 5672),
        <<"ssl">> => SSL
    }),
    SourceName = ConnectorName,
    SourceConfig = source_config(#{
        <<"connector">> => ConnectorName
    }),
    ClientOpts = #{
        host => get_config(host, TCConfig, <<"rabbitmq">>),
        port => get_config(port, TCConfig, 5672),
        use_tls => get_config(enable_tls, TCConfig, false),
        exchange => ?EXCHANGE,
        queue => ?QUEUE,
        routing_key => ?ROUTING_KEY
    },
    emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(ClientOpts),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, source},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {source_type, ?SOURCE_TYPE},
        {source_name, SourceName},
        {source_config, SourceConfig},
        {client_opts, ClientOpts}
        | TCConfig
    ].

end_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:stop(),
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:cleanup_client_and_queue(ClientOpts),
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:connector_config(Overrides).

source_config(Overrides) ->
    emqx_bridge_rabbitmq_testlib:source_config(Overrides).

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

receive_message(TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:receive_message(ClientOpts).

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api([{bridge_kind, source} | TCConfig], Overrides).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

delete_source_api(TCConfig) ->
    #{type := Type, name := Name} = emqx_bridge_v2_testlib:get_common_values(TCConfig),
    Opts = #{query_params => #{<<"also_delete_dep_actions">> => <<"true">>}},
    emqx_bridge_v2_testlib:delete_kind_api(source, Type, Name, Opts).

get_source_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_source_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

simple_create_rule_api(SQL, TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(SQL, TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts) ->
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

prepare_conf_file(Name, Content, TCConfig) ->
    emqx_config_SUITE:prepare_conf_file(Name, Content, TCConfig).

payload() ->
    payload(_Overrides = #{}).

payload(Overrides) ->
    maps:merge(
        #{<<"key">> => 42, <<"data">> => <<"RabbitMQ">>, <<"timestamp">> => 10000},
        Overrides
    ).

unique_payload() ->
    integer_to_binary(erlang:unique_integer()).

publish_message(Payload, TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:publish_message(Payload, ClientOpts).

drain_publishes(Acc, Timeout) ->
    receive
        {publish, Msg} ->
            drain_publishes([Msg | Acc], Timeout)
    after Timeout ->
        lists:reverse(Acc)
    end.

delete_queue(TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:delete_queue(ClientOpts).

connect_and_setup_exchange_and_queue(TCConfig) ->
    ClientOpts = get_config(client_opts, TCConfig),
    emqx_bridge_rabbitmq_testlib:connect_and_setup_exchange_and_queue(ClientOpts).

wait_basic_cancel_received(TCConfig, Fn) when is_list(TCConfig) ->
    #{~"pool_size" := PoolSize} = get_config(connector_config, TCConfig),
    Opts = #{n => PoolSize},
    wait_basic_cancel_received(Opts, Fn);
wait_basic_cancel_received(Opts, Fn) when is_map(Opts) ->
    #{n := N} = Opts,
    TS = trace:session_create(?MODULE, self(), []),
    on_exit(fun() -> trace:session_destroy(TS) end),
    trace:process(TS, all, true, [call]),
    M = emqx_bridge_rabbitmq_source_worker,
    F = handle_info,
    trace:function(TS, {M, F, 2}, [], []),
    Fn(),
    lists:foreach(
        fun(_) ->
            ?assertReceive({trace, _, call, {M, F, [#'basic.cancel'{}, _]}})
        end,
        lists:seq(1, N)
    ),
    trace:session_destroy(TS),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop() ->
    [{matrix, true}].
t_start_stop(matrix) ->
    [[?tcp], [?tls]];
t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "rabbitmq_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_source(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_source_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(
        <<
            "select *, queue as payload.queue, exchange as payload.exchange,"
            "routing_key as payload.routing_key from \"${t}\""
        >>,
        TCConfig
    ),
    C = start_client(),
    {ok, #{}, [0]} = emqtt:subscribe(C, Topic, [{qos, 0}, {rh, 0}]),
    Payload = payload(),
    PayloadBin = emqx_utils_json:encode(Payload),
    publish_message(PayloadBin, TCConfig),
    {publish, #{payload := ReceivedPayload}} = ?assertReceive(
        {publish, #{
            dup := false,
            properties := undefined,
            topic := Topic,
            qos := 0,
            payload := _,
            retain := false
        }}
    ),
    Meta = #{
        <<"exchange">> => ?EXCHANGE,
        <<"routing_key">> => ?ROUTING_KEY,
        <<"queue">> => ?QUEUE
    },
    ExpectedPayload = maps:merge(payload(), Meta),
    ?assertMatch(
        #{<<"payload">> := ExpectedPayload},
        emqx_utils_json:decode(ReceivedPayload),
        #{expected => ExpectedPayload}
    ),
    ?retry(
        500,
        20,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"received">> := 1,
                    <<"matched">> := 0,
                    <<"success">> := 0,
                    <<"failed">> := 0,
                    <<"dropped">> := 0
                }
            }},
            get_source_metrics_api(TCConfig)
        )
    ),
    ok.

t_queue_deleted_while_consuming(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"resource_opts" => #{~"health_check_timeout" => ~"1s"}
    }),
    {201, _} = create_source_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, _, [_]} = emqtt:subscribe(C, RuleTopic, [{qos, 1}]),
    Payload1 = payload(#{key => 1}),
    PayloadBin1 = emqx_utils_json:encode(Payload1),
    publish_message(PayloadBin1, TCConfig),
    %% sanity check: it's initially working
    ?assertReceivePublish(#{payload := #{~"payload" := #{~"key" := 1}}}),

    wait_basic_cancel_received(TCConfig, fun() ->
        delete_queue(TCConfig)
    end),
    %% give it some time to process the `#'basic.cancel'{}` message and start trying to
    %% resubscribe.
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{~"status" := ~"disconnected"}},
            get_source_api(TCConfig)
        )
    ),

    %% recovery
    connect_and_setup_exchange_and_queue(TCConfig),
    Payload2 = payload(#{key => 2}),
    PayloadBin2 = emqx_utils_json:encode(Payload2),
    publish_message(PayloadBin2, TCConfig),
    ?assertReceivePublish(#{payload := #{~"payload" := #{~"key" := 2}}}, 5_000),

    ok.

-doc """
Verifies that the health from subscription workers is also checked when doing channel health checks.
""".
t_resubscribe_timeout(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{
        ~"pool_size" => 1,
        ~"resource_opts" => #{~"health_check_timeout" => ~"1s"}
    }),
    {201, _} = create_source_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    {ok, _, [_]} = emqtt:subscribe(C, RuleTopic, [{qos, 1}]),
    Payload1 = payload(#{key => 1}),
    PayloadBin1 = emqx_utils_json:encode(Payload1),
    publish_message(PayloadBin1, TCConfig),
    %% sanity check: it's initially working
    ?assertReceivePublish(#{payload := #{~"payload" := #{~"key" := 1}}}),

    emqx_bridge_v2_testlib:print_tree(emqx_bridge_rabbitmq_sup),
    WorkerPids = [
        P
     || {_, SP, supervisor, _} <- supervisor:which_children(emqx_bridge_rabbitmq_sup),
        {_, P, worker, _} <- supervisor:which_children(SP)
    ],
    [WorkerPid] = WorkerPids,
    PreviousCallTimeout = application:get_env(amqp_client, gen_server_call_timeout),
    on_exit(fun() ->
        case PreviousCallTimeout of
            {ok, T} -> application:set_env(amqp_client, gen_server_call_timeout, T);
            undefined -> application:unset_env(amqp_client, gen_server_call_timeout)
        end
    end),
    {ok, Agent} = emqx_utils_agent:start_link(false),
    emqx_common_test_helpers:with_mock(
        amqp_channel,
        subscribe,
        fun(Chan, Consume, Pid) ->
            case Pid == self() of
                false ->
                    %% connector resubscribing if restarted
                    meck:passthrough([Chan, Consume, Pid]);
                true ->
                    %% worker
                    %% can't call `amqp_util:update_call_timeout` here, as it overrides
                    %% the application level config....
                    Triggered = emqx_utils_agent:get_and_update(
                        Agent, fun(Triggered) -> {Triggered, true} end
                    ),
                    case Triggered of
                        true ->
                            erase(gen_server_call_timeout),
                            meck:passthrough([Chan, Consume, Pid]);
                        false ->
                            put(gen_server_call_timeout, 0),
                            try
                                meck:passthrough([Chan, Consume, Pid])
                            after
                                erase(gen_server_call_timeout)
                            end
                    end
            end
        end,
        fun() ->
            %% make just one worker get "cancelled"
            wait_basic_cancel_received(#{n => 1}, fun() ->
                WorkerPid ! #'basic.cancel'{consumer_tag = ~"mocked"},
                ?retry(
                    500,
                    10,
                    ?assertMatch(
                        {200, #{~"status" := ~"disconnected"}},
                        get_source_api(TCConfig)
                    )
                ),
                %% shouldn't receive late replies from channel
                ?assertNotReceive(
                    {trace, _, call,
                        {emqx_bridge_rabbitmq_source_worker, handle_info, [
                            #'basic.consume_ok'{}, _
                        ]}}
                ),
                %% recovery
                ?retry(
                    500,
                    10,
                    ?assertMatch(
                        {200, #{~"status" := ~"connected"}},
                        get_source_api(TCConfig)
                    )
                ),
                ok
            end),
            ok
        end
    ),

    Payload2 = payload(#{key => 2}),
    PayloadBin2 = emqx_utils_json:encode(Payload2),
    publish_message(PayloadBin2, TCConfig),
    ?assertReceivePublish(#{payload := #{~"payload" := #{~"key" := 2}}}),
    %% receive only once
    ?assertNotReceive({decoded, _}),
    ?assertNotReceive({publish, _}),

    %% recreate source; should clear optvars
    {204, _} = delete_source_api(TCConfig),
    ?assertMatch({201, #{~"status" := ~"connected"}}, create_source_api(TCConfig, #{})),
    Payload3 = payload(#{key => 3}),
    PayloadBin3 = emqx_utils_json:encode(Payload3),
    publish_message(PayloadBin3, TCConfig),
    ?assertReceivePublish(#{payload := #{~"payload" := #{~"key" := 3}}}),
    %% receive only once
    ?assertNotReceive({decoded, _}),
    ?assertNotReceive({publish, _}),

    ok.
