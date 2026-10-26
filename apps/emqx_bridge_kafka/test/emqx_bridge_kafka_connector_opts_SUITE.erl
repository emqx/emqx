%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Tests for the Kafka producer connector client options. A minimal fake Kafka
%% broker is used to observe the
%% actual requests and to let messages be produced end to end.
-module(emqx_bridge_kafka_connector_opts_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, kafka_producer).
-define(ACTION_TYPE, kafka_producer).

-define(API_PRODUCE, 0).
-define(API_API_VERSIONS, 18).

-define(TOPIC, <<"emqx-fake-broker-topic">>).
-define(WAIT_TIMEOUT, 15_000).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        t_fake_broker_fail_fast,
        t_defaults,
        t_client_id,
        t_jmq_compatibility,
        t_empty_client_id,
        t_omitted_client_id,
        t_same_client_id_two_connectors,
        t_update_client_id,
        t_reconnect_after_broker_close
    ].

init_per_suite(TCConfig) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kafka,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [{apps, Apps} | TCConfig].

end_per_suite(TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    ok.

init_per_testcase(TestCase, TCConfig) ->
    {ok, Broker} = emqx_bridge_kafka_fake_broker:start_link(self(), #{
        jmq => TestCase =:= t_jmq_compatibility
    }),
    on_exit(fun() -> catch emqx_bridge_kafka_fake_broker:stop(Broker) end),
    {Host, Port} = emqx_bridge_kafka_fake_broker:bootstrap_address(Broker),
    Name = atom_to_binary(TestCase),
    ConnectorConfig = emqx_bridge_kafka_testlib:action_connector_config(#{
        <<"bootstrap_hosts">> => bootstrap_hosts(Host, Port),
        <<"resource_opts">> => #{<<"health_check_interval">> => <<"10s">>}
    }),
    ActionConfig = action_config(Name),
    [
        {broker, Broker},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_config, ConnectorConfig},
        {action_name, Name},
        {action_type, ?ACTION_TYPE},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc "An unexpected fake-broker request fails the fixture immediately.".
t_fake_broker_fail_fast(TCConfig) ->
    Broker = ?config(broker, TCConfig),
    unlink(Broker),
    Monitor = monitor(process, Broker),
    {Host, Port} = emqx_bridge_kafka_fake_broker:bootstrap_address(Broker),
    {ok, Sock} = gen_tcp:connect(Host, Port, [binary, {packet, 4}, {active, false}]),
    try
        ok = gen_tcp:send(Sock, <<36:16, 1:16, 77:32, 0:16, 3:32, "bad">>),
        receive
            {'DOWN', Monitor, process, Broker, Reason} ->
                ?assertMatch({{badmatch, _}, _}, Reason)
        after 1000 ->
            ct:fail(broker_failure_not_propagated)
        end
    after
        demonitor(Monitor, [flush]),
        gen_tcp:close(Sock)
    end.

%% No client options configured: the Kafka client ID is the connector resource
%% ID, and the client queries API versions as before.
-doc "Default connections query versions and use the connector resource ID.".
t_defaults(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertNotEqual(undefined, ConnResId),
    Requests = produce_and_wait(RuleTopic),
    assert_api_versions_request_version(Requests, 0),
    assert_client_id(Requests, ConnResId),
    assert_produce(Requests, ConnResId),
    assert_action_success(TCConfig),
    ok.

%% A configured `client_id` is sent by both the metadata and produce (leader)
%% connections.
-doc "Configured client IDs reach bootstrap and leader connections.".
t_client_id(TCConfig) ->
    ClientId = <<"jmq-client-id">>,
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{<<"kafka_client_id">> => ClientId})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Requests = produce_and_wait(RuleTopic),
    assert_api_versions_requested(Requests),
    assert_client_id(Requests, ClientId),
    assert_produce(Requests, ClientId),
    assert_action_success(TCConfig),
    ok.

-doc "JMQ-style version and PLAIN responses allow producing and reconnecting through REST.".
t_jmq_compatibility(TCConfig) ->
    ClientId = <<"test-jmq-client">>,
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{
            <<"kafka_client_id">> => ClientId,
            <<"authentication">> => #{
                <<"mechanism">> => <<"plain">>,
                <<"username">> => <<"test-user">>,
                <<"password">> => <<"test-password">>
            }
        })
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Requests1 = produce_and_wait(RuleTopic),
    assert_jmq_requests(Requests1, ClientId),
    assert_action_success(TCConfig),
    SuccessBeforeReconnect = action_success(TCConfig),
    flush_requests(),
    ok = emqx_bridge_kafka_fake_broker:close_connections(?config(broker, TCConfig)),
    Requests2 = produce_and_wait(RuleTopic),
    assert_jmq_requests(Requests2, ClientId),
    ?assertEqual(
        [], ordsets:intersection(produce_conn_ids(Requests1), produce_conn_ids(Requests2))
    ),
    assert_action_success(TCConfig, SuccessBeforeReconnect + 1).

assert_jmq_requests(Requests, ClientId) ->
    assert_api_versions_request_version(Requests, 0),
    assert_client_id(Requests, ClientId),
    assert_produce(Requests, ClientId),
    lists:foreach(
        fun(Key) ->
            Versions = [V || #{api_key := K, api_version := V} <- Requests, K =:= Key],
            ?assertNotEqual([], Versions),
            ?assertEqual([1], lists:usort(Versions))
        end,
        [17, 36]
    ).

%% An empty `client_id` is treated as "not configured".
-doc "An empty client ID uses the connector resource ID.".
t_empty_client_id(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{<<"kafka_client_id">> => <<>>})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    Requests = produce_and_wait(RuleTopic),
    assert_client_id(Requests, ConnResId),
    assert_produce(Requests, ConnResId),
    assert_action_success(TCConfig),
    ok.

-doc "Omitting the client-ID key during an update restores the resource ID.".
t_omitted_client_id(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{<<"kafka_client_id">> => <<"configured-client">>})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    assert_produce(produce_and_wait(RuleTopic), <<"configured-client">>),
    assert_action_success(TCConfig),
    Success = action_success(TCConfig),
    flush_requests(),
    ?assertMatch({200, _}, update_connector_api(TCConfig, #{})),
    wait_connector_connected(TCConfig),
    assert_produce(
        produce_and_wait(RuleTopic), emqx_bridge_v2_testlib:connector_resource_id(TCConfig)
    ),
    assert_action_success(TCConfig, Success + 1).

%% Two connectors may use the same Kafka client ID: they keep separate wolff
%% clients and separate connections.
-doc "Connectors with the same Kafka client ID keep independent connections.".
t_same_client_id_two_connectors(TCConfig) ->
    ClientId = <<"shared-client-id">>,
    Name1 = <<"connector_one">>,
    Name2 = <<"connector_two">>,
    TC1 = replace(TCConfig, [
        {connector_name, Name1},
        {action_name, Name1},
        {action_config, action_config(Name1)}
    ]),
    TC2 = replace(TCConfig, [
        {connector_name, Name2},
        {action_name, Name2},
        {action_config, action_config(Name2)}
    ]),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TC1, #{<<"kafka_client_id">> => ClientId})
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TC2, #{<<"kafka_client_id">> => ClientId})
    ),
    {201, _} = create_action_api(TC1, #{}),
    {201, _} = create_action_api(TC2, #{}),
    #{topic := RuleTopic1} = simple_create_rule_api(TC1),
    #{topic := RuleTopic2} = simple_create_rule_api(TC2),
    ConnResId1 = emqx_bridge_v2_testlib:connector_resource_id(TC1),
    ConnResId2 = emqx_bridge_v2_testlib:connector_resource_id(TC2),
    {ok, WolffPid1} = wolff_client_sup:find_client(ConnResId1),
    {ok, WolffPid2} = wolff_client_sup:find_client(ConnResId2),
    ?assertNotEqual(WolffPid1, WolffPid2),
    ?retry(
        100,
        20,
        ?assert(alive_leader_connections(WolffPid1, emqx_bridge_v2_testlib:resource_id(TC1)))
    ),
    ?retry(
        100,
        20,
        ?assert(alive_leader_connections(WolffPid2, emqx_bridge_v2_testlib:resource_id(TC2)))
    ),
    Requests1 = produce_and_wait(RuleTopic1),
    Requests2 = produce_and_wait(RuleTopic2),
    assert_client_id(Requests1, ClientId),
    assert_client_id(Requests2, ClientId),
    ?assertNotEqual([], produce_conn_ids(Requests1)),
    ?assertNotEqual([], produce_conn_ids(Requests2)),
    assert_action_success(TC1),
    assert_action_success(TC2),
    ok.

%% Updating the connector applies the new `client_id` on the new connections,
%% and clearing it restores the default.
-doc "Updating and clearing the client ID replaces connection headers.".
t_update_client_id(TCConfig) ->
    ClientId1 = <<"first-client-id">>,
    ClientId2 = <<"second-client-id">>,
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{<<"kafka_client_id">> => ClientId1})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Requests1 = produce_and_wait(RuleTopic),
    assert_produce(Requests1, ClientId1),
    assert_action_success(TCConfig),
    Success1 = action_success(TCConfig),
    %% Update the client ID.
    flush_requests(),
    ?assertMatch({200, _}, update_connector_api(TCConfig, #{<<"kafka_client_id">> => ClientId2})),
    wait_connector_connected(TCConfig),
    Requests2 = produce_and_wait(RuleTopic),
    assert_produce(Requests2, ClientId2),
    assert_action_success(TCConfig, Success1 + 1),
    Success2 = action_success(TCConfig),
    %% Clearing it falls back to the connector resource ID.
    flush_requests(),
    ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
    ?assertMatch({200, _}, update_connector_api(TCConfig, #{<<"kafka_client_id">> => <<>>})),
    wait_connector_connected(TCConfig),
    Requests3 = produce_and_wait(RuleTopic),
    assert_produce(Requests3, ConnResId),
    assert_action_success(TCConfig, Success2 + 1),
    ok.

%% When the broker closes the connections, the client reconnects and keeps
%% using the configured options.
-doc "Reconnected clients preserve the configured client ID.".
t_reconnect_after_broker_close(TCConfig) ->
    ClientId = <<"jmq-client-id">>,
    Broker = ?config(broker, TCConfig),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{<<"kafka_client_id">> => ClientId})
    ),
    {201, _} = create_action_api(TCConfig, #{}),
    #{topic := RuleTopic} = simple_create_rule_api(TCConfig),
    Requests1 = produce_and_wait(RuleTopic),
    assert_action_success(TCConfig),
    SuccessBeforeReconnect = action_success(TCConfig),
    ProduceConn1 = hd(produce_conn_ids(Requests1)),
    flush_requests(),
    ok = emqx_bridge_kafka_fake_broker:close_connections(Broker),
    Requests2 = produce_and_wait(RuleTopic),
    assert_client_id(Requests2, ClientId),
    ProduceConns2 = produce_conn_ids(Requests2),
    ?assertNotEqual([], ProduceConns2),
    ?assertNot(lists:member(ProduceConn1, ProduceConns2)),
    assert_action_success(TCConfig, SuccessBeforeReconnect + 1),
    ok.

%%------------------------------------------------------------------------------
%% Request collection and assertions
%%------------------------------------------------------------------------------

produce_and_wait(RuleTopic) ->
    produce_and_wait(RuleTopic, _Attempts = 3).

produce_and_wait(_RuleTopic, 0) ->
    ct:fail(no_produce_request);
produce_and_wait(RuleTopic, Attempts) ->
    ok = publish(RuleTopic),
    case wait_requests_soft(fun(Rs) -> has_api_key(?API_PRODUCE, Rs) end, ?WAIT_TIMEOUT) of
        {ok, Requests} ->
            Requests;
        {timeout, _Requests} ->
            produce_and_wait(RuleTopic, Attempts - 1)
    end.

publish(RuleTopic) ->
    {ok, C} = emqtt:start_link(#{proto_ver => v5}),
    {ok, _} = emqtt:connect(C),
    Payload = integer_to_binary(erlang:unique_integer([positive])),
    %% No matching MQTT subscribers is expected: the rule engine has no
    %% subscription, it only hooks the publish.
    _ = emqtt:publish(C, RuleTopic, Payload, [{qos, 1}]),
    _ = emqtt:stop(C),
    ok.

wait_requests_soft(Pred, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    wait_requests_soft(Pred, Deadline, []).

wait_requests_soft(Pred, Deadline, Acc) ->
    NewAcc = Acc ++ collect_requests(),
    case Pred(NewAcc) of
        true ->
            {ok, NewAcc};
        false ->
            case erlang:monotonic_time(millisecond) >= Deadline of
                true ->
                    {timeout, NewAcc};
                false ->
                    timer:sleep(50),
                    wait_requests_soft(Pred, Deadline, NewAcc)
            end
    end.

collect_requests() ->
    collect_requests([]).

collect_requests(Acc) ->
    receive
        {kafka_request, Role, ConnId, ApiKey, ApiVersion, ClientId} ->
            collect_requests([
                #{
                    role => Role,
                    conn_id => ConnId,
                    api_key => ApiKey,
                    api_version => ApiVersion,
                    client_id => ClientId
                }
                | Acc
            ])
    after 0 ->
        lists:reverse(Acc)
    end.

flush_requests() ->
    _ = collect_requests(),
    ok.

has_api_key(ApiKey, Requests) ->
    lists:any(fun(#{api_key := K}) -> K =:= ApiKey end, Requests).

produce_conn_ids(Requests) ->
    lists:usort([ConnId || #{api_key := ?API_PRODUCE, conn_id := ConnId} <- Requests]).

assert_api_versions_requested(Requests) ->
    ?assert(has_api_key(?API_API_VERSIONS, Requests)).

assert_api_versions_request_version(Requests, Version) ->
    Queries = [R || #{api_key := ?API_API_VERSIONS} = R <- Requests],
    ?assertNotEqual([], Queries),
    ?assert(lists:all(fun(#{api_version := V}) -> V =:= Version end, Queries)).

assert_client_id(Requests, ClientId) ->
    ?assertNotEqual([], Requests),
    ?assertEqual([ClientId], lists:usort([C || #{client_id := C} <- Requests])).

assert_produce(Requests, ClientId) ->
    Produce = [R || #{api_key := ?API_PRODUCE} = R <- Requests],
    ?assertNotEqual([], Produce),
    ?assertEqual([ClientId], lists:usort([C || #{client_id := C} <- Produce])),
    ?assert(lists:all(fun(#{role := Role}) -> Role =:= leader end, Produce)).

assert_action_success(TCConfig) ->
    assert_action_success(TCConfig, 1).

assert_action_success(TCConfig, Minimum) ->
    ?retry(100, 20, ?assert(action_success(TCConfig) >= Minimum)),
    ok.

action_success(TCConfig) ->
    {200, #{<<"metrics">> := #{<<"success">> := Success}}} = action_metrics(TCConfig),
    Success.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

action_config(ActionName) ->
    emqx_bridge_kafka_testlib:action_config(#{
        <<"connector">> => ActionName,
        <<"parameters">> => #{<<"topic">> => ?TOPIC}
    }).

bootstrap_hosts(Host, Port) ->
    iolist_to_binary([Host, ":", integer_to_binary(Port)]).

replace(TCConfig, Overrides) ->
    Kept = lists:foldl(
        fun({Key, _}, Acc) -> lists:keydelete(Key, 1, Acc) end,
        TCConfig,
        Overrides
    ),
    Overrides ++ Kept.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

update_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:update_connector_api_simple(TCConfig, Overrides).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

action_metrics(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

wait_connector_connected(TCConfig) ->
    Name = ?config(connector_name, TCConfig),
    ?retry(
        200,
        100,
        begin
            {200, #{<<"status">> := Status}} =
                emqx_bridge_v2_testlib:simplify_result(
                    emqx_bridge_v2_testlib:get_connector_api(?CONNECTOR_TYPE, Name)
                ),
            ?assertEqual(<<"connected">>, Status)
        end
    ).

alive_leader_connections(WolffPid, ActionResId) ->
    case wolff_client:get_leader_connections(WolffPid, ActionResId, ?TOPIC, all_partitions) of
        {ok, Leaders} ->
            lists:all(
                fun
                    ({_Partition, Pid}) -> is_pid(Pid) andalso erlang:is_process_alive(Pid);
                    (_) -> false
                end,
                Leaders
            ) andalso Leaders =/= [];
        {error, _} ->
            false
    end.
