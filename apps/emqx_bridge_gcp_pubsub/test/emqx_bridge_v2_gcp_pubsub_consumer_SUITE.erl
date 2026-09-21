%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_gcp_pubsub_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(CONNECTOR_TYPE_BIN, <<"gcp_pubsub_consumer">>).
-define(SOURCE_TYPE_BIN, <<"gcp_pubsub_consumer">>).

-define(PREPARED_REQUEST_PAT(METHOD, PATH, BODY),
    {prepared_request, {METHOD, PATH, BODY}, _}
).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    emqx_bridge_gcp_pubsub_consumer_SUITE:init_per_suite(Config).

end_per_suite(Config) ->
    emqx_bridge_gcp_pubsub_consumer_SUITE:end_per_suite(Config).

init_per_testcase(TestCase, Config) ->
    common_init_per_testcase(TestCase, Config).

common_init_per_testcase(TestCase, Config0) ->
    ct:timetrap(timer:seconds(60)),
    ServiceAccountJSON =
        #{<<"project_id">> := ProjectId} =
        emqx_bridge_gcp_pubsub_utils:generate_service_account_json(),
    UniqueNum = integer_to_binary(erlang:unique_integer()),
    Name = <<(atom_to_binary(TestCase))/binary, UniqueNum/binary>>,
    ConnectorConfig = connector_config(Name, ServiceAccountJSON),
    PubsubTopic = Name,
    SourceConfig = source_config(#{
        connector => Name,
        parameters => #{topic => PubsubTopic}
    }),
    Config = [
        {bridge_kind, source},
        {source_type, ?SOURCE_TYPE_BIN},
        {source_name, Name},
        {source_config, SourceConfig},
        {connector_name, Name},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_config, ConnectorConfig},
        {service_account_json, ServiceAccountJSON},
        {project_id, ProjectId},
        {pubsub_topic, PubsubTopic}
        | Config0
    ],
    ok = emqx_bridge_gcp_pubsub_consumer_SUITE:ensure_topic(Config, PubsubTopic),
    snabbkaffe:start_trace(),
    Config.

end_per_testcase(_Testcase, Config) ->
    ProxyHost = ?config(proxy_host, Config),
    ProxyPort = ?config(proxy_port, Config),
    emqx_common_test_helpers:reset_proxy(ProxyHost, ProxyPort),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = snabbkaffe:stop(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

connector_config(Name, ServiceAccountJSON) ->
    InnerConfigMap0 =
        #{
            <<"enable">> => true,
            <<"tags">> => [<<"bridge">>],
            <<"description">> => <<"my cool bridge">>,
            <<"connect_timeout">> => <<"5s">>,
            <<"pool_size">> => 8,
            <<"pipelining">> => <<"100">>,
            <<"max_retries">> => <<"2">>,
            <<"max_inactive">> => <<"10s">>,
            <<"service_account_json">> => ServiceAccountJSON,
            <<"resource_opts">> =>
                emqx_bridge_v2_testlib:common_connector_resource_opts()
        },
    emqx_bridge_v2_testlib:parse_and_check_connector(?SOURCE_TYPE_BIN, Name, InnerConfigMap0).

source_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"enable">> => true,
            <<"connector">> => <<"please override">>,
            <<"parameters">> =>
                #{
                    <<"topic">> => <<"my-topic">>
                },
            <<"resource_opts">> =>
                maps:merge(
                    emqx_bridge_v2_testlib:common_source_resource_opts(),
                    #{<<"request_ttl">> => <<"1s">>}
                )
        },
    maps:merge(CommonConfig, Overrides).

assert_persisted_service_account_json_is_binary(ConnectorName) ->
    %% ensure cluster.hocon has a binary encoded json string as the value
    {ok, Hocon} = hocon:files([application:get_env(emqx, cluster_hocon_file, undefined)]),
    ?assertMatch(
        Bin when is_binary(Bin),
        emqx_utils_maps:deep_get(
            [
                <<"connectors">>,
                <<"gcp_pubsub_consumer">>,
                ConnectorName,
                <<"service_account_json">>
            ],
            Hocon
        )
    ),
    ok.

create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

create_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:create_source_api(TCConfig, Overrides).

get_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

get_source_api(TCConfig) ->
    #{type := Type, name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_source_api(Type, Name)
    ).

start_source_api(TCConfig) ->
    #{
        kind := Kind,
        type := Type,
        name := Name
    } =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:op_bridge_api(
            Kind, "start", Type, Name
        )
    ).

probe_source_api(TCConfig) ->
    probe_source_api(TCConfig, _Overrides = #{}).

probe_source_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:probe_bridge_api(TCConfig, Overrides)
    ).

disable_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:disable_connector_api(Type, Name)
    ).

enable_connector_api(TCConfig) ->
    #{connector_type := Type, connector_name := Name} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:enable_connector_api(Type, Name)
    ).

source_resource_id(Config) ->
    Name = ?config(source_name, Config),
    emqx_bridge_v2:source_id(?SOURCE_TYPE_BIN, Name, Name).

get_pull_worker_pids(Config) ->
    SourceResId = source_resource_id(Config),
    [
        PullWorkerPid
     || {_WorkerName, PoolWorkerPid} <- ecpool:workers(SourceResId),
        {ok, PullWorkerPid} <- [ecpool_worker:client(PoolWorkerPid)]
    ].

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_start_stop(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_stop(Config, gcp_pubsub_stop),
    ok.

t_create_via_http(Config) ->
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    ok.

t_create_via_http_json_object_service_account(Config0) ->
    %% After the config goes through the roundtrip with `hocon_tconf:check_plain', service
    %% account json comes back as a binary even if the input is a json object.
    ConnectorName = ?config(connector_name, Config0),
    ConnConfig0 = ?config(connector_config, Config0),
    Config1 = proplists:delete(connector_config, Config0),
    ConnConfig1 = maps:update_with(
        <<"service_account_json">>,
        fun(X) ->
            ?assert(is_binary(X), #{json => X}),
            JSON = emqx_utils_json:decode(X),
            ?assert(is_map(JSON)),
            JSON
        end,
        ConnConfig0
    ),
    Config = [{connector_config, ConnConfig1} | Config1],
    ok = emqx_bridge_v2_testlib:t_create_via_http(Config),
    assert_persisted_service_account_json_is_binary(ConnectorName),
    ok.

t_consume(Config) ->
    Topic = ?config(pubsub_topic, Config),
    Payload = #{<<"key">> => <<"value">>},
    Attributes = #{<<"hkey">> => <<"hval">>},
    ProduceFn = fun() ->
        emqx_bridge_gcp_pubsub_consumer_SUITE:pubsub_publish(
            Config,
            Topic,
            [
                #{
                    <<"data">> => Payload,
                    <<"orderingKey">> => <<"ok">>,
                    <<"attributes">> => Attributes
                }
            ]
        )
    end,
    Encoded = emqx_utils_json:encode(Payload),
    CheckFn = fun(Message) ->
        ?assertMatch(
            #{
                attributes := Attributes,
                message_id := _,
                ordering_key := <<"ok">>,
                publish_time := _,
                topic := Topic,
                value := Encoded
            },
            Message
        )
    end,
    ok = emqx_bridge_v2_testlib:t_consume(
        Config,
        #{
            consumer_ready_tracepoint => ?match_event(
                #{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}
            ),
            produce_fn => ProduceFn,
            check_fn => CheckFn,
            produce_tracepoint => ?match_event(
                #{
                    ?snk_kind := "gcp_pubsub_consumer_worker_handle_message",
                    ?snk_span := {complete, _}
                }
            )
        }
    ),
    ok.

t_update_topic(Config) ->
    %% Tests that, if a bridge originally has the legacy field `topic_mapping' filled in
    %% and later is updated using v2 APIs, then the legacy field is cleared and the new
    %% `topic' field is used.
    ConnectorConfig = ?config(connector_config, Config),
    SourceConfig = ?config(source_config, Config),
    Name = ?config(source_name, Config),
    V1Config0 = emqx_action_info:connector_action_config_to_bridge_v1_config(
        ?SOURCE_TYPE_BIN,
        ConnectorConfig,
        SourceConfig
    ),
    V1Config = emqx_utils_maps:deep_put(
        [<<"consumer">>, <<"topic_mapping">>],
        V1Config0,
        [
            #{
                <<"pubsub_topic">> => <<"old_topic">>,
                <<"mqtt_topic">> => <<"">>,
                <<"qos">> => 2,
                <<"payload_template">> => <<"template">>
            }
        ]
    ),
    %% Note: using v1 API
    {ok, {{_, 201, _}, _, _}} = emqx_bridge_testlib:create_bridge_api(
        ?SOURCE_TYPE_BIN,
        Name,
        V1Config
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"parameters">> := #{<<"topic">> := <<"old_topic">>}}}},
        emqx_bridge_v2_testlib:get_source_api(?SOURCE_TYPE_BIN, Name)
    ),
    %% Note: we don't add `topic_mapping' again here to the parameters.
    {ok, {{_, 200, _}, _, _}} = emqx_bridge_v2_testlib:update_bridge_api(
        Config,
        #{<<"parameters">> => #{<<"topic">> => <<"new_topic">>}}
    ),
    ?assertMatch(
        {ok, {{_, 200, _}, _, #{<<"parameters">> := #{<<"topic">> := <<"new_topic">>}}}},
        emqx_bridge_v2_testlib:get_source_api(?SOURCE_TYPE_BIN, Name)
    ),
    ok.

%% original issue: source was created with a service account without the correct
%% permissions.  later, the permissions were granted to the service account.  in the
%% meantime, if the resource manager attempted to reinstall the source more than once, it
%% could end up in a state where the source would not be part of its internal installed
%% channels, and then the optvar marking the source as unhealthy would not be cleared.
%% here, we ensure that such optvar is cleared, and the source eventually recovers once
%% the permissions are granted.
t_clear_stuck_unhealthy(TCConfig) ->
    emqx_common_test_helpers:with_mock(
        emqx_bridge_gcp_pubsub_client,
        query_sync,
        fun(PreparedRequest = ?PREPARED_REQUEST_PAT(Method, _Path, _Body), Client) ->
            %% original issue: 403 when creating subscription
            case Method =:= put of
                true ->
                    ct:pal("mocking response"),
                    emqx_bridge_gcp_pubsub_consumer_SUITE:permission_denied_response();
                false ->
                    meck:passthrough([PreparedRequest, Client])
            end
        end,
        fun() ->
            {201, #{<<"status">> := <<"connected">>}} =
                create_connector_api(TCConfig, #{}),
            {201, #{<<"status">> := <<"disconnected">>}} =
                create_source_api(TCConfig, #{}),
            ?assertMatch(
                {200, #{<<"status">> := <<"disconnected">>}},
                get_source_api(TCConfig)
            ),
            ok
        end
    ),
    %% now, we "grant" the permissions by removing the mock.  should recover by itself.
    ?retry(
        1_000,
        10,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_source_api(TCConfig)
        )
    ),
    ok.

%% Checks that probing ("Test Connection") a consumer source does not disturb the health
%% status of an already-running source.
%%
%% Each pool worker publishes its subscription status under an optvar key, and the running
%% source's health check reads it back.  Those keys must be scoped by the worker's own pool:
%% worker indices restart from 1 in every pool, so an index-only key would be shared with the
%% probe's temporary pool, whose teardown then clears the live pool's flags — leaving the
%% running source stuck `disconnected` (health check timeout) until manually restarted
%% (issue #18190).
t_probe_does_not_disturb_running_source(TCConfig) ->
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {ok, SRef0} =
                snabbkaffe:subscribe(
                    ?match_event(#{?snk_kind := "gcp_pubsub_consumer_worker_subscription_ready"}),
                    40_000
                ),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_source_api(TCConfig, #{}),
            {ok, _} = snabbkaffe:receive_events(SRef0),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_source_api(TCConfig)
            ),
            %% "Test Connection": a dry-run probe of the same source config; its
            %% temporary worker pool is torn down once the probe concludes.
            ?assertMatch({204, _}, probe_source_api(TCConfig)),
            %% The running source must not be affected by the probe pool teardown.
            ?assertMatch(
                #{status := ?status_connected},
                emqx_bridge_v2_testlib:health_check_channel(TCConfig)
            ),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_source_api(TCConfig)
            ),
            ok
        end,
        []
    ),
    ok.

%% test for hot upgrade post-upgrade hook.
-define(OPTVAR_SUB_OK(X), {emqx_bridge_gcp_pubsub_consumer_worker, subscription_ok, X}).
t_post_upgrade_pr_17624(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_source_api(TCConfig, #{}),
    ConnNameB = <<"disabled">>,
    SourceNameB = ConnNameB,
    TCConfigB = [{connector_name, ConnNameB}, {source_name, SourceNameB} | TCConfig],
    {201, _} = create_connector_api(TCConfigB, #{}),
    {201, _} = create_source_api(TCConfigB, #{
        <<"connector">> => ConnNameB
    }),

    %% set up old opvars to simulate previous version
    WorkerPids = get_pull_worker_pids(TCConfig),
    [Pid0 | _] = WorkerPids,
    optvar:set(?OPTVAR_SUB_OK(Pid0), subscription_ok),
    %% also create one to simulate a dead worker leak
    optvar:set(?OPTVAR_SUB_OK(self()), subscription_ok),

    {204, _} = disable_connector_api(TCConfigB),

    ?check_trace(
        begin
            emqx_post_upgrade:pr_17624_gcp_pubsub_consumer_worker_optvars("vsn"),
            ok
        end,
        fun(Trace) ->
            ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
            ?assertMatch(
                [
                    #{?snk_kind := gcp_pubsub_consumer_stop_enter, instance_id := ConnResId},
                    #{?snk_kind := gcp_pubsub_consumer_start, instance_id := ConnResId}
                ],
                ?of_kind([gcp_pubsub_consumer_stop_enter, gcp_pubsub_consumer_start], Trace)
            ),
            ?assertEqual(
                [],
                [
                    K
                 || ?OPTVAR_SUB_OK(Pid) = K <- optvar:list_all(),
                    is_pid(Pid)
                ]
            ),
            ok
        end
    ),
    ok.

%% Verifies that reading the connector with a legacy service account field (in the root of
%% the connector config) via the HTTP API returns a redacted service account.
%% In 6.2.0, this was moved to under an `authentication` key.
t_legacy_service_account_json_redact(TCConfig) ->
    ?assertMatch(
        {201, #{<<"service_account_json">> := <<"******">>}},
        create_connector_api(TCConfig, #{})
    ),
    ?assertMatch(
        {200, #{<<"service_account_json">> := <<"******">>}},
        get_connector_api(TCConfig)
    ),
    ok.

%% Verifies that the redacted connector body returned by the HTTP API can be sent back via
%% update and probe, and that the stored service account JSON is kept.
t_service_account_json_redacted_round_trip(TCConfig) ->
    ?assertMatch({201, _}, create_connector_api(TCConfig, #{})),
    ok = emqx_bridge_gcp_pubsub_utils:assert_redacted_service_account_json_round_trip(TCConfig),
    ok.

%% Checks the hot-upgrade hook that handles worker optvars keyed by the bare ecpool worker
%% index (left by pre-upgrade beams; current keys are scoped by the source resource id): the
%% affected connectors are restarted and the stale index-keyed entries are swept.
t_post_upgrade_pr_19081(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_source_api(TCConfig, #{}),
    ConnNameB = <<"disabled">>,
    SourceNameB = ConnNameB,
    TCConfigB = [{connector_name, ConnNameB}, {source_name, SourceNameB} | TCConfig],
    {201, _} = create_connector_api(TCConfigB, #{}),
    {201, _} = create_source_api(TCConfigB, #{
        <<"connector">> => ConnNameB
    }),

    %% set up old opvars to simulate a worker started by the previous version
    optvar:set(?OPTVAR_SUB_OK(1), subscription_ok),

    {204, _} = disable_connector_api(TCConfigB),

    ?check_trace(
        begin
            emqx_post_upgrade:pr_19081_gcp_pubsub_consumer_worker_optvars("vsn"),
            ok
        end,
        fun(Trace) ->
            ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
            ?assertMatch(
                [
                    #{?snk_kind := gcp_pubsub_consumer_stop_enter, instance_id := ConnResId},
                    #{?snk_kind := gcp_pubsub_consumer_start, instance_id := ConnResId}
                ],
                ?of_kind([gcp_pubsub_consumer_stop_enter, gcp_pubsub_consumer_start], Trace)
            ),
            ?assertEqual(
                [],
                [
                    K
                 || ?OPTVAR_SUB_OK(Idx) = K <- optvar:list_all(),
                    is_integer(Idx)
                ]
            ),
            ok
        end
    ),
    ok.
