%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_resource/include/emqx_resource_runtime.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(CONNECTOR_TYPE, kinesis).
-define(CONNECTOR_TYPE_BIN, <<"kinesis">>).
-define(ACTION_TYPE, kinesis).
-define(ACTION_TYPE_BIN, <<"kinesis">>).

-define(PROXY_NAME, "kinesis").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(local, local).
-define(cluster, cluster).
-define(without_batch, without_batch).
-define(with_batch, with_batch).

-define(STREAM_NAME, <<"stream0">>).

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
    emqx_bridge_v2_testlib:local_and_cluster_groups(?MODULE, ?local, ?cluster).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(?local = Group, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kinesis,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ];
init_per_group(?cluster = Group, TCConfig) ->
    reset_proxy(),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            emqx_bridge_kinesis,
            emqx_bridge,
            emqx_rule_engine,
            emqx_management
        ],
        #{work_dir => emqx_cth_suite:work_dir(Group, TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ];
init_per_group(?with_batch, TCConfig0) ->
    [{batch_size, 100}, {batch_time, <<"200ms">>} | TCConfig0];
init_per_group(?without_batch, TCConfig0) ->
    [{batch_size, 1}, {batch_time, <<"0ms">>} | TCConfig0];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(?local, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok;
end_per_group(?cluster, TCConfig) ->
    Apps = get_config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig) ->
    reset_proxy(),
    Path = group_path(TCConfig, no_groups),
    ct:pal(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(#{}),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{
        <<"connector">> => ConnectorName,
        <<"resource_opts">> => #{
            <<"batch_size">> => get_config(batch_size, TCConfig, 1),
            <<"batch_time">> => get_config(batch_time, TCConfig, <<"0ms">>)
        }
    }),
    create_stream(?STREAM_NAME),
    ErlcloudConfig = mk_erlcloud_config(ConnectorConfig),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE},
        {action_name, ActionName},
        {action_config, ActionConfig},
        {erlcloud_config, ErlcloudConfig}
        | TCConfig
    ].

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    reset_proxy(),
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
        <<"endpoint">> => <<"http://toxiproxy:4566">>,
        <<"aws_access_key_id">> => <<"aws_access_key_id">>,
        <<"aws_secret_access_key">> => <<"aws_secret_access_key">>,
        <<"max_retries">> => 3,
        <<"pool_size">> => 2,
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
            <<"payload_template">> => <<"${.}">>,
            <<"partition_key">> => <<"key">>,
            <<"stream_name">> => ?STREAM_NAME
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

create_stream(StreamName) ->
    Host = "toxiproxy.emqx.net",
    Port = 4566,
    ErlcloudConfig = erlcloud_kinesis:new("access_key", "secret", Host, Port, "http://"),
    {ok, _} = application:ensure_all_started(erlcloud),
    delete_stream(StreamName, ErlcloudConfig),
    {ok, _} = erlcloud_kinesis:create_stream(StreamName, 1, ErlcloudConfig),
    ?retry(
        _Sleep = 100,
        _Attempts = 10,
        begin
            {ok, [{<<"StreamDescription">>, StreamInfo}]} =
                erlcloud_kinesis:describe_stream(StreamName, ErlcloudConfig),
            ?assertEqual(
                <<"ACTIVE">>,
                proplists:get_value(<<"StreamStatus">>, StreamInfo)
            )
        end
    ),
    on_exit(fun() -> delete_stream(StreamName, ErlcloudConfig) end),
    ok.

delete_stream(TCConfig) when is_list(TCConfig) ->
    #{<<"parameters">> := #{<<"stream_name">> := StreamName}} =
        get_config(action_config, TCConfig),
    ErlcloudConfig = get_config(erlcloud_config, TCConfig),
    delete_stream(StreamName, ErlcloudConfig).

delete_stream(StreamName, ErlcloudConfig) ->
    case erlcloud_kinesis:delete_stream(StreamName, ErlcloudConfig) of
        {ok, _} ->
            ?retry(
                _Sleep = 100,
                _Attempts = 10,
                ?assertMatch(
                    {error, {<<"ResourceNotFoundException">>, _}},
                    erlcloud_kinesis:describe_stream(StreamName, ErlcloudConfig)
                )
            );
        _ ->
            ok
    end,
    ok.

mk_erlcloud_config(ConnectorConfig) ->
    #{<<"endpoint">> := Endpoint} = ConnectorConfig,
    #{scheme := Scheme, hostname := Host, port := Port} =
        emqx_schema:parse_server(
            Endpoint,
            #{
                default_port => 443,
                supported_schemes => ["http", "https"]
            }
        ),
    erlcloud_kinesis:new("access_key", "secret", Host, Port, Scheme ++ "://").

wait_record(TCConfig) ->
    ShardIt = get_shard_iterator(TCConfig),
    wait_record(ShardIt, TCConfig).

wait_record(ShardIt, TCConfig) ->
    Timeout = 300,
    Attempts = 10,
    [Record] = wait_records(TCConfig, ShardIt, 1, Timeout, Attempts),
    Record.

wait_records(TCConfig, ShardIt, Count, Timeout, Attempts) ->
    ErlcloudTCConfig = get_config(erlcloud_config, TCConfig),
    ?retry(
        Timeout,
        Attempts,
        begin
            {ok, Ret} = erlcloud_kinesis:get_records(ShardIt, ErlcloudTCConfig),
            Records = proplists:get_value(<<"Records">>, Ret),
            Count = length(Records),
            Records
        end
    ).

get_shard_iterator(TCConfig) ->
    get_shard_iterator(TCConfig, 1).

get_shard_iterator(TCConfig, Index) ->
    #{<<"parameters">> := #{<<"stream_name">> := StreamName}} =
        get_config(action_config, TCConfig),
    ErlcloudConfig = get_config(erlcloud_config, TCConfig),
    {ok, [{<<"Shards">>, Shards}]} = erlcloud_kinesis:list_shards(StreamName, ErlcloudConfig),
    Shard = lists:nth(Index, lists:sort(Shards)),
    ShardId = proplists:get_value(<<"ShardId">>, Shard),
    {ok, [{<<"ShardIterator">>, ShardIt}]} =
        erlcloud_kinesis:get_shard_iterator(StreamName, ShardId, <<"LATEST">>, ErlcloudConfig),
    ShardIt.

create_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(TCConfig, Overrides)
    ).

create_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(TCConfig, Overrides)
    ).

probe_connector_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:probe_connector_api2(TCConfig, Overrides).

get_connector_api(TCConfig) ->
    #{connector_type := ConnectorType, connector_name := ConnectorName} =
        emqx_bridge_v2_testlib:get_common_values(TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(
            ConnectorType, ConnectorName
        )
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

update_action_api(TCConfig, Overrides) ->
    emqx_bridge_v2_testlib:update_bridge_api2(TCConfig, Overrides).

get_action_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_api2(TCConfig).

get_action_metrics_api(TCConfig) ->
    emqx_bridge_v2_testlib:get_action_metrics_api(TCConfig).

simple_create_rule_api(TCConfig) ->
    emqx_bridge_v2_testlib:simple_create_rule_api(TCConfig).

start_client() ->
    start_client(_Opts = #{}).

start_client(Opts0) ->
    Opts = maps:merge(#{proto_ver => v5}, Opts0),
    {ok, C} = emqtt:start_link(Opts),
    on_exit(fun() -> emqtt:stop(C) end),
    {ok, _} = emqtt:connect(C),
    C.

fmt_atom(Fmt, Args) ->
    binary_to_atom(
        iolist_to_binary(
            io_lib:format(Fmt, Args)
        )
    ).

peer_name(TestName, N) ->
    fmt_atom("~s_~b", [TestName, N]).

next_cluster_n() ->
    case get(cluster_n) of
        undefined ->
            put(cluster_n, 2),
            1;
        N ->
            put(cluster_n, N + 1),
            N
    end.

start_cluster(TestCase, Specs, TCConfig) ->
    AppsSpecs = [
        emqx,
        emqx_conf,
        emqx_bridge_kinesis,
        emqx_bridge,
        emqx_rule_engine,
        emqx_management
    ],
    NodeSpecs =
        lists:map(
            fun({N, Opts0}) ->
                Opts =
                    case N == 1 of
                        true ->
                            Opts0#{
                                apps => AppsSpecs ++
                                    [emqx_mgmt_api_test_util:emqx_dashboard()]
                            };
                        false ->
                            Opts0#{apps => AppsSpecs}
                    end,
                {peer_name(TestCase, N), Opts}
            end,
            lists:enumerate(Specs)
        ),
    Name = fmt_atom("~s_~b", [TestCase, next_cluster_n()]),
    Nodes =
        [N1 | _] = emqx_cth_cluster:start(
            NodeSpecs,
            #{work_dir => emqx_cth_suite:work_dir(Name, TCConfig)}
        ),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
    emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
    Nodes.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_start_stop(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_start_stop(TCConfig, "kinesis_connector_stop").

t_on_get_status(TCConfig) when is_list(TCConfig) ->
    emqx_bridge_v2_testlib:t_on_get_status(TCConfig).

t_rule_action() ->
    [{matrix, true}].
t_rule_action(matrix) ->
    [[?without_batch], [?with_batch]];
t_rule_action(TCConfig) when is_list(TCConfig) ->
    PrePublishFn = fun(Context) ->
        ShardIt = get_shard_iterator(TCConfig),
        Context#{shard_it => ShardIt}
    end,
    PostPublishFn = fun(Context) ->
        #{payload := Payload, shard_it := ShardIt} = Context,
        ?retry(200, 10, begin
            Record = wait_record(ShardIt, TCConfig),
            ct:pal("record:\n  ~p", [Record]),
            {<<"Data">>, PayloadRaw} = lists:keyfind(<<"Data">>, 1, Record),
            ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(PayloadRaw))
        end)
    end,
    Opts = #{
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_publish_success_with_template(TCConfig) ->
    PayloadFn = fun() -> <<"{\"key\":\"my_key\", \"data\":\"my_data\"}">> end,
    PrePublishFn = fun(Context) ->
        ShardIt = get_shard_iterator(TCConfig),
        Context#{shard_it => ShardIt}
    end,
    PostPublishFn = fun(Context) ->
        #{shard_it := ShardIt} = Context,
        ?retry(200, 10, begin
            Record = wait_record(ShardIt, TCConfig),
            ct:pal("record:\n  ~p", [Record]),
            {<<"Data">>, Payload} = lists:keyfind(<<"Data">>, 1, Record),
            ?assertMatch(<<"my_data">>, Payload),
            {<<"PartitionKey">>, Key} = lists:keyfind(<<"PartitionKey">>, 1, Record),
            ?assertMatch(<<"my_key">>, Key)
        end)
    end,
    ActionOverrides = #{
        <<"parameters">> => #{
            <<"payload_template">> => <<"${payload.data}">>,
            <<"partition_key">> => <<"${payload.key}">>
        }
    },
    Opts = #{
        payload_fn => PayloadFn,
        pre_publish_fn => PrePublishFn,
        post_publish_fn => PostPublishFn,
        action_overrides => ActionOverrides
    },
    emqx_bridge_v2_testlib:t_rule_action(TCConfig, Opts).

t_create_unhealthy(TCConfig) ->
    delete_stream(TCConfig),
    {201, _} = create_connector_api(TCConfig, #{}),
    ?assertMatch(
        {201, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"{unhealthy_target,", _/binary>>
        }},
        create_action_api(TCConfig, #{})
    ),
    ok.

t_start_failed_then_fix(TCConfig) ->
    with_failure(down, fun() ->
        ?assertMatch(
            {201, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := <<"Connection refused">>
            }},
            create_connector_api(TCConfig, #{})
        )
    end),
    ?retry(
        _Sleep1 = 1_000,
        _Attempts1 = 30,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    ok.

t_get_status_unhealthy(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    delete_stream(TCConfig),
    ?retry(
        100,
        100,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_action_api(TCConfig)
        )
    ),
    ok.

t_publish_unhealthy(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ShardIt = get_shard_iterator(TCConfig),
    Payload = <<"payload">>,
    delete_stream(TCConfig),
    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    ?assertError(
        {badmatch, {error, {<<"ResourceNotFoundException">>, _}}},
        wait_record(ShardIt, TCConfig)
    ),
    %% to avoid test flakiness
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 0,
                    <<"failed">> := 1,
                    <<"inflight">> := 0,
                    <<"matched">> := 1,
                    <<"queuing">> := 0,
                    <<"retried">> := 0,
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ?retry(
        100,
        100,
        ?assertMatch(
            {200, #{<<"status">> := <<"disconnected">>}},
            get_action_api(TCConfig)
        )
    ),
    ok.

t_publish_big_msg(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    % Maximum size is 1MB. Using 1MB + 1 here.
    Payload = binary:copy(<<"a">>, 1 * 1024 * 1024 + 1),
    emqx:publish(emqx_message:make(Topic, Payload)),
    %% to avoid test flakiness
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 0,
                    <<"failed">> := 1,
                    <<"inflight">> := 0,
                    <<"matched">> := 1,
                    <<"queuing">> := 0,
                    <<"retried">> := 0,
                    <<"success">> := 0
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    ok.

t_publish_connection_down(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ShardIt = get_shard_iterator(TCConfig),
    Payload = <<"payload">>,
    with_failure(down, fun() ->
        ct:sleep(200),
        ?wait_async_action(
            emqtt:publish(C, Topic, Payload),
            #{?snk_kind := emqx_bridge_kinesis_impl_producer_sync_query},
            5_000
        )
    end),
    %% Wait for reconnection.
    ?retry(
        _Sleep3 = 500,
        _Attempts3 = 20,
        ?assertMatch(
            {200, #{<<"status">> := <<"connected">>}},
            get_connector_api(TCConfig)
        )
    ),
    Record = wait_record(ShardIt, TCConfig),
    %% to avoid test flakiness
    Data = proplists:get_value(<<"Data">>, Record),
    ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(Data)),
    ok.

t_wrong_server(TCConfig) ->
    Overrides = #{
        <<"max_retries">> => 0,
        <<"endpoint">> => <<"https://wrong_server:12345">>
    },
    ?assertMatch(
        {400, _},
        probe_connector_api(TCConfig, Overrides)
    ),
    {201, #{
        <<"status">> := <<"disconnected">>,
        <<"status_reason">> := ErrMsg
    }} = create_connector_api(TCConfig, Overrides),
    ?assertEqual(
        match,
        re:run(ErrMsg, <<"Could not resolve host">>, [{capture, none}]),
        #{msg => ErrMsg}
    ),
    ok.

t_access_denied(TCConfig) ->
    AccessError = {<<"AccessDeniedException">>, <<>>},
    emqx_common_test_helpers:with_mock(
        erlcloud_kinesis,
        list_streams,
        fun() -> {error, AccessError} end,
        fun() ->
            %% probe
            ?assertMatch(
                {400, _},
                probe_connector_api(TCConfig, #{})
            ),
            %% create
            {201, #{
                <<"status">> := <<"disconnected">>,
                <<"status_reason">> := ErrMsg
            }} = create_connector_api(TCConfig, #{}),
            ?assertEqual(
                match,
                re:run(ErrMsg, <<"AccessDeniedException">>, [{capture, none}]),
                #{msg => ErrMsg}
            ),
            ok
        end
    ),
    ok.

t_empty_payload_template(TCConfig0) ->
    TCConfig = emqx_bridge_v2_testlib:proplist_update(TCConfig0, action_config, fun(Old) ->
        emqx_utils_maps:deep_remove([<<"parameters">>, <<"payload_template">>], Old)
    end),
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, #{<<"status">> := <<"connected">>}} = create_action_api(TCConfig, #{}),
    #{topic := Topic} = simple_create_rule_api(TCConfig),
    C = start_client(),
    ShardIt = get_shard_iterator(TCConfig),
    Payload = <<"payload">>,
    emqtt:publish(C, Topic, Payload, [{qos, 1}]),
    %% to avoid test flakiness
    ?retry(
        500,
        10,
        ?assertMatch(
            {200, #{
                <<"metrics">> := #{
                    <<"dropped">> := 0,
                    <<"failed">> := 0,
                    <<"inflight">> := 0,
                    <<"matched">> := 1,
                    <<"queuing">> := 0,
                    <<"retried">> := 0,
                    <<"success">> := 1
                }
            }},
            get_action_metrics_api(TCConfig)
        )
    ),
    Record = wait_record(ShardIt, TCConfig),
    Data = proplists:get_value(<<"Data">>, Record),
    ?assertMatch(
        #{<<"payload">> := <<"payload">>, <<"topic">> := Topic},
        emqx_utils_json:decode(Data)
    ),
    ok.

t_validate_static_constraints(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    %% From <https://docs.aws.amazon.com/kinesis/latest/APIReference/API_PutRecords.html>:
    %% "Each PutRecords request can support up to 500 records.
    %%  Each record in the request can be as large as 1 MiB,
    %%  up to a limit of 5 MiB for the entire request, including partition keys."
    %%
    %% Message size and request size shall be controlled by user, so there is no validators
    %% for them - if exceeded, it will fail like on `t_publish_big_msg` test.
    ?assertMatch(
        {400, #{
            <<"message">> := #{
                <<"kind">> := <<"validation_error">>,
                <<"value">> := 501
            }
        }},
        create_action_api(TCConfig, #{<<"resource_opts">> => #{<<"batch_size">> => 501}})
    ),
    ok.

%% Checks that we throttle control plance APIs when doing connector health checks.
%% For connector HCs, AWS quota is 5 TPS.
t_connector_health_check_rate_limit() ->
    [{?cluster, true}].
t_connector_health_check_rate_limit(TCConfig) when is_list(TCConfig) ->
    %% Using long enough interval with few nodes, should not hit limit
    ct:pal("Testing with 1 node, no rate limit"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"500ms">>}
            }),
            ct:sleep(2_000),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ok
        end
    ),
    snabbkaffe:stop(),

    ct:pal("Testing with 1 node"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"100ms">>}
            }),
            ?block_until(#{?snk_kind := "kinesis_connector_hc_rate_limited"}),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ok
        end
    ),
    snabbkaffe:stop(),

    ct:pal("Testing with 3 nodes"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}, #{role => core}, #{role => replicant}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{
                    %% With more nodes, we trigger quota limit earlier
                    <<"health_check_interval">> => <<"400ms">>
                }
            }),
            ?block_until(#{?snk_kind := "kinesis_connector_hc_rate_limited"}),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we handle timeouts while waiting to consume limiter tokens (connector).
t_connector_health_check_rate_limit_timeout(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"health_check_timeout">> => <<"400ms">>,
                    <<"health_check_interval">> => <<"50ms">>
                }
            }),
            ?inject_crash(
                #{?snk_kind := "kinesis_connector_hc_rate_limited"},
                fun(_) ->
                    timer:sleep(400),
                    false
                end
            ),
            ?block_until(#{?snk_kind := "kinesis_producer_connector_hc_timeout"}),

            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind("kinesis_producer_connector_hc_timeout", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we handle timeouts while waiting for client gen_server call (connector).
t_connector_health_check_rate_limit_call_timeout(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            meck:unload(),
            emqx_common_test_helpers:with_mock(
                erlcloud_kinesis,
                list_streams,
                fun() ->
                    timer:sleep(1_500),
                    meck:passthrough([])
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{
                        <<"resource_opts">> => #{
                            <<"health_check_interval">> => <<"50ms">>
                        }
                    }),
                    ?block_until(#{?snk_kind := "kinesis_producer_connector_hc_client_call_timeout"})
                end
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_ | _], ?of_kind("kinesis_producer_connector_hc_client_call_timeout", Trace)
            ),
            ok
        end
    ),
    ok.

%% Checks that we throttle control plance APIs when doing action health checks.
%% For action HCs, AWS quota is 10 TPS.
t_action_health_check_rate_limit() ->
    [{?cluster, true}].
t_action_health_check_rate_limit(TCConfig) when is_list(TCConfig) ->
    %% Using long enough interval with few nodes, should not hit limit
    ct:pal("Testing with 1 node, no rate limit"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
            }),
            ct:sleep(2_000),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ok
        end
    ),
    snabbkaffe:stop(),

    ct:pal("Testing with 1 node"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"50ms">>}
            }),
            ?block_until(#{?snk_kind := "kinesis_action_hc_rate_limited"}),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ok
        end
    ),
    snabbkaffe:stop(),

    ct:pal("Testing with 3 nodes"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}, #{role => core}, #{role => replicant}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                %% With more nodes, we trigger quota limit earlier
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
            }),
            ?block_until(#{?snk_kind := "kinesis_action_hc_rate_limited"}),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ok
        end
    ),

    CreateNamedAction = fun(Name) ->
        {201, _} = create_action_api([{action_name, Name} | TCConfig], #{
            %% With more action, we trigger quota limit earlier
            <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
        })
    end,
    ct:pal("Testing with 3 actions"),
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = CreateNamedAction(<<"a">>),
            {201, _} = CreateNamedAction(<<"b">>),
            {201, _} = CreateNamedAction(<<"c">>),
            ?block_until(#{?snk_kind := "kinesis_action_hc_rate_limited"}),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we handle timeouts while waiting to consume limiter tokens (action).
t_action_health_check_rate_limit_timeout(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{
                    <<"health_check_timeout">> => <<"400ms">>,
                    <<"health_check_interval">> => <<"50ms">>
                }
            }),
            ?inject_crash(
                #{?snk_kind := "kinesis_action_hc_rate_limited"},
                fun(_) ->
                    timer:sleep(400),
                    false
                end
            ),
            ?block_until(#{?snk_kind := "kinesis_producer_action_hc_timeout"}),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_producer_connector_hc_timeout", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_producer_action_hc_timeout", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we handle timeouts while waiting for client gen_server call (action).
t_action_health_check_rate_limit_call_timeout(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 10}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            meck:unload(),
            emqx_common_test_helpers:with_mock(
                erlcloud_kinesis,
                describe_stream,
                fun(StreamName) ->
                    timer:sleep(1_500),
                    meck:passthrough([StreamName])
                end,
                fun() ->
                    {201, _} = create_connector_api(TCConfig, #{}),
                    {201, _} = create_action_api(TCConfig, #{
                        <<"resource_opts">> => #{
                            <<"health_check_interval">> => <<"100ms">>
                        }
                    }),
                    ?block_until(#{?snk_kind := "kinesis_producer_action_hc_client_call_timeout"})
                end
            ),
            ok
        end,
        fun(Trace) ->
            ?assertMatch(
                [_ | _], ?of_kind("kinesis_producer_action_hc_client_call_timeout", Trace)
            ),
            ok
        end
    ),
    ok.

%% Checks that we handle limiter config updates accordingly.
t_limiter_config_updates(TCConfig) when is_list(TCConfig) ->
    {201, _} = create_connector_api(TCConfig, #{}),
    {201, _} = create_action_api(TCConfig, #{}),
    {200, _} = update_connector_api(TCConfig, #{}),
    {200, _} = update_action_api(TCConfig, #{}),
    ok.

%% Checks that we handle `resource_opts.health_check_timeout = infinity`.
t_limiter_config_infinity_timeout(TCConfig) when is_list(TCConfig) ->
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_connector_api(TCConfig, #{
            <<"resource_opts">> => #{
                <<"health_check_timeout">> => <<"infinity">>
            }
        })
    ),
    ?assertMatch(
        {201, #{<<"status">> := <<"connected">>}},
        create_action_api(TCConfig, #{
            <<"resource_opts">> => #{
                <<"health_check_timeout">> => <<"infinity">>
            }
        })
    ),
    ok.

%% Checks that we handle throttled API responses during HC by repeating the last status
%% (connector).
t_connector_health_check_throttled(TCConfig) ->
    meck:unload(),
    ct:timetrap({seconds, 15}),
    emqx_common_test_helpers:with_mock(
        erlcloud_kinesis,
        list_streams,
        fun() ->
            {error, {<<"LimitExceededException">>, <<"Rate exceeded for account 123456789012.">>}}
        end,
        fun() ->
            {201, _} = create_connector_api(TCConfig, #{<<"max_retries">> => 1}),
            ?block_until(#{?snk_kind := "kinesis_producer_connector_hc_throttled"})
        end
    ),
    ok.

%% Checks that we handle throttled API responses during HC by repeating the last status
%% (action).
t_action_health_check_throttled(TCConfig) ->
    meck:unload(),
    ct:timetrap({seconds, 15}),
    emqx_common_test_helpers:with_mock(
        erlcloud_kinesis,
        describe_stream,
        fun(_StreamName) ->
            {error, {<<"LimitExceededException">>, <<"Rate exceeded for account 123456789012.">>}}
        end,
        fun() ->
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{}),
            ?block_until(#{?snk_kind := "kinesis_producer_action_hc_throttled"})
        end
    ),
    ok.

%% Checks that we repeat the last status if there is no core to perform the health check
%% (connector).
t_connector_health_check_no_core() ->
    [{cluster, true}].
t_connector_health_check_no_core(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}, #{role => replicant}],
            [C1, R1] = Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
            }),
            ?assertMatch(
                {200, #{<<"status">> := <<"connected">>}},
                get_connector_api(TCConfig)
            ),
            ok = emqx_cth_cluster:stop([C1]),
            ct:sleep(2_000),
            %% Should not have crashed due to lack of cores.
            ConnResId = emqx_bridge_v2_testlib:connector_resource_id(TCConfig),
            ?assertMatch(
                #{status := ?status_connected},
                ?ON(R1, emqx_resource_cache:read_status(ConnResId))
            ),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_producer_connector_hc_no_core", Trace)),
            ok
        end
    ),
    ok.

%% Checks that we repeat the last status if there is no core to perform the health check
%% (action).
t_action_health_check_no_core() ->
    [{cluster, true}].
t_action_health_check_no_core(TCConfig) when is_list(TCConfig) ->
    Type = get_config(action_type, TCConfig),
    Name = get_config(action_name, TCConfig),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}, #{role => replicant}],
            [C1, R1] = Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig, #{}),
            {201, _} = create_action_api(TCConfig, #{
                <<"resource_opts">> => #{<<"health_check_interval">> => <<"200ms">>}
            }),
            {ok, {_ConnResId, ActionResId}} =
                ?ON(R1, emqx_bridge_v2:get_resource_ids(?global_ns, actions, Type, Name)),
            ?assertMatch(
                {ok, #rt{channel_status = ?status_connected}},
                ?ON(R1, emqx_resource_cache:get_runtime(ActionResId))
            ),
            ok = emqx_cth_cluster:stop([C1]),
            ct:sleep(2_000),
            %% Should not have crashed due to lack of cores.
            ?assertMatch(
                {ok, #rt{channel_status = ?status_connected}},
                ?ON(R1, emqx_resource_cache:get_runtime(ActionResId))
            ),
            emqx_cth_cluster:stop(Nodes),
            ok
        end,
        fun(Trace) ->
            ?assertMatch([], ?of_kind("kinesis_connector_hc_rate_limited", Trace)),
            ?assertMatch([], ?of_kind("kinesis_action_hc_rate_limited", Trace)),
            ?assertMatch([_ | _], ?of_kind("kinesis_producer_action_hc_no_core", Trace)),
            ok
        end
    ),
    ok.
