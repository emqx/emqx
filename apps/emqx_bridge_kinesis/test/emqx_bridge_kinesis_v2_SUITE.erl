%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_kinesis_v2_SUITE).

-feature(maybe_expr, enable).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").
-include_lib("emqx_resource/include/emqx_resource_runtime.hrl").
-include_lib("emqx/include/emqx_config.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(CONNECTOR_TYPE_BIN, <<"kinesis">>).
-define(ACTION_TYPE_BIN, <<"kinesis">>).

-define(ACCESS_KEY_ID, <<"aws_access_key_id">>).
-define(SECRET_ACCESS_KEY, <<"aws_secret_access_key">>).

-define(STREAM_NAME, <<"stream0">>).

-define(PROXY_NAME, "kinesis").
-define(PROXY_HOST, "toxiproxy").
-define(PROXY_PORT, 8474).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    [
        {group, local},
        {group, cluster}
    ].

matrix_cases() ->
    lists:filter(
        fun
            ({testcase, TestCase, _Opts}) ->
                get_tc_prop(TestCase, matrix, false);
            (TestCase) ->
                get_tc_prop(TestCase, matrix, false)
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

groups() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    ClusterCases = cluster_cases(),
    All = All0 -- (matrix_cases() ++ ClusterCases),
    MatrixGroups = emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, MatrixGroups),
    [
        {cluster, ClusterCases},
        {local, Groups ++ All}
        | MatrixGroups
    ].

cluster_cases() ->
    lists:filter(
        fun(TestCase) ->
            maybe
                true ?= erlang:function_exported(?MODULE, TestCase, 0),
                {cluster, true} ?= proplists:lookup(cluster, ?MODULE:TestCase()),
                true
            else
                _ -> false
            end
        end,
        emqx_common_test_helpers:all(?MODULE)
    ).

get_tc_prop(TestCase, Key, Default) ->
    maybe
        true ?= erlang:function_exported(?MODULE, TestCase, 0),
        {Key, Val} ?= proplists:lookup(Key, ?MODULE:TestCase()),
        Val
    else
        _ -> Default
    end.

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_group(local, TCConfig) ->
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
        #{work_dir => emqx_cth_suite:work_dir(TCConfig)}
    ),
    [
        {apps, Apps}
        | TCConfig
    ];
init_per_group(_Group, TCConfig) ->
    TCConfig.

end_per_group(local, TCConfig) ->
    Apps = ?config(apps, TCConfig),
    emqx_cth_suite:stop(Apps),
    reset_proxy(),
    ok;
end_per_group(_Group, _TCConfig) ->
    ok.

init_per_testcase(TestCase, TCConfig0) ->
    reset_proxy(),
    Path = group_path(TCConfig0, no_groups),
    ct:print(asciiart:visible($%, "~p - ~s", [Path, TestCase])),
    TCConfig =
        case erlang:function_exported(?MODULE, TestCase, 2) of
            true ->
                ?MODULE:TestCase(init, TCConfig0);
            false ->
                TCConfig0
        end,
    ConnectorName = atom_to_binary(TestCase),
    ConnectorConfig = connector_config(),
    ActionName = ConnectorName,
    ActionConfig = action_config(#{<<"connector">> => ConnectorName}),
    create_stream(?STREAM_NAME),
    snabbkaffe:start_trace(),
    [
        {bridge_kind, action},
        {connector_type, ?CONNECTOR_TYPE_BIN},
        {connector_name, ConnectorName},
        {connector_config, ConnectorConfig},
        {action_type, ?ACTION_TYPE_BIN},
        {action_name, ActionName},
        {action_config, ActionConfig}
        | TCConfig
    ].

end_per_testcase(TestCase, TCConfig) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true ->
            ?MODULE:TestCase('end', TCConfig);
        false ->
            ok
    end,
    snabbkaffe:stop(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

reset_proxy() ->
    emqx_common_test_helpers:reset_proxy(?PROXY_HOST, ?PROXY_PORT).

group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] -> Default;
        Path -> Path
    end.

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"aws_access_key_id">> => ?ACCESS_KEY_ID,
        <<"aws_secret_access_key">> => ?SECRET_ACCESS_KEY,
        <<"endpoint">> => <<"http://toxiproxy:4566">>,
        <<"max_retries">> => 3,
        <<"pool_size">> => 2,
        <<"resource_opts">> =>
            emqx_bridge_v2_testlib:common_connector_resource_opts()
    },
    InnerConfigMap = emqx_utils_maps:deep_merge(Defaults, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(?CONNECTOR_TYPE_BIN, <<"x">>, InnerConfigMap).

action_config(Overrides) ->
    Defaults = #{
        <<"connector">> => <<"please override">>,
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

create_connector_api(Config) ->
    create_connector_api(Config, _Overrides = #{}).
create_connector_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api(Config, Overrides)
    ).

get_connector_api(TCConfig) ->
    Type = emqx_bridge_v2_testlib:get_value(connector_type, TCConfig),
    Name = emqx_bridge_v2_testlib:get_value(connector_name, TCConfig),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:get_connector_api(Type, Name)
    ).

update_connector_api(Config) ->
    update_connector_api(Config, _Overrides = #{}).
update_connector_api(Config, Overrides) ->
    Type = emqx_bridge_v2_testlib:get_value(connector_type, Config),
    Name = emqx_bridge_v2_testlib:get_value(connector_name, Config),
    Cfg0 = emqx_bridge_v2_testlib:get_value(connector_config, Config),
    Cfg = emqx_utils_maps:deep_merge(Cfg0, Overrides),
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_connector_api(Name, Type, Cfg)
    ).

create_action_api(Config) ->
    create_action_api(Config, _Overrides = #{}).
create_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_action_api(Config, Overrides)
    ).

update_action_api(Config, Overrides) ->
    emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:update_bridge_api(Config, Overrides)
    ).

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

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% Checks that we throttle control plance APIs when doing connector health checks.
%% For connector HCs, AWS quota is 5 TPS.
t_connector_health_check_rate_limit() ->
    [{cluster, true}].
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
    [{cluster, true}].
t_action_health_check_rate_limit(TCConfig) when is_list(TCConfig) ->
    %% Using long enough interval with few nodes, should not hit limit
    ct:pal("Testing with 1 node, no rate limit"),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}],
            Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig),
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
            {201, _} = create_connector_api(TCConfig),
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
            {201, _} = create_connector_api(TCConfig),
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
            {201, _} = create_connector_api(TCConfig),
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
            {201, _} = create_connector_api(TCConfig),
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
                    {201, _} = create_connector_api(TCConfig),
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
    {201, _} = create_connector_api(TCConfig),
    {201, _} = create_action_api(TCConfig),
    {200, _} = update_connector_api(TCConfig),
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
            {201, _} = create_connector_api(TCConfig),
            {201, _} = create_action_api(TCConfig),
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
    Type = emqx_bridge_v2_testlib:get_value(action_type, TCConfig),
    Name = emqx_bridge_v2_testlib:get_value(action_name, TCConfig),
    ct:timetrap({seconds, 30}),
    ?check_trace(
        emqx_bridge_v2_testlib:snk_timetrap(),
        begin
            Specs = [#{role => core}, #{role => replicant}],
            [C1, R1] = Nodes = start_cluster(?FUNCTION_NAME, Specs, TCConfig),
            {201, _} = create_connector_api(TCConfig),
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
