%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_otel_SUITE).

-export([all/0, groups/0]).
-export([
    init_per_suite/1,
    end_per_suite/1,
    init_per_group/2,
    end_per_group/2,
    init_per_testcase/2,
    end_per_testcase/2
]).

-export([
    t_log/1
]).

-export([
    t_trace/1,
    t_trace_disabled/1,
    t_trace_all/1,
    t_distributed_trace/1
]).

-export([
    t_e2e_connect_disconnect/1,
    t_e2e_abnormal_disconnect/1,
    t_e2e_cilent_sub_unsub/1,
    t_e2e_cilent_publish_qos0/1,
    t_e2e_cilent_publish_qos1/1,
    t_e2e_cilent_publish_qos2/1,
    t_e2e_cilent_publish_qos2_with_forward/1,
    t_e2e_cilent_borker_publish_whitelist/1,
    t_e2e_client_pub_qos2_trace_level_0/1,
    t_e2e_client_pub_qos2_trace_level_1/1,
    t_e2e_client_source_republish_to_clients/1,
    t_e2e_client_source_republish_to_clients/2
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/emqx_external_trace.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTEL_SERVICE_NAME, "emqx").
-define(CONF_PATH, [opentelemetry]).

-define(otel_trace_core1, otel_trace_core1).
-define(otel_trace_core2, otel_trace_core2).
-define(otel_trace_repl, otel_trace_repl).

-define(CONN_TYPE_GROUP(T),
    ((T =:= tcp) orelse
        (T =:= ssl) orelse
        (T =:= ws) orelse
        (T =:= wss))
).

-define(WITH_CLUSTER(TC),
    ((TC =:= t_distributed_trace) orelse
        (TC =:= t_e2e_cilent_publish_qos2_with_forward) orelse
        (TC =:= t_e2e_cilent_borker_publish_whitelist) orelse
        (TC =:= t_e2e_client_pub_qos2_trace_level_0) orelse
        (TC =:= t_e2e_client_pub_qos2_trace_level_1))
).

-define(WITH_RULE_ENGINE(TC),
    (TC =:= t_e2e_client_source_republish_to_clients)
).
%% How to run it locally:
%%
%% run ct in docker container
%% run script:
%% ```bash
%% ./scripts/ct/run.sh --app apps/emqx_opentelemetry -- \
%%                     ct -v --readable=true --name 'test@127.0.0.1' \
%%                     --suite apps/emqx_opentelemetry/test/emqx_otel_SUITE.erl
%% ```
%%
%% run with specical envs:
%%  1. Uncomment networks in .ci/docker-compose-file/docker-compose-otel.yaml,
%%     Uncomment OTLP gRPC ports mappings for otel-collector and otel-collector-tls services.
%%     Uncomment jaeger-all-in-one ports mapping.
%%  2. Start deps services:
%%     DOCKER_USER="$(id -u)" docker-compose -f .ci/docker-compose-file/docker-compose-otel.yaml up
%%  3. Run tests with special env variables:
%%         PROFILE=emqx JAEGER_URL="http://localhost:16686" \
%%         OTEL_COLLECTOR_URL="http://localhost:4317" OTEL_COLLECTOR_TLS_URL="https://localhost:14317" \
%%         make "apps/emqx_opentelemetry-ct"
%%     Or run only this suite:
%%         PROFILE=emqx JAEGER_URL="http://localhost:16686" \
%%         OTEL_COLLECTOR_URL="http://localhost:4317" OTEL_COLLECTOR_TLS_URL="https://localhost:14317" \
%%         ./rebar3 ct -v --readable=true --name 'test@127.0.0.1' \
%%                     --suite apps/emqx_opentelemetry/test/emqx_otel_SUITE.erl

all() ->
    [
        {group, otel_tcp},
        {group, otel_tls}
    ].

groups() ->
    LogsCases = [
        t_log
    ],

    %% TODO: Add metrics test cases
    MetricsGroups = [],

    TraceConnTypeGroups = [
        {group, tcp},
        {group, ssl},
        {group, ws},
        {group, wss}
    ],
    TraceGroups = [
        {group, trace_legacy_mode},
        {group, trace_e2e_mode}
    ],
    E2ETraceGroups = [
        {group, e2e_with_traceparent},
        {group, e2e_no_traceparent}
    ],
    LegacyModeTraceCases = [
        t_trace,
        t_trace_disabled,
        t_trace_all,
        t_distributed_trace
    ],
    E2EModeTraceCases = [
        t_e2e_connect_disconnect,
        t_e2e_abnormal_disconnect,
        t_e2e_cilent_sub_unsub,
        t_e2e_cilent_publish_qos0,
        t_e2e_cilent_publish_qos1,
        t_e2e_cilent_publish_qos2,
        t_e2e_cilent_publish_qos2_with_forward,
        t_e2e_cilent_borker_publish_whitelist,
        t_e2e_client_pub_qos2_trace_level_0,
        t_e2e_client_pub_qos2_trace_level_1,
        t_e2e_client_source_republish_to_clients
    ],
    FeatureGroups = [
        {group, logs},
        {group, traces},
        {group, metrics}
    ],
    [
        {otel_tcp, FeatureGroups},
        {otel_tls, FeatureGroups},

        %% FeatureGroups
        {logs, LogsCases},
        {traces, TraceConnTypeGroups},
        {metrics, MetricsGroups},

        %% TraceConnTypeGroups
        {tcp, TraceGroups},
        {ssl, TraceGroups},
        {ws, TraceGroups},
        {wss, TraceGroups},

        %% TraceGroups
        {trace_legacy_mode, LegacyModeTraceCases},
        {trace_e2e_mode, E2ETraceGroups},

        %% E2ETraceGroups
        {e2e_with_traceparent, E2EModeTraceCases},
        {e2e_no_traceparent, E2EModeTraceCases}
    ].

init_per_suite(Config) ->
    %% This is called by emqx_machine in EMQX release
    emqx_otel_app:configure_otel_deps(),
    %% No release name during the test case, we need a reliable service name to query Jaeger
    os:putenv("OTEL_SERVICE_NAME", ?OTEL_SERVICE_NAME),
    JaegerURL = os:getenv("JAEGER_URL", "http://jaeger.emqx.net:16686"),
    [{jaeger_url, JaegerURL} | Config].

end_per_suite(_) ->
    os:unsetenv("OTEL_SERVICE_NAME"),
    ok.

init_per_group(otel_tcp = Group, Config) ->
    OtelCollectorURL = os:getenv("OTEL_COLLECTOR_URL", "http://otel-collector.emqx.net:4317"),
    [
        {group_otel_conn_type, Group},
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ];
init_per_group(otel_tls = Group, Config) ->
    OtelCollectorURL = os:getenv(
        "OTEL_COLLECTOR_TLS_URL", "https://otel-collector-tls.emqx.net:4317"
    ),
    [
        {group_otel_conn_type, Group},
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ];
init_per_group(Group, Config) when ?CONN_TYPE_GROUP(Group) ->
    [
        {group_client_conn_type, Group}
        | Config
    ];
init_per_group(trace_legacy_mode = Group, Config) ->
    [
        {otel_trace_mode, legacy},
        {group_otel_trace_mode, Group}
        | Config
    ];
init_per_group(trace_e2e_mode = Group, Config) ->
    [
        {otel_trace_mode, e2e},
        {group_otel_trace_mode, Group}
        | Config
    ];
init_per_group(e2e_with_traceparent = Group, Config) ->
    [
        {otel_follow_traceparent, true},
        {group_follow_traceparent, Group}
        | Config
    ];
init_per_group(e2e_no_traceparent = Group, Config) ->
    [
        {otel_follow_traceparent, false},
        {group_follow_traceparent, Group}
        | Config
    ];
init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TC, Config) when ?WITH_CLUSTER(TC) ->
    Cluster = cluster(TC, Config),
    local_init_per_test_case(TC, [{tc, TC}, {cluster, Cluster} | Config]);
init_per_testcase(TC, Config) when ?WITH_RULE_ENGINE(TC) ->
    Apps = emqx_cth_suite:start(
        apps_spec_with_rule_engine(),
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    local_init_per_test_case(TC, [{tc, TC}, {suite_apps, Apps} | Config]);
init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(
        apps_spec(),
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    local_init_per_test_case(TC, [{tc, TC}, {suite_apps, Apps} | Config]).

end_per_testcase(TC, Config) when ?WITH_CLUSTER(TC) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    emqx_config:delete_override_conf_files(),
    local_end_per_test_case(TC, Config);
end_per_testcase(TC, Config) when ?WITH_RULE_ENGINE(TC) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    local_end_per_test_case(TC, Config);
end_per_testcase(TC, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    local_end_per_test_case(TC, Config).

local_init_per_test_case(TC, Config) ->
    emqx_common_test_helpers:init_per_testcase(?MODULE, TC, Config).

local_end_per_test_case(TC, Config) ->
    emqx_common_test_helpers:end_per_testcase(?MODULE, TC, Config).

logs_exporter_file_path(Group, Config) ->
    filename:join([project_dir(Config), logs_exporter_filename(Group)]).

project_dir(Config) ->
    filename:join(
        lists:takewhile(
            fun(PathPart) -> PathPart =/= "_build" end,
            filename:split(?config(priv_dir, Config))
        )
    ).

logs_exporter_filename(otel_tcp) ->
    ".ci/docker-compose-file/otel/otel-collector.json";
logs_exporter_filename(otel_tls) ->
    ".ci/docker-compose-file/otel/otel-collector-tls.json".

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% ====================
%% Logs cases

t_log(Config) ->
    Level = emqx_logger:get_primary_log_level(),
    LogsConf = #{
        <<"logs">> => #{
            <<"enable">> => true,
            <<"level">> => atom_to_binary(Level),
            <<"scheduled_delay">> => <<"20ms">>
        },
        <<"exporter">> => exporter_conf(Config)
    },
    {ok, _} = emqx_conf:update(?CONF_PATH, LogsConf, #{override_to => cluster}),

    %% Ids are only needed for matching logs in the file exported by otel-collector
    Id = integer_to_binary(otel_id_generator:generate_trace_id()),
    ?SLOG(Level, #{msg => "otel_test_log_message", id => Id}),
    Id1 = integer_to_binary(otel_id_generator:generate_trace_id()),
    logger:Level("Ordinary log message, id: ~p", [Id1]),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, Logs} = file:read_file(?config(logs_exporter_file_path, Config)),
                binary:match(Logs, Id) =/= nomatch andalso binary:match(Logs, Id1) =/= nomatch
            end,
            10_000
        )
    ).

%% ====================
%% Legacy mode cases

t_trace(Config) ->
    {ok, _} = emqx_conf:update(?CONF_PATH, enabled_trace_conf(Config), #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    TopicNoSubs = <<"t/trace/test/nosub/", (atom_to_binary(?FUNCTION_NAME))/binary>>,

    SubConn1 = connect(Config, <<"sub1">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn1, Topic),
    SubConn2 = connect(Config, <<"sub2">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn2, Topic),
    PubConn = connect(Config, <<"pub">>),

    TraceParent = traceparent(true),
    TraceParentNotSampled = traceparent(false),
    ok = emqtt:publish(PubConn, Topic, props(TraceParent), <<"must be traced">>, []),
    ok = emqtt:publish(PubConn, Topic, props(TraceParentNotSampled), <<"must not be traced">>, []),

    TraceParentNoSub = traceparent(true),
    TraceParentNoSubNotSampled = traceparent(false),
    ok = emqtt:publish(PubConn, TopicNoSubs, props(TraceParentNoSub), <<"must be traced">>, []),
    ok = emqtt:publish(
        PubConn, TopicNoSubs, props(TraceParentNoSubNotSampled), <<"must not be traced">>, []
    ),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := Traces}} = get_jaeger_traces(?config(jaeger_url, Config)),
                [Trace] = filter_traces(trace_id(TraceParent), Traces),
                [] = filter_traces(trace_id(TraceParentNotSampled), Traces),
                [TraceNoSub] = filter_traces(trace_id(TraceParentNoSub), Traces),
                [] = filter_traces(trace_id(TraceParentNoSubNotSampled), Traces),

                #{<<"spans">> := Spans, <<"processes">> := _} = Trace,
                %% 2 sub spans and 1 publish process span
                IsExpectedSpansLen = length(Spans) =:= 3,

                #{<<"spans">> := SpansNoSub, <<"processes">> := _} = TraceNoSub,
                %% Only 1 publish process span
                IsExpectedSpansLen andalso 1 =:= length(SpansNoSub)
            end,
            10_000
        )
    ),
    stop_conns([SubConn1, SubConn2, PubConn]).

t_trace_disabled(Config) ->
    ?assertNot(emqx:get_config(?CONF_PATH ++ [traces, enable])),
    %% Tracer must be actually disabled
    ?assertEqual({otel_tracer_noop, []}, opentelemetry:get_tracer()),
    ?assertEqual(undefined, emqx_external_trace:provider()),

    Topic = <<"t/trace/test", (atom_to_binary(?FUNCTION_NAME))/binary>>,

    SubConn = connect(Config, <<"sub">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn, Topic),
    PubConn = connect(Config, <<"pub">>),

    TraceParent = traceparent(true),
    emqtt:publish(PubConn, Topic, props(TraceParent), <<>>, []),
    receive
        {publish, #{topic := Topic, properties := Props}} ->
            %% traceparent must be propagated by EMQX even if internal otel trace is disabled
            #{'User-Property' := [{<<"traceparent">>, TrParent}]} = Props,
            ?assertEqual(TraceParent, TrParent)
    after 10_000 ->
        ct:fail("published_message_not_received")
    end,

    %%  if otel trace is registered but is actually not running, EMQX must work fine
    %% and the message must be delivered to the subscriber
    ok = emqx_otel_trace:toggle_registered(true),
    TraceParent1 = traceparent(true),
    emqtt:publish(PubConn, Topic, props(TraceParent1), <<>>, []),
    receive
        {publish, #{topic := Topic, properties := Props1}} ->
            #{'User-Property' := [{<<"traceparent">>, TrParent1}]} = Props1,
            ?assertEqual(TraceParent1, TrParent1)
    after 10_000 ->
        ct:fail("published_message_not_received")
    end,
    stop_conns([SubConn, PubConn]).

t_trace_all(Config) ->
    OtelConf = enabled_trace_conf(Config),
    OtelConf1 = emqx_utils_maps:deep_put([<<"traces">>, <<"filter">>], OtelConf, #{
        <<"trace_all">> => true
    }),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf1, #{override_to => cluster}),

    Topic = <<"t/trace/test", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    ClientId = <<"pub-", (integer_to_binary(erlang:system_time(nanosecond)))/binary>>,
    PubConn = connect(Config, ClientId),
    emqtt:publish(PubConn, Topic, #{}, <<>>, []),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := Traces}} = get_jaeger_traces(?config(jaeger_url, Config)),
                Res = lists:filter(
                    fun(#{<<"spans">> := Spans}) ->
                        case Spans of
                            %% Only one span is expected as there are no subscribers
                            [#{<<"tags">> := Tags}] ->
                                lists:any(
                                    fun(#{<<"key">> := K, <<"value">> := Val}) ->
                                        K =:= <<"messaging.client_id">> andalso Val =:= ClientId
                                    end,
                                    Tags
                                );
                            _ ->
                                false
                        end
                    end,
                    Traces
                ),
                %% Expecting exactly 1 span
                length(Res) =:= 1
            end,
            10_000
        )
    ),
    stop_conns([PubConn]).

t_distributed_trace(Config) ->
    [Core1, Core2, Repl] = Cluster = ?config(cluster, Config),
    {ok, _} = rpc:call(
        Core1,
        emqx_conf,
        update,
        [?CONF_PATH, enabled_trace_conf(Config), #{override_to => cluster}]
    ),
    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,

    SubConn1 = connect(Config, Core1, <<"sub1">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn1, Topic),
    SubConn2 = connect(Config, Core2, <<"sub2">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn2, Topic),
    SubConn3 = connect(Config, Repl, <<"sub3">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn3, Topic),

    PubConn = connect(Config, Repl, <<"pub">>),

    TraceParent = traceparent(true),
    TraceParentNotSampled = traceparent(false),

    ok = emqtt:publish(PubConn, Topic, props(TraceParent), <<"must be traced">>, []),
    ok = emqtt:publish(PubConn, Topic, props(TraceParentNotSampled), <<"must not be traced">>, []),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := Traces}} = get_jaeger_traces(?config(jaeger_url, Config)),
                [Trace] = filter_traces(trace_id(TraceParent), Traces),

                [] = filter_traces(trace_id(TraceParentNotSampled), Traces),

                #{<<"spans">> := Spans, <<"processes">> := Procs} = Trace,

                %% 3 sub spans and 1 publish process span
                4 = length(Spans),
                [_, _, _] = SendSpans = filter_spans(<<"send_published_message">>, Spans),

                IsAllNodesSpans =
                    lists:sort([atom_to_binary(N) || N <- Cluster]) =:=
                        lists:sort([span_node(S, Procs) || S <- SendSpans]),

                [PubSpan] = filter_spans(<<"process_message">>, Spans),
                atom_to_binary(Repl) =:= span_node(PubSpan, Procs) andalso IsAllNodesSpans
            end,
            10_000
        )
    ),
    stop_conns([SubConn1, SubConn2, SubConn3, PubConn]).

%% ====================
%% E2E mode cases

t_e2e_connect_disconnect(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    ClientId = e2e_client_id(Config),

    WithTraceparent = ?config(otel_follow_traceparent, Config),
    ConnectTraceParent = traceparent(true),
    DisconnectTraceParent = traceparent(true),

    Conn = connect(Config, node(), ClientId, props(WithTraceparent, ConnectTraceParent)),
    timer:sleep(500),
    _ = emqtt:disconnect(Conn, ?RC_SUCCESS, props(WithTraceparent, DisconnectTraceParent)),
    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ConnectTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.connect",
                    #{
                        <<"client.clientid">> => ClientId,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),

                %% only one traces for client current `ClientId`
                1 = length(ConnectTraces),
                [#{<<"spans">> := ConnectSpans}] = ConnectTraces,

                [ClientConnect_Span] = filter_spans(<<"client.connect">>, ConnectSpans),
                [ClientAuthN_Span] = filter_spans(<<"client.authn">>, ConnectSpans),
                %% TODO: client.authn_backend

                %% `client.connect` span
                #{
                    <<"spanID">> := ClientConnect_SpanID,
                    <<"traceID">> := ClientConnect_TraceID,
                    <<"references">> := Refs1
                } = ClientConnect_Span,

                true = refs_length_with_traceparent(WithTraceparent) =:= length(Refs1),
                true = trace_id_assert(
                    WithTraceparent, ClientConnect_TraceID, trace_id(ConnectTraceParent)
                ),

                %% `client.authn` span
                #{
                    <<"spanID">> := _ClientAuthN_SpanID,
                    <<"traceID">> := _ClientAuthN_TraceID,
                    <<"references">> := [
                        #{
                            <<"refType">> := <<"CHILD_OF">>,
                            <<"traceID">> := ClientConnect_TraceID,
                            <<"spanID">> := ClientConnect_SpanID
                        }
                    ]
                } = ClientAuthN_Span,

                {ok, #{<<"data">> := DisconnectTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.disconnect",
                    #{
                        <<"client.clientid">> => ClientId,
                        <<"client.disconnect.reason">> => <<"success">>,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),

                [#{<<"spans">> := DisconnectSpans}] = DisconnectTraces,

                [ClientDisconnect_Span] = filter_spans(<<"client.disconnect">>, DisconnectSpans),

                %% `client.disconnect` span
                #{
                    <<"traceID">> := ClientDisconnect_TraceID,
                    <<"references">> := Refs2
                } = ClientDisconnect_Span,

                true = refs_length_with_traceparent(WithTraceparent) =:= length(Refs2),
                true = trace_id_assert(
                    WithTraceparent, ClientDisconnect_TraceID, trace_id(DisconnectTraceParent)
                ),

                true
            end,
            10_000
        )
    ),
    ok.

t_e2e_abnormal_disconnect(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    ClientId = e2e_client_id(Config),
    Conn = connect(Config, ClientId),
    timer:sleep(500),
    _ = stop_conn(Conn),
    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := DisconnectTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "broker.disconnect",
                    #{
                        <<"client.clientid">> => ClientId,
                        <<"client.disconnect.reason">> => <<"sock_closed">>,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                %% one normal disconnected
                ct:pal("DisconnectTraces: ~p~n", [DisconnectTraces]),
                1 = length(DisconnectTraces),
                true
            end,
            10_000
        )
    ),
    ok.

t_e2e_cilent_sub_unsub(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_2,

    ClientId = e2e_client_id(Config),

    WithTraceparent = ?config(otel_follow_traceparent, Config),
    SubTraceParent = traceparent(true),
    UnsubTraceParent = traceparent(true),

    Conn = connect(Config, ClientId),
    timer:sleep(500),
    {ok, _, [QoS]} = emqtt:subscribe(Conn, props(WithTraceparent, SubTraceParent), Topic, QoS),
    timer:sleep(500),
    {ok, _, _} = emqtt:unsubscribe(Conn, props(WithTraceparent, UnsubTraceParent), Topic),
    _ = disconnect_conn(Conn),
    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := SubscribeTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.subscribe",
                    #{
                        <<"client.clientid">> => ClientId,
                        <<"client.subscribe.topics">> => emqx_utils_conv:bin(
                            [Topic]
                        ),
                        <<"client.subscribe.sub_opts">> => emqx_utils_conv:bin(
                            [#{rh => 0, rap => 0, qos => QoS, nl => 0}]
                        ),
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),

                %% only one traces for client current `ClientId`
                1 = length(SubscribeTraces),
                [#{<<"spans">> := SubscribeSpans}] = SubscribeTraces,

                [ClientSubscribe_Span] = filter_spans(<<"client.subscribe">>, SubscribeSpans),
                [ClientAuthZ_Span] = filter_spans(<<"client.authz">>, SubscribeSpans),
                %% TODO: client.authz_backend

                %% `client.subscribe` root span
                #{
                    <<"spanID">> := ClientSubscribe_SpanID,
                    <<"traceID">> := ClientSubscribe_TraceID,
                    <<"references">> := Refs1
                } = ClientSubscribe_Span,

                true = refs_length_with_traceparent(WithTraceparent) =:= length(Refs1),
                true = trace_id_assert(
                    WithTraceparent, ClientSubscribe_TraceID, trace_id(SubTraceParent)
                ),

                #{
                    <<"tags">> := ClientAuthZ_Tags,
                    <<"spanID">> := _ClientAuthZ_SpanID,
                    <<"traceID">> := _ClientAuthZ_TraceID,
                    <<"references">> := [
                        #{
                            <<"refType">> := <<"CHILD_OF">>,
                            <<"traceID">> := ClientSubscribe_TraceID,
                            <<"spanID">> := ClientSubscribe_SpanID
                        }
                    ]
                } = ClientAuthZ_Span,

                [#{<<"value">> := <<"subscribe">>}] = filter_tags(
                    <<"authz.action_type">>, ClientAuthZ_Tags
                ),

                %% `client.subscribe` root span
                {ok, #{<<"data">> := UnsubscribeTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.unsubscribe",
                    #{
                        <<"client.clientid">> => ClientId,
                        <<"client.unsubscribe.topics">> => emqx_utils_conv:bin(
                            [Topic]
                        ),
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),

                [#{<<"spans">> := UnsubscribeSpans}] = UnsubscribeTraces,

                [ClientUnsubscribe_Span] = filter_spans(<<"client.unsubscribe">>, UnsubscribeSpans),

                %% `client.unsubscribe` span
                #{
                    <<"traceID">> := ClientUnsubscribe_TraceID,
                    <<"references">> := Refs2
                } = ClientUnsubscribe_Span,

                true = refs_length_with_traceparent(WithTraceparent) =:= length(Refs2),
                true = trace_id_assert(
                    WithTraceparent, ClientUnsubscribe_TraceID, trace_id(UnsubTraceParent)
                ),

                true
            end,
            10_000
        )
    ),
    ok.

-define(MATCH_ROOT_SPAN(SpanID, TraceID), #{
    <<"spanID">> := SpanID, <<"traceID">> := TraceID, <<"references">> := []
}).

-define(MATCH_ROOT_SPAN(SpanID, Tags, TraceID), #{
    <<"tags">> := Tags,
    <<"spanID">> := SpanID,
    <<"traceID">> := TraceID,
    <<"references">> := []
}).

-define(MATCH_SUB_SPAN(SpanID, ParentSpanID, TraceID), #{
    <<"spanID">> := SpanID,
    <<"traceID">> := TraceID,
    <<"references">> := [
        #{
            <<"refType">> := <<"CHILD_OF">>,
            <<"traceID">> := TraceID,
            <<"spanID">> := ParentSpanID
        }
    ]
}).

-define(MATCH_SUB_SPAN(SpanID, ParentSpanID, Tags, TraceID), #{
    <<"tags">> := Tags,
    <<"spanID">> := SpanID,
    <<"traceID">> := TraceID,
    <<"references">> := [
        #{
            <<"refType">> := <<"CHILD_OF">>,
            <<"traceID">> := TraceID,
            <<"spanID">> := ParentSpanID
        }
    ]
}).

-define(F(TagKeyName, TagSeq), fun(OperationName, Spans) ->
    sort_spans_by_key_sequence(TagKeyName, TagSeq, OperationName, Spans)
end).

t_e2e_cilent_publish_qos0(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_0,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(Config, ClientId1),
    Conn2 = connect(Config, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    ok = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    timer:sleep(200),
    _ = disconnect_conns([Conn1, Conn2]),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2]),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                5 = length(Spans),
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  └─ `message.route`
                %%         │
                %% 4.      ├─ `broker.publish` (ClientId1)
                %%         │
                %% 5.      └─ `broker.publish` (ClientId2)

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(_SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(_SpanID5, SpanID3, _)
                ] = F(<<"broker.publish">>, Spans),
                true
            end,
            10_000
        )
    ),
    ok.

t_e2e_cilent_publish_qos1(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_1,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(Config, ClientId1),
    Conn2 = connect(Config, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2]),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                8 = length(Spans),
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  ├─ `message.route`
                %%     │   │
                %% 4.  │   ├─ `broker.publish` (ClientId1)
                %% 5.  │   │   └─ `client.puback`
                %%     │   │
                %% 6.  │   └─ `broker.publish` (ClientId2)
                %% 7.  │       └─ `client.puback`
                %%     │
                %% 8.  └─ `broker.puback` (ClientId1)

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID6, SpanID3, _)
                ] = F(<<"broker.publish">>, Spans),
                [
                    ?MATCH_SUB_SPAN(_SpanID5, SpanID4, _),
                    ?MATCH_SUB_SPAN(_SpanID7, SpanID6, _)
                ] = F(<<"client.puback">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID8, SpanID1, _)] = F(<<"broker.puback">>, Spans),
                true
            end,
            10_000
        )
    ),
    _ = disconnect_conns([Conn1, Conn2]),
    ok.

t_e2e_cilent_publish_qos2(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_2,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(Config, ClientId1),
    Conn2 = connect(Config, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2]),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                14 = length(Spans),
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  ├─ `message.route`
                %%     │   │
                %% 4.  │   ├─ `broker.publish` (ClientId1)
                %% 5.  │   │   └─ `client.pubrec`
                %% 6.  │   │       └─ `broker.pubrel`
                %% 7.  │   │           └─ `client.pubcomp`
                %%     │   │
                %% 8.  │   └─ `broker.publish` (ClientId2)
                %% 9.  │       └─ `client.pubrec`
                %% 10. │           └─ `broker.pubrel`
                %% 11. │               └─ `client.pubcomp`
                %%     │
                %% 12. └─ `broker.pubrec` (ClientId1)
                %% 13.     └─ `client.pubrel`
                %% 14.         └─ `broker.pubcomp`

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID8, SpanID3, _)
                ] = F(<<"broker.publish">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID5, SpanID4, _),
                    ?MATCH_SUB_SPAN(SpanID9, SpanID8, _)
                ] = F(<<"client.pubrec">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID6, SpanID5, _),
                    ?MATCH_SUB_SPAN(SpanID10, SpanID9, _)
                ] = F(<<"broker.pubrel">>, Spans),
                [
                    ?MATCH_SUB_SPAN(_SpanID7, SpanID6, _),
                    ?MATCH_SUB_SPAN(_SpanID11, SpanID10, _)
                ] = F(<<"client.pubcomp">>, Spans),
                [?MATCH_SUB_SPAN(SpanID12, SpanID1, _)] = F(<<"broker.pubrec">>, Spans),
                [?MATCH_SUB_SPAN(SpanID13, SpanID12, _)] = F(<<"client.pubrel">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID14, SpanID13, _)] = F(<<"broker.pubcomp">>, Spans),
                true
            end,
            10_000
        )
    ),
    _ = disconnect_conns([Conn1, Conn2]),
    ok.

t_e2e_cilent_publish_qos2_with_forward(Config) ->
    [Core1, Core2, Repl] = _Cluster = ?config(cluster, Config),

    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = rpc:call(
        Core1,
        emqx_conf,
        update,
        [?CONF_PATH, OtelConf, #{override_to => cluster}]
    ),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_2,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    ClientId3 = <<BaseClientId/binary, "-3">>,

    Conn1 = connect(Config, Core1, ClientId1),
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    Conn2 = connect(Config, Core2, ClientId2),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),
    Conn3 = connect(Config, Repl, ClientId3),
    {ok, _, [QoS]} = emqtt:subscribe(Conn3, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F1 = ?F(<<"client.clientid">>, [ClientId1, ClientId2, ClientId3]),
    F2 = ?F(
        <<"forward.to">>,
        [
            emqx_utils_conv:bin(node_name(?otel_trace_core1)),
            emqx_utils_conv:bin(node_name(?otel_trace_core2)),
            emqx_utils_conv:bin(node_name(?otel_trace_repl))
        ]
    ),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                22 = length(Spans),
                %% Note: Ignoring spans and sorting by time may cause order problems
                %% due to asynchronous requests. Manually sort by span name directly
                %%
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  ├─ `message.route`
                %%     │   │
                %% 4.  │   ├─ `broker.publish` (Core1, ClientId1, to local node)
                %% 5.  │   │   └─ `client.pubrec`
                %% 6.  │   │       └─ `broker.pubrel`
                %% 7.  │   │           └─ `client.pubcomp`
                %%     │   │
                %% 8.  │   ├─ `message.forward` (Core2, ClientId2)
                %% 9.  │   │   └─ `message.handle_forward`
                %% 10. │   │       └─ `broker.publish` (ClientId2)
                %% 11. │   │           └─ `client.pubrec`
                %% 12. │   │               └─ `broker.pubrel`
                %% 13. │   │                   └─ `client.pubcomp`
                %%     │   │
                %% 14. │   └─ `message.forward` (Repl, ClientId3)
                %% 15. │       └─ `message.handle_forward`
                %% 16. │           └─ `broker.publish` (ClientId3)
                %% 17. │               └─ `client.pubrec`
                %% 18. │                   └─ `broker.pubrel`
                %% 19. │                       └─ `client.pubcomp`
                %%     │
                %% 20. └─ `broker.pubrec` (ClientId1)
                %% 21.     └─ `client.pubrel`
                %% 22.         └─ `broker.pubcomp`

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F1(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F1(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F1(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID10, SpanID9, _),
                    ?MATCH_SUB_SPAN(SpanID16, SpanID15, _)
                ] =
                    F1(<<"broker.publish">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID8, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID14, SpanID3, _)
                ] = F2(<<"message.forward">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID9, SpanID8, _),
                    ?MATCH_SUB_SPAN(SpanID15, SpanID14, _)
                ] = F2(<<"message.handle_forward">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID5, SpanID4, _),
                    ?MATCH_SUB_SPAN(SpanID11, SpanID10, _),
                    ?MATCH_SUB_SPAN(SpanID17, SpanID16, _)
                ] = F1(<<"client.pubrec">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID6, SpanID5, _),
                    ?MATCH_SUB_SPAN(SpanID12, SpanID11, _),
                    ?MATCH_SUB_SPAN(SpanID18, SpanID17, _)
                ] = F1(<<"broker.pubrel">>, Spans),
                [
                    ?MATCH_SUB_SPAN(_SpanID7, SpanID6, _),
                    ?MATCH_SUB_SPAN(_SpanID13, SpanID12, _),
                    ?MATCH_SUB_SPAN(_SpanID19, SpanID18, _)
                ] = F1(<<"client.pubcomp">>, Spans),

                [?MATCH_SUB_SPAN(SpanID20, SpanID1, _)] = F1(<<"broker.pubrec">>, Spans),
                [?MATCH_SUB_SPAN(SpanID21, SpanID20, _)] = F1(<<"client.pubrel">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID22, SpanID21, _)] = F1(<<"broker.pubcomp">>, Spans),
                true
            end,
            10_000
        )
    ),

    _ = disconnect_conns([Conn1, Conn2, Conn3]),
    ok.

t_e2e_cilent_borker_publish_whitelist(Config) ->
    [Core1, Core2, Repl] = _Cluster = ?config(cluster, Config),

    OtelConf0 = enabled_e2e_trace_conf_all(Config),
    %% set sample ratio to zero to test span `broker.publish` by whitelist
    OtelConf =
        emqx_utils_maps:deep_put(
            [<<"traces">>, <<"filter">>, <<"e2e_tracing_options">>, <<"sample_ratio">>],
            OtelConf0,
            0.0
        ),
    ct:pal("OtelConf0: ~p~n", [OtelConf0]),
    ct:pal("OtelConf: ~p~n", [OtelConf]),
    {ok, _} = rpc:call(
        Core1,
        emqx_conf,
        update,
        [?CONF_PATH, OtelConf, #{override_to => cluster}]
    ),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    PubQoS = ?QOS_2,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    ClientId3 = <<BaseClientId/binary, "-3">>,

    ok = rpc:call(Core1, emqx_otel_sampler, store_rule, [clientid, ClientId2]),
    ok = rpc:call(Core1, emqx_otel_sampler, store_rule, [clientid, ClientId3]),

    Conn1 = connect(Config, Core1, ClientId1),
    {ok, _, [PubQoS]} = emqtt:subscribe(Conn1, Topic, PubQoS),
    Conn2 = connect(Config, Core2, ClientId2),
    {ok, _, [?QOS_1]} = emqtt:subscribe(Conn2, Topic, ?QOS_1),
    Conn3 = connect(Config, Repl, ClientId3),
    {ok, _, [?QOS_2]} = emqtt:subscribe(Conn3, Topic, ?QOS_2),

    timer:sleep(50),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, PubQoS),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2, ClientId3]),

    timer:sleep(200),
    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := BrokerPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "broker.publish",
                    #{<<"client.clientid">> => ClientId2}
                ),
                %% ct:pal("SubTraces: ~p~n", [BrokerPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = BrokerPublishTraces,
                6 = length(Spans),
                %% Note: Ignoring spans and sorting by time may cause order problems
                %% due to asynchronous requests. Manually sort by span name directly
                %% The following two `broker.publish` both have parent span, but not sampled here
                %%
                %% 1.  ├─`broker.publish` (ClientId2)
                %% 2.  │   └─ `client.puback`
                %%     │
                %% 3.  └─`broker.publish` (ClientId3)
                %% 4.     └─ `client.pubrec`
                %% 5.         └─ `broker.pubrel`
                %% 6.             └─ `client.pubcomp`

                [
                    ?MATCH_SUB_SPAN(SpanID1, _, TraceID),
                    ?MATCH_SUB_SPAN(SpanID3, _, TraceID)
                ] = F(<<"broker.publish">>, Spans),

                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F(<<"client.puback">>, Spans),

                [?MATCH_SUB_SPAN(SpanID4, SpanID3, _)] = F(<<"client.pubrec">>, Spans),
                [?MATCH_SUB_SPAN(SpanID5, SpanID4, _)] = F(<<"broker.pubrel">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID6, SpanID5, _)] = F(<<"client.pubcomp">>, Spans),

                true
            end,
            10_000
        )
    ),

    _ = disconnect_conns([Conn1, Conn2, Conn3]),
    ok.

t_e2e_client_pub_qos2_trace_level_0(Config) ->
    [Core1, Core2, Repl] = _Cluster = ?config(cluster, Config),

    OtelConf0 = enabled_e2e_trace_conf_all(Config),
    OtelConf = emqx_utils_maps:deep_put(
        [<<"traces">>, <<"filter">>, <<"e2e_tracing_options">>, <<"msg_trace_level">>],
        OtelConf0,
        0
    ),

    {ok, _} = rpc:call(
        Core1,
        emqx_conf,
        update,
        [?CONF_PATH, OtelConf, #{override_to => cluster}]
    ),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_2,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    ClientId3 = <<BaseClientId/binary, "-3">>,

    Conn1 = connect(Config, Core1, ClientId1),
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    Conn2 = connect(Config, Core2, ClientId2),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),
    Conn3 = connect(Config, Repl, ClientId3),
    {ok, _, [QoS]} = emqtt:subscribe(Conn3, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F1 = ?F(<<"client.clientid">>, [ClientId1, ClientId2, ClientId3]),
    F2 = ?F(
        <<"forward.to">>,
        [
            emqx_utils_conv:bin(node_name(?otel_trace_core1)),
            emqx_utils_conv:bin(node_name(?otel_trace_core2)),
            emqx_utils_conv:bin(node_name(?otel_trace_repl))
        ]
    ),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                10 = length(Spans),
                %% Note: Ignoring spans and sorting by time may cause order problems
                %% due to asynchronous requests. Manually sort by span name directly
                %%
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  └─ `message.route`
                %%         │
                %% 4.      ├─ `broker.publish` (Core1, ClientId1, to local node)
                %%         │
                %% 5.      ├─ `message.forward` (Core2, ClientId2)
                %% 6.      │   └─ `message.handle_forward`
                %% 7.      │       └─ `broker.publish` (ClientId2)
                %%         │
                %% 8.      └─ `message.forward` (Repl, ClientId3)
                %% 9.          └─ `message.handle_forward`
                %% 10.             └─ `broker.publish` (ClientId3)

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F1(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F1(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F1(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(_SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(_SpanID7, SpanID6, _),
                    ?MATCH_SUB_SPAN(_SpanID10, SpanID9, _)
                ] =
                    F1(<<"broker.publish">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID5, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID8, SpanID3, _)
                ] = F2(<<"message.forward">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID6, SpanID5, _),
                    ?MATCH_SUB_SPAN(SpanID9, SpanID8, _)
                ] = F2(<<"message.handle_forward">>, Spans),

                true
            end,
            10_000
        )
    ),

    _ = disconnect_conns([Conn1, Conn2, Conn3]),
    ok.

t_e2e_client_pub_qos2_trace_level_1(Config) ->
    [Core1, Core2, Repl] = _Cluster = ?config(cluster, Config),

    OtelConf0 = enabled_e2e_trace_conf_all(Config),
    OtelConf = emqx_utils_maps:deep_put(
        [<<"traces">>, <<"filter">>, <<"e2e_tracing_options">>, <<"msg_trace_level">>],
        OtelConf0,
        1
    ),

    {ok, _} = rpc:call(
        Core1,
        emqx_conf,
        update,
        [?CONF_PATH, OtelConf, #{override_to => cluster}]
    ),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_2,

    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    ClientId3 = <<BaseClientId/binary, "-3">>,

    Conn1 = connect(Config, Core1, ClientId1),
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    Conn2 = connect(Config, Core2, ClientId2),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),
    Conn3 = connect(Config, Repl, ClientId3),
    {ok, _, [?QOS_1]} = emqtt:subscribe(Conn3, Topic, ?QOS_1),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F1 = ?F(<<"client.clientid">>, [ClientId1, ClientId2, ClientId3]),
    F2 = ?F(
        <<"forward.to">>,
        [
            emqx_utils_conv:bin(node_name(?otel_trace_core1)),
            emqx_utils_conv:bin(node_name(?otel_trace_core2)),
            emqx_utils_conv:bin(node_name(?otel_trace_repl))
        ]
    ),

    ?assertEqual(
        ok,
        emqx_common_test_helpers:wait_for(
            ?FUNCTION_NAME,
            ?LINE,
            fun() ->
                {ok, #{<<"data">> := ClientPublishTraces}} = search_jaeger_traces(
                    ?config(jaeger_url, Config),
                    "client.publish",
                    #{
                        %% find the publisher
                        <<"client.clientid">> => ClientId1,
                        <<"cluster.id">> => <<"emqxcl">>
                    }
                ),
                ct:pal("SubTraces: ~p~n", [ClientPublishTraces]),

                [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = ClientPublishTraces,
                14 = length(Spans),
                %% Note: Ignoring spans and sorting by time may cause order problems
                %% due to asynchronous requests. Manually sort by span name directly
                %%
                %% 1, `client.publish` (ClientId1) Root span
                %% 2.  ├─ `client.authz`
                %% 3.  ├─ `message.route`
                %%     │   │
                %% 4.  │   ├─ `broker.publish` (Core1, ClientId1, to local node)
                %% 5.  │   │   └─ `client.pubrec`
                %%     │   │
                %% 6.  │   ├─ `message.forward` (Core2, ClientId2)
                %% 7.  │   │   └─ `message.handle_forward`
                %% 8.  │   │       └─ `broker.publish` (ClientId2)
                %% 9.  │   │           └─ `client.pubrec`
                %%     │   │
                %% 10. │   └─ `message.forward` (Repl, ClientId3)
                %% 11. │       └─ `message.handle_forward`
                %% 12. │           └─ `broker.publish` (ClientId3 subscribed QoS1)
                %% 13. │               └─ `client.puback`
                %%     │
                %% 14. └─ `broker.pubrec` (ClientId1)

                [?MATCH_ROOT_SPAN(SpanID1, TraceID)] = F1(<<"client.publish">>, Spans),
                [?MATCH_SUB_SPAN(_SpanID2, SpanID1, _)] = F1(<<"client.authz">>, Spans),
                [?MATCH_SUB_SPAN(SpanID3, SpanID1, _)] = F1(<<"message.route">>, Spans),
                [
                    ?MATCH_SUB_SPAN(SpanID4, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID8, SpanID7, _),
                    ?MATCH_SUB_SPAN(SpanID12, SpanID11, _)
                ] =
                    F1(<<"broker.publish">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID6, SpanID3, _),
                    ?MATCH_SUB_SPAN(SpanID10, SpanID3, _)
                ] = F2(<<"message.forward">>, Spans),

                [
                    ?MATCH_SUB_SPAN(SpanID7, SpanID6, _),
                    ?MATCH_SUB_SPAN(SpanID11, SpanID10, _)
                ] = F2(<<"message.handle_forward">>, Spans),

                [
                    ?MATCH_SUB_SPAN(_SpanID5, SpanID4, _),
                    ?MATCH_SUB_SPAN(_SpanID9, SpanID8, _)
                ] = F1(<<"client.pubrec">>, Spans),

                [?MATCH_SUB_SPAN(_SpanID13, SpanID12, _)] = F1(<<"client.puback">>, Spans),

                [?MATCH_SUB_SPAN(_SpanID14, SpanID1, _)] = F1(<<"broker.pubrec">>, Spans),
                true
            end,
            10_000
        )
    ),

    _ = disconnect_conns([Conn1, Conn2, Conn3]),
    ok.

t_e2e_client_source_republish_to_clients('init', Config) ->
    ConnectorType = mqtt,
    ConnectorName = <<"my_mqtt_connector">>,
    ConnectorConfig = connector_config(),

    SourceType = mqtt,
    SourceName = <<"my_mqtt_source">>,
    SourceConfig = source_config(#{connector => ConnectorName}),

    ActionType = mqtt,
    ActionName = <<"my_mqtt_action">>,
    ActionConfig = action_config(#{connector => ConnectorName}),

    _ = emqx_conf:update([connectors, ConnectorType, ConnectorName], ConnectorConfig, #{
        override_to => cluster
    }),
    _ = emqx_bridge_v2:create(sources, SourceType, SourceName, SourceConfig),
    _ = emqx_bridge_v2:create(actions, ActionType, ActionName, ActionConfig),

    RepublishTopic = <<"republish/1">>,
    RepublishQOS = 2,
    RuleID = rule_id(Config),
    Rule = #{
        id => RuleID,
        sql => <<"SELECT * FROM \"$bridges/mqtt:my_mqtt_source\"">>,
        actions => [
            #{
                function => republish,
                args => #{
                    direct_dispatch => false,
                    payload => <<"${payload}">>,
                    qos => RepublishQOS,
                    retain => false,
                    topic => RepublishTopic,
                    user_properties => <<>>,
                    mqtt_properties => #{}
                }
            },
            #{
                function => console,
                args => #{}
            }
        ]
    },
    {ok, _} = emqx_rule_engine:create_rule(Rule),
    timer:sleep(500),

    [
        {rule_id, RuleID},
        {republish_topic, RepublishTopic},
        {republish_qos, RepublishQOS}
        | Config
    ];
t_e2e_client_source_republish_to_clients('end', _Config) ->
    %% apps stopped, no need to delete rule
    ok.

t_e2e_client_source_republish_to_clients(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    BaseClientId = e2e_client_id(Config),

    PubToTriggerSourceClientId = <<BaseClientId/binary, "-publish-to-trigger-source">>,
    SubRepublishClientId = <<BaseClientId/binary, "-sub-republish">>,

    TriggerSourcePid = connect(Config, PubToTriggerSourceClientId),
    SubRepublishPid = connect(Config, SubRepublishClientId),

    RepublishTopic = ?config(republish_topic, Config),
    {ok, _, [?QOS_2]} = emqtt:subscribe(SubRepublishPid, RepublishTopic, ?QOS_2),

    timer:sleep(200),

    PublishPayload = <<"__REMOTE_PAYLOAD__">>,
    PublishPayloadSize = size(PublishPayload),

    {ok, _} = emqtt:publish(
        TriggerSourcePid, <<"fetch_from_cluster">>, PublishPayload, ?QOS_1
    ),

    timer:sleep(500),

    F = ?F(
        <<"rule.id">>,
        [
            atom_to_binary(?EXT_TRACE__RULE_INTERNAL_CLIENTID),
            atom_to_binary(?EXT_TRACE__ACTION_INTERNAL_CLIENTID),
            SubRepublishClientId
        ]
    ),

    F2 = ?F(
        <<"action.function">>,
        [<<"republish">>, <<"console">>]
    ),

    ok = emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME,
        ?LINE,
        fun() ->
            RuleID = ?config(rule_id, Config),
            {ok, #{<<"data">> := RuleEngineTraces}} = search_jaeger_traces(
                ?config(jaeger_url, Config),
                %% `Source' triggered rule engine and then action(republish)
                "broker.rule_engine.apply",
                #{
                    %% find the publisher
                    <<"rule.id">> => RuleID,
                    <<"cluster.id">> => <<"emqxcl">>
                }
            ),
            ct:pal("SubTraces: ~p~n", [RuleEngineTraces]),

            [#{<<"spans">> := Spans, <<"traceID">> := TraceID}] = RuleEngineTraces,
            8 = length(Spans),

            %% 1. `broker.rule_engine.apply` (triggered by `Source') Root span
            %% 2.  ├─ `broker.rule_engine.action` (republish)
            %%     │
            %% 3.  ├─ `message.route`
            %% 4.  │   └─ `broker.publish` (to client SubRepublishPid)
            %% 5.  │       └─ `client.pubrec`
            %% 6.  │           └─ `broker.pubrel`
            %% 7.  │               └─ `client.pubcomp`
            %%     │
            %% 8.  └─ `broker.rule_engine.action` (console)

            %%%%%%%%%% Root span
            [?MATCH_ROOT_SPAN(SpanID1, Tags1, TraceID)] = F(<<"broker.rule_engine.apply">>, Spans),

            match_tags(
                #{
                    <<"rule.id">> => RuleID,
                    <<"rule.matched">> => <<"$bridges/mqtt:my_mqtt_source">>,
                    <<"rule.trigger">> => <<"$bridges/mqtt:my_mqtt_source">>
                },
                Tags1
            ),
            %%%%%%%%%% actions span
            [
                %% republish action
                ?MATCH_SUB_SPAN(
                    _SpanID2,
                    SpanID1,
                    Tags2,
                    TraceID
                ),
                %% console action
                ?MATCH_SUB_SPAN(
                    _SpanID8,
                    SpanID1,
                    Tags8,
                    TraceID
                )
            ] = F2(<<"broker.rule_engine.action">>, Spans),

            match_tags(
                #{
                    <<"rule.id">> => RuleID,
                    <<"action.function">> => <<"republish">>,
                    <<"action.type">> => <<"function">>
                },
                Tags2
            ),
            match_tags(
                #{
                    <<"rule.id">> => RuleID,
                    <<"action.function">> => <<"console">>,
                    <<"action.type">> => <<"function">>
                },
                Tags8
            ),

            %%%%%%%%%% message.route
            [
                ?MATCH_SUB_SPAN(
                    SpanID3,
                    SpanID1,
                    Tags3,
                    _
                )
            ] = F(<<"message.route">>, Spans),
            match_tags(
                #{
                    <<"client.clientid">> => RuleID,
                    <<"message.payload_size">> => PublishPayloadSize
                },
                Tags3
            ),

            %%%%%%%%%% publish to subscriber
            [
                ?MATCH_SUB_SPAN(
                    SpanID4,
                    SpanID3,
                    Tags4,
                    _
                )
            ] = F(<<"broker.publish">>, Spans),

            match_tags(
                #{
                    <<"client.clientid">> => SubRepublishClientId,
                    <<"message.payload_size">> => PublishPayloadSize
                },
                Tags4
            ),

            [?MATCH_SUB_SPAN(SpanID5, SpanID4, _)] = F(<<"client.pubrec">>, Spans),
            [?MATCH_SUB_SPAN(SpanID6, SpanID5, _)] = F(<<"broker.pubrel">>, Spans),
            [?MATCH_SUB_SPAN(_SpanID7, SpanID6, _)] = F(<<"client.pubcomp">>, Spans),

            true
        end,
        10_000
    ),

    _ = disconnect_conns([TriggerSourcePid, SubRepublishPid]),
    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

enabled_e2e_trace_conf_all(TcConfig) ->
    OtelConf = enabled_trace_conf(TcConfig),
    emqx_utils_maps:deep_put(
        [<<"traces">>, <<"filter">>, <<"e2e_tracing_options">>], OtelConf, #{
            <<"sample_ratio">> => 1.0,
            <<"msg_trace_level">> => 2,
            <<"client_connect_disconnect">> => true,
            <<"client_subscribe_unsubscribe">> => true,
            <<"client_messaging">> => true,
            <<"trace_rule_engine">> => true
        }
    ).

enabled_trace_conf(TcConfig) ->
    #{
        <<"traces">> => #{
            <<"enable">> => true,
            <<"scheduled_delay">> => <<"50ms">>,
            <<"filter">> => filter_conf(TcConfig)
        },
        <<"exporter">> => exporter_conf(TcConfig)
    }.

exporter_conf(TcConfig) ->
    #{<<"endpoint">> => ?config(otel_collector_url, TcConfig)}.

filter_conf(TcConfig) ->
    TrcaeMode = ?config(otel_trace_mode, TcConfig),
    filter_conf(TrcaeMode, TcConfig).

filter_conf(legacy, _TcConfig) ->
    #{<<"trace_mode">> => legacy};
filter_conf(e2e, TcConfig) ->
    #{
        <<"trace_mode">> => e2e,
        <<"e2e_tracing_options">> => e2e_tracing_opts(TcConfig)
    }.

e2e_tracing_opts(TcConfig) ->
    #{<<"follow_traceparent">> => ?config(otel_follow_traceparent, TcConfig)}.

span_node(#{<<"processID">> := ProcId}, Procs) ->
    #{ProcId := #{<<"tags">> := ProcTags}} = Procs,
    [#{<<"value">> := Node}] = lists:filter(
        fun(#{<<"key">> := K}) ->
            K =:= <<"service.instance.id">>
        end,
        ProcTags
    ),
    Node.

trace_id(<<"00-", TraceId:32/binary, _/binary>>) ->
    TraceId.

filter_traces(TraceId, Traces) ->
    lists:filter(fun(#{<<"traceID">> := TrId}) -> TrId =:= TraceId end, Traces).

filter_spans(OpName, Spans) ->
    lists:filter(fun(#{<<"operationName">> := Name}) -> Name =:= OpName end, Spans).

sort_spans_by_key_sequence(TagKeyName, KeySeq, OpName, Spans) ->
    NSpans = filter_spans(OpName, Spans),
    lists:sort(
        fun
            (#{<<"tags">> := TagsA}, #{<<"tags">> := TagsB}) ->
                KeyA = filter_tag_value(TagKeyName, TagsA),
                KeyB = filter_tag_value(TagKeyName, TagsB),
                case find_first(KeyA, KeyB, KeySeq) of
                    not_found -> false;
                    KeyA -> true;
                    KeyB -> false
                end;
            (_, _) ->
                false
        end,
        NSpans
    ).

filter_tag_value(TagKey, Tags) ->
    [#{<<"value">> := Value}] = filter_tags(TagKey, Tags),
    Value.

filter_tags(TagKey, Tags) ->
    lists:filter(fun(#{<<"key">> := Key}) -> Key =:= TagKey end, Tags).

-define(TAG_KEY(TAG), maps:get(<<"key">>, Tag)).
-define(TAG_VALUE(TAG), maps:get(<<"value">>, Tag)).

match_tags(TagsPattern, Tags0) ->
    Tags = #{?TAG_KEY(Tag) => ?TAG_VALUE(Tag) || Tag <- Tags0},
    true =
        lists:all(
            fun({PatternKey, PatternValue}) ->
                maps:get(PatternKey, Tags) =:= PatternValue
            end,
            [{PatternKey, PatternValue} || PatternKey := PatternValue <- TagsPattern]
        ).

find_first(_, _, []) ->
    not_found;
find_first(A, _B, [A | _Rest]) ->
    A;
find_first(_A, B, [B | _Rest]) ->
    B;
find_first(A, B, [_X | Rest]) ->
    find_first(A, B, Rest).

get_jaeger_traces(JaegerBaseURL) ->
    case httpc:request(JaegerBaseURL ++ "/api/traces?service=" ++ ?OTEL_SERVICE_NAME) of
        {ok, {{_, 200, _}, _, RespBpdy}} ->
            {ok, emqx_utils_json:decode(RespBpdy)};
        Err ->
            ct:pal("Jaeger error: ~p", Err),
            Err
    end.

search_jaeger_traces(JaegerBaseURL, SpanName, Tags) ->
    %% `Tags' is the term used in Jaeger,
    %% which refers to the `Attributes' in the trace span.
    QueryString = build_query_string(#{
        service => ?OTEL_SERVICE_NAME,
        operation => SpanName,
        tags => Tags
    }),
    case httpc:request(JaegerBaseURL ++ "/api/traces?" ++ QueryString) of
        {ok, {{_, 200, _}, _, RespBpdy}} ->
            {ok, emqx_utils_json:decode(RespBpdy)};
        Err ->
            ct:pal("Jaeger error: ~p", Err),
            Err
    end.

props(true, TraceParent) ->
    props(TraceParent);
props(false, _) ->
    #{}.

props(TraceParent) ->
    #{'User-Property' => [{<<"traceparent">>, TraceParent}]}.

refs_length_with_traceparent(true) ->
    1;
refs_length_with_traceparent(false) ->
    0.

trace_id_assert(true, TraceId1, TraceId2) ->
    TraceId1 =:= TraceId2;
trace_id_assert(false, _, _) ->
    true.

traceparent(IsSampled) ->
    TraceId = otel_id_generator:generate_trace_id(),
    SpanId = otel_id_generator:generate_span_id(),
    {ok, TraceIdHexStr} = otel_utils:format_binary_string("~32.16.0b", [TraceId]),
    {ok, SpanIdHexStr} = otel_utils:format_binary_string("~16.16.0b", [SpanId]),
    TraceFlags =
        case IsSampled of
            true -> <<"01">>;
            false -> <<"00">>
        end,
    <<"00-", TraceIdHexStr/binary, "-", SpanIdHexStr/binary, "-", TraceFlags/binary>>.

e2e_client_id(Config) ->
    Rand = rand:uniform(1000),
    iolist_to_binary(
        io_lib:format("~s.~s.~s.~s.~B", [
            ?config(group_otel_conn_type, Config),
            ?config(group_client_conn_type, Config),
            ?config(group_follow_traceparent, Config),
            ?config(tc, Config),
            Rand
        ])
    ).

rule_id(Config) ->
    <<"rule_id_", (e2e_client_id(Config))/binary>>.

connect(Config, ClientId) ->
    connect(Config, node(), ClientId).

connect(Config, Node, ClientId) ->
    connect(Config, Node, ClientId, #{}).

connect(Config, Node, ClientId, Props) when is_atom(Node) ->
    connect(Config, mqtt_host_port(Config, Node), ClientId, Props);
connect(Config, {Host, Port}, ClientId, Props) ->
    {ConnFun, ConnOpts} = conn_opts(Config),
    {ok, ConnPid} = emqtt:start_link(
        [
            {proto_ver, v5},
            {host, Host},
            {port, Port},
            {clientid, ClientId},
            {properties, Props}
        ] ++ ConnOpts
    ),
    {ok, _} = ConnFun(ConnPid),
    ConnPid.

-define(CERTS_PATH(CertName), filename:join(["etc", "certs", CertName])).

-define(MQTT_SSL_CLIENT_CERTS, [
    {keyfile, ?CERTS_PATH("client-key.pem")},
    {cacertfile, ?CERTS_PATH("cacert.pem")},
    {certfile, ?CERTS_PATH("client-cert.pem")}
]).

conn_opts(Config) ->
    conn_opts(?config(group_client_conn_type, Config), Config).

conn_opts(tcp, _Config) ->
    {fun emqtt:connect/1, []};
conn_opts(ssl, _Config) ->
    {fun emqtt:connect/1, [{ssl, true}, {ssl_opts, client_ssl_opts()}]};
conn_opts(ws, _Config) ->
    {fun emqtt:ws_connect/1, [{ws_path, "/mqtt"}]};
conn_opts(wss, _Config) ->
    {fun emqtt:ws_connect/1, [
        {ws_path, "/mqtt"},
        {ws_transport_options, [
            {http_opts, #{version => 'HTTP/1.1'}},
            {protocols, [http]},
            {transport, tls},
            {tls_opts, client_ssl_opts()}
        ]}
    ]}.

client_ssl_opts() ->
    [{verify, verify_none}] ++ client_certs().

client_certs() ->
    [
        {Key, emqx_common_test_helpers:app_path(emqx, FilePath)}
     || {Key, FilePath} <- ?MQTT_SSL_CLIENT_CERTS
    ].

disconnect_conn(ConnPid) ->
    disconnect_conns([ConnPid]).

disconnect_conns(Conns) ->
    lists:foreach(fun emqtt:disconnect/1, Conns).

stop_conn(ConnPid) ->
    stop_conns([ConnPid]).

stop_conns(Conns) ->
    lists:foreach(fun emqtt:stop/1, Conns).

mqtt_host_port(Config, Node) ->
    ListenerType = ?config(group_client_conn_type, Config),
    rpc:call(Node, emqx, get_config, [[listeners, ListenerType, default, bind]]).

cluster(TC, Config) ->
    _Nodes = emqx_cth_cluster:start(
        [
            {?otel_trace_core1, #{apps => apps_spec()}},
            {?otel_trace_core2, #{apps => apps_spec()}},
            {?otel_trace_repl, #{apps => apps_spec(), role => replicant}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ).

node_name(Name) ->
    emqx_cth_cluster:node_name(Name).

apps_spec() ->
    [
        emqx,
        emqx_conf,
        emqx_management,
        emqx_opentelemetry
    ].

apps_spec_with_rule_engine() ->
    [
        emqx,
        emqx_conf,
        emqx_management,
        emqx_opentelemetry,
        emqx_rule_engine,
        emqx_connector,
        emqx_bridge_mqtt,
        emqx_bridge
    ].

build_query_string(Query = #{}) ->
    build_query_string(maps:to_list(Query));
build_query_string(Query = [{_, _} | _]) ->
    uri_string:compose_query([{emqx_utils_conv:bin(K), emqx_utils_conv:bin(V)} || {K, V} <- Query]);
build_query_string(QueryString) ->
    unicode:characters_to_list(QueryString).

connector_config() ->
    connector_config(_Overrides = #{}).

connector_config(Overrides) ->
    Defaults = #{
        <<"enable">> => true,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"clean_start">> => true,
        <<"connect_timeout">> => <<"5s">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"1s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    emqx_utils_maps:deep_merge(Defaults, Overrides).

-define(PLEASE_OVERRIDE, <<"__PLEASE_OVERRIDE__">>).

action_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"connector">> => ?PLEASE_OVERRIDE,
            <<"enable">> => true,
            <<"fallback_actions">> => [],
            <<"parameters">> =>
                #{
                    <<"payload">> => <<"${payload}">>,
                    <<"qos">> => 1,
                    <<"retain">> => false,
                    <<"topic">> => <<"pub/to/remote">>
                },
            <<"resource_opts">> =>
                #{
                    <<"health_check_interval">> => <<"15s">>,
                    <<"query_mode">> => <<"async">>
                }
        },

    maps:merge(CommonConfig, Overrides).

source_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    CommonConfig =
        #{
            <<"connector">> => ?PLEASE_OVERRIDE,
            <<"enable">> => true,
            <<"parameters">> =>
                #{
                    <<"no_local">> => false,
                    <<"qos">> => 1,
                    <<"topic">> => <<"$share/group/fetch_from_cluster">>
                },
            <<"resource_opts">> =>
                #{<<"health_check_interval">> => <<"15s">>}
        },

    maps:merge(CommonConfig, Overrides).
