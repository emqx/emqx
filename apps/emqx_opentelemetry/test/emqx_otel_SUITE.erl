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
    t_e2e_cilent_publish_qos2_with_forward/1
]).

-include_lib("emqx/include/logger.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTEL_SERVICE_NAME, "emqx").
-define(CONF_PATH, [opentelemetry]).

-define(otel_trace_core1, otel_trace_core1).
-define(otel_trace_core2, otel_trace_core2).
-define(otel_trace_repl, otel_trace_repl).
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
        {group, tcp},
        {group, tls}
    ].

groups() ->
    LogsCases = [
        t_log
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
        t_e2e_cilent_publish_qos2_with_forward
    ],
    %% TODO: Add metrics test cases
    MetricsGroups = [
        %% t_metrics
    ],
    FeatureGroups = [
        {group, logs},
        {group, traces},
        {group, metrics}
    ],
    [
        {tcp, FeatureGroups},
        {tls, FeatureGroups},
        {logs, LogsCases},
        {traces, TraceGroups},
        {metrics, MetricsGroups},
        {trace_legacy_mode, LegacyModeTraceCases},
        {trace_e2e_mode, E2ETraceGroups},
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

init_per_group(tcp = Group, Config) ->
    OtelCollectorURL = os:getenv("OTEL_COLLECTOR_URL", "http://otel-collector.emqx.net:4317"),
    [
        {group_conn_type, Group},
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ];
init_per_group(tls = Group, Config) ->
    OtelCollectorURL = os:getenv(
        "OTEL_COLLECTOR_TLS_URL", "https://otel-collector-tls.emqx.net:4317"
    ),
    [
        {group_conn_type, Group},
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ];
init_per_group(trace_legacy_mode = Group, Config) ->
    [
        {otel_trace_mode, legacy},
        {trace_mode_group_name, Group}
        | Config
    ];
init_per_group(trace_e2e_mode = Group, Config) ->
    [
        {otel_trace_mode, e2e},
        {trace_mode_group_name, Group}
        | Config
    ];
init_per_group(e2e_with_traceparent = Group, Config) ->
    [
        {otel_follow_traceparent, true},
        {traceparent_group_name, Group}
        | Config
    ];
init_per_group(e2e_no_traceparent = Group, Config) ->
    [
        {otel_follow_traceparent, false},
        {traceparent_group_name, Group}
        | Config
    ];
init_per_group(Group, Config) ->
    [{group, Group} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TC, Config) when
    TC =:= t_distributed_trace orelse
        TC =:= t_e2e_cilent_publish_qos2_with_forward
->
    Cluster = cluster(TC, Config),
    [{tc, TC}, {cluster, Cluster} | Config];
init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(apps_spec(), #{work_dir => emqx_cth_suite:work_dir(TC, Config)}),
    [{tc, TC}, {suite_apps, Apps} | Config].

end_per_testcase(TC, Config) when
    TC =:= t_distributed_trace orelse
        TC =:= t_e2e_cilent_publish_qos2_with_forward
->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    emqx_config:delete_override_conf_files(),
    ok;
end_per_testcase(_TC, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

logs_exporter_file_path(Group, Config) ->
    filename:join([project_dir(Config), logs_exporter_filename(Group)]).

project_dir(Config) ->
    filename:join(
        lists:takewhile(
            fun(PathPart) -> PathPart =/= "_build" end,
            filename:split(?config(priv_dir, Config))
        )
    ).

logs_exporter_filename(tcp) ->
    ".ci/docker-compose-file/otel/otel-collector.json";
logs_exporter_filename(tls) ->
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
    MqttHostPort = mqtt_host_port(),

    {ok, _} = emqx_conf:update(?CONF_PATH, enabled_trace_conf(Config), #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    TopicNoSubs = <<"t/trace/test/nosub/", (atom_to_binary(?FUNCTION_NAME))/binary>>,

    SubConn1 = connect(MqttHostPort, <<"sub1">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn1, Topic),
    SubConn2 = connect(MqttHostPort, <<"sub2">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn2, Topic),
    PubConn = connect(MqttHostPort, <<"pub">>),

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

t_trace_disabled(_Config) ->
    ?assertNot(emqx:get_config(?CONF_PATH ++ [traces, enable])),
    %% Tracer must be actually disabled
    ?assertEqual({otel_tracer_noop, []}, opentelemetry:get_tracer()),
    ?assertEqual(undefined, emqx_external_trace:provider()),

    Topic = <<"t/trace/test", (atom_to_binary(?FUNCTION_NAME))/binary>>,

    SubConn = connect(mqtt_host_port(), <<"sub">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn, Topic),
    PubConn = connect(mqtt_host_port(), <<"pub">>),

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
    PubConn = connect(mqtt_host_port(), ClientId),
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

    SubConn1 = connect(mqtt_host_port(Core1), <<"sub1">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn1, Topic),
    SubConn2 = connect(mqtt_host_port(Core2), <<"sub2">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn2, Topic),
    SubConn3 = connect(mqtt_host_port(Repl), <<"sub3">>),
    {ok, _, [0]} = emqtt:subscribe(SubConn3, Topic),

    PubConn = connect(mqtt_host_port(Repl), <<"pub">>),

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

    MqttHostPort = mqtt_host_port(),
    ClientId = e2e_client_id(Config),

    WithTraceparent = ?config(otel_follow_traceparent, Config),
    ConnectTraceParent = traceparent(true),
    DisconnectTraceParent = traceparent(true),

    Conn = connect(MqttHostPort, ClientId, props(WithTraceparent, ConnectTraceParent)),
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

    MqttHostPort = mqtt_host_port(),
    ClientId = e2e_client_id(Config),
    Conn = connect(MqttHostPort, ClientId),
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

    MqttHostPort = mqtt_host_port(),
    ClientId = e2e_client_id(Config),

    WithTraceparent = ?config(otel_follow_traceparent, Config),
    SubTraceParent = traceparent(true),
    UnsubTraceParent = traceparent(true),

    Conn = connect(MqttHostPort, ClientId),
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

-define(MATCH_SUB_SPAN(SpanID, ParentSpanID, TraceID), #{
    <<"spanID">> := SpanID,
    <<"traceID">> := TraceID,
    <<"references">> := [
        #{<<"refType">> := <<"CHILD_OF">>, <<"traceID">> := TraceID, <<"spanID">> := ParentSpanID}
    ]
}).

-define(F(TagKeyName, TagSeq, OperationName, Spans), fun(OperationName, Spans) ->
    sort_spans_by_key_sequence(TagKeyName, TagSeq, OperationName, Spans)
end).

t_e2e_cilent_publish_qos0(Config) ->
    OtelConf = enabled_e2e_trace_conf_all(Config),
    {ok, _} = emqx_conf:update(?CONF_PATH, OtelConf, #{override_to => cluster}),

    Topic = <<"t/trace/test/", (atom_to_binary(?FUNCTION_NAME))/binary>>,
    QoS = ?QOS_0,

    MqttHostPort = mqtt_host_port(),
    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(MqttHostPort, ClientId1),
    Conn2 = connect(MqttHostPort, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    ok = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    timer:sleep(200),
    _ = disconnect_conns([Conn1, Conn2]),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2], OperationName, Spans),

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

    MqttHostPort = mqtt_host_port(),
    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(MqttHostPort, ClientId1),
    Conn2 = connect(MqttHostPort, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2], OperationName, Spans),

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

    MqttHostPort = mqtt_host_port(),
    BaseClientId = e2e_client_id(Config),
    ClientId1 = <<BaseClientId/binary, "-1">>,
    ClientId2 = <<BaseClientId/binary, "-2">>,
    Conn1 = connect(MqttHostPort, ClientId1),
    Conn2 = connect(MqttHostPort, ClientId2),

    timer:sleep(200),
    %% both subscribe the topic
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F = ?F(<<"client.clientid">>, [ClientId1, ClientId2], OperationName, Spans),

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

    Conn1 = connect(mqtt_host_port(Core1), ClientId1),
    {ok, _, [QoS]} = emqtt:subscribe(Conn1, Topic, QoS),
    Conn2 = connect(mqtt_host_port(Core2), ClientId2),
    {ok, _, [QoS]} = emqtt:subscribe(Conn2, Topic, QoS),
    Conn3 = connect(mqtt_host_port(Repl), ClientId3),
    {ok, _, [QoS]} = emqtt:subscribe(Conn3, Topic, QoS),

    timer:sleep(200),
    {ok, _} = emqtt:publish(Conn1, Topic, <<"must be traced">>, QoS),

    F1 = ?F(<<"client.clientid">>, [ClientId1, ClientId2, ClientId3], OperationName, Spans),
    F2 = ?F(
        <<"forward.to">>,
        [
            emqx_utils_conv:bin(node_name(?otel_trace_core1)),
            emqx_utils_conv:bin(node_name(?otel_trace_core2)),
            emqx_utils_conv:bin(node_name(?otel_trace_repl))
        ],
        OperationName,
        Spans
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

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

%% TODO: Update msg_trace_level to test qos upgrade/downgrade
enabled_e2e_trace_conf_all(TcConfig) ->
    OtelConf = enabled_trace_conf(TcConfig),
    emqx_utils_maps:deep_put(
        [<<"traces">>, <<"filter">>, <<"e2e_tracing_options">>], OtelConf, #{
            <<"sample_ratio">> => 1.0,
            <<"msg_trace_level">> => 2,
            <<"client_connect_disconnect">> => true,
            <<"client_subscribe_unsubscribe">> => true,
            <<"client_messaging">> => true
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
            ?config(group_conn_type, Config),
            ?config(trace_mode_group_name, Config),
            ?config(traceparent_group_name, Config),
            ?config(tc, Config),
            Rand
        ])
    ).

connect({Host, Port}, ClientId) ->
    connect({Host, Port}, ClientId, #{}).

connect({Host, Port}, ClientId, Props) ->
    {ok, ConnPid} = emqtt:start_link([
        {proto_ver, v5},
        {host, Host},
        {port, Port},
        {clientid, ClientId},
        {properties, Props}
    ]),
    {ok, _} = emqtt:connect(ConnPid),
    ConnPid.

disconnect_conn(ConnPid) ->
    disconnect_conns([ConnPid]).

disconnect_conns(Conns) ->
    lists:foreach(fun emqtt:disconnect/1, Conns).

stop_conn(ConnPid) ->
    stop_conns([ConnPid]).

stop_conns(Conns) ->
    lists:foreach(fun emqtt:stop/1, Conns).

mqtt_host_port() ->
    emqx:get_config([listeners, tcp, default, bind]).

mqtt_host_port(Node) ->
    rpc:call(Node, emqx, get_config, [[listeners, tcp, default, bind]]).

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

build_query_string(Query = #{}) ->
    build_query_string(maps:to_list(Query));
build_query_string(Query = [{_, _} | _]) ->
    uri_string:compose_query([{emqx_utils_conv:bin(K), emqx_utils_conv:bin(V)} || {K, V} <- Query]);
build_query_string(QueryString) ->
    unicode:characters_to_list(QueryString).

to_list(A) when is_atom(A) ->
    atom_to_list(A);
to_list(B) when is_binary(B) ->
    binary_to_list(B);
to_list(L) when is_list(L) ->
    L.
