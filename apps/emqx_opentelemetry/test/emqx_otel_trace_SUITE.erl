%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_otel_trace_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/logger.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(OTEL_SERVICE_NAME, "emqx").
-define(CONF_PATH, [opentelemetry]).

%% How to run it locally:
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
%%                     --suite apps/emqx_opentelemetry/test/emqx_otel_trace_SUITE.erl

all() ->
    [
        {group, tcp},
        {group, tls}
    ].

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    [
        {tcp, TCs},
        {tls, TCs}
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
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ];
init_per_group(tls = Group, Config) ->
    OtelCollectorURL = os:getenv(
        "OTEL_COLLECTOR_TLS_URL", "https://otel-collector-tls.emqx.net:4317"
    ),
    [
        {otel_collector_url, OtelCollectorURL},
        {logs_exporter_file_path, logs_exporter_file_path(Group, Config)}
        | Config
    ].

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(t_distributed_trace = TC, Config) ->
    Cluster = cluster(TC, Config),
    [{cluster, Cluster} | Config];
init_per_testcase(TC, Config) ->
    Apps = emqx_cth_suite:start(apps_spec(), #{work_dir => emqx_cth_suite:work_dir(TC, Config)}),
    [{suite_apps, Apps} | Config].

end_per_testcase(t_distributed_trace = _TC, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    emqx_config:delete_override_conf_files(),
    ok;
end_per_testcase(_TC, Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)),
    emqx_config:delete_override_conf_files(),
    ok.

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

%% Keeping this test in this SUITE as there is no separate module for logs
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

enabled_trace_conf(TcConfig) ->
    #{
        <<"traces">> => #{
            <<"enable">> => true,
            <<"scheduled_delay">> => <<"50ms">>
        },
        <<"exporter">> => exporter_conf(TcConfig)
    }.

exporter_conf(TcConfig) ->
    #{<<"endpoint">> => ?config(otel_collector_url, TcConfig)}.

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

get_jaeger_traces(JagerBaseURL) ->
    case httpc:request(JagerBaseURL ++ "/api/traces?service=" ++ ?OTEL_SERVICE_NAME) of
        {ok, {{_, 200, _}, _, RespBpdy}} ->
            {ok, emqx_utils_json:decode(RespBpdy)};
        Err ->
            ct:pal("Jager error: ~p", Err),
            Err
    end.

stop_conns(Conns) ->
    lists:foreach(fun emqtt:stop/1, Conns).

props(TraceParent) ->
    #{'User-Property' => [{<<"traceparent">>, TraceParent}]}.

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

connect({Host, Port}, ClientId) ->
    {ok, ConnPid} = emqtt:start_link([
        {proto_ver, v5},
        {host, Host},
        {port, Port},
        {clientid, ClientId}
    ]),
    {ok, _} = emqtt:connect(ConnPid),
    ConnPid.

mqtt_host_port() ->
    emqx:get_config([listeners, tcp, default, bind]).

mqtt_host_port(Node) ->
    rpc:call(Node, emqx, get_config, [[listeners, tcp, default, bind]]).

cluster(TC, Config) ->
    Nodes = emqx_cth_cluster:start(
        [
            {otel_trace_node1, #{apps => apps_spec()}},
            {otel_trace_node2, #{apps => apps_spec()}},
            {otel_trace_node3, #{apps => apps_spec()}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    Nodes.

apps_spec() ->
    [
        emqx,
        emqx_conf,
        emqx_management,
        emqx_opentelemetry
    ].
