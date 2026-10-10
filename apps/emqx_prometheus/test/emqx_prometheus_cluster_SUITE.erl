%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_prometheus_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_prometheus/include/emqx_prometheus.hrl").

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(TCConfig) ->
    TCConfig.

end_per_suite(_TCConfig) ->
    ok.

init_per_testcase(_TestCase, TCConfig) ->
    snabbkaffe:start_trace(),
    TCConfig.

end_per_testcase(_TestCase, _TCConfig) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

mk_cluster(TestCase, #{n := NumNodes} = Opts, TCConfig) ->
    Overrides0 = maps:get(overrides, Opts, #{}),
    ExtraApps = maps:get(extra_apps, Opts, []),
    PrometheusConf = maps:get(
        prometheus_conf, Opts, "prometheus { enable_basic_auth = false }"
    ),
    AppSpecs0 =
        [
            emqx_conf,
            emqx_management,
            {emqx_prometheus, PrometheusConf}
        ] ++ ExtraApps,
    NodeSpecs0 = lists:map(
        fun(N) ->
            Overrides = maps:get(N, Overrides0, #{}),
            Role = maps:get(role, Overrides, core),
            Name = mk_node_name(TestCase, N),
            Apps = lists:flatten([
                AppSpecs0,
                [emqx_mgmt_api_test_util:emqx_dashboard() || N == 1]
            ]),
            {Name, #{apps => Apps, role => Role}}
        end,
        lists:seq(1, NumNodes)
    ),
    Nodes = emqx_cth_cluster:start(
        NodeSpecs0,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    ?ON_ALL(Nodes, begin
        meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
        meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end),
        %% 1859673600 epoch corresponds to 2028-12-06; mirror it in dump/0
        %% so the gauge for emqx_license_expiry_at stays bit-exact equal.
        meck:expect(emqx_license_checker, dump, fun() ->
            [
                {customer, "TestCo"},
                {max_sessions, 1000},
                {start_at, <<"2024-01-01">>},
                {expiry_at, <<"2028-12-06">>}
            ]
        end)
    end),
    Nodes.

mk_node_name(TestCase, N) ->
    Name0 = iolist_to_binary([atom_to_binary(TestCase), "_", integer_to_binary(N)]),
    binary_to_atom(Name0).

get_prometheus_stats(Mode) ->
    QueryString = uri_string:compose_query([{"mode", atom_to_binary(Mode)}]),
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => [],
        query_params => QueryString,
        auth_header => {"no", "auth"}
    }),
    case Status of
        200 ->
            {Status, parse_prometheus(Response)};
        _ ->
            {Status, Response}
    end.

parse_prometheus(RawData) ->
    lists:foldl(
        fun
            (<<"#", _/binary>>, Acc) ->
                Acc;
            (Line, Acc) ->
                {Name, Labels, Value} = parse_prometheus_line(Line),
                maps:update_with(
                    Name,
                    fun(Old) -> Old#{Labels => Value} end,
                    #{Labels => Value},
                    Acc
                )
        end,
        #{},
        binary:split(iolist_to_binary(RawData), <<"\n">>, [global, trim_all])
    ).

parse_prometheus_line(Line) ->
    RE = <<"(?<name>[a-z0-9A-Z_]+)(\\{(?<labels>[^)]*)\\})? *(?<value>[0-9]+(\\.[0-9]+)?)">>,
    {match, [Name, Labels0, Value0]} = re:run(
        Line, RE, [{capture, [<<"name">>, <<"labels">>, <<"value">>], binary}]
    ),
    Labels = parse_prometheus_labels(Labels0),
    Value =
        try
            binary_to_float(Value0)
        catch
            error:badarg ->
                binary_to_integer(Value0)
        end,
    {Name, Labels, Value}.

parse_prometheus_labels(<<"">>) ->
    #{};
parse_prometheus_labels(Labels) ->
    lists:foldl(
        fun(Label, Acc) ->
            [K, V0] = binary:split(Label, <<"=">>),
            V = binary:replace(V0, <<"\"">>, <<"">>, [global]),
            Acc#{K => V}
        end,
        #{},
        binary:split(Labels, <<",">>, [global])
    ).

%% Publish a message over the public broker path, attributed to a
%% namespace through `client_attrs.tns'.  Run it on the node whose local
%% counter should move.
publish_as(Namespace, Topic) ->
    Msg = emqx_message:set_headers(
        #{client_attrs => #{?CLIENT_ATTR_NAME_TNS => Namespace}},
        emqx_message:make(<<"cluster-test">>, Topic, <<>>)
    ),
    _ = emqx_broker:publish(Msg),
    ok.

get_topic_metrics(Mode, Opts) ->
    Ns = maps:get(ns, Opts, undefined),
    AuthHeader = maps:get(auth_header, Opts, {"no", "auth"}),
    QueryString = uri_string:compose_query(
        lists:flatten([
            {"mode", atom_to_binary(Mode)},
            [{"ns", Ns} || Ns /= undefined]
        ])
    ),
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "topic_metrics"]),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => [],
        query_params => QueryString,
        auth_header => AuthHeader
    }),
    case Status of
        200 ->
            {Status, parse_prometheus(Response)};
        _ ->
            {Status, Response}
    end.

%% A bearer token for a dashboard user, obtained over HTTP.
login_token(Username, Password) ->
    {200, #{<<"token">> := Token}} = emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["login"]),
        auth_header => [{"no", "auth"}],
        body => #{<<"username">> => Username, <<"password">> => Password}
    }),
    Token.

bearer_auth_header(Token) ->
    {"Authorization", <<"Bearer ", Token/binary>>}.

%% Create a dashboard user scoped to one namespace, then log it in.
tenant_auth_header(GlobalAuth, Ns, Role) ->
    Username = <<Ns/binary, "_", Role/binary>>,
    Password = <<"SuperP@ss!1">>,
    {200, _} = emqx_mgmt_api_test_util:simple_request(#{
        method => post,
        url => emqx_mgmt_api_test_util:api_path(["users"]),
        auth_header => GlobalAuth,
        body => #{
            <<"username">> => Username,
            <<"password">> => Password,
            <<"role">> => <<"ns:", Ns/binary, "::", Role/binary>>,
            <<"description">> => <<"cluster namespace test">>
        }
    }),
    bearer_auth_header(login_token(Username, Password)).

%% Expected parsed exposition for `{BinName, TopicFilter, OwnerNs, N1In,
%% N2In, BytesPerMessage}' rows, where `OwnerNs = undefined' is a
%% global-owned collection.  `node' mode is served by the dashboard node
%% (N1); `all_nodes_aggregated' sums both nodes into one series;
%% `all_nodes_unaggregated' keeps one series per node.  Every fixture
%% publish has no subscriber, so it is counted as dropped, never as
%% delivered.
expected_exposition(N1, N2, Mode, Rows) ->
    lists:foldl(
        fun(Row, Acc) ->
            lists:foldl(
                fun(Point, AccIn) -> put_row_points(AccIn, Point) end,
                Acc,
                row_points(N1, N2, Mode, Row)
            )
        end,
        #{},
        Rows
    ).

row_points(N1, N2, Mode, {BinName, TopicFilter, OwnerNs, N1In, N2In, Bytes}) ->
    Labels0 = #{<<"name">> => BinName, <<"topic_filter">> => TopicFilter},
    Labels =
        case OwnerNs of
            undefined -> Labels0;
            _ -> Labels0#{<<"namespace">> => OwnerNs}
        end,
    case Mode of
        ?PROM_DATA_MODE__NODE ->
            [{Labels, N1In, N1In * Bytes}];
        ?PROM_DATA_MODE__ALL_NODES_AGGREGATED ->
            [{Labels, N1In + N2In, (N1In + N2In) * Bytes}];
        ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED ->
            [
                {Labels#{<<"node">> => atom_to_binary(N1, utf8)}, N1In, N1In * Bytes},
                {Labels#{<<"node">> => atom_to_binary(N2, utf8)}, N2In, N2In * Bytes}
            ]
    end.

put_row_points(Acc0, {Labels, MessagesIn, BytesIn}) ->
    lists:foldl(
        fun({Family, Value}, Acc) ->
            maps:update_with(
                Family,
                fun(Series) -> Series#{Labels => Value} end,
                #{Labels => Value},
                Acc
            )
        end,
        Acc0,
        [
            {<<"emqx_topic_metric_messages_in_count">>, MessagesIn},
            {<<"emqx_topic_metric_messages_out_count">>, 0},
            {<<"emqx_topic_metric_messages_dropped_count">>, MessagesIn},
            {<<"emqx_topic_metric_bytes_in">>, BytesIn},
            {<<"emqx_topic_metric_bytes_out">>, 0}
        ]
    ).

%% Distinct `namespace' label values present in one metric family.
family_namespaces(Res, MetricName) ->
    lists:usort([
        maps:get(<<"namespace">>, Labels)
     || Labels <- maps:keys(maps:get(MetricName, Res, #{})),
        is_map_key(<<"namespace">>, Labels)
    ]).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_mria_shard_lag_cache(TCConfig) ->
    Opts = #{
        n => 2,
        overrides => #{2 => #{role => replicant}}
    },
    Nodes = mk_cluster(?FUNCTION_NAME, Opts, TCConfig),
    %% Sync cache process to ensure it has already cached some stuff.
    ?ON_ALL(Nodes, gen_server:call(emqx_prometheus_cache, i_dont_exist)),
    %% We make getting the shard lag take a long time.  Calling the API shouldn't timeout
    %% due to that.
    ?ON_ALL(Nodes, begin
        ok = meck:new(mria_status, [passthrough, no_link]),
        ok = meck:expect(mria_status, get_stat, fun(Shard, Metric) ->
            case Metric of
                core_intercept ->
                    timer:sleep(30_000);
                _ ->
                    ok
            end,
            meck:passthrough([Shard, Metric])
        end)
    end),
    T0 = erlang:monotonic_time(millisecond),
    {200, Stats0} = get_prometheus_stats(?PROM_DATA_MODE__ALL_NODES_AGGREGATED),
    T1 = erlang:monotonic_time(millisecond),
    ct:pal("call took ~b ms", [T1 - T0]),
    #{<<"emqx_mria_lag">> := Stats1} = Stats0,
    ?assert(lists:all(fun is_number/1, maps:values(Stats1)), #{stats => Stats1}),
    ok.

-doc """
Checks that `/prometheus/topic_metrics' renders one namespace scope across the cluster:
`node' returns the entry node's local counters, `all_nodes_aggregated' sums the per-node
counters into a single series, and `all_nodes_unaggregated' keeps one series per node.  The
same fixture also checks that a global actor's unscoped view adds the global-owned collection.
""".
t_topic_metrics_namespace_cluster(TCConfig) ->
    Nodes = mk_cluster(
        ?FUNCTION_NAME, #{n => 2, overrides => #{2 => #{role => replicant}}}, TCConfig
    ),
    [N1, N2] = Nodes,
    Ns = <<"ns1">>,
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, Ns)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"metric-g">>, <<"global/#">>, ?global_ns)),

    ok = ?ON(N1, publish_as(Ns, <<"tenant/a/1">>)),
    ok = ?ON(N1, publish_as(Ns, <<"tenant/a/1">>)),
    ok = ?ON(N1, publish_as(Ns, <<"global/1">>)),
    ok = ?ON(N2, publish_as(Ns, <<"tenant/a/1">>)),
    ok = ?ON(N2, publish_as(Ns, <<"tenant/a/1">>)),
    ok = ?ON(N2, publish_as(Ns, <<"tenant/a/1">>)),

    Metric = <<"emqx_topic_metric_messages_in_count">>,
    A = #{
        <<"name">> => <<"metric-a">>,
        <<"topic_filter">> => <<"tenant/a/#">>,
        <<"namespace">> => Ns
    },
    %% Only the `messages.in' family is asserted here; the API suite
    %% covers the full label set of every family.
    InFamily = fun(Res) -> maps:get(Metric, Res, #{}) end,

    %% `node': the counters of the node serving the request (the one with
    %% the dashboard listener), scoped to the requested namespace.
    {200, NodeRes} = get_topic_metrics(?PROM_DATA_MODE__NODE, #{ns => Ns}),
    ?assertEqual(#{A => 2}, InFamily(NodeRes)),
    ?assertEqual([Ns], family_namespaces(NodeRes, Metric)),

    %% `all_nodes_aggregated': both nodes' counters folded into one series.
    {200, AggRes} = get_topic_metrics(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, #{ns => Ns}),
    ?assertEqual(#{A => 5}, InFamily(AggRes)),

    %% `all_nodes_unaggregated': one series per node, each labelled with
    %% its own node.
    {200, UnaggRes} = get_topic_metrics(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, #{ns => Ns}),
    ?assertEqual(
        #{
            A#{<<"node">> => atom_to_binary(N1, utf8)} => 2,
            A#{<<"node">> => atom_to_binary(N2, utf8)} => 3
        },
        InFamily(UnaggRes)
    ),

    %% Without `ns', a global actor also sees the global-owned collection.
    G = #{<<"name">> => <<"metric-g">>, <<"topic_filter">> => <<"global/#">>},
    {200, AllRes} = get_topic_metrics(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, #{}),
    ?assertEqual(#{A => 5, G => 1}, InFamily(AllRes)),

    %% A node that does not implement the namespaced callback (an older
    %% release) is skipped: the answering node's data is still served.
    ok = ?ON(N1, begin
        ok = meck:new(emqx_prometheus_proto_v3, [passthrough, no_link]),
        ok = meck:expect(
            emqx_prometheus_proto_v3,
            raw_prom_data,
            fun(Nodes0, M, F, Args, _T) ->
                [
                    case N of
                        N1 -> {ok, apply(M, F, Args)};
                        _ -> {error, {exception, undef, [{M, F, Args, []}]}}
                    end
                 || N <- Nodes0
                ]
            end
        ),
        ok
    end),
    try
        {200, PartialRes} = get_topic_metrics(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, #{ns => Ns}),
        ?assertEqual(#{A => 2}, InFamily(PartialRes))
    after
        ok = ?ON(N1, meck:unload(emqx_prometheus_proto_v3))
    end,
    ok.

-doc """
Checks the namespace scope of `/prometheus/topic_metrics' on a real 2-node cluster with two
tenants: each tenant token sees only its own series in every mode, a tenant cannot address the
other namespace, and the global view keeps the two same-named collections apart.
""".
t_topic_metrics_tenant_cluster(TCConfig) ->
    Nodes = mk_cluster(
        ?FUNCTION_NAME,
        #{
            n => 2,
            overrides => #{2 => #{role => replicant}},
            extra_apps => [emqx_mt],
            prometheus_conf =>
                "prometheus { enable_basic_auth = true,"
                " namespaced_metrics_limiter.rate = infinity }"
        },
        TCConfig
    ),
    [N1, N2] = Nodes,
    Ns1 = <<"ns1">>,
    Ns2 = <<"ns2">>,
    ok = ?ON(N1, emqx_mt_config:create_managed_ns(Ns1)),
    ok = ?ON(N1, emqx_mt_config:create_managed_ns(Ns2)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"metric-a">>, <<"tenant/a/#">>, Ns1)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"same">>, <<"same/#">>, Ns1)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"metric-b">>, <<"tenant/b/#">>, Ns2)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"same">>, <<"same/#">>, Ns2)),
    ok = ?ON(N1, emqx_topic_metrics2:register(<<"metric-g">>, <<"global/#">>, ?global_ns)),

    %% ns1 publishes on both nodes, ns2 only on N2.
    ok = ?ON(N1, publish_as(Ns1, <<"tenant/a/1">>)),
    ok = ?ON(N1, publish_as(Ns1, <<"tenant/a/1">>)),
    ok = ?ON(N1, publish_as(Ns1, <<"same/1">>)),
    ok = ?ON(N1, publish_as(Ns1, <<"global/1">>)),
    ok = ?ON(N2, publish_as(Ns1, <<"tenant/a/1">>)),
    ok = ?ON(N2, publish_as(Ns1, <<"tenant/a/1">>)),
    ok = ?ON(N2, publish_as(Ns1, <<"tenant/a/1">>)),
    ok = ?ON(N2, publish_as(Ns2, <<"tenant/b/1">>)),
    ok = ?ON(N2, publish_as(Ns2, <<"same/1">>)),
    ok = ?ON(N2, publish_as(Ns2, <<"same/1">>)),

    %% The dashboard credentials come from the serving node's own config.
    {AdminUser, AdminPass} = ?ON(N1, {
        emqx_dashboard_admin:default_username(), emqx_dashboard_admin:default_password()
    }),
    GlobalAuth = bearer_auth_header(login_token(AdminUser, AdminPass)),
    Ns1Auth = tenant_auth_header(GlobalAuth, Ns1, <<"administrator">>),
    Ns2Auth = tenant_auth_header(GlobalAuth, Ns2, <<"administrator">>),

    Ns1Rows = [
        {<<"metric-a">>, <<"tenant/a/#">>, Ns1, 2, 3, 10},
        {<<"same">>, <<"same/#">>, Ns1, 1, 0, 6}
    ],
    Ns2Rows = [
        {<<"metric-b">>, <<"tenant/b/#">>, Ns2, 0, 1, 10},
        {<<"same">>, <<"same/#">>, Ns2, 0, 2, 6}
    ],
    GlobalRows = [{<<"metric-g">>, <<"global/#">>, undefined, 1, 0, 8}],

    lists:foreach(
        fun(Mode) ->
            ct:pal("mode ~s", [Mode]),
            Ns1Expected = expected_exposition(N1, N2, Mode, Ns1Rows),
            Ns2Expected = expected_exposition(N1, N2, Mode, Ns2Rows),
            AllExpected = expected_exposition(N1, N2, Mode, Ns1Rows ++ Ns2Rows ++ GlobalRows),

            %% Each tenant token sees exactly its own series.
            {200, Ns1Res} = get_topic_metrics(Mode, #{auth_header => Ns1Auth}),
            ?assertEqual(Ns1Expected, Ns1Res),
            {200, Ns2Res} = get_topic_metrics(Mode, #{auth_header => Ns2Auth}),
            ?assertEqual(Ns2Expected, Ns2Res),

            %% A tenant cannot name the other namespace.
            ?assertMatch(
                {403, _},
                get_topic_metrics(Mode, #{auth_header => Ns1Auth, ns => Ns2})
            ),

            %% The global view holds both tenants' series, including the two
            %% same-named collections, and can narrow to one namespace.
            {200, GlobalRes} = get_topic_metrics(Mode, #{auth_header => GlobalAuth}),
            ?assertEqual(AllExpected, GlobalRes),
            {200, Ns2OnlyRes} = get_topic_metrics(Mode, #{
                auth_header => GlobalAuth, ns => Ns2
            }),
            ?assertEqual(Ns2Expected, Ns2OnlyRes)
        end,
        ?PROM_DATA_MODES
    ),
    ok.
