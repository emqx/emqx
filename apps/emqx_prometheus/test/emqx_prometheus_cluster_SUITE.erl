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
-include_lib("emqx_prometheus/include/emqx_prometheus.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    AppSpecs0 = [
        emqx_conf,
        emqx_management,
        {emqx_prometheus, "prometheus { enable_basic_auth = false }"}
    ],
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

announce_prometheus_version(Node, Version) ->
    APIs0 = ?ON(Node, emqx_bpapi:supported_apis(Node)),
    APIs = [{emqx_prometheus, Version} | lists:keydelete(emqx_prometheus, 1, APIs0)],
    {atomic, ok} = ?ON(
        Node,
        mria:transaction(emqx_common_shard, fun emqx_bpapi:announce_fun/2, [Node, APIs])
    ),
    ok.

wait_prometheus_version(Nodes, Version) ->
    lists:foreach(
        fun(Node) ->
            ?retry(
                100,
                20,
                ?assertEqual(Version, ?ON(Node, emqx_bpapi:supported_version(emqx_prometheus)))
            )
        end,
        Nodes
    ).

%% Parse only the `emqx_client_accept_result` lines: `parse_prometheus/1`
%% cannot parse every line of the `node` mode output.
accept_result_points(Mode) ->
    QueryString = uri_string:compose_query([{"mode", atom_to_binary(Mode)}]),
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {200, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => [],
        query_params => QueryString,
        auth_header => {"no", "auth"}
    }),
    Lines = binary:split(iolist_to_binary(Response), <<"\n">>, [global, trim_all]),
    maps:from_list([
        {Labels, Value}
     || <<"emqx_client_accept_result{", _/binary>> = Line <- Lines,
        {_Name, Labels, Value} <- [parse_prometheus_line(Line)]
    ]).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

-doc """
While a node announces `emqx_prometheus` BPAPI version 3, as a node of an older
release does, nodes leave `emqx_client_accept_result` out of the data they send
to each other, so the older node can still aggregate it. The `node` mode keeps
the metric. The metric returns in all modes after every node supports version 4.
""".
t_listener_accept_result_mixed_version(TCConfig) ->
    [N1, N2] = Nodes = mk_cluster(?FUNCTION_NAME, #{n => 2}, TCConfig),
    ok = announce_prometheus_version(N2, 3),
    wait_prometheus_version(Nodes, 3),
    %% This is what a node of an older release receives.
    {_, #{emqx_client_data := ClientData}} = ?ON(
        N1, emqx_prometheus:fetch_from_local_node(?PROM_DATA_MODE__ALL_NODES_AGGREGATED)
    ),
    ?assertNot(is_map_key(emqx_client_accept_result, ClientData)),
    ?assertEqual(#{}, accept_result_points(?PROM_DATA_MODE__ALL_NODES_AGGREGATED)),
    ?assertEqual(#{}, accept_result_points(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED)),
    ?assertNotEqual(#{}, accept_result_points(?PROM_DATA_MODE__NODE)),
    ok = announce_prometheus_version(N2, 4),
    wait_prometheus_version(Nodes, 4),
    ?assertNotEqual(#{}, accept_result_points(?PROM_DATA_MODE__ALL_NODES_AGGREGATED)),
    ?assertNotEqual(#{}, accept_result_points(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED)),
    ok.

-doc """
The aggregated and unaggregated modes skip metrics that a node of a newer
release returns and this node does not know, instead of failing the scrape.
""".
t_aggregate_skips_unknown_metrics(TCConfig) ->
    [_N1, N2] = mk_cluster(?FUNCTION_NAME, #{n => 2}, TCConfig),
    ?ON(N2, begin
        ok = meck:new(emqx_prometheus, [passthrough, no_link]),
        ok = meck:expect(emqx_prometheus, fetch_from_local_node, fun(Mode) ->
            {Node, #{emqx_client_data := ClientData} = Data} = meck:passthrough([Mode]),
            {Node, Data#{
                emqx_future_data => #{emqx_future_metric => [{[], 1}]},
                emqx_client_data := ClientData#{emqx_client_future_metric => [{[], 1}]}
            }}
        end)
    end),
    {200, Aggregated} = get_prometheus_stats(?PROM_DATA_MODE__ALL_NODES_AGGREGATED),
    {200, Unaggregated} = get_prometheus_stats(?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED),
    ?assertNot(is_map_key(<<"emqx_client_future_metric">>, Aggregated)),
    ?assertNot(is_map_key(<<"emqx_client_future_metric">>, Unaggregated)),
    ok.

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
