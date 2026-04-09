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
        emqx_prometheus
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
        meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end)
    end),
    Nodes.

mk_node_name(TestCase, N) ->
    Name0 = iolist_to_binary([atom_to_binary(TestCase), "_", integer_to_binary(N)]),
    binary_to_atom(Name0).

get_prometheus_stats(Mode, Format) ->
    Headers =
        case Format of
            json -> [{"accept", "application/json"}];
            prometheus -> []
        end,
    QueryString = uri_string:compose_query([{"mode", atom_to_binary(Mode)}]),
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "stats"]),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => Headers,
        query_params => QueryString,
        auth_header => {"no", "auth"}
    }),
    case Format of
        json ->
            {Status, Response};
        prometheus when Status == 200 ->
            {Status, parse_prometheus(Response)};
        prometheus ->
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
    {200, Stats0} = get_prometheus_stats(?PROM_DATA_MODE__ALL_NODES_AGGREGATED, prometheus),
    T1 = erlang:monotonic_time(millisecond),
    ct:pal("call took ~b ms", [T1 - T0]),
    #{<<"emqx_mria_lag">> := Stats1} = Stats0,
    ?assert(lists:all(fun is_number/1, maps:values(Stats1)), #{stats => Stats1}),
    ok.
