%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_tool_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_cm.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

suite() -> [{timetrap, {minutes, 2}}].

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    clear_table(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    clear_table().

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

clear_table() ->
    catch ets:delete_all_objects(?CHAN_INFO_TAB).

%% Insert a synthetic channel-info row the way emqx_cm:insert_channel_info
%% would: key {ClientId, ChanPid}, an info map, and a cached stats
%% proplist. The scan reads the stats proplist directly, so no live
%% channel process is required. A real (local, alive) pid is used so the
%% extras re-lookup in resolve_row works.
insert_session(ClientId, Stats) ->
    insert_session(ClientId, Stats, #{}).

insert_session(ClientId, Stats, Info) ->
    true = ets:insert(?CHAN_INFO_TAB, {{ClientId, self()}, Info, Stats}),
    ClientId.

clientids(Rows) ->
    [maps:get(clientid, R) || R <- Rows].

values(Rows) ->
    [maps:get(value, R) || R <- Rows].

%%--------------------------------------------------------------------
%% Single-node test cases
%%--------------------------------------------------------------------

-doc "top_by ranks sessions by the metric, highest first, limited to top_k.".
t_top_by_ranks_by_metric(_Config) ->
    insert_session(<<"c5">>, [{mqueue_len, 5}]),
    insert_session(<<"c3">>, [{mqueue_len, 3}]),
    insert_session(<<"c9">>, [{mqueue_len, 9}]),
    insert_session(<<"c1">>, [{mqueue_len, 1}]),
    insert_session(<<"c7">>, [{mqueue_len, 7}]),
    Rows = emqx_session_tool:top_by(mqueue_len, #{top_k => 3}),
    ?assertEqual([<<"c9">>, <<"c7">>, <<"c5">>], clientids(Rows)),
    ?assertEqual([9, 7, 5], values(Rows)),
    ?assertEqual([mqueue_len, mqueue_len, mqueue_len], [maps:get(metric, R) || R <- Rows]),
    ?assertEqual([node(), node(), node()], [maps:get(node, R) || R <- Rows]).

-doc "min_value excludes sessions whose metric is below the threshold (and 0 by default).".
t_min_value_filter(_Config) ->
    insert_session(<<"zero">>, [{mqueue_dropped, 0}]),
    insert_session(<<"one">>, [{mqueue_dropped, 1}]),
    insert_session(<<"two">>, [{mqueue_dropped, 2}]),
    %% Default min_value = 1 drops the zero-valued session.
    Default = emqx_session_tool:top_by(mqueue_dropped, #{}),
    ?assertEqual([<<"two">>, <<"one">>], clientids(Default)),
    %% Explicit min_value = 2 keeps only the >= 2 session.
    Filtered = emqx_session_tool:top_by(mqueue_dropped, #{min_value => 2}),
    ?assertEqual([<<"two">>], clientids(Filtered)).

-doc "top_k bounds the number of returned rows regardless of how many qualify.".
t_top_k_limits_result(_Config) ->
    lists:foreach(
        fun(N) ->
            insert_session(integer_to_binary(N), [{inflight_cnt, N}])
        end,
        lists:seq(1, 50)
    ),
    Rows = emqx_session_tool:top_by(inflight_cnt, #{top_k => 5}),
    ?assertEqual(5, length(Rows)),
    ?assertEqual([50, 49, 48, 47, 46], values(Rows)).

-doc "extra_keys attaches cached info fields (session/clientinfo/conninfo) to each row.".
t_extra_keys_populate_extras(_Config) ->
    Info = #{
        session => #{created_at => 1000},
        clientinfo => #{username => <<"alice">>},
        conninfo => #{}
    },
    insert_session(<<"c1">>, [{mqueue_len, 4}], Info),
    [Row] = emqx_session_tool:top_by(mqueue_len, #{
        extra_keys => [created_at, username, missing_key]
    }),
    ?assertEqual(
        #{created_at => 1000, username => <<"alice">>, missing_key => undefined},
        maps:get(extras, Row)
    ).

-doc "Without extra_keys the row carries no extras map.".
t_no_extra_keys_no_extras(_Config) ->
    insert_session(<<"c1">>, [{mqueue_len, 4}]),
    [Row] = emqx_session_tool:top_by(mqueue_len, #{}),
    ?assertNot(maps:is_key(extras, Row)).

-doc "An empty session set (or all below min_value) yields an empty result.".
t_empty_result(_Config) ->
    ?assertEqual([], emqx_session_tool:top_by(mqueue_len, #{})),
    insert_session(<<"c0">>, [{mqueue_len, 0}]),
    ?assertEqual([], emqx_session_tool:top_by(mqueue_len, #{})).

-doc "available_metrics/0 advertises the common session gauges and channel counters.".
t_available_metrics(_Config) ->
    Metrics = emqx_session_tool:available_metrics(),
    lists:foreach(
        fun(M) -> ?assert(lists:member(M, Metrics)) end,
        [mqueue_len, mqueue_dropped, inflight_cnt, recv_msg, send_msg]
    ).

-doc "scan rejects a metric that is not advertised by available_metrics/0.".
t_unsupported_metric_errors(_Config) ->
    ?assertError(
        {unsupported_metric, not_a_metric, _},
        emqx_session_tool:top_by(not_a_metric, #{})
    ).

-doc "scan requires the metric option to be present.".
t_missing_metric_errors(_Config) ->
    ?assertError(
        {missing_required_option, metric},
        emqx_session_tool:scan(#{top_k => 5})
    ).

-doc "chunk and sleep_ms options are accepted and do not change the result.".
t_chunk_and_sleep_options(_Config) ->
    lists:foreach(
        fun(N) -> insert_session(integer_to_binary(N), [{mqueue_len, N}]) end,
        lists:seq(1, 10)
    ),
    Rows = emqx_session_tool:top_by(mqueue_len, #{top_k => 3, chunk => 2, sleep_ms => 0}),
    ?assertEqual([10, 9, 8], values(Rows)).

-doc "The incremental scan_acc engine drives a scan batch-by-batch and matches scan/1.".
t_incremental_scan_acc(_Config) ->
    lists:foreach(
        fun(N) -> insert_session(integer_to_binary(N), [{mqueue_len, N}]) end,
        lists:seq(1, 10)
    ),
    Opts = #{metric => mqueue_len, top_k => 3, chunk => 4},
    Acc0 = emqx_session_tool:scan_acc_new(Opts),
    %% A partial result is available before the scan completes (abort-safe).
    {_, AccMid} = emqx_session_tool:scan_acc(Acc0),
    ?assert(length(emqx_session_tool:scan_acc_rows(AccMid)) =< 3),
    %% Driving to completion matches the all-at-once scan/1.
    AccDone = drive_scan(Acc0),
    ?assertEqual([10, 9, 8], values(emqx_session_tool:scan_acc_rows(AccDone))),
    ?assertEqual(emqx_session_tool:scan(Opts), emqx_session_tool:scan_acc_rows(AccDone)),
    %% scan_acc on a finished accumulator is idempotent.
    ?assertMatch({done, _}, emqx_session_tool:scan_acc(AccDone)).

drive_scan(Acc) ->
    case emqx_session_tool:scan_acc(Acc) of
        {done, Acc1} -> Acc1;
        {continue, Acc1} -> drive_scan(Acc1)
    end.

-doc "Real connected clients are scannable through the cached stats (inflight_max).".
t_real_clients_scannable(_Config) ->
    clear_table(),
    ClientIds = [<<"real-1">>, <<"real-2">>, <<"real-3">>],
    Clients = [
        begin
            {ok, C} = emqtt:start_link([{clientid, Id}]),
            {ok, _} = emqtt:connect(C),
            C
        end
     || Id <- ClientIds
    ],
    try
        Rows = emqx_session_tool:top_by(inflight_max, #{top_k => 10}),
        Found = clientids(Rows),
        lists:foreach(fun(Id) -> ?assert(lists:member(Id, Found)) end, ClientIds),
        %% inflight_max defaults to 32 and is present in the registration
        %% stats snapshot.
        lists:foreach(fun(V) -> ?assert(V > 0) end, values(Rows))
    after
        lists:foreach(fun(C) -> catch emqtt:stop(C) end, Clients)
    end.

%%--------------------------------------------------------------------
%% Cluster test case
%%--------------------------------------------------------------------

-doc "cluster_top_by merges per-node top-K heaps into a cluster-wide top-K.".
t_cluster_top_by(Config) ->
    Cluster = emqx_cth_cluster:mk_nodespecs(
        [
            {emqx_session_tool_c1, #{role => core, apps => [emqx]}},
            {emqx_session_tool_c2, #{role => core, apps => [emqx]}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
    ),
    [N1, N2] = Nodes = emqx_cth_cluster:start(Cluster),
    try
        ok = insert_remote(N1, <<"n1-a">>, 10),
        ok = insert_remote(N1, <<"n1-b">>, 20),
        ok = insert_remote(N2, <<"n2-a">>, 15),
        ok = insert_remote(N2, <<"n2-b">>, 5),
        Rows = erpc:call(N1, emqx_session_tool, cluster_top_by, [
            mqueue_len, #{top_k => 3, rpc_timeout => 15_000}
        ]),
        ?assertEqual([<<"n1-b">>, <<"n2-a">>, <<"n1-a">>], clientids(Rows)),
        ?assertEqual([20, 15, 10], values(Rows)),
        %% Each row is tagged with the node that owns the session.
        ByClient = maps:from_list([{maps:get(clientid, R), maps:get(node, R)} || R <- Rows]),
        ?assertEqual(N1, maps:get(<<"n1-b">>, ByClient)),
        ?assertEqual(N2, maps:get(<<"n2-a">>, ByClient)),
        ?assertEqual(N1, maps:get(<<"n1-a">>, ByClient))
    after
        emqx_cth_cluster:stop(Nodes)
    end.

insert_remote(Node, ClientId, MqueueLen) ->
    Row = {{ClientId, self()}, #{}, [{mqueue_len, MqueueLen}]},
    true = erpc:call(Node, ets, insert, [?CHAN_INFO_TAB, Row]),
    ok.
