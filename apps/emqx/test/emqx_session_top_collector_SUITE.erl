%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_top_collector_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [{emqx, #{}}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_TestCase, Config) ->
    reset_services(),
    clear_table(),
    Config.

end_per_testcase(_TestCase, _Config) ->
    reset_services(),
    clear_table().

t_status_idle(_) ->
    ?assertEqual(#{status => idle}, emqx_session_top_collector:status()).

t_scanner_busy_reports_collector(_) ->
    OtherCollector = 'other@127.0.0.1',
    Pid = spawn_waiter(),
    try
        insert_channel_infos(50, Pid),
        ScanOpts = top_opts(#{
            scan_id => make_ref(),
            collector => OtherCollector,
            batch_size => 1,
            sleep_ms => 100
        }),
        {ok, accepted} = emqx_session_top_scanner:start_scan(ScanOpts),
        try
            ?assertEqual(
                {error, {busy, OtherCollector}},
                emqx_session_top_collector:run(top_opts(#{}), fun(_Rows) -> ok end)
            )
        after
            _ = emqx_session_top_scanner:cancel(maps:get(scan_id, ScanOpts))
        end
    after
        stop_waiter(Pid)
    end.

t_running_busy_and_cancel(_) ->
    Pid = spawn_waiter(),
    try
        insert_channel_infos(50, Pid),
        Opts = top_opts(#{batch_size => 1, sleep_ms => 100}),
        {ok, _ScanId} = emqx_session_top_collector:run(Opts, fun(_Rows) -> ok end),
        Status = emqx_session_top_collector:status(),
        ?assertMatch(
            #{
                status := running,
                cluster_nodes := 1,
                started_at := _
            },
            Status
        ),
        ?assertNot(maps:is_key(initiator, Status)),
        ?assertNot(maps:is_key(collector, Status)),
        ?assertNot(maps:is_key(role, Status)),
        ?assertNot(maps:is_key(out, Status)),
        ?assertEqual(
            {error, busy},
            emqx_session_top_collector:run(top_opts(#{}), fun(_Rows) -> ok end)
        ),
        ?assertEqual({ok, cancelled}, emqx_session_top_collector:cancel()),
        ?assertMatch(
            #{status := cancelled, reason := cancelled, started_at := _},
            emqx_session_top_collector:status()
        ),
        ?assertEqual(#{status => idle}, emqx_session_top_collector:status())
    after
        stop_waiter(Pid)
    end.

t_completed_calls_completion(_) ->
    Pid = spawn_waiter(),
    TestPid = self(),
    try
        insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
        Completion = fun(Rows) ->
            TestPid ! {completed_rows, Rows},
            ok
        end,
        {ok, _ScanId} = emqx_session_top_collector:run(top_opts(#{}), Completion),
        receive
            {completed_rows, [Row]} ->
                ?assertMatch(#{clientid := <<"c1">>, total_payload_bytes := 100}, Row)
        after 1000 ->
            error(completion_not_called)
        end,
        ?assertMatch(
            #{status := completed, rows := 1, started_at := _},
            wait_status(completed, 100)
        )
    after
        stop_waiter(Pid)
    end.

t_completion_error_sets_failed_status(_) ->
    {ok, _ScanId} = emqx_session_top_collector:run(
        top_opts(#{}),
        fun(_Rows) -> {error, enoent} end
    ),
    ?assertMatch(
        #{status := failed, reason := enoent, started_at := _},
        wait_status(failed, 100)
    ).

t_empty_result(_) ->
    TestPid = self(),
    {ok, _ScanId} = emqx_session_top_collector:run(
        top_opts(#{}),
        fun(Rows) ->
            TestPid ! {rows, Rows},
            ok
        end
    ),
    receive
        {rows, []} -> ok
    after 1000 ->
        error(empty_completion_not_called)
    end,
    ?assertMatch(#{status := completed, rows := 0}, wait_status(completed, 100)).

t_remote_timeout_is_reported(_) ->
    RemoteNode = 'remote@127.0.0.4',
    with_remote_node(RemoteNode, fun() ->
        {ok, ScanId} = emqx_session_top_collector:run(top_opts(#{}), fun(_Rows) -> ok end),
        ok = emqx_session_top_collector:top_scan_result(ScanId, node(), {ok, []}),
        whereis(emqx_session_top_collector) ! {top_scan_timeout, ScanId},
        ?assertMatch(
            #{
                status := completed,
                rows := 0,
                bad_replies := [{RemoteNode, timeout}]
            },
            wait_status(completed, 100)
        )
    end).

t_remote_error_is_reported(_) ->
    RemoteNode = 'remote@127.0.0.2',
    with_remote_node(RemoteNode, fun() ->
        {ok, ScanId} = emqx_session_top_collector:run(top_opts(#{}), fun(_Rows) -> ok end),
        ok = emqx_session_top_collector:top_scan_result(ScanId, RemoteNode, {error, timeout}),
        ?assertMatch(
            #{
                status := completed,
                rows := 0,
                bad_replies := [{RemoteNode, timeout}]
            },
            wait_status(completed, 100)
        )
    end).

t_legacy_remote_row_is_normalized(_) ->
    RemoteNode = 'remote@127.0.0.1',
    TestPid = self(),
    LegacyRow = #{
        clientid => <<"legacy-c1">>,
        node => RemoteNode,
        metric => mqueue_len,
        value => 10
    },
    with_remote_node(RemoteNode, fun() ->
        Completion = fun(Rows) ->
            TestPid ! {rows, Rows},
            ok
        end,
        {ok, ScanId} = emqx_session_top_collector:run(
            top_opts(#{sort => mqueue_length}), Completion
        ),
        ok = emqx_session_top_collector:top_scan_result(
            ScanId, RemoteNode, {ok, [LegacyRow]}
        ),
        receive
            {rows, [Row]} ->
                ?assertEqual(
                    #{
                        clientid => <<"legacy-c1">>,
                        node => RemoteNode,
                        mqueue_length => 10,
                        total_payload_bytes => 0,
                        inflight_count => 0
                    },
                    Row
                )
        after 1000 ->
            error(legacy_completion_not_called)
        end
    end).

t_cancel_remote_scan(_) ->
    RemoteNode = 'remote@127.0.0.3',
    with_remote_node(RemoteNode, fun() ->
        {ok, _ScanId} = emqx_session_top_collector:run(
            top_opts(#{batch_size => 1, sleep_ms => 100}),
            fun(_Rows) -> ok end
        ),
        ?assertEqual({ok, cancelled}, emqx_session_top_collector:cancel()),
        receive
            {top_scan_cancelled, [RemoteNode]} -> ok
        after 1000 ->
            error(remote_scan_not_cancelled)
        end
    end).

t_cancel_includes_failed_remote_start(_) ->
    RemoteNode = 'remote@127.0.0.5',
    Pid = spawn_waiter(),
    try
        insert_channel_infos(50, Pid),
        with_remote_node(RemoteNode, {error, timeout}, fun() ->
            {ok, _ScanId} = emqx_session_top_collector:run(
                top_opts(#{batch_size => 1, sleep_ms => 100}),
                fun(_Rows) -> ok end
            ),
            ?assertEqual({ok, cancelled}, emqx_session_top_collector:cancel()),
            receive
                {top_scan_cancelled, [RemoteNode]} -> ok
            after 1000 ->
                error(failed_remote_scan_not_cancelled)
            end
        end)
    after
        stop_waiter(Pid)
    end.

reset_services() ->
    _ = emqx_session_top_collector:cancel(),
    _ = emqx_session_top_collector:status(),
    ok = supervisor:terminate_child(emqx_sys_sup, emqx_session_top_scanner),
    {ok, _} = supervisor:restart_child(emqx_sys_sup, emqx_session_top_scanner),
    ok.

clear_table() ->
    catch ets:delete_all_objects(?CHAN_INFO_TAB),
    ok.

top_opts(Overrides) ->
    maps:merge(
        #{
            count => 1,
            sort => total_payload_bytes,
            batch_size => 1000,
            sleep_ms => 0
        },
        Overrides
    ).

with_remote_node(RemoteNode, TestFun) ->
    with_remote_node(RemoteNode, {ok, {ok, accepted}}, TestFun).

with_remote_node(RemoteNode, StartReply, TestFun) ->
    TestPid = self(),
    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = meck:new(emqx_session_top_proto_v1, [passthrough, no_link]),
    try
        ok = meck:expect(
            emqx_bpapi,
            nodes_supporting_bpapi_version,
            fun
                (emqx_session_top, 1) -> [node(), RemoteNode];
                (Name, Vsn) -> meck:passthrough([Name, Vsn])
            end
        ),
        ok = meck:expect(
            emqx_session_top_proto_v1,
            start_top_scan,
            fun(Nodes, _Req, Timeout) ->
                TestPid ! {top_scan_started, Nodes, Timeout},
                [StartReply || _ <- Nodes]
            end
        ),
        ok = meck:expect(
            emqx_session_top_proto_v1,
            cancel_top_scan,
            fun(Nodes, _ScanId) ->
                TestPid ! {top_scan_cancelled, Nodes},
                ok
            end
        ),
        TestFun(),
        receive
            {top_scan_started, [RemoteNode], 5000} -> ok
        after 0 ->
            error(remote_scan_not_started)
        end
    after
        ok = meck:unload(emqx_session_top_proto_v1),
        ok = meck:unload(emqx_bpapi)
    end.

wait_status(Expected, Attempts) when Attempts > 0 ->
    Status = emqx_session_top_collector:status(),
    case maps:get(status, Status) of
        Expected ->
            Status;
        _ ->
            timer:sleep(10),
            wait_status(Expected, Attempts - 1)
    end;
wait_status(_Expected, 0) ->
    emqx_session_top_collector:status().

insert_channel_info(ClientId, Pid, MqueueLength, TotalPayloadBytes, InflightCount) ->
    true = ets:insert(
        ?CHAN_INFO_TAB,
        {
            {ClientId, Pid},
            #{},
            [
                {mqueue_len, MqueueLength},
                {total_payload_bytes, TotalPayloadBytes},
                {inflight_cnt, InflightCount}
            ]
        }
    ),
    ok.

insert_channel_infos(Count, Pid) ->
    lists:foreach(
        fun(N) ->
            insert_channel_info(<<"c", (integer_to_binary(N))/binary>>, Pid, N, N, 0)
        end,
        lists:seq(1, Count)
    ).

spawn_waiter() ->
    spawn(fun() ->
        receive
            stop -> ok
        end
    end).

stop_waiter(Pid) ->
    Pid ! stop.
