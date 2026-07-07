%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

t_a_top_status_idle(_) ->
    with_session_buffer_mon(fun() ->
        ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
    end).

t_top_status_running_and_busy(Config) ->
    with_session_buffer_mon(
        fun() ->
            OutFile = filename:join(?config(priv_dir, Config), "session-top-running.csv"),
            _ = file:delete(OutFile),
            with_chan_info_table(
                fun() ->
                    Pid = spawn_waiter(),
                    try
                        insert_channel_infos(50, Pid),
                        Opts = #{
                            count => 1,
                            sort => total_payload_bytes,
                            out => OutFile,
                            batch_size => 1,
                            sleep_ms => 100
                        },
                        {ok, ScanId} = emqx_session_buffer_mon:run_top(Opts),
                        Node = node(),
                        ?assertMatch(
                            #{
                                status := running,
                                role := collector,
                                scan_id := ScanId,
                                out := OutFile,
                                count := 1,
                                sort := total_payload_bytes,
                                batch_size := 1,
                                sleep_ms := 100,
                                started_at := _,
                                initiator := Node,
                                collector := Node,
                                progress := #{nodes_total := 1, nodes_done := 0}
                            },
                            emqx_session_buffer_mon:top_status()
                        ),
                        ?assertEqual(
                            {error, busy},
                            emqx_session_buffer_mon:run_top(#{
                                count => 1,
                                sort => mqueue_length,
                                out => OutFile
                            })
                        ),
                        ?assertEqual({ok, cancelled}, emqx_session_buffer_mon:cancel_top())
                    after
                        stop_waiter(Pid),
                        _ = file:delete(OutFile)
                    end
                end
            )
        end
    ).

t_top_cancel_running_scan(Config) ->
    with_session_buffer_mon(
        fun() ->
            OutFile = filename:join(?config(priv_dir, Config), "session-top-cancel.csv"),
            _ = file:delete(OutFile),
            with_chan_info_table(
                fun() ->
                    Pid = spawn_waiter(),
                    try
                        insert_channel_infos(50, Pid),
                        {ok, _Pid} = emqx_session_buffer_mon:run_top(#{
                            count => 1,
                            sort => total_payload_bytes,
                            out => OutFile,
                            batch_size => 1,
                            sleep_ms => 100
                        }),
                        ?assertEqual({ok, cancelled}, emqx_session_buffer_mon:cancel_top()),
                        ?assertMatch(
                            #{
                                status := cancelled,
                                role := collector,
                                out := OutFile,
                                reason := cancelled
                            },
                            wait_top_status(cancelled, 100)
                        ),
                        ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
                    after
                        stop_waiter(Pid),
                        _ = file:delete(OutFile)
                    end
                end
            )
        end
    ).

t_top_cancel_remote_running_scan(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-cancel-remote.csv"),
        _ = file:delete(OutFile),
        RemoteNode = 'remote@127.0.0.3',
        try
            with_chan_info_table(fun() ->
                Pid = spawn_waiter(),
                try
                    insert_channel_infos(50, Pid),
                    with_remote_top_scan_node(RemoteNode, fun() ->
                        {ok, _ScanId} = emqx_session_buffer_mon:run_top(#{
                            count => 1,
                            sort => total_payload_bytes,
                            out => OutFile,
                            batch_size => 1,
                            sleep_ms => 100
                        }),
                        ?assertEqual({ok, cancelled}, emqx_session_buffer_mon:cancel_top()),
                        receive
                            {top_scan_cancelled, [RemoteNode], 5000} -> ok
                        after 1000 ->
                            error(remote_top_scan_not_cancelled)
                        end
                    end)
                after
                    stop_waiter(Pid)
                end
            end)
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_completed_after_remote_timeout(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-remote-timeout.csv"),
        _ = file:delete(OutFile),
        RemoteNode = 'remote@127.0.0.4',
        try
            with_chan_info_table(fun() ->
                with_remote_top_scan_node(RemoteNode, fun() ->
                    {ok, ScanId} = emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile
                    }),
                    _ = wait_top_progress_nodes_done(1, 100),
                    whereis(emqx_session_buffer_mon) ! {top_scan_timeout, ScanId},
                    ?assertMatch(
                        #{
                            status := completed,
                            out := OutFile,
                            rows := 0,
                            partial := true,
                            bad_replies := [{RemoteNode, timeout}]
                        },
                        wait_top_status(completed, 100)
                    )
                end)
            end)
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_worker_status(_) ->
    with_session_buffer_mon(fun() ->
        with_chan_info_table(fun() ->
            Pid = spawn_waiter(),
            try
                insert_channel_info(<<"worker-c1">>, Pid, 1, 10, 0),
                ScanId = {?FUNCTION_NAME, make_ref()},
                Req = local_top_opts(#{
                    scan_id => ScanId,
                    collector => node(),
                    count => 1,
                    sort => total_payload_bytes,
                    batch_size => 1,
                    sleep_ms => 100
                }),
                Node = node(),
                ?assertEqual({ok, accepted}, emqx_session_buffer_mon:start_top_scan(Req)),
                ?assertMatch(
                    #{
                        status := running,
                        role := worker,
                        scan_id := ScanId,
                        collector := Node,
                        progress := #{batches_done := _}
                    },
                    emqx_session_buffer_mon:top_status()
                ),
                ?assertEqual({ok, cancelled}, emqx_session_buffer_mon:cancel_top())
            after
                stop_waiter(Pid)
            end
        end)
    end).

t_top_status_completed(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-completed.csv"),
        _ = file:delete(OutFile),
        with_chan_info_table(fun() ->
            Pid = spawn_waiter(),
            try
                insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
                ?assertMatch(
                    {ok, _ScanId},
                    emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile
                    })
                ),
                ?assertMatch(
                    #{status := completed, role := collector, out := OutFile, rows := 1},
                    wait_top_status(completed, 100)
                ),
                ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
            after
                stop_waiter(Pid),
                _ = file:delete(OutFile)
            end
        end)
    end).

t_top_status_completed_after_session_tool_scan(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-completed-local.csv"),
        _ = file:delete(OutFile),
        with_chan_info_table(fun() ->
            Pid = spawn_waiter(),
            try
                insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
                with_cluster_top_mock(
                    fun(_Metric, _Opts) -> error(cluster_top_by_should_not_be_called) end,
                    fun() ->
                        ?assertMatch(
                            {ok, _ScanId},
                            emqx_session_buffer_mon:run_top(#{
                                count => 1,
                                sort => total_payload_bytes,
                                out => OutFile,
                                batch_size => 1,
                                sleep_ms => 0
                            })
                        ),
                        ?assertMatch(
                            #{status := completed, out := OutFile, rows := 1},
                            wait_top_status(completed, 100)
                        )
                    end
                )
            after
                stop_waiter(Pid),
                _ = file:delete(OutFile)
            end
        end)
    end).

t_top_status_completed_with_legacy_session_tool_row(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-legacy-row.csv"),
        _ = file:delete(OutFile),
        RemoteNode = 'remote@127.0.0.1',
        LegacyRow = #{
            clientid => <<"legacy-c1">>,
            node => RemoteNode,
            metric => mqueue_len,
            value => 10
        },
        try
            with_chan_info_table(fun() ->
                with_remote_top_scan_node(RemoteNode, fun() ->
                    {ok, ScanId} = emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => mqueue_length,
                        out => OutFile
                    }),
                    ok = emqx_session_buffer_mon:top_scan_result(
                        ScanId, RemoteNode, {ok, [LegacyRow]}
                    ),
                    ?assertMatch(
                        #{status := completed, out := OutFile, rows := 1},
                        wait_top_status(completed, 100)
                    ),
                    ?assertEqual(
                        {ok,
                            <<"clientid,node,mqueue_length,total_payload_bytes,inflight_count\n",
                                "legacy-c1,remote@127.0.0.1,10,0,0\n">>},
                        file:read_file(OutFile)
                    )
                end)
            end)
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_completed_with_empty_result(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-empty-result.csv"),
        _ = file:delete(OutFile),
        try
            with_chan_info_table(fun() ->
                ?assertMatch(
                    {ok, _ScanId},
                    emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile
                    })
                ),
                ?assertMatch(
                    #{status := completed, out := OutFile, rows := 0},
                    wait_top_status(completed, 100)
                )
            end)
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_failed_on_write_error(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join([?config(priv_dir, Config), "missing-dir", "session-top.csv"]),
        with_chan_info_table(fun() ->
            Pid = spawn_waiter(),
            try
                insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
                ?assertMatch(
                    {ok, _ScanId},
                    emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile
                    })
                ),
                ?assertMatch(
                    #{status := failed, out := OutFile, reason := enoent},
                    wait_top_status(failed, 100)
                ),
                ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
            after
                stop_waiter(Pid)
            end
        end)
    end).

t_top_status_completed_with_remote_problem(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-remote-problem.csv"),
        _ = file:delete(OutFile),
        RemoteNode = 'remote@127.0.0.2',
        try
            with_chan_info_table(fun() ->
                with_remote_top_scan_node(RemoteNode, fun() ->
                    {ok, ScanId} = emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile
                    }),
                    ok = emqx_session_buffer_mon:top_scan_result(
                        ScanId, RemoteNode, {error, timeout}
                    ),
                    ?assertMatch(
                        #{
                            status := completed,
                            out := OutFile,
                            rows := 0,
                            partial := true,
                            bad_replies := [{RemoteNode, timeout}]
                        },
                        wait_top_status(completed, 100)
                    )
                end)
            end)
        after
            _ = file:delete(OutFile)
        end
    end).

t_scan_tool_wrapper_top_by_total_payload_bytes(_) ->
    with_chan_info_table(fun() ->
        Pid1 = spawn_waiter(),
        Pid2 = spawn_waiter(),
        Pid3 = spawn_waiter(),
        try
            insert_channel_info(<<"c1">>, Pid1, 10, 100, 1),
            insert_channel_info(<<"c2">>, Pid2, 5, 300, 2),
            insert_channel_info(<<"c3">>, Pid3, 20, 200, 3),
            ?assertMatch(
                [
                    #{clientid := <<"c2">>, total_payload_bytes := 300},
                    #{clientid := <<"c3">>, total_payload_bytes := 200}
                ],
                emqx_session_buffer_mon:scan_local(2, total_payload_bytes)
            )
        after
            stop_waiter(Pid1),
            stop_waiter(Pid2),
            stop_waiter(Pid3)
        end
    end).

t_local_top_scans_without_server(_) ->
    without_session_buffer_mon(fun() ->
        with_chan_info_table(fun() ->
            Pid = spawn_waiter(),
            try
                insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
                ?assertMatch(
                    [#{clientid := <<"c1">>, total_payload_bytes := 100}],
                    emqx_session_buffer_mon:local_top(1, total_payload_bytes)
                )
            after
                stop_waiter(Pid)
            end
        end)
    end).

t_local_top_wrapper_does_not_update_job_status(_) ->
    with_session_buffer_mon(fun() ->
        with_chan_info_table(fun() ->
            Pid1 = spawn_waiter(),
            Pid2 = spawn_waiter(),
            try
                insert_channel_info(<<"c1">>, Pid1, 10, 100, 1),
                insert_channel_info(<<"c2">>, Pid2, 20, 200, 2),
                Opts = local_top_opts(#{batch_size => 1, sleep_ms => 100}),
                ?assertMatch(
                    [#{clientid := <<"c2">>, total_payload_bytes := 200}],
                    emqx_session_buffer_mon:local_top(Opts)
                ),
                ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
            after
                stop_waiter(Pid1),
                stop_waiter(Pid2)
            end
        end)
    end).

t_scan_tool_wrapper_top_by_mqueue_length(_) ->
    with_chan_info_table(fun() ->
        Pid1 = spawn_waiter(),
        Pid2 = spawn_waiter(),
        Pid3 = spawn_waiter(),
        try
            insert_channel_info(<<"c1">>, Pid1, 10, 100, 1),
            insert_channel_info(<<"c2">>, Pid2, 5, 300, 2),
            insert_channel_info(<<"c3">>, Pid3, 20, 200, 3),
            ?assertMatch(
                [
                    #{clientid := <<"c3">>, mqueue_length := 20},
                    #{clientid := <<"c1">>, mqueue_length := 10}
                ],
                emqx_session_buffer_mon:scan_local(2, mqueue_length)
            )
        after
            stop_waiter(Pid1),
            stop_waiter(Pid2),
            stop_waiter(Pid3)
        end
    end).

t_scan_tool_wrapper_top_with_equal_values(_) ->
    with_chan_info_table(fun() ->
        Pid1 = spawn_waiter(),
        Pid2 = spawn_waiter(),
        Pid3 = spawn_waiter(),
        try
            insert_channel_info(<<"c1">>, Pid1, 10, 100, 1),
            insert_channel_info(<<"c2">>, Pid2, 10, 100, 1),
            insert_channel_info(<<"c3">>, Pid3, 10, 100, 1),
            ?assertMatch(
                [
                    #{clientid := <<"c3">>, mqueue_length := 10},
                    #{clientid := <<"c2">>, mqueue_length := 10}
                ],
                emqx_session_buffer_mon:scan_local(2, mqueue_length)
            )
        after
            stop_waiter(Pid1),
            stop_waiter(Pid2),
            stop_waiter(Pid3)
        end
    end).

t_csv_rows(_) ->
    Row = #{
        clientid => <<"c,1\"">>,
        node => node(),
        mqueue_length => 2,
        total_payload_bytes => 10,
        inflight_count => 1
    },
    ?assertEqual(
        iolist_to_binary([
            <<"\"c,1\"\"\",">>,
            atom_to_binary(node(), utf8),
            <<",2,10,1\n">>
        ]),
        iolist_to_binary(emqx_session_buffer_mon:csv_rows([Row]))
    ).

t_write_csv_reports_write_error(Config) ->
    OutFile = filename:join([?config(priv_dir, Config), "missing-dir", "session-top.csv"]),
    ?assertEqual({error, enoent}, emqx_session_buffer_mon:write_csv(OutFile, [])).

t_write_csv_does_not_overwrite_existing_file(Config) ->
    OutFile = filename:join(?config(priv_dir, Config), "session-top-existing.csv"),
    ok = file:write_file(OutFile, <<"keep">>),
    try
        ?assertEqual({error, eexist}, emqx_session_buffer_mon:write_csv(OutFile, [])),
        ?assertEqual({ok, <<"keep">>}, file:read_file(OutFile))
    after
        _ = file:delete(OutFile)
    end.

t_maybe_log_uses_supported_throttle_key(_) ->
    try
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => 1}),
        ?assertEqual(
            ok,
            emqx_session_buffer_mon:maybe_log(
                <<"c1">>,
                self(),
                [{mqueue_len, 1}, {inflight_cnt, 1}, {total_payload_bytes, 2}]
            )
        )
    after
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => 0})
    end.

t_maybe_log_skips_stats_when_disabled(_) ->
    try
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => 0}),
        ?assertEqual(
            ok,
            emqx_session_buffer_mon:maybe_log(<<"c1">>, self(), invalid_stats)
        )
    after
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => 0})
    end.

t_update_normalizes_missing_upgrade_conf(_) ->
    try
        ok = emqx_session_buffer_mon:update(undefined),
        ?assertEqual(
            #{buffered_payload_high_watermark => 0},
            persistent_term:get({emqx_session_buffer_mon, conf})
        ),
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => undefined}),
        ?assertEqual(
            #{buffered_payload_high_watermark => 0},
            persistent_term:get({emqx_session_buffer_mon, conf})
        ),
        ?assertEqual(
            ok,
            emqx_session_buffer_mon:maybe_log(
                <<"c1">>,
                self(),
                [{mqueue_len, 1}, {inflight_cnt, 1}, {total_payload_bytes, 2}]
            )
        )
    after
        ok = emqx_session_buffer_mon:update(#{buffered_payload_high_watermark => 0})
    end.

with_session_buffer_mon(Fun) ->
    case whereis(emqx_session_buffer_mon) of
        undefined ->
            {ok, Pid} = emqx_session_buffer_mon:start_link(),
            try
                Fun()
            after
                ok = gen_server:stop(Pid)
            end;
        _Pid ->
            Fun()
    end.

without_session_buffer_mon(Fun) ->
    case whereis(emqx_session_buffer_mon) of
        undefined ->
            Fun();
        Pid ->
            ok = gen_server:stop(Pid),
            Fun()
    end.

with_chan_info_table(Fun) ->
    case ets:info(?CHAN_INFO_TAB) of
        undefined ->
            _ = ets:new(?CHAN_INFO_TAB, [named_table, public, ordered_set]),
            try
                Fun()
            after
                ets:delete(?CHAN_INFO_TAB)
            end;
        _ ->
            Saved = ets:tab2list(?CHAN_INFO_TAB),
            ets:delete_all_objects(?CHAN_INFO_TAB),
            try
                Fun()
            after
                ets:delete_all_objects(?CHAN_INFO_TAB),
                _ = [ets:insert(?CHAN_INFO_TAB, Row) || Row <- Saved]
            end
    end.

insert_channel_info(ClientId, Pid, MqueueLength, TotalPayloadBytes, InflightCount) ->
    ets:insert(
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
    ).

insert_channel_infos(Count, Pid) ->
    lists:foreach(
        fun(N) ->
            ClientId = <<"c", (integer_to_binary(N))/binary>>,
            insert_channel_info(ClientId, Pid, N, N, 0)
        end,
        lists:seq(1, Count)
    ).

with_cluster_top_mock(ClusterTopFun, TestFun) ->
    ok = meck:new(emqx_session_tool, [passthrough, no_link]),
    try
        ok = meck:expect(emqx_session_tool, cluster_top_by, ClusterTopFun),
        TestFun()
    after
        ok = meck:unload(emqx_session_tool)
    end.

with_remote_top_scan_node(RemoteNode, TestFun) ->
    TestPid = self(),
    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = meck:new(emqx_session_buffer_mon_proto_v1, [passthrough, no_link]),
    try
        ok = meck:expect(
            emqx_bpapi,
            nodes_supporting_bpapi_version,
            fun
                (emqx_session_buffer_mon, 1) -> [node(), RemoteNode];
                (Name, Vsn) -> meck:passthrough([Name, Vsn])
            end
        ),
        ok = meck:expect(
            emqx_session_buffer_mon_proto_v1,
            start_top_scan,
            fun(Nodes, Req, Timeout) ->
                TestPid ! {top_scan_started, Nodes, Req, Timeout},
                [{ok, {ok, accepted}} || _ <- Nodes]
            end
        ),
        ok = meck:expect(
            emqx_session_buffer_mon_proto_v1,
            cancel_top_scan,
            fun(Nodes, _ScanId, Timeout) ->
                TestPid ! {top_scan_cancelled, Nodes, Timeout},
                [{ok, {ok, cancelled}} || _ <- Nodes]
            end
        ),
        TestFun(),
        receive
            {top_scan_started, [RemoteNode], _Req, 5000} -> ok
        after 0 ->
            error(remote_top_scan_not_started)
        end
    after
        ok = meck:unload(emqx_session_buffer_mon_proto_v1),
        ok = meck:unload(emqx_bpapi)
    end.

wait_top_progress_nodes_done(Expected, Attempts) when Attempts > 0 ->
    Status = emqx_session_buffer_mon:top_status(),
    case Status of
        #{status := running, progress := #{nodes_done := Expected}} ->
            Status;
        _ ->
            timer:sleep(10),
            wait_top_progress_nodes_done(Expected, Attempts - 1)
    end;
wait_top_progress_nodes_done(_Expected, 0) ->
    emqx_session_buffer_mon:top_status().

wait_top_status(Expected, Attempts) when Attempts > 0 ->
    Status = emqx_session_buffer_mon:top_status(),
    case maps:get(status, Status) of
        Expected ->
            Status;
        _ ->
            timer:sleep(10),
            wait_top_status(Expected, Attempts - 1)
    end;
wait_top_status(_Expected, 0) ->
    emqx_session_buffer_mon:top_status().

local_top_opts(Overrides) ->
    maps:merge(
        #{
            scan_id => {?MODULE, erlang:unique_integer([positive])},
            initiator => node(),
            started_at => erlang:system_time(millisecond),
            collector => self(),
            count => 1,
            sort => total_payload_bytes,
            batch_size => 1000,
            sleep_ms => 0
        },
        Overrides
    ).

spawn_waiter() ->
    spawn(fun() ->
        receive
            stop -> ok
        end
    end).

stop_waiter(Pid) ->
    Pid ! stop.

session_tool_opts(Opts) ->
    #{
        metric => sort_metric(maps:get(sort, Opts)),
        top_k => maps:get(count, Opts),
        min_value => 0,
        chunk => maps:get(batch_size, Opts, 1000),
        sleep_ms => maps:get(sleep_ms, Opts, 1),
        extra_stats => [mqueue_len, total_payload_bytes, inflight_cnt]
    }.

sort_metric(mqueue_length) ->
    mqueue_len;
sort_metric(total_payload_bytes) ->
    total_payload_bytes.
