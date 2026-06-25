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
            TestPid = self(),
            with_cluster_top_mock(
                fun(_Metric, _Opts) ->
                    TestPid ! top_scan_started,
                    receive
                        finish_top_scan -> []
                    after 5000 ->
                        error(timeout)
                    end
                end,
                fun() ->
                    Opts = #{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile,
                        batch_size => 10,
                        sleep_ms => 0
                    },
                    {ok, Pid} = emqx_session_buffer_mon:run_top(Opts),
                    receive
                        top_scan_started -> ok
                    after 1000 ->
                        error(top_scan_not_started)
                    end,
                    Node = node(),
                    ?assertMatch(
                        #{
                            status := running,
                            pid := Pid,
                            out := OutFile,
                            count := 1,
                            sort := total_payload_bytes,
                            batch_size := 10,
                            sleep_ms := 0,
                            started_at := _,
                            initiator := Node,
                            progress := #{nodes_total := _, nodes_done := 0}
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
                    Pid ! finish_top_scan,
                    ?assertMatch(
                        #{status := completed, out := OutFile, rows := 0},
                        wait_top_status(completed, 100)
                    )
                end
            )
        end
    ).

t_top_cancel_running_scan(Config) ->
    with_session_buffer_mon(
        fun() ->
            OutFile = filename:join(?config(priv_dir, Config), "session-top-cancel.csv"),
            TestPid = self(),
            with_cluster_top_mock(
                fun(_Metric, _Opts) ->
                    TestPid ! top_scan_started,
                    receive
                        finish_top_scan -> []
                    after 5000 ->
                        error(timeout)
                    end
                end,
                fun() ->
                    {ok, _Pid} = emqx_session_buffer_mon:run_top(#{
                        count => 1,
                        sort => total_payload_bytes,
                        out => OutFile,
                        batch_size => 1,
                        sleep_ms => 100
                    }),
                    receive
                        top_scan_started -> ok
                    after 1000 ->
                        error(top_scan_not_started)
                    end,
                    ?assertEqual({ok, cancelled}, emqx_session_buffer_mon:cancel_top()),
                    ?assertMatch(
                        #{status := cancelled, out := OutFile, reason := cancelled},
                        wait_top_status(cancelled, 100)
                    ),
                    ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
                end
            )
        end
    ).

t_top_status_completed(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-completed.csv"),
        _ = file:delete(OutFile),
        Row = top_row(<<"c1">>, 10, 100, 1),
        try
            with_cluster_top_mock(
                fun(_Metric, _Opts) -> [Row] end,
                fun() ->
                    ?assertMatch(
                        {ok, _Pid},
                        emqx_session_buffer_mon:run_top(#{
                            count => 1,
                            sort => total_payload_bytes,
                            out => OutFile
                        })
                    ),
                    ?assertMatch(
                        #{status := completed, out := OutFile, rows := 1},
                        wait_top_status(completed, 100)
                    ),
                    ?assertEqual(#{status => idle}, emqx_session_buffer_mon:top_status())
                end
            )
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_completed_after_session_tool_scan(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-completed-local.csv"),
        _ = file:delete(OutFile),
        TestPid = self(),
        Row = top_row(<<"c1">>, 10, 100, 1),
        try
            with_cluster_top_mock(
                fun(total_payload_bytes, Opts) ->
                    TestPid ! {cluster_top_called, Opts},
                    [Row]
                end,
                fun() ->
                    ?assertMatch(
                        {ok, _Pid},
                        emqx_session_buffer_mon:run_top(#{
                            count => 1,
                            sort => total_payload_bytes,
                            out => OutFile,
                            batch_size => 1,
                            sleep_ms => 0
                        })
                    ),
                    receive
                        {cluster_top_called, #{
                            top_k := 1,
                            min_value := 0,
                            chunk := 1,
                            sleep_ms := 0,
                            extra_stats := [mqueue_len, total_payload_bytes, inflight_cnt],
                            rpc_timeout := 300000
                        }} ->
                            ok
                    after 1000 ->
                        error(cluster_top_not_called)
                    end,
                    ?assertMatch(
                        #{status := completed, out := OutFile, rows := 1},
                        wait_top_status(completed, 100)
                    )
                end
            )
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_completed_with_legacy_session_tool_row(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-legacy-row.csv"),
        _ = file:delete(OutFile),
        LegacyRow = #{
            clientid => <<"legacy-c1">>,
            node => node(),
            metric => mqueue_len,
            value => 10
        },
        try
            with_cluster_top_mock(
                fun(_Metric, _Opts) -> [LegacyRow] end,
                fun() ->
                    ?assertMatch(
                        {ok, _Pid},
                        emqx_session_buffer_mon:run_top(#{
                            count => 1,
                            sort => mqueue_length,
                            out => OutFile
                        })
                    ),
                    ?assertMatch(
                        #{status := completed, out := OutFile, rows := 1},
                        wait_top_status(completed, 100)
                    ),
                    ?assertEqual(
                        {ok,
                            <<"clientid,pid,node,mqueue_length,total_payload_bytes,inflight_count\n",
                                "legacy-c1,undefined,", (atom_to_binary(node()))/binary,
                                ",10,0,0\n">>},
                        file:read_file(OutFile)
                    )
                end
            )
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_follows_cluster_top_by_result(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join(?config(priv_dir, Config), "session-top-empty-cluster-result.csv"),
        _ = file:delete(OutFile),
        try
            with_cluster_top_mock(
                fun(_Metric, _Opts) -> [] end,
                fun() ->
                    ?assertMatch(
                        {ok, _Pid},
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
                end
            )
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_failed_on_write_error(Config) ->
    with_session_buffer_mon(fun() ->
        OutFile = filename:join([?config(priv_dir, Config), "missing-dir", "session-top.csv"]),
        Row = top_row(<<"c1">>, 10, 100, 1),
        with_cluster_top_mock(
            fun(_Metric, _Opts) -> [Row] end,
            fun() ->
                ?assertMatch(
                    {ok, _Pid},
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
            end
        )
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
        pid => self(),
        node => node(),
        mqueue_length => 2,
        total_payload_bytes => 10,
        inflight_count => 1
    },
    ?assertEqual(
        iolist_to_binary([
            <<"\"c,1\"\"\",">>,
            pid_to_list(self()),
            <<",">>,
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

top_row(ClientId, MqueueLength, TotalPayloadBytes, InflightCount) ->
    #{
        clientid => ClientId,
        pid => self(),
        node => node(),
        metric => total_payload_bytes,
        value => TotalPayloadBytes,
        extras => #{
            mqueue_len => MqueueLength,
            total_payload_bytes => TotalPayloadBytes,
            inflight_cnt => InflightCount
        }
    }.

with_cluster_top_mock(ClusterTopFun, TestFun) ->
    ok = meck:new(emqx_session_tool, [passthrough, no_link]),
    try
        ok = meck:expect(emqx_session_tool, cluster_top_by, ClusterTopFun),
        TestFun()
    after
        ok = meck:unload(emqx_session_tool)
    end.

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
