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
            with_top_scan_mocks(
                fun(_Nodes, _Count, _SortBy, _Timeout) ->
                    TestPid ! top_scan_started,
                    receive
                        finish_top_scan -> {[], []}
                    after 5000 ->
                        error(timeout)
                    end
                end,
                fun() ->
                    Opts = #{count => 1, sort => total_payload_bytes, out => OutFile},
                    {ok, Pid} = emqx_session_buffer_mon:run_top(Opts),
                    receive
                        top_scan_started -> ok
                    after 1000 ->
                        error(top_scan_not_started)
                    end,
                    ?assertEqual(
                        #{
                            status => running,
                            pid => Pid,
                            out => OutFile,
                            count => 1,
                            sort => total_payload_bytes
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
                        #{status := completed, out := OutFile, rows := 0, partial := false},
                        wait_top_status(completed, 100)
                    )
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
            with_top_scan_mocks(
                fun(_Nodes, _Count, _SortBy, _Timeout) -> {[[Row]], []} end,
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
                        #{status := completed, out := OutFile, rows := 1, partial := false},
                        wait_top_status(completed, 100)
                    )
                end
            )
        after
            _ = file:delete(OutFile)
        end
    end).

t_top_status_failed_on_write_error(_) ->
    with_session_buffer_mon(fun() ->
        case file:open("/dev/full", [write, raw, binary]) of
            {ok, IoDev} ->
                ok = file:close(IoDev),
                Row = top_row(<<"c1">>, 10, 100, 1),
                with_top_scan_mocks(
                    fun(_Nodes, _Count, _SortBy, _Timeout) -> {[[Row]], []} end,
                    fun() ->
                        ?assertMatch(
                            {ok, _Pid},
                            emqx_session_buffer_mon:run_top(#{
                                count => 1,
                                sort => total_payload_bytes,
                                out => "/dev/full"
                            })
                        ),
                        ?assertMatch(
                            #{status := failed, out := "/dev/full", reason := enospc},
                            wait_top_status(failed, 100)
                        )
                    end
                );
            {error, _} ->
                {skip, no_dev_full}
        end
    end).

t_scan_local_top_by_total_payload_bytes(_) ->
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

t_scan_local_top_by_mqueue_length(_) ->
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

t_scan_local_top_with_equal_values(_) ->
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

t_result_rows_keeps_bad_replies(_) ->
    Row = #{
        clientid => <<"c1">>,
        pid => self(),
        node => node(),
        mqueue_length => 1,
        total_payload_bytes => 10,
        inflight_count => 0
    },
    ?assertEqual(
        {[Row], [nodedown, unexpected]},
        emqx_session_buffer_mon:result_rows([[Row], {badrpc, nodedown}, unexpected])
    ).

t_write_csv_reports_write_error(_) ->
    case file:open("/dev/full", [write, raw, binary]) of
        {ok, IoDev} ->
            ok = file:close(IoDev),
            ?assertEqual({error, enospc}, emqx_session_buffer_mon:write_csv("/dev/full", []));
        {error, _} ->
            {skip, no_dev_full}
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
        mqueue_length => MqueueLength,
        total_payload_bytes => TotalPayloadBytes,
        inflight_count => InflightCount
    }.

with_top_scan_mocks(LocalTopFun, TestFun) ->
    ok = meck:new(emqx, [passthrough, no_link]),
    ok = meck:new(emqx_bpapi, [passthrough, no_link]),
    ok = meck:new(emqx_session_buffer_proto_v1, [passthrough, no_link]),
    try
        ok = meck:expect(emqx, running_nodes, fun() -> [node()] end),
        ok = meck:expect(
            emqx_bpapi,
            nodes_supporting_bpapi_version,
            fun(emqx_session_buffer, 1) -> [node()] end
        ),
        ok = meck:expect(emqx_session_buffer_proto_v1, local_top, LocalTopFun),
        TestFun()
    after
        ok = meck:unload(emqx_session_buffer_proto_v1),
        ok = meck:unload(emqx_bpapi),
        ok = meck:unload(emqx)
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

spawn_waiter() ->
    spawn(fun() ->
        receive
            stop -> ok
        end
    end).

stop_waiter(Pid) ->
    Pid ! stop.
