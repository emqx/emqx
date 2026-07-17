%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_top_scanner_SUITE).

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

t_running_busy_and_cancel(_) ->
    Pid = spawn_waiter(),
    try
        insert_channel_infos(50, Pid),
        Collector = node(),
        Opts = scan_opts(#{
            collector => Collector,
            batch_size => 1,
            sleep_ms => 100
        }),
        ScanId = maps:get(scan_id, Opts),
        ?assertEqual({ok, accepted}, emqx_session_top_scanner:start_scan(Opts)),
        ?assertEqual(
            {error, {busy, Collector}},
            emqx_session_top_scanner:start_scan(scan_opts(#{}))
        ),
        ?assertEqual({ok, cancelled}, emqx_session_top_scanner:cancel(ScanId)),
        ?assertEqual({ok, cancelled}, emqx_session_top_scanner:cancel(ScanId))
    after
        stop_waiter(Pid)
    end.

t_completed_result_uses_scan_snapshot(_) ->
    Pid = spawn_waiter(),
    TestPid = self(),
    ok = meck:new(emqx_session_top_proto_v1, [passthrough, no_link]),
    ok = meck:expect(
        emqx_session_top_proto_v1,
        top_scan_result,
        fun(Collector, ScanId, Node, Result) ->
            TestPid ! {scan_result, Collector, ScanId, Node, Result},
            ok
        end
    ),
    try
        insert_channel_info(<<"c1">>, Pid, 10, 100, 1),
        Collector = node(),
        Opts = scan_opts(#{collector => Collector}),
        ScanId = maps:get(scan_id, Opts),
        ?assertEqual({ok, accepted}, emqx_session_top_scanner:start_scan(Opts)),
        receive
            {scan_result, Collector, ScanId, Node, {ok, [Row]}} when Node =:= node() ->
                ?assertMatch(
                    #{
                        clientid := <<"c1">>,
                        mqueue_length := 10,
                        total_payload_bytes := 100,
                        inflight_count := 1
                    },
                    Row
                )
        after 1000 ->
            error(scan_result_not_received)
        end
    after
        ok = meck:unload(emqx_session_top_proto_v1),
        stop_waiter(Pid)
    end.

t_collector_down_aborts_scan(_) ->
    Pid = spawn_waiter(),
    try
        insert_channel_infos(50, Pid),
        Opts = scan_opts(#{
            collector => 'down@127.0.0.1',
            batch_size => 1,
            sleep_ms => 100
        }),
        ?assertEqual({ok, accepted}, emqx_session_top_scanner:start_scan(Opts)),
        NextOpts = scan_opts(#{batch_size => 1, sleep_ms => 100}),
        ?assertEqual({ok, accepted}, wait_start_scan(NextOpts, 100)),
        ?assertEqual(
            {ok, cancelled},
            emqx_session_top_scanner:cancel(maps:get(scan_id, NextOpts))
        )
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

scan_opts(Overrides) ->
    maps:merge(
        #{
            scan_id => {?MODULE, erlang:unique_integer([positive])},
            collector => node(),
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

wait_start_scan(Opts, Attempts) when Attempts > 0 ->
    case emqx_session_top_scanner:start_scan(Opts) of
        {error, {busy, _Collector}} ->
            timer:sleep(10),
            wait_start_scan(Opts, Attempts - 1);
        Result ->
            Result
    end;
wait_start_scan(Opts, 0) ->
    emqx_session_top_scanner:start_scan(Opts).
