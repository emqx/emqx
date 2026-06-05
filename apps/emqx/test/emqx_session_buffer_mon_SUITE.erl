%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_session_buffer_mon_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

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

spawn_waiter() ->
    spawn(fun() ->
        receive
            stop -> ok
        end
    end).

stop_waiter(Pid) ->
    Pid ! stop.
