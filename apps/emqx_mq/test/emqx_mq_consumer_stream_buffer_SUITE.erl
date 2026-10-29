%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_stream_buffer_SUITE).
-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_durable_storage/include/emqx_ds.hrl").
-include("../src/emqx_mq_internal.hrl").

-define(PAYLOAD_SIZE, 4).

all() -> emqx_common_test_helpers:all(?MODULE).

init_per_testcase(_, Config) ->
    meck:new(emqx_mq_message_db, [passthrough, no_link]),
    meck:expect(emqx_mq_message_db, suback, fun(_, _, _) -> ok end),
    Config.

end_per_testcase(_, _) ->
    meck:unload().

%% Check that partial and out-of-order acknowledgments report progress only for complete buffers.
t_ordered_retirement(_Config) ->
    SB0 = emqx_mq_consumer_stream_buffer:new(start, mq()),
    {undefined, SB1} = push(SB0, lower_end, [10, 20]),
    {undefined, SB2} = push(SB1, upper_end, [30, 40]),
    {undefined, SB3} = ack(SB2, 40),
    {undefined, SB4} = ack(SB3, 30),
    {undefined, SB5} = ack(SB4, 10),
    %% One acknowledgment retires both buffers and reports their merged progress
    {Progress, SB6} = ack(SB5, 20),
    ?assertEqual(#progress{boundary = 41, totals = totals(4)}, Progress),
    %% Repeated acknowledgments and empty batches report no progress
    {undefined, SB7} = ack(SB6, 20),
    ?assertMatch({undefined, _}, push(SB7, empty_end, [])).

%% Check that the progress of a retired lower buffer excludes unacknowledged upper records.
t_lower_boundary(_Config) ->
    SB0 = emqx_mq_consumer_stream_buffer:new(start, mq()),
    {undefined, SB1} = push(SB0, lower_end, [10, 20]),
    {undefined, SB2} = push(SB1, upper_end, [30]),
    {undefined, SB3} = ack(SB2, 10),
    {#progress{boundary = 21}, SB4} = ack(SB3, 20),
    ?assertMatch({#progress{boundary = 31}, _}, ack(SB4, 30)).

%% Check that acknowledging the lower buffer resumes a paused subscription once before the next batch.
t_pause_resume_on_ack(_Config) ->
    SB0 = emqx_mq_consumer_stream_buffer:new(start, mq()),
    {_, SB1} = push(SB0, lower_end, [10, 20], 1),
    {_, SB2} = push(SB1, upper_end, [30, 40], 2),
    ?assertMatch(#{paused := true}, emqx_mq_consumer_stream_buffer:inspect(SB2)),
    ?assertEqual(0, meck:num_calls(emqx_mq_message_db, suback, [mq(), handle, 2])),
    {_, SB3} = ack(SB2, 10),
    {_, SB4} = ack(SB3, 20),
    ?assertMatch(#{paused := false}, emqx_mq_consumer_stream_buffer:inspect(SB4)),
    ?assertEqual(1, meck:num_calls(emqx_mq_message_db, suback, [mq(), handle, 2])),
    {_, SB5} = push(SB4, next_end, [50], 3),
    ?assertMatch(#{paused := false}, emqx_mq_consumer_stream_buffer:inspect(SB5)),
    ?assertEqual(1, meck:num_calls(emqx_mq_message_db, suback, [mq(), handle, 2])),
    ?assertEqual(1, meck:num_calls(emqx_mq_message_db, suback, [mq(), handle, 3])).

%% Check that restoration waits for a new record when cleanup removed the last checkpoint record.
t_restoration_after_deletion(_Config) ->
    Progress = #{it => checkpoint, last_message_id => 20, unacked => [#{}]},
    SB0 = emqx_mq_consumer_stream_buffer:restore(Progress, mq()),
    {undefined, SB1} = push(SB0, empty_end, []),
    ?assertMatch(#{status := restoring}, emqx_mq_consumer_stream_buffer:inspect(SB1)),
    {undefined, SB2} = push(SB1, new_end, [30]),
    ?assertMatch(#{status := active}, emqx_mq_consumer_stream_buffer:inspect(SB2)),
    {Progress1, SB3} = ack(SB2, 30),
    ?assertEqual(#progress{boundary = 31, totals = undefined}, Progress1),
    ?assertEqual(
        [it, last_message_id, unacked],
        lists:sort(maps:keys(emqx_mq_consumer_stream_buffer:progress(SB3)))
    ).

%% Check that an empty batch cannot complete restoration even when the checkpoint timestamp is zero.
t_empty_restore_boundary(_Config) ->
    Progress = #{it => checkpoint, last_message_id => 0, unacked => [#{}]},
    SB0 = emqx_mq_consumer_stream_buffer:restore(Progress, mq()),
    {undefined, SB1} = push(SB0, empty_end, []),
    ?assertMatch(#{status := restoring}, emqx_mq_consumer_stream_buffer:inspect(SB1)).

%% Check that a restored buffer reports progress without totals only after its records are read.
t_restored_acknowledgments(_Config) ->
    Progress = #{it => checkpoint, last_message_id => 20, unacked => [#{10 => true}]},
    SB0 = emqx_mq_consumer_stream_buffer:restore(Progress, mq()),
    {undefined, SB1} = push(SB0, restored_end, [10, 20]),
    ?assertMatch(
        {#progress{boundary = 21, totals = undefined}, _},
        ack(SB1, 10)
    ).

%% Check that a buffer restored at the end of the stream reports the checkpoint boundary.
t_restored_end_of_stream(_Config) ->
    Progress = #{it => checkpoint, last_message_id => 20, unacked => [#{10 => true}]},
    SB0 = emqx_mq_consumer_stream_buffer:restore(Progress, mq()),
    %% Cleanup deleted record 20, so the stream ends before the checkpoint is reached
    {undefined, SB1} = push(SB0, restored_end, [10]),
    %% The end of the stream completes restoration and redelivers the unacknowledged record
    {ok, [{_, 10, _}], undefined, SB2} = end_of_stream(SB1),
    ?assertMatch(#{status := active}, emqx_mq_consumer_stream_buffer:inspect(SB2)),
    %% Acknowledging the restored record retires the buffer and finishes the stream
    ?assertEqual(
        {finished, #progress{boundary = 21, totals = undefined}},
        emqx_mq_consumer_stream_buffer:handle_ack(SB2, 10)
    ).

%% Check that a finished stream reports the progress of its last retired buffer.
t_finished_progress(_Config) ->
    SB0 = emqx_mq_consumer_stream_buffer:new(start, mq()),
    {undefined, SB1} = push(SB0, lower_end, [10]),
    {ok, [], undefined, SB2} = end_of_stream(SB1),
    ?assertEqual(
        {finished, #progress{boundary = 11, totals = totals(1)}},
        emqx_mq_consumer_stream_buffer:handle_ack(SB2, 10)
    ).

%% Check that each retired buffer reports the count and bytes of its own records.
t_retired_totals(_Config) ->
    SB0 = emqx_mq_consumer_stream_buffer:new(start, mq()),
    {undefined, SB1} = push(SB0, lower_end, [10, 20]),
    {undefined, SB2} = push(SB1, upper_end, [30]),
    {undefined, SB3} = ack(SB2, 10),
    {Progress0, SB4} = ack(SB3, 20),
    ?assertEqual(#progress{boundary = 21, totals = totals(2)}, Progress0),
    %% The rotated buffer reports only its own records
    {Progress1, SB5} = ack(SB4, 30),
    ?assertEqual(#progress{boundary = 31, totals = totals(1)}, Progress1),
    %% A compacted buffer starts from zero totals
    {undefined, SB6} = push(SB5, next_end, [50]),
    ?assertMatch({#progress{boundary = 51, totals = #{count := 1}}, _}, ack(SB6, 50)).

mq() -> #{stream_max_buffer_size => 4}.

totals(Count) ->
    #{count => Count, bytes => Count * ?PAYLOAD_SIZE}.

push(SB0, It, IDs) ->
    push(SB0, It, IDs, 1).

push(SB0, It, IDs, SeqNo) ->
    TTVs = [{[], ID, <<ID:(?PAYLOAD_SIZE * 8)>>} || ID <- IDs],
    {ok, _, Progress, SB1} = emqx_mq_consumer_stream_buffer:handle_ds_reply(
        SB0,
        handle,
        #ds_sub_reply{payload = {ok, It, TTVs}, seqno = SeqNo, size = length(TTVs)}
    ),
    {Progress, SB1}.

end_of_stream(SB) ->
    emqx_mq_consumer_stream_buffer:handle_ds_reply(
        SB, handle, #ds_sub_reply{payload = {ok, end_of_stream}, seqno = 2, size = 0}
    ).

ack(SB0, ID) ->
    {ok, Progress, SB1} = emqx_mq_consumer_stream_buffer:handle_ack(SB0, ID),
    {Progress, SB1}.
