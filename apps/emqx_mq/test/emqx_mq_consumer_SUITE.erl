%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx.hrl").
-include("../src/emqx_mq_internal.hrl").

all() ->
    Standalone = emqx_common_test_helpers:all(?MODULE) -- queue_type_cases(),
    Standalone ++ [{group, Group} || {Group, _, _} <- groups()].

groups() ->
    [
        {regular, [], queue_type_cases()},
        {limited_regular, [], queue_type_cases()},
        {lastvalue, [], queue_type_cases()},
        {limited_lastvalue, [], queue_type_cases()}
    ].

queue_type_cases() ->
    [t_deliver_cutoff_date].

init_per_group(Group, Config) ->
    {IsLastvalue, MaxCount} =
        case Group of
            regular -> {false, infinity};
            limited_regular -> {false, 100};
            lastvalue -> {true, infinity};
            limited_lastvalue -> {true, 100}
        end,
    Limits = #{max_shard_message_count => MaxCount, max_shard_message_bytes => infinity},
    [{is_lastvalue, IsLastvalue}, {limits, Limits} | Config].

end_per_group(_Group, _Config) ->
    ok.

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_durable_storage,
            {emqx, emqx_mq_test_utils:cth_config(emqx)},
            {emqx_mq, emqx_mq_test_utils:cth_config(emqx_mq)}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_CaseName, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    _ = meck:unload(),
    ok = snabbkaffe:stop(),
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Verify cutoff boundaries, message timestamps, and persisted auto-acks for the group's queue type.
t_deliver_cutoff_date(Config) ->
    IsLastvalue = ?config(is_lastvalue, Config),
    Name = <<"deliver_cutoff_date">>,
    Now = erlang:system_time(millisecond),
    Cutoff = Now - 60_000,
    CutoffUs = erlang:convert_time_unit(Cutoff, millisecond, microsecond),
    MQOpts0 = #{
        name => Name,
        topic_filter => <<"t/#">>,
        is_lastvalue => IsLastvalue,
        limits => ?config(limits, Config),
        deliver_cutoff_date => Cutoff,
        stream_max_buffer_size => 1,
        stream_max_unacked => 1
    },
    MQOpts1 =
        case IsLastvalue of
            true -> MQOpts0#{key_expression => <<"message.payload">>};
            false -> MQOpts0
        end,
    MQ0 = emqx_mq_test_utils:ensure_mq_created(MQOpts1),
    #{id := Id} = MQ0,
    DB =
        case emqx_mq_prop:is_append_only(MQ0) of
            true -> ?MQ_MESSAGE_REGULAR_DB;
            false -> ?MQ_MESSAGE_LASTVALUE_DB
        end,
    [Shard | _] = emqx_ds:list_shards(DB),
    #{Shard := Generation} = emqx_mq_message_db:initial_generations(MQ0),

    %% Store old messages after the cutoff to check delivery filtering after the DS seek.
    Records = [
        begin
            Message = emqx_message:make(<<"publisher">>, 1, <<"t/1">>, Payload),
            {
                [<<"topic">>, <<"t/#">>, Id, <<"key">>, Payload],
                StoredAt,
                emqx_mq_message_db:encode_message(Message#message{timestamp = PublishedAt})
            }
        end
     || {Payload, StoredAt, PublishedAt} <- [
            {<<"old-1">>, CutoffUs + 1, Cutoff - 3},
            {<<"old-2">>, CutoffUs + 2, Cutoff - 2},
            {<<"old-3">>, CutoffUs + 3, Cutoff - 1},
            {<<"boundary">>, CutoffUs, Cutoff},
            {<<"fresh">>, CutoffUs + 1000, Cutoff + 1},
            {<<"late-old">>, CutoffUs + 1001, Cutoff - 1},
            {<<"expired">>, CutoffUs + 1002, Now - 7_200_000}
        ]
    ],
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ),

    %% Auto-acks must advance past more old messages than the stream buffer can hold.
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(CSub0, Name),
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(2, 1000),
    ?assertEqual([<<"boundary">>, <<"fresh">>], [P || #{payload := P} <- Msgs0]),
    ok = emqtt:disconnect(CSub0),
    ?assertEqual(lists:sort(Records), lists:sort(emqx_mq_message_db:dirty_read_all(MQ0))),

    %% Lower the cutoff and restart the consumer without replaying acknowledged messages.
    {ok, MQ1} = emqx_mq_registry:update(Name, MQ0#{deliver_cutoff_date => 0}),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(CSub1, Name),
    Marker = emqx_message:make(<<"publisher">>, 1, <<"t/1">>, <<"marker">>),
    ok = emqx_mq_message_db:insert(MQ1, Marker),
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(1, 1000),
    ?assertEqual([<<"marker">>], [P || #{payload := P} <- Msgs1]),
    ok = emqtt:disconnect(CSub1),
    ok = emqx_mq_registry:delete(Name).

%% Check that subscription start times use the later of retention and the optional delivery cutoff.
t_delivery_start_time(_Config) ->
    NowMs = erlang:system_time(millisecond),
    RetentionMs = 60_000,
    lists:foreach(
        fun(#{settings := Settings, earliest := Earliest}) ->
            StartUs = emqx_mq_prop:delivery_start_time(Settings),
            ?assert(StartUs >= Earliest * 1000),
            Latest = max(Earliest, erlang:system_time(millisecond) - RetentionMs),
            ?assert(StartUs =< Latest * 1000)
        end,
        [
            #{settings => #{data_retention_period => RetentionMs}, earliest => NowMs - RetentionMs},
            #{
                settings => #{
                    data_retention_period => RetentionMs, deliver_cutoff_date => NowMs - 120_000
                },
                earliest => NowMs - RetentionMs
            },
            #{
                settings => #{data_retention_period => RetentionMs, deliver_cutoff_date => NowMs},
                earliest => NowMs
            }
        ]
    ).

%% Check that fresh and restored consumers skip old DS records while preserving acknowledgements.
t_delivery_start_time_replay(_Config) ->
    %% Store three records around the cutoff in one shard.
    #{id := Id} =
        MQ0 = emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"delivery_start_time">>,
            topic_filter => <<"start/#">>,
            is_lastvalue => true,
            key_expression => <<"message.payload">>,
            stream_max_buffer_size => 100,
            stream_max_unacked => 100
        }),
    CutoffMs = erlang:system_time(millisecond) - 60_000,
    DB = ?MQ_MESSAGE_LASTVALUE_DB,
    [Shard | _] = emqx_ds:list_shards(DB),
    #{Shard := Generation} = emqx_mq_message_db:initial_generations(MQ0),
    Records = [
        begin
            Message = emqx_message:make(<<"publisher">>, 1, <<"start/1">>, Payload),
            {
                [<<"topic">>, <<"start/#">>, Id, <<"key">>, Payload],
                Timestamp * 1000,
                emqx_mq_message_db:encode_message(Message#message{timestamp = Timestamp})
            }
        end
     || #{payload := Payload, timestamp := Timestamp} <- [
            #{payload => <<"old">>, timestamp => CutoffMs - 1},
            #{payload => <<"boundary">>, timestamp => CutoffMs},
            #{payload => <<"acked">>, timestamp => CutoffMs + 1}
        ]
    ],
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ),

    %% Save progress with old and boundary records unacknowledged.
    CS0 = emqx_mq_consumer_streams:new(MQ0, #{}),
    {[_, _, {AckedId, _}], CS1} = receive_consumer_messages(CS0, 3),
    CS2 = emqx_mq_consumer_streams:handle_ack(CS1, AckedId),
    {Progress, CS3} = emqx_mq_consumer_streams:fetch_progress_updates(CS2),
    _ = emqx_mq_consumer_streams:destroy(CS3),

    %% Restore at the cutoff and read only the two retained records from DS.
    MQ1 = MQ0#{deliver_cutoff_date => CutoffMs},
    Received0 = ds_received_messages(),
    CS4 = emqx_mq_consumer_streams:new(MQ1, Progress),
    {[{_, Boundary}], CS5} = receive_consumer_messages(CS4, 1),
    ?assertEqual(<<"boundary">>, emqx_message:payload(Boundary)),
    ?assertEqual(2, ds_received_messages() - Received0),
    _ = emqx_mq_consumer_streams:destroy(CS5),

    %% A fresh consumer also starts at the cutoff and delivers both retained records.
    Received1 = ds_received_messages(),
    CS6 = emqx_mq_consumer_streams:new(MQ1, #{}),
    {Fresh, CS7} = receive_consumer_messages(CS6, 2),
    ?assertEqual([<<"boundary">>, <<"acked">>], [emqx_message:payload(M) || {_, M} <- Fresh]),
    ?assertEqual(2, ds_received_messages() - Received1),
    _ = emqx_mq_consumer_streams:destroy(CS7),
    ?assertEqual(lists:sort(Records), lists:sort(emqx_mq_message_db:dirty_read_all(MQ0))).

t_restore_empty_stream_buffer(_Config) ->
    MQ = #{stream_max_buffer_size => 100},
    It = fake_iterator,
    SB0 = emqx_mq_consumer_stream_buffer:new(It, MQ),

    Progress = emqx_mq_consumer_stream_buffer:progress(SB0),
    SB = emqx_mq_consumer_stream_buffer:restore(Progress, MQ),

    ?assertMatch(
        #{
            status := active,
            last_message_id := undefined,
            lower_buffer := #{n := 0, unacked := 0}
        },
        emqx_mq_consumer_stream_buffer:inspect(SB)
    ).

%% Verify that a stream buffer accepts acks while it restores its state
t_ack_restoring_stream_buffer(_Config) ->
    MQ = #{stream_max_buffer_size => 100},
    Progress = #{
        it => fake_iterator,
        last_message_id => 2,
        unacked => [#{1 => true, 2 => true}]
    },
    SB0 = emqx_mq_consumer_stream_buffer:restore(Progress, MQ),

    {ok, SB} = emqx_mq_consumer_stream_buffer:handle_ack(SB0, 1),
    ?assertMatch(
        #{status := restoring, unacked := 1},
        emqx_mq_consumer_stream_buffer:inspect(SB)
    ).

%% Verify that the consumer stops itself after there are no active subscribers for a while
t_auto_shutdown(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"auto_shutdown">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        consumer_max_inactive => 50
    }),

    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"auto_shutdown">>),

    ?assertWaitEvent(
        emqtt:disconnect(CSub),
        #{
            ?snk_kind := mq_consumer_shutdown,
            mq_topic_filter := <<"t/#">>,
            mq_name := <<"auto_shutdown">>
        },
        1000
    ).

%% Verify that if a subscriber fails to spawn a consumer because of "already_registered" error
%% (a conflicting consumer is already running), then the subscriber will quickly reconnect
%% and start consuming messages from the already existing consumer.
t_quick_reconnect(_Config) ->
    %% Create a non-lastvalue Queue
    #{id := MQId} =
        _ = emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"quick_reconnect">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            consumer_max_inactive => 50
        }),

    %% Publish a message to the queue
    CPub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_pub_mq(CPub, <<"t/1">>, <<"test message">>),
    ok = emqtt:disconnect(CPub),

    %% Create a "conflicting consumer"
    Pid = spawn_link(fun() ->
        global:register_name(emqx_mq_consumer:global_name(MQId), self()),
        receive
            stop ->
                ok
        end
    end),
    meck:new(emqx_mq_consumer, [passthrough, no_link, no_passthrough_cover]),
    meck:expect(emqx_mq_consumer, find, fun(_) -> not_found end),

    %% Connect a client to the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"quick_reconnect">>),
        #{?snk_kind := mq_sub_handle_connect_error},
        1000
    ),

    %% Stop the "conflicting consumer"
    erlang:send(Pid, stop),

    %% Verify that the message is received within little threshold
    receive
        {publish, #{topic := <<"t/1">>, payload := <<"test message">>}} ->
            ok
    after 1000 ->
        ct:fail("message not received")
    end,

    %% Cleanly tear down the subscriber and wait for the consumer it
    %% started to auto-shut down BEFORE `meck:unload' restores the
    %% original `emqx_mq_consumer' BEAM.  Otherwise meck races with
    %% cover purge: the still-running consumer gen_server holds the
    %% cover-compiled module, and `restore_original' -> `code:load_file'
    %% returns `{error, not_purged}', crashing `meck_proc'.
    ?assertWaitEvent(
        emqtt:disconnect(CSub),
        #{?snk_kind := mq_consumer_shutdown, mq_topic_filter := <<"t/#">>},
        1000
    ),
    meck:unload(emqx_mq_consumer).

%% A subscriber may be considered dead and removed by the consumer,
%% but the it may still try to send late acks/pings.
%% We verify that such acks/pings are ignored.
t_ack_from_unknown_subscriber(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"ack_from_unknown_subscriber">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        consumer_max_inactive => 50
    }),

    %% Create a subscriber spawning a consumer
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"ack_from_unknown_subscriber">>),

    %% Find the consumer send ack & ping from a fake subscriber
    [Consumer] = emqx_mq_test_utils:all_consumers(),
    FakeSub = make_ref(),
    ok = emqx_mq_consumer:ack(Consumer, FakeSub, {{<<"0">>, 1}, 123}, ?MQ_ACK),
    ok = emqx_mq_consumer:ping(Consumer, FakeSub),

    %% Verify that the consumer is fully functional and ignored the ack & ping
    ok = emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 2000),
    ?assertEqual(1, length(Msgs)),
    ?assert(is_process_alive(Consumer)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

receive_consumer_messages(CS, Count) ->
    receive_consumer_messages(CS, Count, []).

receive_consumer_messages(CS, Count, Acc) when length(Acc) >= Count ->
    {Acc, CS};
receive_consumer_messages(CS0, Count, Acc) ->
    receive
        Info ->
            case emqx_mq_consumer_streams:handle_ds_info(CS0, Info) of
                {ok, Messages, CS1} -> receive_consumer_messages(CS1, Count, Acc ++ Messages);
                ignore -> receive_consumer_messages(CS0, Count, Acc)
            end
    after 5000 ->
        ct:fail({missing_messages, Count, Acc})
    end.

ds_received_messages() ->
    #{received_messages := Count} = emqx_mq_metrics:get_counters(ds),
    Count.
