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
    [t_deliver_cutoff_date, t_storage_timestamp_retention].

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

%% Check that initial subscriptions use the retention boundary in microseconds.
t_subscription_start_time(_Config) ->
    MQ = emqx_mq_test_utils:fill_mq_defaults(#{id => <<"start">>, topic_filter => <<"start/#">>}),
    #{data_retention_period := RetentionMs} = MQ,
    BeforeMs = erlang:system_time(millisecond),
    ok = meck:new(emqx_ds_client, [passthrough]),
    ok = meck:expect(emqx_ds_client, subscribe, fun(Client, Opts, State) ->
        #{start_time := StartUs} = Opts,
        ?assert(StartUs >= (BeforeMs - RetentionMs) * 1000),
        ?assert(StartUs =< (erlang:system_time(millisecond) - RetentionMs) * 1000),
        {ok, Client, State}
    end),
    ?assertEqual(
        {ok, client, state}, emqx_mq_message_db:subscribe(MQ, client, [], state)
    ),
    ok = meck:unload(emqx_ds_client),
    ?assertEqual(
        0,
        emqx_mq_prop:delivery_start_time(#{
            data_retention_period => erlang:system_time(millisecond) + 60_000
        })
    ).

%% Check that iterator creation skips expired records and preserves saved acknowledgements.
t_delivery_start_time_replay(_Config) ->
    %% Store expired and retained records in one shard without running garbage collection.
    {ok, #{id := Id} = MQ0} = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"start/#">>,
        is_lastvalue => true,
        key_expression => <<"message.payload">>,
        stream_max_buffer_size => 100,
        stream_max_unacked => 100
    }),
    NowMs = erlang:system_time(millisecond),
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
            #{payload => <<"old">>, timestamp => NowMs - 120_000},
            #{payload => <<"unacked">>, timestamp => NowMs - 30_000},
            #{payload => <<"acked">>, timestamp => NowMs - 20_000}
        ]
    ],
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ),

    %% Save progress with the oldest two records unacknowledged.
    {[<<"old">>, <<"unacked">>, <<"acked">>], Progress} =
        consume_messages(MQ0, #{}, 3, [<<"acked">>]),

    %% Restore with shorter retention and read only retained records from DS.
    MQ1 = MQ0#{data_retention_period => 60_000},
    Received0 = ds_received_messages(),
    {[<<"unacked">>], _} = consume_messages(MQ1, Progress, 1, []),
    ?assertEqual(2, ds_received_messages() - Received0),

    %% A fresh consumer skips expired records without deleting them.
    Received1 = ds_received_messages(),
    {[<<"unacked">>, <<"acked">>], _} = consume_messages(MQ1, #{}, 2, []),
    ?assertEqual(2, ds_received_messages() - Received1),
    ?assertEqual(lists:sort(Records), lists:sort(emqx_mq_message_db:dirty_read_all(MQ0))).

%% Verify storage-time cutoff boundaries and persisted acknowledgments for the group's queue type.
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

    %% Store records on both sides of the cutoff with different publication timestamps.
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
            {<<"old-1">>, CutoffUs - 3, Now},
            {<<"old-2">>, CutoffUs - 2, Now},
            {<<"old-3">>, CutoffUs - 1, Now},
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

    %% Seek by storage time and deliver records whose publication timestamps precede the cutoff.
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    ok = emqx_mq_test_utils:emqtt_sub_mq(CSub0, Name),
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(4, 1000),
    ?assertEqual([<<"boundary">>, <<"fresh">>, <<"late-old">>, <<"expired">>], [
        P
     || #{payload := P} <- Msgs0
    ]),
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

%% Check that retention filters and acknowledges by storage time and delivered timestamps use DS time.
t_storage_timestamp_retention(Config) ->
    #{id := Id} =
        MQ = emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"storage_timestamp">>,
            topic_filter => <<"time/#">>,
            is_lastvalue => ?config(is_lastvalue, Config),
            limits => ?config(limits, Config),
            data_retention_period => 60000
        }),
    DB =
        case emqx_mq_prop:is_append_only(MQ) of
            true -> ?MQ_MESSAGE_REGULAR_DB;
            false -> ?MQ_MESSAGE_LASTVALUE_DB
        end,
    [Shard | _] = emqx_ds:list_shards(DB),
    #{Shard := Generation} = emqx_mq_message_db:initial_generations(MQ),
    NowMs = erlang:system_time(millisecond),
    FreshUs = (NowMs - 1000) * 1000 + 123,
    Records = [
        begin
            Message = emqx_message:make(<<"publisher">>, 1, <<"time/message">>, Payload),
            {
                [<<"topic">>, <<"time/#">>, Id, <<"key">>, Payload],
                StoredAt,
                emqx_mq_message_db:encode_message(Message#message{timestamp = PublishedAt})
            }
        end
     || #{payload := Payload, stored_at := StoredAt, published_at := PublishedAt} <- [
            #{
                payload => <<"expired">>,
                stored_at => (NowMs - 120000) * 1000,
                published_at => NowMs
            },
            #{payload => <<"fresh">>, stored_at => FreshUs, published_at => NowMs - 120000}
        ]
    ],
    ?assertMatch(
        {atomic, _, ok},
        emqx_ds:trans(#{db => DB, shard => Shard, generation => Generation}, fun() ->
            lists:foreach(fun emqx_ds:tx_write/1, Records)
        end)
    ),
    %% Read the expired record too, so this checks filtering and auto-ack after retrieval.
    meck:new(emqx_mq_prop, [passthrough, no_link]),
    meck:expect(emqx_mq_prop, delivery_start_time, fun(_) -> 0 end),
    CS0 = emqx_mq_consumer_streams:new(MQ, #{}),
    {[{MessageId, Delivered}], CS1} = receive_consumer_messages(CS0, 1),
    ?assertEqual(<<"fresh">>, emqx_message:payload(Delivered)),
    ?assertEqual(FreshUs div 1000, emqx_message:timestamp(Delivered)),
    {Progress0, CS2} = emqx_mq_consumer_streams:fetch_progress_updates(CS1),
    #{Shard := #{buffer_progress := #{unacked := Unacked0}}} = Progress0,
    ?assertEqual(#{FreshUs => true}, emqx_utils_maps:merge(Unacked0)),
    CS3 = emqx_mq_consumer_streams:handle_ack(CS2, MessageId),
    {Progress1, CS4} = emqx_mq_consumer_streams:fetch_progress_updates(CS3),
    #{Shard := #{buffer_progress := #{unacked := Unacked1}}} = Progress1,
    ?assertEqual(#{}, emqx_utils_maps:merge(Unacked1)),
    _ = emqx_mq_consumer_streams:destroy(CS4),
    ?assertEqual(lists:sort(Records), lists:sort(emqx_mq_message_db:dirty_read_all(MQ))).

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
t_delivery_cutoff_replay(_Config) ->
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

    {ok, undefined, SB} = emqx_mq_consumer_stream_buffer:handle_ack(SB0, 1),
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

consume_messages(MQ, Progress0, Count, AckPayloads) ->
    emqx_utils:nolink_apply(
        fun() ->
            CS0 = emqx_mq_consumer_streams:new(MQ, Progress0),
            {Messages, CS1} = receive_consumer_messages(CS0, Count, []),
            CS2 = lists:foldl(
                fun({Id, Message}, Acc) ->
                    case lists:member(emqx_message:payload(Message), AckPayloads) of
                        true -> emqx_mq_consumer_streams:handle_ack(Acc, Id);
                        false -> Acc
                    end
                end,
                CS1,
                Messages
            ),
            {Progress1, _} = emqx_mq_consumer_streams:fetch_progress_updates(CS2),
            {[emqx_message:payload(M) || {_, M} <- Messages], Progress1}
        end,
        10_000
    ).

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
