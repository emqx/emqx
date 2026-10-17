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
    emqx_common_test_helpers:all(?MODULE).

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
    #{id := Id} =
        MQ0 = emqx_mq_test_utils:create_mq(#{
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
    _ = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"t/#">>, is_lastvalue => false, consumer_max_inactive => 50
    }),

    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),

    ?assertWaitEvent(
        emqtt:disconnect(CSub),
        #{?snk_kind := mq_consumer_shutdown, mq_topic_filter := <<"t/#">>},
        1000
    ).

%% Verify that if a subscriber fails to spawn a consumer because of "already_registered" error
%% (a conflicting consumer is already running), then the subscriber will quickly reconnect
%% and start consuming messages from the already existing consumer.
t_quick_reconnect(_Config) ->
    %% Create a non-lastvalue Queue
    #{id := MQId} =
        _ = emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>, is_lastvalue => false, consumer_max_inactive => 50
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
    meck:new(emqx_mq_consumer, [passthrough]),
    meck:expect(emqx_mq_consumer, find, fun(_) -> not_found end),

    %% Connect a client to the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
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
    _ = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"t/#">>, is_lastvalue => false, consumer_max_inactive => 50
    }),

    %% Create a subscriber spawning a consumer
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),

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
