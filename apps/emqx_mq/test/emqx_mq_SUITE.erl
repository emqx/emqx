%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-include("../src/emqx_mq_internal.hrl").

-define(COMMON_DISPATCH_TESTS, [t_redispatch, t_redispatch_delay]).
-define(COMMON_LIMITED_TESTS, [t_publish_and_consume_regular, t_publish_and_consume_lastvalue]).

-define(MANY, 999_999_999_999).

all() ->
    All =
        emqx_common_test_helpers:all(?MODULE) -- (?COMMON_DISPATCH_TESTS ++ ?COMMON_LIMITED_TESTS),
    [
        {group, unlimited},
        {group, limited},
        {group, random},
        {group, least_inflight},
        {group, round_robin}
    ] ++
        All.

groups() ->
    [
        {random, [], ?COMMON_DISPATCH_TESTS},
        {least_inflight, [], ?COMMON_DISPATCH_TESTS},
        {round_robin, [], ?COMMON_DISPATCH_TESTS},
        {limited, [], ?COMMON_LIMITED_TESTS},
        {unlimited, [], ?COMMON_LIMITED_TESTS}
    ].

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
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

init_per_group(DispatchStrategy, Config) when
    DispatchStrategy =:= random;
    DispatchStrategy =:= least_inflight;
    DispatchStrategy =:= round_robin
->
    [{dispatch_strategy, DispatchStrategy} | Config];
init_per_group(limited, Config) ->
    [{limits, #{max_shard_message_bytes => ?MANY, max_shard_message_count => ?MANY}} | Config];
init_per_group(unlimited, Config) ->
    [
        {limits, #{max_shard_message_bytes => infinity, max_shard_message_count => infinity}}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(_CaseName, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = snabbkaffe:stop(),
    ok = emqx_mq_test_utils:cleanup_mqs(),
    ok = emqx_mq_test_utils:reset_config().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_smoke(_Config) ->
    %% Create a non-lastvalue Queue
    MQ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"smoke">>, topic_filter => <<"t/#">>, is_lastvalue => false
    }),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    AllMessages = emqx_mq_message_db:dirty_read_all(MQ),
    ?assertEqual(100, length(AllMessages)),

    % Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"smoke">>, <<"t/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 1000),

    %% Verify the messages
    ?assertEqual(100, length(Msgs)),
    ok.

%% Consume some history messages from a non-lastvalue(regular) queue
t_publish_and_consume_regular(Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"regular">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        limits => ?config(limits, Config)
    }),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_mq_test_utils:populate(50, #{topic_prefix => <<"t/">>, qos => 0}),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"regular">>, <<"t/#">>),
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 100, _Timeout0 = 5000),

    %% Verify the messages
    ?assertEqual(100, length(Msgs0)),

    %% Add a generation
    ok = emqx_mq_message_db:add_regular_db_generation(),
    % %% And another one
    ok = emqx_mq_message_db:add_regular_db_generation(),

    %% Publish 100 more messages to the queue
    emqx_mq_test_utils:populate(50, #{topic_prefix => <<"t/">>}),
    emqx_mq_test_utils:populate(50, #{topic_prefix => <<"t/">>, qos => 0}),

    %% Consume the rest messages
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 100, _Timeout1 = 1000),

    %% Verify the messages
    ?assertEqual(100, length(Msgs1)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Consume some history messages from a lastvalue queue
t_publish_and_consume_lastvalue(Config) ->
    %% Create a lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"lastvalue">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => true,
        limits => ?config(limits, Config)
    }),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate_lastvalue(100, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-">>,
        n_keys => 10
    }),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"lastvalue">>, <<"t/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(10, length(Msgs)).

%% Verify that the consumer redispatches the message to another subscriber
%% immediately if a subscriber rejected the message
t_redispatch(Config) ->
    %% Create a non-lastvalue Queue
    _ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"redispatch">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            dispatch_strategy => ?config(dispatch_strategy, Config),
            redispatch_interval => 1000
        }),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"redispatch">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"redispatch">>),

    %% Publish 1 message to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Receive the message and reject it
    CSub =
        receive
            {publish, #{
                topic := <<"t/0">>,
                client_pid := Pid,
                packet_id := PacketId
            }} ->
                ok = emqtt:puback(Pid, PacketId, ?RC_UNSPECIFIED_ERROR),
                Pid
        after 100 ->
            ct:fail("t/0 message from MQ not received")
        end,

    %% Verify that the message is re-delivered to the other subscriber
    [OtherCSub] = [CSub0, CSub1] -- [CSub],
    receive
        {publish, #{topic := <<"t/0">>, client_pid := OtherCSub}} ->
            ok
    after 100 ->
        ct:fail("t/0 message from MQ not received by the other subscriber")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%% Verify that the consumer redispatches after a delay
%% if a subscriber rejected the message and there are no other subscribers
t_redispatch_delay(Config) ->
    %% Create a non-lastvalue Queue
    RedispatchInterval = 500,
    _ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"redispatch_delay">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            dispatch_strategy => ?config(dispatch_strategy, Config),
            redispatch_interval => RedispatchInterval
        }),

    %% Connect two subscribers
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"redispatch_delay">>),

    %% Publish 1 message to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Receive the message and reject it
    RecieveTime =
        receive
            {publish, #{
                topic := <<"t/0">>,
                client_pid := Pid,
                packet_id := PacketId
            }} ->
                ok = emqtt:puback(Pid, PacketId, ?RC_UNSPECIFIED_ERROR),
                now_ms()
        after 100 ->
            ct:fail("t/0 message from MQ not received")
        end,

    %% Verify that the message is re-delivered to the other subscriber
    WaitTime = RedispatchInterval * 2,
    receive
        {publish, #{topic := <<"t/0">>, client_pid := CSub}} ->
            ElapsedTime = now_ms() - RecieveTime,
            ?assert(
                ElapsedTime >= RedispatchInterval,
                binfmt("Messsage received in ~p ms which is less than redispatch interval ~p ms", [
                    ElapsedTime, RedispatchInterval
                ])
            )
    after WaitTime ->
        ct:fail("t/0 message from MQ not received by the other subscriber")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub).

% Verify that the consumer stops consuming DS messages once there is
% a critical amount of unacked messages
t_backpressure(_Config) ->
    StreamMaxUnacked = 5,
    StreamMaxBufferSize = 10,
    %% Create a non-lastvalue Queue
    _ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"backpressure">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            local_max_inflight => 100,
            stream_max_unacked => StreamMaxUnacked,
            stream_max_buffer_size => StreamMaxBufferSize
        }),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Consume the messages from the queue
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
    emqx_config:put([mqtt, max_inflight], 0),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"backpressure">>),
    {ok, Msgs0} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = StreamMaxBufferSize, _Timeout = 200),

    %% Messages should stop being dispatched once the buffer is full and we acked nothing
    ?assert(
        length(Msgs0) =< StreamMaxBufferSize + StreamMaxUnacked + 1,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs0), StreamMaxBufferSize + StreamMaxUnacked + 1]
        )
    ),

    %% Acknowledge the messages
    ok = emqx_mq_test_utils:emqtt_ack(Msgs0),

    %% After acknowledging, the messages should start being dispatched again
    {ok, Msgs1} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = StreamMaxBufferSize, _Timeout = 200),
    ?assert(
        length(Msgs1) =< StreamMaxBufferSize + StreamMaxUnacked * 2,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs1), StreamMaxBufferSize + StreamMaxUnacked * 2]
        )
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the consumer redispatches a message to another subscriber
%% if a subscriber received the message but disconnected before acknowledging it
t_redispatch_on_disconnect(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"redispatch_on_disconnect">>, topic_filter => <<"t/#">>, is_lastvalue => false
    }),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"redispatch_on_disconnect">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"redispatch_on_disconnect">>),

    %% Publish just 1 message to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Drain the message
    {ok, [#{client_pid := CSub, payload := <<"payload-0">>}]} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 100),

    %% Disconnect the subscriber which received the message
    ok = emqtt:disconnect(CSub),

    %% The other subscriber should receive the message now
    [OtherCSub] = [CSub0, CSub1] -- [CSub],
    {ok, [
        #{
            client_pid := OtherCSub,
            payload := <<"payload-0">>,
            packet_id := PacketId
        }
    ]} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 100),

    %% Clean up
    ok = emqtt:puback(OtherCSub, PacketId),
    ok = emqtt:disconnect(OtherCSub).

%% Cooperatively consume online messages with random dispatching
t_dispatch_random(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"dispatch_random">>, topic_filter => <<"t/#">>, is_lastvalue => false
    }),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"dispatch_random">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"dispatch_random">>),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Drain the messages
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 500),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1),

    %% Verify the messages
    ?assertEqual(100, length(Msgs)),
    {Sub0Msgs, Sub1Msgs} =
        lists:partition(
            fun
                (#{client_pid := Pid}) when Pid =:= CSub0 ->
                    true;
                (#{client_pid := Pid}) when Pid =:= CSub1 ->
                    false
            end,
            Msgs
        ),
    ?assert(length(Sub0Msgs) > 0),
    ?assert(length(Sub1Msgs) > 0).

%% Cooperatively consume online messages with round-robin dispatching
t_dispatch_round_robin(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"dispatch_round_robin">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        dispatch_strategy => round_robin
    }),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    CSub2 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"dispatch_round_robin">>),
    %% Subscribe via full topic is also legal
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"dispatch_round_robin">>, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub2, <<"dispatch_round_robin">>),

    %% Verify with snabbkaffe because of much asynchrony.
    %% Messages may be received out of order
    ?check_trace(
        begin
            %% Publish 6 messages to the queue
            emqx_mq_test_utils:populate(6, #{topic_prefix => <<"t/">>}),

            %% Drain the messages
            {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 6, _Timeout = 500),
            ?assertEqual(6, length(Msgs))
        end,
        fun(Trace) ->
            %% Verify the messages were dispatched cyclically to the subscribers
            DispatchSubscribers =
                [
                    SubscriberRef
                 || #{pick_result := {ok, SubscriberRef}} <- ?of_kind(
                        mq_consumer_dispatch_message, Trace
                    )
                ],
            ?assertEqual(6, length(DispatchSubscribers)),
            {Head, Tail} = lists:split(3, DispatchSubscribers),
            %% Check that the subscribers are different
            ?assertEqual(3, length(lists:usort(Head))),
            ?assertEqual(Head, Tail)
        end
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1),
    ok = emqtt:disconnect(CSub2).

%% Cooperatively consume online messages with least inflight dispatching
t_dispatch_least_inflight(_Config) ->
    %% Create a non-lastvalue Queue
    _ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"dispatch_least_inflight">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            dispatch_strategy => least_inflight
        }),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"dispatch_least_inflight">>, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"dispatch_least_inflight">>, <<"t/#">>),

    %% Publish 100 messages to the queue
    CPub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_pub_mq(CPub, <<"t/1">>, <<"payload-0">>),
    emqx_mq_test_utils:emqtt_pub_mq(CPub, <<"t/1">>, <<"payload-1">>),

    %% Each subscriber should receive one message
    {CSub, PacketId} =
        receive
            {publish, #{
                topic := <<"t/1">>,
                client_pid := Pid,
                packet_id := PktId
            }} ->
                {Pid, PktId}
        after 100 ->
            ct:fail("No messages from MQ received")
        end,
    [OtherCSub] = [CSub0, CSub1] -- [CSub],
    receive
        {publish, #{topic := <<"t/1">>, client_pid := OtherCSub}} ->
            ok
    after 100 ->
        ct:fail("Message not delivered to both subscribers")
    end,

    %% The next message should be delivered to the subscriber which acked the previous message
    ok = emqtt:puback(CSub, PacketId),
    emqx_mq_test_utils:emqtt_pub_mq(CPub, <<"t/1">>, <<"payload-2">>),
    receive
        {publish, #{topic := <<"t/1">>, client_pid := CSub}} ->
            ok
    after 100 ->
        ct:fail(
            "Message not delivered to the subscriber which acked the previous "
            "message"
        )
    end,

    %% Clean up
    ok = emqtt:disconnect(CPub),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%% Verify that messages are eventually delivered to a busy session,
%% i.e. a session that exhausted the limit of inflight messages
t_busy_session(_Config) ->
    %% Create a non-lastvalue Queue
    _ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"busy_session">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            local_max_inflight => 4
        }),

    %% Publish 100 messages to the queue
    emqx_mq_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Connect with max_inflight=10
    emqx_config:put([mqtt, max_inflight], 10),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),

    %% Fill the session's inflight buffer
    emqtt:subscribe(CSub, <<"busytopic">>, 1),
    CPub = emqx_mq_test_utils:emqtt_connect([]),
    lists:foreach(
        fun(_) ->
            emqx_mq_test_utils:emqtt_pub_mq(CPub, <<"busytopic">>, <<"payload">>)
        end,
        lists:seq(1, 20)
    ),

    %% Now subscribe to the queue, check that the session is busy
    %% and prevents the queue's messages from being delivered
    ?assertWaitEvent(
        emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"busy_session">>),
        #{
            ?snk_kind := mq_sub_handle_nack_session_busy,
            sub := #{name := <<"busy_session">>}
        },
        100
    ),

    %% Assert that after acks, the messages are delivered to the session
    ReceiveFun =
        fun RFun(NRecFromMQ) ->
            receive
                {publish, #{topic := <<"busytopic">>, packet_id := PacketId}} ->
                    ok = emqtt:puback(CSub, PacketId),
                    RFun(NRecFromMQ);
                {publish, #{topic := <<"t/", _/binary>>, packet_id := PacketId}} ->
                    ok = emqtt:puback(CSub, PacketId),
                    RFun(NRecFromMQ + 1)
            after 500 ->
                NRecFromMQ
            end
        end,
    NRecFromMQ = ReceiveFun(0),
    ?assert(NRecFromMQ == 100, binfmt("Expected 100 queued messages, got: ~p", [NRecFromMQ])),

    %% Clean up
    ok = emqtt:disconnect(CSub),
    ok = emqtt:disconnect(CPub).

%% We check that the consumption progress is restored correctly
%% when the consumer is stopped and started again:
%% * acked messages are not re-delivered
%% * unacked messages are re-delivered
t_progress_restoration(_Config) ->
    ok = emqx_mq_message_db:add_regular_db_generation(),
    %% Create a non-lastvalue Queue
    MQ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"progress_restoration">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            consumer_max_inactive => 50
        }),

    %% Publish 20 messages to the queue
    emqx_mq_test_utils:populate(20, #{topic_prefix => <<"t/">>}),

    %% Start to consume the messages from the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"progress_restoration">>),

    %% Receive first message and NOT acknowledge it
    receive
        {publish, #{topic := <<"t/0">>, client_pid := CSub0}} ->
            ok
    after 200 ->
        ct:fail("t/0 message from MQ not received")
    end,

    %% Receive second message and acknowledge it
    receive
        {publish, #{
            topic := <<"t/1">>,
            packet_id := PacketId,
            client_pid := CSub0
        }} ->
            ok = emqtt:puback(CSub0, PacketId)
    after 200 ->
        ct:fail("t/1 message from MQ not received")
    end,

    %% Disconnect the client and wait for the consumer to stop and save the progress
    ok = emqtt:disconnect(CSub0),
    ok = wait_for_consumer_stop(MQ, 100),
    ok = emqx_mq_message_db:add_regular_db_generation(),

    %% Start the client and the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"progress_restoration">>),

    %% Verify that we receive unacknowledged messages from t/0, and t/2
    %% and DO NOT receive already acknowledged from t/1
    receive
        {publish, #{topic := <<"t/1">>, client_pid := CSub1}} ->
            ct:fail("Already acknowledged t/1 message from MQ received")
    after 100 ->
        ok
    end,
    receive
        {publish, #{topic := <<"t/0">>, client_pid := CSub1}} ->
            ok
    after 100 ->
        ct:fail("t/0 unacknowledged message from MQ not received")
    end,
    receive
        {publish, #{topic := <<"t/2">>, client_pid := CSub1}} ->
            ok
    after 100 ->
        ct:fail("t/2 message from MQ not received")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% We check that the consumption progress is restored correctly
%% when the generation where the consumer stopped is removed
t_progress_restoration_from_removed_gen(_Config) ->
    ok = emqx_mq_message_db:add_regular_db_generation(),
    %% Create a non-lastvalue Queue
    MQ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"progress_restoration_from_removed_gen">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            consumer_max_inactive => 50
        }),

    %% Publish 20 messages to the queue
    emqx_mq_test_utils:populate(20, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-old-">>
    }),

    %% Start to consume the messages from the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"progress_restoration_from_removed_gen">>),
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 20, _Timeout0 = 500),
    ?assertEqual(20, length(Msgs0)),

    %% Disconnect the client and wait for the consumer to stop and save the progress
    ok = emqtt:disconnect(CSub0),
    ok = wait_for_consumer_stop(MQ, 100),

    %% Add some messages to the new generation and delete the old one (where the consumer stopped)
    SlabInfo = emqx_mq_message_db:regular_db_slab_info(),
    ok = emqx_mq_message_db:add_regular_db_generation(),
    emqx_mq_test_utils:populate(20, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-new-">>
    }),
    ok = lists:foreach(
        fun(Slab) ->
            emqx_mq_message_db:drop_regular_db_slab(Slab)
        end,
        maps:keys(SlabInfo)
    ),

    %% Start the client and the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"progress_restoration_from_removed_gen">>),

    %% Verify that we receive all new messages
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 20, _Timeout1 = 500),
    ?assertEqual(20, length(Msgs1), binfmt("Expected 20 messages, got: ~p", [length(Msgs1)])),
    ?assert(
        lists:all(
            fun
                (#{payload := <<"payload-new-", _/binary>>}) -> true;
                (_) -> false
            end,
            Msgs1
        ),
        binfmt("Expected all messages to be from new generation with payload-new-* payload", [])
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% We check that the consumption progress is restored correctly
%% when the consumption buffer is full
t_progress_restoration_full_buffer(_Config) ->
    %% Create a non-lastvalue Queue
    MQ =
        emqx_mq_test_utils:ensure_mq_created(#{
            name => <<"progress_restoration_full_buffer">>,
            topic_filter => <<"t/#">>,
            is_lastvalue => false,
            consumer_max_inactive => 50,
            local_max_inflight => 100
        }),

    %% Publish 20 messages to the queue
    emqx_mq_test_utils:populate(100, #{topic_prefix => <<"t/">>}),

    %% Start to consume the messages from the queue
    emqx_config:put([mqtt, max_inflight], 100),
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"progress_restoration_full_buffer">>),

    %% Receive all messages and acknowledge only the last one
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 100),
    #{packet_id := PacketId} = lists:last(Msgs0),
    ok = emqtt:puback(CSub0, PacketId),

    %% Disconnect the client and wait for the consumer to stop and save the progress
    ok = emqtt:disconnect(CSub0),
    ok = wait_for_consumer_stop(MQ, 100),

    %% Start the client and the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"progress_restoration_full_buffer">>, <<"t/#">>),

    %% Verify that we receive all messages
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 10, _Timeout1 = 100),
    ?assertEqual(99, length(Msgs1), binfmt("Expected 99 messages, got: ~p", [length(Msgs1)])),

    %% Clean up
    ok = emqtt:disconnect(CSub1).

t_queue_deletion(_Config) ->
    %% Create a non-lastvalue Queue
    #{id := Id} = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"queue_deletion">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        dispatch_strategy => random
    }),

    %% Publish 20 messages to the queue
    emqx_mq_test_utils:populate(20, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-1-">>
    }),

    %% Connect and start a consumer
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"queue_deletion">>),

    %% Find the consumer
    {ok, ConsumerRef} = emqx_mq_consumer:find(Id),
    MRef = erlang:monitor(process, ConsumerRef),

    %% Delete the queue
    ok = emqx_mq_registry:delete(<<"queue_deletion">>),
    receive
        {'DOWN', MRef, process, ConsumerRef, _Reason} -> ok
    after 1000 ->
        ct:fail("Consumer not down")
    end,

    %% Disconnect the consumer
    ok = emqtt:disconnect(CSub0),

    %% Create a new queue with the same name and topic filter
    _MQ1 = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"queue_deletion">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        dispatch_strategy => random
    }),

    %% Publish 20 messages to the new queue
    emqx_mq_test_utils:populate(20, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-2-">>
    }),

    %% Connect and start a consumer
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"queue_deletion">>),

    %% Verify that the old messages are not received
    receive
        {publish, #{payload := <<"payload-1-", _/binary>>, client_pid := CSub1}} ->
            ct:fail("Old message received")
    after 100 ->
        ok
    end,

    %% Verify that the new messages are received
    receive
        {publish, #{payload := <<"payload-2-", _/binary>>, client_pid := CSub1}} ->
            ok
    after 100 ->
        ct:fail("New message not received")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% Verify that the queue is deleted correctly when there is a new empty generation
%% TODO
%% Drop this test and the related workaround once the bug in tx manager is fixed
t_queue_deletion_with_new_empty_generation(_Config) ->
    %% Create a non-lastvalue Queue and a new generation
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"queue_deletion_with_new_empty_generation">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    }),
    ok = emqx_mq_message_db:add_regular_db_generation(),

    %% Delete the queue
    ok = emqx_mq_registry:delete(<<"queue_deletion_with_new_empty_generation">>).

%% Check that a session of a disconnected client does not receive messages
t_disconnected_session_does_not_receive_messages(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"disconnected_session_does_not_receive_messages">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false
    }),

    %% Publish some messages to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Connect a client
    CSub0 = emqx_mq_test_utils:emqtt_connect([
        {auto_ack, false},
        {clientid, <<"c0">>},
        {properties, #{'Session-Expiry-Interval' => 1000}}
    ]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"disconnected_session_does_not_receive_messages">>),

    %% Assert that the message is received
    receive
        {publish, #{payload := <<"payload-0">>, client_pid := CSub0}} ->
            ok
    after 1000 ->
        ct:fail("Message not received by CSub0")
    end,

    %% Disconnect the client
    ok = emqtt:disconnect(CSub0),

    %% Verify that the session's channel is alive
    ?assertMatch([_], emqx_cm:lookup_channels(<<"c0">>)),

    %% Verify that the message is redispatched to another subscriber
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"disconnected_session_does_not_receive_messages">>),
    receive
        {publish, #{payload := <<"payload-0">>, client_pid := CSub1}} ->
            ok
    after 1000 ->
        ct:fail("Message not received by CSub1")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% Verify that the expired messages are not received
t_expired_messages(_Config) ->
    %% Create a non-lastvalue Queue
    _ = emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"expired_messages">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        data_retention_period => 1000
    }),

    %% Publish some messages to the queue
    emqx_mq_test_utils:populate(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-expired-">>
    }),

    %% Wait for the messages to expire
    ct:sleep(1000),

    %% Publish a few more messages
    ok = emqx_mq_test_utils:populate(5, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-new-">>
    }),

    %% Connect a client
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"expired_messages">>),

    %% Verify that only new messages are received
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 5, _Timeout = 1000),
    ?assertEqual(5, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify the queue handles acking messages from the finished generation
t_ack_from_finished_generation(_Config) ->
    %% Create a non-lastvalue Queue
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"ack_from_finished_generation">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        local_max_inflight => 20,
        stream_max_buffer_size => 16
    }),

    % Publish 10 messages to the queue
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Connect a client
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"ack_from_finished_generation">>),

    %% Drain the messages
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 2000),
    ?assertEqual(10, length(Msgs0)),

    %% Add a generation
    ok = emqx_mq_message_db:add_regular_db_generation(),
    ct:sleep(100),

    %% Acknowledge the messages and disconnect the client
    ok = emqx_mq_test_utils:emqtt_ack(Msgs0),
    ok = emqtt:disconnect(CSub0),

    %% Now publish some more messages and connect a new client
    emqx_mq_test_utils:populate(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-new-">>
    }),

    %% The new client should receive the new messages, the old one
    %% should have been successfully acknowledged
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"ack_from_finished_generation">>),
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 10, _Timeout1 = 1000),
    ?assertEqual(10, length(Msgs1)),
    ok = emqtt:disconnect(CSub1).

%% Verify the consumer progress is saved and restored correctly when
%% there are unacked messages in the finished generation
t_save_and_restore_unacked_messages_in_finished_generation(_Config) ->
    %% Create a non-lastvalue Queue
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"save_and_restore_unacked_messages_in_finished_generation">>,
        topic_filter => <<"t/#">>,
        is_lastvalue => false,
        local_max_inflight => 100,
        stream_max_buffer_size => 16
    }),

    % Publish 10 messages to the queue
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Connect a client
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(
        CSub0, <<"save_and_restore_unacked_messages_in_finished_generation">>
    ),

    %% Drain the messages but do not acknowledge them
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 2000),
    ?assertEqual(10, length(Msgs0)),

    %% Add a generation
    ok = emqx_mq_message_db:add_regular_db_generation(),
    ct:sleep(100),

    %% Drop the consumer
    ok = emqx_mq_test_utils:stop_all_consumers(),
    ok = emqtt:disconnect(CSub0),

    %% Publish some more messages (in the new generation)
    emqx_mq_test_utils:populate(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-new-">>
    }),

    %% Start the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(
        CSub1, <<"save_and_restore_unacked_messages_in_finished_generation">>
    ),

    %% Verify we receive the unacknowledged messages both
    %% from the old generation and the new one
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 20, _Timeout1 = 1000),
    ?assertEqual(20, length(Msgs1)),
    ok = emqtt:disconnect(CSub1).

%% Verify that the queue is eventually found even if it is not present when
%% subscribing
t_find_queue(_Config) ->
    emqx_config:put([mq, find_queue_retry_interval], 100),

    %% Connect a client and subscribe to a non-existent queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"noexistent">>),

    %% Create the queue
    emqx_mq_test_utils:ensure_mq_created(#{name => <<"noexistent">>, topic_filter => <<"t/#">>}),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Verify that the message is received
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),
    ?assertEqual(1, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the inspect functions work
t_inspect(_Config) ->
    %% Create queue and a client
    emqx_mq_test_utils:ensure_mq_created(#{name => <<"inspect">>, topic_filter => <<"t/#">>}),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}, {clientid, <<"csub">>}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"inspect">>),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Inspect the consumer and the subscriber
    [ConsumerRef] = emqx_mq_test_utils:all_consumers(),
    ?assertMatch(
        #{
            server := _,
            streams := _
        },
        emqx_mq_consumer:inspect(ConsumerRef, 1000)
    ),
    [ChanPid] = emqx_cm:lookup_channels(<<"csub">>),
    ?assertMatch(
        #{
            status := #{name := connected}
        },
        emqx_mq:inspect(ChanPid, <<"inspect">>)
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that
%% * the offline client does not receive messages
%% * the client receives messages when it reconnects
t_offline_session(_Config) ->
    %% Create a non-lastvalue Queue and two clients
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"offline_session">>, topic_filter => <<"t/#">>, dispatch_strategy => round_robin
    }),
    CSub0 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub0">>},
        {properties, #{'Session-Expiry-Interval' => 1000}},
        {clean_start, false}
    ]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"offline_session">>),
    CSub1 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub1">>},
        {properties, #{'Session-Expiry-Interval' => 1000}},
        {clean_start, false}
    ]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"offline_session">>),

    %% Disconnect the first client
    ok = emqtt:disconnect(CSub0),

    %% Publish messages to the queue
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% All messages should be received by the second client
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 5000),
    ?assertEqual(10, length(Msgs0)),
    lists:foreach(
        fun(#{client_pid := Pid}) ->
            ?assertEqual(CSub1, Pid)
        end,
        Msgs0
    ),

    %% Reconnect the first client
    CSub01 = emqx_mq_test_utils:emqtt_connect([
        {clientid, <<"csub0">>},
        {properties, #{'Session-Expiry-Interval' => 1000}},
        {clean_start, false}
    ]),

    %% Publish messages to the queue
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),

    %% Messages should be received by both clients
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 10, _Timeout1 = 5000),
    ?assertEqual(5, length([Pid || #{client_pid := Pid} <- Msgs1, Pid =:= CSub01])),
    ?assertEqual(5, length([Pid || #{client_pid := Pid} <- Msgs1, Pid =:= CSub1])),

    %% Clean up
    ok = emqtt:disconnect(CSub01),
    ok = emqtt:disconnect(CSub1).

%% Verify that the metrics are updated correctly
t_metrics(_Config) ->
    #{received_messages := ReceivedMessages0, inserted_messages := InsertedMessages0} =
        emqx_mq_metrics:get_counters(ds),

    %% Create a queue, publish and consume some messages
    _MQ = emqx_mq_test_utils:ensure_mq_created(#{name => <<"metrics">>, topic_filter => <<"t/#">>}),
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"t/">>}),
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"metrics">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),
    ok = emqtt:disconnect(CSub),
    ?assertEqual(10, length(Msgs)),

    %% Verify that the metrics are updated correctly
    #{received_messages := ReceivedMessages1, inserted_messages := InsertedMessages1} = emqx_mq_metrics:get_counters(
        ds
    ),
    ?assertEqual(10, ReceivedMessages1 - ReceivedMessages0),
    ?assertEqual(10, InsertedMessages1 - InsertedMessages0),
    #{received_messages := #{current := Current}} = emqx_mq_metrics:get_rates(ds),
    ?assert(Current > 0),

    %% Verify that other accessors work
    ?assert(is_integer(emqx_mq_metrics:get_quota_buffer_inbox_size())),
    emqx_mq_metrics:print_common_hists(),
    emqx_mq_metrics:print_flush_quota_hist(),
    emqx_mq_metrics:print_common_hists(regular_limited).

t_update_key_expression(_Config) ->
    %% Create a lastvalue Queue
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"update_key_expression">>, topic_filter => <<"t/#">>, is_lastvalue => true
    }),

    %% Publish 10 messages to the queue, with 10 keys
    %% In tests, the default key is "mq-key" user property.
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-old-">>,
        n_keys => 10
    }),

    %% Consume the messages from the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"update_key_expression">>),
    {ok, _Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 10, _Timeout0 = 1000),
    ok = emqtt:disconnect(CSub0),

    %% Update the key expression
    {ok, _} = emqx_mq_registry:update(<<"update_key_expression">>, #{
        is_lastvalue => true,
        key_expression =>
            <<"concat([message.from, message.headers.peername, message.headers.peerhost])">>
    }),

    %% Publish 10 more messages to the queue, with "mq-key" keys wich are ignored now.
    %% The key expression is now the same for all messages (based on client connection info).
    emqx_mq_test_utils:populate_lastvalue(10, #{
        topic_prefix => <<"t/">>,
        payload_prefix => <<"payload-new-">>,
        n_keys => 10
    }),
    %% Stop the consumer to emulate reconnect after some significant absence.
    %% Otherwise, the consumer will receive online messages for some time, and
    %% we will not see the lastvalue effect.
    emqx_mq_test_utils:stop_all_consumers(),

    %% Consume the messages from the queue
    %% We should receive only one message, because the key expression is the same for all messages.
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"update_key_expression">>),
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 1, _Timeout1 = 100),
    ?assertEqual(1, length(Msgs1)),

    %% Clean up
    ok = emqtt:disconnect(CSub1).

t_unsubscribe(_Config) ->
    %% Create a non-lastvalue Queue
    emqx_mq_test_utils:ensure_mq_created(#{name => <<"unsubscribe">>, topic_filter => <<"t/#">>}),

    %% Connect a client and subscribe to the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"unsubscribe">>),

    %% Verify that the client is subscribed to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>, payload_prefix => <<"payload-old-">>}),
    receive
        {publish, #{payload := <<"payload-old-", _/binary>>, client_pid := CSub}} ->
            ok
    after 1000 ->
        ct:fail("Message not received")
    end,

    %% Unsubscribe from the queue
    {ok, _, [0]} = emqtt:unsubscribe(CSub, <<"$queue/unsubscribe">>),

    %% Verify that the client is not subscribed to the queue
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>, payload_prefix => <<"payload-new-">>}),
    receive
        {publish, #{payload := <<"payload-new-", _/binary>>, client_pid := CSub}} ->
            ct:fail("Message received")
    after 1000 ->
        ok
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that we gracefully handle acks to a message from a lost consumer.
t_ack_to_message_from_lost_consumer(_Config) ->
    %% Create a non-lastvalue Queue
    emqx_mq_test_utils:ensure_mq_created(#{
        name => <<"ack_to_message_from_lost_consumer">>,
        topic_filter => <<"t/#">>,
        ping_interval => 100
    }),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t/">>}),

    %% Connect a client and subscribe to the queue
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"ack_to_message_from_lost_consumer">>),

    %% Drain the message
    {ok, [#{packet_id := PacketId}]} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 1000),

    %% Kill the consumer
    [Pid] = emqx_mq_test_utils:all_consumers(),
    ?assertWaitEvent(
        exit(Pid, kill),
        #{?snk_kind := mq_sub_consumer_timeout, mq_name := <<"ack_to_message_from_lost_consumer">>},
        1000
    ),

    %% Ack the message, check that the event is emitted implying that the ack was handled gracefully
    ?assertWaitEvent(
        ok = emqtt:puback(CSub, PacketId),
        #{?snk_kind := mq_on_delivery_completed_sub_not_found},
        1000
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the queue may be automatically created when a client subscribes to a non-existent queue
t_auto_create(_Config) ->
    %% Enable automatic creation of regular queues
    emqx:update_config([mq, auto_create], #{<<"regular">> => #{}, <<"lastvalue">> => false}),

    %% Connect a client and subscribe to a non-existent queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"auto_create">>, <<"non-existent/#">>),

    %% Verify that the queue was automatically created
    ?assertMatch({ok, #{name := <<"auto_create">>}}, emqx_mq_registry:find(<<"auto_create">>)),

    %% Publish and verfy some messages
    emqx_mq_test_utils:populate(10, #{topic_prefix => <<"non-existent/">>}),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 1000),
    ?assertEqual(10, length(Msgs)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the queue is not automatically created when automatic creation is disabled
t_auto_create_disabled(_Config) ->
    %% Ensure automatic creation of queues is disabled
    emqx:update_config([mq, auto_create], #{<<"regular">> => false, <<"lastvalue">> => false}),

    %% Connect a client and subscribe to a non-existent queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"auto_create_disabled">>, <<"non-existent/#">>),

    %% Verify that the queue was not automatically created
    ?assertEqual(not_found, emqx_mq_registry:find(<<"auto_create_disabled">>)),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Subscribe to the same name under different topic filters
t_conflicting_queues(_Config) ->
    %% Enable automatic creation of regular queues
    emqx:update_config([mq, auto_create], #{<<"regular">> => #{}, <<"lastvalue">> => false}),
    emqx_config:put([mq, find_queue_retry_interval], 100),

    %% Connect a client and subscribe to the queues
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    %% This queue will be created
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"cq">>, <<"t1/#">>),
    %% This queue will not be created, since cq is already created with topic filter t1/#
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"cq">>, <<"t2/#">>),

    %% Publish a message to the queue and verify that it is received
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t1/">>}),
    ?assertMatch(
        {ok, [#{topic := <<"t1/", _/binary>>}]},
        emqx_mq_test_utils:emqtt_drain(_MinMsg0 = 1, _Timeout0 = 1000)
    ),

    %% Now recreate the queue with the second topic filter and verify that the message is also received
    %% without reconnecting the client
    emqx_mq_test_utils:stop_all_consumers(),
    emqx_mq_registry:delete(<<"cq">>),
    emqx_mq_test_utils:ensure_mq_created(#{name => <<"cq">>, topic_filter => <<"t2/#">>}),
    emqx_mq_test_utils:populate(1, #{topic_prefix => <<"t2/">>}),
    ?assertMatch(
        {ok, [#{topic := <<"t2/", _/binary>>}]},
        emqx_mq_test_utils:emqtt_drain(_MinMsg1 = 1, _Timeout1 = 1000)
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that only MQTT v5 clients are allowed to subscribe to queues
t_allow_only_mqtt_v5(_Config) ->
    %% Connect a client and subscribe to a queue
    {ok, CSub} = emqtt:start_link([{proto_ver, v3}]),
    {ok, _} = emqtt:connect(CSub),

    %% Try to subscribe to a queue with MQTT v3
    {ok, _, [?RC_UNSPECIFIED_ERROR]} = emqtt:subscribe(CSub, {<<"$queue/some_queue/t/#">>, 1}),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

now_ms() ->
    erlang:system_time(millisecond).

wait_for_consumer_stop(#{id := Id} = _MQ, Ms) when Ms > 5 ->
    ?retry(
        5,
        1 + Ms div 5,
        ?assert(emqx_mq_consumer:find(Id) == not_found)
    ).
