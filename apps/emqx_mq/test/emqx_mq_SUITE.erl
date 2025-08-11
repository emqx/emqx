%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

all() ->
    [{group, all}, {group, redispatch}].

groups() ->
    All = emqx_common_test_helpers:all(?MODULE) -- [t_redispatch],
    [
        {all, [], All},
        {redispatch, [], [{group, random}, {group, least_inflight}]},
        {random, [], [t_redispatch]},
        {least_inflight, [], [t_redispatch]}
    ].

init_per_suite(Config) ->
    Apps =
        emqx_cth_suite:start(
            [
                {emqx_durable_storage, #{override_env => [{poll_batch_size, 1}]}},
                emqx,
                emqx_mq
            ],
            #{work_dir => emqx_cth_suite:work_dir(Config)}
        ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_group(DispatchStrategy, Config) when
    DispatchStrategy =:= random; DispatchStrategy =:= least_inflight
->
    [{dispatch_strategy, DispatchStrategy} | Config];
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
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Consume some history messages from a non-compacted queue
t_publish_and_consume(_Config) ->
    %% Create a non-compacted Queue
    _ = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(100, length(Msgs)).

%% Consume some history messages from a compacted queue
t_publish_and_consume_compacted(_Config) ->
    %% Create a non-compacted Queue
    _ = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => true}),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate_compacted(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                CompactionKey =
                    <<"k-", (integer_to_binary(I rem 10))/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload, CompactionKey}
            end
        ),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(10, length(Msgs)).

%% Verify that the consumer stops consuming DS messages once there is
%% a critical amount of unacked messages
t_backpressure(_Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            local_max_inflight => 100
        }),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Consume the messages from the queue
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
    emqx_config:put([mqtt, max_inflight], 0),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
    {ok, Msgs0} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = ?MQ_CONSUMER_MAX_BUFFER_SIZE, _Timeout = 200),

    %% Messages should stop being dispatched once the buffer is full and we acked nothing
    ?assert(
        length(Msgs0) =< ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED + 1,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs0), ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED + 1]
        )
    ),

    %% Acknowledge the messages
    ok =
        lists:foreach(
            fun(#{client_pid := Pid, packet_id := PacketId}) ->
                emqtt:puback(Pid, PacketId)
            end,
            Msgs0
        ),

    %% After acknowledging, the messages should start being dispatched again
    {ok, Msgs1} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = ?MQ_CONSUMER_MAX_BUFFER_SIZE, _Timeout = 200),
    ?assert(
        length(Msgs1) =< ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED * 2,
        binfmt(
            "Msgs received: ~p, expected less than or equal to: ~p",
            [length(Msgs1), ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED + 1]
        )
    ),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the consumer re-dispatches the message to another subscriber
%% if a subscriber received the message but disconnected before acknowledging it
t_redispatch_on_disconnect(_Config) ->
    %% Create a non-compacted Queue
    _ = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish just 1 message to the queue
    ok =
        emqx_mq_test_utils:populate(
            1,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Drain the message
    {ok, [#{client_pid := CSub, payload := <<"payload-0">>}]} =
        emqx_mq_test_utils:emqtt_drain(_MinMsg = 1, _Timeout = 100),

    %% Disconnect the subscriber which received the message
    ok = emqtt:disconnect(CSub),

    %% The other subscriber should receive the message now
    OtherCSub =
        case CSub of
            CSub0 ->
                CSub1;
            CSub1 ->
                CSub0
        end,
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
t_random_dispatch(_Config) ->
    %% Create a non-compacted Queue
    _ = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

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

%% Cooperatively consume online messages with hash dispatching
t_hash_dispatch(_Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => {hash, <<"m.topic(message)">>}
        }),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", (integer_to_binary(I rem 10))/binary>>,
                {Topic, Payload}
            end
        ),

    %% Drain the messages
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 500),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1),

    %% Verify the messages: check that messages with the same topic
    %% were not delivered to different clients
    ?assertEqual(100, length(Msgs)),
    ClientsByTopic =
        lists:foldl(
            fun(#{client_pid := Pid, topic := Topic}, Acc) ->
                case Acc of
                    #{Topic := Pids} -> Acc#{Topic => Pids#{Pid => true}};
                    _ -> Acc#{Topic => #{Pid => true}}
                end
            end,
            #{},
            Msgs
        ),
    ?assert(
        lists:all(
            fun({_Topic, Pids}) -> map_size(Pids) == 1 end,
            maps:to_list(ClientsByTopic)
        ),
        "Same topic delivered to different clients"
    ).

%% Cooperatively consume online messages with least inflight dispatching
t_least_inflight_dispatch(_Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => least_inflight
        }),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

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
    OtherCSub =
        case CSub of
            CSub0 ->
                CSub1;
            CSub1 ->
                CSub0
        end,
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
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            local_max_inflight => 4
        }),

    %% Publish 100 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Consume the messages from the queue
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
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
        emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
        #{
            ?snk_kind := mq_sub_handle_nack_session_busy,
            sub := #{mq_topic_filter := <<"t/#">>}
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
    %% Create a non-compacted Queue
    MQ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            consumer_max_inactive_ms => 50
        }),

    %% Publish 20 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            20,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Start to consume the messages from the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),

    %% Receive first message and NOT acknowledge it
    receive
        {publish, #{topic := <<"t/0">>, client_pid := CSub0}} ->
            ok
    after 100 ->
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
    after 100 ->
        ct:fail("t/1 message from MQ not received")
    end,

    %% Disconnect the client and wait for the consumer to stop and save the progress
    ok = emqtt:disconnect(CSub0),
    ok = wait_for_consumer_stop(MQ, 100),

    %% Start the client and the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

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
        ct:fail("t/0 message from MQ not received")
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
%% when the consumption buffer is full
t_progress_restoration_full_buffer(_Config) ->
    %% Create a non-compacted Queue
    MQ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            consumer_max_inactive_ms => 50,
            local_max_inflight => 100
        }),

    %% Publish 20 messages to the queue
    ok =
        emqx_mq_test_utils:populate(
            100,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

    %% Start to consume the messages from the queue
    emqx_config:put([mqtt, max_inflight], 100),
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),

    %% Receive all messages and acknowledge only the last one
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    #{packet_id := PacketId} = lists:last(Msgs0),
    ok = emqtt:puback(CSub0, PacketId),

    %% Disconnect the client and wait for the consumer to stop and save the progress
    ok = emqtt:disconnect(CSub0),
    ok = wait_for_consumer_stop(MQ, 100),

    %% Start the client and the consumer again
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Verify that we receive all messages
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    ?assertEqual(99, length(Msgs1), binfmt("Expected 99 messages, got: ~p", [length(Msgs1)])),

    %% Clean up
    ok = emqtt:disconnect(CSub1).

%% Verify that the consumer re-dispatches the message to another subscriber
%% if a subscriber rejected the message
t_redispatch_on_reject_random(_Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => random
        }),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 1 message to the queue
    ok =
        emqx_mq_test_utils:populate(
            1,
            fun(I) ->
                IBin = integer_to_binary(I),
                Payload = <<"payload-", IBin/binary>>,
                Topic = <<"t/", IBin/binary>>,
                {Topic, Payload}
            end
        ),

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
    OtherCSub =
        case CSub of
            CSub0 ->
                CSub1;
            CSub1 ->
                CSub0
        end,
    receive
        {publish, #{topic := <<"t/0">>, client_pid := OtherCSub}} ->
            ok
    after 100 ->
        ct:fail("t/0 message from MQ not received by the other subscriber")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%% Verify that the consumer re-dispatches the message to another subscriber
%% immediately if a subscriber rejected the message
%% (random & least_inflight dispatch strategies)
t_redispatch(Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => ?config(dispatch_strategy, Config),
            redispatch_interval_ms => 1000
        }),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 1 message to the queue
    ok = emqx_mq_test_utils:populate(1, fun(_I) -> {<<"t/0">>, <<"payload-0">>} end),

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
    OtherCSub =
        case CSub of
            CSub0 ->
                CSub1;
            CSub1 ->
                CSub0
        end,
    receive
        {publish, #{topic := <<"t/0">>, client_pid := OtherCSub}} ->
            ok
    after 100 ->
        ct:fail("t/0 message from MQ not received by the other subscriber")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%% Verify that the consumer re-dispatches the message to the same subscriber
%% after a delay if a subscriber rejected the message
%% (hash dispatch strategy)
t_redispatch_on_reject_hash(_Config) ->
    %% Create a non-compacted Queue
    _ =
        emqx_mq_test_utils:create_mq(#{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => {hash, <<"m.topic(message)">>},
            redispatch_interval_ms => 100
        }),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 1 message to the queue
    ok = emqx_mq_test_utils:populate(1, fun(_I) -> {<<"t/0">>, <<"payload-0">>} end),

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
        after 1000 ->
            ct:fail("t/0 message from MQ not received")
        end,

    %% Verify that the message is re-delivered to the same subscriber
    Now = now_ms(),
    receive
        {publish, #{topic := <<"t/0">>, client_pid := CSub}} ->
            ok
    after 200 ->
        ct:fail("t/0 message from MQ not received by the same subscriber")
    end,
    ?assert(now_ms() - Now >= 100, "Message was re-delivered too early"),

    %% Clean up
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

binfmt(Format, Args) ->
    iolist_to_binary(io_lib:format(Format, Args)).

now_ms() ->
    erlang:system_time(millisecond).

wait_for_consumer_stop(MQ, Ms) when Ms > 5 ->
    ?retry(
        5,
        1 + Ms div 5,
        ?assert(emqx_mq_consumer_db:find_consumer(MQ, now_ms()) == not_found)
    ).
