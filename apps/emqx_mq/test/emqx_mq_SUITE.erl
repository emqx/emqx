%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("../src/emqx_mq_internal.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
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

init_per_testcase(_CaseName, Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs(),
    Config.

end_per_testcase(_CaseName, _Config) ->
    ok = emqx_mq_test_utils:cleanup_mqs().

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

%% Consume some history messages from a non-compacted queue
t_publish_and_consume(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t/", IBin/binary>>,
        {Topic, Payload}
    end),

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
    ok = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => true}),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate_compacted(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        CompactionKey = <<"k-", (integer_to_binary(I rem 10))/binary>>,
        Topic = <<"t/", IBin/binary>>,
        {Topic, Payload, CompactionKey}
    end),

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
    ok = emqx_mq_test_utils:create_mq(#{
        topic_filter => <<"t/#">>,
        is_compacted => false,
        local_max_inflight => 100
    }),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t/", IBin/binary>>,
        {Topic, Payload}
    end),

    %% Consume the messages from the queue
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
    emqx_config:put([mqtt, max_inflight], 0),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t/#">>),
    {ok, Msgs0} = emqx_mq_test_utils:emqtt_drain(
        _MinMsg = ?MQ_CONSUMER_MAX_BUFFER_SIZE, _Timeout = 200
    ),

    %% Messages should stop being dispatched once the buffer is full and we acked nothing
    ?assert(length(Msgs0) =< ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED),

    %% Acknowledge the messages
    ok = lists:foreach(
        fun(#{client_pid := Pid, packet_id := PacketId}) ->
            emqtt:puback(Pid, PacketId)
        end,
        Msgs0
    ),

    %% After acknowledging, the messages should start being dispatched again
    {ok, Msgs1} = emqx_mq_test_utils:emqtt_drain(
        _MinMsg = ?MQ_CONSUMER_MAX_BUFFER_SIZE, _Timeout = 200
    ),
    ?assert(length(Msgs1) =< ?MQ_CONSUMER_MAX_BUFFER_SIZE + ?MQ_CONSUMER_MAX_UNACKED),

    %% Clean up
    ok = emqtt:disconnect(CSub).

%% Verify that the consumer re-dispatches the message to another subscriber
%% if a subscriber received the message but disconnected before acknowledging it
t_redispatch_on_disconnect(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Connect two subscribers
    CSub0 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish just 1 message to the queue
    ok = emqx_mq_test_utils:populate(1, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t/", IBin/binary>>,
        {Topic, Payload}
    end),

    %% Drain the message
    {ok, [#{client_pid := CSub, payload := <<"payload-0">>}]} = emqx_mq_test_utils:emqtt_drain(
        _MinMsg = 1, _Timeout = 100
    ),

    %% Disconnect the subscriber which received the message
    ok = emqtt:disconnect(CSub),

    %% The other subscriber should receive the message now
    OtherCSub =
        case CSub of
            CSub0 -> CSub1;
            CSub1 -> CSub0
        end,
    {ok, [#{client_pid := OtherCSub, payload := <<"payload-0">>, packet_id := PacketId}]} = emqx_mq_test_utils:emqtt_drain(
        _MinMsg = 1, _Timeout = 100
    ),

    %% Clean up
    ok = emqtt:puback(OtherCSub, PacketId),
    ok = emqtt:disconnect(OtherCSub).

%% Cooperatively consume online messages with random dispatching
t_random_dispatch(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(#{topic_filter => <<"t/#">>, is_compacted => false}),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t/", IBin/binary>>,
        {Topic, Payload}
    end),

    %% Drain the messages
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 500),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1),

    %% Verify the messages
    ?assertEqual(100, length(Msgs)),
    {Sub0Msgs, Sub1Msgs} = lists:partition(
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
    ok = emqx_mq_test_utils:create_mq(
        #{
            topic_filter => <<"t/#">>,
            is_compacted => false,
            dispatch_strategy => {hash, <<"m.topic(message)">>}
        }
    ),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t/#">>),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t/", (integer_to_binary(I rem 10))/binary>>,
        {Topic, Payload}
    end),

    %% Drain the messages
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 500),
    ok = emqtt:disconnect(CSub0),
    ok = emqtt:disconnect(CSub1),

    %% Verify the messages: check that messages with the same topic
    %% were not delivered to different clients
    ?assertEqual(100, length(Msgs)),
    ClientsByTopic = lists:foldl(
        fun(#{client_pid := Pid, topic := Topic}, Acc) ->
            case Acc of
                #{Topic := Pids} ->
                    Acc#{Topic => Pids#{Pid => true}};
                _ ->
                    Acc#{Topic => #{Pid => true}}
            end
        end,
        #{},
        Msgs
    ),
    ?assert(
        lists:all(fun({_Topic, Pids}) -> map_size(Pids) == 1 end, maps:to_list(ClientsByTopic)),
        "Same topic delivered to different clients"
    ).
