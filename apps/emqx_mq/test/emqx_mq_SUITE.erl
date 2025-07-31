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

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_publish_and_consume(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(<<"t1/#">>, false),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t1/", IBin/binary>>,
        {Topic, Payload}
    end),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t1/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 100, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(100, length(Msgs)).

t_publish_and_consume_compacted(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(<<"t2/#">>, true),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate_compacted(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        CompactionKey = <<"k-", (integer_to_binary(I rem 10))/binary>>,
        Topic = <<"t2/", IBin/binary>>,
        {Topic, Payload, CompactionKey}
    end),

    %% Consume the messages from the queue
    CSub = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t2/#">>),
    {ok, Msgs} = emqx_mq_test_utils:emqtt_drain(_MinMsg = 10, _Timeout = 100),
    ok = emqtt:disconnect(CSub),

    %% Verify the messages
    ?assertEqual(10, length(Msgs)).

t_cooperative_consumption(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(<<"t3/#">>, false),

    %% Subscribe to the queue
    CSub0 = emqx_mq_test_utils:emqtt_connect([]),
    CSub1 = emqx_mq_test_utils:emqtt_connect([]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub0, <<"t3/#">>),
    emqx_mq_test_utils:emqtt_sub_mq(CSub1, <<"t3/#">>),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t3/", IBin/binary>>,
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

t_backpressure(_Config) ->
    %% Create a non-compacted Queue
    ok = emqx_mq_test_utils:create_mq(<<"t4/#">>, false),

    %% Publish 100 messages to the queue
    ok = emqx_mq_test_utils:populate(100, fun(I) ->
        IBin = integer_to_binary(I),
        Payload = <<"payload-", IBin/binary>>,
        Topic = <<"t4/", IBin/binary>>,
        {Topic, Payload}
    end),

    %% Consume the messages from the queue
    %% Set max_inflight to 0 to avoid nacking messages by the client's session
    emqx_config:put([mqtt, max_inflight], 0),
    CSub = emqx_mq_test_utils:emqtt_connect([{auto_ack, false}]),
    emqx_mq_test_utils:emqtt_sub_mq(CSub, <<"t4/#">>),
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
