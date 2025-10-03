%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_consumer_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
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
    meck:unload(emqx_mq_consumer),

    %% Verify that the message is received within little threshold
    receive
        {publish, #{topic := <<"t/1">>, payload := <<"test message">>}} ->
            ok
    after 1000 ->
        ct:fail("message not received")
    end,

    %% Clean up
    ok = emqtt:disconnect(CSub).

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
