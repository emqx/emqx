%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_cluster_SUITE).

%% NOTE
%% Streams currently involve cluster operations only implicitly
%% as DS consumers.
%% There is no dataplane APIs for streams yet.

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(PUB_PORT, 20000).
-define(SUB0_PORT, 20100).
-define(SUB1_PORT, 20200).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TCName, Config) ->
    Apps = [
        emqx_durable_storage,
        {emqx, emqx_streams_test_utils:cth_config(emqx)},
        {emqx_mq, emqx_streams_test_utils:cth_config(emqx_mq)},
        {emqx_streams,
            emqx_streams_test_utils:cth_config(emqx_streams, #{
                <<"streams">> => #{<<"check_stream_status_interval">> => <<"100ms">>},
                <<"durable_storage">> => #{<<"streams_messages">> => #{<<"n_shards">> => 2}}
            })}
    ],
    ClusterSpec = [
        {pub, #{apps => Apps, base_port => ?PUB_PORT}},
        {sub0, #{apps => Apps, base_port => ?SUB0_PORT}},
        {sub1, #{apps => Apps, base_port => ?SUB1_PORT}}
    ],
    Nodes = emqx_cth_cluster:start(
        ClusterSpec,
        #{work_dir => emqx_cth_suite:work_dir(TCName, Config)}
    ),
    ok = snabbkaffe:start_trace(),
    %% Wait for the cluster to stabilize. TODO: this should be avoided
    %% by making `emqx_ds:wait_db' taking more readiness conditions
    %% into account, and by better error propagation, in particular
    %% between DS and MQTT publisher client.
    ct:sleep(5_000),
    [{nodes, Nodes} | Config].

end_per_testcase(_TCName, Config) ->
    Nodes = ?config(nodes, Config),
    ok = snabbkaffe:stop(),
    ok = emqx_cth_cluster:stop(Nodes).

%% Test that inter-cluster message dispatching works
t_cluster_smoke(Config) ->
    [PubNode, Sub0Node, Sub1Node] = _Nodes = ?config(nodes, Config),

    %% Create Message Queue and make sure it is available on all nodes
    _Stream = erpc:call(PubNode, emqx_streams_test_utils, create_stream, [
        #{topic_filter => <<"q/1">>, is_lastvalue => true}
    ]),
    ?retry(
        5,
        100,
        begin
            ?assertMatch(
                {ok, #{topic_filter := <<"q/1">>}},
                erpc:call(Sub0Node, emqx_streams_registry, find, [<<"q/1">>])
            ),
            ?assertMatch(
                {ok, #{topic_filter := <<"q/1">>}},
                erpc:call(Sub1Node, emqx_streams_registry, find, [<<"q/1">>])
            )
        end
    ),

    %% Create subscribers and subscribe to the stream
    SubClient0 = emqx_streams_test_utils:emqtt_connect([{port, ?SUB0_PORT}]),
    SubClient1 = emqx_streams_test_utils:emqtt_connect([{port, ?SUB1_PORT}]),
    SubClient2 = emqx_streams_test_utils:emqtt_connect([{port, ?SUB1_PORT}]),
    ok = emqx_streams_test_utils:emqtt_sub(SubClient0, <<"$sp/0/earliest/q/1">>),
    ok = emqx_streams_test_utils:emqtt_sub(SubClient1, <<"$sp/1/earliest/q/1">>),
    ok = emqx_streams_test_utils:emqtt_sub(SubClient2, <<"$s/earliest/q/1">>),

    %% Publish 100 messages to the queue
    PubClient = emqx_streams_test_utils:emqtt_connect([{port, ?PUB_PORT}]),
    ok = lists:foreach(
        fun(I) ->
            IBin = integer_to_binary(I),
            emqx_streams_test_utils:emqtt_pub_stream(
                PubClient, <<"q/1">>, <<"msg-", IBin/binary>>, #{key => IBin}
            )
        end,
        lists:seq(0, 99)
    ),

    %% Make sure the messages are distributed more-or-less evenly between partition subscribers.
    %% Make sure that the whole stream subscriber receives all messages.
    {ok, Msgs} = emqx_streams_test_utils:emqtt_drain(_MinMsg = 200, _Timeout = 2000),
    MessagesByClient = lists:foldl(
        fun(#{client_pid := Pid} = Msg, Acc) ->
            maps:update_with(Pid, fun(ClientMsgs) -> [Msg | ClientMsgs] end, [Msg], Acc)
        end,
        #{},
        Msgs
    ),
    Client0Cnt = length(maps:get(SubClient0, MessagesByClient)),
    Client1Cnt = length(maps:get(SubClient1, MessagesByClient)),
    Client2Cnt = length(maps:get(SubClient2, MessagesByClient)),
    ?assert(Client0Cnt > 30),
    ?assert(Client1Cnt > 30),
    ?assertEqual(100, Client0Cnt + Client1Cnt),
    ?assertEqual(100, Client2Cnt),

    %% Clean up
    ok = emqtt:disconnect(PubClient),
    ok = emqtt:disconnect(SubClient0),
    ok = emqtt:disconnect(SubClient1),
    ok = emqtt:disconnect(SubClient2).
