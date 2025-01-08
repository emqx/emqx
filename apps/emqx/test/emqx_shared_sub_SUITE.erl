%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_shared_sub_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(SUITE, ?MODULE).

-define(WAIT(TIMEOUT, PATTERN, Res),
    (fun() ->
        receive
            PATTERN ->
                Res;
            Other ->
                ct:fail(#{
                    expected => ??PATTERN,
                    got => Other
                })
        after TIMEOUT ->
            ct:fail({timeout, ??PATTERN})
        end
    end)()
).

-define(ack, shared_sub_ack).
-define(no_ack, no_ack).

all() -> emqx_common_test_helpers:all(?SUITE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start([emqx], #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({'init', Config})
    catch
        error:function_clause ->
            Config
    end.

end_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({'end', Config})
    catch
        error:function_clause ->
            ok
    end.

t_is_ack_required(Config) when is_list(Config) ->
    ?assertEqual(false, emqx_shared_sub:is_ack_required(#message{headers = #{}})).

t_maybe_nack_dropped(Config) when is_list(Config) ->
    ?assertEqual(false, emqx_shared_sub:maybe_nack_dropped(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {<<"group">>, self(), for_test}}},
    ?assertEqual(true, emqx_shared_sub:maybe_nack_dropped(Msg)),
    ?assertEqual(
        ok,
        receive
            {for_test, {shared_sub_nack, dropped}} -> ok
        after 100 -> timeout
        end
    ).

t_nack_no_connection(Config) when is_list(Config) ->
    Msg = #message{headers = #{shared_dispatch_ack => {<<"group">>, self(), for_test}}},
    ?assertEqual(ok, emqx_shared_sub:nack_no_connection(Msg)),
    ?assertEqual(
        ok,
        receive
            {for_test, {shared_sub_nack, no_connection}} -> ok
        after 100 -> timeout
        end
    ).

t_maybe_ack(Config) when is_list(Config) ->
    ?assertEqual(#message{headers = #{}}, emqx_shared_sub:maybe_ack(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {<<"group">>, self(), for_test}}},
    ?assertEqual(
        #message{headers = #{shared_dispatch_ack => ?no_ack}},
        emqx_shared_sub:maybe_ack(Msg)
    ),
    ?assertEqual(
        ok,
        receive
            {for_test, ?ack} -> ok
        after 100 -> timeout
        end
    ).

t_random_basic(Config) when is_list(Config) ->
    ok = ensure_config(random),
    ClientId = <<"ClientId">>,
    Topic = <<"foo">>,
    Payload = <<"hello">>,
    Group = <<"group1">>,
    emqx_broker:subscribe(emqx_topic:make_shared_record(Group, Topic), #{qos => 2}),
    MsgQoS2 = emqx_message:make(ClientId, 2, Topic, Payload),
    %% wait for the subscription to show up
    ct:sleep(200),
    ?assertEqual(true, subscribed(<<"group1">>, Topic, self())),
    emqx:publish(MsgQoS2),
    receive
        {deliver, Topic0, #message{
            from = ClientId0,
            payload = Payload0
        }} = M ->
            ct:pal("==== received: ~p", [M]),
            ?assertEqual(Topic, Topic0),
            ?assertEqual(ClientId, ClientId0),
            ?assertEqual(Payload, Payload0)
    after 1000 -> ct:fail(waiting_basic_failed)
    end,
    ok.

%% Start two subscribers share subscribe to "$share/g1/foo/bar"
%% Set 'sticky' dispatch strategy, send 1st message to find
%% out which member it picked, then close its connection
%% send the second message, the message should be 'nack'ed
%% by the sticky session and delivered to the 2nd session.
%% After the connection for the 2nd session is also closed,
%% i.e. when all clients are offline, the following message(s)
%% should be delivered randomly.
t_no_connection_nack(Config) when is_list(Config) ->
    ok = ensure_config(sticky),
    Publisher = <<"publisher">>,
    Subscriber1 = <<"Subscriber1">>,
    Subscriber2 = <<"Subscriber2">>,
    QoS = 1,
    Group = <<"g1">>,
    Topic = <<"foo/bar">>,
    ShareTopic = <<"$share/", Group/binary, $/, Topic/binary>>,

    ExpProp = [{properties, #{'Session-Expiry-Interval' => timer:seconds(30)}}],
    {ok, SubConnPid1} = emqtt:start_link([{clientid, Subscriber1}] ++ ExpProp),
    {ok, _Props1} = emqtt:connect(SubConnPid1),
    {ok, SubConnPid2} = emqtt:start_link([{clientid, Subscriber2}] ++ ExpProp),
    {ok, _Props2} = emqtt:connect(SubConnPid2),
    emqtt:subscribe(SubConnPid1, ShareTopic, QoS),
    emqtt:subscribe(SubConnPid1, ShareTopic, QoS),

    %% wait for the subscriptions to show up
    ct:sleep(200),
    MkPayload = fun(PacketId) ->
        iolist_to_binary(["hello-", integer_to_list(PacketId)])
    end,
    SendF = fun(PacketId) ->
        M = emqx_message:make(Publisher, QoS, Topic, MkPayload(PacketId)),
        emqx:publish(M#message{id = PacketId})
    end,
    SendF(1),
    timer:sleep(200),
    %% This is the connection which was picked by broker to dispatch (sticky) for 1st message

    ?assertMatch([#{packet_id := 1}], recv_msgs(1)),
    ok.

t_random(Config) when is_list(Config) ->
    ok = ensure_config(random, true),
    test_two_messages(random).

t_round_robin(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, true),
    test_two_messages(round_robin).

t_round_robin_per_group(Config) when is_list(Config) ->
    ok = ensure_config(round_robin_per_group, true),
    test_two_messages(round_robin_per_group).

%% this would fail if executed with the standard round_robin strategy
t_round_robin_per_group_even_distribution_one_group(Config) when is_list(Config) ->
    ok = ensure_config(round_robin_per_group, true),
    Topic = <<"foo/bar">>,
    Group = <<"group1">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, <<"C0">>}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, <<"C1">>}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/", Topic/binary>>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/", Topic/binary>>, 0}),

    %% publisher with persistent connection
    {ok, PublisherPid} = emqtt:start_link(),
    {ok, _} = emqtt:connect(PublisherPid),

    lists:foreach(
        fun(I) ->
            Message = erlang:integer_to_binary(I),
            emqtt:publish(PublisherPid, Topic, Message)
        end,
        lists:seq(0, 9)
    ),

    AllReceivedMessages = lists:map(
        fun(#{client_pid := SubscriberPid, payload := Payload}) -> {SubscriberPid, Payload} end,
        lists:reverse(recv_msgs(10))
    ),
    MessagesReceivedSubscriber1 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid1 end, AllReceivedMessages
    ),
    MessagesReceivedSubscriber2 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid2 end, AllReceivedMessages
    ),

    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    emqtt:stop(PublisherPid),

    %% ensure each subscriber received 5 messages in alternating fashion
    %% one receives all even and the other all uneven payloads
    ?assertEqual(
        [
            {ConnPid1, <<"0">>},
            {ConnPid1, <<"2">>},
            {ConnPid1, <<"4">>},
            {ConnPid1, <<"6">>},
            {ConnPid1, <<"8">>}
        ],
        MessagesReceivedSubscriber1
    ),

    ?assertEqual(
        [
            {ConnPid2, <<"1">>},
            {ConnPid2, <<"3">>},
            {ConnPid2, <<"5">>},
            {ConnPid2, <<"7">>},
            {ConnPid2, <<"9">>}
        ],
        MessagesReceivedSubscriber2
    ),
    ok.

t_round_robin_per_group_even_distribution_two_groups(Config) when is_list(Config) ->
    ok = ensure_config(round_robin_per_group, true),
    Topic = <<"foo/bar">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, <<"C0">>}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, <<"C1">>}]),
    {ok, ConnPid3} = emqtt:start_link([{clientid, <<"C2">>}]),
    {ok, ConnPid4} = emqtt:start_link([{clientid, <<"C3">>}]),
    ConnPids = [ConnPid1, ConnPid2, ConnPid3, ConnPid4],
    lists:foreach(fun(P) -> emqtt:connect(P) end, ConnPids),

    %% group1 subscribers
    emqtt:subscribe(ConnPid1, {<<"$share/group1/", Topic/binary>>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/group1/", Topic/binary>>, 0}),
    %% group2 subscribers
    emqtt:subscribe(ConnPid3, {<<"$share/group2/", Topic/binary>>, 0}),
    emqtt:subscribe(ConnPid4, {<<"$share/group2/", Topic/binary>>, 0}),

    publish_fire_and_forget(10, Topic),

    AllReceivedMessages = lists:map(
        fun(#{client_pid := SubscriberPid, payload := Payload}) -> {SubscriberPid, Payload} end,
        lists:reverse(recv_msgs(20))
    ),
    MessagesReceivedSubscriber1 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid1 end, AllReceivedMessages
    ),
    MessagesReceivedSubscriber2 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid2 end, AllReceivedMessages
    ),
    MessagesReceivedSubscriber3 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid3 end, AllReceivedMessages
    ),
    MessagesReceivedSubscriber4 = lists:filter(
        fun({P, _Payload}) -> P == ConnPid4 end, AllReceivedMessages
    ),

    lists:foreach(fun(P) -> emqtt:stop(P) end, ConnPids),

    %% ensure each subscriber received 5 messages in alternating fashion in each group
    %% subscriber 1 and 3 should receive all even messages
    %% subscriber 2 and 4 should receive all uneven messages
    ?assertEqual(
        [
            {ConnPid3, <<"0">>},
            {ConnPid3, <<"2">>},
            {ConnPid3, <<"4">>},
            {ConnPid3, <<"6">>},
            {ConnPid3, <<"8">>}
        ],
        MessagesReceivedSubscriber3
    ),

    ?assertEqual(
        [
            {ConnPid2, <<"1">>},
            {ConnPid2, <<"3">>},
            {ConnPid2, <<"5">>},
            {ConnPid2, <<"7">>},
            {ConnPid2, <<"9">>}
        ],
        MessagesReceivedSubscriber2
    ),

    ?assertEqual(
        [
            {ConnPid4, <<"1">>},
            {ConnPid4, <<"3">>},
            {ConnPid4, <<"5">>},
            {ConnPid4, <<"7">>},
            {ConnPid4, <<"9">>}
        ],
        MessagesReceivedSubscriber4
    ),

    ?assertEqual(
        [
            {ConnPid1, <<"0">>},
            {ConnPid1, <<"2">>},
            {ConnPid1, <<"4">>},
            {ConnPid1, <<"6">>},
            {ConnPid1, <<"8">>}
        ],
        MessagesReceivedSubscriber1
    ),
    ok.

t_sticky(Config) when is_list(Config) ->
    ok = ensure_config(sticky, true),
    test_two_messages(sticky).

t_sticky_initial_pick_hash_clientid(Config) when is_list(Config) ->
    ok = ensure_config(sticky, hash_clientid, false),
    test_two_messages(sticky).

t_sticky_initial_pick_hash_topic(Config) when is_list(Config) ->
    ok = ensure_config(sticky, hash_topic, false),
    test_two_messages(sticky).

%% two subscribers in one shared group
%% one unsubscribe after receiving a message
%% the other one in the group should receive the next message
t_sticky_unsubscribe(Config) when is_list(Config) ->
    ok = ensure_config(sticky, false),
    Topic = <<"foo/bar/sticky-unsub">>,
    ClientId1 = <<"c1-sticky-unsub">>,
    ClientId2 = <<"c2-sticky-unsub">>,
    Group = <<"gsu">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    ShareTopic = <<"$share/", Group/binary, "/", Topic/binary>>,
    emqtt:subscribe(ConnPid1, {ShareTopic, 0}),
    emqtt:subscribe(ConnPid2, {ShareTopic, 0}),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),
    ct:sleep(100),

    emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1, ConnPid2]),
    emqtt:unsubscribe(UsedSubPid1, ShareTopic),
    emqx:publish(Message2),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1, ConnPid2]),
    ?assertNotEqual(UsedSubPid1, UsedSubPid2),

    kill_process(ConnPid1, fun(_) -> emqtt:stop(ConnPid1) end),
    kill_process(ConnPid2, fun(_) -> emqtt:stop(ConnPid2) end),
    ok.

t_hash(Config) when is_list(Config) ->
    ok = ensure_config(hash_clientid, false),
    test_two_messages(hash_clientid).

t_hash_clientid(Config) when is_list(Config) ->
    ok = ensure_config(hash_clientid, false),
    test_two_messages(hash_clientid).

t_hash_topic(Config) when is_list(Config) ->
    ok = ensure_config(hash_topic, false),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid2),

    Topic1 = <<"foo/bar1">>,
    Topic2 = <<"foo/bar2">>,
    ?assert(erlang:phash2(Topic1) rem 2 =/= erlang:phash2(Topic2) rem 2),
    Message1 = emqx_message:make(ClientId1, 0, Topic1, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, Topic2, <<"hello2">>),
    emqtt:subscribe(ConnPid1, {<<"$share/group1/foo/#">>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/group1/foo/#">>, 0}),
    ct:sleep(100),
    emqx:publish(Message1),
    Me = self(),
    WaitF = fun(ExpectedPayload) ->
        case last_message(ExpectedPayload, [ConnPid1, ConnPid2]) of
            {true, Pid} ->
                Me ! {subscriber, Pid},
                true;
            Other ->
                Other
        end
    end,
    WaitF(<<"hello1">>),
    UsedSubPid1 =
        receive
            {subscriber, P1} -> P1
        end,
    emqx_broker:publish(Message2),
    WaitF(<<"hello2">>),
    UsedSubPid2 =
        receive
            {subscriber, P2} -> P2
        end,
    ?assert(UsedSubPid1 =/= UsedSubPid2),
    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    ok.

%% if the original subscriber dies, change to another one alive
t_not_so_sticky(Config) when is_list(Config) ->
    ok = ensure_config(sticky),
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, C1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(C1),
    {ok, C2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(C2),

    emqtt:subscribe(C1, {<<"$share/group1/foo/bar">>, 0}),
    timer:sleep(50),
    emqtt:publish(C2, <<"foo/bar">>, <<"hello1">>),
    ?assertMatch([#{payload := <<"hello1">>}], recv_msgs(1)),

    emqtt:unsubscribe(C1, <<"$share/group1/foo/bar">>),
    timer:sleep(50),
    emqtt:subscribe(C1, {<<"$share/group1/foo/#">>, 0}),
    timer:sleep(50),
    emqtt:publish(C2, <<"foo/bar">>, <<"hello2">>),
    ?assertMatch([#{payload := <<"hello2">>}], recv_msgs(1)),
    emqtt:disconnect(C1),
    emqtt:disconnect(C2),
    ok.

test_two_messages(Strategy) ->
    test_two_messages(Strategy, <<"group1">>).

test_two_messages(Strategy, Group) ->
    Topic = <<"foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/", Topic/binary>>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/", Topic/binary>>, 0}),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),
    ct:sleep(100),

    emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1, ConnPid2]),

    emqx:publish(Message2),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1, ConnPid2]),

    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),

    case Strategy of
        sticky -> ?assertEqual(UsedSubPid1, UsedSubPid2);
        round_robin -> ?assertNotEqual(UsedSubPid1, UsedSubPid2);
        round_robin_per_group -> ?assertNotEqual(UsedSubPid1, UsedSubPid2);
        hash_clientid -> ?assertEqual(UsedSubPid1, UsedSubPid2);
        _ -> ok
    end,
    ok.

last_message(ExpectedPayload, Pids) ->
    last_message(ExpectedPayload, Pids, 1000).

last_message(ExpectedPayload, Pids, Timeout) ->
    receive
        {publish, #{client_pid := Pid, payload := ExpectedPayload}} ->
            ?assert(lists:member(Pid, Pids)),
            {true, Pid}
    after Timeout ->
        ct:pal("not yet"),
        <<"not yet?">>
    end.

t_dispatch(Config) when is_list(Config) ->
    ok = ensure_config(random),
    Topic = <<"foo">>,
    Group = <<"group1">>,
    ?assertEqual(
        {error, no_subscribers},
        emqx_shared_sub:dispatch(Group, Topic, #delivery{message = #message{}})
    ),
    emqx_broker:subscribe(emqx_topic:make_shared_record(Group, Topic), #{qos => 2}),
    ?assertEqual(
        {ok, 1},
        emqx_shared_sub:dispatch(Group, Topic, #delivery{message = #message{}})
    ).

t_uncovered_func(Config) when is_list(Config) ->
    ignored = gen_server:call(emqx_shared_sub, ignored),
    ok = gen_server:cast(emqx_shared_sub, ignored),
    ignored = emqx_shared_sub ! ignored,
    {mnesia_table_event, []} = emqx_shared_sub ! {mnesia_table_event, []}.

t_per_group_config(Config) when is_list(Config) ->
    ok = ensure_group_config(#{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky,
        <<"sticky_group_initial_pick_hash_clientid">> => {sticky, hash_clientid},
        <<"sticky_group_initial_pick_hash_topic">> => {sticky, hash_topic},
        <<"round_robin_per_group_group">> => round_robin_per_group
    }),
    %% Each test is repeated 4 times because random strategy may technically pass the test
    %% so we run 8 tests to make random pass in only 1/256 runs

    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_clientid">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_clientid">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_topic">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_topic">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_clientid">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_clientid">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_topic">>),
    test_two_messages(sticky, <<"sticky_group_initial_pick_hash_topic">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(round_robin_per_group, <<"round_robin_per_group_group">>),
    test_two_messages(round_robin_per_group, <<"round_robin_per_group_group">>).

t_local(Config) when is_list(Config) ->
    GroupConfig = #{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky
    },

    Node = start_peer('local_shared_sub_local_1', 21999),
    ok = ensure_group_config(GroupConfig),
    ok = ensure_group_config(Node, GroupConfig),

    Topic = <<"local_foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {port, 21999}]),

    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/local_group/", Topic/binary>>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/local_group/", Topic/binary>>, 0}),

    ct:sleep(100),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),

    emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1, ConnPid2]),

    rpc:call(Node, emqx, publish, [Message2]),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1, ConnPid2]),
    RemoteLocalGroupStrategy = rpc:call(Node, emqx_shared_sub, strategy, [<<"local_group">>]),

    emqtt:stop(ConnPid1),
    emqtt:stop(ConnPid2),
    stop_peer(Node),

    ?assertEqual(local, emqx_shared_sub:strategy(<<"local_group">>)),
    ?assertEqual(local, RemoteLocalGroupStrategy),

    ?assertNotEqual(UsedSubPid1, UsedSubPid2),
    ok.

t_remote(Config) when is_list(Config) ->
    %% This testcase verifies dispatching of shared messages to the remote nodes via backplane API.
    %%
    %% In this testcase we start two EMQX nodes: local and remote.
    %% A subscriber connects to the remote node.
    %% A publisher connects to the local node and sends three messages with different QoS.
    %% The test verifies that the remote side received all three messages.
    ok = ensure_config(sticky, true),
    GroupConfig = #{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky
    },

    Node = start_peer('remote_shared_sub_remote_1', 21999),
    ok = ensure_group_config(GroupConfig),
    ok = ensure_group_config(Node, GroupConfig),

    Topic = <<"foo/bar">>,
    ClientIdLocal = <<"ClientId1">>,
    ClientIdRemote = <<"ClientId2">>,

    {ok, ConnPidLocal} = emqtt:start_link([{clientid, ClientIdLocal}]),
    {ok, ConnPidRemote} = emqtt:start_link([{clientid, ClientIdRemote}, {port, 21999}]),

    try
        {ok, ClientPidLocal} = emqtt:connect(ConnPidLocal),
        {ok, _ClientPidRemote} = emqtt:connect(ConnPidRemote),

        emqtt:subscribe(ConnPidRemote, {<<"$share/remote_group/", Topic/binary>>, 0}),

        ct:sleep(100),

        Message1 = emqx_message:make(ClientPidLocal, 0, Topic, <<"hello1">>),
        Message2 = emqx_message:make(ClientPidLocal, 1, Topic, <<"hello2">>),
        Message3 = emqx_message:make(ClientPidLocal, 2, Topic, <<"hello3">>),

        emqx:publish(Message1),
        {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPidRemote]),

        emqx:publish(Message2),
        {true, UsedSubPid1} = last_message(<<"hello2">>, [ConnPidRemote]),

        emqx:publish(Message3),
        {true, UsedSubPid1} = last_message(<<"hello3">>, [ConnPidRemote]),

        ok
    after
        emqtt:stop(ConnPidLocal),
        emqtt:stop(ConnPidRemote),
        stop_peer(Node)
    end.

t_local_fallback(Config) when is_list(Config) ->
    ok = ensure_group_config(#{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky
    }),

    Topic = <<"local_foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    Node = start_peer('local_fallback_shared_sub_1', 11888),

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),

    emqtt:subscribe(ConnPid1, {<<"$share/local_group/", Topic/binary>>, 0}),

    emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1]),

    rpc:call(Node, emqx, publish, [Message2]),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1], 2_000),

    emqtt:stop(ConnPid1),
    stop_peer(Node),

    ?assertEqual(UsedSubPid1, UsedSubPid2),
    ok.

%% This one tests that broker tries to select another shared subscriber
%% If the first one doesn't return an ACK
t_redispatch_qos1_with_ack(Config) when is_list(Config) ->
    test_redispatch_qos1(Config, true).

t_redispatch_qos1_no_ack(Config) when is_list(Config) ->
    test_redispatch_qos1(Config, false).

test_redispatch_qos1(_Config, AckEnabled) ->
    ok = ensure_config(sticky, AckEnabled),
    Group = <<"group1">>,
    Topic = <<"foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {auto_ack, false}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {auto_ack, false}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/foo/bar">>, 1}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/foo/bar">>, 1}),

    Message = emqx_message:make(ClientId1, 1, Topic, <<"hello1">>),

    emqx:publish(Message),

    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1, ConnPid2]),
    ok = emqtt:stop(UsedSubPid1),

    Res = last_message(<<"hello1">>, [ConnPid1, ConnPid2], 6000),
    ?assertMatch({true, Pid} when Pid =/= UsedSubPid1, Res),

    {true, UsedSubPid2} = Res,
    emqtt:stop(UsedSubPid2),
    ok.

t_qos1_random_dispatch_if_all_members_are_down(Config) when is_list(Config) ->
    ok = ensure_config(sticky, true),
    Group = <<"group1">>,
    Topic = <<"foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    SubOpts = [{clean_start, false}],
    {ok, ConnPub} = emqtt:start_link([{clientid, <<"pub">>}]),
    {ok, _} = emqtt:connect(ConnPub),

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1} | SubOpts]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2} | SubOpts]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/foo/bar">>, 1}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/foo/bar">>, 1}),

    ok = emqtt:stop(ConnPid1),
    ok = emqtt:stop(ConnPid2),

    [Pid1, Pid2] = emqx_shared_sub:subscribers(Group, Topic),
    ?assert(is_process_alive(Pid1)),
    ?assert(is_process_alive(Pid2)),

    {ok, _} = emqtt:publish(ConnPub, Topic, <<"hello11">>, 1),
    ?retry(
        100,
        10,
        begin
            Msgs1 = emqx_mqueue:to_list(get_mqueue(Pid1)),
            Msgs2 = emqx_mqueue:to_list(get_mqueue(Pid2)),
            %% assert the message is in mqueue (because socket is closed)
            ?assertMatch([#message{payload = <<"hello11">>}], Msgs1 ++ Msgs2)
        end
    ),
    emqtt:stop(ConnPub),
    ok.

get_mqueue(ConnPid) ->
    emqx_connection:info({channel, {session, mqueue}}, sys:get_state(ConnPid)).

%% No ack, QoS 2 subscriptions,
%% client1 receives one message, send pubrec, then suspend
%% client2 acts normal (auto_ack=true)
%% Expected behaviour:
%% the messages sent to client1's inflight and mq are re-dispatched after client1 is down
t_dispatch_qos2({init, Config}) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = false),
    emqx_config:put_zone_conf(default, [mqtt, max_inflight], 1),
    Config;
t_dispatch_qos2({'end', Config}) when is_list(Config) ->
    emqx_config:put_zone_conf(default, [mqtt, max_inflight], 0);
t_dispatch_qos2(Config) when is_list(Config) ->
    Topic = <<"foo/bar/1">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {auto_ack, false}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {auto_ack, true}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/group/foo/bar/#">>, 2}),
    emqtt:subscribe(ConnPid2, {<<"$share/group/foo/bar/#">>, 2}),

    Message1 = emqx_message:make(ClientId1, 2, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 2, Topic, <<"hello2">>),
    Message3 = emqx_message:make(ClientId1, 2, Topic, <<"hello3">>),
    Message4 = emqx_message:make(ClientId1, 2, Topic, <<"hello4">>),
    ct:sleep(100),

    ok = sys:suspend(ConnPid1),

    %% One message is inflight
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message1)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message2)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message3)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message4)),

    %% assert client 2 receives two messages, they are eiter 1,3 or 2,4 depending
    %% on if it's picked as the first one for round_robin
    MsgRec1 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P1}}, P1),
    MsgRec2 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P2}}, P2),
    case MsgRec2 of
        <<"hello3">> ->
            ?assertEqual(<<"hello1">>, MsgRec1);
        <<"hello4">> ->
            ?assertEqual(<<"hello2">>, MsgRec1)
    end,
    sys:resume(ConnPid1),
    %% emqtt subscriber automatically sends PUBREC, but since auto_ack is set to false
    %% so it will never send PUBCOMP, hence EMQX should not attempt to send
    %% the 4th message yet since max_inflight is 1.
    MsgRec3 = ?WAIT(2000, {publish, #{client_pid := ConnPid1, payload := P3}}, P3),
    ct:sleep(100),
    %% no message expected
    ?assertEqual([], collect_msgs(0)),
    %% now kill client 1
    kill_process(ConnPid1),
    %% client 2 should receive the message
    MsgRec4 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P4}}, P4),
    case MsgRec2 of
        <<"hello3">> ->
            ?assertEqual(<<"hello2">>, MsgRec3),
            ?assertEqual(<<"hello4">>, MsgRec4);
        <<"hello4">> ->
            ?assertEqual(<<"hello1">>, MsgRec3),
            ?assertEqual(<<"hello3">>, MsgRec4)
    end,
    emqtt:stop(ConnPid2),
    ok.

t_dispatch_qos0({init, Config}) when is_list(Config) ->
    Config;
t_dispatch_qos0({'end', Config}) when is_list(Config) ->
    ok;
t_dispatch_qos0(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = false),
    Topic = <<"foo/bar/1">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {auto_ack, false}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {auto_ack, true}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    %% subscribe with QoS 0
    emqtt:subscribe(ConnPid1, {<<"$share/group/foo/bar/#">>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/group/foo/bar/#">>, 0}),

    %% publish with QoS 2, but should be downgraded to 0 as the subscribers
    %% subscribe with QoS 0
    Message1 = emqx_message:make(ClientId1, 2, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 2, Topic, <<"hello2">>),
    Message3 = emqx_message:make(ClientId1, 2, Topic, <<"hello3">>),
    Message4 = emqx_message:make(ClientId1, 2, Topic, <<"hello4">>),
    ct:sleep(100),

    ok = sys:suspend(ConnPid1),

    ?assertMatch([_], emqx:publish(Message1)),
    ?assertMatch([_], emqx:publish(Message2)),
    ?assertMatch([_], emqx:publish(Message3)),
    ?assertMatch([_], emqx:publish(Message4)),

    MsgRec1 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P1}}, P1),
    MsgRec2 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P2}}, P2),
    %% assert hello2 > hello1 or hello4 > hello3
    ?assert(MsgRec2 > MsgRec1),

    kill_process(ConnPid1),
    %% expect no redispatch
    ?assertEqual([], collect_msgs(timer:seconds(2))),
    emqtt:stop(ConnPid2),
    ok.

t_session_takeover({init, Config}) when is_list(Config) ->
    Config;
t_session_takeover({'end', Config}) when is_list(Config) ->
    ok;
t_session_takeover(Config) when is_list(Config) ->
    Topic = <<"t1/a">>,
    ClientId = iolist_to_binary("c" ++ integer_to_list(erlang:system_time())),
    Opts = [
        {clientid, ClientId},
        {auto_ack, true},
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 60}}
    ],
    {ok, ConnPid1} = emqtt:start_link(Opts),
    %% with the same client ID, start another client
    {ok, ConnPid2} = emqtt:start_link(Opts),
    {ok, _} = emqtt:connect(ConnPid1),
    emqtt:subscribe(ConnPid1, {<<"$share/t1/", Topic/binary>>, _QoS = 1}),
    Message1 = emqx_message:make(<<"dummypub">>, 2, Topic, <<"hello1">>),
    Message2 = emqx_message:make(<<"dummypub">>, 2, Topic, <<"hello2">>),
    Message3 = emqx_message:make(<<"dummypub">>, 2, Topic, <<"hello3">>),
    Message4 = emqx_message:make(<<"dummypub">>, 2, Topic, <<"hello4">>),
    %% Make sure client1 is functioning
    ?assertMatch([_], emqx:publish(Message1)),
    {true, _} = last_message(<<"hello1">>, [ConnPid1]),
    %% Kill client1
    emqtt:stop(ConnPid1),
    %% publish another message (should end up in client1's session)
    ?assertMatch([_], emqx:publish(Message2)),
    %% connect client2 (with the same clientid)

    %% should trigger session take over
    {ok, _} = emqtt:connect(ConnPid2),
    ?assertMatch([_], emqx:publish(Message3)),
    ?assertMatch([_], emqx:publish(Message4)),
    {true, _} = last_message(<<"hello2">>, [ConnPid2]),
    %% We may or may not recv dup hello2 due to QoS1 redelivery
    _ = last_message(<<"hello2">>, [ConnPid2]),
    {true, _} = last_message(<<"hello3">>, [ConnPid2]),
    {true, _} = last_message(<<"hello4">>, [ConnPid2]),
    ?assertEqual([], collect_msgs(timer:seconds(2))),
    emqtt:stop(ConnPid2),
    ok.

t_session_kicked({init, Config}) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = false),
    emqx_config:put_zone_conf(default, [mqtt, max_inflight], 1),
    Config;
t_session_kicked({'end', Config}) when is_list(Config) ->
    emqx_config:put_zone_conf(default, [mqtt, max_inflight], 0);
t_session_kicked(Config) when is_list(Config) ->
    Topic = <<"foo/bar/1">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {auto_ack, false}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {auto_ack, true}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/group/foo/bar/#">>, 2}),
    emqtt:subscribe(ConnPid2, {<<"$share/group/foo/bar/#">>, 2}),

    Message1 = emqx_message:make(ClientId1, 2, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 2, Topic, <<"hello2">>),
    Message3 = emqx_message:make(ClientId1, 2, Topic, <<"hello3">>),
    Message4 = emqx_message:make(ClientId1, 2, Topic, <<"hello4">>),
    ct:sleep(100),

    ok = sys:suspend(ConnPid1),

    %% One message is inflight
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message1)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message2)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message3)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message4)),

    %% assert client 2 receives two messages, they are eiter 1,3 or 2,4 depending
    %% on if it's picked as the first one for round_robin
    MsgRec1 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P1}}, P1),
    MsgRec2 = ?WAIT(2000, {publish, #{client_pid := ConnPid2, payload := P2}}, P2),

    ct:pal("MsgRec1: ~p MsgRec2 ~p ~n", [MsgRec1, MsgRec2]),
    case MsgRec2 of
        <<"hello3">> ->
            ?assertEqual(<<"hello1">>, MsgRec1);
        <<"hello4">> ->
            ?assertEqual(<<"hello2">>, MsgRec1)
    end,
    sys:resume(ConnPid1),
    %% emqtt subscriber automatically sends PUBREC, but since auto_ack is set to false
    %% so it will never send PUBCOMP, hence EMQX should not attempt to send
    %% the 4th message yet since max_inflight is 1.
    MsgRec3 = ?WAIT(2000, {publish, #{client_pid := ConnPid1, payload := P3}}, P3),
    case MsgRec2 of
        <<"hello3">> ->
            ?assertEqual(<<"hello2">>, MsgRec3);
        <<"hello4">> ->
            ?assertEqual(<<"hello1">>, MsgRec3)
    end,
    %% no message expected
    ?assertEqual([], collect_msgs(0)),
    %% now kick client 1
    kill_process(ConnPid1, fun(_Pid) -> emqx_cm:kick_session(ClientId1) end),
    %% client 2 should NOT receive the message
    ?assertEqual([], collect_msgs(1000)),
    emqtt:stop(ConnPid2),
    ?assertEqual([], collect_msgs(0)),
    ok.

-define(UPDATE_SUB_QOS(ConnPid, Topic, QoS),
    ?assertMatch({ok, _, [QoS]}, emqtt:subscribe(ConnPid, {Topic, QoS}))
).

t_different_groups_same_topic({init, Config}) ->
    TestName = atom_to_binary(?FUNCTION_NAME),
    ClientId = <<TestName/binary, (integer_to_binary(erlang:unique_integer()))/binary>>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),
    [{client, C}, {clientid, ClientId} | Config];
t_different_groups_same_topic({'end', Config}) ->
    C = ?config(client, Config),
    emqtt:stop(C),
    ok;
t_different_groups_same_topic(Config) when is_list(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),
    %% Subscribe and unsubscribe to different group `aa` and `bb` with same topic
    GroupA = <<"aa">>,
    GroupB = <<"bb">>,
    Topic = <<"t/1">>,

    SharedTopicGroupA = format_share(GroupA, Topic),
    ?UPDATE_SUB_QOS(C, SharedTopicGroupA, ?QOS_2),
    SharedTopicGroupB = format_share(GroupB, Topic),
    ?UPDATE_SUB_QOS(C, SharedTopicGroupB, ?QOS_2),

    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        begin
            ?assertEqual(2, length(emqx_router:match_routes(Topic)))
        end
    ),

    Message0 = emqx_message:make(ClientId, ?QOS_2, Topic, <<"hi">>),
    emqx:publish(Message0),
    ?assertMatch(
        [
            {publish, #{payload := <<"hi">>}},
            {publish, #{payload := <<"hi">>}}
        ],
        collect_msgs(5_000),
        #{routes => ets:tab2list(emqx_route)}
    ),

    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, SharedTopicGroupA),
    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, SharedTopicGroupB),

    ok.

t_different_groups_update_subopts({init, Config}) ->
    TestName = atom_to_binary(?FUNCTION_NAME),
    ClientId = <<TestName/binary, (integer_to_binary(erlang:unique_integer()))/binary>>,
    {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),
    [{client, C}, {clientid, ClientId} | Config];
t_different_groups_update_subopts({'end', Config}) ->
    C = ?config(client, Config),
    emqtt:stop(C),
    ok;
t_different_groups_update_subopts(Config) when is_list(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),
    %% Subscribe and unsubscribe to different group `aa` and `bb` with same topic
    Topic = <<"t/1">>,
    GroupA = <<"aa">>,
    GroupB = <<"bb">>,
    SharedTopicGroupA = format_share(GroupA, Topic),
    SharedTopicGroupB = format_share(GroupB, Topic),

    Fun = fun(Group, QoS) ->
        ?UPDATE_SUB_QOS(C, format_share(Group, Topic), QoS),
        ?assertMatch(
            #{qos := QoS},
            emqx_broker:get_subopts(ClientId, emqx_topic:make_shared_record(Group, Topic))
        )
    end,

    [Fun(Group, QoS) || QoS <- [?QOS_0, ?QOS_1, ?QOS_2], Group <- [GroupA, GroupB]],

    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        begin
            ?assertEqual(2, length(emqx_router:match_routes(Topic)))
        end
    ),

    Message0 = emqx_message:make(ClientId, _QoS = 2, Topic, <<"hi">>),
    emqx:publish(Message0),
    ?assertMatch(
        [
            {publish, #{payload := <<"hi">>}},
            {publish, #{payload := <<"hi">>}}
        ],
        collect_msgs(5_000),
        #{routes => ets:tab2list(emqx_route)}
    ),

    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, SharedTopicGroupA),
    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, SharedTopicGroupB),

    ok.

t_queue_subscription({init, Config}) ->
    TestName = atom_to_binary(?FUNCTION_NAME),
    ClientId = <<TestName/binary, (integer_to_binary(erlang:unique_integer()))/binary>>,

    {ok, C} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v5}]),
    {ok, _} = emqtt:connect(C),

    [{client, C}, {clientid, ClientId} | Config];
t_queue_subscription({'end', Config}) ->
    C = ?config(client, Config),
    emqtt:stop(C),
    ok;
t_queue_subscription(Config) when is_list(Config) ->
    C = ?config(client, Config),
    ClientId = ?config(clientid, Config),
    %% Subscribe and unsubscribe to both $queue share and $share/<group> with same topic
    Topic = <<"t/1">>,
    QueueTopic = <<"$queue/", Topic/binary>>,
    SharedTopic = <<"$share/aa/", Topic/binary>>,

    ?UPDATE_SUB_QOS(C, QueueTopic, ?QOS_2),
    ?UPDATE_SUB_QOS(C, SharedTopic, ?QOS_2),

    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        begin
            ?assertEqual(2, length(emqx_router:match_routes(Topic)))
        end
    ),

    %% now publish to the underlying topic
    Message0 = emqx_message:make(ClientId, _QoS = 2, Topic, <<"hi">>),
    emqx:publish(Message0),
    ?assertMatch(
        [
            {publish, #{payload := <<"hi">>}},
            {publish, #{payload := <<"hi">>}}
        ],
        collect_msgs(5_000),
        #{routes => ets:tab2list(emqx_route)}
    ),

    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, QueueTopic),
    {ok, _, [?RC_SUCCESS]} = emqtt:unsubscribe(C, SharedTopic),

    ?retry(
        _Sleep0 = 100,
        _Attempts0 = 50,
        begin
            ?assertEqual(0, length(emqx_router:match_routes(Topic)))
        end
    ),
    ct:sleep(500),

    Message1 = emqx_message:make(ClientId, _QoS = 2, Topic, <<"hello">>),
    emqx:publish(Message1),
    %% we should *not* receive any messages.
    ?assertEqual([], collect_msgs(1_000), #{routes => ets:tab2list(emqx_route)}),

    ok.

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

format_share(Group, Topic) ->
    emqx_topic:maybe_format_share(emqx_topic:make_shared_record(Group, Topic)).

kill_process(Pid) ->
    kill_process(Pid, fun(_) -> erlang:exit(Pid, kill) end).

kill_process(Pid, WithFun) ->
    _ = unlink(Pid),
    _ = monitor(process, Pid),
    _ = WithFun(Pid),
    receive
        {'DOWN', _, process, Pid, _} ->
            ok
    after 10_000 ->
        error(timeout)
    end.

collect_msgs(Timeout) ->
    collect_msgs([], Timeout).

collect_msgs(Acc, Timeout) ->
    receive
        Msg ->
            collect_msgs([Msg | Acc], Timeout)
    after Timeout ->
        lists:reverse(Acc)
    end.

ensure_config(Strategy) ->
    ensure_config(Strategy, _InitialStickyPick = random, _AckEnabled = true).

ensure_config(Strategy, AckEnabled) when is_boolean(AckEnabled) ->
    ensure_config(Strategy, _InitialStickyPick = random, AckEnabled).

ensure_config(Strategy, InitialStickyPick, AckEnabled) ->
    emqx_config:put([mqtt, shared_subscription_strategy], Strategy),
    emqx_config:put([mqtt, shared_subscription_initial_sticky_pick], InitialStickyPick),
    emqx_config:put([broker, shared_dispatch_ack_enabled], AckEnabled),
    ok.

ensure_node_config(Node, Strategy) ->
    rpc:call(Node, emqx_config, force_put, [[mqtt, shared_subscription_strategy], Strategy]).

ensure_group_config(Group2Strategy) ->
    lists:foreach(
        fun({Group, Strategy}) ->
            if
                is_tuple(Strategy) ->
                    {PrimaryStrategy, InitialStickyPick} = Strategy,
                    emqx_config:force_put(
                        [broker, shared_subscription_group, Group, strategy],
                        PrimaryStrategy,
                        unsafe
                    ),
                    emqx_config:force_put(
                        [broker, shared_subscription_group, Group, initial_sticky_pick],
                        InitialStickyPick,
                        unsafe
                    );
                true ->
                    emqx_config:force_put(
                        [broker, shared_subscription_group, Group, strategy], Strategy, unsafe
                    )
            end
        end,
        maps:to_list(Group2Strategy)
    ).

ensure_group_config(Node, Group2Strategy) ->
    lists:foreach(
        fun({Group, Strategy}) ->
            rpc:call(
                Node,
                emqx_config,
                force_put,
                [[broker, shared_subscription_group, Group, strategy], Strategy, unsafe]
            )
        end,
        maps:to_list(Group2Strategy)
    ).

publish_fire_and_forget(Count, Topic) when Count > 1 ->
    lists:foreach(
        fun(I) ->
            Message = erlang:integer_to_binary(I),
            {ok, PublisherPid} = emqtt:start_link(),
            {ok, _} = emqtt:connect(PublisherPid),
            emqtt:publish(PublisherPid, Topic, Message),
            emqtt:stop(PublisherPid),
            ct:sleep(50)
        end,
        lists:seq(0, Count - 1)
    ).

subscribed(Group, Topic, Pid) ->
    lists:member(Pid, emqx_shared_sub:subscribers(Group, Topic)).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count - 1, [Msg | Msgs])
    after 100 ->
        Msgs
    end.

start_peer(Name, Port) ->
    {ok, Node} = emqx_cth_peer:start_link(
        Name,
        emqx_common_test_helpers:ebin_path()
    ),
    pong = net_adm:ping(Node),
    setup_node(Node, Port),
    Node.

stop_peer(Node) ->
    rpc:call(Node, mria, leave, []),
    emqx_cth_peer:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.

setup_node(Node, Port) ->
    EnvHandler =
        fun(_) ->
            %% We load configuration, and than set the special enviroment variable
            %% which says that emqx shouldn't load configuration at startup
            emqx_config:init_load(emqx_schema),
            emqx_app:set_config_loader(?MODULE),

            ok = emqx_config:put([listeners, tcp, default, bind], {{127, 0, 0, 1}, Port}),
            ok = emqx_config:put([listeners, ssl, default, bind], {{127, 0, 0, 1}, Port + 1}),
            ok = emqx_config:put([listeners, ws, default, bind], {{127, 0, 0, 1}, Port + 3}),
            ok = emqx_config:put([listeners, wss, default, bind], {{127, 0, 0, 1}, Port + 4}),
            ok
        end,

    MyPort = Port - 1,
    PeerPort = Port - 2,
    ok = pair_gen_rpc(node(), MyPort, PeerPort),
    ok = pair_gen_rpc(Node, PeerPort, MyPort),

    %% Here we start the node and make it join the cluster
    ok = rpc:call(Node, emqx_common_test_helpers, start_apps, [[], EnvHandler]),

    %% warm it up, also assert the peer ndoe name
    Node = emqx_rpc:call(Node, erlang, node, []),
    rpc:call(Node, mria, join, [node()]),
    ok.

pair_gen_rpc(Node, LocalPort, RemotePort) ->
    _ = rpc:call(Node, application, load, [gen_rpc]),
    ok = rpc:call(Node, application, set_env, [gen_rpc, port_discovery, manual]),
    ok = rpc:call(Node, application, set_env, [gen_rpc, tcp_server_port, LocalPort]),
    ok = rpc:call(Node, application, set_env, [gen_rpc, tcp_client_port, RemotePort]),
    ok = rpc:call(Node, application, set_env, [gen_rpc, default_client_driver, tcp]),
    _ = rpc:call(Node, application, stop, [gen_rpc]),
    {ok, _} = rpc:call(Node, application, ensure_all_started, [gen_rpc]),
    ok.
