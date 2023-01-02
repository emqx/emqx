%%--------------------------------------------------------------------
%% Copyright (c) 2018-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(SUITE, ?MODULE).

-define(ack, shared_sub_ack).
-define(no_ack, no_ack).

-define(WAIT(TIMEOUT, PATTERN, Res),
        (fun() ->
            receive
                PATTERN ->
                    Res;
                Other ->
                    ct:fail(#{expected => ??PATTERN,
                              got => Other
                             })
            after
                TIMEOUT ->
                    ct:fail({timeout, ??PATTERN})
            end
        end)()).

all() -> emqx_ct:all(?SUITE).

init_per_suite(Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    emqx_ct_helpers:boot_modules(all),
    PortDiscovery = application:get_env(gen_rpc, port_discovery),
    application:set_env(gen_rpc, port_discovery, stateless),
    application:ensure_all_started(gen_rpc),
    %% ensure emqx_modules app modules are loaded
    %% so the mnesia tables are created
    ok = load_app(emqx_modules),
    emqx_ct_helpers:start_apps([]),
    [{port_discovery, PortDiscovery} | Config].

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([gen_rpc]),
    case proplists:get_value(port_discovery, Config) of
      {ok, OldValue} -> application:set_env(gen_rpc, port_discovery, OldValue);
      _ -> ok
    end.

init_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({'init', Config})
    catch
        error : function_clause ->
            Config
    end.

end_per_testcase(Case, Config) ->
    try
        ?MODULE:Case({'end', Config})
    catch
        error : function_clause ->
            ok
    end.

t_is_ack_required(Config) when is_list(Config) ->
    ?assertEqual(false, emqx_shared_sub:is_ack_required(#message{headers = #{}})).

t_maybe_nack_dropped(Config) when is_list(Config) ->
    ?assertEqual(store, emqx_shared_sub:maybe_nack_dropped(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {self(), {fresh, <<"group">>, for_test}}}},
    ?assertEqual(drop, emqx_shared_sub:maybe_nack_dropped(Msg)),
    ?assertEqual(ok,receive {for_test, {shared_sub_nack, dropped}} -> ok after 100 -> timeout end).

t_nack_no_connection(Config) when is_list(Config) ->
    Msg = #message{headers = #{shared_dispatch_ack => {self(), {fresh, <<"group">>, for_test}}}},
    ?assertEqual(ok, emqx_shared_sub:nack_no_connection(Msg)),
    ?assertEqual(ok,receive {for_test, {shared_sub_nack, no_connection}} -> ok
                    after 100 -> timeout end).

t_maybe_ack(Config) when is_list(Config) ->
    ?assertEqual(#message{headers = #{}}, emqx_shared_sub:maybe_ack(#message{headers = #{}})),
    Msg = #message{headers = #{shared_dispatch_ack => {self(), {fresh, <<"group">>, for_test}}}},
    ?assertEqual(#message{headers = #{shared_dispatch_ack => ?no_ack}},
                 emqx_shared_sub:maybe_ack(Msg)),
    ?assertEqual(ok,receive {for_test, ?ack} -> ok after 100 -> timeout end).

t_random_basic(Config) when is_list(Config) ->
    ok = ensure_config(random),
    ClientId = <<"ClientId">>,
    Topic = <<"foo">>,
    Payload = <<"hello">>,
    emqx:subscribe(Topic, #{qos => 2, share => <<"group1">>}),
    MsgQoS2 = emqx_message:make(ClientId, 2, Topic, Payload),
    %% wait for the subscription to show up
    ct:sleep(200),
    ?assertEqual(true, subscribed(<<"group1">>, Topic, self())),
    emqx:publish(MsgQoS2),
    receive
        {deliver, Topic0, #message{from = ClientId0,
                                   payload = Payload0}} = M->
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
    {ok, _} = emqtt:connect(SubConnPid1),
    {ok, SubConnPid2} = emqtt:start_link([{clientid, Subscriber2}] ++ ExpProp),
    {ok, _} = emqtt:connect(SubConnPid2),
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
    ct:sleep(200),
    %% This is the connection which was picked by broker to dispatch (sticky) for 1st message
    ?assertMatch([#{packet_id := 1}], recv_msgs(1)),
    ok.

t_random(Config) when is_list(Config) ->
    ok = ensure_config(random, true),
    test_two_messages(random).

t_round_robin(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, true),
    test_two_messages(round_robin).

t_sticky(Config) when is_list(Config) ->
    ok = ensure_config(sticky, true),
    test_two_messages(sticky).

t_hash(Config) when is_list(Config) ->
    ok = ensure_config(hash, false),
    test_two_messages(hash).

t_hash_clinetid(Config) when is_list(Config) ->
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
    UsedSubPid1 = receive {subscriber, P1} -> P1 end,
    emqx_broker:publish(Message2),
    WaitF(<<"hello2">>),
    UsedSubPid2 = receive {subscriber, P2} -> P2 end,
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

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/foo/bar">>, 0}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/foo/bar">>, 0}),

    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 0, Topic, <<"hello2">>),
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
        hash -> ?assertEqual(UsedSubPid1, UsedSubPid2);
        _ -> ok
    end,
    ok.

last_message(ExpectedPayload, Pids) ->
    last_message(ExpectedPayload, Pids, 6000).

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
    ?assertEqual({error, no_subscribers},
                 emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})),
    emqx:subscribe(Topic, #{qos => 2, share => <<"group1">>}),
    ?assertEqual({ok, 1},
                 emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})).

t_uncovered_func(Config) when is_list(Config) ->
    ignored = gen_server:call(emqx_shared_sub, ignored),
    ok = gen_server:cast(emqx_shared_sub, ignored),
    ignored = emqx_shared_sub ! ignored,
    {mnesia_table_event, []} = emqx_shared_sub ! {mnesia_table_event, []}.

t_per_group_config(Config) when is_list(Config) ->
    ok = ensure_group_config(#{
                               <<"local_group_fallback">> => local,
                               <<"local_group">> => local,
                               <<"round_robin_group">> => round_robin,
                               <<"sticky_group">> => sticky
                              }),
    %% Each test is repeated 4 times because random strategy may technically pass the test
    %% so we run 8 tests to make random pass in only 1/256 runs

    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(sticky, <<"sticky_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>),
    test_two_messages(round_robin, <<"round_robin_group">>).

t_local({'init', Config}) ->
    Node = start_slave(local_shared_sub_test19, 21884),
    GroupConfig = #{
                    <<"local_group_fallback">> => local,
                    <<"local_group">> => local,
                    <<"round_robin_group">> => round_robin,
                    <<"sticky_group">> => sticky
                   },
    ok = ensure_group_config(Node, GroupConfig),
    ok = ensure_group_config(GroupConfig),
    [{slave_node, Node} | Config];
t_local({'end', _Config}) ->
    ok = stop_slave(local_shared_sub_test19);
t_local(Config) when is_list(Config) ->
    Node = proplists:get_value(slave_node, Config),
    Topic = <<"local_foo1/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {port, 21884}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),

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
    emqx_node_helpers:stop_slave(Node),

    ?assertEqual(local, emqx_shared_sub:strategy(<<"local_group">>)),
    ?assertEqual(local, RemoteLocalGroupStrategy),

    ?assertNotEqual(UsedSubPid1, UsedSubPid2),
    ok.

t_local_fallback({'init', Config}) ->
    ok = ensure_group_config(#{
                               <<"local_group_fallback">> => local,
                               <<"local_group">> => local,
                               <<"round_robin_group">> => round_robin,
                               <<"sticky_group">> => sticky
                              }),

    Node = start_slave(local_fallback_shared_sub_test19, 11885),
    [{slave_node, Node} | Config];
t_local_fallback({'end', _}) ->
    ok = stop_slave(local_fallback_shared_sub_test19);
t_local_fallback(Config) when is_list(Config) ->
    Topic = <<"local_foo2/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    Node = proplists:get_value(slave_node, Config),

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),

    emqtt:subscribe(ConnPid1, {<<"$share/local_group_fallback/", Topic/binary>>, 0}),
    ok = emqx_node_helpers:wait_for_synced_routes([node(), Node], Topic, timer:seconds(10)),

    [{share, Topic, {ok, 1}}] = emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1]),

    [{share, Topic, {ok, 1}}] = rpc:call(Node, emqx, publish, [Message2]),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1]),

    emqtt:stop(ConnPid1),
    emqx_node_helpers:stop_slave(Node),

    ?assertEqual(UsedSubPid1, UsedSubPid2),
    ok.

%% This one tests that broker tries to select another shared subscriber
%% If the first one doesn't return an ACK
t_redispatch_with_ack(Config) when is_list(Config) ->
    test_redispatch(Config, true).

t_redispatch_no_ack(Config) when is_list(Config) ->
    test_redispatch(Config, false).

test_redispatch(_Config, AckEnabled) ->
    ok = ensure_config(sticky, AckEnabled),
    application:set_env(emqx, shared_dispatch_ack_enabled, true),

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

t_redispatch_wildcard_with_ack(Config) when is_list(Config)->
    redispatch_wildcard(Config, true).

t_redispatch_wildcard_no_ack(Config) when is_list(Config) ->
    redispatch_wildcard(Config, false).

%% This one tests that broker tries to redispatch to another member in the group
%% if the first one disconnected before acking (auto_ack set to false)
redispatch_wildcard(_Config, AckEnabled) ->
    ok = ensure_config(sticky, AckEnabled),

    Group = <<"group1">>,

    Topic = <<"foo/bar/1">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}, {auto_ack, false}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}, {auto_ack, false}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/", Group/binary, "/foo/bar/#">>, 1}),
    emqtt:subscribe(ConnPid2, {<<"$share/", Group/binary, "/foo/bar/#">>, 1}),

    Message = emqx_message:make(ClientId1, 1, Topic, <<"hello1">>),

    emqx:publish(Message),

    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1, ConnPid2]),
    ok = emqtt:stop(UsedSubPid1),

    Res = last_message(<<"hello1">>, [ConnPid1, ConnPid2], 6000),
    ?assertMatch({true, Pid} when Pid =/= UsedSubPid1, Res),

    {true, UsedSubPid2} = Res,
    emqtt:stop(UsedSubPid2),
    ok.

t_dispatch_when_inflights_are_full({init, Config}) ->
    %% make sure broker does not push more than one inflight
    meck:new(emqx_zone, [passthrough, no_history]),
    meck:expect(emqx_zone, max_inflight, fun(_Zone) -> 1 end),
    Config;
t_dispatch_when_inflights_are_full({'end', _Config}) ->
    meck:unload(emqx_zone);
t_dispatch_when_inflights_are_full(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = true),
    Topic = <<"foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, ConnPid2} = emqtt:start_link([{clientid, ClientId2}]),
    {ok, _} = emqtt:connect(ConnPid1),
    {ok, _} = emqtt:connect(ConnPid2),

    emqtt:subscribe(ConnPid1, {<<"$share/group/foo/bar">>, 2}),
    emqtt:subscribe(ConnPid2, {<<"$share/group/foo/bar">>, 2}),

    Message1 = emqx_message:make(ClientId1, 2, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId1, 2, Topic, <<"hello2">>),
    Message3 = emqx_message:make(ClientId1, 2, Topic, <<"hello3">>),
    Message4 = emqx_message:make(ClientId1, 2, Topic, <<"hello4">>),
    ct:sleep(100),

    sys:suspend(ConnPid1),
    sys:suspend(ConnPid2),

    %% Fill in the inflight for first client
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message1)),

    %% Fill in the inflight for second client
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message2)),

    %% Now kill any client
    ok = kill_process(ConnPid1),

    %% And try to send the message
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message3)),
    ?assertMatch([{_, _, {ok, 1}}], emqx:publish(Message4)),

    %% And see that it gets dispatched to the client which is alive, even if it's inflight is full
    sys:resume(ConnPid2),
    ct:sleep(100),
    ?assertMatch({true, ConnPid2}, last_message(<<"hello3">>, [ConnPid1, ConnPid2])),
    ?assertMatch({true, ConnPid2}, last_message(<<"hello4">>, [ConnPid1, ConnPid2])),

    emqtt:stop(ConnPid2),
    ok.

%% No ack, QoS 2 subscriptions,
%% client1 receives one message, send pubrec, then suspend
%% client2 acts normal (auto_ack=true)
%% Expected behaviour:
%% the messages sent to client1's inflight and mq are re-dispatched after client1 is down
t_dispatch_qos2({init, Config}) when is_list(Config) ->
    meck:new(emqx_zone, [passthrough, no_history]),
    meck:expect(emqx_zone, max_inflight, fun(_Zone) -> 1 end),
    Config;
t_dispatch_qos2({'end', Config}) when is_list(Config) ->
    meck:unload(emqx_zone);
t_dispatch_qos2(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = false),
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
    Opts = [{clientid, ClientId},
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
    {ok, _} = emqtt:connect(ConnPid2), %% should trigger session take over
    ?assertMatch([_], emqx:publish(Message3)),
    ?assertMatch([_], emqx:publish(Message4)),
    {true, _} = last_message(<<"hello2">>, [ConnPid2]),
    {true, _} = last_message(<<"hello3">>, [ConnPid2]),
    {true, _} = last_message(<<"hello4">>, [ConnPid2]),
    ?assertEqual([], collect_msgs(timer:seconds(2))),
    emqtt:stop(ConnPid2),
    ok.


t_session_kicked({init, Config}) when is_list(Config) ->
    meck:new(emqx_zone, [passthrough, no_history]),
    meck:expect(emqx_zone, max_inflight, fun(_Zone) -> 1 end),
    Config;
t_session_kicked({'end', Config}) when is_list(Config) ->
    meck:unload(emqx_zone);
t_session_kicked(Config) when is_list(Config) ->
    ok = ensure_config(round_robin, _AckEnabled = false),
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

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

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
    after
        Timeout ->
            lists:reverse(Acc)
    end.

ensure_config(Strategy) ->
    ensure_config(Strategy, _AckEnabled = true).

ensure_config(Strategy, AckEnabled) ->
    application:set_env(emqx, shared_subscription_strategy, Strategy),
    application:set_env(emqx, shared_dispatch_ack_enabled, AckEnabled),
    ok.

ensure_group_config(Node, Group2Strategy) ->
    rpc:call(Node, application, set_env, [emqx, shared_subscription_strategy_per_group, Group2Strategy]).

ensure_group_config(Group2Strategy) ->
    application:set_env(emqx, shared_subscription_strategy_per_group, Group2Strategy).

subscribed(Group, Topic, Pid) ->
    lists:member(Pid, emqx_shared_sub:subscribers(Group, Topic)).

recv_msgs(Count) ->
    recv_msgs(Count, []).

recv_msgs(0, Msgs) ->
    Msgs;
recv_msgs(Count, Msgs) ->
    receive
        {publish, Msg} ->
            recv_msgs(Count-1, [Msg|Msgs]);
        _Other -> recv_msgs(Count, Msgs) %%TODO:: remove the branch?
    after 100 ->
        Msgs
    end.

start_slave(Name, Port) ->
    ok = emqx_ct_helpers:start_apps([emqx_modules]),
    Listeners = [#{listen_on => {{127,0,0,1}, Port},
                   start_apps => [emqx, emqx_modules],
                   name => "internal",
                   opts => [{zone,internal}],
                   proto => tcp}],
    emqx_node_helpers:start_slave(Name, #{listeners => Listeners}).

stop_slave(Name) ->
    emqx_node_helpers:stop_slave(Name).

load_app(App) ->
    case application:load(App) of
        ok -> ok;
        {error, {already_loaded, _}} -> ok;
        {error, Reason} -> error({failed_to_load_app, App, Reason})
    end.
