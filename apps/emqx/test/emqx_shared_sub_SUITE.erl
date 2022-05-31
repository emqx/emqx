%%--------------------------------------------------------------------
%% Copyright (c) 2018-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(wait(For, Timeout),
    emqx_common_test_helpers:wait_for(
        ?FUNCTION_NAME, ?LINE, fun() -> For end, Timeout
    )
).

-define(ack, shared_sub_ack).
-define(no_ack, no_ack).

all() -> emqx_common_test_helpers:all(?SUITE).

init_per_suite(Config) ->
    net_kernel:start(['master@127.0.0.1', longnames]),
    emqx_common_test_helpers:boot_modules(all),
    emqx_common_test_helpers:start_apps([]),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([]).

t_is_ack_required(_) ->
    ?assertEqual(false, emqx_shared_sub:is_ack_required(#message{headers = #{}})).

t_maybe_nack_dropped(_) ->
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

t_nack_no_connection(_) ->
    Msg = #message{headers = #{shared_dispatch_ack => {<<"group">>, self(), for_test}}},
    ?assertEqual(ok, emqx_shared_sub:nack_no_connection(Msg)),
    ?assertEqual(
        ok,
        receive
            {for_test, {shared_sub_nack, no_connection}} -> ok
        after 100 -> timeout
        end
    ).

t_maybe_ack(_) ->
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

% t_subscribers(_) ->
%     error('TODO').

t_random_basic(_) ->
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
t_no_connection_nack(_) ->
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
    %% Now kill the connection, expect all following messages to be delivered to the other
    %% subscriber.
    %emqx_mock_client:stop(ConnPid),
    %% sleep then make synced calls to session processes to ensure that
    %% the connection pid's 'EXIT' message is propagated to the session process
    %% also to be sure sessions are still alive
    %   timer:sleep(2),
    %   _ = emqx_session:info(SPid1),
    %   _ = emqx_session:info(SPid2),
    %   %% Now we know what is the other still alive connection
    %   [TheOtherConnPid] = [SubConnPid1, SubConnPid2] -- [ConnPid],
    %   %% Send some more messages
    %   PacketIdList = lists:seq(2, 10),
    %   lists:foreach(fun(Id) ->
    %                         SendF(Id),
    %                         ?wait(Received(Id, TheOtherConnPid), 1000)
    %                 end, PacketIdList),
    %   %% Now close the 2nd (last connection)
    %   emqx_mock_client:stop(TheOtherConnPid),
    %   timer:sleep(2),
    %   %% both sessions should have conn_pid = undefined
    %   ?assertEqual({conn_pid, undefined}, lists:keyfind(conn_pid, 1, emqx_session:info(SPid1))),
    %   ?assertEqual({conn_pid, undefined}, lists:keyfind(conn_pid, 1, emqx_session:info(SPid2))),
    %   %% send more messages, but all should be queued in session state
    %   lists:foreach(fun(Id) -> SendF(Id) end, PacketIdList),
    %   {_, L1} = lists:keyfind(mqueue_len, 1, emqx_session:info(SPid1)),
    %   {_, L2} = lists:keyfind(mqueue_len, 1, emqx_session:info(SPid2)),
    %   ?assertEqual(length(PacketIdList), L1 + L2),
    %   %% clean up
    %   emqx_mock_client:close_session(PubConnPid),
    %   emqx_sm:close_session(SPid1),
    %   emqx_sm:close_session(SPid2),
    ok.

t_random(_) ->
    ok = ensure_config(random, true),
    test_two_messages(random).

t_round_robin(_) ->
    ok = ensure_config(round_robin, true),
    test_two_messages(round_robin).

t_sticky(_) ->
    ok = ensure_config(sticky, true),
    test_two_messages(sticky).

t_hash(_) ->
    ok = ensure_config(hash, false),
    test_two_messages(hash).

t_hash_clinetid(_) ->
    ok = ensure_config(hash_clientid, false),
    test_two_messages(hash_clientid).

t_hash_topic(_) ->
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
t_not_so_sticky(_) ->
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
    last_message(ExpectedPayload, Pids, 100).

last_message(ExpectedPayload, Pids, Timeout) ->
    receive
        {publish, #{client_pid := Pid, payload := ExpectedPayload}} ->
            ct:pal("~p ====== ~p", [Pids, Pid]),
            {true, Pid}
    after Timeout ->
        ct:pal("not yet"),
        <<"not yet?">>
    end.

t_dispatch(_) ->
    ok = ensure_config(random),
    Topic = <<"foo">>,
    ?assertEqual(
        {error, no_subscribers},
        emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})
    ),
    emqx:subscribe(Topic, #{qos => 2, share => <<"group1">>}),
    ?assertEqual(
        {ok, 1},
        emqx_shared_sub:dispatch(<<"group1">>, Topic, #delivery{message = #message{}})
    ).

t_uncovered_func(_) ->
    ignored = gen_server:call(emqx_shared_sub, ignored),
    ok = gen_server:cast(emqx_shared_sub, ignored),
    ignored = emqx_shared_sub ! ignored,
    {mnesia_table_event, []} = emqx_shared_sub ! {mnesia_table_event, []}.

t_per_group_config(_) ->
    ok = ensure_group_config(#{
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

t_local(_) ->
    GroupConfig = #{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky
    },

    Node = start_slave('local_shared_sub_testtesttest', 21999),
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
    stop_slave(Node),

    ?assertEqual(local, emqx_shared_sub:strategy(<<"local_group">>)),
    ?assertEqual(local, RemoteLocalGroupStrategy),

    ?assertNotEqual(UsedSubPid1, UsedSubPid2),
    ok.

t_local_fallback(_) ->
    ok = ensure_group_config(#{
        <<"local_group">> => local,
        <<"round_robin_group">> => round_robin,
        <<"sticky_group">> => sticky
    }),

    Topic = <<"local_foo/bar">>,
    ClientId1 = <<"ClientId1">>,
    ClientId2 = <<"ClientId2">>,
    Node = start_slave('local_fallback_shared_sub_test', 11888),

    {ok, ConnPid1} = emqtt:start_link([{clientid, ClientId1}]),
    {ok, _} = emqtt:connect(ConnPid1),
    Message1 = emqx_message:make(ClientId1, 0, Topic, <<"hello1">>),
    Message2 = emqx_message:make(ClientId2, 0, Topic, <<"hello2">>),

    emqtt:subscribe(ConnPid1, {<<"$share/local_group/", Topic/binary>>, 0}),

    emqx:publish(Message1),
    {true, UsedSubPid1} = last_message(<<"hello1">>, [ConnPid1]),

    rpc:call(Node, emqx, publish, [Message2]),
    {true, UsedSubPid2} = last_message(<<"hello2">>, [ConnPid1]),

    emqtt:stop(ConnPid1),
    stop_slave(Node),

    ?assertEqual(UsedSubPid1, UsedSubPid2),
    ok.

%% This one tests that broker tries to select another shared subscriber
%% If the first one doesn't return an ACK
t_redispatch(_) ->
    ok = ensure_config(sticky, true),

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

%%--------------------------------------------------------------------
%% help functions
%%--------------------------------------------------------------------

ensure_config(Strategy) ->
    ensure_config(Strategy, _AckEnabled = true).

ensure_config(Strategy, AckEnabled) ->
    emqx_config:put([broker, shared_subscription_strategy], Strategy),
    emqx_config:put([broker, shared_dispatch_ack_enabled], AckEnabled),
    ok.

ensure_group_config(Group2Strategy) ->
    lists:foreach(
        fun({Group, Strategy}) ->
            emqx_config:force_put(
                [broker, shared_subscription_group, Group, strategy], Strategy, unsafe
            )
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

start_slave(Name, Port) ->
    {ok, Node} = ct_slave:start(
        list_to_atom(atom_to_list(Name) ++ "@" ++ host()),
        [
            {kill_if_fail, true},
            {monitor_master, true},
            {init_timeout, 10000},
            {startup_timeout, 10000},
            {erl_flags, ebin_path()}
        ]
    ),

    pong = net_adm:ping(Node),
    setup_node(Node, Port),
    Node.

stop_slave(Node) ->
    rpc:call(Node, mria, leave, []),
    ct_slave:stop(Node).

host() ->
    [_, Host] = string:tokens(atom_to_list(node()), "@"),
    Host.

ebin_path() ->
    string:join(["-pa" | lists:filter(fun is_lib/1, code:get_path())], " ").

is_lib(Path) ->
    string:prefix(Path, code:lib_dir()) =:= nomatch.

setup_node(Node, Port) ->
    EnvHandler =
        fun(_) ->
            %% We load configuration, and than set the special enviroment variable
            %% which says that emqx shouldn't load configuration at startup
            emqx_config:init_load(emqx_schema),
            application:set_env(emqx, init_config_load_done, true),

            ok = emqx_config:put([listeners, tcp, default, bind], {{127, 0, 0, 1}, Port}),
            ok = emqx_config:put([listeners, ssl, default, bind], {{127, 0, 0, 1}, Port + 1}),
            ok = emqx_config:put([listeners, ws, default, bind], {{127, 0, 0, 1}, Port + 3}),
            ok = emqx_config:put([listeners, wss, default, bind], {{127, 0, 0, 1}, Port + 4}),
            ok
        end,

    %% Load env before doing anything
    [ok = rpc:call(Node, application, load, [App]) || App <- [gen_rpc, emqx, ekka, mria]],

    %% Needs to be set explicitly because ekka:start() (which calls `gen` is called without Handler
    %% in emqx_common_test_helpers:start_apps(...)
    ok = rpc:call(Node, application, set_env, [gen_rpc, tcp_server_port, Port - 1]),
    ok = rpc:call(Node, application, set_env, [gen_rpc, port_discovery, manual]),

    %% Here we start the node and make it join the cluster
    ok = rpc:call(Node, emqx_common_test_helpers, start_apps, [[], EnvHandler]),
    rpc:call(Node, mria, join, [node()]),

    ok.
