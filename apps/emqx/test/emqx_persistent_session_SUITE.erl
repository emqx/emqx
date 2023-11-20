%%--------------------------------------------------------------------
%% Copyright (c) 2021-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_persistent_session_SUITE).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-define(PERSISTENT_MESSAGE_DB, emqx_persistent_message).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        % NOTE
        % Tests are disabled while existing session persistence impl is being
        % phased out.
        {group, persistent_store_disabled},
        {group, persistent_store_ds}
    ].

%% A persistent session can be resumed in two ways:
%%    1. The old connection process is still alive, and the session is taken
%%       over by the new connection.
%%    2. The old session process has died (e.g., because of node down).
%%       The new process resumes the session from the stored state, and finds
%%       any subscribed messages from the persistent message store.
%%
%% We want to test both ways, both with the db backend enabled and disabled.
%%
%% In addition, we test both tcp and quic connections.

groups() ->
    TCs = emqx_common_test_helpers:all(?MODULE),
    TCsNonGeneric = [t_choose_impl],
    [
        {persistent_store_disabled, [{group, no_kill_connection_process}]},
        {persistent_store_ds, [{group, no_kill_connection_process}]},
        {no_kill_connection_process, [], [{group, tcp}, {group, quic}, {group, ws}]},
        {tcp, [], TCs},
        {quic, [], TCs -- TCsNonGeneric},
        {ws, [], TCs -- TCsNonGeneric}
    ].

init_per_group(persistent_store_disabled, Config) ->
    [
        {emqx_config, "persistent_session_store { enabled = false }"},
        {persistent_store, false}
        | Config
    ];
init_per_group(persistent_store_ds, Config) ->
    [
        {emqx_config, "persistent_session_store { ds = true }"},
        {persistent_store, ds}
        | Config
    ];
init_per_group(Group, Config) when Group == tcp ->
    Apps = emqx_cth_suite:start(
        [{emqx, ?config(emqx_config, Config)}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {port, get_listener_port(tcp, default)},
        {conn_fun, connect},
        {group_apps, Apps}
        | Config
    ];
init_per_group(Group, Config) when Group == ws ->
    Apps = emqx_cth_suite:start(
        [{emqx, ?config(emqx_config, Config)}],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {ssl, false},
        {host, "localhost"},
        {enable_websocket, true},
        {port, get_listener_port(ws, default)},
        {conn_fun, ws_connect},
        {group_apps, Apps}
        | Config
    ];
init_per_group(Group, Config) when Group == quic ->
    Apps = emqx_cth_suite:start(
        [
            {emqx,
                ?config(emqx_config, Config) ++
                    "\n listeners.quic.test {"
                    "\n   enable = true"
                    "\n   ssl_options.verify = verify_peer"
                    "\n }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [
        {port, get_listener_port(quic, test)},
        {conn_fun, quic_connect},
        {ssl_opts, emqx_common_test_helpers:client_ssl_twoway()},
        {ssl, true},
        {group_apps, Apps}
        | Config
    ];
init_per_group(no_kill_connection_process, Config) ->
    [{kill_connection_process, false} | Config];
init_per_group(kill_connection_process, Config) ->
    [{kill_connection_process, true} | Config].

get_listener_port(Type, Name) ->
    case emqx_config:get([listeners, Type, Name, bind]) of
        {_, Port} -> Port;
        Port -> Port
    end.

end_per_group(Group, Config) when Group == tcp; Group == ws; Group == quic ->
    ok = emqx_cth_suite:stop(?config(group_apps, Config));
end_per_group(_, _Config) ->
    catch emqx_ds:drop_db(?PERSISTENT_MESSAGE_DB),
    ok.

init_per_testcase(TestCase, Config) ->
    Config1 = preconfig_per_testcase(TestCase, Config),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config1);
        _ -> Config1
    end.

end_per_testcase(TestCase, Config) ->
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase('end', Config);
        false -> ok
    end,
    Config.

preconfig_per_testcase(TestCase, Config) ->
    {BaseName, Config1} =
        case ?config(tc_group_properties, Config) of
            [] ->
                %% We are running a single testcase
                {
                    atom_to_binary(TestCase),
                    init_per_group(tcp, init_per_group(kill_connection_process, Config))
                };
            [_ | _] = Props ->
                Path = lists:reverse(?config(tc_group_path, Config) ++ Props),
                Pre0 = [atom_to_list(N) || {name, N} <- lists:flatten(Path)],
                Pre1 = lists:join("_", Pre0 ++ [atom_to_binary(TestCase)]),
                {iolist_to_binary(Pre1), Config}
        end,
    [
        {topic, iolist_to_binary([BaseName, "/foo"])},
        {stopic, iolist_to_binary([BaseName, "/+"])},
        {stopic_alt, iolist_to_binary([BaseName, "/foo"])},
        {client_id, BaseName}
        | Config1
    ].

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

client_info(Key, Client) ->
    maps:get(Key, maps:from_list(emqtt:info(Client)), undefined).

receive_messages(Count) ->
    receive_messages(Count, 15000).

receive_messages(Count, Timeout) ->
    Deadline = erlang:monotonic_time(millisecond) + Timeout,
    receive_message_loop(Count, Deadline).

receive_message_loop(0, _Deadline) ->
    [];
receive_message_loop(Count, Deadline) ->
    Timeout = max(0, Deadline - erlang:monotonic_time(millisecond)),
    receive
        {publish, Msg} ->
            [Msg | receive_message_loop(Count - 1, Deadline)];
        _Other ->
            receive_message_loop(Count, Deadline)
    after Timeout ->
        []
    end.

maybe_kill_connection_process(ClientId, Config) ->
    case ?config(kill_connection_process, Config) of
        true ->
            case emqx_cm:lookup_channels(ClientId) of
                [] ->
                    ok;
                [ConnectionPid] ->
                    ?assert(is_pid(ConnectionPid)),
                    Ref = monitor(process, ConnectionPid),
                    ConnectionPid ! die_if_test,
                    receive
                        {'DOWN', Ref, process, ConnectionPid, normal} -> ok
                    after 3000 -> error(process_did_not_die)
                    end,
                    wait_for_cm_unregister(ClientId)
            end;
        false ->
            ok
    end.

wait_for_cm_unregister(ClientId) ->
    wait_for_cm_unregister(ClientId, 100).

wait_for_cm_unregister(_ClientId, 0) ->
    error(cm_did_not_unregister);
wait_for_cm_unregister(ClientId, N) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ok;
        [_] ->
            timer:sleep(100),
            wait_for_cm_unregister(ClientId, N - 1)
    end.

messages(Topic, Payloads) ->
    messages(Topic, Payloads, ?QOS_2).

messages(Topic, Payloads, QoS) ->
    [#mqtt_msg{topic = Topic, payload = P, qos = QoS} || P <- Payloads].

publish(Topic, Payload) ->
    publish(Topic, Payload, ?QOS_2).

publish(Topic, Payload, QoS) ->
    publish_many(messages(Topic, [Payload], QoS)).

publish_many(Messages) ->
    publish_many(Messages, false).

publish_many(Messages, WaitForUnregister) ->
    Fun = fun(Client, Message) ->
        {ok, _} = emqtt:publish(Client, Message)
    end,
    do_publish(Messages, Fun, WaitForUnregister).

do_publish(Messages = [_ | _], PublishFun, WaitForUnregister) ->
    %% Publish from another process to avoid connection confusion.
    {Pid, Ref} =
        spawn_monitor(
            fun() ->
                %% For convenience, always publish using tcp.
                %% The publish path is not what we are testing.
                ClientID = <<"ps_SUITE_publisher">>,
                {ok, Client} = emqtt:start_link([
                    {proto_ver, v5},
                    {clientid, ClientID},
                    {port, 1883}
                ]),
                {ok, _} = emqtt:connect(Client),
                lists:foreach(fun(Message) -> PublishFun(Client, Message) end, Messages),
                ok = emqtt:disconnect(Client),
                %% Snabbkaffe sometimes fails unless all processes are gone.
                case WaitForUnregister of
                    false ->
                        ok;
                    true ->
                        case emqx_cm:lookup_channels(ClientID) of
                            [] ->
                                ok;
                            [ConnectionPid] ->
                                ?assert(is_pid(ConnectionPid)),
                                Ref1 = monitor(process, ConnectionPid),
                                receive
                                    {'DOWN', Ref1, process, ConnectionPid, _} -> ok
                                after 3000 -> error(process_did_not_die)
                                end,
                                wait_for_cm_unregister(ClientID)
                        end
                end
            end
        ),
    receive
        {'DOWN', Ref, process, Pid, normal} -> ok;
        {'DOWN', Ref, process, Pid, What} -> error({failed_publish, What})
    end.

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

t_choose_impl(Config) ->
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client),
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ?assertEqual(
        case ?config(persistent_store, Config) of
            false -> emqx_session_mem;
            ds -> emqx_persistent_session_ds
        end,
        emqx_connection:info({channel, {session, impl}}, sys:get_state(ChanPid))
    ).

t_connect_discards_existing_client(Config) ->
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    ClientOpts = [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ],

    {ok, Client1} = emqtt:start_link(ClientOpts),
    true = unlink(Client1),
    MRef = erlang:monitor(process, Client1),
    {ok, _} = emqtt:ConnFun(Client1),

    {ok, Client2} = emqtt:start_link(ClientOpts),
    {ok, _} = emqtt:ConnFun(Client2),

    receive
        {'DOWN', MRef, process, Client1, Reason} ->
            ok = ?assertMatch({disconnected, ?RC_SESSION_TAKEN_OVER, _}, Reason),
            ok = emqtt:stop(Client2),
            ok
    after 1000 ->
        error({client_still_connected, Client1})
    end.

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval(init, Config) -> skip_ds_tc(Config);
t_connect_session_expiry_interval('end', _Config) -> ok.
t_connect_session_expiry_interval(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload),

    {ok, Client2} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    [Msg | _] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

t_without_client_id(Config) ->
    %% Emqtt client dies
    process_flag(trap_exit, true),
    ConnFun = ?config(conn_fun, Config),
    {ok, Client0} = emqtt:start_link([
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {error, {client_identifier_not_valid, _}} = emqtt:ConnFun(Client0),
    ok.

t_assigned_clientid_persistent_session(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(AssignedClientId, Config),

    {ok, Client2} = emqtt:start_link([
        {clientid, AssignedClientId},
        {proto_ver, v5},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(Config) ->
    %% Open a persistent session, but cancel the persistence when
    %% shutting down the connection.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    wait_for_cm_unregister(ClientId),

    {ok, Client2} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_persist_on_disconnect(Config) ->
    %% Open a non-persistent session, but add the persistence when
    %% shutting down the connection. This is a protocol error, and
    %% should not convert the session into a persistent session.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 0}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),

    %% Strangely enough, the disconnect is reported as successful by emqtt.
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 30}),

    wait_for_cm_unregister(ClientId),

    {ok, Client2} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    %% The session should not be known, since it wasn't persisted because of the
    %% changed expiry interval in the disconnect call.
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_process_dies_session_expires(init, Config) -> skip_ds_tc(Config);
t_process_dies_session_expires('end', _Config) -> ok.
t_process_dies_session_expires(Config) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload = <<"test">>,
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 1}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    ok = publish(Topic, Payload),

    timer:sleep(1100),

    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),

    %% We should not receive the pending message
    ?assertEqual([], receive_messages(1)),

    emqtt:disconnect(Client2).

t_publish_while_client_is_gone_qos1(Config) ->
    %% A persistent session should receive messages in its
    %% subscription even if the process owning the session dies.
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [1]} = emqtt:subscribe(Client1, STopic, qos1),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish_many(messages(Topic, [Payload1, Payload2], ?QOS_1)),

    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    Msgs = receive_messages(2),
    ?assertMatch([_, _], Msgs),
    [Msg1, Msg2] = Msgs,
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 1}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 1}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

t_publish_many_while_client_is_gone_qos1(Config) ->
    %% A persistent session should receive all of the still unacked messages
    %% for its subscriptions after the client dies or reconnects, in addition
    %% to new messages that were published while the client was gone. The order
    %% of the messages should be consistent across reconnects.
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true},
        {auto_ack, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),

    STopics = [
        <<"t/+/foo">>,
        <<"msg/feed/#">>,
        <<"loc/+/+/+">>
    ],
    [{ok, _, [?QOS_1]} = emqtt:subscribe(Client1, ST, ?QOS_1) || ST <- STopics],

    Pubs1 = [
        #mqtt_msg{topic = <<"t/42/foo">>, payload = <<"M1">>, qos = 1},
        #mqtt_msg{topic = <<"t/42/foo">>, payload = <<"M2">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/me">>, payload = <<"M3">>, qos = 1},
        #mqtt_msg{topic = <<"loc/1/2/42">>, payload = <<"M4">>, qos = 1},
        #mqtt_msg{topic = <<"t/42/foo">>, payload = <<"M5">>, qos = 1},
        #mqtt_msg{topic = <<"loc/3/4/5">>, payload = <<"M6">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/me">>, payload = <<"M7">>, qos = 1}
    ],
    ok = publish_many(Pubs1),
    NPubs1 = length(Pubs1),

    Msgs1 = receive_messages(NPubs1),
    NMsgs1 = length(Msgs1),
    ?assertEqual(NPubs1, NMsgs1),

    ct:pal("Msgs1 = ~p", [Msgs1]),

    %% TODO
    %% This assertion doesn't currently hold because `emqx_ds` doesn't enforce
    %% strict ordering reflecting client publishing order. Instead, per-topic
    %% ordering is guaranteed per each client. In fact, this violates the MQTT
    %% specification, but we deemed it acceptable for now.
    %% ?assertMatch([
    %%     #{payload := <<"M1">>},
    %%     #{payload := <<"M2">>},
    %%     #{payload := <<"M3">>},
    %%     #{payload := <<"M4">>},
    %%     #{payload := <<"M5">>},
    %%     #{payload := <<"M6">>},
    %%     #{payload := <<"M7">>}
    %% ], Msgs1),

    ?assertEqual(
        get_topicwise_order(Pubs1),
        get_topicwise_order(Msgs1),
        Msgs1
    ),

    NAcked = 4,
    [ok = emqtt:puback(Client1, PktId) || #{packet_id := PktId} <- lists:sublist(Msgs1, NAcked)],

    %% Ensure that PUBACKs are propagated to the channel.
    pong = emqtt:ping(Client1),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    Pubs2 = [
        #mqtt_msg{topic = <<"loc/3/4/5">>, payload = <<"M8">>, qos = 1},
        #mqtt_msg{topic = <<"t/100/foo">>, payload = <<"M9">>, qos = 1},
        #mqtt_msg{topic = <<"t/100/foo">>, payload = <<"M10">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/friend">>, payload = <<"M11">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/me">>, payload = <<"M12">>, qos = 1}
    ],
    ok = publish_many(Pubs2),
    NPubs2 = length(Pubs2),

    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false},
        {auto_ack, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% Try to receive _at most_ `NPubs` messages.
    %% There shouldn't be that much unacked messages in the replay anyway,
    %% but it's an easy number to pick.
    NPubs = NPubs1 + NPubs2,
    Msgs2 = receive_messages(NPubs, _Timeout = 2000),
    NMsgs2 = length(Msgs2),

    ct:pal("Msgs2 = ~p", [Msgs2]),

    ?assert(NMsgs2 < NPubs, Msgs2),
    ?assert(NMsgs2 > NPubs2, Msgs2),
    ?assert(NMsgs2 >= NPubs - NAcked, Msgs2),
    NSame = NMsgs2 - NPubs2,
    ?assert(
        lists:all(fun(#{dup := Dup}) -> Dup end, lists:sublist(Msgs2, NSame))
    ),
    ?assertNot(
        lists:all(fun(#{dup := Dup}) -> Dup end, lists:nthtail(NSame, Msgs2))
    ),
    ?assertEqual(
        [maps:with([packet_id, topic, payload], M) || M <- lists:nthtail(NMsgs1 - NSame, Msgs1)],
        [maps:with([packet_id, topic, payload], M) || M <- lists:sublist(Msgs2, NSame)]
    ),

    ok = emqtt:disconnect(Client2).

get_topicwise_order(Msgs) ->
    maps:groups_from_list(fun get_msgpub_topic/1, fun get_msgpub_payload/1, Msgs).

get_msgpub_topic(#mqtt_msg{topic = Topic}) ->
    Topic;
get_msgpub_topic(#{topic := Topic}) ->
    Topic.

get_msgpub_payload(#mqtt_msg{payload = Payload}) ->
    Payload;
get_msgpub_payload(#{payload := Payload}) ->
    Payload.

t_publish_while_client_is_gone(init, Config) -> skip_ds_tc(Config);
t_publish_while_client_is_gone('end', _Config) -> ok.
t_publish_while_client_is_gone(Config) ->
    %% A persistent session should receive messages in its
    %% subscription even if the process owning the session dies.
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    ClientId = ?config(client_id, Config),
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish_many(messages(Topic, [Payload1, Payload2])),

    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    Msgs = receive_messages(2),
    ?assertMatch([_, _], Msgs),
    [Msg1, Msg2] = Msgs,
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

%% TODO: don't skip after QoS2 support is added to DS.
t_clean_start_drops_subscriptions(init, Config) -> skip_ds_tc(Config);
t_clean_start_drops_subscriptions('end', _Config) -> ok.
t_clean_start_drops_subscriptions(Config) ->
    %% 1. A persistent session is started and disconnected.
    %% 2. While disconnected, a message is published and persisted.
    %% 3. When connecting again, the clean start flag is set, the subscription is renewed,
    %%    then we disconnect again.
    %% 4. Finally, a new connection is made with clean start set to false.
    %% The original message should not be delivered.

    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payload1 = <<"hello1">>,
    Payload2 = <<"hello2">>,
    Payload3 = <<"hello3">>,
    ClientId = ?config(client_id, Config),

    %% 1.
    {ok, Client1} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1),

    %% 3.
    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertEqual(0, client_info(session_present, Client2)),
    {ok, _, [2]} = emqtt:subscribe(Client2, STopic, qos2),

    ok = publish(Topic, Payload2),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg1)),

    ok = emqtt:disconnect(Client2),
    maybe_kill_connection_process(ClientId, Config),

    %% 4.
    {ok, Client3} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client3),

    ok = publish(Topic, Payload3),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    ok = emqtt:disconnect(Client3).

t_unsubscribe(Config) ->
    ConnFun = ?config(conn_fun, Config),
    STopic = ?config(stopic, Config),
    ClientId = ?config(client_id, Config),
    {ok, Client} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client),
    {ok, _, [2]} = emqtt:subscribe(Client, STopic, qos2),
    ?assertMatch([_], [Sub || {ST, _} = Sub <- emqtt:subscriptions(Client), ST =:= STopic]),
    {ok, _, _} = emqtt:unsubscribe(Client, STopic),
    ?assertMatch([], [Sub || {ST, _} = Sub <- emqtt:subscriptions(Client), ST =:= STopic]),
    ok = emqtt:disconnect(Client).

t_multiple_subscription_matches(init, Config) -> skip_ds_tc(Config);
t_multiple_subscription_matches('end', _Config) -> ok.
t_multiple_subscription_matches(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    STopic1 = ?config(stopic, Config),
    STopic2 = ?config(stopic_alt, Config),
    Payload = <<"test message">>,
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic1, qos2),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic2, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload),

    {ok, Client2} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% We will receive the same message twice because it matches two subscriptions.
    [Msg1, Msg2] = receive_messages(2),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg2)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),
    ok = emqtt:disconnect(Client2).

skip_ds_tc(Config) ->
    case ?config(persistent_store, Config) of
        ds ->
            {skip, "Testcase not yet supported under 'emqx_persistent_session_ds' implementation"};
        _ ->
            Config
    end.
