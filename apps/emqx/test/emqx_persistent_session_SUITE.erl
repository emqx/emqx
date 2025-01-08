%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/asserts.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx_utils/include/emqx_message.hrl").

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_persistent_message.hrl").

-define(EMQX_CONFIG, "sys_topics.sys_heartbeat_interval = 1s\n").

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        % NOTE
        % Tests are disabled while existing session persistence impl is being
        % phased out.
        {group, persistence_disabled},
        {group, persistence_enabled}
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
    TCsNonGeneric = [
        t_choose_impl,
        t_transient,
        t_client_replies_pubrec_when_qos1,
        t_client_replies_pubcomp_when_qos1,
        t_client_replies_puback_when_qos2
    ],
    TCGroups = [{group, tcp}, {group, quic}, {group, ws}],
    [
        {persistence_disabled, TCGroups},
        {persistence_enabled, TCGroups},
        {tcp, [], TCs},
        {quic, [], TCs -- TCsNonGeneric},
        {ws, [], TCs -- TCsNonGeneric}
    ].

init_per_group(persistence_disabled, Config) ->
    [
        {emqx_config, ?EMQX_CONFIG ++ "durable_sessions { enable = false }"},
        {persistence, false}
        | Config
    ];
init_per_group(persistence_enabled, Config) ->
    [
        {emqx_config,
            ?EMQX_CONFIG ++
                "durable_sessions {\n"
                "  enable = true\n"
                "  heartbeat_interval = 100ms\n"
                "  renew_streams_interval = 100ms\n"
                "  session_gc_interval = 2s\n"
                "}\n"
                "durable_storage.messages.backend = builtin_local"},
        {persistence, ds}
        | Config
    ];
init_per_group(tcp, Config) ->
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
init_per_group(ws, Config) ->
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
init_per_group(quic, Config) ->
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
        {ssl_opts, emqx_common_test_helpers:client_mtls()},
        {ssl, true},
        {group_apps, Apps}
        | Config
    ].

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
        {pubrel, Msg} ->
            [{pubrel, Msg} | receive_message_loop(Count - 1, Deadline)];
        _Other ->
            receive_message_loop(Count, Deadline)
    after Timeout ->
        []
    end.

maybe_kill_connection_process(ClientId, Config) ->
    Persistence = ?config(persistence, Config),
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ok;
        [ConnectionPid] when Persistence == ds ->
            Ref = monitor(process, ConnectionPid),
            ConnectionPid ! die_if_test,
            ?assertReceive(
                {'DOWN', Ref, process, ConnectionPid, Reason} when
                    Reason == normal orelse Reason == noproc,
                3000
            ),
            wait_connection_process_unregistered(ClientId);
        _ ->
            ok
    end.

wait_connection_process_dies(ClientId) ->
    case emqx_cm:lookup_channels(ClientId) of
        [] ->
            ok;
        [ConnectionPid] ->
            Ref = monitor(process, ConnectionPid),
            ?assertReceive(
                {'DOWN', Ref, process, ConnectionPid, Reason} when
                    Reason == normal orelse Reason == noproc,
                3000
            ),
            wait_connection_process_unregistered(ClientId)
    end.

wait_connection_process_unregistered(ClientId) ->
    ?retry(
        _Timeout = 100,
        _Retries = 20,
        ?assertEqual([], emqx_cm:lookup_channels(ClientId))
    ).

wait_channel_disconnected(ClientId) ->
    ?retry(
        _Timeout = 100,
        _Retries = 20,
        case emqx_cm:lookup_channels(ClientId) of
            [] ->
                false;
            [ChanPid] ->
                false = emqx_cm:is_channel_connected(ChanPid)
        end
    ).

disconnect_client(ClientPid) ->
    ClientId = proplists:get_value(clientid, emqtt:info(ClientPid)),
    ok = emqtt:disconnect(ClientPid),
    false = wait_channel_disconnected(ClientId),
    ok.

messages(Topic, Payloads) ->
    messages(Topic, Payloads, ?QOS_2).

messages(Topic, Payloads, QoS) ->
    lists:map(
        fun
            (Bin) when is_binary(Bin) ->
                #mqtt_msg{topic = Topic, payload = Bin, qos = QoS};
            (Msg = #mqtt_msg{}) ->
                Msg#mqtt_msg{topic = Topic}
        end,
        Payloads
    ).

publish(Topic, Payload) ->
    publish(Topic, Payload, ?QOS_2).

publish(Topic, Payload, QoS) ->
    publish_many(messages(Topic, [Payload], QoS)).

publish_many(Messages) ->
    publish_many(Messages, false).

publish_many(Messages, WaitForUnregister) ->
    Fun = fun(Client, Message) ->
        case emqtt:publish(Client, Message) of
            ok -> ok;
            {ok, _} -> ok
        end
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
                WaitForUnregister andalso wait_connection_process_dies(ClientID)
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
        case ?config(persistence, Config) of
            false -> emqx_session_mem;
            ds -> emqx_persistent_session_ds
        end,
        emqx_connection:info({channel, {session, impl}}, sys:get_state(ChanPid))
    ),
    ok = emqtt:disconnect(Client).

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
            ok = ?assertMatch({shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}, Reason),
            ok = emqtt:stop(Client2),
            ok
    after 1000 ->
        error({client_still_connected, Client1})
    end.

%% [MQTT-3.1.2-23]
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
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client1, STopic, ?QOS_1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, ?QOS_1),

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
    ?assertEqual({ok, ?QOS_1}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

%% [MQTT-3.1.2-23]
t_connect_session_expiry_interval_qos2(Config) ->
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

    wait_connection_process_unregistered(ClientId),

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

    wait_connection_process_unregistered(ClientId),

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

t_process_dies_session_expires(Config) ->
    %% Emulate an error in the connect process,
    %% or that the node of the process goes down.
    %% A persistent session should eventually expire.
    ?check_trace(
        begin
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

            timer:sleep(1500),

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

            emqtt:disconnect(Client2)
        end,
        []
    ).

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
        {auto_ack, never}
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
        get_topicwise_order(Msgs1)
    ),

    NAcked = 4,
    ?assert(NMsgs1 >= NAcked),
    [ok = emqtt:puback(Client1, PktId) || #{packet_id := PktId} <- lists:sublist(Msgs1, NAcked)],

    %% Ensure that PUBACKs are propagated to the channel.
    pong = emqtt:ping(Client1),

    ok = disconnect_client(Client1),
    maybe_kill_connection_process(ClientId, Config),

    Pubs2 = [
        #mqtt_msg{topic = <<"loc/3/4/6">>, payload = <<"M8">>, qos = 1},
        #mqtt_msg{topic = <<"t/100/foo">>, payload = <<"M9">>, qos = 1},
        #mqtt_msg{topic = <<"t/100/foo">>, payload = <<"M10">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/friend">>, payload = <<"M11">>, qos = 1},
        #mqtt_msg{topic = <<"msg/feed/me">>, payload = <<"M12">>, qos = 1}
    ],
    ok = publish_many(Pubs2),
    NPubs2 = length(Pubs2),

    %% Now reconnect with auto ack to make sure all streams are
    %% replayed till the end:
    {ok, Client2} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
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

    ?assert(NMsgs2 < NPubs, {NMsgs2, '<', NPubs}),
    ?assert(NMsgs2 > NPubs2, {NMsgs2, '>', NPubs2}),
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

    ok = disconnect_client(Client2).

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

t_publish_many_while_client_is_gone(Config) ->
    %% A persistent session should receive all of the still unacked messages
    %% for its subscriptions after the client dies or reconnects, in addition
    %% to PUBRELs for the messages it has PUBRECed. While client must send
    %% PUBACKs and PUBRECs in order, those orders are independent of each other.
    %%
    %% Developer's note: for simplicity we publish all messages to the
    %% same topic, since persistent session ds may reorder messages
    %% that belong to different streams, and this particular test is
    %% very sensitive the order.
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {auto_ack, never}
        | Config
    ],

    {ok, Client1} = emqtt:start_link([{clean_start, true} | ClientOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [?QOS_2]} = emqtt:subscribe(Client1, <<"t">>, ?QOS_2),

    Pubs1 = [
        #mqtt_msg{topic = <<"t">>, payload = <<"M1">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M2">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M3">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M4">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M5">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M6">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M7">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M8">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M9">>, qos = 2}
    ],
    ok = publish_many(Pubs1),
    NPubs1 = length(Pubs1),

    Msgs1 = receive_messages(NPubs1),
    ct:pal("Msgs1 = ~p", [Msgs1]),
    NMsgs1 = length(Msgs1),
    ?assertEqual(NPubs1, NMsgs1, emqx_persistent_session_ds:print_session(ClientId)),

    ?assertEqual(
        get_topicwise_order(Pubs1),
        get_topicwise_order(Msgs1),
        emqx_persistent_session_ds:print_session(ClientId)
    ),

    %% PUBACK every QoS 1 message.
    lists:foreach(
        fun(PktId) -> ok = emqtt:puback(Client1, PktId) end,
        [PktId || #{qos := 1, packet_id := PktId} <- Msgs1]
    ),

    %% PUBREC first `NRecs` QoS 2 messages (up to "M5")
    NRecs = 3,
    PubRecs1 = lists:sublist([PktId || #{qos := 2, packet_id := PktId} <- Msgs1], NRecs),
    lists:foreach(
        fun(PktId) -> ok = emqtt:pubrec(Client1, PktId) end,
        PubRecs1
    ),

    %% Ensure that PUBACKs / PUBRECs are propagated to the channel.
    pong = emqtt:ping(Client1),

    %% Receive PUBRELs for the sent PUBRECs.
    PubRels1 = receive_messages(NRecs),
    ct:pal("PubRels1 = ~p", [PubRels1]),
    ?assertEqual(
        PubRecs1,
        [PktId || {pubrel, #{packet_id := PktId}} <- PubRels1],
        PubRels1
    ),

    ok = disconnect_client(Client1),
    maybe_kill_connection_process(ClientId, Config),

    Pubs2 = [
        #mqtt_msg{topic = <<"t">>, payload = <<"M10">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M11">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M12">>, qos = 2}
    ],
    ok = publish_many(Pubs2),
    NPubs2 = length(Pubs2),

    {ok, Client2} = emqtt:start_link([{clean_start, false} | ClientOpts]),
    {ok, _} = emqtt:ConnFun(Client2),

    %% Try to receive _at most_ `NPubs` messages.
    %% There shouldn't be that much unacked messages in the replay anyway,
    %% but it's an easy number to pick.
    NPubs = NPubs1 + NPubs2,
    Msgs2 = receive_messages(NPubs, _Timeout = 2000),
    ct:pal("Msgs2 = ~p", [Msgs2]),

    %% We should again receive PUBRELs for the PUBRECs we sent earlier.
    ?assertEqual(
        get_msgs_essentials(PubRels1),
        [get_msg_essentials(PubRel) || PubRel = {pubrel, _} <- Msgs2]
    ),

    %% We should receive duplicates only for QoS 2 messages where PUBRELs were
    %% not sent, in the same order as the original messages.
    Msgs2Dups = [get_msg_essentials(M) || M = #{dup := true} <- Msgs2],
    ?assertEqual(
        Msgs2Dups,
        [M || M = #{qos := 2} <- Msgs2Dups]
    ),
    ?assertEqual(
        get_msgs_essentials(pick_respective_msgs(Msgs2Dups, Msgs1)),
        Msgs2Dups
    ),

    %% Ack more messages:
    PubRecs2 = lists:sublist([PktId || #{qos := 2, packet_id := PktId} <- Msgs2], 2),
    lists:foreach(
        fun(PktId) -> ok = emqtt:pubrec(Client2, PktId) end,
        PubRecs2
    ),

    PubRels2 = receive_messages(length(PubRecs2)),
    ct:pal("PubRels2 = ~p", [PubRels2]),
    ?assertEqual(
        PubRecs2,
        [PktId || {pubrel, #{packet_id := PktId}} <- PubRels2],
        PubRels2
    ),

    %% PUBCOMP every PUBREL.
    PubComps = [PktId || {pubrel, #{packet_id := PktId}} <- PubRels1 ++ PubRels2],
    ct:pal("PubComps: ~p", [PubComps]),
    lists:foreach(
        fun(PktId) -> ok = emqtt:pubcomp(Client2, PktId) end,
        PubComps
    ),

    %% Ensure that PUBCOMPs are propagated to the channel.
    pong = emqtt:ping(Client2),
    %% Reconnect for the last time
    ok = disconnect_client(Client2),
    maybe_kill_connection_process(ClientId, Config),

    {ok, Client3} = emqtt:start_link([{clean_start, false} | ClientOpts]),
    {ok, _} = emqtt:ConnFun(Client3),

    %% Check that we receive the rest of the messages:
    Msgs3 = receive_messages(NPubs, _Timeout = 2000),
    ct:pal("Msgs3 = ~p", [Msgs3]),
    ?assertMatch(
        [<<"M10">>, <<"M11">>, <<"M12">>],
        [I || #{payload := I} <- Msgs3]
    ),

    ok = disconnect_client(Client3).

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
    {ok, _, [1]} = emqtt:subscribe(Client1, STopic, qos1),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1, ?QOS_1),

    timer:sleep(1000),

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
    {ok, _, [1]} = emqtt:subscribe(Client2, STopic, qos1),

    timer:sleep(100),
    ok = publish(Topic, Payload2, ?QOS_1),
    [Msg1] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg1)),

    pong = emqtt:ping(Client2),
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

    ok = publish(Topic, Payload3, ?QOS_1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    pong = emqtt:ping(Client3),
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

%% This testcase verifies that un-acked messages that were once sent
%% to the client are still retransmitted after the session
%% unsubscribes from the topic and reconnects.
t_unsubscribe_replay(Config) ->
    ConnFun = ?config(conn_fun, Config),
    TopicPrefix = ?config(topic, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 10}},
        {max_inflight, 10}
        | Config
    ],
    {ok, Sub} = emqtt:start_link([{clean_start, true}, {auto_ack, never} | ClientOpts]),
    {ok, _} = emqtt:ConnFun(Sub),
    %% 1. Make two subscriptions, one is to be deleted:
    Topic1 = iolist_to_binary([TopicPrefix, $/, <<"unsub">>]),
    Topic2 = iolist_to_binary([TopicPrefix, $/, <<"sub">>]),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, Topic1, qos2)),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, Topic2, qos2)),
    %% 2. Publish 2 messages to the first and second topics each
    %% (client doesn't ack them):
    ok = publish(Topic1, <<"1">>, ?QOS_1),
    ok = publish(Topic1, <<"2">>, ?QOS_2),
    [Msg1, Msg2] = receive_messages(2),
    ?assertMatch(
        [
            #{payload := <<"1">>},
            #{payload := <<"2">>}
        ],
        [Msg1, Msg2]
    ),
    ok = publish(Topic2, <<"3">>, ?QOS_1),
    ok = publish(Topic2, <<"4">>, ?QOS_2),
    [Msg3, Msg4] = receive_messages(2),
    ?assertMatch(
        [
            #{payload := <<"3">>},
            #{payload := <<"4">>}
        ],
        [Msg3, Msg4]
    ),
    %% 3. Unsubscribe from the topic and disconnect:
    ?assertMatch({ok, _, _}, emqtt:unsubscribe(Sub, Topic1)),
    ok = emqtt:disconnect(Sub),
    %% 5. Publish more messages to the disconnected topic:
    ok = publish(Topic1, <<"5">>, ?QOS_1),
    ok = publish(Topic1, <<"6">>, ?QOS_2),
    %% 4. Reconnect the client. It must only receive only four
    %% messages from the time when it was subscribed:
    {ok, Sub1} = emqtt:start_link([{clean_start, false}, {auto_ack, true} | ClientOpts]),
    ?assertMatch({ok, _}, emqtt:ConnFun(Sub1)),
    %% Note: we ask for 6 messages, but expect only 4, it's
    %% intentional:
    ?assertMatch(
        #{
            Topic1 := [<<"1">>, <<"2">>],
            Topic2 := [<<"3">>, <<"4">>]
        },
        get_topicwise_order(receive_messages(6, 5_000)),
        debug_info(ClientId)
    ),
    %% 5. Now let's resubscribe, and check that the session can receive new messages:
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub1, Topic1, qos2)),
    ok = publish(Topic1, <<"7">>, ?QOS_0),
    ok = publish(Topic1, <<"8">>, ?QOS_1),
    ok = publish(Topic1, <<"9">>, ?QOS_2),
    ?assertMatch(
        [<<"7">>, <<"8">>, <<"9">>],
        lists:map(fun get_msgpub_payload/1, receive_messages(3))
    ),
    ok = emqtt:disconnect(Sub1).

%% This testcase verifies that persistent sessions handle "transient"
%% mesages correctly.
%%
%% Transient messages are delivered to the channel directly, bypassing
%% the broker code that decides whether the messages should be
%% persisted or not, and therefore they are not persisted.
%%
%% `emqx_retainer' is an example of application that uses this
%% mechanism.
%%
%% This testcase creates the conditions when the transient messages
%% appear in the middle of the replay, to make sure the durable
%% session doesn't get confused and/or stuck if retained messages are
%% changed while the session was down.
t_transient(Config) ->
    ConnFun = ?config(conn_fun, Config),
    TopicPrefix = ?config(topic, Config),
    ClientId = atom_to_binary(?FUNCTION_NAME),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 100}},
        {max_inflight, 100}
        | Config
    ],
    Deliver = fun(Topic, Payload, QoS) ->
        [Pid] = emqx_cm:lookup_channels(ClientId),
        Msg = emqx_message:make(_From = <<"test">>, QoS, Topic, Payload),
        Pid ! {deliver, Topic, Msg}
    end,
    Topic1 = <<TopicPrefix/binary, "/1">>,
    Topic2 = <<TopicPrefix/binary, "/2">>,
    Topic3 = <<TopicPrefix/binary, "/3">>,
    %% 1. Start the client and subscribe to the topic:
    {ok, Sub} = emqtt:start_link([{clean_start, true}, {auto_ack, never} | ClientOpts]),
    ?assertMatch({ok, _}, emqtt:ConnFun(Sub)),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, <<TopicPrefix/binary, "/#">>, qos2)),
    %% 2. Publish regular messages:
    publish(Topic1, <<"1">>, ?QOS_1),
    publish(Topic1, <<"2">>, ?QOS_2),
    Msgs1 = receive_messages(2),
    [#{payload := <<"1">>, packet_id := PI1}, #{payload := <<"2">>, packet_id := PI2}] = Msgs1,
    %% 3. Publish and recieve transient messages:
    Deliver(Topic2, <<"3">>, ?QOS_0),
    Deliver(Topic2, <<"4">>, ?QOS_1),
    Deliver(Topic2, <<"5">>, ?QOS_2),
    Msgs2 = receive_messages(3),
    ?assertMatch(
        [
            #{payload := <<"3">>, qos := ?QOS_0},
            #{payload := <<"4">>, qos := ?QOS_1},
            #{payload := <<"5">>, qos := ?QOS_2}
        ],
        Msgs2
    ),
    %% 4. Publish more regular messages:
    publish(Topic3, <<"6">>, ?QOS_1),
    publish(Topic3, <<"7">>, ?QOS_2),
    Msgs3 = receive_messages(2),
    [#{payload := <<"6">>, packet_id := PI6}, #{payload := <<"7">>, packet_id := PI7}] = Msgs3,
    %% 5. Reconnect the client:
    ok = emqtt:disconnect(Sub),
    {ok, Sub1} = emqtt:start_link([{clean_start, false}, {auto_ack, true} | ClientOpts]),
    ?assertMatch({ok, _}, emqtt:ConnFun(Sub1)),
    %% 6. Recieve the historic messages and check that their packet IDs didn't change:
    %% Note: durable session currenty WON'T replay transient messages.
    ProcessMessage = fun(#{payload := P, packet_id := ID}) -> {ID, P} end,
    ?assertMatch(
        #{
            Topic1 := [{PI1, <<"1">>}, {PI2, <<"2">>}],
            Topic3 := [{PI6, <<"6">>}, {PI7, <<"7">>}]
        },
        maps:groups_from_list(fun get_msgpub_topic/1, ProcessMessage, receive_messages(7, 5_000))
    ),
    %% 7. Finish off by sending messages to all the topics to make
    %% sure none of the streams are blocked:
    [publish(T, <<"fin">>, ?QOS_2) || T <- [Topic1, Topic2, Topic3]],
    ?assertMatch(
        #{
            Topic1 := [<<"fin">>],
            Topic2 := [<<"fin">>],
            Topic3 := [<<"fin">>]
        },
        get_topicwise_order(receive_messages(3))
    ),
    ok = emqtt:disconnect(Sub1).

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

%% Check that we don't get a will message when the client disconnects with success reason
%% code, with `Will-Delay-Interval' = 0, `Session-Expiry-Interval' > 0, QoS = 1.
t_no_will_message(Config) ->
    ConnFun = ?config(conn_fun, Config),
    WillTopic = ?config(topic, Config),
    WillPayload = <<"will message">>,
    ClientId = ?config(client_id, Config),

    ?check_trace(
        #{timetrap => 15_000},
        begin
            ok = emqx:subscribe(WillTopic, #{qos => 2}),
            {ok, Client} = emqtt:start_link([
                {clientid, ClientId},
                {proto_ver, v5},
                {properties, #{'Session-Expiry-Interval' => 1}},
                {will_topic, WillTopic},
                {will_payload, WillPayload},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => 0}}
                | Config
            ]),
            {ok, _} = emqtt:ConnFun(Client),
            ok = emqtt:disconnect(Client, ?RC_SUCCESS),

            %% No will message
            ?assertNotReceive({deliver, WillTopic, _}, 5_000),

            ok
        end,
        []
    ),
    ok.

%% Check that we get a single will message when the client disconnects with a non
%% successfull reason code, with `Will-Delay-Interval' = `Session-Expiry-Interval' > 0,
%% QoS = 1.
t_will_message1(Config) ->
    do_t_will_message(Config, #{will_delay => 1, session_expiry => 1}),
    ok.

%% Check that we get a single will message when the client disconnects with a non
%% successfull reason code, with `Will-Delay-Interval' = 0, `Session-Expiry-Interval' > 0,
%% QoS = 1.
t_will_message2(Config) ->
    do_t_will_message(Config, #{will_delay => 0, session_expiry => 1}),
    ok.

%% Check that we get a single will message when the client disconnects with a non
%% successfull reason code, with `Will-Delay-Interval' >> `Session-Expiry-Interval' > 0,
%% QoS = 1.
t_will_message3(Config) ->
    do_t_will_message(Config, #{will_delay => 300, session_expiry => 1}),
    ok.

do_t_will_message(Config, Opts) ->
    #{
        session_expiry := SessionExpiry,
        will_delay := WillDelay
    } = Opts,
    ConnFun = ?config(conn_fun, Config),
    WillTopic = ?config(topic, Config),
    WillPayload = <<"will message">>,
    ClientId = ?config(client_id, Config),

    ?check_trace(
        #{timetrap => 15_000},
        begin
            ok = emqx:subscribe(WillTopic, #{qos => 2}),
            {ok, Client} = emqtt:start_link([
                {clientid, ClientId},
                {proto_ver, v5},
                {properties, #{'Session-Expiry-Interval' => SessionExpiry}},
                {will_topic, WillTopic},
                {will_payload, WillPayload},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => WillDelay}}
                | Config
            ]),
            {ok, _} = emqtt:ConnFun(Client),
            ok = emqtt:disconnect(Client, ?RC_UNSPECIFIED_ERROR),

            ?assertReceive({deliver, WillTopic, #message{payload = WillPayload}}, 10_000),
            %% No duplicates
            ?assertNotReceive({deliver, WillTopic, _}, 100),

            ok
        end,
        []
    ),
    ok.

t_sys_message_delivery(Config) ->
    ConnFun = ?config(conn_fun, Config),
    SysTopicFilter = emqx_topic:join(["$SYS", "brokers", '+', "uptime"]),
    SysTopic = emqx_topic:join(["$SYS", "brokers", atom_to_list(node()), "uptime"]),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [1]} = emqtt:subscribe(Client1, SysTopicFilter, [{qos, 1}, {rh, 2}]),
    ?assertMatch(
        [
            #{topic := SysTopic, qos := 0, retain := false, payload := _Uptime1},
            #{topic := SysTopic, qos := 0, retain := false, payload := _Uptime2}
        ],
        receive_messages(2)
    ),

    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt:start_link([
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    {ok, _} = emqtt:ConnFun(Client2),
    ?assertMatch(
        [#{topic := SysTopic, qos := 0, retain := false, payload := _Uptime3}],
        receive_messages(1)
    ).

t_client_replies_pubrec_when_qos1(Config) ->
    Host = "127.0.0.1",
    Port = ?config(port, Config),
    {ok, Client} = emqx_mqtt_test_client:start_link(Host, Port),
    emqx_mqtt_test_client:connect(Client, #{'Session-Expiry-Interval' => 30}),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS)} = emqx_mqtt_test_client:receive_packet(),
    Topic = <<"t/1">>,
    PacketId1 = 1,
    emqx_mqtt_test_client:subscribe(
        Client,
        PacketId1,
        _Props1 = #{},
        [{Topic, #{nl => 0, qos => 2, rap => 0, rh => 0}}]
    ),
    {ok, ?SUBACK_PACKET(PacketId1, [?RC_GRANTED_QOS_2])} = emqx_mqtt_test_client:receive_packet(),
    QoS = 1,
    QoS1Msg = emqx_message:make(<<"sender">>, QoS, Topic, <<"hey">>),
    emqx:publish(QoS1Msg),
    {ok, ?PUBLISH_PACKET(QoS, PacketId2)} = emqx_mqtt_test_client:receive_packet(),
    %% Now, reply this QoS1 message with a PUBREC instead of PUBACK.
    emqx_mqtt_test_client:pubrec(Client, PacketId2, ?RC_SUCCESS, _Props2 = #{}),
    %% Should be disconnected due to protocol error
    ?assertMatch(
        {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR)},
        emqx_mqtt_test_client:receive_packet()
    ),
    ok.

t_client_replies_pubcomp_when_qos1(Config) ->
    Host = "127.0.0.1",
    Port = ?config(port, Config),
    {ok, Client} = emqx_mqtt_test_client:start_link(Host, Port),
    emqx_mqtt_test_client:connect(Client, #{'Session-Expiry-Interval' => 30}),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS)} = emqx_mqtt_test_client:receive_packet(),
    Topic = <<"t/1">>,
    PacketId1 = 1,
    emqx_mqtt_test_client:subscribe(
        Client,
        PacketId1,
        _Props1 = #{},
        [{Topic, #{nl => 0, qos => 2, rap => 0, rh => 0}}]
    ),
    {ok, ?SUBACK_PACKET(PacketId1, [?RC_GRANTED_QOS_2])} = emqx_mqtt_test_client:receive_packet(),
    QoS = 1,
    QoS1Msg = emqx_message:make(<<"sender">>, QoS, Topic, <<"hey">>),
    emqx:publish(QoS1Msg),
    {ok, ?PUBLISH_PACKET(QoS, PacketId2)} = emqx_mqtt_test_client:receive_packet(),
    %% Now, reply this QoS1 message with a PUBREC instead of PUBACK.
    emqx_mqtt_test_client:pubcomp(Client, PacketId2, ?RC_SUCCESS, _Props2 = #{}),
    %% Should be disconnected due to protocol error
    ?assertMatch(
        {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR)},
        emqx_mqtt_test_client:receive_packet()
    ),
    ok.

t_client_replies_puback_when_qos2(Config) ->
    Host = "127.0.0.1",
    Port = ?config(port, Config),
    {ok, Client} = emqx_mqtt_test_client:start_link(Host, Port),
    emqx_mqtt_test_client:connect(Client, #{'Session-Expiry-Interval' => 30}),
    {ok, ?CONNACK_PACKET(?RC_SUCCESS)} = emqx_mqtt_test_client:receive_packet(),
    Topic = <<"t/1">>,
    PacketId1 = 1,
    emqx_mqtt_test_client:subscribe(
        Client,
        PacketId1,
        _Props1 = #{},
        [{Topic, #{nl => 0, qos => 2, rap => 0, rh => 0}}]
    ),
    {ok, ?SUBACK_PACKET(PacketId1, [?RC_GRANTED_QOS_2])} = emqx_mqtt_test_client:receive_packet(),
    QoS = 2,
    QoS2Msg = emqx_message:make(<<"sender">>, QoS, Topic, <<"hey">>),
    emqx:publish(QoS2Msg),
    {ok, ?PUBLISH_PACKET(QoS, PacketId2)} = emqx_mqtt_test_client:receive_packet(),
    %% Now, reply this QoS2 message with a PUBACK instead of PUBREC.
    emqx_mqtt_test_client:puback(Client, PacketId2, ?RC_SUCCESS, _Props2 = #{}),
    %% Should be disconnected due to protocol error
    ?assertMatch(
        {ok, ?DISCONNECT_PACKET(?RC_PROTOCOL_ERROR)},
        emqx_mqtt_test_client:receive_packet()
    ),
    ok.

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

get_msg_essentials(Msg = #{}) ->
    maps:with([packet_id, topic, payload, qos], Msg);
get_msg_essentials({pubrel, Msg}) ->
    {pubrel, maps:with([packet_id, reason_code], Msg)}.

get_msgs_essentials(Msgs) ->
    [get_msg_essentials(M) || M <- Msgs].

pick_respective_msgs(MsgRefs, Msgs) ->
    [M || M <- Msgs, Ref <- MsgRefs, maps:get(packet_id, M) =:= maps:get(packet_id, Ref)].

debug_info(ClientId) ->
    Info = emqx_persistent_session_ds:print_session(ClientId),
    ct:pal("*** State:~n~p", [Info]).
