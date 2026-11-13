%%--------------------------------------------------------------------
%% Copyright (c) 2021-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

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
    DurableSessionsOpts = #{<<"enable">> => false},
    EMQXOpts = #{
        <<"sys_topics">> => #{
            <<"sys_heartbeat_interval">> => <<"1s">>
        }
    },
    Opts = #{
        durable_sessions_opts => DurableSessionsOpts,
        emqx_opts => EMQXOpts,
        start_emqx_conf => false
    },
    [
        {cth_opts, Opts},
        {persistence, false}
        | Config
    ];
init_per_group(persistence_enabled, Config) ->
    DurableSessionsOpts = #{
        <<"enable">> => true,
        <<"checkpoint_interval">> => <<"100ms">>
    },
    EMQXOpts = #{
        <<"sys_topics">> => #{
            <<"sys_heartbeat_interval">> => <<"1s">>
        },
        <<"durable_storage">> => #{
            <<"messages">> => #{<<"backend">> => <<"builtin_local">>}
        }
    },
    Opts = #{
        durable_sessions_opts => DurableSessionsOpts,
        emqx_opts => EMQXOpts,
        start_emqx_conf => false
    },
    [
        {cth_opts, Opts},
        {persistence, ds}
        | Config
    ];
init_per_group(tcp, Config0) ->
    CTHOpts = ?config(cth_opts, Config0),
    Config = emqx_common_test_helpers:start_apps_ds(Config0, _ExtraApps = [], CTHOpts),
    [
        {port, get_listener_port(tcp, default)},
        {conn_fun, connect}
        | Config
    ];
init_per_group(ws, Config0) ->
    CTHOpts = ?config(cth_opts, Config0),
    Config = emqx_common_test_helpers:start_apps_ds(Config0, _ExtraApps = [], CTHOpts),
    [
        {ssl, false},
        {host, "localhost"},
        {enable_websocket, true},
        {port, get_listener_port(ws, default)},
        {conn_fun, ws_connect}
        | Config
    ];
init_per_group(quic, Config0) ->
    CTHOpts0 = #{emqx_opts := EMQXOpts0} = ?config(cth_opts, Config0),
    EMQXOpts = EMQXOpts0#{
        <<"listeners">> => #{
            <<"quic">> => #{
                <<"test">> => #{
                    <<"enable">> => true,
                    <<"ssl_options">> => #{<<"verify">> => <<"verify_peer">>}
                }
            }
        }
    },
    CTHOpts = CTHOpts0#{emqx_opts := EMQXOpts},
    Config = emqx_common_test_helpers:start_apps_ds(Config0, _ExtraApps = [], CTHOpts),
    [
        {port, get_listener_port(quic, test)},
        {conn_fun, quic_connect},
        {ssl_opts, emqx_common_test_helpers:client_mtls()},
        {ssl, true}
        | Config
    ].

get_listener_port(Type, Name) ->
    case emqx_config:get([listeners, Type, Name, bind]) of
        {_, Port} -> Port;
        Port -> Port
    end.

end_per_group(Group, Config) when Group == tcp; Group == ws; Group == quic ->
    emqx_common_test_helpers:stop_apps_ds(Config),
    ok;
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
    snabbkaffe:stop(),
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
    receive_messages(Count, 15_000).

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
        Other ->
            ct:pal("received other message:\n  ~p", [Other]),
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
                {ok, Client} = emqtt_start_and_connect(connect, [
                    {proto_ver, v5},
                    {clientid, ClientID},
                    {port, 1883}
                ]),
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
    {ok, Client} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    ?assertEqual(
        case ?config(persistence, Config) of
            false -> emqx_session_mem;
            ds -> emqx_persistent_session_ds
        end,
        emqx_cth_broker:connection_info({channel, {session, impl}}, ClientId)
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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, ClientOpts),
    MRef = erlang:monitor(process, Client1),
    true = unlink(Client1),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, ClientOpts),

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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _, [?RC_GRANTED_QOS_1]} = emqtt:subscribe(Client1, STopic, ?QOS_1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload, ?QOS_1),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload),

    {ok, Client2} = emqtt_start_and_connect(
        ConnFun,
        [
            {clientid, ClientId},
            {proto_ver, v5},
            {properties, #{'Session-Expiry-Interval' => 30}},
            {clean_start, false}
            | Config
        ]
    ),
    [Msg | _] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Topic)}, maps:find(topic, Msg)),
    ?assertEqual({ok, iolist_to_binary(Payload)}, maps:find(payload, Msg)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg)),
    ok = emqtt:disconnect(Client2).

t_without_client_id(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {error, {client_identifier_not_valid, _}} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    ok.

t_assigned_clientid_persistent_session(Config) ->
    ConnFun = ?config(conn_fun, Config),
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),

    AssignedClientId = client_info(clientid, Client1),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(AssignedClientId, Config),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, AssignedClientId},
        {proto_ver, v5},
        {clean_start, false}
        | Config
    ]),
    ?assertEqual(1, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_cancel_on_disconnect(Config) ->
    %% Open a persistent session, but cancel the persistence when
    %% shutting down the connection.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 0}),

    wait_connection_process_unregistered(ClientId),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    ?assertEqual(0, client_info(session_present, Client2)),
    ok = emqtt:disconnect(Client2).

t_persist_on_disconnect(Config) ->
    %% Open a non-persistent session, but add the persistence when
    %% shutting down the connection. This is a protocol error, and
    %% should not convert the session into a persistent session.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 0}},
        {clean_start, true}
        | Config
    ]),

    %% Strangely enough, the disconnect is reported as successful by emqtt.
    ok = emqtt:disconnect(Client1, 0, #{'Session-Expiry-Interval' => 30}),

    wait_connection_process_unregistered(ClientId),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {clean_start, false},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
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
            {ok, Client1} = emqtt_start_and_connect(ConnFun, [
                {proto_ver, v5},
                {clientid, ClientId},
                {properties, #{'Session-Expiry-Interval' => 1}},
                {clean_start, true}
                | Config
            ]),
            {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
            ok = emqtt:disconnect(Client1),

            maybe_kill_connection_process(ClientId, Config),

            ok = publish(Topic, Payload),

            timer:sleep(1500),

            {ok, Client2} = emqtt_start_and_connect(ConnFun, [
                {proto_ver, v5},
                {clientid, ClientId},
                {properties, #{'Session-Expiry-Interval' => 30}},
                {clean_start, false}
                | Config
            ]),
            ?assertEqual(0, client_info(session_present, Client2)),

            %% We should not receive the pending message
            ?assertEqual([], receive_messages(1, 5000)),

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
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _, [1]} = emqtt:subscribe(Client1, STopic, qos1),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish_many(messages(Topic, [Payload1, Payload2], ?QOS_1)),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
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
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true},
        {auto_ack, never}
        | Config
    ]),

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
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),

    %% Try to receive _at most_ `NPubs` messages.
    %% There shouldn't be that much unacked messages in the replay anyway,
    %% but it's an easy number to pick.
    NPubs = NPubs1 + NPubs2,

    Msgs2 = receive_messages(NPubs),
    NMsgs2 = length(Msgs2),

    ct:pal("Msgs2 = ~p", [Msgs2]),

    ?assert(NMsgs2 < NPubs, {NMsgs2, '<', NPubs}),
    ?assert(NMsgs2 > NPubs2, {NMsgs2, '>', NPubs2}),
    %% Once reconnected, we should receive all the published messages above (`Msgs1 ++
    %% Msgs2'), *except* those from `Msgs1' that we acked.
    ?assert(NMsgs2 == NPubs - NAcked, #{msgs2 => Msgs2, n_pubs => NPubs, n_acked => NAcked}),
    %% All messages from `Msgs1' that were not acked will be marked with `dup = true'.
    NDup = NMsgs1 - NAcked,
    ?assert(
        lists:all(fun(#{dup := Dup}) -> Dup end, lists:sublist(Msgs2, NDup))
    ),
    ?assertNot(
        lists:all(fun(#{dup := Dup}) -> Dup end, lists:nthtail(NDup, Msgs2))
    ),
    ?assertEqual(
        [maps:with([packet_id, topic, payload], M) || M <- lists:nthtail(NMsgs1 - NDup, Msgs1)],
        [maps:with([packet_id, topic, payload], M) || M <- lists:sublist(Msgs2, NDup)]
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
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ok = publish_many(messages(Topic, [Payload1, Payload2])),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    Msgs = receive_messages(2),
    ?assertMatch([_, _], Msgs),
    [Msg1, Msg2] = Msgs,
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

t_negative_pubrec(Config) ->
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 1}},
        {auto_ack, never}
        | Config
    ],
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [{clean_start, true} | ClientOpts]),
    try
        {ok, _, [?QOS_2]} = emqtt:subscribe(Client1, Topic, ?QOS_2),
        %% Given one inflight message and another waiting for send quota.
        ok = publish_many(messages(Topic, [<<"rejected">>, <<"accepted">>])),
        [#{packet_id := RejectedId, payload := <<"rejected">>}] = receive_messages(1),
        ?assertNotReceive({publish, #{client_pid := Client1}}, 100),
        %% When the subscriber rejects the first message without sending PUBCOMP.
        ok = emqtt:pubrec(Client1, RejectedId, ?RC_UNSPECIFIED_ERROR),
        ?assertEqual(pong, emqtt:ping(Client1)),
        ?assertNotReceive({pubrel, _}, 100),
        %% Then its quota is released and the next QoS 2 exchange can complete.
        [#{packet_id := AcceptedId, payload := <<"accepted">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client1, AcceptedId),
        ?assertReceive({pubrel, #{packet_id := AcceptedId}}),
        ok = emqtt:pubcomp(Client1, AcceptedId),
        ?assertEqual(pong, emqtt:ping(Client1)),
        ok = disconnect_client(Client1)
    after
        catch emqtt:stop(Client1)
    end,
    %% When the stored session is reopened, the rejected message must not replay
    %% as either PUBLISH or PUBREL, and the subscription must still work.
    maybe_kill_connection_process(ClientId, Config),
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [{clean_start, false} | ClientOpts]),
    try
        ?assertEqual(1, client_info(session_present, Client2)),
        ok = publish(Topic, <<"after reconnect">>),
        [#{packet_id := NextId, payload := <<"after reconnect">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client2, NextId),
        ?assertReceive({pubrel, #{packet_id := NextId}}),
        ok = emqtt:pubcomp(Client2, NextId),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ?assertEqual([], receive_messages(1, 100)),
        ok = disconnect_client(Client2)
    after
        catch emqtt:stop(Client2)
    end.

t_negative_pubrec_after_transient(Config) ->
    case ?config(persistence, Config) of
        false -> {skip, "Only durable sessions omit transient messages on replay"};
        ds -> do_t_negative_pubrec_after_transient(Config)
    end.

do_t_negative_pubrec_after_transient(Config) ->
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 2}},
        {auto_ack, never}
        | Config
    ],
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [{clean_start, true} | ClientOpts]),
    RejectedId =
        try
            {ok, _, [?QOS_2]} = emqtt:subscribe(Client1, Topic, ?QOS_2),
            %% A transient delivery consumes a sequence number but is not stored.
            [Pid] = emqx_cm:lookup_channels(ClientId),
            Msg = emqx_message:make(<<"test">>, ?QOS_2, Topic, <<"transient">>),
            Pid ! {deliver, Topic, Msg},
            [#{payload := <<"transient">>}] = receive_messages(1),
            ok = publish(Topic, <<"rejected">>),
            [#{packet_id := Id, payload := <<"rejected">>}] = receive_messages(1),
            ok = disconnect_client(Client1),
            Id
        after
            catch emqtt:stop(Client1)
        end,
    maybe_kill_connection_process(ClientId, Config),
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [{clean_start, false} | ClientOpts]),
    try
        %% Replay omits the transient message, leaving a gap before this PUBLISH.
        [#{packet_id := RejectedId, payload := <<"rejected">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client2, RejectedId, ?RC_UNSPECIFIED_ERROR),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ?assertNotReceive({pubrel, _}, 100),
        %% Rejecting the oldest remaining exchange must unblock the next batch.
        ok = publish(Topic, <<"after rejection">>),
        [#{packet_id := NextId, payload := <<"after rejection">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client2, NextId),
        ?assertReceive({pubrel, #{packet_id := NextId}}),
        ok = emqtt:pubcomp(Client2, NextId),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ok = disconnect_client(Client2)
    after
        catch emqtt:stop(Client2)
    end.

t_negative_pubrec_with_pending_pubcomp(Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    {ok, Client} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ?config(client_id, Config)},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 2}},
        {auto_ack, never},
        {clean_start, true}
        | Config
    ]),
    try
        RejectedTopic = <<Topic/binary, "/rejected">>,
        QueuedTopic = <<Topic/binary, "/queued">>,
        lists:foreach(
            fun(T) -> {ok, _, [?QOS_2]} = emqtt:subscribe(Client, T, ?QOS_2) end,
            [Topic, RejectedTopic, QueuedTopic]
        ),
        %% Independent streams let the third message wait for send quota even
        %% if DS returns each publication in a separate batch.
        ok = publish(Topic, <<"pending">>),
        [#{packet_id := PendingId, payload := <<"pending">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client, PendingId),
        ?assertReceive({pubrel, #{packet_id := PendingId}}),
        ok = publish(RejectedTopic, <<"rejected">>),
        [#{packet_id := RejectedId, payload := <<"rejected">>}] = receive_messages(1),
        ok = publish(QueuedTopic, <<"queued">>),
        ?assertNotReceive({publish, #{client_pid := Client}}, 100),
        %% When the second message is rejected while the first awaits PUBCOMP.
        ok = emqtt:pubrec(Client, RejectedId, ?RC_QUOTA_EXCEEDED),
        ?assertEqual(pong, emqtt:ping(Client)),
        ?assertNotReceive({pubrel, _}, 100),
        %% Then rejection releases quota independently of the earlier exchange.
        {publish, #{packet_id := QueuedId}} = ?assertReceive(
            {publish, #{client_pid := Client, payload := <<"queued">>, packet_id := _}},
            3000
        ),
        %% Completing the earlier exchange must not cause the rejected message
        %% to reappear as a PUBREL.
        ok = emqtt:pubcomp(Client, PendingId),
        ?assertEqual(pong, emqtt:ping(Client)),
        ?assertNotReceive({pubrel, _}, 100),
        ok = emqtt:pubrec(Client, QueuedId),
        ?assertReceive({pubrel, #{packet_id := QueuedId}}),
        ok = emqtt:pubcomp(Client, QueuedId),
        ?assertEqual(pong, emqtt:ping(Client)),
        ok = disconnect_client(Client)
    after
        catch emqtt:stop(Client)
    end.

t_negative_pubrec_pending_reconnect(Config) ->
    ClientId = ?config(client_id, Config),
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    ClientOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => 2}},
        {auto_ack, never}
        | Config
    ],
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [{clean_start, true} | ClientOpts]),
    PendingId =
        try
            {ok, _, [?QOS_2]} = emqtt:subscribe(Client1, Topic, ?QOS_2),
            %% Given one accepted exchange followed by two rejected messages.
            ok = publish(Topic, <<"pending">>),
            [#{packet_id := Id1, qos := ?QOS_2}] = receive_messages(1),
            ok = emqtt:pubrec(Client1, Id1),
            ?assertReceive({pubrel, #{packet_id := Id1}}),
            %% Separate subscriptions avoid depending on DS batch boundaries:
            %% the first stream remains blocked until its PUBCOMP arrives.
            lists:foreach(
                fun({Suffix, ReasonCode}) ->
                    RejectedTopic = <<Topic/binary, "/", Suffix/binary>>,
                    {ok, _, [?QOS_2]} = emqtt:subscribe(Client1, RejectedTopic, ?QOS_2),
                    ok = publish(RejectedTopic, <<"rejected">>),
                    [#{packet_id := Id, qos := ?QOS_2}] = receive_messages(1),
                    ok = emqtt:pubrec(Client1, Id, ReasonCode)
                end,
                [{<<"reject1">>, ?RC_UNSPECIFIED_ERROR}, {<<"reject2">>, ?RC_QUOTA_EXCEEDED}]
            ),
            ?assertEqual(pong, emqtt:ping(Client1)),
            ?assertNotReceive({pubrel, _}, 100),
            ok = disconnect_client(Client1),
            Id1
        after
            catch emqtt:stop(Client1)
        end,
    %% When the session is restored before the first PUBCOMP arrives, only the
    %% accepted exchange may replay. The rejections must survive the checkpoint.
    maybe_kill_connection_process(ClientId, Config),
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [{clean_start, false} | ClientOpts]),
    try
        ?assertEqual(1, client_info(session_present, Client2)),
        ?assertMatch([{pubrel, #{packet_id := PendingId}}], receive_messages(2, 1000)),
        ok = emqtt:pubcomp(Client2, PendingId),
        ?assertEqual(pong, emqtt:ping(Client2)),
        %% Completing that exchange must also advance past both rejections.
        case ?config(persistence, Config) of
            ds ->
                #{s := State} = emqx_cth_broker:connection_info({channel, session_state}, Client2),
                ?assertEqual(#{}, emqx_persistent_session_ds_state:get_qos2_rejected(State));
            false ->
                ok
        end,
        ok = publish(Topic, <<"after reconnect">>),
        [#{packet_id := NextId, payload := <<"after reconnect">>}] = receive_messages(1),
        ok = emqtt:pubrec(Client2, NextId),
        ?assertReceive({pubrel, #{packet_id := NextId}}),
        ok = emqtt:pubcomp(Client2, NextId),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ?assertEqual([], receive_messages(1, 100)),
        ok = disconnect_client(Client2)
    after
        catch emqtt:stop(Client2)
    end.

-doc "Reconcile saved rejections when reconnect omits all earlier transient exchanges.".
t_negative_pubrec_transient_reconnect(Config) ->
    rejected_transient_reconnect(false, Config).

-doc "Reconcile saved rejections after a surviving exchange completes across a transient gap.".
t_negative_pubrec_surviving_reconnect(Config) ->
    rejected_transient_reconnect(true, Config).

rejected_transient_reconnect(Survives, Config) ->
    case ?config(persistence, Config) of
        false -> {skip, requires_persistent_session};
        ds -> do_rejected_transient_reconnect(Survives, Config)
    end.

do_rejected_transient_reconnect(Survives, Config) ->
    Topic = ?config(topic, Config),
    RejectedTopics = [<<Topic/binary, "/reject1">>, <<Topic/binary, "/reject2">>],
    Client1 = rejection_test_client(true, 4, Config),
    PendingId =
        try
            subscribe_rejection_topics(Client1, [Topic | RejectedTopics]),
            Pending =
                case Survives of
                    true ->
                        ok = publish(Topic, <<"pending">>),
                        [#{packet_id := Id}] = receive_messages(1),
                        accept_qos2(Client1, Id),
                        Id;
                    false ->
                        undefined
                end,
            [Pid] = emqx_cm:lookup_channels(?config(client_id, Config)),
            lists:foreach(
                fun(RejectedTopic) ->
                    Msg = emqx_message:make(<<"test">>, ?QOS_2, Topic, <<"transient">>),
                    Pid ! {deliver, Topic, Msg},
                    [#{packet_id := TransientId, payload := <<"transient">>}] = receive_messages(1),
                    accept_qos2(Client1, TransientId),
                    ok = publish(RejectedTopic, <<"rejected">>),
                    [#{packet_id := RejectedId}] = receive_messages(1),
                    ok = emqtt:pubrec(Client1, RejectedId, ?RC_UNSPECIFIED_ERROR)
                end,
                RejectedTopics
            ),
            ?assertEqual(pong, emqtt:ping(Client1)),
            ?assertEqual(2, map_size(saved_rejections(Client1))),
            ok = disconnect_client(Client1),
            Pending
        after
            catch emqtt:stop(Client1)
        end,
    maybe_kill_connection_process(?config(client_id, Config), Config),
    Client2 = rejection_test_client(false, 4, Config),
    try
        ?assertEqual(1, client_info(session_present, Client2)),
        case Survives of
            true ->
                ?assertMatch([{pubrel, #{packet_id := PendingId}}], receive_messages(2, 500)),
                ?assertEqual(2, map_size(saved_rejections(Client2)));
            false ->
                ?assertEqual([], receive_messages(1, 100)),
                ?assertEqual(#{}, saved_rejections(Client2))
        end,
        [ok = publish(T, <<"after reconnect">>) || T <- RejectedTopics],
        case Survives of
            true ->
                ?assertEqual([], receive_messages(1, 100)),
                ok = emqtt:pubcomp(Client2, PendingId);
            false ->
                ok
        end,
        complete_rejection_test_messages(Client2, 2),
        ?assertEqual(#{}, saved_rejections(Client2)),
        ok = disconnect_client(Client2)
    after
        catch emqtt:stop(Client2)
    end,
    maybe_kill_connection_process(?config(client_id, Config), Config),
    Client3 = rejection_test_client(false, 4, Config),
    try
        ?assertEqual([], receive_messages(1, 100)),
        ?assertEqual(#{}, saved_rejections(Client3)),
        [ok = publish(T, <<"after checkpoint">>) || T <- RejectedTopics],
        complete_rejection_test_messages(Client3, 2),
        ok = disconnect_client(Client3)
    after
        catch emqtt:stop(Client3)
    end.

-doc "A rejection after a replayed PUBCOMP cannot create extra send quota.".
t_negative_pubrec_replayed_quota(Config) ->
    case ?config(persistence, Config) of
        false -> {skip, requires_persistent_session};
        ds -> negative_pubrec_replayed_quota(Config)
    end.

negative_pubrec_replayed_quota(Config) ->
    Topic = ?config(topic, Config),
    Topics = [<<Topic/binary, "/", N>> || N <- lists:seq($1, $5)],
    [Old1, Old2, Rejected, Next1, Next2] = Topics,
    Client1 = rejection_test_client(true, 2, Config),
    [Id1, Id2] =
        try
            subscribe_rejection_topics(Client1, Topics),
            Ids = lists:map(
                fun(T) ->
                    ok = publish(T, <<"pending">>),
                    [#{packet_id := Id}] = receive_messages(1),
                    accept_qos2(Client1, Id),
                    Id
                end,
                [Old1, Old2]
            ),
            ok = disconnect_client(Client1),
            Ids
        after
            catch emqtt:stop(Client1)
        end,
    maybe_kill_connection_process(?config(client_id, Config), Config),
    Client2 = rejection_test_client(false, 1, Config),
    try
        ?assertMatch(
            [{pubrel, #{packet_id := Id1}}, {pubrel, #{packet_id := Id2}}], receive_messages(2)
        ),
        ok = publish(Rejected, <<"rejected">>),
        [#{packet_id := RejectedId}] = receive_messages(1),
        ok = emqtt:pubcomp(Client2, Id1),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ok = emqtt:pubrec(Client2, RejectedId, ?RC_UNSPECIFIED_ERROR),
        ?assertEqual(pong, emqtt:ping(Client2)),
        #{inflight := Inflight} = emqx_cth_broker:connection_info(
            {channel, session_state}, Client2
        ),
        ?assertEqual(0, emqx_persistent_session_ds_inflight:n_inflight(Inflight)),
        ok = publish(Next1, <<"next">>),
        ok = publish(Next2, <<"next">>),
        [#{packet_id := NextId1}] = receive_messages(2, 500),
        ok = emqtt:pubcomp(Client2, Id2),
        [#{packet_id := NextId2}] = receive_messages(1),
        lists:foreach(
            fun(Id) ->
                accept_qos2(Client2, Id),
                ok = emqtt:pubcomp(Client2, Id)
            end,
            [NextId1, NextId2]
        ),
        ?assertEqual(pong, emqtt:ping(Client2)),
        ok = disconnect_client(Client2)
    after
        catch emqtt:stop(Client2)
    end.

rejection_test_client(CleanStart, ReceiveMaximum, Config) ->
    {ok, Client} = emqtt_start_and_connect(?config(conn_fun, Config), [
        {proto_ver, v5},
        {clientid, ?config(client_id, Config)},
        {properties, #{'Session-Expiry-Interval' => 30, 'Receive-Maximum' => ReceiveMaximum}},
        {auto_ack, never},
        {clean_start, CleanStart}
        | Config
    ]),
    Client.

subscribe_rejection_topics(Client, Topics) ->
    lists:foreach(fun(T) -> {ok, _, [?QOS_2]} = emqtt:subscribe(Client, T, ?QOS_2) end, Topics).

accept_qos2(Client, Id) ->
    ok = emqtt:pubrec(Client, Id),
    ?assertReceive({pubrel, #{packet_id := Id}}),
    ok.

saved_rejections(Client) ->
    #{s := State} = emqx_cth_broker:connection_info({channel, session_state}, Client),
    emqx_persistent_session_ds_state:get_qos2_rejected(State).

complete_rejection_test_messages(Client, Count) ->
    Messages = receive_messages(Count),
    ?assertEqual(Count, length(Messages)),
    lists:foreach(
        fun(#{packet_id := Id}) ->
            accept_qos2(Client, Id),
            ok = emqtt:pubcomp(Client, Id)
        end,
        Messages
    ),
    ?assertEqual(pong, emqtt:ping(Client)).

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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [{clean_start, true} | ClientOpts]),
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

    ct:pal("Disconnecting..."),
    ok = disconnect_client(Client1),
    maybe_kill_connection_process(ClientId, Config),

    Pubs2 = [
        #mqtt_msg{topic = <<"t">>, payload = <<"M10">>, qos = 2},
        #mqtt_msg{topic = <<"t">>, payload = <<"M11">>, qos = 1},
        #mqtt_msg{topic = <<"t">>, payload = <<"M12">>, qos = 2}
    ],
    ok = publish_many(Pubs2),
    NPubs2 = length(Pubs2),

    ct:pal("Reconnecting..."),
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [{clean_start, false} | ClientOpts]),

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
    ct:pal("Disconnecting..."),
    pong = emqtt:ping(Client2),
    %% Reconnect for the last time
    ok = disconnect_client(Client2),
    maybe_kill_connection_process(ClientId, Config),

    ct:pal("Reconnecting..."),
    {ok, Client3} = emqtt_start_and_connect(ConnFun, [{clean_start, false} | ClientOpts]),

    %% Check that we receive the rest of the messages:
    Msgs3 = receive_messages(NPubs, _Timeout = 2000),
    ct:pal("Msgs3 = ~p", [Msgs3]),
    ?assertMatch(
        [<<"M10">>, <<"M11">>, <<"M12">>],
        [
            I
         || #{payload := I, client_pid := C3} <- Msgs3,
            C3 =:= Client3
        ]
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
    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
    {ok, _, [1]} = emqtt:subscribe(Client1, STopic, qos1),

    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    %% 2.
    ok = publish(Topic, Payload1, ?QOS_1),

    timer:sleep(1000),

    %% 3.
    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, true}
        | Config
    ]),
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
    {ok, Client3} = emqtt_start_and_connect(ConnFun, [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),

    ok = publish(Topic, Payload3, ?QOS_1),
    [Msg2] = receive_messages(1),
    ?assertEqual({ok, iolist_to_binary(Payload3)}, maps:find(payload, Msg2)),

    pong = emqtt:ping(Client3),
    ok = emqtt:disconnect(Client3).

t_unsubscribe(Config) ->
    ConnFun = ?config(conn_fun, Config),
    STopic = ?config(stopic, Config),
    ClientId = ?config(client_id, Config),
    {ok, Client} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _, [2]} = emqtt:subscribe(Client, STopic, qos2),
    ?assertMatch([_], [Sub || {ST, _} = Sub <- emqtt:subscriptions(Client), ST =:= STopic]),
    {ok, _, _} = emqtt:unsubscribe(Client, STopic),
    ?assertMatch([], [Sub || {ST, _} = Sub <- emqtt:subscriptions(Client), ST =:= STopic]),
    ok = emqtt:disconnect(Client).

t_unsubscribe_replay_qos1(Config) ->
    do_t_unsubscribe_replay(?QOS_1, Config).

t_unsubscribe_replay_qos2(Config) ->
    do_t_unsubscribe_replay(?QOS_2, Config).

%% This testcase verifies that un-acked messages that were once sent
%% to the client are retransmitted after the session
%% unsubscribes from the topic and reconnects.
do_t_unsubscribe_replay(UnackedQoS, Config) ->
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
    {ok, Sub} = emqtt_start_and_connect(ConnFun, [
        {clean_start, true}, {auto_ack, never} | ClientOpts
    ]),
    %% 1. Make two subscriptions, one is to be deleted:
    Topic1 = iolist_to_binary([TopicPrefix, $/, <<"unsub">>]),
    Topic2 = iolist_to_binary([TopicPrefix, $/, <<"sub">>]),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, Topic1, qos2)),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, Topic2, qos2)),
    %% 2. Publish 2 messages to the first and second topics each
    %% (client doesn't ack them):
    ok = publish(Topic1, <<"1">>, UnackedQoS),
    [Msg1] = receive_messages(1),
    ?assertMatch(
        [
            #{payload := <<"1">>}
        ],
        [Msg1]
    ),
    ok = publish(Topic2, <<"2">>, UnackedQoS),
    [Msg2] = receive_messages(1),
    ?assertMatch(
        [
            #{payload := <<"2">>}
        ],
        [Msg2]
    ),
    %% 3. Unsubscribe from the topic and disconnect:
    ?assertMatch({ok, _, _}, emqtt:unsubscribe(Sub, Topic1)),
    %% UNSUBACK precedes the route teardown, so without this the "5"/"6"
    %% publishes below can still reach the session and replay on reconnect.
    ?retry(100, 20, ?assertEqual([], emqx_router:match_routes(Topic1))),
    ok = emqtt:disconnect(Sub),
    %% 5. Publish more messages to the disconnected topic:
    ok = publish(Topic1, <<"5">>, ?QOS_1),
    ok = publish(Topic1, <<"6">>, ?QOS_2),
    %% 4. Reconnect the client. It must only receive only four
    %% messages from the time when it was subscribed:
    {ok, Sub1} = emqtt_start_and_connect(ConnFun, [
        {clean_start, false}, {auto_ack, true} | ClientOpts
    ]),
    %% Note: we ask for 4 messages, but expect only 2, it's
    %% intentional:
    ?assertMatch(
        #{
            Topic1 := [<<"1">>],
            Topic2 := [<<"2">>]
        },
        get_topicwise_order(receive_messages(4, 5_000)),
        debug_info(ClientId)
    ),
    %% 5. Now let's resubscribe, and check that the session can receive new messages:
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub1, Topic1, qos2)),
    %% Give it some time to ensure subscription is in place to avoid flakiness such as
    %% missing the qos 0 message...
    ct:sleep(1000),
    ok = publish(Topic1, <<"7">>, ?QOS_0),
    ok = publish(Topic1, <<"8">>, ?QOS_1),
    ok = publish(Topic1, <<"9">>, ?QOS_2),
    ?assertMatch(
        [<<"7">>, <<"8">>, <<"9">>],
        lists:map(fun get_msgpub_payload/1, receive_messages(3))
    ),
    ok = emqtt:disconnect(Sub1).

t_transient_qos1(Config) ->
    do_t_transient(?QOS_1, Config).

t_transient_qos2(Config) ->
    do_t_transient(?QOS_2, Config).

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
%%
%% `QoSNormal` is the QoS for the message published before and after the transient
%% messages are published.
do_t_transient(QoSNormal, Config) ->
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
    {ok, Sub} = emqtt_start_and_connect(ConnFun, [
        {clean_start, true}, {auto_ack, never} | ClientOpts
    ]),
    ?assertMatch({ok, _, _}, emqtt:subscribe(Sub, <<TopicPrefix/binary, "/#">>, qos2)),
    %% 2. Publish regular messages:
    publish(Topic1, <<"1">>, QoSNormal),
    Msgs1 = receive_messages(1),
    [#{payload := <<"1">>, packet_id := PI1}] = Msgs1,
    %% 3. Publish and receive transient messages:
    Deliver(Topic2, <<"2">>, ?QOS_0),
    Deliver(Topic2, <<"3">>, ?QOS_1),
    Deliver(Topic2, <<"4">>, ?QOS_2),
    Msgs2 = receive_messages(3),
    ?assertMatch(
        [
            #{payload := <<"2">>, qos := ?QOS_0},
            #{payload := <<"3">>, qos := ?QOS_1},
            #{payload := <<"4">>, qos := ?QOS_2}
        ],
        Msgs2
    ),
    %% 4. Publish more regular messages:
    publish(Topic3, <<"5">>, QoSNormal),
    Msgs3 = receive_messages(1),
    [#{payload := <<"5">>, packet_id := PI6}] = Msgs3,
    %% 5. Reconnect the client:
    ok = emqtt:disconnect(Sub),
    {ok, Sub1} = emqtt_start_and_connect(ConnFun, [
        {clean_start, false}, {auto_ack, true} | ClientOpts
    ]),
    %% 6. Receive the historic messages and check that their packet IDs didn't change:
    %% Note: durable session currenty WON'T replay transient messages.
    ProcessMessage = fun(#{payload := P, packet_id := ID}) -> {ID, P} end,
    ?assertMatch(
        #{
            Topic1 := [{PI1, <<"1">>}],
            Topic3 := [{PI6, <<"5">>}]
        },
        maps:groups_from_list(fun get_msgpub_topic/1, ProcessMessage, receive_messages(5, 5_000))
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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic1, qos2),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic2, qos2),
    ok = emqtt:disconnect(Client1),

    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payload),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),

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
            {ok, Client} = emqtt_start_and_connect(ConnFun, [
                {clientid, ClientId},
                {proto_ver, v5},
                {properties, #{'Session-Expiry-Interval' => 1}},
                {will_topic, WillTopic},
                {will_payload, WillPayload},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => 0}}
                | Config
            ]),
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
            {ok, Client} = emqtt_start_and_connect(ConnFun, [
                {clientid, ClientId},
                {proto_ver, v5},
                {properties, #{'Session-Expiry-Interval' => SessionExpiry}},
                {will_topic, WillTopic},
                {will_payload, WillPayload},
                {will_qos, 1},
                {will_props, #{'Will-Delay-Interval' => WillDelay}}
                | Config
            ]),
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

    {ok, Client1} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ]),
    {ok, _, [1]} = emqtt:subscribe(Client1, SysTopicFilter, [{qos, 1}, {rh, 2}]),
    ?assertMatch(
        [
            #{topic := SysTopic, qos := 0, retain := false, payload := _Uptime1},
            #{topic := SysTopic, qos := 0, retain := false, payload := _Uptime2}
        ],
        receive_messages(2)
    ),

    ok = emqtt:disconnect(Client1),

    {ok, Client2} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{'Session-Expiry-Interval' => 30}},
        {clean_start, false}
        | Config
    ]),
    ?assertMatch(
        [#{topic := SysTopic, qos := 0, retain := false, payload := _Uptime3}],
        receive_messages(1)
    ),

    ok = emqtt:disconnect(Client2).

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

-doc """
A QoS 1 delivery discarded as too large for the client's Maximum Packet Size
leaves the inflight window, so the next delivery is sent with Receive Maximum 1.
""".
t_deliver_too_large_qos1(Config) ->
    deliver_too_large_then_small(?QOS_1, Config).

-doc """
A QoS 2 delivery discarded as too large for the client's Maximum Packet Size
completes the QoS 2 flow, so the next delivery is sent with Receive Maximum 1.
""".
t_deliver_too_large_qos2(Config) ->
    deliver_too_large_then_small(?QOS_2, Config).

deliver_too_large_then_small(QoS, Config) ->
    ConnFun = ?config(conn_fun, Config),
    Topic = ?config(topic, Config),
    ClientId = ?config(client_id, Config),
    {ok, Client} = emqtt_start_and_connect(ConnFun, [
        {clientid, ClientId},
        {proto_ver, v5},
        {properties, #{
            'Session-Expiry-Interval' => 30,
            'Maximum-Packet-Size' => 128,
            'Receive-Maximum' => 1
        }}
        | Config
    ]),
    {ok, _, [QoS]} = emqtt:subscribe(Client, Topic, QoS),
    TooLarge0 = emqx_metrics:val('delivery.dropped.too_large'),
    publish(Topic, binary:copy(<<"x">>, 256), QoS),
    publish(Topic, <<"small">>, QoS),
    ?assertMatch([#{payload := <<"small">>}], receive_messages(1)),
    ?assertEqual(1, emqx_metrics:val('delivery.dropped.too_large') - TooLarge0),
    ok = emqtt:disconnect(Client).

emqtt_start_and_connect(ConnFun, Opts) ->
    {ok, Pid} = emqtt:start_link(Opts),
    case emqtt_connect(ConnFun, Pid) of
        {ok, _ConnAck} ->
            {ok, Pid};
        {error, {server_busy, _ConnAckProps}} ->
            timer:sleep(10),
            ClientId = proplists:get_value(clientid, Opts),
            ct:pal("~s reconnect after delay", [ClientId]),
            emqtt_start_and_connect(ConnFun, Opts);
        {error, Reason} ->
            {error, Reason}
    end.

emqtt_connect(ConnFun, ClientPid) ->
    unlink(ClientPid),
    case erlang:apply(emqtt, ConnFun, [ClientPid]) of
        {ok, ConnAck} ->
            link(ClientPid),
            {ok, ConnAck};
        {error, Reason} ->
            %% ensure failed client is killed
            exit(ClientPid, kill),
            {error, Reason}
    end.

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
