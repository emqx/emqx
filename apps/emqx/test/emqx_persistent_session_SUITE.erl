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
-include_lib("../include/emqx.hrl").

-compile(export_all).
-compile(nowarn_export_all).

%%--------------------------------------------------------------------
%% SUITE boilerplate
%%--------------------------------------------------------------------

all() ->
    [
        % NOTE
        % Tests are disabled while existing session persistence impl is being
        % phased out.
        % {group, persistent_store_enabled},
        {group, persistent_store_disabled}
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
    SnabbkaffeTCs = [TC || TC <- TCs, is_snabbkaffe_tc(TC)],
    GCTests = [],
    OtherTCs = (TCs -- SnabbkaffeTCs) -- GCTests,
    [
        {persistent_store_disabled, [{group, no_kill_connection_process}]},
        {no_kill_connection_process, [], [{group, tcp}, {group, quic}, {group, ws}]},
        {snabbkaffe, [], [
            {group, tcp_snabbkaffe}, {group, quic_snabbkaffe}, {group, ws_snabbkaffe}
        ]},
        {tcp, [], OtherTCs},
        {quic, [], OtherTCs},
        {ws, [], OtherTCs},
        {tcp_snabbkaffe, [], SnabbkaffeTCs},
        {quic_snabbkaffe, [], SnabbkaffeTCs},
        {ws_snabbkaffe, [], SnabbkaffeTCs}
    ].

is_snabbkaffe_tc(TC) ->
    re:run(atom_to_list(TC), "^t_snabbkaffe_") /= nomatch.

init_per_group(persistent_store_enabled, Config) ->
    [{persistent_store_enabled, true} | Config];
init_per_group(persistent_store_disabled, Config) ->
    %% Start Apps
    emqx_common_test_helpers:boot_modules(all),
    meck:new(emqx_config, [non_strict, passthrough, no_history, no_link]),
    meck:expect(emqx_config, get, fun
        ([persistent_session_store, enabled]) -> false;
        (Other) -> meck:passthrough([Other])
    end),
    emqx_common_test_helpers:start_apps([], fun set_special_confs/1),
    [{persistent_store_enabled, false} | Config];
init_per_group(Group, Config) when Group == ws; Group == ws_snabbkaffe ->
    [
        {ssl, false},
        {host, "localhost"},
        {enable_websocket, true},
        {port, 8083},
        {conn_fun, ws_connect}
        | Config
    ];
init_per_group(Group, Config) when Group == tcp; Group == tcp_snabbkaffe ->
    [{port, 1883}, {conn_fun, connect} | Config];
init_per_group(Group, Config) when Group == quic; Group == quic_snabbkaffe ->
    UdpPort = 1883,
    emqx_common_test_helpers:ensure_quic_listener(?MODULE, UdpPort),
    [{port, UdpPort}, {conn_fun, quic_connect} | Config];
init_per_group(no_kill_connection_process, Config) ->
    [{kill_connection_process, false} | Config];
init_per_group(kill_connection_process, Config) ->
    [{kill_connection_process, true} | Config];
init_per_group(snabbkaffe, Config) ->
    [{kill_connection_process, true} | Config].

init_per_suite(Config) ->
    Config.

set_special_confs(_) ->
    ok.

end_per_suite(_Config) ->
    emqx_common_test_helpers:ensure_mnesia_stopped(),
    ok.

end_per_group(gc_tests, Config) ->
    meck:unload(mnesia),
    ?config(store_owner, Config) ! stop,
    ok;
end_per_group(Group, _Config) when
    Group =:= ram_tables; Group =:= disc_tables
->
    meck:unload(emqx_config),
    emqx_common_test_helpers:stop_apps([]);
end_per_group(persistent_store_disabled, _Config) ->
    meck:unload(emqx_config),
    emqx_common_test_helpers:stop_apps([]);
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    Config1 = preconfig_per_testcase(TestCase, Config),
    case erlang:function_exported(?MODULE, TestCase, 2) of
        true -> ?MODULE:TestCase(init, Config1);
        _ -> Config1
    end.

end_per_testcase(TestCase, Config) ->
    case is_snabbkaffe_tc(TestCase) of
        true -> snabbkaffe:stop();
        false -> skip
    end,
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
    receive_messages(Count, []).

receive_messages(0, Msgs) ->
    Msgs;
receive_messages(Count, Msgs) ->
    receive
        {publish, Msg} ->
            receive_messages(Count - 1, [Msg | Msgs]);
        _Other ->
            receive_messages(Count, Msgs)
    after 5000 ->
        Msgs
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

snabbkaffe_sync_publish(Topic, Payloads) ->
    Fun = fun(Client, Payload) ->
        ?check_trace(
            begin
                ?wait_async_action(
                    {ok, _} = emqtt:publish(Client, Topic, Payload, 2),
                    #{?snk_kind := ps_persist_msg, payload := Payload}
                )
            end,
            fun(_, _Trace) -> ok end
        )
    end,
    do_publish(Payloads, Fun, true).

publish(Topic, Payloads) ->
    publish(Topic, Payloads, false).

publish(Topic, Payloads, WaitForUnregister) ->
    Fun = fun(Client, Payload) ->
        {ok, _} = emqtt:publish(Client, Topic, Payload, 2)
    end,
    do_publish(Payloads, Fun, WaitForUnregister).

do_publish(Payloads = [_ | _], PublishFun, WaitForUnregister) ->
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
                lists:foreach(fun(Payload) -> PublishFun(Client, Payload) end, Payloads),
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
    end;
do_publish(Payload, PublishFun, WaitForUnregister) ->
    do_publish([Payload], PublishFun, WaitForUnregister).

%%--------------------------------------------------------------------
%% Test Cases
%%--------------------------------------------------------------------

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

    ok = publish(Topic, [Payload]),

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

    ok = publish(Topic, [Payload1, Payload2]),

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
    [Msg2, Msg1] = Msgs,
    ?assertEqual({ok, iolist_to_binary(Payload1)}, maps:find(payload, Msg1)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg1)),
    ?assertEqual({ok, iolist_to_binary(Payload2)}, maps:find(payload, Msg2)),
    ?assertEqual({ok, 2}, maps:find(qos, Msg2)),

    ok = emqtt:disconnect(Client2).

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

%%--------------------------------------------------------------------
%% Snabbkaffe helpers
%%--------------------------------------------------------------------

check_snabbkaffe_vanilla(Trace) ->
    ResumeTrace = [
        T
     || #{?snk_kind := K} = T <- Trace,
        re:run(to_list(K), "^ps_") /= nomatch
    ],
    ?assertMatch([_ | _], ResumeTrace),
    [_Sid] = lists:usort(?projection(sid, ResumeTrace)),
    %% Check internal flow of the emqx_cm resuming
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_resuming},
            #{?snk_kind := ps_initial_pendings},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_initial_pendings},
            #{?snk_kind := ps_persist_pendings},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_persist_pendings},
            #{?snk_kind := ps_notify_writers},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_notify_writers},
            #{?snk_kind := ps_node_markers},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_node_markers},
            #{?snk_kind := ps_resume_session},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_resume_session},
            #{?snk_kind := ps_marker_pendings},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_marker_pendings},
            #{?snk_kind := ps_marker_pendings_msgs},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_marker_pendings_msgs},
            #{?snk_kind := ps_resume_end},
            ResumeTrace
        )
    ),

    %% Check flow between worker and emqx_cm
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_notify_writers},
            #{?snk_kind := ps_worker_started},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_marker_pendings},
            #{?snk_kind := ps_worker_resume_end},
            ResumeTrace
        )
    ),
    ?assert(
        ?strict_causality(
            #{?snk_kind := ps_worker_resume_end},
            #{?snk_kind := ps_worker_shutdown},
            ResumeTrace
        )
    ),

    [Markers] = ?projection(markers, ?of_kind(ps_node_markers, Trace)),
    ?assertMatch([_], Markers).

to_list(L) when is_list(L) -> L;
to_list(A) when is_atom(A) -> atom_to_list(A);
to_list(B) when is_binary(B) -> binary_to_list(B).

%%--------------------------------------------------------------------
%% Snabbkaffe tests
%%--------------------------------------------------------------------

t_snabbkaffe_vanilla_stages(Config) ->
    %% Test that all stages of session resume works ok in the simplest case
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    EmqttOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ?check_trace(
        begin
            {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
            {ok, _} = emqtt:ConnFun(Client2),
            ok = emqtt:disconnect(Client2)
        end,
        fun(ok, Trace) ->
            check_snabbkaffe_vanilla(Trace)
        end
    ),
    ok.

t_snabbkaffe_pending_messages(Config) ->
    %% Make sure there are pending messages are fetched during the init stage.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads = [<<"test", (integer_to_binary(X))/binary>> || X <- [1, 2, 3, 4, 5]],
    EmqttOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    ?check_trace(
        begin
            snabbkaffe_sync_publish(Topic, Payloads),
            {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
            {ok, _} = emqtt:ConnFun(Client2),
            Msgs = receive_messages(length(Payloads)),
            ReceivedPayloads = [P || #{payload := P} <- Msgs],
            ?assertEqual(lists:sort(ReceivedPayloads), lists:sort(Payloads)),
            ok = emqtt:disconnect(Client2)
        end,
        fun(ok, Trace) ->
            check_snabbkaffe_vanilla(Trace),
            %% Check that all messages was delivered from the DB
            [Delivers1] = ?projection(msgs, ?of_kind(ps_persist_pendings_msgs, Trace)),
            [Delivers2] = ?projection(msgs, ?of_kind(ps_marker_pendings_msgs, Trace)),
            Delivers = Delivers1 ++ Delivers2,
            ?assertEqual(length(Payloads), length(Delivers)),
            %% Check for no duplicates
            ?assertEqual(lists:usort(Delivers), lists:sort(Delivers))
        end
    ),
    ok.

t_snabbkaffe_buffered_messages(Config) ->
    %% Make sure to buffer messages during startup.
    ConnFun = ?config(conn_fun, Config),
    ClientId = ?config(client_id, Config),
    Topic = ?config(topic, Config),
    STopic = ?config(stopic, Config),
    Payloads1 = [<<"test", (integer_to_binary(X))/binary>> || X <- [1, 2, 3]],
    Payloads2 = [<<"test", (integer_to_binary(X))/binary>> || X <- [4, 5, 6]],
    EmqttOpts = [
        {proto_ver, v5},
        {clientid, ClientId},
        {properties, #{'Session-Expiry-Interval' => 30}}
        | Config
    ],
    {ok, Client1} = emqtt:start_link([{clean_start, true} | EmqttOpts]),
    {ok, _} = emqtt:ConnFun(Client1),
    {ok, _, [2]} = emqtt:subscribe(Client1, STopic, qos2),
    ok = emqtt:disconnect(Client1),
    maybe_kill_connection_process(ClientId, Config),

    publish(Topic, Payloads1),

    ?check_trace(
        begin
            %% Make the resume init phase wait until the first message is delivered.
            ?force_ordering(
                #{?snk_kind := ps_worker_deliver},
                #{?snk_kind := ps_resume_end}
            ),
            Parent = self(),
            spawn_link(fun() ->
                ?block_until(#{?snk_kind := ps_marker_pendings_msgs}, infinity, 5000),
                publish(Topic, Payloads2, true),
                Parent ! publish_done,
                ok
            end),
            {ok, Client2} = emqtt:start_link([{clean_start, false} | EmqttOpts]),
            {ok, _} = emqtt:ConnFun(Client2),
            receive
                publish_done -> ok
            after 10000 -> error(too_long_to_publish)
            end,
            Msgs = receive_messages(length(Payloads1) + length(Payloads2) + 1),
            ReceivedPayloads = [P || #{payload := P} <- Msgs],
            ?assertEqual(
                lists:sort(Payloads1 ++ Payloads2),
                lists:sort(ReceivedPayloads)
            ),
            ok = emqtt:disconnect(Client2)
        end,
        fun(ok, Trace) ->
            check_snabbkaffe_vanilla(Trace),
            %% Check that some messages was buffered in the writer process
            [Msgs] = ?projection(msgs, ?of_kind(ps_writer_pendings, Trace)),
            ?assertMatch(
                X when 0 < X andalso X =< length(Payloads2),
                length(Msgs)
            )
        end
    ),
    ok.
