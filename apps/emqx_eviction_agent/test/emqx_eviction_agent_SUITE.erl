%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_cm.hrl").

-import(
    emqx_eviction_agent_test_helpers,
    [
        emqtt_connect/0, emqtt_connect/1, emqtt_connect/2,
        emqtt_connect_for_publish/1
    ]
).

-define(assertPrinted(Printed, Code),
    ?assertMatch(
        {match, _},
        re:run(Code, Printed)
    )
).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_eviction_agent
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(Config)
        }
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(Case, Config) ->
    _ = emqx_eviction_agent:disable(test_eviction),
    ok = snabbkaffe:start_trace(),
    start_peer(Case, Config).

start_peer(Case, Config) when
    Case =:= t_explicit_session_takeover; Case =:= t_evict_phantom_session
->
    NodeNames =
        [
            session_donor_node,
            session_recipient_node
        ],
    ClusterNodes = emqx_eviction_agent_test_helpers:start_cluster(
        [{tc_name, Case} | Config],
        NodeNames,
        [emqx_conf, emqx, emqx_eviction_agent]
    ),
    ok = snabbkaffe:start_trace(),
    [{evacuate_nodes, ClusterNodes} | Config];
start_peer(_Case, Config) ->
    Config.

end_per_testcase(TestCase, Config) ->
    emqx_eviction_agent:disable(test_eviction),
    ok = snabbkaffe:stop(),
    ok = kick_all_sessions(),
    stop_peer(TestCase, Config).

stop_peer(Case, Config) when
    Case =:= t_explicit_session_takeover; Case =:= t_evict_phantom_session
->
    emqx_eviction_agent_test_helpers:stop_cluster(
        ?config(evacuate_nodes, Config)
    );
stop_peer(_Case, _Config) ->
    ok.

%%--------------------------------------------------------------------
%% Tests
%%--------------------------------------------------------------------

t_enable_disable(_Config) ->
    erlang:process_flag(trap_exit, true),

    ?assertMatch(
        disabled,
        emqx_eviction_agent:status()
    ),

    {ok, C0} = emqtt_connect(),
    ok = emqtt:disconnect(C0),

    %% Enable
    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    %% Can't enable with different kind
    ?assertMatch(
        {error, eviction_agent_busy},
        emqx_eviction_agent:enable(bar, undefined)
    ),

    %% Enable with the same kind but different server ref
    ?assertMatch(
        ok,
        emqx_eviction_agent:enable(test_eviction, <<"srv">>)
    ),

    ?assertMatch(
        {enabled, #{}},
        emqx_eviction_agent:status()
    ),

    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_connect()
    ),

    %% Enable with the same kind and server ref and explicit options
    ?assertMatch(
        ok,
        emqx_eviction_agent:enable(test_eviction, <<"srv">>, #{allow_connections => false})
    ),

    ?assertMatch(
        {enabled, #{}},
        emqx_eviction_agent:status()
    ),

    ?assertMatch(
        {error, {use_another_server, #{}}},
        emqtt_connect()
    ),

    %% Enable with the same kind and server ref and permissive options
    ?assertMatch(
        ok,
        emqx_eviction_agent:enable(test_eviction, <<"srv">>, #{allow_connections => true})
    ),

    ?assertMatch(
        {enabled, #{}},
        emqx_eviction_agent:status()
    ),

    ?assertMatch(
        {ok, _},
        emqtt_connect()
    ),

    %% Can't enable using different kind
    ?assertMatch(
        {error, eviction_agent_busy},
        emqx_eviction_agent:disable(bar)
    ),

    ?assertMatch(
        ok,
        emqx_eviction_agent:disable(test_eviction)
    ),

    ?assertMatch(
        {error, disabled},
        emqx_eviction_agent:disable(test_eviction)
    ),

    ?assertMatch(
        disabled,
        emqx_eviction_agent:status()
    ),

    {ok, C1} = emqtt_connect(),
    ok = emqtt:disconnect(C1).

t_evict_connections_status(_Config) ->
    erlang:process_flag(trap_exit, true),

    {ok, _C} = emqtt_connect(),

    {error, disabled} = emqx_eviction_agent:evict_connections(1),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertMatch(
        {enabled, #{connections := 1, sessions := _}},
        emqx_eviction_agent:status()
    ),

    ok = emqx_eviction_agent:evict_connections(1),

    ct:sleep(100),

    ?assertMatch(
        {enabled, #{connections := 0, sessions := _}},
        emqx_eviction_agent:status()
    ),

    ok = emqx_eviction_agent:disable(test_eviction).

t_explicit_session_takeover(Config) ->
    _ = erlang:process_flag(trap_exit, true),
    ok = restart_emqx(),

    [{Node1, Port1}, {Node2, _Port2}] = ?config(evacuate_nodes, Config),

    {ok, C0} = emqtt_connect([
        {clientid, <<"client_with_session">>},
        {clean_start, false},
        {port, Port1}
    ]),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),
    emqx_cth_cluster:sync_routes([Node1, Node2]),

    ?assertEqual(
        1,
        rpc:call(Node1, emqx_eviction_agent, connection_count, [])
    ),

    [ChanPid] = rpc:call(Node1, emqx_cm, lookup_channels, [<<"client_with_session">>]),

    ok = rpc:call(Node1, emqx_eviction_agent, enable, [test_eviction, undefined]),

    ?assertWaitEvent(
        begin
            ok = rpc:call(Node1, emqx_eviction_agent, evict_connections, [1]),
            receive
                {'EXIT', C0, {shutdown, {disconnected, ?RC_USE_ANOTHER_SERVER, _}}} -> ok
            after 1000 ->
                ?assert(false, "Connection not evicted")
            end
        end,
        #{?snk_kind := emqx_cm_connected_client_count_dec, chan_pid := ChanPid},
        2000
    ),

    ?assertEqual(
        0,
        rpc:call(Node1, emqx_eviction_agent, connection_count, [])
    ),

    ?assertEqual(
        1,
        rpc:call(Node1, emqx_eviction_agent, session_count, [])
    ),

    %% First, evacuate to the same node

    ?assertWaitEvent(
        rpc:call(Node1, emqx_eviction_agent, evict_sessions, [1, Node1]),
        #{?snk_kind := emqx_channel_takeover_end, clientid := <<"client_with_session">>},
        1000
    ),
    emqx_cth_cluster:sync_routes([Node1, Node2]),

    ok = rpc:call(Node1, emqx_eviction_agent, disable, [test_eviction]),

    {ok, C1} = emqtt_connect_for_publish(Port1),
    {ok, _} = emqtt:publish(C1, <<"t1">>, <<"MessageToEvictedSession1">>, qos1),
    ok = emqtt:disconnect(C1),

    ok = rpc:call(Node1, emqx_eviction_agent, enable, [test_eviction, undefined]),

    %% Evacuate to another node

    ?assertWaitEvent(
        rpc:call(Node1, emqx_eviction_agent, evict_sessions, [1, Node2]),
        #{?snk_kind := emqx_channel_takeover_end, clientid := <<"client_with_session">>},
        1000
    ),

    ?assertEqual(
        0,
        rpc:call(Node1, emqx_eviction_agent, session_count, [])
    ),

    ?assertEqual(
        1,
        rpc:call(Node2, emqx_eviction_agent, session_count, [])
    ),

    ok = rpc:call(Node1, emqx_eviction_agent, disable, [test_eviction]),
    emqx_cth_cluster:sync_routes([Node1, Node2]),

    emqx_cth_cluster:sync_routes([Node1, Node2]),

    %% Session is on Node2, but we connect to Node1
    {ok, C2} = emqtt_connect_for_publish(Port1),
    {ok, _} = emqtt:publish(C2, <<"t1">>, <<"MessageToEvictedSession2">>, qos1),
    ok = emqtt:disconnect(C2),

    ct:sleep(100),

    %% Session is on Node2, but we connect the subscribed client to Node1
    %% It should take over the session for the third time and recieve
    %% previously published messages
    {ok, C3} = emqtt_connect([
        {clientid, <<"client_with_session">>},
        {clean_start, false},
        {port, Port1}
    ]),

    ok = assert_receive_publish(
        [
            #{payload => <<"MessageToEvictedSession1">>, topic => <<"t1">>},
            #{payload => <<"MessageToEvictedSession2">>, topic => <<"t1">>}
        ]
    ),
    ok = emqtt:disconnect(C3).

t_evict_lost_session(_Config) ->
    _ = erlang:process_flag(trap_exit, true),
    ok = restart_emqx(),

    %% Make a session
    {ok, C0} = emqtt_connect([
        {clientid, <<"client_with_session">>},
        {clean_start, false}
    ]),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),
    ok = emqtt:disconnect(C0),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ?assertEqual(1, emqx_eviction_agent:session_count()),

    %% Emulate lost session
    [ChanPid] = emqx_cm:lookup_channels(<<"client_with_session">>),
    emqx_cm_registry:unregister_channel({<<"client_with_session">>, ChanPid}),
    %% unregister is async, wait for it
    ct:sleep(100),
    ok = emqx_eviction_agent:evict_sessions(1, node()),
    ?retry(_Sleep = 10, _Retries = 20, ?assertEqual(0, emqx_eviction_agent:session_count())).

%% By phantom session we mean a session that has a record in `emqx_cm`
%% but the session process is not running for some reason.
t_evict_phantom_session(Config) ->
    [{Node1, Port1}, {Node2, _Port2}] = ?config(evacuate_nodes, Config),

    %% Make a session
    {ok, C0} = emqtt_connect([
        {clientid, <<"client_phantom_session">>},
        {clean_start, false},
        {port, Port1}
    ]),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),
    emqx_cth_cluster:sync_routes([Node1, Node2]),
    ChanInfos = rpc:call(Node1, ets, tab2list, [?CHAN_INFO_TAB]),
    ?assertEqual(1, length(ChanInfos)),
    ok = emqtt:disconnect(C0),
    ct:sleep(100),
    %% We restore saved channel info to emulate the situation
    %% when we have a record about actually missing session.
    rpc:call(Node1, ets, insert, [?CHAN_INFO_TAB, ChanInfos]),
    ?assertEqual(1, rpc:call(Node1, emqx_eviction_agent, session_count, [])),

    ok = rpc:call(Node1, emqx_eviction_agent, enable, [test_eviction, undefined]),
    ok = rpc:call(Node1, emqx_eviction_agent, evict_sessions, [1, Node2]),
    ?assertEqual(0, rpc:call(Node1, emqx_eviction_agent, session_count, [])).

t_disable_on_restart(_Config) ->
    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ok = supervisor:terminate_child(emqx_eviction_agent_sup, emqx_eviction_agent),
    {ok, _} = supervisor:restart_child(emqx_eviction_agent_sup, emqx_eviction_agent),

    ?assertEqual(
        disabled,
        emqx_eviction_agent:status()
    ).

t_session_serialization(_Config) ->
    _ = erlang:process_flag(trap_exit, true),
    ok = restart_emqx(),

    {ok, C0} = emqtt_connect(<<"client_with_session">>, false),
    {ok, _, _} = emqtt:subscribe(C0, <<"t1">>),
    ok = emqtt:disconnect(C0),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertEqual(
        1,
        emqx_eviction_agent:session_count()
    ),

    [ChanPid0] = emqx_cm:lookup_channels(<<"client_with_session">>),
    MRef0 = erlang:monitor(process, ChanPid0),

    %% Evacuate to the same node

    _ = emqx_eviction_agent:evict_sessions(1, node()),

    ?assertReceive({'DOWN', MRef0, process, ChanPid0, _}),

    ok = emqx_eviction_agent:disable(test_eviction),

    ?retry(
        200,
        10,
        ?assertEqual(
            1,
            emqx_eviction_agent:session_count()
        )
    ),

    ?assertMatch(
        #{data := [#{clientid := <<"client_with_session">>}]},
        emqx_mgmt_api:cluster_query(
            ?CHAN_INFO_TAB,
            #{},
            [],
            fun emqx_mgmt_api_clients:qs2ms/2,
            fun emqx_mgmt_api_clients:format_channel_info/2
        )
    ),

    mock_print(),

    ?assertPrinted(
        "client_with_session",
        printed(fun() -> emqx_mgmt_cli:clients(["list"]) end)
    ),

    ?assertPrinted(
        "client_with_session",
        emqx_mgmt_cli:clients(["show", "client_with_session"])
    ),

    ?assertWaitEvent(
        emqx_cm:kick_session(<<"client_with_session">>),
        #{?snk_kind := emqx_cm_clean_down, client_id := <<"client_with_session">>},
        1000
    ),

    ?assertEqual(
        0,
        emqx_eviction_agent:session_count()
    ).

-doc "A connection eviction does not publish the will message of an MQTT 5.0 client.".
t_will_msg(_Config) ->
    erlang:process_flag(trap_exit, true),
    WillTopic = <<"will_topic">>,
    ok = emqx:subscribe(WillTopic),
    {ok, _} = emqtt_connect([
        {clean_start, false},
        {clientid, <<"client_with_will">>},
        {will_payload, <<"will_msg">>},
        {will_topic, WillTopic}
    ]),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_connection(<<"client_with_will">>),
    ?assertNotReceive({deliver, WillTopic, _}, 1000),
    ok = emqx:unsubscribe(WillTopic).

-doc "A connection eviction does not publish the will message of an MQTT 3.1.1 client.".
t_will_msg_v3_conn_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v3_conn_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v4, false, []),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_connection(ClientId),
    ?assertNotReceive({deliver, WillTopic, _}, 1000),
    %% The session is still on the node, and evicting it does not publish either.
    ok = evict_session(ClientId),
    ?assertNotReceive({deliver, WillTopic, _}, 1000),
    ok = emqx:unsubscribe(WillTopic).

-doc """
A session eviction of a connected MQTT 3.1.1 client does not publish the will message.
The session keeps working after the eviction.
""".
t_will_msg_v3_session_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v3_session_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, C0} = connect_with_will(ClientId, v4, false, []),
    {ok, _, _} = emqtt:subscribe(C0, <<"t/v3">>, qos1),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_session(ClientId),
    ?assertNotReceive({deliver, WillTopic, _}, 1000),
    ?assertEqual(1, emqx_eviction_agent:session_count()),
    ok = emqx_eviction_agent:disable(test_eviction),
    _ = emqx:publish(emqx_message:make(<<"test">>, 1, <<"t/v3">>, <<"to_evicted_session">>)),
    {ok, C1} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v4}, {clean_start, false}]),
    {ok, _} = emqtt:connect(C1),
    ok = assert_receive_publish([#{payload => <<"to_evicted_session">>, topic => <<"t/v3">>}]),
    ok = emqtt:disconnect(C1),
    ok = emqx:unsubscribe(WillTopic).

-doc "A session eviction of a connected MQTT 5.0 client with will delay 0 does not publish the will message.".
t_will_msg_v5_session_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v5_session_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v5, false, [
        {properties, #{'Session-Expiry-Interval' => 600}}
    ]),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_session(ClientId),
    ?assertNotReceive({deliver, WillTopic, _}, 1000),
    ok = emqx:unsubscribe(WillTopic).

-doc "A connection eviction cancels a delayed will message of an MQTT 5.0 client.".
t_will_msg_v5_delayed_conn_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v5_delayed_will_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v5, false, [
        {properties, #{'Session-Expiry-Interval' => 600}},
        {will_props, #{'Will-Delay-Interval' => 1}}
    ]),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_connection(ClientId),
    ?assertNotReceive({deliver, WillTopic, _}, 2500),
    ok = emqx:unsubscribe(WillTopic).

-doc """
A connection eviction publishes the will message of an MQTT 3.1.1 clean session client.
The session ends with the connection, so the client goes away from the point of view of
the will message.
""".
t_will_msg_v3_clean_session_conn_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v3_clean_conn_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v4, true, []),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = evict_connection(ClientId),
    ?assertReceive({deliver, WillTopic, #message{payload = <<"will">>}}, 2000),
    ok = emqx:unsubscribe(WillTopic).

-doc """
A session eviction publishes the will message of a connected MQTT 5.0 client with
session expiry interval 0, because the session ends instead of moving.
""".
t_will_msg_v5_expiry_zero_session_eviction(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v5_expiry0_evicted">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v5, true, [
        {properties, #{'Session-Expiry-Interval' => 0}}
    ]),
    ok = emqx_eviction_agent:enable(test_eviction, undefined),
    ok = emqx_eviction_agent:evict_sessions(1, node()),
    ?assertReceive({deliver, WillTopic, #message{payload = <<"will">>}}, 2000),
    ok = emqx:unsubscribe(WillTopic).

-doc "A client takeover of an MQTT 3.1.1 session still publishes the will message.".
t_will_msg_v3_client_takeover(_Config) ->
    erlang:process_flag(trap_exit, true),
    ClientId = <<"v3_taken_over">>,
    WillTopic = will_topic(ClientId),
    ok = emqx:subscribe(WillTopic),
    {ok, _} = connect_with_will(ClientId, v4, false, []),
    {ok, C1} = emqtt:start_link([{clientid, ClientId}, {proto_ver, v4}, {clean_start, false}]),
    {ok, _} = emqtt:connect(C1),
    ?assertReceive({deliver, WillTopic, #message{payload = <<"will">>}}, 2000),
    ok = emqtt:disconnect(C1),
    ok = emqx:unsubscribe(WillTopic).

t_ws_conn(_Config) ->
    erlang:process_flag(trap_exit, true),

    ClientId = <<"ws_client">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {port, 8083},
        {ws_path, "/mqtt"}
    ]),
    {ok, _} = emqtt:ws_connect(C),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertEqual(
        1,
        emqx_eviction_agent:connection_count()
    ),

    ?assertWaitEvent(
        ok = emqx_eviction_agent:evict_connections(1),
        #{?snk_kind := emqx_cm_connected_client_count_dec},
        1000
    ),

    ?assertEqual(
        0,
        emqx_eviction_agent:connection_count()
    ).

-ifndef(BUILD_WITHOUT_QUIC).

t_quic_conn(_Config) ->
    erlang:process_flag(trap_exit, true),

    QuicPort = emqx_common_test_helpers:select_free_port(quic),
    application:ensure_all_started(quicer),
    emqx_common_test_helpers:ensure_quic_listener(?MODULE, QuicPort),

    ClientId = <<"quic_client">>,
    {ok, C} = emqtt:start_link([
        {proto_ver, v5},
        {clientid, ClientId},
        {port, QuicPort}
    ]),
    {ok, _} = emqtt:quic_connect(C),

    ok = emqx_eviction_agent:enable(test_eviction, undefined),

    ?assertEqual(
        1,
        emqx_eviction_agent:connection_count()
    ),

    ?assertWaitEvent(
        ok = emqx_eviction_agent:evict_connections(1),
        #{?snk_kind := emqx_cm_connected_client_count_dec},
        1000
    ),

    ?assertEqual(
        0,
        emqx_eviction_agent:connection_count()
    ).

-endif.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

assert_receive_publish([]) ->
    ok;
assert_receive_publish([#{payload := Msg, topic := Topic} | Rest]) ->
    receive
        {publish, #{
            payload := Msg,
            topic := Topic
        }} ->
            assert_receive_publish(Rest)
    after 5000 ->
        ?assert(false, "Message `" ++ binary_to_list(Msg) ++ "` is lost")
    end.

connect_and_publish(Topic, Message) ->
    {ok, C} = emqtt_connect(),
    emqtt:publish(C, Topic, Message),
    ok = emqtt:disconnect(C).

restart_emqx() ->
    _ = application:stop(emqx),
    _ = application:start(emqx),
    _ = application:stop(emqx_eviction_agent),
    _ = application:start(emqx_eviction_agent),
    ok.

printed(Fun) ->
    ok = meck:reset(emqx_ctl),
    _ = Fun(),
    [Out || {_Pid, {emqx_ctl, print, _Args}, Out} <- meck:history(emqx_ctl)].

mock_print() ->
    catch meck:unload(emqx_ctl),
    meck:new(emqx_ctl, [non_strict, passthrough]),
    meck:expect(emqx_ctl, print, fun(Arg) -> emqx_ctl:format(Arg, []) end),
    meck:expect(emqx_ctl, print, fun(Msg, Arg) -> emqx_ctl:format(Msg, Arg) end),
    meck:expect(emqx_ctl, usage, fun(Usages) -> emqx_ctl:format_usage(Usages) end),
    meck:expect(emqx_ctl, usage, fun(Cmd, Descr) -> emqx_ctl:format_usage(Cmd, Descr) end).

will_topic(ClientId) ->
    <<"will/", ClientId/binary>>.

connect_with_will(ClientId, ProtoVer, CleanStart, Opts) ->
    {ok, C} = emqtt:start_link(
        [
            {clientid, ClientId},
            {proto_ver, ProtoVer},
            {clean_start, CleanStart},
            {will_topic, will_topic(ClientId)},
            {will_payload, <<"will">>}
        ] ++ Opts
    ),
    {ok, _} = emqtt:connect(C),
    {ok, C}.

evict_connection(ClientId) ->
    [ChanPid] = emqx_cm:lookup_channels(ClientId),
    ?assertWaitEvent(
        ok = emqx_eviction_agent:evict_connections(1),
        #{?snk_kind := emqx_cm_connected_client_count_dec, chan_pid := ChanPid},
        2000
    ),
    ok.

evict_session(ClientId) ->
    ?assertWaitEvent(
        ok = emqx_eviction_agent:evict_sessions(1, node()),
        #{?snk_kind := emqx_channel_takeover_end, clientid := ClientId},
        2000
    ),
    ok.

kick_all_sessions() ->
    lists:foreach(fun emqx_cm:try_kick_session/1, emqx_cm:all_client_ids()),
    ?retry(100, 50, ?assertEqual([], emqx_cm:all_client_ids())),
    ok.
