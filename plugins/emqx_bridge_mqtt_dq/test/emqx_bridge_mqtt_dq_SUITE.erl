%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [emqx, emqx_conf],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    Config.

init_per_testcase(_Case, Config) ->
    %% Create the ETS table if not exists
    case ets:info(emqx_bridge_mqtt_dq_conns) of
        undefined ->
            emqx_bridge_mqtt_dq_conns = ets:new(emqx_bridge_mqtt_dq_conns, [
                public, set, named_table, {read_concurrency, true}
            ]);
        _ ->
            ets:delete_all_objects(emqx_bridge_mqtt_dq_conns)
    end,
    Config.

end_per_testcase(_Case, _Config) ->
    %% Unhook if registered
    catch emqx_bridge_mqtt_dq:unhook(),
    %% Stop all children under the top-level sup (if running)
    case whereis(emqx_bridge_mqtt_dq_sup) of
        undefined ->
            ok;
        _Pid ->
            Children = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
            lists:foreach(
                fun({Id, _, _, _}) ->
                    _ = supervisor:terminate_child(emqx_bridge_mqtt_dq_sup, Id),
                    _ = supervisor:delete_child(emqx_bridge_mqtt_dq_sup, Id)
                end,
                Children
            )
    end,
    %% Clean up persistent_term entries for buffer workers
    lists:foreach(
        fun
            ({{emqx_bridge_mqtt_dq_buffer, _, _} = Key, _}) ->
                persistent_term:erase(Key);
            (_) ->
                ok
        end,
        persistent_term:get()
    ),
    %% Clean up persistent_term entries for config
    catch persistent_term:erase({emqx_bridge_mqtt_dq_config, settings}),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_basic_forward(_Config) ->
    %% Configure one bridge that forwards test/source/# -> test/sink/${topic}
    BridgeConfig = make_bridge_config(<<"basic">>, #{
        filter_topic => <<"test/source/#">>,
        remote_topic => <<"test/sink/${topic}">>
    }),
    setup_config([BridgeConfig]),

    %% Start buffer and connector infrastructure
    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),

    %% Register the hook
    ok = emqx_bridge_mqtt_dq:hook(),

    %% Allow connectors to establish connection
    timer:sleep(500),

    %% Subscribe to the sink topic
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"test/sink/#">>, 1),
    timer:sleep(100),

    %% Publish a message to source topic
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"test/source/hello">>, <<"world">>, 0),

    %% Wait for the message to be forwarded
    Received = receive_messages(1, 3000),
    ?assertMatch([#{topic := <<"test/sink/test/source/hello">>, payload := <<"world">>}], Received),

    %% Cleanup
    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

t_topic_filter_no_match(_Config) ->
    BridgeConfig = make_bridge_config(<<"nomatch">>, #{
        filter_topic => <<"devices/#">>,
        remote_topic => <<"forwarded/${topic}">>
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    timer:sleep(500),

    %% Subscribe to forwarded topics
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub2">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"forwarded/#">>, 1),
    timer:sleep(100),

    %% Publish to a non-matching topic
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub2">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"other/topic">>, <<"data">>, 0),

    %% Should NOT receive anything
    Received = receive_messages(1, 1000),
    ?assertEqual([], Received),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

t_topic_template(_Config) ->
    BridgeConfig = make_bridge_config(<<"tmpl">>, #{
        filter_topic => <<"devices/#">>,
        remote_topic => <<"forwarded/${topic}">>
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    timer:sleep(500),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub3">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"forwarded/#">>, 1),
    timer:sleep(100),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub3">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"devices/sensor1/temp">>, <<"42">>, 0),

    Received = receive_messages(1, 3000),
    ?assertMatch(
        [#{topic := <<"forwarded/devices/sensor1/temp">>, payload := <<"42">>}],
        Received
    ),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

t_multiple_bridges(_Config) ->
    Bridge1 = make_bridge_config(<<"multi1">>, #{
        filter_topic => <<"a/#">>,
        remote_topic => <<"sink_a/${topic}">>
    }),
    Bridge2 = make_bridge_config(<<"multi2">>, #{
        filter_topic => <<"b/#">>,
        remote_topic => <<"sink_b/${topic}">>
    }),
    setup_config([Bridge1, Bridge2]),

    {ok, Sup1} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge1),
    true = unlink(Sup1),
    {ok, Sup2} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge2),
    true = unlink(Sup2),
    ok = emqx_bridge_mqtt_dq:hook(),
    timer:sleep(500),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub4">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"sink_a/#">>, 1),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"sink_b/#">>, 1),
    timer:sleep(100),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub4">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"a/hello">>, <<"from_a">>, 0),
    ok = emqtt:publish(Pub, <<"b/hello">>, <<"from_b">>, 0),
    %% This should NOT be forwarded by either bridge
    ok = emqtt:publish(Pub, <<"c/hello">>, <<"from_c">>, 0),

    Received = receive_messages(2, 3000),
    Topics = lists:sort([T || #{topic := T} <- Received]),
    ?assertEqual([<<"sink_a/a/hello">>, <<"sink_b/b/hello">>], Topics),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(Sup1, shutdown),
    exit(Sup2, shutdown),
    ok.

t_disk_queue_persistence(_Config) ->
    QueueDir = "/tmp/emqx_bridge_mqtt_dq_test_persist_" ++ integer_to_list(erlang:system_time()),
    BridgeConfig = make_bridge_config(<<"persist">>, #{
        filter_topic => <<"persist/#">>,
        remote_topic => <<"${topic}">>,
        queue_dir => list_to_binary(QueueDir)
    }),

    %% Phase 1: Start with connectors pointing to a bad port (won't connect).
    %% Messages will queue up on disk.
    BadConfig = BridgeConfig#{server := "127.0.0.1:19999"},
    setup_config([BadConfig]),
    {ok, BadSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BadConfig),
    true = unlink(BadSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    timer:sleep(500),

    %% Publish messages — they'll be buffered on disk since connectors can't connect
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub5">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    lists:foreach(
        fun(I) ->
            ok = emqtt:publish(
                Pub,
                <<"persist/data">>,
                integer_to_binary(I),
                0
            )
        end,
        lists:seq(1, 5)
    ),
    timer:sleep(500),

    %% Verify queue files exist
    QueueDirs = filelib:wildcard(QueueDir ++ "/*"),
    ?assert(length(QueueDirs) > 0, "Queue directories should exist"),

    %% Phase 2: Stop the bad bridge, start a good one pointing to local EMQX.
    %% The replayq will reopen the same directory and replay.
    exit(BadSup, shutdown),
    timer:sleep(500),
    emqx_bridge_mqtt_dq:unhook(),

    %% Clean persistent_term entries from old buffer workers
    lists:foreach(
        fun(I) ->
            catch persistent_term:erase({emqx_bridge_mqtt_dq_buffer, <<"persist">>, I})
        end,
        lists:seq(0, 7)
    ),

    %% Subscribe to receive replayed messages
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub5">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"persist/#">>, 1),
    timer:sleep(100),

    %% Start good bridge pointing to local broker
    GoodConfig = BridgeConfig#{server := "127.0.0.1:1883"},
    setup_config([GoodConfig]),
    {ok, GoodSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(GoodConfig),
    true = unlink(GoodSup),
    ok = emqx_bridge_mqtt_dq:hook(),

    %% Wait for replay and delivery
    Received = receive_messages(5, 5000),
    ?assertEqual(5, length(Received), "All 5 queued messages should be replayed"),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(GoodSup, shutdown),
    ok.

t_per_topic_ordering(_Config) ->
    BridgeConfig = make_bridge_config(<<"order">>, #{
        filter_topic => <<"order/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    timer:sleep(500),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub6">>, clean_start => true}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"order/#">>, 1),
    timer:sleep(100),

    %% Publish 10 messages to the same topic in order
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub6">>, clean_start => true}),
    {ok, _} = emqtt:connect(Pub),
    lists:foreach(
        fun(I) ->
            ok = emqtt:publish(
                Pub,
                <<"order/test">>,
                integer_to_binary(I),
                0
            )
        end,
        lists:seq(1, 10)
    ),

    Received = receive_messages(10, 5000),
    Payloads = [P || #{payload := P} <- Received],
    Expected = [integer_to_binary(I) || I <- lists:seq(1, 10)],
    ?assertEqual(Expected, Payloads, "Messages should arrive in order"),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

%%--------------------------------------------------------------------
%% sync_bridges test cases
%%--------------------------------------------------------------------

t_sync_bridges_noop_on_same_config(_Config) ->
    %% Start sup and a bridge, then sync with the same config.
    %% The bridge supervisor pid should not change.
    ensure_sup(),
    Bridge = make_bridge_config(<<"noop">>, #{
        filter_topic => <<"noop/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid1)),

    %% Sync again with identical config — pid should be the same
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assertEqual(Pid1, Pid2),
    ok.

t_sync_bridges_restart_on_config_change(_Config) ->
    %% Start a bridge, change pool_size, sync — bridge must be restarted.
    ensure_sup(),
    Bridge = make_bridge_config(<<"chg">>, #{
        filter_topic => <<"chg/#">>,
        remote_topic => <<"${topic}">>,
        pool_size => 2
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid1)),

    %% Change pool_size from 2 to 3
    Bridge2 = Bridge#{pool_size := 3},
    setup_config([Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid2)),
    ?assertNotEqual(Pid1, Pid2),
    ok.

t_sync_bridges_remove_bridge(_Config) ->
    %% Start a bridge, then sync with empty config — bridge must be removed.
    ensure_sup(),
    Bridge = make_bridge_config(<<"rm">>, #{
        filter_topic => <<"rm/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], supervisor:which_children(emqx_bridge_mqtt_dq_sup)),

    %% Remove the bridge from config
    setup_config([]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertEqual([], supervisor:which_children(emqx_bridge_mqtt_dq_sup)),
    ok.

t_sync_bridges_add_bridge(_Config) ->
    %% Start with one bridge, then add a second via sync.
    ensure_sup(),
    Bridge1 = make_bridge_config(<<"add1">>, #{
        filter_topic => <<"add1/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge1]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], supervisor:which_children(emqx_bridge_mqtt_dq_sup)),

    %% Add a second bridge
    Bridge2 = make_bridge_config(<<"add2">>, #{
        filter_topic => <<"add2/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge1, Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    Children = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assertEqual(2, length(Children)),
    ok.

t_sync_bridges_mixed(_Config) ->
    %% Start with bridges A and B.
    %% Then sync: A unchanged, B changed, C added, (A stays, B restarts, C starts).
    ensure_sup(),
    BridgeA = make_bridge_config(<<"mx_a">>, #{
        filter_topic => <<"a/#">>,
        remote_topic => <<"${topic}">>
    }),
    BridgeB = make_bridge_config(<<"mx_b">>, #{
        filter_topic => <<"b/#">>,
        remote_topic => <<"${topic}">>,
        pool_size => 2
    }),
    setup_config([BridgeA, BridgeB]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ChildMap1 = child_pid_map(emqx_bridge_mqtt_dq_sup),
    PidA1 = maps:get({bridge, <<"mx_a">>}, ChildMap1),
    PidB1 = maps:get({bridge, <<"mx_b">>}, ChildMap1),

    %% A unchanged, B changed (pool_size 2→3), C is new
    BridgeB2 = BridgeB#{pool_size := 3},
    BridgeC = make_bridge_config(<<"mx_c">>, #{
        filter_topic => <<"c/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([BridgeA, BridgeB2, BridgeC]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ChildMap2 = child_pid_map(emqx_bridge_mqtt_dq_sup),

    %% A: same pid
    ?assertEqual(PidA1, maps:get({bridge, <<"mx_a">>}, ChildMap2)),
    %% B: restarted (different pid)
    PidB2 = maps:get({bridge, <<"mx_b">>}, ChildMap2),
    ?assertNotEqual(PidB1, PidB2),
    %% C: new
    ?assert(is_pid(maps:get({bridge, <<"mx_c">>}, ChildMap2))),
    ?assertEqual(3, map_size(ChildMap2)),
    ok.

t_sync_bridges_disable_bridge(_Config) ->
    %% An enabled bridge that becomes disabled should be stopped.
    ensure_sup(),
    Bridge = make_bridge_config(<<"dis">>, #{
        filter_topic => <<"dis/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], supervisor:which_children(emqx_bridge_mqtt_dq_sup)),

    %% Disable the bridge
    DisabledBridge = Bridge#{enable := false},
    setup_config([DisabledBridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertEqual([], supervisor:which_children(emqx_bridge_mqtt_dq_sup)),
    ok.

t_sync_bridges_buffer_pool_size_change(_Config) ->
    %% Changing buffer_pool_size triggers a restart.
    ensure_sup(),
    Bridge = make_bridge_config(<<"bps">>, #{
        filter_topic => <<"bps/#">>,
        remote_topic => <<"${topic}">>,
        buffer_pool_size => 4
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),

    %% Change buffer_pool_size from 4 to 8
    Bridge2 = Bridge#{buffer_pool_size := 8},
    setup_config([Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
    ?assertNotEqual(Pid1, Pid2),
    ok.

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

ensure_sup() ->
    case whereis(emqx_bridge_mqtt_dq_sup) of
        undefined ->
            {ok, _} = emqx_bridge_mqtt_dq_sup:start_link(),
            ok;
        _ ->
            ok
    end.

child_pid_map(Sup) ->
    maps:from_list([{Id, Pid} || {Id, Pid, _, _} <- supervisor:which_children(Sup)]).

make_bridge_config(Name, Overrides) ->
    QueueDir = iolist_to_binary([<<"/tmp/emqx_bridge_mqtt_dq_test/">>, Name]),
    Default = #{
        name => Name,
        enable => true,
        server => "127.0.0.1:1883",
        proto_ver => v4,
        clientid_prefix => <<"test_dq_", Name/binary>>,
        username => <<>>,
        password => <<>>,
        clean_start => true,
        keepalive_s => 60,
        ssl => #{enable => false},
        pool_size => 2,
        buffer_pool_size => 4,
        filter_topic => <<"#">>,
        remote_topic => <<"${topic}">>,
        enqueue_timeout_ms => 5000,
        remote_qos => 1,
        remote_retain => false,
        queue_dir => QueueDir,
        seg_bytes => 1048576,
        max_total_bytes => 10485760
    },
    maps:merge(Default, Overrides).

setup_config(Bridges) ->
    Settings = #{bridges => Bridges},
    persistent_term:put({emqx_bridge_mqtt_dq_config, settings}, Settings).

receive_messages(Count, Timeout) ->
    receive_messages(Count, Timeout, []).

receive_messages(0, _Timeout, Acc) ->
    lists:reverse(Acc);
receive_messages(Count, Timeout, Acc) ->
    receive
        {publish, #{topic := Topic, payload := Payload} = Msg} ->
            receive_messages(
                Count - 1,
                Timeout,
                [#{topic => Topic, payload => Payload, msg => Msg} | Acc]
            )
    after Timeout ->
        lists:reverse(Acc)
    end.
