%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Port = emqx_common_test_helpers:select_free_port(tcp),
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{
                config => #{
                    listeners => #{
                        tcp => #{default => #{bind => "127.0.0.1:" ++ integer_to_list(Port)}},
                        ssl => #{default => #{enable => false}},
                        ws => #{default => #{enable => false}},
                        wss => #{default => #{enable => false}}
                    }
                }
            }},
            emqx_conf,
            emqx_retainer
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps}, {mqtt_port, Port} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(apps, Config)),
    Config.

init_per_testcase(_Case, Config) ->
    ok = snabbkaffe:start_trace(),
    ensure_sup(),
    ok = emqx_bridge_mqtt_dq_metrics:reset(),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = snabbkaffe:stop(),
    %% Unhook if registered
    catch emqx_bridge_mqtt_dq:unhook(),
    %% Stop all children under the top-level sup (if running)
    case whereis(emqx_bridge_mqtt_dq_sup) of
        undefined ->
            ok;
        _Pid ->
            Children = supervisor:which_children(emqx_bridge_mqtt_dq_sup),
            lists:foreach(
                fun(Child) ->
                    case is_metrics_child(Child) of
                        true ->
                            ok;
                        false ->
                            {Id, _, _, _} = Child,
                            _ = supervisor:terminate_child(emqx_bridge_mqtt_dq_sup, Id),
                            _ = supervisor:delete_child(emqx_bridge_mqtt_dq_sup, Id)
                    end
                end,
                Children
            )
    end,
    %% Clean up persistent_term entries for config
    catch persistent_term:erase({emqx_bridge_mqtt_dq_config, settings}),
    catch emqx_bridge_mqtt_dq_metrics:reset(),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

-doc "Message published to source topic is forwarded to sink topic.".
t_basic_forward(Config) ->
    Port = ?config(mqtt_port, Config),
    BridgeConfig = make_bridge_config(<<"basic">>, Port, #{
        filter_topic => <<"test/source/#">>,
        remote_topic => <<"test/sink/${topic}">>
    }),
    setup_config([BridgeConfig]),

    %% Start buffer and connector infrastructure
    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),

    %% Register the hook
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"basic">>),

    %% Subscribe to the sink topic
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"test/sink/#">>, 1),

    %% Publish a message to source topic
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub">>, port => Port}),
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

-doc "Original QoS and retain flag are preserved when remote_qos=${qos} and remote_retain=${retain}.".
t_preserve_qos_and_retain(Config) ->
    Port = ?config(mqtt_port, Config),
    BridgeConfig = make_bridge_config(<<"preserve">>, Port, #{
        filter_topic => <<"pqr/#">>,
        remote_topic => <<"sink/${topic}">>,
        remote_qos => '${qos}',
        remote_retain => '${retain}'
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"preserve">>),

    %% Subscribe at QoS 2 so the subscriber can receive any QoS level
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_pqr_sub">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [2]} = emqtt:subscribe(Sub, <<"sink/pqr/#">>, 2),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pqr_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),

    %% Publish QoS 0, retain false
    ok = emqtt:publish(Pub, <<"pqr/a">>, <<"q0">>, 0),
    %% Publish QoS 1, retain false
    {ok, _} = emqtt:publish(Pub, <<"pqr/b">>, <<"q1">>, 1),
    %% Publish QoS 1, retain true
    {ok, _} = emqtt:publish(Pub, <<"pqr/c">>, #{}, <<"q1r">>, [{qos, 1}, {retain, true}]),

    Received = receive_messages(3, 5000),
    ?assertEqual(3, length(Received), "Expected 3 forwarded messages"),

    MsgA = find_by_topic(Received, <<"sink/pqr/a">>),
    MsgB = find_by_topic(Received, <<"sink/pqr/b">>),

    ?assertEqual(0, maps:get(qos, maps:get(msg, MsgA))),
    ?assertEqual(false, maps:get(retain, maps:get(msg, MsgA))),

    ?assertEqual(1, maps:get(qos, maps:get(msg, MsgB))),
    ?assertEqual(false, maps:get(retain, maps:get(msg, MsgB))),

    %% Verify retain was preserved: subscribe AFTER publish so the broker
    %% delivers the stored retained message (MQTT sets retain=true only
    %% for retained message delivery upon subscription, not for live delivery).
    %% Wait for bridge acks to ensure the retained publish has been stored.
    _ = wait_until_snapshot(
        fun(S) ->
            case bridge_metrics(S, <<"preserve">>) of
                #{dequeue := N} when N >= 3 -> true;
                _ -> false
            end
        end,
        5000
    ),
    ok = emqtt:disconnect(Sub),
    {ok, Sub2} = emqtt:start_link(#{clientid => <<"test_pqr_sub2">>, port => Port}),
    {ok, _} = emqtt:connect(Sub2),
    {ok, _, [2]} = emqtt:subscribe(Sub2, <<"sink/pqr/c">>, 2),
    RetainedMsgs = receive_messages(1, 3000),
    ?assertMatch([#{topic := <<"sink/pqr/c">>, payload := <<"q1r">>}], RetainedMsgs),
    [RetainedMsg] = RetainedMsgs,
    ?assertEqual(true, maps:get(retain, maps:get(msg, RetainedMsg))),

    %% Clean up retained message
    ok = emqtt:publish(Pub, <<"sink/pqr/c">>, #{}, <<>>, [{qos, 0}, {retain, true}]),

    ok = emqtt:disconnect(Sub2),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

-doc "Message on non-matching topic is not forwarded.".
t_topic_filter_no_match(Config) ->
    Port = ?config(mqtt_port, Config),
    BridgeConfig = make_bridge_config(<<"nomatch">>, Port, #{
        filter_topic => <<"devices/#">>,
        remote_topic => <<"forwarded/${topic}">>
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"nomatch">>),

    %% Subscribe to forwarded topics
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub2">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"forwarded/#">>, 1),

    %% Publish to a non-matching topic
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub2">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"other/topic">>, <<"data">>, 0),

    %% Should NOT receive anything
    Received = receive_messages(1, 1000),
    ?assertEqual([], Received),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

-doc "Two bridges forward to different sinks; non-matching topic ignored.".
t_multiple_bridges(Config) ->
    Port = ?config(mqtt_port, Config),
    Bridge1 = make_bridge_config(<<"multi1">>, Port, #{
        filter_topic => <<"a/#">>,
        remote_topic => <<"sink_a/${topic}">>
    }),
    Bridge2 = make_bridge_config(<<"multi2">>, Port, #{
        filter_topic => <<"b/#">>,
        remote_topic => <<"sink_b/${topic}">>
    }),
    setup_config([Bridge1, Bridge2]),

    {ok, Sup1} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge1),
    true = unlink(Sup1),
    {ok, Sup2} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge2),
    true = unlink(Sup2),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"multi1">>),
    wait_bridge_connected(<<"multi2">>),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub4">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"sink_a/#">>, 1),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"sink_b/#">>, 1),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub4">>, port => Port}),
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

-doc "Messages queued on disk are replayed after bridge restart with good connection.".
t_disk_queue_persistence(Config) ->
    Port = ?config(mqtt_port, Config),
    QueueDir = "/tmp/emqx_bridge_mqtt_dq_test_persist_" ++ integer_to_list(erlang:system_time()),
    BridgeConfig = make_bridge_config(<<"persist">>, Port, #{
        filter_topic => <<"persist/#">>,
        remote_topic => <<"${topic}">>,
        queue_base_dir => list_to_binary(QueueDir)
    }),

    %% Phase 1: Start with connectors pointing to a bad port (won't connect).
    %% Messages will queue up on disk.
    BadConfig = BridgeConfig#{server := "127.0.0.1:19999"},
    setup_config([BadConfig]),
    {ok, BadSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BadConfig),
    true = unlink(BadSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    %% Wait for connector to fail at least once (confirms it's running)
    {ok, _} = ?block_until(#{?snk_kind := mqtt_dq_connector_connect_failed}, 10000),

    %% Publish messages — they'll be buffered on disk since connectors can't connect
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub5">>, port => Port}),
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
    %% Wait until buffer has enqueued at least one batch (messages are in replayq)
    {ok, _} = ?block_until(#{?snk_kind := mqtt_dq_buffer_enqueued}, 5000),

    %% Verify queue files exist
    QueueDirs = filelib:wildcard(QueueDir ++ "/*"),
    ?assert(length(QueueDirs) > 0, "Queue directories should exist"),

    %% Phase 2: Stop the bad bridge, start a good one pointing to local EMQX.
    %% The replayq will reopen the same directory and replay.
    BadSupMon = monitor(process, BadSup),
    exit(BadSup, shutdown),
    receive
        {'DOWN', BadSupMon, process, BadSup, _} -> ok
    after 5000 -> ct:fail("BadSup did not stop")
    end,
    emqx_bridge_mqtt_dq:unhook(),

    %% Clean persistent_term entries from old buffer workers
    %% Subscribe to receive replayed messages
    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub5">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"persist/#">>, 1),

    %% Start good bridge pointing to local broker
    Server = "127.0.0.1:" ++ integer_to_list(Port),
    GoodConfig = BridgeConfig#{server := Server},
    setup_config([GoodConfig]),
    {ok, GoodSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(GoodConfig),
    true = unlink(GoodSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"persist">>),

    %% Wait for replay and delivery
    Received = receive_messages(5, 5000),
    ?assertEqual(5, length(Received), "All 5 queued messages should be replayed"),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(GoodSup, shutdown),
    ok.

-doc "Messages on the same topic are forwarded in publish order.".
t_per_topic_ordering(Config) ->
    Port = ?config(mqtt_port, Config),
    BridgeConfig = make_bridge_config(<<"order">>, Port, #{
        filter_topic => <<"order/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([BridgeConfig]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(BridgeConfig),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"order">>),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_sub6">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"order/#">>, 1),

    %% Publish 10 messages to the same topic in order
    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_pub6">>, port => Port}),
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

-doc "Bridge counters and API payload reflect forwarded messages.".
t_remote_config_reference(_Config) ->
    RawConfig = #{
        <<"bridges">> => #{
            <<"r1">> => #{
                <<"enable">> => true,
                <<"remote">> => <<"loopback">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"devices/#">>,
                <<"remote_topic">> => <<"forwarded/${topic}">>
            }
        },
        <<"remotes">> => #{
            <<"loopback">> => #{
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"username">> => <<"u">>,
                <<"password">> => <<"p">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(RawConfig),
    [
        #{
            name := <<"r1">>,
            server := "127.0.0.1:1883",
            username := <<"u">>,
            password := <<"p">>,
            ssl := #{enable := false}
        }
    ] = emqx_bridge_mqtt_dq_config:get_bridges(),
    ok.

-doc "Remote SSL file options are parsed from config and carried into bridge config.".
t_remote_ssl_file_options(_Config) ->
    RawConfig = #{
        <<"bridges">> => #{
            <<"r1">> => #{
                <<"enable">> => true,
                <<"remote">> => <<"tls_remote">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"devices/#">>,
                <<"remote_topic">> => <<"forwarded/${topic}">>
            }
        },
        <<"remotes">> => #{
            <<"tls_remote">> => #{
                <<"server">> => <<"mqtt.example.com:8883">>,
                <<"ssl">> => #{
                    <<"enable">> => true,
                    <<"verify">> => <<"verify_peer">>,
                    <<"sni">> => <<"mqtt.example.com">>,
                    <<"cacertfile">> => <<"/etc/certs/ca.pem">>,
                    <<"certfile">> => <<"/etc/certs/client.pem">>,
                    <<"keyfile">> => <<"/etc/certs/client.key">>
                }
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(RawConfig),
    [
        #{
            ssl := #{
                enable := true,
                verify := verify_peer,
                server_name_indication := "mqtt.example.com",
                cacertfile := "/etc/certs/ca.pem",
                certfile := "/etc/certs/client.pem",
                keyfile := "/etc/certs/client.key"
            }
        }
    ] = emqx_bridge_mqtt_dq_config:get_bridges(),
    ok.

-doc "Config values wrapped in ${EMQXDQ_*} are resolved from OS environment.".
t_config_env_var_resolution(_Config) ->
    %% Set env vars for the test
    true = os:putenv("EMQXDQ_SERVER", "env.example.com:1883"),
    true = os:putenv("EMQXDQ_USER", "env_user"),
    RawConfig = #{
        <<"bridges">> => #{
            <<"env1">> => #{
                <<"enable">> => true,
                <<"server">> => <<"${EMQXDQ_SERVER}">>,
                <<"username">> => <<"${EMQXDQ_USER}">>,
                <<"password">> => <<"${EMQXDQ_UNSET_VAR}">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"env/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(RawConfig),
    [
        #{
            name := <<"env1">>,
            server := "env.example.com:1883",
            username := <<"env_user">>,
            %% Unset var keeps original string
            password := <<"${EMQXDQ_UNSET_VAR}">>
        }
    ] = emqx_bridge_mqtt_dq_config:get_bridges(),
    %% Non-EMQXDQ_ placeholders like ${topic} are left untouched
    TopicConfig = #{
        <<"bridges">> => #{
            <<"env2">> => #{
                <<"enable">> => true,
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"env2/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(TopicConfig),
    [#{remote_topic := <<"fwd/${topic}">>}] = emqx_bridge_mqtt_dq_config:get_bridges(),
    %% Malformed EMQXDQ_ placeholders must not crash parsing
    MalformedConfig = #{
        <<"bridges">> => #{
            <<"env3">> => #{
                <<"enable">> => true,
                <<"server">> => <<"${EMQXDQ_">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"env3/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(MalformedConfig),
    [#{server := "${EMQXDQ_"}] = emqx_bridge_mqtt_dq_config:get_bridges(),
    %% Cleanup
    os:unsetenv("EMQXDQ_SERVER"),
    os:unsetenv("EMQXDQ_USER"),
    ok.

-doc "Config parser handles ${qos}/${retain} placeholders and literal values.".
t_config_qos_retain_placeholders(_Config) ->
    %% Default: ${qos} and ${retain} placeholders → atoms
    DefaultConfig = #{
        <<"bridges">> => #{
            <<"qr_default">> => #{
                <<"enable">> => true,
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"qr/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(DefaultConfig),
    [#{remote_qos := '${qos}', remote_retain := '${retain}'}] =
        emqx_bridge_mqtt_dq_config:get_bridges(),
    %% Explicit string values: "2" for qos, "true" for retain
    ExplicitConfig = #{
        <<"bridges">> => #{
            <<"qr_explicit">> => #{
                <<"enable">> => true,
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"qr/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"remote_qos">> => <<"2">>,
                <<"remote_retain">> => <<"true">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(ExplicitConfig),
    [#{remote_qos := 2, remote_retain := true}] =
        emqx_bridge_mqtt_dq_config:get_bridges(),
    %% String form: "0" for qos, "false" for retain
    StringConfig = #{
        <<"bridges">> => #{
            <<"qr_string">> => #{
                <<"enable">> => true,
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"proto_ver">> => <<"v4">>,
                <<"filter_topic">> => <<"qr/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>,
                <<"remote_qos">> => <<"0">>,
                <<"remote_retain">> => <<"false">>,
                <<"ssl">> => #{<<"enable">> => false}
            }
        }
    },
    ok = emqx_bridge_mqtt_dq_config:update(StringConfig),
    [#{remote_qos := 0, remote_retain := false}] =
        emqx_bridge_mqtt_dq_config:get_bridges(),
    ok.

-doc "Invalid config is rejected by update/1 with a descriptive error message.".
t_config_invalid_rejected(_Config) ->
    %% Bridge referencing a non-existent remote
    BadRemoteRef = #{
        <<"bridges">> => #{
            <<"bad1">> => #{
                <<"enable">> => true,
                <<"remote">> => <<"no_such_remote">>,
                <<"filter_topic">> => <<"t/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>
            }
        }
    },
    ?assertMatch(
        {error, <<"bridges.bad1: ", _/binary>>},
        emqx_bridge_mqtt_dq_config:update(BadRemoteRef)
    ),
    %% Bridge with empty remote string
    EmptyRemote = #{
        <<"bridges">> => #{
            <<"bad2">> => #{
                <<"enable">> => true,
                <<"remote">> => <<>>,
                <<"filter_topic">> => <<"t/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>
            }
        }
    },
    ?assertMatch(
        {error, <<"bridges.bad2: ", _/binary>>},
        emqx_bridge_mqtt_dq_config:update(EmptyRemote)
    ),
    %% Invalid bridge name
    BadName = #{
        <<"bridges">> => #{
            <<"has spaces">> => #{
                <<"enable">> => true,
                <<"server">> => <<"127.0.0.1:1883">>,
                <<"filter_topic">> => <<"t/#">>,
                <<"remote_topic">> => <<"fwd/${topic}">>
            }
        }
    },
    ?assertMatch(
        {error, <<"bridges.has spaces: invalid name", _/binary>>},
        emqx_bridge_mqtt_dq_config:update(BadName)
    ),
    %% Invalid remote name
    BadRemoteName = #{
        <<"remotes">> => #{
            <<"has spaces">> => #{
                <<"server">> => <<"127.0.0.1:1883">>
            }
        }
    },
    ?assertMatch(
        {error, <<"remotes.has spaces: invalid name", _/binary>>},
        emqx_bridge_mqtt_dq_config:update(BadRemoteName)
    ),
    %% Invalid remote config (not a map)
    BadRemoteConfig = #{
        <<"remotes">> => #{
            <<"bad">> => <<"not_a_map">>
        }
    },
    ?assertMatch(
        {error, <<"remotes.bad: ", _/binary>>},
        emqx_bridge_mqtt_dq_config:update(BadRemoteConfig)
    ),
    ok.

-doc "Bridge counters and API payload reflect forwarded messages.".
t_metrics_and_api(Config) ->
    Port = ?config(mqtt_port, Config),
    Bridge = make_bridge_config(<<"metrics">>, Port, #{
        filter_topic => <<"metrics/#">>,
        remote_topic => <<"sink/${topic}">>
    }),
    setup_config([Bridge]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    wait_bridge_connected(<<"metrics">>),

    {ok, Sub} = emqtt:start_link(#{clientid => <<"test_metrics_sub">>, port => Port}),
    {ok, _} = emqtt:connect(Sub),
    {ok, _, [1]} = emqtt:subscribe(Sub, <<"sink/metrics/#">>, 1),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_metrics_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"metrics/ok">>, <<"one">>, 0),
    ok = emqtt:publish(Pub, <<"other/skip">>, <<"two">>, 0),
    ?assertMatch([#{topic := <<"sink/metrics/ok">>}], receive_messages(1, 3000)),

    Snapshot = wait_until_snapshot(
        fun(S) ->
            case bridge_metrics(S, <<"metrics">>) of
                #{enqueue := 1, dequeue := 1, publish := 1, drop := 0} -> true;
                _ -> false
            end
        end,
        5000
    ),
    ?assertEqual(1, maps:get(enqueue, bridge_metrics(Snapshot, <<"metrics">>))),
    ?assertEqual(1, maps:get(dequeue, bridge_metrics(Snapshot, <<"metrics">>))),
    ?assertEqual(1, maps:get(publish, bridge_metrics(Snapshot, <<"metrics">>))),
    ?assertEqual(0, maps:get(drop, bridge_metrics(Snapshot, <<"metrics">>))),
    ?assertEqual(#{}, maps:get(retried_by_reason, bridge_metrics(Snapshot, <<"metrics">>), #{})),
    assert_bridge_balance(bridge_metrics(Snapshot, <<"metrics">>)),

    {ok, 200, StatsHeaders, Body} = emqx_bridge_mqtt_dq_app:on_handle_api_call(
        get, [<<"stats">>], #{}, #{}
    ),
    ?assertEqual(
        <<"application/json; charset=utf-8">>,
        maps:get(<<"content-type">>, StatsHeaders)
    ),
    ?assertMatch(
        #{
            cluster := #{complete := true},
            summary := #{bridge_count := 1, enqueue := 1, dequeue := 1, publish := 1, drop := 0},
            bridges := [_]
        },
        Body
    ),
    ?assertMatch(
        #{
            name := <<"metrics">>,
            config_state := enabled,
            runtime_state := running,
            status := ok,
            enqueue := 1,
            dequeue := 1,
            publish := 1,
            drop := 0,
            retried_by_reason := #{}
        },
        find_bridge_metric(Body, <<"metrics">>)
    ),
    {ok, 200, _, BridgeBody} = emqx_bridge_mqtt_dq_app:on_handle_api_call(
        get, [<<"stats">>, <<"metrics">>], #{}, #{}
    ),
    ?assertMatch(
        #{
            cluster := #{complete := true},
            bridge := #{
                name := <<"metrics">>,
                status := ok,
                enqueue := 1,
                dequeue := 1,
                publish := 1,
                drop := 0
            }
        },
        BridgeBody
    ),
    {ok, 200, _, StatusBody} = emqx_bridge_mqtt_dq_app:on_handle_api_call(
        get, [<<"status">>], #{}, #{}
    ),
    ?assertMatch(
        #{plugin := <<"emqx_bridge_mqtt_dq">>, status := ok, bridge_count := 1},
        StatusBody
    ),
    {ok, 200, Headers, MetricsBody} = emqx_bridge_mqtt_dq_app:on_handle_api_call(
        get, [<<"metrics">>], #{}, #{}
    ),
    ?assertEqual(
        <<"text/plain; version=0.0.4; charset=utf-8">>,
        maps:get(<<"content-type">>, Headers)
    ),
    ?assertNotEqual(
        nomatch, binary:match(MetricsBody, <<"emqx_bridge_mqtt_dq_bridge_enqueue_total">>)
    ),
    ?assertNotEqual(nomatch, binary:match(MetricsBody, <<"emqx_bridge_mqtt_dq_bridge_status">>)),
    ?assertNotEqual(nomatch, binary:match(MetricsBody, <<"bridge=\"metrics\"">>)),
    {ok, 200, UiHeaders, UiBody} = emqx_bridge_mqtt_dq_app:on_handle_api_call(
        get, [<<"ui">>], #{}, #{}
    ),
    ?assertEqual(<<"text/html; charset=utf-8">>, maps:get(<<"content-type">>, UiHeaders)),
    ?assertNotEqual(
        nomatch, binary:match(UiBody, <<"MQTT Egress Bridge With Durable Queue">>)
    ),

    ok = emqtt:disconnect(Sub),
    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

-doc "Missing buffer workers are not counted in queue lifecycle counters.".
t_dropped_when_buffer_not_ready(Config) ->
    Port = ?config(mqtt_port, Config),
    Bridge = make_bridge_config(<<"drop_no_buffer">>, Port, #{
        filter_topic => <<"drop/no_buffer/#">>,
        remote_topic => <<"sink/${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq:hook(),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_drop_no_buffer_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"drop/no_buffer/a">>, <<"1">>, 0),

    Snapshot = wait_until_snapshot(
        fun(S) ->
            case bridge_metrics(S, <<"drop_no_buffer">>) of
                #{enqueue := 0, dequeue := 0, publish := 0, drop := 0, retried_by_reason := #{}} ->
                    true;
                _ ->
                    false
            end
        end,
        5000
    ),
    ?assertEqual(0, maps:get(drop, bridge_metrics(Snapshot, <<"drop_no_buffer">>))),
    ?assertEqual(#{}, maps:get(retried_by_reason, bridge_metrics(Snapshot, <<"drop_no_buffer">>))),
    assert_bridge_balance(bridge_metrics(Snapshot, <<"drop_no_buffer">>)),

    ok = emqtt:disconnect(Pub),
    ok.

-doc "Buffered gauge is restored immediately when replayq reopens an existing disk queue.".
t_buffered_reported_immediately_after_open(Config) ->
    Port = ?config(mqtt_port, Config),
    QueueDir =
        "/tmp/emqx_bridge_mqtt_dq_test_buffered_open_" ++ integer_to_list(erlang:system_time()),
    Bridge = make_bridge_config(<<"buffered_open">>, Port, #{
        filter_topic => <<"buffered/open/#">>,
        remote_topic => <<"${topic}">>,
        queue_base_dir => list_to_binary(QueueDir),
        server => "127.0.0.1:19999"
    }),
    setup_config([Bridge]),

    {ok, BadSup1} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge),
    true = unlink(BadSup1),
    ok = emqx_bridge_mqtt_dq:hook(),
    {ok, _} = ?block_until(#{?snk_kind := mqtt_dq_connector_connect_failed}, 10000),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_buffered_open_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"buffered/open/a">>, <<"1">>, 0),
    ok = emqtt:publish(Pub, <<"buffered/open/a">>, <<"2">>, 0),
    ok = emqtt:publish(Pub, <<"buffered/open/a">>, <<"3">>, 0),

    _ = wait_until_snapshot(
        fun(S) -> buffer_value(S, <<"buffered_open">>) >= 3 end,
        5000
    ),

    stop_bridge_sup(BadSup1),
    ok = emqx_bridge_mqtt_dq_metrics:reset(),

    {ok, BadSup2} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge),
    true = unlink(BadSup2),
    Snapshot = wait_until_snapshot(
        fun(S) ->
            buffer_value(S, <<"buffered_open">>) >= 3 andalso
                buffer_bytes_value(S, <<"buffered_open">>) > 0
        end,
        5000
    ),
    ?assert(buffer_value(Snapshot, <<"buffered_open">>) >= 3),
    ?assert(buffer_bytes_value(Snapshot, <<"buffered_open">>) > 0),

    ok = emqtt:disconnect(Pub),
    exit(BadSup2, shutdown),
    ok.

-doc "Metrics reset refreshes buffered from live replayq state immediately.".
t_metrics_reset_refreshes_buffered(Config) ->
    Port = ?config(mqtt_port, Config),
    QueueDir =
        "/tmp/emqx_bridge_mqtt_dq_test_buffered_reset_" ++ integer_to_list(erlang:system_time()),
    Bridge = make_bridge_config(<<"buffered_reset">>, Port, #{
        filter_topic => <<"buffered/reset/#">>,
        remote_topic => <<"${topic}">>,
        queue_base_dir => list_to_binary(QueueDir),
        server => "127.0.0.1:19999"
    }),
    setup_config([Bridge]),

    {ok, BridgeSup} = emqx_bridge_mqtt_dq_bridge_sup:start_link(Bridge),
    true = unlink(BridgeSup),
    ok = emqx_bridge_mqtt_dq:hook(),
    {ok, _} = ?block_until(#{?snk_kind := mqtt_dq_connector_connect_failed}, 10000),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_buffered_reset_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"buffered/reset/a">>, <<"1">>, 0),
    ok = emqtt:publish(Pub, <<"buffered/reset/a">>, <<"2">>, 0),

    _ = wait_until_snapshot(
        fun(S) -> buffer_value(S, <<"buffered_reset">>) >= 2 end,
        5000
    ),

    ok = emqx_bridge_mqtt_dq_metrics:reset(),
    Snapshot = wait_until_snapshot(
        fun(S) ->
            buffer_value(S, <<"buffered_reset">>) >= 2 andalso
                buffer_bytes_value(S, <<"buffered_reset">>) > 0
        end,
        5000
    ),
    ?assert(buffer_value(Snapshot, <<"buffered_reset">>) >= 2),
    ?assert(buffer_bytes_value(Snapshot, <<"buffered_reset">>) > 0),

    ok = emqtt:disconnect(Pub),
    exit(BridgeSup, shutdown),
    ok.

-doc "Plugin health check fails when any local buffer exceeds 50% of its configured capacity.".
t_health_check_buffer_threshold(Config) ->
    Port = ?config(mqtt_port, Config),
    Bridge = make_bridge_config(<<"health">>, Port, #{
        pool_size => 1,
        buffer_pool_size => 1,
        max_total_bytes => 1000
    }),
    setup_config([Bridge]),
    {ok, BufferPid} = emqx_bridge_mqtt_dq_buffer:start_link(Bridge, 0),
    true = unlink(BufferPid),
    ItemLow = #{
        topic => <<"health/a">>,
        payload => <<0:800>>,
        qos => 0,
        retain => false,
        timestamp => erlang:system_time(millisecond),
        properties => #{}
    },
    ok = emqx_bridge_mqtt_dq_buffer:enqueue(BufferPid, ItemLow, no_ack),
    UsageLow = wait_until_buffer_usage(BufferPid, 1, 5000),
    ?assert(maps:get(buffered_bytes, UsageLow) < 500),
    ?assertEqual(ok, emqx_bridge_mqtt_dq_app:on_health_check(#{})),

    ItemHigh = ItemLow#{topic => <<"health/b">>, payload => <<0:4800>>},
    ok = emqx_bridge_mqtt_dq_buffer:enqueue(BufferPid, ItemHigh, no_ack),
    _ = wait_until_buffer_usage(BufferPid, 501, 5000),
    ?assertMatch(
        {error, <<"buffer usage exceeds 50% capacity: health[0]=", _/binary>>},
        emqx_bridge_mqtt_dq_app:on_health_check(#{})
    ),
    exit(BufferPid, shutdown),
    ok.

%%--------------------------------------------------------------------
%% sync_bridges test cases
%%--------------------------------------------------------------------

-doc "sync_bridges with unchanged config keeps the same bridge pid.".
t_sync_bridges_noop_on_same_config(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    Bridge = make_bridge_config(<<"noop">>, Port, #{
        filter_topic => <<"noop/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid1)),

    %% Sync again with identical config — pid should be the same
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),
    ?assertEqual(Pid1, Pid2),
    ok.

-doc "sync_bridges restarts bridge when pool_size changes.".
t_sync_bridges_restart_on_config_change(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    Bridge = make_bridge_config(<<"chg">>, Port, #{
        filter_topic => <<"chg/#">>,
        remote_topic => <<"${topic}">>,
        pool_size => 2
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid1)),

    %% Change pool_size from 2 to 3
    Bridge2 = Bridge#{pool_size := 3},
    setup_config([Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),
    ?assert(is_pid(Pid2)),
    ?assertNotEqual(Pid1, Pid2),
    ok.

-doc "sync_bridges removes bridge when it disappears from config.".
t_sync_bridges_remove_bridge(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    Bridge = make_bridge_config(<<"rm">>, Port, #{
        filter_topic => <<"rm/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], bridge_children(emqx_bridge_mqtt_dq_sup)),

    %% Remove the bridge from config
    setup_config([]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertEqual([], bridge_children(emqx_bridge_mqtt_dq_sup)),
    ok.

-doc "sync_bridges starts a newly added bridge.".
t_sync_bridges_add_bridge(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    Bridge1 = make_bridge_config(<<"add1">>, Port, #{
        filter_topic => <<"add1/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge1]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], bridge_children(emqx_bridge_mqtt_dq_sup)),

    %% Add a second bridge
    Bridge2 = make_bridge_config(<<"add2">>, Port, #{
        filter_topic => <<"add2/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([Bridge1, Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    Children = bridge_children(emqx_bridge_mqtt_dq_sup),
    ?assertEqual(2, length(Children)),
    ok.

-doc "sync_bridges handles add, update, and keep in one call.".
t_sync_bridges_mixed(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    BridgeA = make_bridge_config(<<"mx_a">>, Port, #{
        filter_topic => <<"a/#">>,
        remote_topic => <<"${topic}">>
    }),
    BridgeB = make_bridge_config(<<"mx_b">>, Port, #{
        filter_topic => <<"b/#">>,
        remote_topic => <<"${topic}">>,
        pool_size => 2
    }),
    setup_config([BridgeA, BridgeB]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ChildMap1 = bridge_child_pid_map(emqx_bridge_mqtt_dq_sup),
    PidA1 = maps:get({bridge, <<"mx_a">>}, ChildMap1),
    PidB1 = maps:get({bridge, <<"mx_b">>}, ChildMap1),

    %% A unchanged, B changed (pool_size 2→3), C is new
    BridgeB2 = BridgeB#{pool_size := 3},
    BridgeC = make_bridge_config(<<"mx_c">>, Port, #{
        filter_topic => <<"c/#">>,
        remote_topic => <<"${topic}">>
    }),
    setup_config([BridgeA, BridgeB2, BridgeC]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ChildMap2 = bridge_child_pid_map(emqx_bridge_mqtt_dq_sup),

    %% A: same pid
    ?assertEqual(PidA1, maps:get({bridge, <<"mx_a">>}, ChildMap2)),
    %% B: restarted (different pid)
    PidB2 = maps:get({bridge, <<"mx_b">>}, ChildMap2),
    ?assertNotEqual(PidB1, PidB2),
    %% C: new
    ?assert(is_pid(maps:get({bridge, <<"mx_c">>}, ChildMap2))),
    ?assertEqual(3, map_size(ChildMap2)),
    ok.

-doc "sync_bridges stops a bridge when enable flips to false.".
t_sync_bridges_disable_bridge(Config) ->
    Port = ?config(mqtt_port, Config),
    QueueDir =
        "/tmp/emqx_bridge_mqtt_dq_test_disable_purge_" ++ integer_to_list(erlang:system_time()),
    ensure_sup(),
    Bridge = make_bridge_config(<<"dis">>, Port, #{
        filter_topic => <<"dis/#">>,
        remote_topic => <<"${topic}">>,
        queue_base_dir => list_to_binary(QueueDir),
        server => "127.0.0.1:19999"
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertMatch([_], bridge_children(emqx_bridge_mqtt_dq_sup)),
    ok = emqx_bridge_mqtt_dq:hook(),
    {ok, _} = ?block_until(#{?snk_kind := mqtt_dq_connector_connect_failed}, 10000),

    {ok, Pub} = emqtt:start_link(#{clientid => <<"test_disable_bridge_pub">>, port => Port}),
    {ok, _} = emqtt:connect(Pub),
    ok = emqtt:publish(Pub, <<"dis/a">>, <<"1">>, 0),

    _ = wait_until_snapshot(
        fun(S) -> buffer_value(S, <<"dis">>) >= 1 end,
        5000
    ),
    ?assert(filelib:is_dir(QueueDir)),

    %% Disable the bridge
    DisabledBridge = Bridge#{enable := false},
    setup_config([DisabledBridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    ?assertEqual([], bridge_children(emqx_bridge_mqtt_dq_sup)),
    ?assertNot(filelib:is_dir(filename:join(QueueDir, "dis"))),
    Snapshot = emqx_bridge_mqtt_dq_metrics:snapshot(),
    ?assertEqual(false, maps:is_key(<<"dis">>, maps:get(bridges, Snapshot, #{}))),
    ?assertEqual(
        false,
        lists:any(
            fun({{BridgeName, _Index}, _Metrics}) -> BridgeName =:= <<"dis">> end,
            maps:to_list(maps:get(buffers, Snapshot, #{}))
        )
    ),
    ?assertEqual(
        false,
        lists:any(
            fun({{BridgeName, _Index}, _Metrics}) -> BridgeName =:= <<"dis">> end,
            maps:to_list(maps:get(connectors, Snapshot, #{}))
        )
    ),
    ok = emqtt:disconnect(Pub),
    ok.

-doc "sync_bridges restarts bridge when buffer_pool_size changes.".
t_sync_bridges_buffer_pool_size_change(Config) ->
    Port = ?config(mqtt_port, Config),
    ensure_sup(),
    Bridge = make_bridge_config(<<"bps">>, Port, #{
        filter_topic => <<"bps/#">>,
        remote_topic => <<"${topic}">>,
        buffer_pool_size => 4
    }),
    setup_config([Bridge]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid1, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),

    %% Change buffer_pool_size from 4 to 8
    Bridge2 = Bridge#{buffer_pool_size := 8},
    setup_config([Bridge2]),
    ok = emqx_bridge_mqtt_dq_app:sync_bridges(),
    [{_, Pid2, _, _}] = bridge_children(emqx_bridge_mqtt_dq_sup),
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
            case
                lists:any(
                    fun is_metrics_child/1, supervisor:which_children(emqx_bridge_mqtt_dq_sup)
                )
            of
                true ->
                    ok;
                false ->
                    {ok, _} = supervisor:start_child(
                        emqx_bridge_mqtt_dq_sup,
                        #{
                            id => emqx_bridge_mqtt_dq_metrics,
                            start => {emqx_bridge_mqtt_dq_metrics, start_link, []},
                            restart => permanent,
                            shutdown => 5000,
                            type => worker,
                            modules => [emqx_bridge_mqtt_dq_metrics]
                        }
                    ),
                    ok
            end
    end.

is_metrics_child({emqx_bridge_mqtt_dq_metrics, Pid, _, _}) when is_pid(Pid) ->
    true;
is_metrics_child(_) ->
    false.

bridge_children(Sup) ->
    [
        Child
     || {Id, _Pid, _Type, _Mods} = Child <- supervisor:which_children(Sup), is_bridge_child_id(Id)
    ].

bridge_child_pid_map(Sup) ->
    maps:from_list([{Id, Pid} || {Id, Pid, _, _} <- bridge_children(Sup)]).

is_bridge_child_id({bridge, Name}) when is_binary(Name) ->
    true;
is_bridge_child_id(_) ->
    false.

make_bridge_config(Name, Port, Overrides) ->
    QueueDir = iolist_to_binary([<<"/tmp/emqx_bridge_mqtt_dq_test/">>, Name]),
    Default = #{
        name => Name,
        enable => true,
        server => "127.0.0.1:" ++ integer_to_list(Port),
        proto_ver => v4,
        clientid_prefix => <<"test_dq_", Name/binary>>,
        username => <<>>,
        password => <<>>,
        keepalive_s => 60,
        ssl => #{enable => false},
        pool_size => 2,
        buffer_pool_size => 4,
        max_inflight => 32,
        filter_topic => <<"#">>,
        remote_topic => <<"${topic}">>,
        enqueue_timeout_ms => 5000,
        remote_qos => '${qos}',
        remote_retain => '${retain}',
        queue_base_dir => QueueDir,
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

wait_until_snapshot(CheckFun, TimeoutMs) ->
    Started = erlang:monotonic_time(millisecond),
    wait_until_snapshot(CheckFun, TimeoutMs, Started).

wait_until_snapshot(CheckFun, TimeoutMs, Started) ->
    Snapshot = emqx_bridge_mqtt_dq_metrics:snapshot(),
    case CheckFun(Snapshot) of
        true ->
            Snapshot;
        _ ->
            case erlang:monotonic_time(millisecond) - Started >= TimeoutMs of
                true ->
                    ct:fail({snapshot_timeout, Snapshot});
                false ->
                    receive
                    after 100 -> ok
                    end,
                    wait_until_snapshot(CheckFun, TimeoutMs, Started)
            end
    end.

wait_bridge_connected(BridgeName) ->
    {ok, _} =
        ?block_until(#{?snk_kind := mqtt_dq_connector_connected, bridge := BridgeName}, 10000).

stop_bridge_sup(Pid) ->
    MRef = monitor(process, Pid),
    exit(Pid, shutdown),
    receive
        {'DOWN', MRef, process, Pid, _} -> ok
    after 5000 ->
        ct:fail("bridge supervisor did not stop")
    end.

bridge_metrics(Snapshot, BridgeName) ->
    Bridges = maps:get(bridges, Snapshot, #{}),
    maps:get(BridgeName, Bridges, #{
        enqueue => 0, dequeue => 0, publish => 0, drop => 0, retried_by_reason => #{}
    }).

assert_bridge_balance(#{enqueue := Enqueue, dequeue := Dequeue, publish := Publish, drop := Drop}) ->
    ?assertEqual(Enqueue, Dequeue),
    ?assertEqual(Dequeue, Publish + Drop).

find_by_topic(Messages, Topic) ->
    case [M || #{topic := T} = M <- Messages, T =:= Topic] of
        [Msg | _] -> Msg;
        [] -> ct:fail({topic_not_found, Topic, Messages})
    end.

find_bridge_metric(Body, BridgeName) ->
    Bridges = maps:get(bridges, Body, []),
    case lists:filter(fun(#{name := Name}) -> Name =:= BridgeName end, Bridges) of
        [BridgeMetric | _] -> BridgeMetric;
        [] -> ct:fail({bridge_metric_not_found, BridgeName, Body})
    end.

buffer_value(Snapshot, BridgeName) ->
    Buffers = maps:get(buffers, Snapshot, #{}),
    maps:fold(
        fun
            ({Name, _Index}, #{buffered := Value}, Acc) when Name =:= BridgeName ->
                Acc + Value;
            (_, _, Acc) ->
                Acc
        end,
        0,
        Buffers
    ).

buffer_bytes_value(Snapshot, BridgeName) ->
    Buffers = maps:get(buffers, Snapshot, #{}),
    maps:fold(
        fun
            ({Name, _Index}, #{buffered_bytes := Value}, Acc) when Name =:= BridgeName ->
                Acc + Value;
            (_, _, Acc) ->
                Acc
        end,
        0,
        Buffers
    ).

wait_until_buffer_usage(Pid, MinBytes, TimeoutMs) ->
    Started = erlang:monotonic_time(millisecond),
    wait_until_buffer_usage(Pid, MinBytes, TimeoutMs, Started).

wait_until_buffer_usage(Pid, MinBytes, TimeoutMs, Started) ->
    Usage = emqx_bridge_mqtt_dq_buffer:usage(Pid),
    case maps:get(buffered_bytes, Usage, 0) >= MinBytes of
        true ->
            Usage;
        false ->
            case erlang:monotonic_time(millisecond) - Started >= TimeoutMs of
                true ->
                    ct:fail({buffer_usage_timeout, Usage});
                false ->
                    receive
                    after 50 -> ok
                    end,
                    wait_until_buffer_usage(Pid, MinBytes, TimeoutMs, Started)
            end
    end.
