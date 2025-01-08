%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%% http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_bridge_mqtt_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_dashboard_api_test_helpers, [request/4, uri/1]).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_hooks.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

%% output functions
-export([inspect/3]).

-define(TYPE_MQTT, <<"mqtt">>).
-define(BRIDGE_NAME_INGRESS, <<"ingress_mqtt_bridge">>).
-define(BRIDGE_NAME_EGRESS, <<"egress_mqtt_bridge">>).

%% Having ingress/egress prefixs of topic names to avoid dead loop while bridging
-define(INGRESS_REMOTE_TOPIC, "ingress_remote_topic").
-define(INGRESS_LOCAL_TOPIC, "ingress_local_topic").
-define(EGRESS_REMOTE_TOPIC, "egress_remote_topic").
-define(EGRESS_LOCAL_TOPIC, "egress_local_topic").

-define(SERVER_CONF, #{
    <<"type">> => ?TYPE_MQTT,
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).

-define(SERVER_CONF(Username, Password), (?SERVER_CONF)#{
    <<"username">> => Username,
    <<"password">> => Password
}).

-define(INGRESS_CONF, #{
    <<"remote">> => #{
        <<"topic">> => <<?INGRESS_REMOTE_TOPIC, "/#">>,
        <<"qos">> => 1
    },
    <<"local">> => #{
        <<"topic">> => <<?INGRESS_LOCAL_TOPIC, "/${topic}">>,
        <<"qos">> => <<"${qos}">>,
        <<"payload">> => <<"${payload}">>,
        <<"retain">> => <<"${retain}">>
    }
}).

-define(EGRESS_CONF, #{
    <<"local">> => #{
        <<"topic">> => <<?EGRESS_LOCAL_TOPIC, "/#">>
    },
    <<"remote">> => #{
        <<"topic">> => <<?EGRESS_REMOTE_TOPIC, "/${topic}">>,
        <<"payload">> => <<"${payload}">>,
        <<"qos">> => <<"${qos}">>,
        <<"retain">> => <<"${retain}">>
    }
}).

-define(INGRESS_CONF_NO_PAYLOAD_TEMPLATE, #{
    <<"remote">> => #{
        <<"topic">> => <<?INGRESS_REMOTE_TOPIC, "/#">>,
        <<"qos">> => 1
    },
    <<"local">> => #{
        <<"topic">> => <<?INGRESS_LOCAL_TOPIC, "/${topic}">>,
        <<"qos">> => <<"${qos}">>,
        <<"retain">> => <<"${retain}">>
    }
}).

-define(EGRESS_CONF_NO_PAYLOAD_TEMPLATE, #{
    <<"local">> => #{
        <<"topic">> => <<?EGRESS_LOCAL_TOPIC, "/#">>
    },
    <<"remote">> => #{
        <<"topic">> => <<?EGRESS_REMOTE_TOPIC, "/${topic}">>,
        <<"qos">> => <<"${qos}">>,
        <<"retain">> => <<"${retain}">>
    }
}).

-define(assertMetrics(Pat, BridgeID),
    ?assertMetrics(Pat, true, BridgeID)
).
-define(assertMetrics(Pat, Guard, BridgeID),
    ?retry(
        _Sleep = 300,
        _Attempts0 = 20,
        ?assertMatch(
            #{
                <<"metrics">> := Pat,
                <<"node_metrics">> := [
                    #{
                        <<"node">> := _,
                        <<"metrics">> := Pat
                    }
                ]
            } when Guard,
            request_bridge_metrics(BridgeID)
        )
    )
).

inspect(Selected, _Envs, _Args) ->
    persistent_term:put(?MODULE, #{inspect => Selected}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
    [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_bridge,
            emqx_rule_engine,
            emqx_bridge_mqtt,
            {emqx_dashboard,
                "dashboard {"
                "\n listeners.http { bind = 18083 }"
                "\n default_username = connector_admin"
                "\n default_password = public"
                "\n }"}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_, Config) ->
    ok = snabbkaffe:start_trace(),
    Config.

end_per_testcase(_, _Config) ->
    ok = unhook_authenticate(),
    clear_resources(),
    snabbkaffe:stop(),
    ok.

clear_resources() ->
    lists:foreach(
        fun(#{id := Id}) ->
            ok = emqx_rule_engine:delete_rule(Id)
        end,
        emqx_rule_engine:get_rules()
    ),
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            ok = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_conf_bridge_authn_anonymous(_) ->
    ok = hook_authenticate(),
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF#{
            <<"name">> => <<"t_conf_bridge_anonymous">>,
            <<"ingress">> => ?INGRESS_CONF#{<<"pool_size">> => 1}
        }
    ),
    ?assertReceive(
        {authenticate, #{username := undefined, password := undefined}}
    ).

t_conf_bridge_authn_password(_) ->
    Username1 = <<"user1">>,
    Password1 = <<"from-here">>,
    ok = hook_authenticate(),
    {ok, 201, _Bridge1} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(Username1, Password1)#{
            <<"name">> => <<"t_conf_bridge_authn_password">>,
            <<"ingress">> => ?INGRESS_CONF#{<<"pool_size">> => 1}
        }
    ),
    ?assertReceive(
        {authenticate, #{username := Username1, password := Password1}}
    ).

t_conf_bridge_authn_passfile(Config) ->
    %% test_server_ctrl:run_test_cases_loop
    DataDir = ?config(data_dir, Config),
    Username2 = <<"user2">>,
    PasswordFilename = filename:join(DataDir, "password"),
    Password2 = <<"from-there">>,
    ok = hook_authenticate(),
    {ok, 201, _Bridge2} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(Username2, iolist_to_binary(["file://", PasswordFilename]))#{
            <<"name">> => <<"t_conf_bridge_authn_passfile">>,
            <<"ingress">> => ?INGRESS_CONF#{<<"pool_size">> => 1}
        }
    ),
    ?assertReceive(
        {authenticate, #{username := Username2, password := Password2}}
    ),
    {ok, 201, #{
        <<"status">> := <<"disconnected">>,
        <<"status_reason">> := Reason
    }} =
        request_json(
            post,
            uri(["bridges"]),
            ?SERVER_CONF(<<>>, <<"file://im/pretty/sure/theres/no/such/file">>)#{
                <<"name">> => <<"t_conf_bridge_authn_no_passfile">>,
                <<"ingress">> => ?INGRESS_CONF#{<<"pool_size">> => 1}
            }
        ),
    ?assertMatch({match, _}, re:run(Reason, <<"failed_to_read_secret_file">>)).

hook_authenticate() ->
    emqx_hooks:add('client.authenticate', {?MODULE, authenticate, [self()]}, ?HP_HIGHEST).

unhook_authenticate() ->
    emqx_hooks:del('client.authenticate', {?MODULE, authenticate}).

authenticate(Credential, _, TestRunnerPid) ->
    _ = TestRunnerPid ! {authenticate, Credential},
    ignore.

%%------------------------------------------------------------------------------

t_mqtt_conn_bridge_ingress(_) ->
    %% create an MQTT bridge, using POST
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ServerConf = ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),
    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_INGRESS
    } = emqx_utils_json:decode(Bridge),

    BridgeIDIngress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_INGRESS),

    %% try to create the bridge again
    ?assertMatch(
        {ok, 400, _},
        request(post, uri(["bridges"]), ServerConf)
    ),

    %% try to reconfigure the bridge
    ?assertMatch(
        {ok, 200, _},
        request(put, uri(["bridges", BridgeIDIngress]), ServerConf)
    ),

    %% non-shared subscription, verify that only one client is subscribed
    ?assertEqual(
        1,
        length(emqx:subscribers(<<?INGRESS_REMOTE_TOPIC, "/#">>))
    ),

    %% we now test if the bridge works as expected
    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(LocalTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'remote' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(RemoteTopic, Payload)),
    %% we should receive a message on the local broker, with specified topic
    assert_mqtt_msg_received(LocalTopic, Payload),

    %% verify the metrics of the bridge
    ?assertMetrics(
        #{<<"matched">> := 0, <<"received">> := 1},
        BridgeIDIngress
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    ok.

t_mqtt_conn_bridge_ingress_full_context(_Config) ->
    IngressConf =
        emqx_utils_maps:deep_merge(
            ?INGRESS_CONF,
            #{<<"local">> => #{<<"payload">> => <<"${.}">>}}
        ),
    {ok, 201, _Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => IngressConf
        }
    ),

    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(LocalTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'remote' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(RemoteTopic, Payload)),
    %% we should receive a message on the local broker, with specified topic
    #message{payload = EncodedPayload} = assert_mqtt_msg_received(LocalTopic),
    ?assertMatch(
        #{
            <<"dup">> := false,
            <<"id">> := _,
            <<"message_received_at">> := _,
            <<"payload">> := <<"hello">>,
            <<"pub_props">> := #{},
            <<"qos">> := 0,
            <<"retain">> := false,
            <<"server">> := <<"127.0.0.1:1883">>,
            <<"topic">> := <<"ingress_remote_topic/1">>
        },
        emqx_utils_json:decode(EncodedPayload, [return_maps])
    ),

    ok.

t_mqtt_conn_bridge_ingress_shared_subscription(_) ->
    PoolSize = 4,
    Ns = lists:seq(1, 10),
    BridgeName = atom_to_binary(?FUNCTION_NAME),
    BridgeID = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => BridgeName,
            <<"ingress">> => #{
                <<"pool_size">> => PoolSize,
                <<"remote">> => #{
                    <<"topic">> => <<"$share/ingress/", ?INGRESS_REMOTE_TOPIC, "/#">>,
                    <<"qos">> => 1
                },
                <<"local">> => #{
                    <<"topic">> => <<?INGRESS_LOCAL_TOPIC, "/${topic}">>,
                    <<"qos">> => <<"${qos}">>,
                    <<"payload">> => <<"${clientid}">>,
                    <<"retain">> => <<"${retain}">>
                }
            }
        }
    ),

    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    ok = emqx:subscribe(LocalTopic),

    _ = emqx_utils:pmap(
        fun emqx:publish/1,
        [emqx_message:make(RemoteTopic, <<>>) || _ <- Ns]
    ),
    _ = [assert_mqtt_msg_received(LocalTopic) || _ <- Ns],

    ?assertEqual(
        PoolSize,
        length(emqx_shared_sub:subscribers(<<"ingress">>, <<?INGRESS_REMOTE_TOPIC, "/#">>))
    ),

    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),
    ok.

t_mqtt_egress_bridge_warns_clean_start(_) ->
    BridgeName = atom_to_binary(?FUNCTION_NAME),
    Action = fun() ->
        BridgeID = create_bridge(
            ?SERVER_CONF#{
                <<"name">> => BridgeName,
                <<"egress">> => ?EGRESS_CONF,
                <<"clean_start">> => false
            }
        ),

        %% delete the bridge
        {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),

        ok
    end,
    {ok, {ok, _}} =
        ?wait_async_action(
            Action(),
            #{?snk_kind := mqtt_clean_start_egress_action_warning},
            10000
        ),
    ok.

t_mqtt_conn_bridge_ingress_downgrades_qos_2(_) ->
    BridgeName = atom_to_binary(?FUNCTION_NAME),
    BridgeID = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => BridgeName,
            <<"ingress">> => emqx_utils_maps:deep_merge(
                ?INGRESS_CONF,
                #{<<"remote">> => #{<<"qos">> => 2}}
            )
        }
    ),

    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    Payload = <<"whatqos">>,
    emqx:subscribe(LocalTopic),
    emqx:publish(emqx_message:make(undefined, _QoS = 2, RemoteTopic, Payload)),

    %% we should receive a message on the local broker, with specified topic
    Msg = assert_mqtt_msg_received(LocalTopic, Payload),
    ?assertMatch(#message{qos = 1}, Msg),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), []),

    ok.

t_mqtt_conn_bridge_ingress_no_payload_template(_) ->
    BridgeIDIngress = create_bridge(
        ?SERVER_CONF#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF_NO_PAYLOAD_TEMPLATE
        }
    ),

    %% we now test if the bridge works as expected
    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(LocalTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'remote' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(RemoteTopic, Payload)),
    %% we should receive a message on the local broker, with specified topic
    Msg = assert_mqtt_msg_received(LocalTopic),
    ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(Msg#message.payload)),

    %% verify the metrics of the bridge
    ?assertMetrics(
        #{<<"matched">> := 0, <<"received">> := 1},
        BridgeIDIngress
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    ok.

t_mqtt_conn_bridge_egress(_) ->
    %% then we add a mqtt connector, using POST
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),

    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),

    %% verify the metrics of the bridge
    ?retry(
        _Interval = 200,
        _Attempts = 5,
        ?assertMetrics(
            #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0},
            BridgeIDEgress
        )
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

t_mqtt_conn_bridge_egress_no_payload_template(_) ->
    %% then we add a mqtt connector, using POST
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF_NO_PAYLOAD_TEMPLATE
        }
    ),

    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    Msg = assert_mqtt_msg_received(RemoteTopic),
    ?assertMatch(#{<<"payload">> := Payload}, emqx_utils_json:decode(Msg#message.payload)),

    %% verify the metrics of the bridge
    ?retry(
        _Interval = 200,
        _Attempts = 5,
        ?assertMetrics(
            #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0},
            BridgeIDEgress
        )
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    ok.

t_egress_short_clientid(_Config) ->
    %% Name is short, expect the actual client ID in use is hashed from
    %% <name><nodename-hash>:<pool_worker_id>
    Name = <<"abc01234">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = iolist_to_binary([BaseId, $:, "1"]),
    ?assertMatch(<<"abc01234", _/binary>>, ExpectedClientId),
    test_egress_clientid(Name, ExpectedClientId).

t_egress_long_clientid(_Config) ->
    %% Expect the actual client ID in use is hashed from
    %% <name><nodename-hash>:<pool_worker_id>
    Name = <<"abc012345678901234567890">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = emqx_bridge_mqtt_lib:bytes23(BaseId, 1),
    test_egress_clientid(Name, ExpectedClientId).

t_egress_with_short_prefix(_Config) ->
    %% Expect the actual client ID in use is hashed from
    %% <prefix>head(sha1(<name><nodename-hash>:<pool_worker_id>), 16)
    Prefix = <<"012-">>,
    Name = <<"345">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = emqx_bridge_mqtt_lib:bytes23_with_prefix(Prefix, BaseId, 1),
    ?assertMatch(<<"012-", _/binary>>, ExpectedClientId),
    test_egress_clientid(Name, Prefix, ExpectedClientId).

t_egress_with_long_prefix(_Config) ->
    %% Expect the actual client ID in use is hashed from
    %% <prefix><name><nodename-hash>:<pool_worker_id>
    Prefix = <<"0123456789abcdef01234-">>,
    Name = <<"345">>,
    BaseId = emqx_bridge_mqtt_lib:clientid_base([Name]),
    ExpectedClientId = iolist_to_binary([Prefix, BaseId, <<":1">>]),
    test_egress_clientid(Name, Prefix, ExpectedClientId).

test_egress_clientid(Name, ExpectedClientId) ->
    test_egress_clientid(Name, <<>>, ExpectedClientId).

test_egress_clientid(Name, ClientIdPrefix, ExpectedClientId) ->
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => Name,
            <<"egress">> => (?EGRESS_CONF)#{<<"pool_size">> => 1},
            <<"clientid_prefix">> => ClientIdPrefix
        }
    ),
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    emqx:publish(emqx_message:make(LocalTopic, Payload)),

    Msg = assert_mqtt_msg_received(RemoteTopic, Payload),
    ?assertEqual(ExpectedClientId, Msg#message.from),

    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    ok.

t_mqtt_conn_bridge_ingress_and_egress(_) ->
    BridgeIDIngress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),

    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    #{
        <<"metrics">> := #{
            <<"matched">> := CntMatched1, <<"success">> := CntSuccess1, <<"failed">> := 0
        },
        <<"node_metrics">> :=
            [
                #{
                    <<"node">> := _,
                    <<"metrics">> :=
                        #{
                            <<"matched">> := NodeCntMatched1,
                            <<"success">> := NodeCntSuccess1,
                            <<"failed">> := 0
                        }
                }
            ]
    } = request_bridge_metrics(BridgeIDEgress),

    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic, Payload),

    %% verify the metrics of the bridge
    timer:sleep(1000),
    #{
        <<"metrics">> := #{
            <<"matched">> := CntMatched2, <<"success">> := CntSuccess2, <<"failed">> := 0
        },
        <<"node_metrics">> :=
            [
                #{
                    <<"node">> := _,
                    <<"metrics">> :=
                        #{
                            <<"matched">> := NodeCntMatched2,
                            <<"success">> := NodeCntSuccess2,
                            <<"failed">> := 0
                        }
                }
            ]
    } = request_bridge_metrics(BridgeIDEgress),
    ?assertEqual(CntMatched2, CntMatched1 + 1),
    ?assertEqual(CntSuccess2, CntSuccess1 + 1),
    ?assertEqual(NodeCntMatched2, NodeCntMatched1 + 1),
    ?assertEqual(NodeCntSuccess2, NodeCntSuccess1 + 1),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

t_ingress_mqtt_bridge_with_rules(_) ->
    BridgeIDIngress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),

    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"A_rule_get_messages_from_a_source_mqtt_bridge">>,
            <<"enable">> => true,
            <<"actions">> => [#{<<"function">> => <<"emqx_bridge_mqtt_SUITE:inspect">>}],
            <<"sql">> => <<"SELECT * from \"$bridges/", BridgeIDIngress/binary, "\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_utils_json:decode(Rule),

    %% we now test if the bridge works as expected

    RemoteTopic = <<?INGRESS_REMOTE_TOPIC, "/1">>,
    LocalTopic = <<?INGRESS_LOCAL_TOPIC, "/", RemoteTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(LocalTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'remote' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(RemoteTopic, Payload)),
    %% we should receive a message on the local broker, with specified topic
    assert_mqtt_msg_received(LocalTopic, Payload),
    %% and also the rule should be matched, with matched + 1:
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    {ok, 200, Metrics} = request(get, uri(["rules", RuleId, "metrics"]), []),
    ?assertMatch(#{<<"id">> := RuleId}, emqx_utils_json:decode(Rule1)),
    ?assertMatch(
        #{
            <<"metrics">> := #{
                <<"matched">> := 1,
                <<"passed">> := 1,
                <<"failed">> := 0,
                <<"failed.exception">> := 0,
                <<"failed.no_result">> := 0,
                <<"matched.rate">> := _,
                <<"matched.rate.max">> := _,
                <<"matched.rate.last5m">> := _,
                <<"actions.total">> := 1,
                <<"actions.success">> := 1,
                <<"actions.failed">> := 0,
                <<"actions.failed.out_of_service">> := 0,
                <<"actions.failed.unknown">> := 0
            }
        },
        emqx_utils_json:decode(Metrics)
    ),

    %% we also check if the actions of the rule is triggered
    ?assertMatch(
        #{
            inspect := #{
                event := <<"$bridges/mqtt", _/binary>>,
                id := MsgId,
                payload := Payload,
                topic := RemoteTopic,
                qos := 0,
                dup := false,
                retain := false,
                pub_props := #{},
                timestamp := _
            }
        } when is_binary(MsgId),
        persistent_term:get(?MODULE)
    ),

    %% verify the metrics of the bridge
    ?assertMetrics(
        #{<<"matched">> := 0, <<"received">> := 1},
        BridgeIDIngress
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []).

t_egress_mqtt_bridge_with_rules(_) ->
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),

    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"A_rule_send_messages_to_a_sink_mqtt_bridge">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeIDEgress],
            <<"sql">> => <<"SELECT * from \"t/1\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_utils_json:decode(Rule),

    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'local' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(LocalTopic, Payload)),
    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic, Payload),
    emqx:unsubscribe(RemoteTopic),

    %% PUBLISH a message to the rule.
    Payload2 = <<"hi">>,
    RuleTopic = <<"t/1">>,
    RemoteTopic2 = <<?EGRESS_REMOTE_TOPIC, "/", RuleTopic/binary>>,
    emqx:subscribe(RemoteTopic2),
    timer:sleep(100),
    emqx:publish(emqx_message:make(RuleTopic, Payload2)),
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    ?assertMatch(#{<<"id">> := RuleId, <<"name">> := _}, emqx_utils_json:decode(Rule1)),
    {ok, 200, Metrics} = request(get, uri(["rules", RuleId, "metrics"]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{
                <<"matched">> := 1,
                <<"passed">> := 1,
                <<"failed">> := 0,
                <<"failed.exception">> := 0,
                <<"failed.no_result">> := 0,
                <<"matched.rate">> := _,
                <<"matched.rate.max">> := _,
                <<"matched.rate.last5m">> := _,
                <<"actions.total">> := 1,
                <<"actions.success">> := 1,
                <<"actions.failed">> := 0,
                <<"actions.failed.out_of_service">> := 0,
                <<"actions.failed.unknown">> := 0
            }
        },
        emqx_utils_json:decode(Metrics)
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic2, Payload2),

    %% verify the metrics of the bridge
    ?assertMetrics(
        #{<<"matched">> := 2, <<"success">> := 2, <<"failed">> := 0},
        BridgeIDEgress
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []).

t_egress_mqtt_bridge_with_dummy_rule(_) ->
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),

    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"A_rule_send_empty_messages_to_a_sink_mqtt_bridge">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeIDEgress],
            %% select something useless from what a message cannot be composed
            <<"sql">> => <<"SELECT x from \"t/1\"">>
        }
    ),
    #{<<"id">> := RuleId} = emqx_utils_json:decode(Rule),

    %% PUBLISH a message to the rule.
    Payload = <<"hi">>,
    RuleTopic = <<"t/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    emqx:publish(emqx_message:make(RuleTopic, Payload)),
    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic, <<>>),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []).

t_mqtt_conn_bridge_egress_reconnect(_) ->
    %% then we add a mqtt connector, using POST
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF,
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"sync">>,
                %% using a long time so we can test recovery
                <<"request_ttl">> => <<"15s">>,
                %% to make it check the healthy and reconnect quickly
                <<"health_check_interval">> => <<"0.5s">>
            }
        }
    ),

    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload0 = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    ?wait_async_action(
        %% PUBLISH a message to the 'local' broker, as we have only one broker,
        %% the remote broker is also the local one.
        emqx:publish(emqx_message:make(LocalTopic, Payload0)),
        #{?snk_kind := buffer_worker_flush_ack}
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic, Payload0),

    %% verify the metrics of the bridge
    ?assertMetrics(
        #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0},
        BridgeIDEgress
    ),

    %% stop the listener 1883 to make the bridge disconnected
    ok = emqx_listeners:stop_listener('tcp:default'),
    ct:sleep(1500),

    %% PUBLISH 2 messages to the 'local' broker, the messages should
    %% be enqueued and the resource will block
    {ok, SRef} =
        snabbkaffe:subscribe(
            fun
                (#{?snk_kind := buffer_worker_retry_inflight_failed}) ->
                    true;
                (#{?snk_kind := buffer_worker_flush_nack}) ->
                    true;
                (_) ->
                    false
            end,
            _NEvents = 2,
            _Timeout = 1_000
        ),
    Payload1 = <<"hello2">>,
    Payload2 = <<"hello3">>,
    %% We need to do it in other processes because it'll block due to
    %% the long timeout
    spawn(fun() -> emqx:publish(emqx_message:make(LocalTopic, Payload1)) end),
    spawn(fun() -> emqx:publish(emqx_message:make(LocalTopic, Payload2)) end),
    {ok, _} = snabbkaffe:receive_events(SRef),

    %% verify the metrics of the bridge, the message should be queued
    ?assertMatch(
        #{<<"status">> := Status} when
            Status == <<"connecting">> orelse Status == <<"disconnected">>,
        request_bridge(BridgeIDEgress)
    ),
    %% matched >= 3 because of possible retries.
    ?assertMetrics(
        #{
            <<"matched">> := Matched,
            <<"success">> := 1,
            <<"failed">> := 0,
            <<"queuing">> := Queuing,
            <<"inflight">> := Inflight
        },
        Matched >= 3 andalso Inflight + Queuing == 2,
        BridgeIDEgress
    ),

    %% start the listener 1883 to make the bridge reconnected
    ok = emqx_listeners:start_listener('tcp:default'),
    timer:sleep(1500),
    %% verify the metrics of the bridge, the 2 queued messages should have been sent
    ?assertMatch(#{<<"status">> := <<"connected">>}, request_bridge(BridgeIDEgress)),
    %% matched >= 3 because of possible retries.
    ?assertMetrics(
        #{
            <<"matched">> := Matched,
            <<"success">> := 3,
            <<"failed">> := 0,
            <<"queuing">> := 0,
            <<"retried">> := _
        },
        Matched >= 3,
        BridgeIDEgress
    ),
    %% also verify the 2 messages have been sent to the remote broker
    assert_mqtt_msg_received(RemoteTopic, Payload1),
    assert_mqtt_msg_received(RemoteTopic, Payload2),
    ok.

t_mqtt_conn_bridge_egress_async_reconnect(_) ->
    BridgeIDEgress = create_bridge(
        ?SERVER_CONF#{
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF,
            <<"resource_opts">> => #{
                <<"query_mode">> => <<"async">>,
                %% using a long time so we can test recovery
                <<"request_ttl">> => <<"15s">>,
                %% to make it check the healthy and reconnect quickly
                <<"health_check_interval">> => <<"0.5s">>
            }
        }
    ),

    Self = self(),
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    emqx:subscribe(RemoteTopic),

    Publisher = start_publisher(LocalTopic, 200, Self),
    ct:sleep(1000),

    %% stop the listener 1883 to make the bridge disconnected
    ?check_trace(
        begin
            ok = emqx_listeners:stop_listener('tcp:default'),
            ct:sleep(1500),
            ?assertMatch(
                #{<<"status">> := Status} when
                    Status == <<"connecting">> orelse Status == <<"disconnected">>,
                request_bridge(BridgeIDEgress)
            ),

            %% start the listener 1883 to make the bridge reconnected
            ok = emqx_listeners:start_listener('tcp:default'),
            timer:sleep(1500),
            ?assertMatch(
                #{<<"status">> := <<"connected">>},
                request_bridge(BridgeIDEgress)
            ),

            N = stop_publisher(Publisher),

            %% all those messages should eventually be delivered
            [
                assert_mqtt_msg_received(RemoteTopic, Payload)
             || I <- lists:seq(1, N),
                Payload <- [integer_to_binary(I)]
            ],
            ok
        end,
        fun(Trace) ->
            ?assertMatch([_ | _], ?of_kind(emqx_bridge_mqtt_connector_econnrefused_error, Trace)),
            ok
        end
    ),
    ok.

start_publisher(Topic, Interval, CtrlPid) ->
    spawn_link(fun() -> publisher(Topic, 1, Interval, CtrlPid) end).

stop_publisher(Pid) ->
    _ = Pid ! {self(), stop},
    receive
        {Pid, N} -> N
    after 1_000 -> ct:fail("publisher ~p did not stop", [Pid])
    end.

publisher(Topic, N, Delay, CtrlPid) ->
    _ = emqx:publish(emqx_message:make(Topic, integer_to_binary(N))),
    receive
        {CtrlPid, stop} ->
            CtrlPid ! {self(), N}
    after Delay ->
        publisher(Topic, N + 1, Delay, CtrlPid)
    end.

%%

assert_mqtt_msg_received(Topic) ->
    assert_mqtt_msg_received(Topic, '_', 200).

assert_mqtt_msg_received(Topic, Payload) ->
    assert_mqtt_msg_received(Topic, Payload, 200).

assert_mqtt_msg_received(Topic, Payload, Timeout) ->
    receive
        {deliver, Topic, Msg = #message{}} when Payload == '_' ->
            ct:pal("received mqtt ~p on topic ~p", [Msg, Topic]),
            Msg;
        {deliver, Topic, Msg = #message{payload = Payload}} ->
            ct:pal("received mqtt ~p on topic ~p", [Msg, Topic]),
            Msg
    after Timeout ->
        {messages, Messages} = process_info(self(), messages),
        ct:fail("timeout waiting ~p ms for ~p on topic '~s', messages = ~0p", [
            Timeout,
            Payload,
            Topic,
            Messages
        ])
    end.

create_bridge(Config = #{<<"type">> := Type, <<"name">> := Name}) ->
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        Config
    ),
    ?assertMatch(
        #{
            <<"type">> := Type,
            <<"name">> := Name
        },
        emqx_utils_json:decode(Bridge),
        #{expected_type => Type, expected_name => Name}
    ),
    emqx_bridge_resource:bridge_id(Type, Name).

request_bridge(BridgeID) ->
    {ok, 200, Bridge} = request(get, uri(["bridges", BridgeID]), []),
    emqx_utils_json:decode(Bridge).

request_bridge_metrics(BridgeID) ->
    {ok, 200, BridgeMetrics} = request(get, uri(["bridges", BridgeID, "metrics"]), []),
    emqx_utils_json:decode(BridgeMetrics).

request_json(Method, Url, Body) ->
    {ok, Code, Response} = request(Method, Url, Body),
    {ok, Code, emqx_utils_json:decode(Response)}.

request(Method, Url, Body) ->
    request(<<"connector_admin">>, Method, Url, Body).
