%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_dashboard/include/emqx_dashboard.hrl").

%% output functions
-export([inspect/3]).

-define(BRIDGE_CONF_DEFAULT, <<"bridges: {}">>).
-define(TYPE_MQTT, <<"mqtt">>).
-define(NAME_MQTT, <<"my_mqtt_bridge">>).
-define(BRIDGE_NAME_INGRESS, <<"ingress_mqtt_bridge">>).
-define(BRIDGE_NAME_EGRESS, <<"egress_mqtt_bridge">>).

%% Having ingress/egress prefixs of topic names to avoid dead loop while bridging
-define(INGRESS_REMOTE_TOPIC, "ingress_remote_topic").
-define(INGRESS_LOCAL_TOPIC, "ingress_local_topic").
-define(EGRESS_REMOTE_TOPIC, "egress_remote_topic").
-define(EGRESS_LOCAL_TOPIC, "egress_local_topic").

-define(SERVER_CONF(Username), #{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => Username,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).

-define(INGRESS_CONF, #{
    <<"remote">> => #{
        <<"topic">> => <<?INGRESS_REMOTE_TOPIC, "/#">>,
        <<"qos">> => 2
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

inspect(Selected, _Envs, _Args) ->
    persistent_term:put(?MODULE, #{inspect => Selected}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
    [{timetrap, {seconds, 30}}].

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    %% some testcases (may from other app) already get emqx_connector started
    _ = application:stop(emqx_resource),
    _ = application:stop(emqx_connector),
    ok = emqx_common_test_helpers:start_apps(
        [
            emqx_rule_engine,
            emqx_bridge,
            emqx_dashboard
        ],
        fun set_special_configs/1
    ),
    ok = emqx_common_test_helpers:load_config(
        emqx_rule_engine_schema,
        <<"rule_engine {rules {}}">>
    ),
    ok = emqx_common_test_helpers:load_config(emqx_bridge_schema, ?BRIDGE_CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx_rule_engine,
        emqx_bridge,
        emqx_dashboard
    ]),
    ok.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(<<"connector_admin">>);
set_special_configs(_) ->
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.
end_per_testcase(_, _Config) ->
    clear_resources(),
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
            {ok, _} = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------
t_mqtt_conn_bridge_ingress(_) ->
    User1 = <<"user1">>,
    %% create an MQTT bridge, using POST
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(User1)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),
    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_INGRESS
    } = jsx:decode(Bridge),
    BridgeIDIngress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_INGRESS),

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
    ?assert(
        receive
            {deliver, LocalTopic, #message{payload = Payload}} ->
                ct:pal("local broker got message: ~p on topic ~p", [Payload, LocalTopic]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),

    %% verify the metrics of the bridge
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDIngress]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"matched">> := 0, <<"received">> := 1},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> :=
                            #{<<"matched">> := 0, <<"received">> := 1}
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    ok.

t_mqtt_conn_bridge_egress(_) ->
    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,

    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(User1)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),
    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_EGRESS
    } = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_EGRESS),
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
    ?assert(
        receive
            {deliver, RemoteTopic, #message{payload = Payload}} ->
                ct:pal("local broker got message: ~p on topic ~p", [Payload, RemoteTopic]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),

    %% verify the metrics of the bridge
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDEgress]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> :=
                            #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0}
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

t_mqtt_conn_bridge_ingress_and_egress(_) ->
    User1 = <<"user1">>,
    %% create an MQTT bridge, using POST
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(User1)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),

    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_INGRESS
    } = jsx:decode(Bridge),
    BridgeIDIngress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_INGRESS),
    {ok, 201, Bridge2} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(User1)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),
    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_EGRESS
    } = jsx:decode(Bridge2),

    BridgeIDEgress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_EGRESS),
    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),

    {ok, 200, BridgeStr1} = request(get, uri(["bridges", BridgeIDEgress]), []),
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
    } = jsx:decode(BridgeStr1),
    timer:sleep(100),
    %% PUBLISH a message to the 'local' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(LocalTopic, Payload)),

    %% we should receive a message on the "remote" broker, with specified topic
    ?assert(
        receive
            {deliver, RemoteTopic, #message{payload = Payload}} ->
                ct:pal("local broker got message: ~p on topic ~p", [Payload, RemoteTopic]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),

    %% verify the metrics of the bridge
    timer:sleep(1000),
    {ok, 200, BridgeStr2} = request(get, uri(["bridges", BridgeIDEgress]), []),
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
    } = jsx:decode(BridgeStr2),
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
    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(<<"user1">>)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_INGRESS,
            <<"ingress">> => ?INGRESS_CONF
        }
    ),
    BridgeIDIngress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_INGRESS),

    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"A_rule_get_messages_from_a_source_mqtt_bridge">>,
            <<"enable">> => true,
            <<"actions">> => [#{<<"function">> => "emqx_bridge_mqtt_SUITE:inspect"}],
            <<"sql">> => <<"SELECT * from \"$bridges/", BridgeIDIngress/binary, "\"">>
        }
    ),
    #{<<"id">> := RuleId} = jsx:decode(Rule),

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
    ?assert(
        receive
            {deliver, LocalTopic, #message{payload = Payload}} ->
                ct:pal("local broker got message: ~p on topic ~p", [Payload, LocalTopic]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),
    %% and also the rule should be matched, with matched + 1:
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    {ok, 200, Metrics} = request(get, uri(["rules", RuleId, "metrics"]), []),
    ?assertMatch(#{<<"id">> := RuleId}, jsx:decode(Rule1)),
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
        jsx:decode(Metrics)
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
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDIngress]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"matched">> := 0, <<"received">> := 1},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> :=
                            #{<<"matched">> := 0, <<"received">> := 1}
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []).

t_egress_mqtt_bridge_with_rules(_) ->
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(<<"user1">>)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF
        }
    ),
    #{<<"type">> := ?TYPE_MQTT, <<"name">> := ?BRIDGE_NAME_EGRESS} = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_EGRESS),

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
    #{<<"id">> := RuleId} = jsx:decode(Rule),

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
    ?assert(
        receive
            {deliver, RemoteTopic, #message{payload = Payload}} ->
                ct:pal("remote broker got message: ~p on topic ~p", [Payload, RemoteTopic]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),
    emqx:unsubscribe(RemoteTopic),

    %% PUBLISH a message to the rule.
    Payload2 = <<"hi">>,
    RuleTopic = <<"t/1">>,
    RemoteTopic2 = <<?EGRESS_REMOTE_TOPIC, "/", RuleTopic/binary>>,
    emqx:subscribe(RemoteTopic2),
    timer:sleep(100),
    emqx:publish(emqx_message:make(RuleTopic, Payload2)),
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    ?assertMatch(#{<<"id">> := RuleId, <<"name">> := _}, jsx:decode(Rule1)),
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
        jsx:decode(Metrics)
    ),

    %% we should receive a message on the "remote" broker, with specified topic
    ?assert(
        receive
            {deliver, RemoteTopic2, #message{payload = Payload2}} ->
                ct:pal("remote broker got message: ~p on topic ~p", [Payload2, RemoteTopic2]),
                true;
            Msg ->
                ct:pal("Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ),

    %% verify the metrics of the bridge
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDEgress]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"matched">> := 2, <<"success">> := 2, <<"failed">> := 0},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> := #{
                            <<"matched">> := 2, <<"success">> := 2, <<"failed">> := 0
                        }
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []).

t_mqtt_conn_bridge_egress_reconnect(_) ->
    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,

    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?SERVER_CONF(User1)#{
            <<"type">> => ?TYPE_MQTT,
            <<"name">> => ?BRIDGE_NAME_EGRESS,
            <<"egress">> => ?EGRESS_CONF,
            %% to make it reconnect quickly
            <<"reconnect_interval">> => <<"1s">>,
            <<"resource_opts">> => #{
                <<"worker_pool_size">> => 2,
                <<"enable_queue">> => true,
                <<"query_mode">> => <<"sync">>,
                %% to make it check the healthy quickly
                <<"health_check_interval">> => <<"0.5s">>
            }
        }
    ),
    #{
        <<"type">> := ?TYPE_MQTT,
        <<"name">> := ?BRIDGE_NAME_EGRESS
    } = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?TYPE_MQTT, ?BRIDGE_NAME_EGRESS),
    %% we now test if the bridge works as expected
    LocalTopic = <<?EGRESS_LOCAL_TOPIC, "/1">>,
    RemoteTopic = <<?EGRESS_REMOTE_TOPIC, "/", LocalTopic/binary>>,
    Payload0 = <<"hello">>,
    emqx:subscribe(RemoteTopic),
    timer:sleep(100),
    %% PUBLISH a message to the 'local' broker, as we have only one broker,
    %% the remote broker is also the local one.
    emqx:publish(emqx_message:make(LocalTopic, Payload0)),

    %% we should receive a message on the "remote" broker, with specified topic
    assert_mqtt_msg_received(RemoteTopic, Payload0),

    %% verify the metrics of the bridge
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDEgress]), []),
    ?assertMatch(
        #{
            <<"metrics">> := #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> :=
                            #{<<"matched">> := 1, <<"success">> := 1, <<"failed">> := 0}
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    %% stop the listener 1883 to make the bridge disconnected
    ok = emqx_listeners:stop_listener('tcp:default'),
    ct:sleep(1500),

    %% PUBLISH 2 messages to the 'local' broker, the message should
    ok = snabbkaffe:start_trace(),
    {ok, SRef} =
        snabbkaffe:subscribe(
            fun
                (
                    #{
                        ?snk_kind := call_query_enter,
                        query := {query, _From, {send_message, #{}}, _Sent}
                    }
                ) ->
                    true;
                (_) ->
                    false
            end,
            _NEvents = 2,
            _Timeout = 1_000
        ),
    Payload1 = <<"hello2">>,
    Payload2 = <<"hello3">>,
    emqx:publish(emqx_message:make(LocalTopic, Payload1)),
    emqx:publish(emqx_message:make(LocalTopic, Payload2)),
    {ok, _} = snabbkaffe:receive_events(SRef),
    ok = snabbkaffe:stop(),

    %% verify the metrics of the bridge, the message should be queued
    {ok, 200, BridgeStr1} = request(get, uri(["bridges", BridgeIDEgress]), []),
    %% matched >= 3 because of possible retries.
    ?assertMatch(
        #{
            <<"status">> := Status,
            <<"metrics">> := #{
                <<"matched">> := Matched, <<"success">> := 1, <<"failed">> := 0, <<"queuing">> := 2
            }
        } when Matched >= 3 andalso (Status == <<"connected">> orelse Status == <<"connecting">>),
        jsx:decode(BridgeStr1)
    ),

    %% start the listener 1883 to make the bridge reconnected
    ok = emqx_listeners:start_listener('tcp:default'),
    timer:sleep(1500),
    %% verify the metrics of the bridge, the 2 queued messages should have been sent
    {ok, 200, BridgeStr2} = request(get, uri(["bridges", BridgeIDEgress]), []),
    %% matched >= 3 because of possible retries.
    ?assertMatch(
        #{
            <<"status">> := <<"connected">>,
            <<"metrics">> := #{
                <<"matched">> := Matched,
                <<"success">> := 3,
                <<"failed">> := 0,
                <<"queuing">> := 0,
                <<"retried">> := _
            }
        } when Matched >= 3,
        jsx:decode(BridgeStr2)
    ),
    %% also verify the 2 messages have been sent to the remote broker
    assert_mqtt_msg_received(RemoteTopic, Payload1),
    assert_mqtt_msg_received(RemoteTopic, Payload2),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),
    ok.

assert_mqtt_msg_received(Topic, Payload) ->
    ?assert(
        receive
            {deliver, Topic, #message{payload = Payload}} ->
                ct:pal("Got mqtt message: ~p on topic ~p", [Payload, Topic]),
                true;
            Msg ->
                ct:pal("Unexpected Msg: ~p", [Msg]),
                false
        after 100 ->
            false
        end
    ).

request(Method, Url, Body) ->
    request(<<"connector_admin">>, Method, Url, Body).
