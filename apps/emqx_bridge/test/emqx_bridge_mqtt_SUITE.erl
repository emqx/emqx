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
-include("emqx_dashboard/include/emqx_dashboard.hrl").

%% output functions
-export([inspect/3]).

-define(BRIDGE_CONF_DEFAULT, <<"bridges: {}">>).
-define(TYPE_MQTT, <<"mqtt">>).
-define(NAME_MQTT, <<"my_mqtt_bridge">>).
-define(BRIDGE_NAME_INGRESS, <<"ingress_mqtt_bridge">>).
-define(BRIDGE_NAME_EGRESS, <<"egress_mqtt_bridge">>).
-define(SERVER_CONF(Username), #{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => Username,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).

-define(INGRESS_CONF, #{
    <<"remote">> => #{
        <<"topic">> => <<"remote_topic/#">>,
        <<"qos">> => 2
    },
    <<"local">> => #{
        <<"topic">> => <<"local_topic/${topic}">>,
        <<"qos">> => <<"${qos}">>,
        <<"payload">> => <<"${payload}">>,
        <<"retain">> => <<"${retain}">>
    }
}).

-define(EGRESS_CONF, #{
    <<"local">> => #{
        <<"topic">> => <<"local_topic/#">>
    },
    <<"remote">> => #{
        <<"topic">> => <<"remote_topic/${topic}">>,
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
    RemoteTopic = <<"remote_topic/1">>,
    LocalTopic = <<"local_topic/", RemoteTopic/binary>>,
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
    LocalTopic = <<"local_topic/1">>,
    RemoteTopic = <<"remote_topic/", LocalTopic/binary>>,
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
            <<"metrics">> := #{<<"matched">> := 1, <<"sent.success">> := 1, <<"sent.failed">> := 0},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> :=
                            #{<<"matched">> := 1, <<"sent.success">> := 1, <<"sent.failed">> := 0}
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
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

    RemoteTopic = <<"remote_topic/1">>,
    LocalTopic = <<"local_topic/", RemoteTopic/binary>>,
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
    ?assertMatch(
        #{
            <<"id">> := RuleId,
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
        jsx:decode(Rule1)
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
    LocalTopic = <<"local_topic/1">>,
    RemoteTopic = <<"remote_topic/", LocalTopic/binary>>,
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
    RemoteTopic2 = <<"remote_topic/", RuleTopic/binary>>,
    emqx:subscribe(RemoteTopic2),
    timer:sleep(100),
    emqx:publish(emqx_message:make(RuleTopic, Payload2)),
    {ok, 200, Rule1} = request(get, uri(["rules", RuleId]), []),
    #{
        <<"id">> := RuleId,
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
    } = jsx:decode(Rule1),
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
            <<"metrics">> := #{<<"matched">> := 2, <<"sent.success">> := 2, <<"sent.failed">> := 0},
            <<"node_metrics">> :=
                [
                    #{
                        <<"node">> := _,
                        <<"metrics">> := #{
                            <<"matched">> := 2, <<"sent.success">> := 2, <<"sent.failed">> := 0
                        }
                    }
                ]
        },
        jsx:decode(BridgeStr)
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []).

request(Method, Url, Body) ->
    request(<<"connector_admin">>, Method, Url, Body).
