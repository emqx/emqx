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

-module(emqx_connector_api_SUITE).

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
-define(CONNECTR_TYPE, <<"mqtt">>).
-define(CONNECTR_NAME, <<"test_connector">>).
-define(BRIDGE_NAME_INGRESS, <<"ingress_test_bridge">>).
-define(BRIDGE_NAME_EGRESS, <<"egress_test_bridge">>).
-define(MQTT_CONNECTOR(Username), #{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => Username,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).
-define(MQTT_CONNECTOR2(Server), ?MQTT_CONNECTOR(<<"user1">>)#{<<"server">> => Server}).

-define(MQTT_BRIDGE_INGRESS(ID), #{
    <<"connector">> => ID,
    <<"direction">> => <<"ingress">>,
    <<"remote_topic">> => <<"remote_topic/#">>,
    <<"remote_qos">> => 2,
    <<"local_topic">> => <<"local_topic/${topic}">>,
    <<"local_qos">> => <<"${qos}">>,
    <<"payload">> => <<"${payload}">>,
    <<"retain">> => <<"${retain}">>
}).

-define(MQTT_BRIDGE_EGRESS(ID), #{
    <<"connector">> => ID,
    <<"direction">> => <<"egress">>,
    <<"local_topic">> => <<"local_topic/#">>,
    <<"remote_topic">> => <<"remote_topic/${topic}">>,
    <<"payload">> => <<"${payload}">>,
    <<"remote_qos">> => <<"${qos}">>,
    <<"retain">> => <<"${retain}">>
}).

-define(metrics(MATCH, SUCC, FAILED, SPEED, SPEED5M, SPEEDMAX), #{
    <<"matched">> := MATCH,
    <<"success">> := SUCC,
    <<"failed">> := FAILED,
    <<"rate">> := SPEED,
    <<"rate_last5m">> := SPEED5M,
    <<"rate_max">> := SPEEDMAX
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
            emqx_connector,
            emqx_bridge,
            emqx_dashboard
        ],
        fun set_special_configs/1
    ),
    ok = emqx_common_test_helpers:load_config(emqx_connector_schema, <<"connectors: {}">>),
    ok = emqx_common_test_helpers:load_config(
        emqx_rule_engine_schema,
        <<"rule_engine {rules {}}">>
    ),
    ok = emqx_common_test_helpers:load_config(emqx_bridge_schema, ?BRIDGE_CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([
        emqx_rule_engine,
        emqx_connector,
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
    ),
    lists:foreach(
        fun(#{<<"type">> := Type, <<"name">> := Name}) ->
            {ok, _} = emqx_connector:delete(Type, Name)
        end,
        emqx_connector:list_raw()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_mqtt_crud_apis(_) ->
    %% assert we there's no connectors at first
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),

    %% then we add a mqtt connector, using POST
    %% POST /connectors/ will create a connector
    User1 = <<"user1">>,
    {ok, 400, <<
        "{\"code\":\"BAD_REQUEST\",\"message\""
        ":\"missing some required fields: [name, type]\"}"
    >>} =
        request(
            post,
            uri(["connectors"]),
            ?MQTT_CONNECTOR(User1)#{<<"type">> => ?CONNECTR_TYPE}
        ),
    {ok, 201, Connector} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR(User1)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),

    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?CONNECTR_NAME,
        <<"server">> := <<"127.0.0.1:1883">>,
        <<"username">> := User1,
        <<"password">> := <<"">>,
        <<"proto_ver">> := <<"v4">>,
        <<"ssl">> := #{<<"enable">> := false}
    } = jsx:decode(Connector),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% update the request-path of the connector
    User2 = <<"user2">>,
    {ok, 200, Connector2} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR(User2)
    ),
    ?assertMatch(
        #{
            <<"type">> := ?CONNECTR_TYPE,
            <<"name">> := ?CONNECTR_NAME,
            <<"server">> := <<"127.0.0.1:1883">>,
            <<"username">> := User2,
            <<"password">> := <<"">>,
            <<"proto_ver">> := <<"v4">>,
            <<"ssl">> := #{<<"enable">> := false}
        },
        jsx:decode(Connector2)
    ),

    %% list all connectors again, assert Connector2 is in it
    {ok, 200, Connector2Str} = request(get, uri(["connectors"]), []),
    ?assertMatch(
        [
            #{
                <<"type">> := ?CONNECTR_TYPE,
                <<"name">> := ?CONNECTR_NAME,
                <<"server">> := <<"127.0.0.1:1883">>,
                <<"username">> := User2,
                <<"password">> := <<"">>,
                <<"proto_ver">> := <<"v4">>,
                <<"ssl">> := #{<<"enable">> := false}
            }
        ],
        jsx:decode(Connector2Str)
    ),

    %% get the connector by id
    {ok, 200, Connector3Str} = request(get, uri(["connectors", ConnctorID]), []),
    ?assertMatch(
        #{
            <<"type">> := ?CONNECTR_TYPE,
            <<"name">> := ?CONNECTR_NAME,
            <<"server">> := <<"127.0.0.1:1883">>,
            <<"username">> := User2,
            <<"password">> := <<"">>,
            <<"proto_ver">> := <<"v4">>,
            <<"ssl">> := #{<<"enable">> := false}
        },
        jsx:decode(Connector3Str)
    ),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),

    %% update a deleted connector returns an error
    {ok, 404, ErrMsg2} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR(User2)
    ),
    ?assertMatch(
        #{
            <<"code">> := _,
            <<"message">> := <<"connector not found">>
        },
        jsx:decode(ErrMsg2)
    ),
    ok.

t_mqtt_conn_bridge_ingress(_) ->
    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,
    {ok, 201, Connector} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR(User1)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),

    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?CONNECTR_NAME,
        <<"server">> := <<"127.0.0.1:1883">>,
        <<"num_of_bridges">> := 0,
        <<"username">> := User1,
        <<"password">> := <<"">>,
        <<"proto_ver">> := <<"v4">>,
        <<"ssl">> := #{<<"enable">> := false}
    } = jsx:decode(Connector),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    timer:sleep(50),
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_INGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_INGRESS
        }
    ),
    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?BRIDGE_NAME_INGRESS,
        <<"connector">> := ConnctorID
    } = jsx:decode(Bridge),
    BridgeIDIngress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_INGRESS),
    wait_for_resource_ready(BridgeIDIngress, 5),

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

    %% get the connector by id, verify the num_of_bridges now is 1
    {ok, 200, Connector1Str} = request(get, uri(["connectors", ConnctorID]), []),
    ?assertMatch(#{<<"num_of_bridges">> := 1}, jsx:decode(Connector1Str)),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    ok.

t_mqtt_conn_bridge_egress(_) ->
    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,
    {ok, 201, Connector} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR(User1)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),

    %ct:pal("---connector: ~p", [Connector]),
    #{
        <<"server">> := <<"127.0.0.1:1883">>,
        <<"username">> := User1,
        <<"password">> := <<"">>,
        <<"proto_ver">> := <<"v4">>,
        <<"ssl">> := #{<<"enable">> := false}
    } = jsx:decode(Connector),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?BRIDGE_NAME_EGRESS,
        <<"connector">> := ConnctorID
    } = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_EGRESS),
    wait_for_resource_ready(BridgeIDEgress, 5),

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
            <<"metrics">> := ?metrics(1, 1, 0, _, _, _),
            <<"node_metrics">> :=
                [#{<<"node">> := _, <<"metrics">> := ?metrics(1, 1, 0, _, _, _)}]
        },
        jsx:decode(BridgeStr)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    ok.

%% t_mqtt_conn_update:
%% - update a connector should also update all of the the bridges
%% - cannot delete a connector that is used by at least one bridge
t_mqtt_conn_update(_) ->
    %% then we add a mqtt connector, using POST
    {ok, 201, Connector} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:1883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),

    %ct:pal("---connector: ~p", [Connector]),
    #{<<"server">> := <<"127.0.0.1:1883">>} = jsx:decode(Connector),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?BRIDGE_NAME_EGRESS,
        <<"connector">> := ConnctorID
    } = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_EGRESS),
    wait_for_resource_ready(BridgeIDEgress, 5),

    %% Then we try to update 'server' of the connector, to an unavailable IP address
    %% The update OK, we recreate the resource even if the resource is current connected,
    %% and the target resource we're going to update is unavailable.
    {ok, 200, _} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:2603">>)
    ),
    %% we fix the 'server' parameter to a normal one, it should work
    {ok, 200, _} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1 : 1883">>)
    ),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []).

t_mqtt_conn_update2(_) ->
    %% then we add a mqtt connector, using POST
    %% but this connector is point to a unreachable server "2603"
    {ok, 201, Connector} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:2603">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),

    #{<<"server">> := <<"127.0.0.1:2603">>} = jsx:decode(Connector),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    #{
        <<"type">> := ?CONNECTR_TYPE,
        <<"name">> := ?BRIDGE_NAME_EGRESS,
        <<"status">> := <<"disconnected">>,
        <<"connector">> := ConnctorID
    } = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_EGRESS),
    %% We try to fix the 'server' parameter, to another unavailable server..
    %% The update should success: we don't check the connectivity of the new config
    %% if the resource is now disconnected.
    {ok, 200, _} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:2604">>)
    ),
    %% we fix the 'server' parameter to a normal one, it should work
    {ok, 200, _} = request(
        put,
        uri(["connectors", ConnctorID]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:1883">>)
    ),
    wait_for_resource_ready(BridgeIDEgress, 5),
    {ok, 200, BridgeStr} = request(get, uri(["bridges", BridgeIDEgress]), []),
    ?assertMatch(#{<<"status">> := <<"connected">>}, jsx:decode(BridgeStr)),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []).

t_mqtt_conn_update3(_) ->
    %% we add a mqtt connector, using POST
    {ok, 201, _} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:1883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    #{<<"connector">> := ConnctorID} = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_EGRESS),
    wait_for_resource_ready(BridgeIDEgress, 5),

    %% delete the connector should fail because it is in use by a bridge
    {ok, 403, _} = request(delete, uri(["connectors", ConnctorID]), []),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    %% the connector now can be deleted without problems
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []).

t_mqtt_conn_testing(_) ->
    %% APIs for testing the connectivity
    %% then we add a mqtt connector, using POST
    {ok, 204, <<>>} = request(
        post,
        uri(["connectors_test"]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:1883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    {ok, 400, _} = request(
        post,
        uri(["connectors_test"]),
        ?MQTT_CONNECTOR2(<<"127.0.0.1:2883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ).

t_ingress_mqtt_bridge_with_rules(_) ->
    {ok, 201, _} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR(<<"user1">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),

    {ok, 201, _} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_INGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_INGRESS
        }
    ),
    BridgeIDIngress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_INGRESS),

    {ok, 201, Rule} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"A_rule_get_messages_from_a_source_mqtt_bridge">>,
            <<"enable">> => true,
            <<"actions">> => [#{<<"function">> => "emqx_connector_api_SUITE:inspect"}],
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
    wait_for_resource_ready(BridgeIDIngress, 5),
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

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDIngress]), []),
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []).

t_egress_mqtt_bridge_with_rules(_) ->
    {ok, 201, _} = request(
        post,
        uri(["connectors"]),
        ?MQTT_CONNECTOR(<<"user1">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?CONNECTR_NAME
        }
    ),
    ConnctorID = emqx_connector:connector_id(?CONNECTR_TYPE, ?CONNECTR_NAME),
    {ok, 201, Bridge} = request(
        post,
        uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(ConnctorID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }
    ),
    #{<<"type">> := ?CONNECTR_TYPE, <<"name">> := ?BRIDGE_NAME_EGRESS} = jsx:decode(Bridge),
    BridgeIDEgress = emqx_bridge_resource:bridge_id(?CONNECTR_TYPE, ?BRIDGE_NAME_EGRESS),

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
    wait_for_resource_ready(BridgeIDEgress, 5),
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
    wait_for_resource_ready(BridgeIDEgress, 5),
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
            <<"metrics">> := ?metrics(2, 2, 0, _, _, _),
            <<"node_metrics">> :=
                [#{<<"node">> := _, <<"metrics">> := ?metrics(2, 2, 0, _, _, _)}]
        },
        jsx:decode(BridgeStr)
    ),

    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), []),
    {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeIDEgress]), []),
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnctorID]), []).

request(Method, Url, Body) ->
    request(<<"connector_admin">>, Method, Url, Body).

wait_for_resource_ready(InstId, 0) ->
    ct:pal("--- bridge ~p: ~p", [InstId, emqx_bridge:lookup(InstId)]),
    ct:fail(wait_resource_timeout);
wait_for_resource_ready(InstId, Retry) ->
    case emqx_bridge:lookup(InstId) of
        {ok, #{resource_data := #{status := connected}}} ->
            ok;
        _ ->
            timer:sleep(100),
            wait_for_resource_ready(InstId, Retry - 1)
    end.
