%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include("emqx/include/emqx.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-define(CONF_DEFAULT, <<"connectors: {}">>).
-define(BRIDGE_CONF_DEFAULT, <<"bridges: {}">>).
-define(CONNECTR_TYPE, <<"mqtt">>).
-define(CONNECTR_NAME, <<"test_connector">>).
-define(CONNECTR_ID, <<"mqtt:test_connector">>).
-define(BRIDGE_NAME_INGRESS, <<"ingress_test_bridge">>).
-define(BRIDGE_NAME_EGRESS, <<"egress_test_bridge">>).
-define(BRIDGE_ID_INGRESS, <<"mqtt:ingress_test_bridge">>).
-define(BRIDGE_ID_EGRESS, <<"mqtt:egress_test_bridge">>).
-define(MQTT_CONNECOTR(Username),
#{
    <<"server">> => <<"127.0.0.1:1883">>,
    <<"username">> => Username,
    <<"password">> => <<"">>,
    <<"proto_ver">> => <<"v4">>,
    <<"ssl">> => #{<<"enable">> => false}
}).
-define(MQTT_CONNECOTR2(Server),
    ?MQTT_CONNECOTR(<<"user1">>)#{<<"server">> => Server}).

-define(MQTT_BRIDGE_INGRESS(ID),
#{
    <<"connector">> => ID,
    <<"direction">> => <<"ingress">>,
    <<"from_remote_topic">> => <<"remote_topic/#">>,
    <<"to_local_topic">> => <<"local_topic/${topic}">>,
    <<"subscribe_qos">> => 1,
    <<"payload">> => <<"${payload}">>,
    <<"qos">> => <<"${qos}">>,
    <<"retain">> => <<"${retain}">>
}).

-define(MQTT_BRIDGE_EGRESS(ID),
#{
    <<"connector">> => ID,
    <<"direction">> => <<"egress">>,
    <<"from_local_topic">> => <<"local_topic/#">>,
    <<"to_remote_topic">> => <<"remote_topic/${topic}">>,
    <<"payload">> => <<"${payload}">>,
    <<"qos">> => <<"${qos}">>,
    <<"retain">> => <<"${retain}">>
}).

-define(metrics(MATCH, SUCC, FAILED, SPEED, SPEED5M, SPEEDMAX),
    #{<<"matched">> := MATCH, <<"success">> := SUCC,
      <<"failed">> := FAILED, <<"rate">> := SPEED,
      <<"rate_last5m">> := SPEED5M, <<"rate_max">> := SPEEDMAX}).

all() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    [].

suite() ->
	[{timetrap,{seconds,30}}].

init_per_suite(Config) ->
    ok = emqx_config:put([emqx_dashboard], #{
        default_username => <<"admin">>,
        default_password => <<"public">>,
        listeners => [#{
            protocol => http,
            port => 18083
        }]
    }),
    _ = application:load(emqx_conf),
    %% some testcases (may from other app) already get emqx_connector started
    _ = application:stop(emqx_resource),
    _ = application:stop(emqx_connector),
    ok = emqx_common_test_helpers:start_apps([emqx_connector, emqx_bridge, emqx_dashboard]),
    ok = emqx_config:init_load(emqx_connector_schema, ?CONF_DEFAULT),
    ok = emqx_config:init_load(emqx_bridge_schema, ?BRIDGE_CONF_DEFAULT),
    Config.

end_per_suite(_Config) ->
    emqx_common_test_helpers:stop_apps([emqx_connector, emqx_bridge, emqx_dashboard]),
    ok.

init_per_testcase(_, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.
end_per_testcase(_, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_mqtt_crud_apis(_) ->
    %% assert we there's no connectors at first
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),

    %% then we add a mqtt connector, using POST
    %% POST /connectors/ will create a connector
    User1 = <<"user1">>,
    {ok, 201, Connector} = request(post, uri(["connectors"]),
        ?MQTT_CONNECOTR(User1)#{ <<"type">> => ?CONNECTR_TYPE
                               , <<"name">> => ?CONNECTR_NAME
                               }),

    %ct:pal("---connector: ~p", [Connector]),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  , <<"username">> := User1
                  , <<"password">> := <<"">>
                  , <<"proto_ver">> := <<"v4">>
                  , <<"ssl">> := #{<<"enable">> := false}
                  }, jsx:decode(Connector)),

    %% create a again returns an error
    {ok, 400, RetMsg} = request(post, uri(["connectors"]),
        ?MQTT_CONNECOTR(User1)#{ <<"type">> => ?CONNECTR_TYPE
                               , <<"name">> => ?CONNECTR_NAME
                               }),
    ?assertMatch(
        #{ <<"code">> := _
         , <<"message">> := <<"connector already exists">>
         }, jsx:decode(RetMsg)),

    %% update the request-path of the connector
    User2 = <<"user2">>,
    {ok, 200, Connector2} = request(put, uri(["connectors", ?CONNECTR_ID]),
                                 ?MQTT_CONNECOTR(User2)),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  , <<"username">> := User2
                  , <<"password">> := <<"">>
                  , <<"proto_ver">> := <<"v4">>
                  , <<"ssl">> := #{<<"enable">> := false}
                  }, jsx:decode(Connector2)),

    %% list all connectors again, assert Connector2 is in it
    {ok, 200, Connector2Str} = request(get, uri(["connectors"]), []),
    ?assertMatch([#{ <<"id">> := ?CONNECTR_ID
                   , <<"server">> := <<"127.0.0.1:1883">>
                   , <<"username">> := User2
                   , <<"password">> := <<"">>
                   , <<"proto_ver">> := <<"v4">>
                   , <<"ssl">> := #{<<"enable">> := false}
                   }], jsx:decode(Connector2Str)),

    %% get the connector by id
    {ok, 200, Connector3Str} = request(get, uri(["connectors", ?CONNECTR_ID]), []),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  , <<"username">> := User2
                  , <<"password">> := <<"">>
                  , <<"proto_ver">> := <<"v4">>
                  , <<"ssl">> := #{<<"enable">> := false}
                  }, jsx:decode(Connector3Str)),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ?CONNECTR_ID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),

    %% update a deleted connector returns an error
    {ok, 404, ErrMsg2} = request(put, uri(["connectors", ?CONNECTR_ID]),
                                 ?MQTT_CONNECOTR(User2)),
    ?assertMatch(
        #{ <<"code">> := _
         , <<"message">> := <<"connector not found">>
         }, jsx:decode(ErrMsg2)),
    ok.

t_mqtt_conn_bridge_ingress(_) ->
    %% assert we there's no connectors and no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,
    {ok, 201, Connector} = request(post, uri(["connectors"]),
        ?MQTT_CONNECOTR(User1)#{ <<"type">> => ?CONNECTR_TYPE
                               , <<"name">> => ?CONNECTR_NAME
                               }),

    %ct:pal("---connector: ~p", [Connector]),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  , <<"username">> := User1
                  , <<"password">> := <<"">>
                  , <<"proto_ver">> := <<"v4">>
                  , <<"ssl">> := #{<<"enable">> := false}
                  }, jsx:decode(Connector)),

    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(post, uri(["bridges"]),
        ?MQTT_BRIDGE_INGRESS(?CONNECTR_ID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_INGRESS
        }),

    %ct:pal("---bridge: ~p", [Bridge]),
    ?assertMatch(#{ <<"id">> := ?BRIDGE_ID_INGRESS
                  , <<"type">> := <<"mqtt">>
                  , <<"status">> := <<"connected">>
                  , <<"connector">> := ?CONNECTR_ID
                  }, jsx:decode(Bridge)),

    %% we now test if the bridge works as expected

    RemoteTopic = <<"remote_topic/1">>,
    LocalTopic = <<"local_topic/", RemoteTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(LocalTopic),
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
        end),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", ?BRIDGE_ID_INGRESS]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ?CONNECTR_ID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    ok.

t_mqtt_conn_bridge_egress(_) ->
    %% assert we there's no connectors and no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a mqtt connector, using POST
    User1 = <<"user1">>,
    {ok, 201, Connector} = request(post, uri(["connectors"]),
        ?MQTT_CONNECOTR(User1)#{ <<"type">> => ?CONNECTR_TYPE
                               , <<"name">> => ?CONNECTR_NAME
                               }),

    %ct:pal("---connector: ~p", [Connector]),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  , <<"username">> := User1
                  , <<"password">> := <<"">>
                  , <<"proto_ver">> := <<"v4">>
                  , <<"ssl">> := #{<<"enable">> := false}
                  }, jsx:decode(Connector)),

    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(post, uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(?CONNECTR_ID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }),

    %ct:pal("---bridge: ~p", [Bridge]),
    ?assertMatch(#{ <<"id">> := ?BRIDGE_ID_EGRESS
                  , <<"type">> := ?CONNECTR_TYPE
                  , <<"name">> := ?BRIDGE_NAME_EGRESS
                  , <<"status">> := <<"connected">>
                  , <<"connector">> := ?CONNECTR_ID
                  }, jsx:decode(Bridge)),

    %% we now test if the bridge works as expected
    LocalTopic = <<"local_topic/1">>,
    RemoteTopic = <<"remote_topic/", LocalTopic/binary>>,
    Payload = <<"hello">>,
    emqx:subscribe(RemoteTopic),
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
        end),

    %% verify the metrics of the bridge
    {ok, 200, BridgeStr} = request(get, uri(["bridges", ?BRIDGE_ID_EGRESS]), []),
    ?assertMatch(#{ <<"id">> := ?BRIDGE_ID_EGRESS
                  , <<"metrics">> := ?metrics(1, 1, 0, _, _, _)
                  , <<"node_metrics">> :=
                      [#{<<"node">> := _, <<"metrics">> := ?metrics(1, 1, 0, _, _, _)}]
                  }, jsx:decode(BridgeStr)),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", ?BRIDGE_ID_EGRESS]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ?CONNECTR_ID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    ok.

%% t_mqtt_conn_update:
%% - update a connector should also update all of the the bridges
%% - cannot delete a connector that is used by at least one bridge
t_mqtt_conn_update(_) ->
    %% assert we there's no connectors and no bridges at first
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% then we add a mqtt connector, using POST
    {ok, 201, Connector} = request(post, uri(["connectors"]),
                            ?MQTT_CONNECOTR2(<<"127.0.0.1:1883">>)
                                #{ <<"type">> => ?CONNECTR_TYPE
                                 , <<"name">> => ?CONNECTR_NAME
                                 }),

    %ct:pal("---connector: ~p", [Connector]),
    ?assertMatch(#{ <<"id">> := ?CONNECTR_ID
                  , <<"server">> := <<"127.0.0.1:1883">>
                  }, jsx:decode(Connector)),

    %% ... and a MQTT bridge, using POST
    %% we bind this bridge to the connector created just now
    {ok, 201, Bridge} = request(post, uri(["bridges"]),
        ?MQTT_BRIDGE_EGRESS(?CONNECTR_ID)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }),
    ?assertMatch(#{ <<"id">> := ?BRIDGE_ID_EGRESS
                  , <<"type">> := <<"mqtt">>
                  , <<"name">> := ?BRIDGE_NAME_EGRESS
                  , <<"status">> := <<"connected">>
                  , <<"connector">> := ?CONNECTR_ID
                  }, jsx:decode(Bridge)),

    %% then we try to update 'server' of the connector, to an unavailable IP address
    %% the update should fail because of 'unreachable' or 'connrefused'
    {ok, 400, _ErrorMsg} = request(put, uri(["connectors", ?CONNECTR_ID]),
                                 ?MQTT_CONNECOTR2(<<"127.0.0.1:2883">>)),
    %% we fix the 'server' parameter to a normal one, it should work
    {ok, 200, _} = request(put, uri(["connectors", ?CONNECTR_ID]),
                                 ?MQTT_CONNECOTR2(<<"127.0.0.1 : 1883">>)),
    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri(["bridges", ?BRIDGE_ID_EGRESS]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["bridges"]), []),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ?CONNECTR_ID]), []),
    {ok, 200, <<"[]">>} = request(get, uri(["connectors"]), []).

t_mqtt_conn_testing(_) ->
    %% APIs for testing the connectivity
    %% then we add a mqtt connector, using POST
    {ok, 200, <<>>} = request(post, uri(["connectors_test"]),
        ?MQTT_CONNECOTR2(<<"127.0.0.1:1883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }),
    {ok, 400, _} = request(post, uri(["connectors_test"]),
        ?MQTT_CONNECOTR2(<<"127.0.0.1:2883">>)#{
            <<"type">> => ?CONNECTR_TYPE,
            <<"name">> => ?BRIDGE_NAME_EGRESS
        }).

%%--------------------------------------------------------------------
%% HTTP Request
%%--------------------------------------------------------------------
-define(HOST, "http://127.0.0.1:18083/").
-define(API_VERSION, "v5").
-define(BASE_PATH, "api").

request(Method, Url, Body) ->
    Request = case Body of
        [] -> {Url, [auth_header_()]};
        _ -> {Url, [auth_header_()], "application/json", jsx:encode(Body)}
    end,
    ct:pal("Method: ~p, Request: ~p", [Method, Request]),
    case httpc:request(Method, Request, [], [{body_format, binary}]) of
        {error, socket_closed_remotely} ->
            {error, socket_closed_remotely};
        {ok, {{"HTTP/1.1", Code, _}, _Headers, Return} } ->
            {ok, Code, Return};
        {ok, {Reason, _, _}} ->
            {error, Reason}
    end.

uri() -> uri([]).
uri(Parts) when is_list(Parts) ->
    NParts = [E || E <- Parts],
    ?HOST ++ filename:join([?BASE_PATH, ?API_VERSION | NParts]).

auth_header_() ->
    Username = <<"admin">>,
    Password = <<"public">>,
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    {"Authorization", "Bearer " ++ binary_to_list(Token)}.

