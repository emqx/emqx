%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(CONNECTOR_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(CONNECTOR(NAME, TYPE), #{
    <<"enable">> => true,
    %<<"ssl">> => #{<<"enable">> => false},
    <<"type">> => TYPE,
    <<"name">> => NAME
}).

-define(CONNECTOR_TYPE_KAFKA, <<"kafka">>).
-define(KAFKA_CONNECTOR(Name, BootstrapHosts), ?CONNECTOR(Name, ?CONNECTOR_TYPE_KAFKA)#{
    <<"authentication">> => <<"none">>,
    <<"bootstrap_hosts">> => BootstrapHosts,
    <<"connect_timeout">> => <<"5s">>,
    <<"metadata_request_timeout">> => <<"5s">>,
    <<"min_metadata_refresh_interval">> => <<"3s">>,
    <<"socket_opts">> =>
        #{
            <<"nodelay">> => true,
            <<"recbuf">> => <<"1024KB">>,
            <<"sndbuf">> => <<"1024KB">>,
            <<"tcp_keepalive">> => <<"none">>
        }
}).

%% -define(CONNECTOR_TYPE_MQTT, <<"mqtt">>).
%% -define(MQTT_CONNECTOR(SERVER, NAME), ?CONNECTOR(NAME, ?CONNECTOR_TYPE_MQTT)#{
%%     <<"server">> => SERVER,
%%     <<"username">> => <<"user1">>,
%%     <<"password">> => <<"">>,
%%     <<"proto_ver">> => <<"v5">>,
%%     <<"egress">> => #{
%%         <<"remote">> => #{
%%             <<"topic">> => <<"emqx/${topic}">>,
%%             <<"qos">> => <<"${qos}">>,
%%             <<"retain">> => false
%%         }
%%     }
%% }).
%% -define(MQTT_CONNECTOR(SERVER), ?MQTT_CONNECTOR(SERVER, <<"mqtt_egress_test_connector">>)).

%% -define(CONNECTOR_TYPE_HTTP, <<"kafka">>).
%% -define(HTTP_CONNECTOR(URL, NAME), ?CONNECTOR(NAME, ?CONNECTOR_TYPE_HTTP)#{
%%     <<"url">> => URL,
%%     <<"local_topic">> => <<"emqx_webhook/#">>,
%%     <<"method">> => <<"post">>,
%%     <<"body">> => <<"${payload}">>,
%%     <<"headers">> => #{
%%         % NOTE
%%         % The Pascal-Case is important here.
%%         % The reason is kinda ridiculous: `emqx_connector_resource:create_dry_run/2` converts
%%         % connector config keys into atoms, and the atom 'Content-Type' exists in the ERTS
%%         % when this happens (while the 'content-type' does not).
%%         <<"Content-Type">> => <<"application/json">>
%%     }
%% }).
%% -define(HTTP_CONNECTOR(URL), ?HTTP_CONNECTOR(URL, ?CONNECTOR_NAME)).

%% -define(URL(PORT, PATH),
%%         list_to_binary(
%%           io_lib:format(
%%             "http://localhost:~s/~s",
%%             [integer_to_list(PORT), PATH]
%%            )
%%          )
%%        ).

-define(APPSPECS, [
    emqx_conf,
    emqx,
    emqx_authn,
    emqx_management,
    {emqx_connector, "connectors {}"}
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

all() ->
    [
        %,
        {group, single}
        %{group, cluster_later_join},
        %{group, cluster}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    SingleOnlyTests = [
        t_connector_lifecycle
    ],
    ClusterLaterJoinOnlyTCs = [
        % t_cluster_later_join_metrics
    ],
    [
        {single, [], AllTCs -- ClusterLaterJoinOnlyTCs},
        {cluster_later_join, [], ClusterLaterJoinOnlyTCs},
        {cluster, [], (AllTCs -- SingleOnlyTests) -- ClusterLaterJoinOnlyTCs}
    ].

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config),
    init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]);
init_per_group(cluster_later_join = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config, #{join_to => undefined}),
    init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]);
init_per_group(Name, Config) ->
    WorkDir = filename:join(?config(priv_dir, Config), Name),
    Apps = emqx_cth_suite:start(?APPSPECS ++ [?APPSPEC_DASHBOARD], #{work_dir => WorkDir}),
    init_api([{group, single}, {group_apps, Apps}, {node, node()} | Config]).

init_api(Config) ->
    APINode = ?config(node, Config),
    {ok, App} = erpc:call(APINode, emqx_common_test_http, create_default_app, []),
    [{api, App} | Config].

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [?APPSPEC_DASHBOARD],
    Node2Apps = ?APPSPECS,
    emqx_cth_cluster:start(
        [
            {emqx_bridge_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_bridge_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => filename:join(?config(priv_dir, Config), Name)}
    ).

end_per_group(Group, Config) when
    Group =:= cluster;
    Group =:= cluster_later_join
->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config)),
    ok.

init_per_testcase(TestCase, Config) ->
    ?MODULE:TestCase({init, Config}).

end_per_testcase(TestCase, Config) ->
    Node = ?config(node, Config),
    ok = emqx_common_test_helpers:call_janitor(),
    ok = erpc:call(Node, fun clear_resources/0),
    ?MODULE:TestCase({'end', Config}).

clear_resources() ->
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            {ok, _} = emqx_bridge:remove(Type, Name)
        end,
        emqx_bridge:list()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% We have to pretend testing a kafka connector since at this point that's the
%% only one that's implemented.

-define(CONNECTOR_IMPL, dummy_connector_impl).
t_connector_lifecycle({init, Config}) ->
    meck:new(emqx_connector_ee_schema, [passthrough]),
    meck:expect(emqx_connector_ee_schema, resource_type, 1, ?CONNECTOR_IMPL),
    meck:new(?CONNECTOR_IMPL, [non_strict]),
    meck:expect(?CONNECTOR_IMPL, callback_mode, 0, async_if_possible),
    meck:expect(?CONNECTOR_IMPL, on_start, 2, {ok, connector_state}),
    meck:expect(?CONNECTOR_IMPL, on_stop, 2, ok),
    meck:expect(?CONNECTOR_IMPL, on_get_status, 2, connected),
    [{mocked_mods, [?CONNECTOR_IMPL, emqx_connector_ee_schema]} | Config];
t_connector_lifecycle({'end', Config}) ->
    MockedMods = ?config(mocked_mods, Config),
    meck:unload(MockedMods),
    Config;
t_connector_lifecycle(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    {ok, 404, _} = request(get, uri(["connectors", "foo"]), Config),
    {ok, 404, _} = request(get, uri(["connectors", "kafka:foo"]), Config),

    BootstrapHosts = <<"localhost:9092">>,
    % needed for patterns below
    ConnectorName = ?CONNECTOR_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE_KAFKA,
            <<"name">> := ConnectorName,
            <<"enable">> := true,
            <<"bootstrap_hosts">> := BootstrapHosts,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(?CONNECTOR_NAME, BootstrapHosts),
            Config
        )
    ),

    %% list all connectors, assert Connector is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?CONNECTOR_TYPE_KAFKA,
                <<"name">> := ConnectorName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri(["connectors"]), Config)
    ),

    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE_KAFKA, ?CONNECTOR_NAME),
    %% send an message to emqx and the message should be forwarded to the HTTP server

    %% [TODO] update the request-path of the connector
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?CONNECTOR_TYPE_KAFKA,
            <<"name">> := ConnectorName,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            put,
            uri(["connectors", ConnectorID]),
            ?KAFKA_CONNECTOR(?CONNECTOR_NAME, BootstrapHosts),
            Config
        )
    ),

    %% list all connectors, assert Connector is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?CONNECTOR_TYPE_KAFKA,
                <<"name">> := ConnectorName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri(["connectors"]), Config)
    ),

    %% get the connector by id
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?CONNECTOR_TYPE_KAFKA,
            <<"name">> := ConnectorName,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),

    %% Test bad updates
    %% ================

    %% Add connector with a name that is too long
    %% We only support connector names up to 255 characters
    %% LongName = list_to_binary(lists:duplicate(256, $a)),
    %% NameTooLongRequestResult = request_json(
    %%     post,
    %%     uri(["connectors"]),
    %%     ?HTTP_CONNECTOR(URL1, LongName),
    %%     Config
    %% ),
    %% ?assertMatch(
    %%     {ok, 400, _},
    %%     NameTooLongRequestResult
    %% ),
    %% {ok, 400, #{<<"message">> := NameTooLongMessage}} = NameTooLongRequestResult,
    %% %% Use regex to check that the message contains the name
    %% Match = re:run(NameTooLongMessage, LongName),
    %% ?assertMatch({match, _}, Match),
    %% %% Add connector without the URL field
    %% {ok, 400, PutFail1} = request_json(
    %%     put,
    %%     uri(["connectors", ConnectorID]),
    %%     maps:remove(<<"url">>, ?HTTP_CONNECTOR(URL2, Name)),
    %%     Config
    %% ),
    %% ?assertMatch(
    %%     #{<<"reason">> := <<"required_field">>},
    %%     json(maps:get(<<"message">>, PutFail1))
    %% ),
    %% {ok, 400, PutFail2} = request_json(
    %%     put,
    %%     uri(["connectors", ConnectorID]),
    %%     maps:put(<<"curl">>, URL2, maps:remove(<<"url">>, ?HTTP_CONNECTOR(URL2, Name))),
    %%     Config
    %% ),
    %% ?assertMatch(
    %%     #{
    %%         <<"reason">> := <<"unknown_fields">>,
    %%         <<"unknown">> := <<"curl">>
    %%     },
    %%     json(maps:get(<<"message">>, PutFail2))
    %% ),
    %% {ok, 400, _} = request_json(
    %%     put,
    %%     uri(["connectors", ConnectorID]),
    %%     ?HTTP_CONNECTOR(<<"localhost:1234/foo">>, Name),
    %%     Config
    %% ),
    %% {ok, 400, _} = request_json(
    %%     put,
    %%     uri(["connectors", ConnectorID]),
    %%     ?HTTP_CONNECTOR(<<"htpp://localhost:12341234/foo">>, Name),
    %%     Config
    %% ),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnectorID]), Config),
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    %% update a deleted connector returns an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(
            put,
            uri(["connectors", ConnectorID]),
            ?KAFKA_CONNECTOR(?CONNECTOR_NAME, BootstrapHosts),
            Config
        )
    ),

    %% try delete bad connector id
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid connector ID", _/binary>>
        }},
        request_json(delete, uri(["connectors", "foo"]), Config)
    ),

    %% Deleting a non-existing connector should result in an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(delete, uri(["connectors", ConnectorID]), Config)
    ),

    %% Create non working connector
    %% BrokenURL = ?URL(Port + 1, "/foo"),
    %% {ok, 201, BrokenConnector} = request(
    %%     post,
    %%     uri(["connectors"]),
    %%     ?HTTP_CONNECTOR(BrokenURL, Name),
    %%     fun json/1,
    %%     Config
    %% ),
    %% ?assertMatch(
    %%     #{
    %%         <<"type">> := ?CONNECTOR_TYPE_HTTP,
    %%         <<"name">> := Name,
    %%         <<"enable">> := true,
    %%         <<"status">> := <<"disconnected">>,
    %%         <<"status_reason">> := <<"Connection refused">>,
    %%         <<"node_status">> := [
    %%             #{
    %%                 <<"status">> := <<"disconnected">>,
    %%                 <<"status_reason">> := <<"Connection refused">>
    %%             }
    %%             | _
    %%         ],
    %%         <<"url">> := BrokenURL
    %%     },
    %%     BrokenConnector
    %% ),

    %% {ok, 200, FixedConnector} = request_json(
    %%     put,
    %%     uri(["connectors", ConnectorID]),
    %%     ?HTTP_CONNECTOR(URL1),
    %%     Config
    %% ),
    %% ?assertMatch(
    %%     #{
    %%         <<"status">> := <<"connected">>,
    %%         <<"node_status">> := [FixedNodeStatus = #{<<"status">> := <<"connected">>} | _]
    %%     } when
    %%         not is_map_key(<<"status_reason">>, FixedConnector) andalso
    %%             not is_map_key(<<"status_reason">>, FixedNodeStatus),
    %%     FixedConnector
    %% ),

    %% %% Try create connector with bad characters as name
    %% {ok, 400, _} = request(post, uri(["connectors"]), ?HTTP_CONNECTOR(URL1, <<"隋达"/utf8>>), Config),

    %% %% Missing scheme in URL
    %% {ok, 400, _} = request(
    %%     post,
    %%     uri(["connectors"]),
    %%     ?HTTP_CONNECTOR(<<"localhost:1234/foo">>, <<"missing_url_scheme">>),
    %%     Config
    %% ),

    %% %% Invalid port
    %% {ok, 400, _} = request(
    %%     post,
    %%     uri(["connectors"]),
    %%     ?HTTP_CONNECTOR(<<"http://localhost:12341234/foo">>, <<"invalid_port">>),
    %%     Config
    %% ),

    %% {ok, 204, <<>>} = request(delete, uri(["connectors", ConnectorID]), Config)
    ok.

%% t_start_bridge_unknown_node(Config) ->
%%     {ok, 404, _} =
%%         request(
%%             post,
%%             uri(["nodes", "thisbetterbenotanatomyet", "bridges", "webhook:foo", start]),
%%             Config
%%         ),
%%     {ok, 404, _} =
%%         request(
%%             post,
%%             uri(["nodes", "undefined", "bridges", "webhook:foo", start]),
%%             Config
%%         ).

%% t_start_stop_bridges_node(Config) ->
%%     do_start_stop_bridges(node, Config).

%% t_start_stop_bridges_cluster(Config) ->
%%     do_start_stop_bridges(cluster, Config).

%% do_start_stop_bridges(Type, Config) ->
%%     %% assert we there's no bridges at first
%%     {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

%%     Port = ?config(port, Config),
%%     URL1 = ?URL(Port, "abc"),
%%     Name = atom_to_binary(Type),
%%     ?assertMatch(
%%         {ok, 201, #{
%%             <<"type">> := ?BRIDGE_TYPE_HTTP,
%%             <<"name">> := Name,
%%             <<"enable">> := true,
%%             <<"status">> := <<"connected">>,
%%             <<"node_status">> := [_ | _],
%%             <<"url">> := URL1
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges"]),
%%             ?HTTP_BRIDGE(URL1, Name),
%%             Config
%%         )
%%     ),

%%     BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
%%     ExpectedStatus =
%%         case ?config(group, Config) of
%%             cluster when Type == node ->
%%                 <<"inconsistent">>;
%%             _ ->
%%                 <<"stopped">>
%%         end,

%%     %% stop it
%%     {ok, 204, <<>>} = request(post, {operation, Type, stop, BridgeID}, Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := ExpectedStatus}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% start again
%%     {ok, 204, <<>>} = request(post, {operation, Type, start, BridgeID}, Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% start a started bridge
%%     {ok, 204, <<>>} = request(post, {operation, Type, start, BridgeID}, Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% restart an already started bridge
%%     {ok, 204, <<>>} = request(post, {operation, Type, restart, BridgeID}, Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% stop it again
%%     {ok, 204, <<>>} = request(post, {operation, Type, stop, BridgeID}, Config),
%%     %% restart a stopped bridge
%%     {ok, 204, <<>>} = request(post, {operation, Type, restart, BridgeID}, Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),

%%     {ok, 404, _} = request(post, {operation, Type, invalidop, BridgeID}, Config),

%%     %% delete the bridge
%%     {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
%%     {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

%%     %% Fail parse-id check
%%     {ok, 404, _} = request(post, {operation, Type, start, <<"wreckbook_fugazi">>}, Config),
%%     %% Looks ok but doesn't exist
%%     {ok, 404, _} = request(post, {operation, Type, start, <<"webhook:cptn_hook">>}, Config),

%%     %% Create broken bridge
%%     {ListenPort, Sock} = listen_on_random_port(),
%%     %% Connecting to this endpoint should always timeout
%%     BadServer = iolist_to_binary(io_lib:format("localhost:~B", [ListenPort])),
%%     BadName = <<"bad_", (atom_to_binary(Type))/binary>>,
%%     ?assertMatch(
%%         {ok, 201, #{
%%             <<"type">> := ?BRIDGE_TYPE_MQTT,
%%             <<"name">> := BadName,
%%             <<"enable">> := true,
%%             <<"server">> := BadServer,
%%             <<"status">> := <<"connecting">>,
%%             <<"node_status">> := [_ | _]
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges"]),
%%             ?MQTT_BRIDGE(BadServer, BadName),
%%             Config
%%         )
%%     ),
%%     BadBridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_MQTT, BadName),
%%     ?assertMatch(
%%         %% request from product: return 400 on such errors
%%         {ok, SC, _} when SC == 500 orelse SC == 400,
%%         request(post, {operation, Type, start, BadBridgeID}, Config)
%%     ),
%%     ok = gen_tcp:close(Sock),
%%     ok.

%% t_start_stop_inconsistent_bridge_node(Config) ->
%%     start_stop_inconsistent_bridge(node, Config).

%% t_start_stop_inconsistent_bridge_cluster(Config) ->
%%     start_stop_inconsistent_bridge(cluster, Config).

%% start_stop_inconsistent_bridge(Type, Config) ->
%%     Port = ?config(port, Config),
%%     URL = ?URL(Port, "abc"),
%%     Node = ?config(node, Config),

%%     erpc:call(Node, fun() ->
%%         meck:new(emqx_bridge_resource, [passthrough, no_link]),
%%         meck:expect(
%%             emqx_bridge_resource,
%%             stop,
%%             fun
%%                 (_, <<"bridge_not_found">>) -> {error, not_found};
%%                 (BridgeType, Name) -> meck:passthrough([BridgeType, Name])
%%             end
%%         )
%%     end),

%%     emqx_common_test_helpers:on_exit(fun() ->
%%         erpc:call(Node, fun() ->
%%             meck:unload([emqx_bridge_resource])
%%         end)
%%     end),

%%     {ok, 201, _Bridge} = request(
%%         post,
%%         uri(["bridges"]),
%%         ?HTTP_BRIDGE(URL, <<"bridge_not_found">>),
%%         Config
%%     ),
%%     {ok, 503, _} = request(
%%         post, {operation, Type, stop, <<"webhook:bridge_not_found">>}, Config
%%     ).

%% t_enable_disable_bridges(Config) ->
%%     %% assert we there's no bridges at first
%%     {ok, 200, []} = request_json(get, uri(["bridges"]), Config),

%%     Name = ?BRIDGE_NAME,
%%     Port = ?config(port, Config),
%%     URL1 = ?URL(Port, "abc"),
%%     ?assertMatch(
%%         {ok, 201, #{
%%             <<"type">> := ?BRIDGE_TYPE_HTTP,
%%             <<"name">> := Name,
%%             <<"enable">> := true,
%%             <<"status">> := <<"connected">>,
%%             <<"node_status">> := [_ | _],
%%             <<"url">> := URL1
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges"]),
%%             ?HTTP_BRIDGE(URL1, Name),
%%             Config
%%         )
%%     ),
%%     BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE_HTTP, Name),
%%     %% disable it
%%     {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"stopped">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% enable again
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% enable an already started bridge
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% disable it again
%%     {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), Config),

%%     %% bad param
%%     {ok, 404, _} = request(put, enable_path(foo, BridgeID), Config),
%%     {ok, 404, _} = request(put, enable_path(true, "foo"), Config),
%%     {ok, 404, _} = request(put, enable_path(true, "webhook:foo"), Config),

%%     {ok, 400, Res} = request(post, {operation, node, start, BridgeID}, <<>>, fun json/1, Config),
%%     ?assertEqual(
%%         #{
%%             <<"code">> => <<"BAD_REQUEST">>,
%%             <<"message">> => <<"Forbidden operation, bridge not enabled">>
%%         },
%%         Res
%%     ),
%%     {ok, 400, Res} = request(post, {operation, cluster, start, BridgeID}, <<>>, fun json/1, Config),

%%     %% enable a stopped bridge
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri(["bridges", BridgeID]), Config)
%%     ),
%%     %% delete the bridge
%%     {ok, 204, <<>>} = request(delete, uri(["bridges", BridgeID]), Config),
%%     {ok, 200, []} = request_json(get, uri(["bridges"]), Config).

%% t_with_redact_update(Config) ->
%%     Name = <<"redact_update">>,
%%     Type = <<"mqtt">>,
%%     Password = <<"123456">>,
%%     Template = #{
%%         <<"type">> => Type,
%%         <<"name">> => Name,
%%         <<"server">> => <<"127.0.0.1:1883">>,
%%         <<"username">> => <<"test">>,
%%         <<"password">> => Password,
%%         <<"ingress">> =>
%%             #{<<"remote">> => #{<<"topic">> => <<"t/#">>}}
%%     },

%%     {ok, 201, _} = request(
%%         post,
%%         uri(["bridges"]),
%%         Template,
%%         Config
%%     ),

%%     %% update with redacted config
%%     BridgeConf = emqx_utils:redact(Template),
%%     BridgeID = emqx_bridge_resource:bridge_id(Type, Name),
%%     {ok, 200, _} = request(put, uri(["bridges", BridgeID]), BridgeConf, Config),
%%     ?assertEqual(
%%         Password,
%%         get_raw_config([bridges, Type, Name, password], Config)
%%     ),
%%     ok.

%% t_bridges_probe(Config) ->
%%     Port = ?config(port, Config),
%%     URL = ?URL(Port, "some_path"),

%%     {ok, 204, <<>>} = request(
%%         post,
%%         uri(["bridges_probe"]),
%%         ?HTTP_BRIDGE(URL),
%%         Config
%%     ),

%%     %% second time with same name is ok since no real bridge created
%%     {ok, 204, <<>>} = request(
%%         post,
%%         uri(["bridges_probe"]),
%%         ?HTTP_BRIDGE(URL),
%%         Config
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := _
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?HTTP_BRIDGE(<<"http://203.0.113.3:1234/foo">>),
%%             Config
%%         )
%%     ),

%%     %% Missing scheme in URL
%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := _
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?HTTP_BRIDGE(<<"203.0.113.3:1234/foo">>),
%%             Config
%%         )
%%     ),

%%     %% Invalid port
%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := _
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?HTTP_BRIDGE(<<"http://203.0.113.3:12341234/foo">>),
%%             Config
%%         )
%%     ),

%%     {ok, 204, _} = request(
%%         post,
%%         uri(["bridges_probe"]),
%%         ?MQTT_BRIDGE(<<"127.0.0.1:1883">>),
%%         Config
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := <<"Connection refused">>
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?MQTT_BRIDGE(<<"127.0.0.1:2883">>),
%%             Config
%%         )
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := <<"Could not resolve host">>
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?MQTT_BRIDGE(<<"nohost:2883">>),
%%             Config
%%         )
%%     ),

%%     AuthnConfig = #{
%%         <<"mechanism">> => <<"password_based">>,
%%         <<"backend">> => <<"built_in_database">>,
%%         <<"user_id_type">> => <<"username">>
%%     },
%%     Chain = 'mqtt:global',
%%     {ok, _} = update_config(
%%         [authentication],
%%         {create_authenticator, Chain, AuthnConfig},
%%         Config
%%     ),
%%     User = #{user_id => <<"u">>, password => <<"p">>},
%%     AuthenticatorID = <<"password_based:built_in_database">>,
%%     {ok, _} = add_user_auth(
%%         Chain,
%%         AuthenticatorID,
%%         User,
%%         Config
%%     ),

%%     emqx_common_test_helpers:on_exit(fun() ->
%%         delete_user_auth(Chain, AuthenticatorID, User, Config)
%%     end),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := <<"Unauthorized client">>
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{<<"proto_ver">> => <<"v4">>},
%%             Config
%%         )
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := <<"Bad username or password">>
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?MQTT_BRIDGE(<<"127.0.0.1:1883">>)#{
%%                 <<"proto_ver">> => <<"v4">>,
%%                 <<"password">> => <<"mySecret">>,
%%                 <<"username">> => <<"u">>
%%             },
%%             Config
%%         )
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{
%%             <<"code">> := <<"TEST_FAILED">>,
%%             <<"message">> := <<"Not authorized">>
%%         }},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?MQTT_BRIDGE(<<"127.0.0.1:1883">>),
%%             Config
%%         )
%%     ),

%%     ?assertMatch(
%%         {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
%%         request_json(
%%             post,
%%             uri(["bridges_probe"]),
%%             ?BRIDGE(<<"bad_bridge">>, <<"unknown_type">>),
%%             Config
%%         )
%%     ),
%%     ok.

%%% helpers
listen_on_random_port() ->
    SockOpts = [binary, {active, false}, {packet, raw}, {reuseaddr, true}, {backlog, 1000}],
    case gen_tcp:listen(0, SockOpts) of
        {ok, Sock} ->
            {ok, Port} = inet:port(Sock),
            {Port, Sock};
        {error, Reason} when Reason /= eaddrinuse ->
            {error, Reason}
    end.

request(Method, URL, Config) ->
    request(Method, URL, [], Config).

request(Method, {operation, Type, Op, BridgeID}, Body, Config) ->
    URL = operation_path(Type, Op, BridgeID, Config),
    request(Method, URL, Body, Config);
request(Method, URL, Body, Config) ->
    AuthHeader = emqx_common_test_http:auth_header(?config(api, Config)),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], AuthHeader, Body, Opts).

request(Method, URL, Body, Decoder, Config) ->
    case request(Method, URL, Body, Config) of
        {ok, Code, Response} ->
            {ok, Code, Decoder(Response)};
        Otherwise ->
            Otherwise
    end.

request_json(Method, URLLike, Config) ->
    request(Method, URLLike, [], fun json/1, Config).

request_json(Method, URLLike, Body, Config) ->
    request(Method, URLLike, Body, fun json/1, Config).

operation_path(node, Oper, BridgeID, Config) ->
    uri(["nodes", ?config(node, Config), "bridges", BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID, _Config) ->
    uri(["bridges", BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri(["bridges", BridgeID, "enable", Enable]).

publish_message(Topic, Body, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, publish, [emqx_message:make(Topic, Body)]).

update_config(Path, Value, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, update_config, [Path, Value]).

get_raw_config(Path, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx, get_raw_config, [Path]).

add_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx_authentication, add_user, [Chain, AuthenticatorID, User]).

delete_user_auth(Chain, AuthenticatorID, User, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, emqx_authentication, delete_user, [Chain, AuthenticatorID, User]).

str(S) when is_list(S) -> S;
str(S) when is_binary(S) -> binary_to_list(S).

json(B) when is_binary(B) ->
    emqx_utils_json:decode(B, [return_maps]).
