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

-module(emqx_bridge_v2_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(ROOT, "actions").

-define(CONNECTOR_NAME, <<"my_connector">>).

-define(RESOURCE(NAME, TYPE), #{
    <<"enable">> => true,
    %<<"ssl">> => #{<<"enable">> => false},
    <<"type">> => TYPE,
    <<"name">> => NAME
}).

-define(CONNECTOR_TYPE_STR, "kafka_producer").
-define(CONNECTOR_TYPE, <<?CONNECTOR_TYPE_STR>>).
-define(KAFKA_BOOTSTRAP_HOST, <<"127.0.0.1:9092">>).
-define(KAFKA_CONNECTOR(Name, BootstrapHosts), ?RESOURCE(Name, ?CONNECTOR_TYPE)#{
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

-define(CONNECTOR(Name), ?KAFKA_CONNECTOR(Name, ?KAFKA_BOOTSTRAP_HOST)).
-define(CONNECTOR, ?CONNECTOR(?CONNECTOR_NAME)).

-define(MQTT_LOCAL_TOPIC, <<"mqtt/local/topic">>).
-define(BRIDGE_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(BRIDGE_TYPE_STR, "kafka_producer").
-define(BRIDGE_TYPE, <<?BRIDGE_TYPE_STR>>).
-define(KAFKA_BRIDGE(Name, Connector), ?RESOURCE(Name, ?BRIDGE_TYPE)#{
    <<"connector">> => Connector,
    <<"kafka">> => #{
        <<"buffer">> => #{
            <<"memory_overload_protection">> => true,
            <<"mode">> => <<"hybrid">>,
            <<"per_partition_limit">> => <<"2GB">>,
            <<"segment_bytes">> => <<"100MB">>
        },
        <<"compression">> => <<"no_compression">>,
        <<"kafka_ext_headers">> => [
            #{
                <<"kafka_ext_header_key">> => <<"clientid">>,
                <<"kafka_ext_header_value">> => <<"${clientid}">>
            },
            #{
                <<"kafka_ext_header_key">> => <<"topic">>,
                <<"kafka_ext_header_value">> => <<"${topic}">>
            }
        ],
        <<"kafka_header_value_encode_mode">> => <<"none">>,
        <<"kafka_headers">> => <<"${pub_props}">>,
        <<"max_batch_bytes">> => <<"896KB">>,
        <<"max_inflight">> => 10,
        <<"message">> => #{
            <<"key">> => <<"${.clientid}">>,
            <<"timestamp">> => <<"${.timestamp}">>,
            <<"value">> => <<"${.}">>
        },
        <<"partition_count_refresh_interval">> => <<"60s">>,
        <<"partition_strategy">> => <<"random">>,
        <<"required_acks">> => <<"all_isr">>,
        <<"topic">> => <<"kafka-topic">>
    },
    <<"local_topic">> => ?MQTT_LOCAL_TOPIC,
    <<"resource_opts">> => #{
        <<"health_check_interval">> => <<"32s">>
    }
}).
-define(KAFKA_BRIDGE(Name), ?KAFKA_BRIDGE(Name, ?CONNECTOR_NAME)).

-define(KAFKA_BRIDGE_UPDATE(Name, Connector),
    maps:without([<<"name">>, <<"type">>], ?KAFKA_BRIDGE(Name, Connector))
).
-define(KAFKA_BRIDGE_UPDATE(Name), ?KAFKA_BRIDGE_UPDATE(Name, ?CONNECTOR_NAME)).

-define(APPSPECS, [
    emqx_conf,
    emqx,
    emqx_auth,
    emqx_management,
    emqx_connector,
    {emqx_bridge, "actions {}"},
    {emqx_rule_engine, "rule_engine { rules {} }"}
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

-if(?EMQX_RELEASE_EDITION == ee).
%% For now we got only kafka implementing `bridge_v2` and that is enterprise only.
all() ->
    [
        {group, single},
        {group, cluster_later_join},
        {group, cluster}
    ].
-else.
all() ->
    [].
-endif.

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    SingleOnlyTests = [
        t_bridges_probe,
        t_broken_bridge_config,
        t_fix_broken_bridge_config
    ],
    ClusterLaterJoinOnlyTCs = [
        t_cluster_later_join_metrics
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
    Node = ?config(node, Config),
    {ok, ApiKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    [{api_key, ApiKey} | Config].

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [?APPSPEC_DASHBOARD],
    Node2Apps = ?APPSPECS,
    emqx_cth_cluster:start(
        [
            {emqx_bridge_v2_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_bridge_v2_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Name, Config)}
    ).

end_per_group(Group, Config) when
    Group =:= cluster;
    Group =:= cluster_later_join
->
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(_, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config)),
    ok.

init_per_testcase(t_action_types, Config) ->
    case ?config(cluster_nodes, Config) of
        undefined ->
            init_mocks();
        Nodes ->
            [erpc:call(Node, ?MODULE, init_mocks, []) || Node <- Nodes]
    end,
    Config;
init_per_testcase(_TestCase, Config) ->
    case ?config(cluster_nodes, Config) of
        undefined ->
            init_mocks();
        Nodes ->
            [erpc:call(Node, ?MODULE, init_mocks, []) || Node <- Nodes]
    end,
    {ok, 201, _} = request(post, uri(["connectors"]), ?CONNECTOR, Config),
    Config.

end_per_testcase(_TestCase, Config) ->
    Node = ?config(node, Config),
    ok = erpc:call(Node, fun clear_resources/0),
    case ?config(cluster_nodes, Config) of
        undefined ->
            meck:unload();
        ClusterNodes ->
            [erpc:call(ClusterNode, meck, unload, []) || ClusterNode <- ClusterNodes]
    end,
    ok = emqx_common_test_helpers:call_janitor(),
    ok.

-define(CONNECTOR_IMPL, emqx_bridge_v2_dummy_connector).
init_mocks() ->
    case emqx_release:edition() of
        ee ->
            meck:new(emqx_connector_ee_schema, [passthrough, no_link]),
            meck:expect(emqx_connector_ee_schema, resource_type, 1, ?CONNECTOR_IMPL),
            ok;
        ce ->
            ok
    end,
    meck:new(?CONNECTOR_IMPL, [non_strict, no_link]),
    meck:expect(?CONNECTOR_IMPL, callback_mode, 0, async_if_possible),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_start,
        fun
            (<<"connector:", ?CONNECTOR_TYPE_STR, ":bad_", _/binary>>, _C) ->
                {ok, bad_connector_state};
            (_I, _C) ->
                {ok, connector_state}
        end
    ),
    meck:expect(?CONNECTOR_IMPL, on_stop, 2, ok),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_get_status,
        fun
            (_, bad_connector_state) -> connecting;
            (_, _) -> connected
        end
    ),
    meck:expect(?CONNECTOR_IMPL, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR_IMPL, on_remove_channel, 3, {ok, connector_state}),
    meck:expect(?CONNECTOR_IMPL, on_get_channel_status, 3, connected),
    ok = meck:expect(?CONNECTOR_IMPL, on_get_channels, fun(ResId) ->
        emqx_bridge_v2:get_channels_for_connector(ResId)
    end),
    ok.

clear_resources() ->
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            ok = emqx_bridge_v2:remove(Type, Name)
        end,
        emqx_bridge_v2:list()
    ),
    lists:foreach(
        fun(#{type := Type, name := Name}) ->
            ok = emqx_connector:remove(Type, Name)
        end,
        emqx_connector:list()
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

%% We have to pretend testing a kafka bridge since at this point that's the
%% only one that's implemented.

t_bridges_lifecycle(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    {ok, 404, _} = request(get, uri([?ROOT, "foo"]), Config),
    {ok, 404, _} = request(get, uri([?ROOT, "kafka_producer:foo"]), Config),

    %% need a var for patterns below
    BridgeName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"connector">> := ?CONNECTOR_NAME,
            <<"kafka">> := #{},
            <<"local_topic">> := _,
            <<"resource_opts">> := _
        }},
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),

    %% list all bridges, assert bridge is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?BRIDGE_TYPE,
                <<"name">> := BridgeName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri([?ROOT]), Config)
    ),

    %% list all bridges, assert bridge is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?BRIDGE_TYPE,
                <<"name">> := BridgeName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri([?ROOT]), Config)
    ),

    %% get the bridge by id
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(get, uri([?ROOT, BridgeID]), Config)
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := _
        }},
        request_json(post, uri([?ROOT, BridgeID, "brababbel"]), Config)
    ),

    %% update bridge config
    {ok, 201, _} = request(post, uri(["connectors"]), ?CONNECTOR(<<"foobla">>), Config),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := BridgeName,
            <<"connector">> := <<"foobla">>,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            put,
            uri([?ROOT, BridgeID]),
            ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME, <<"foobla">>),
            Config
        )
    ),

    %% update bridge with unknown connector name
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Message1
    }} =
        request_json(
            put,
            uri([?ROOT, BridgeID]),
            ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME, <<"does_not_exist">>),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message1)
    ),

    %% update bridge with connector of wrong type
    {ok, 201, _} =
        request(
            post,
            uri(["connectors"]),
            (?CONNECTOR(<<"foobla2">>))#{
                <<"type">> => <<"azure_event_hub_producer">>,
                <<"authentication">> => #{
                    <<"username">> => <<"emqxuser">>,
                    <<"password">> => <<"topSecret">>,
                    <<"mechanism">> => <<"plain">>
                },
                <<"ssl">> => #{
                    <<"enable">> => true,
                    <<"server_name_indication">> => <<"auto">>,
                    <<"verify">> => <<"verify_none">>,
                    <<"versions">> => [<<"tlsv1.3">>, <<"tlsv1.2">>]
                }
            },
            Config
        ),
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Message2
    }} =
        request_json(
            put,
            uri([?ROOT, BridgeID]),
            ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME, <<"foobla2">>),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message2)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri([?ROOT, BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    %% try create with unknown connector name
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Message3
    }} =
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME, <<"does_not_exist">>),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message3)
    ),

    %% try create bridge with connector of wrong type
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Message4
    }} =
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME, <<"foobla2">>),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message4)
    ),

    %% make sure nothing has been created above
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    %% update a deleted bridge returns an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(
            put,
            uri([?ROOT, BridgeID]),
            ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME),
            Config
        )
    ),

    %% deleting a non-existing bridge should result in an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(delete, uri([?ROOT, BridgeID]), Config)
    ),

    %% try delete unknown bridge id
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid bridge ID", _/binary>>
        }},
        request_json(delete, uri([?ROOT, "foo"]), Config)
    ),

    %% Try create bridge with bad characters as name
    {ok, 400, _} = request(post, uri([?ROOT]), ?KAFKA_BRIDGE(<<"隋达"/utf8>>), Config),
    {ok, 400, _} = request(post, uri([?ROOT]), ?KAFKA_BRIDGE(<<"a.b">>), Config),
    ok.

t_broken_bridge_config(Config) ->
    emqx_cth_suite:stop_apps([emqx_bridge]),
    BridgeName = ?BRIDGE_NAME,
    StartOps =
        #{
            config =>
                "actions {\n"
                "  "
                ?BRIDGE_TYPE_STR
                " {\n"
                "    " ++ binary_to_list(BridgeName) ++
                " {\n"
                "      connector = does_not_exist\n"
                "      enable = true\n"
                "      kafka {\n"
                "        topic = test-topic-one-partition\n"
                "      }\n"
                "      local_topic = \"mqtt/local/topic\"\n"
                "      resource_opts {health_check_interval = 32s}\n"
                "    }\n"
                "  }\n"
                "}\n"
                "\n",
            schema_mod => emqx_bridge_v2_schema
        },
    emqx_cth_suite:start_app(emqx_bridge, StartOps),

    ?assertMatch(
        {ok, 200, [
            #{
                <<"name">> := BridgeName,
                <<"type">> := ?BRIDGE_TYPE,
                <<"connector">> := <<"does_not_exist">>,
                <<"status">> := <<"disconnected">>,
                <<"error">> := <<"Not installed">>
            }
        ]},
        request_json(get, uri([?ROOT]), Config)
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    ?assertEqual(
        {ok, 204, <<>>},
        request(delete, uri([?ROOT, BridgeID]), Config)
    ),

    ?assertEqual(
        {ok, 200, []},
        request_json(get, uri([?ROOT]), Config)
    ),

    ok.

t_fix_broken_bridge_config(Config) ->
    emqx_cth_suite:stop_apps([emqx_bridge]),
    BridgeName = ?BRIDGE_NAME,
    StartOps =
        #{
            config =>
                "actions {\n"
                "  "
                ?BRIDGE_TYPE_STR
                " {\n"
                "    " ++ binary_to_list(BridgeName) ++
                " {\n"
                "      connector = does_not_exist\n"
                "      enable = true\n"
                "      kafka {\n"
                "        topic = test-topic-one-partition\n"
                "      }\n"
                "      local_topic = \"mqtt/local/topic\"\n"
                "      resource_opts {health_check_interval = 32s}\n"
                "    }\n"
                "  }\n"
                "}\n"
                "\n",
            schema_mod => emqx_bridge_v2_schema
        },
    emqx_cth_suite:start_app(emqx_bridge, StartOps),

    ?assertMatch(
        {ok, 200, [
            #{
                <<"name">> := BridgeName,
                <<"type">> := ?BRIDGE_TYPE,
                <<"connector">> := <<"does_not_exist">>,
                <<"status">> := <<"disconnected">>,
                <<"error">> := <<"Not installed">>
            }
        ]},
        request_json(get, uri([?ROOT]), Config)
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    request_json(
        put,
        uri([?ROOT, BridgeID]),
        ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME, ?CONNECTOR_NAME),
        Config
    ),

    ?assertMatch(
        {ok, 200, #{
            <<"connector">> := ?CONNECTOR_NAME,
            <<"status">> := <<"connected">>
        }},
        request_json(get, uri([?ROOT, BridgeID]), Config)
    ),

    ok.

t_start_bridge_unknown_node(Config) ->
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "thisbetterbenotanatomyet", ?ROOT, "kafka_producer:foo", start]),
            Config
        ),
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "undefined", ?ROOT, "kafka_producer:foo", start]),
            Config
        ).

t_start_bridge_node(Config) ->
    do_start_bridge(node, Config).

t_start_bridge_cluster(Config) ->
    do_start_bridge(cluster, Config).

do_start_bridge(TestType, Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    Name = atom_to_binary(TestType),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(Name),
            Config
        )
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),

    %% start again
    {ok, 204, <<>>} = request(post, {operation, TestType, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri([?ROOT, BridgeID]), Config)
    ),
    %% start a started bridge
    {ok, 204, <<>>} = request(post, {operation, TestType, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri([?ROOT, BridgeID]), Config)
    ),

    {ok, 400, _} = request(post, {operation, TestType, invalidop, BridgeID}, Config),

    %% Make start bridge fail
    expect_on_all_nodes(
        ?CONNECTOR_IMPL,
        on_add_channel,
        fun(_, _, _ResId, _Channel) -> {error, <<"my_error">>} end,
        Config
    ),

    connector_operation(Config, ?BRIDGE_TYPE, ?CONNECTOR_NAME, stop),
    connector_operation(Config, ?BRIDGE_TYPE, ?CONNECTOR_NAME, start),

    {ok, 400, _} = request(post, {operation, TestType, start, BridgeID}, Config),

    %% Make start bridge succeed

    expect_on_all_nodes(
        ?CONNECTOR_IMPL,
        on_add_channel,
        fun(_, _, _ResId, _Channel) -> {ok, connector_state} end,
        Config
    ),

    %% try to start again
    {ok, 204, <<>>} = request(post, {operation, TestType, start, BridgeID}, Config),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri([?ROOT, BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    %% Fail parse-id check
    {ok, 404, _} = request(post, {operation, TestType, start, <<"wreckbook_fugazi">>}, Config),
    %% Looks ok but doesn't exist
    {ok, 404, _} = request(post, {operation, TestType, start, <<"webhook:cptn_hook">>}, Config),
    ok.

expect_on_all_nodes(Mod, Function, Fun, Config) ->
    case ?config(cluster_nodes, Config) of
        undefined ->
            ok = meck:expect(Mod, Function, Fun);
        Nodes ->
            [erpc:call(Node, meck, expect, [Mod, Function, Fun]) || Node <- Nodes]
    end,
    ok.

connector_operation(Config, ConnectorType, ConnectorName, OperationName) ->
    case ?config(group, Config) of
        cluster ->
            case ?config(cluster_nodes, Config) of
                undefined ->
                    Node = ?config(node, Config),
                    ok = rpc:call(
                        Node,
                        emqx_connector_resource,
                        OperationName,
                        [ConnectorType, ConnectorName],
                        500
                    );
                Nodes ->
                    erpc:multicall(
                        Nodes,
                        emqx_connector_resource,
                        OperationName,
                        [ConnectorType, ConnectorName],
                        500
                    )
            end;
        _ ->
            ok = emqx_connector_resource:OperationName(ConnectorType, ConnectorName)
    end.

%% t_start_stop_inconsistent_bridge_node(Config) ->
%%     start_stop_inconsistent_bridge(node, Config).

%% t_start_stop_inconsistent_bridge_cluster(Config) ->
%%     start_stop_inconsistent_bridge(cluster, Config).

%% start_stop_inconsistent_bridge(Type, Config) ->
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
%%         uri([?ROOT]),
%%         ?KAFKA_BRIDGE(<<"bridge_not_found">>),
%%         Config
%%     ),
%%     {ok, 503, _} = request(
%%         post, {operation, Type, stop, <<"kafka:bridge_not_found">>}, Config
%%     ).

%% [TODO] This is a mess, need to clarify what the actual behavior needs to be
%% like.
%% t_enable_disable_bridges(Config) ->
%%     %% assert we there's no bridges at first
%%     {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

%%     Name = ?BRIDGE_NAME,
%%     ?assertMatch(
%%         {ok, 201, #{
%%             <<"type">> := ?BRIDGE_TYPE,
%%             <<"name">> := Name,
%%             <<"enable">> := true,
%%             <<"status">> := <<"connected">>,
%%             <<"node_status">> := [_ | _]
%%         }},
%%         request_json(
%%             post,
%%             uri([?ROOT]),
%%             ?KAFKA_BRIDGE(Name),
%%             Config
%%         )
%%     ),
%%     BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
%%     %% disable it
%%     meck:expect(?CONNECTOR_IMPL, on_get_channel_status, 3, connecting),
%%     {ok, 204, <<>>} = request(put, enable_path(false, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"stopped">>}},
%%         request_json(get, uri([?ROOT, BridgeID]), Config)
%%     ),
%%     %% enable again
%%     meck:expect(?CONNECTOR_IMPL, on_get_channel_status, 3, connected),
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri([?ROOT, BridgeID]), Config)
%%     ),
%%     %% enable an already started bridge
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri([?ROOT, BridgeID]), Config)
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
%%     {ok, 400, Res} = request(
%%         post, {operation, cluster, start, BridgeID}, <<>>, fun json/1, Config
%%     ),

%%     %% enable a stopped bridge
%%     {ok, 204, <<>>} = request(put, enable_path(true, BridgeID), Config),
%%     ?assertMatch(
%%         {ok, 200, #{<<"status">> := <<"connected">>}},
%%         request_json(get, uri([?ROOT, BridgeID]), Config)
%%     ),
%%     %% delete the bridge
%%     {ok, 204, <<>>} = request(delete, uri([?ROOT, BridgeID]), Config),
%%     {ok, 200, []} = request_json(get, uri([?ROOT]), Config).

t_bridges_probe(Config) ->
    {ok, 204, <<>>} = request(
        post,
        uri(["actions_probe"]),
        ?KAFKA_BRIDGE(?BRIDGE_NAME),
        Config
    ),

    %% second time with same name is ok since no real bridge created
    {ok, 204, <<>>} = request(
        post,
        uri(["actions_probe"]),
        ?KAFKA_BRIDGE(?BRIDGE_NAME),
        Config
    ),

    meck:expect(?CONNECTOR_IMPL, on_start, 2, {error, on_start_error}),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"TEST_FAILED">>,
            <<"message">> := _
        }},
        request_json(
            post,
            uri(["actions_probe"]),
            ?KAFKA_BRIDGE(<<"broken_bridge">>, <<"brokenhost:1234">>),
            Config
        )
    ),

    meck:expect(?CONNECTOR_IMPL, on_start, 2, {ok, bridge_state}),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            post,
            uri(["actions_probe"]),
            ?RESOURCE(<<"broken_bridge">>, <<"unknown_type">>),
            Config
        )
    ),
    ok.

t_cascade_delete_actions(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),
    %% then we add a a bridge, using POST
    %% POST /actions/ will create a bridge
    BridgeID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ?BRIDGE_NAME),
    {ok, 201, _} = request(
        post,
        uri([?ROOT]),
        ?KAFKA_BRIDGE(?BRIDGE_NAME),
        Config
    ),
    {ok, 201, #{<<"id">> := RuleId}} = request_json(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        },
        Config
    ),
    %% delete the bridge will also delete the actions from the rules
    {ok, 204, _} = request(
        delete,
        uri([?ROOT, BridgeID]) ++ "?also_delete_dep_actions=true",
        Config
    ),
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),
    ?assertMatch(
        {ok, 200, #{<<"actions">> := []}},
        request_json(get, uri(["rules", RuleId]), Config)
    ),
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), Config),

    {ok, 201, _} = request(
        post,
        uri([?ROOT]),
        ?KAFKA_BRIDGE(?BRIDGE_NAME),
        Config
    ),
    {ok, 201, _} = request(
        post,
        uri(["rules"]),
        #{
            <<"name">> => <<"t_http_crud_apis">>,
            <<"enable">> => true,
            <<"actions">> => [BridgeID],
            <<"sql">> => <<"SELECT * from \"t\"">>
        },
        Config
    ),
    {ok, 400, _} = request(
        delete,
        uri([?ROOT, BridgeID]),
        Config
    ),
    {ok, 200, [_]} = request_json(get, uri([?ROOT]), Config),
    %% Cleanup
    {ok, 204, _} = request(
        delete,
        uri([?ROOT, BridgeID]) ++ "?also_delete_dep_actions=true",
        Config
    ),
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config).

t_action_types(Config) ->
    Res = request_json(get, uri(["action_types"]), Config),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Types} = Res,
    ?assert(is_list(Types), #{types => Types}),
    ?assert(lists:all(fun is_binary/1, Types), #{types => Types}),
    ok.

t_bad_name(Config) ->
    Name = <<"_bad_name">>,
    Res = request_json(
        post,
        uri([?ROOT]),
        ?KAFKA_BRIDGE(Name),
        Config
    ),
    ?assertMatch({ok, 400, #{<<"message">> := _}}, Res),
    {ok, 400, #{<<"message">> := Msg0}} = Res,
    Msg = emqx_utils_json:decode(Msg0, [return_maps]),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"Invalid name format.", _/binary>>
        },
        Msg
    ),
    ok.

t_metrics(Config) ->
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    ActionName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),

    ActionID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ActionName),

    ?assertMatch(
        {ok, 200, #{
            <<"metrics">> := #{<<"matched">> := 0},
            <<"node_metrics">> := [#{<<"metrics">> := #{<<"matched">> := 0}} | _]
        }},
        request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
    ),

    {ok, 200, Bridge} = request_json(get, uri([?ROOT, ActionID]), Config),
    ?assertNot(maps:is_key(<<"metrics">>, Bridge)),
    ?assertNot(maps:is_key(<<"node_metrics">>, Bridge)),

    Body = <<"my msg">>,
    _ = publish_message(?MQTT_LOCAL_TOPIC, Body, Config),

    %% check for non-empty bridge metrics
    ?retry(
        _Sleep0 = 200,
        _Retries0 = 20,
        ?assertMatch(
            {ok, 200, #{
                <<"metrics">> := #{<<"matched">> := 1},
                <<"node_metrics">> := [#{<<"metrics">> := #{<<"matched">> := 1}} | _]
            }},
            request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
        )
    ),

    %% check for absence of metrics when listing all bridges
    {ok, 200, Bridges} = request_json(get, uri([?ROOT]), Config),
    ?assertNotMatch(
        [
            #{
                <<"metrics">> := #{},
                <<"node_metrics">> := [_ | _]
            }
        ],
        Bridges
    ),
    ok.

t_reset_metrics(Config) ->
    %% assert there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ROOT]), Config),

    ActionName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri([?ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),
    ActionID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ActionName),

    Body = <<"my msg">>,
    _ = publish_message(?MQTT_LOCAL_TOPIC, Body, Config),
    ?retry(
        _Sleep0 = 200,
        _Retries0 = 20,
        ?assertMatch(
            {ok, 200, #{
                <<"metrics">> := #{<<"matched">> := 1},
                <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
            }},
            request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
        )
    ),

    {ok, 204, <<>>} = request(put, uri([?ROOT, ActionID, "metrics", "reset"]), Config),

    ?retry(
        _Sleep0 = 200,
        _Retries0 = 20,
        ?assertMatch(
            {ok, 200, #{
                <<"metrics">> := #{<<"matched">> := 0},
                <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
            }},
            request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
        )
    ),

    ok.

t_cluster_later_join_metrics(Config) ->
    [PrimaryNode, OtherNode | _] = ?config(cluster_nodes, Config),
    Name = ?BRIDGE_NAME,
    ActionParams = ?KAFKA_BRIDGE(Name),
    ActionID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, Name),
    ?check_trace(
        begin
            %% Create a bridge on only one of the nodes.
            ?assertMatch({ok, 201, _}, request_json(post, uri([?ROOT]), ActionParams, Config)),
            %% Pre-condition.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
                }},
                request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
            ),
            %% Now join the other node join with the api node.
            ok = erpc:call(OtherNode, ekka, join, [PrimaryNode]),
            %% Check metrics; shouldn't crash even if the bridge is not
            %% ready on the node that just joined the cluster.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}}, #{<<"metrics">> := #{}} | _]
                }},
                request_json(get, uri([?ROOT, ActionID, "metrics"]), Config)
            ),
            ok
        end,
        []
    ),
    ok.

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
    AuthHeader = emqx_common_test_http:auth_header(?config(api_key, Config)),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], AuthHeader, Body, Opts).

request(Method, URL, Body, Decoder, Config) ->
    case request(Method, URL, Body, Config) of
        {ok, Code, Response} ->
            case Decoder(Response) of
                {error, _} = Error -> Error;
                Decoded -> {ok, Code, Decoded}
            end;
        Otherwise ->
            Otherwise
    end.

request_json(Method, URLLike, Config) ->
    request(Method, URLLike, [], fun json/1, Config).

request_json(Method, URLLike, Body, Config) ->
    request(Method, URLLike, Body, fun json/1, Config).

operation_path(node, Oper, BridgeID, Config) ->
    uri(["nodes", ?config(node, Config), ?ROOT, BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID, _Config) ->
    uri([?ROOT, BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri([?ROOT, BridgeID, "enable", Enable]).

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
    case emqx_utils_json:safe_decode(B, [return_maps]) of
        {ok, Term} ->
            Term;
        {error, Reason} = Error ->
            ct:pal("Failed to decode json: ~p~n~p", [Reason, B]),
            Error
    end.
