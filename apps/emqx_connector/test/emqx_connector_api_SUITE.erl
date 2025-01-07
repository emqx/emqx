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

-module(emqx_connector_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).
-import(emqx_common_test_helpers, [on_exit/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx_resource/include/emqx_resource.hrl").

-define(CONNECTOR_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(RESOURCE(NAME, TYPE), #{
    %<<"ssl">> => #{<<"enable">> => false},
    <<"type">> => TYPE,
    <<"name">> => NAME
}).

-define(CONNECTOR_TYPE_STR, "kafka_producer").
-define(CONNECTOR_TYPE, <<?CONNECTOR_TYPE_STR>>).
-define(KAFKA_BOOTSTRAP_HOST, <<"127.0.0.1:9092">>).
-define(KAFKA_CONNECTOR_BASE(BootstrapHosts), #{
    <<"authentication">> => <<"none">>,
    <<"bootstrap_hosts">> => BootstrapHosts,
    <<"connect_timeout">> => <<"5s">>,
    <<"enable">> => true,
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
-define(KAFKA_CONNECTOR_BASE, ?KAFKA_CONNECTOR_BASE(?KAFKA_BOOTSTRAP_HOST)).
-define(KAFKA_CONNECTOR(Name, BootstrapHosts),
    maps:merge(
        ?RESOURCE(Name, ?CONNECTOR_TYPE),
        ?KAFKA_CONNECTOR_BASE(BootstrapHosts)
    )
).
-define(KAFKA_CONNECTOR(Name), ?KAFKA_CONNECTOR(Name, ?KAFKA_BOOTSTRAP_HOST)).

-define(BRIDGE_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(BRIDGE_TYPE_STR, "kafka_producer").
-define(BRIDGE_TYPE, <<?BRIDGE_TYPE_STR>>).
-define(KAFKA_BRIDGE(Name, Connector), ?RESOURCE(Name, ?BRIDGE_TYPE)#{
    <<"enable">> => true,
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
        <<"max_linger_time">> => <<"1ms">>,
        <<"max_linger_bytes">> => <<"1MB">>,
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
    <<"local_topic">> => <<"mqtt/local/topic">>,
    <<"resource_opts">> => #{
        <<"health_check_interval">> => <<"32s">>
    }
}).
-define(KAFKA_BRIDGE(Name), ?KAFKA_BRIDGE(Name, ?CONNECTOR_NAME)).

-define(APPSPECS, [
    emqx_conf,
    emqx,
    emqx_auth,
    emqx_connector,
    emqx_bridge,
    emqx_rule_engine,
    emqx_management
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

-if(?EMQX_RELEASE_EDITION == ee).
%% For now we got only kafka_producer implementing `bridge_v2` and that is enterprise only.
all() ->
    [
        {group, single},
        %{group, cluster_later_join},
        {group, cluster}
    ].
-else.
all() ->
    [].
-endif.

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    SingleOnlyTests = [
        t_connectors_probe,
        t_fail_delete_with_action,
        t_actions_field
    ],
    ClusterOnlyTests = [
        t_inconsistent_state
    ],
    ClusterLaterJoinOnlyTCs = [
        % t_cluster_later_join_metrics
    ],
    [
        {single, [], (AllTCs -- ClusterLaterJoinOnlyTCs) -- ClusterOnlyTests},
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
%% init_per_group(cluster_later_join = Name, Config) ->
%%     Nodes = [NodePrimary | _] = mk_cluster(Name, Config, #{join_to => undefined}),
%%     init_api([{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config]);
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
            {emqx_connector_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_connector_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
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
    case ?config(cluster_nodes, Config) of
        undefined ->
            init_mocks(TestCase);
        Nodes ->
            [erpc:call(Node, ?MODULE, init_mocks, [TestCase]) || Node <- Nodes]
    end,
    Config.

end_per_testcase(TestCase, Config) ->
    Node = ?config(node, Config),
    ok = erpc:call(Node, ?MODULE, clear_resources, [TestCase]),
    case ?config(cluster_nodes, Config) of
        undefined ->
            meck:unload();
        Nodes ->
            [erpc:call(N, meck, unload, []) || N <- Nodes]
    end,
    ok = emqx_common_test_helpers:call_janitor(),
    ok.

-define(CONNECTOR_IMPL, dummy_connector_impl).
init_mocks(_TestCase) ->
    meck:new(emqx_connector_resource, [passthrough, no_link]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR_IMPL),
    meck:new(?CONNECTOR_IMPL, [non_strict, no_link]),
    meck:expect(?CONNECTOR_IMPL, resource_type, 0, dummy),
    meck:expect(?CONNECTOR_IMPL, callback_mode, 0, async_if_possible),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_start,
        fun
            (<<"connector:", ?CONNECTOR_TYPE_STR, ":bad_", _/binary>>, _C) ->
                {ok, bad_connector_state};
            (_I, #{bootstrap_hosts := <<"nope:9092">>}) ->
                {ok, worst_connector_state};
            (_I, _C) ->
                {ok, connector_state}
        end
    ),
    meck:expect(?CONNECTOR_IMPL, on_stop, 2, ok),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_get_status,
        fun
            (_, bad_connector_state) ->
                connecting;
            (_, worst_connector_state) ->
                {?status_disconnected, [
                    #{
                        host => <<"nope:9092">>,
                        reason => unresolvable_hostname
                    }
                ]};
            (_, _) ->
                connected
        end
    ),
    meck:expect(?CONNECTOR_IMPL, on_add_channel, 4, {ok, connector_state}),
    meck:expect(?CONNECTOR_IMPL, on_remove_channel, 3, {ok, connector_state}),
    meck:expect(?CONNECTOR_IMPL, on_get_channel_status, 3, connected),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_get_channels,
        fun(ResId) ->
            emqx_bridge_v2:get_channels_for_connector(ResId)
        end
    ),
    [?CONNECTOR_IMPL, emqx_connector_resource].

clear_resources(_) ->
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

%% We have to pretend testing a kafka_producer connector since at this point that's the
%% only one that's implemented.

t_connectors_lifecycle(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    {ok, 404, _} = request(get, uri(["connectors", "foo"]), Config),
    {ok, 404, _} = request(get, uri(["connectors", "kafka_producer:foo"]), Config),

    %% need a var for patterns below
    ConnectorName = ?CONNECTOR_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := ConnectorName,
            <<"enable">> := true,
            <<"bootstrap_hosts">> := _,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
            Config
        )
    ),

    %% list all connectors, assert Connector is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?CONNECTOR_TYPE,
                <<"name">> := ConnectorName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri(["connectors"]), Config)
    ),

    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, ?CONNECTOR_NAME),

    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := ConnectorName,
            <<"bootstrap_hosts">> := <<"foobla:1234">>,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            put,
            uri(["connectors", ConnectorID]),
            ?KAFKA_CONNECTOR_BASE(<<"foobla:1234">>),
            Config
        )
    ),

    %% list all connectors, assert Connector is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := ?CONNECTOR_TYPE,
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
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := ConnectorName,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := _
        }},
        request_json(post, uri(["connectors", ConnectorID, "brababbel"]), Config)
    ),

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
            ?KAFKA_CONNECTOR_BASE,
            Config
        )
    ),

    %% Deleting a non-existing connector should result in an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(delete, uri(["connectors", ConnectorID]), Config)
    ),

    %% try delete unknown connector id
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid connector ID", _/binary>>
        }},
        request_json(delete, uri(["connectors", "foo"]), Config)
    ),

    %% Try create connector with bad characters as name
    {ok, 400, _} = request(post, uri(["connectors"]), ?KAFKA_CONNECTOR(<<"隋达"/utf8>>), Config),
    ok.

t_start_connector_unknown_node(Config) ->
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "thisbetterbenotanatomyet", "connectors", "kafka_producer:foo", start]),
            Config
        ),
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "undefined", "connectors", "kafka_producer:foo", start]),
            Config
        ).

t_start_connector_node(Config) ->
    do_start_connector(node, Config).

t_start_connector_cluster(Config) ->
    do_start_connector(cluster, Config).

do_start_connector(TestType, Config) ->
    %% assert we there's no connectors at first
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    Name = atom_to_binary(TestType),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(Name),
            Config
        )
    ),

    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, Name),

    %% Starting a healthy connector shouldn't do any harm
    {ok, 204, <<>>} = request(post, {operation, TestType, start, ConnectorID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),

    ExpectedStatus =
        case ?config(group, Config) of
            cluster when TestType == node ->
                <<"inconsistent">>;
            _ ->
                <<"stopped">>
        end,

    %% stop it
    case ?config(group, Config) of
        cluster ->
            case TestType of
                node ->
                    Node = ?config(node, Config),
                    ok = rpc:call(
                        Node, emqx_connector_resource, stop, [?CONNECTOR_TYPE, Name], 500
                    );
                cluster ->
                    Nodes = ?config(cluster_nodes, Config),
                    [{ok, ok}, {ok, ok}] = erpc:multicall(
                        Nodes, emqx_connector_resource, stop, [?CONNECTOR_TYPE, Name], 500
                    )
            end;
        _ ->
            ok = emqx_connector_resource:stop(?CONNECTOR_TYPE, Name)
    end,
    ?assertMatch(
        {ok, 200, #{<<"status">> := ExpectedStatus}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),
    %% start again
    {ok, 204, <<>>} = request(post, {operation, TestType, start, ConnectorID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),

    %% test invalid op
    {ok, 400, _} = request(post, {operation, TestType, invalidop, ConnectorID}, Config),

    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnectorID]), Config),
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    %% Fail parse-id check
    {ok, 404, _} = request(post, {operation, TestType, start, <<"wreckbook_fugazi">>}, Config),
    %% Looks ok but doesn't exist
    {ok, 404, _} = request(post, {operation, TestType, start, <<"webhook:cptn_hook">>}, Config),

    %% Create broken connector
    {ListenPort, Sock} = listen_on_random_port(),
    %% Connecting to this endpoint should always timeout
    BadServer = iolist_to_binary(io_lib:format("localhost:~B", [ListenPort])),
    BadName = <<"bad_", (atom_to_binary(TestType))/binary>>,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := BadName,
            <<"enable">> := true,
            <<"bootstrap_hosts">> := BadServer,
            <<"status">> := <<"connecting">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            (?KAFKA_CONNECTOR(BadName, BadServer))#{
                <<"resource_opts">> => #{
                    <<"start_timeout">> => <<"10ms">>
                }
            },
            Config
        )
    ),
    BadConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, BadName),
    %% Checks that an `emqx_resource_manager:start' timeout when waiting for the resource to
    %% be connected doesn't return a 500 error.
    ?assertMatch(
        %% request from product: return 400 on such errors
        {ok, 400, _},
        request(post, {operation, TestType, start, BadConnectorID}, Config)
    ),
    ok = gen_tcp:close(Sock),
    ok.

t_enable_disable_connectors(Config) ->
    %% assert we there's no connectors at first
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config),

    Name = ?CONNECTOR_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(Name),
            Config
        )
    ),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, Name),
    %% disable it
    {ok, 204, <<>>} = request(put, enable_path(false, ConnectorID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"stopped">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),
    %% enable again
    {ok, 204, <<>>} = request(put, enable_path(true, ConnectorID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),
    %% enable an already started connector
    {ok, 204, <<>>} = request(put, enable_path(true, ConnectorID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),
    %% disable it again
    {ok, 204, <<>>} = request(put, enable_path(false, ConnectorID), Config),

    %% bad param
    {ok, 400, _} = request(put, enable_path(foo, ConnectorID), Config),
    {ok, 404, _} = request(put, enable_path(true, "foo"), Config),
    {ok, 404, _} = request(put, enable_path(true, "webhook:foo"), Config),

    {ok, 400, Res} = request(post, {operation, node, start, ConnectorID}, <<>>, fun json/1, Config),
    ?assertEqual(
        #{
            <<"code">> => <<"BAD_REQUEST">>,
            <<"message">> => <<"Forbidden operation, connector not enabled">>
        },
        Res
    ),
    {ok, 400, Res} = request(
        post, {operation, cluster, start, ConnectorID}, <<>>, fun json/1, Config
    ),

    %% enable a stopped connector
    {ok, 204, <<>>} = request(put, enable_path(true, ConnectorID), Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri(["connectors", ConnectorID]), Config)
    ),
    %% delete the connector
    {ok, 204, <<>>} = request(delete, uri(["connectors", ConnectorID]), Config),
    {ok, 200, []} = request_json(get, uri(["connectors"]), Config).

t_with_redact_update(Config) ->
    Name = <<"redact_update">>,
    Password = <<"123456">>,
    Template = (?KAFKA_CONNECTOR(Name))#{
        <<"authentication">> => #{
            <<"mechanism">> => <<"plain">>,
            <<"username">> => <<"test">>,
            <<"password">> => Password
        }
    },

    {ok, 201, _} = request(
        post,
        uri(["connectors"]),
        Template,
        Config
    ),

    %% update with redacted config
    ConnectorUpdatedConf = maps:without([<<"name">>, <<"type">>], emqx_utils:redact(Template)),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, Name),
    {ok, 200, _} = request(put, uri(["connectors", ConnectorID]), ConnectorUpdatedConf, Config),
    ?assertEqual(
        Password,
        get_raw_config([connectors, ?CONNECTOR_TYPE, Name, authentication, password], Config)
    ),
    ok.

t_connectors_probe(Config) ->
    {ok, 204, <<>>} = request(
        post,
        uri(["connectors_probe"]),
        ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
        Config
    ),

    %% second time with same name is ok since no real connector created
    {ok, 204, <<>>} = request(
        post,
        uri(["connectors_probe"]),
        ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
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
            uri(["connectors_probe"]),
            ?KAFKA_CONNECTOR(<<"broken_connector">>, <<"brokenhost:1234">>),
            Config
        )
    ),

    meck:expect(?CONNECTOR_IMPL, on_start, 2, {ok, connector_state}),

    ?assertMatch(
        {ok, 400, #{<<"code">> := <<"BAD_REQUEST">>}},
        request_json(
            post,
            uri(["connectors_probe"]),
            ?RESOURCE(<<"broken_connector">>, <<"unknown_type">>),
            Config
        )
    ),
    ok.

t_create_with_bad_name(Config) ->
    ConnectorName = <<"test_哈哈"/utf8>>,
    Conf0 = ?KAFKA_CONNECTOR(ConnectorName),
    %% Note: must contain SSL options to trigger original bug.
    Cacertfile = emqx_common_test_helpers:app_path(
        emqx,
        filename:join(["etc", "certs", "cacert.pem"])
    ),
    Conf = Conf0#{<<"ssl">> => #{<<"cacertfile">> => Cacertfile}},
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Msg0
    }} = request_json(
        post,
        uri(["connectors"]),
        Conf,
        Config
    ),
    Msg = emqx_utils_json:decode(Msg0, [return_maps]),
    ?assertMatch(#{<<"kind">> := <<"validation_error">>}, Msg),
    ok.

%% Checks that we correctly handle `throw({bad_ssl_config, _})' from
%% `emqx_connector:convert_certs' and massage the error message accordingly.
t_create_with_bad_tls_files(Config) ->
    ConnectorName = atom_to_binary(?FUNCTION_NAME),
    Conf0 = ?KAFKA_CONNECTOR(ConnectorName),
    Conf = Conf0#{<<"ssl">> => #{<<"cacertfile">> => <<"bad_pem_file">>}},
    ?check_trace(
        begin
            {ok, 400, #{
                <<"message">> := Msg0
            }} = request_json(
                post,
                uri(["connectors"]),
                Conf,
                Config
            ),
            ?assertMatch(
                #{
                    <<"kind">> := <<"validation_error">>,
                    <<"reason">> := <<"bad_ssl_config">>,
                    <<"details">> :=
                        <<"Failed to access certificate / key file: No such file or directory">>,
                    <<"bad_field">> := <<"cacertfile">>
                },
                json(Msg0)
            ),
            ok
        end,
        []
    ),
    ok.

t_actions_field(Config) ->
    Name = ?CONNECTOR_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"actions">> := []
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(Name),
            Config
        )
    ),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, Name),
    BridgeName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"connector">> := Name,
            <<"parameters">> := #{},
            <<"local_topic">> := _,
            <<"resource_opts">> := _
        }},
        request_json(
            post,
            uri(["actions"]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"actions">> := [BridgeName]
        }},
        request_json(
            get,
            uri(["connectors", ConnectorID]),
            Config
        )
    ),
    ok.

t_fail_delete_with_action(Config) ->
    Name = ?CONNECTOR_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?CONNECTOR_TYPE,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri(["connectors"]),
            ?KAFKA_CONNECTOR(Name),
            Config
        )
    ),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, Name),
    BridgeName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := ?BRIDGE_TYPE,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"connector">> := Name,
            <<"parameters">> := #{},
            <<"local_topic">> := _,
            <<"resource_opts">> := _
        }},
        request_json(
            post,
            uri(["actions"]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),

    %% delete the connector
    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> :=
                <<"{<<\"Cannot delete connector while there are active channels",
                    " defined for this connector\">>,", _/binary>>
        }},
        request_json(delete, uri(["connectors", ConnectorID]), Config)
    ),
    ok.

t_list_disabled_channels(Config) ->
    ConnectorParams = ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri(["connectors"]),
            ConnectorParams,
            Config
        )
    ),
    ActionName = ?BRIDGE_NAME,
    ActionParams = (?KAFKA_BRIDGE(ActionName))#{<<"enable">> := false},
    ?assertMatch(
        {ok, 201, #{<<"enable">> := false}},
        request_json(
            post,
            uri(["actions"]),
            ActionParams,
            Config
        )
    ),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, ?CONNECTOR_NAME),
    ActionID = emqx_bridge_resource:bridge_id(?BRIDGE_TYPE, ActionName),
    ?assertMatch(
        {ok, 200, #{
            <<"status">> := <<"disconnected">>,
            <<"status_reason">> := <<"Not installed">>,
            <<"error">> := <<"Not installed">>
        }},
        request_json(
            get,
            uri(["actions", ActionID]),
            Config
        )
    ),
    %% This should be fast even if the connector resource process is unresponsive.
    ConnectorResID = emqx_connector_resource:resource_id(?CONNECTOR_TYPE, ?CONNECTOR_NAME),
    suspend_connector_resource(ConnectorResID, Config),
    try
        ?assertMatch(
            {ok, 200, #{<<"actions">> := [ActionName]}},
            request_json(
                get,
                uri(["connectors", ConnectorID]),
                Config
            )
        ),
        ok
    after
        resume_connector_resource(ConnectorResID, Config)
    end,
    ok.

t_raw_config_response_defaults(Config) ->
    Params = maps:without([<<"enable">>, <<"resource_opts">>], ?KAFKA_CONNECTOR(?CONNECTOR_NAME)),
    ?assertMatch(
        {ok, 201, #{<<"enable">> := true, <<"resource_opts">> := #{}}},
        request_json(
            post,
            uri(["connectors"]),
            Params,
            Config
        )
    ),
    ok.

t_inconsistent_state(Config) ->
    [_, Node2] = ?config(cluster_nodes, Config),
    Params = ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
    ?assertMatch(
        {ok, 201, #{<<"enable">> := true, <<"resource_opts">> := #{}}},
        request_json(
            post,
            uri(["connectors"]),
            Params,
            Config
        )
    ),
    BadParams = maps:without(
        [<<"name">>, <<"type">>],
        Params#{<<"bootstrap_hosts">> := <<"nope:9092">>}
    ),
    {ok, _} = erpc:call(
        Node2,
        emqx,
        update_config,
        [[connectors, ?CONNECTOR_TYPE, ?CONNECTOR_NAME], BadParams, #{}]
    ),

    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, ?CONNECTOR_NAME),
    ?assertMatch(
        {ok, 200, #{
            <<"status">> := <<"inconsistent">>,
            <<"node_status">> := [
                #{<<"status">> := <<"connected">>},
                #{
                    <<"status">> := <<"disconnected">>,
                    <<"status_reason">> := _
                }
            ],
            <<"status_reason">> := _
        }},
        request_json(
            get,
            uri(["connectors", ConnectorID]),
            Config
        )
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

operation_path(node, Oper, ConnectorID, Config) ->
    uri(["nodes", ?config(node, Config), "connectors", ConnectorID, Oper]);
operation_path(cluster, Oper, ConnectorID, _Config) ->
    uri(["connectors", ConnectorID, Oper]).

enable_path(Enable, ConnectorID) ->
    uri(["connectors", ConnectorID, "enable", Enable]).

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

suspend_connector_resource(ConnectorResID, Config) ->
    Node = ?config(node, Config),
    Pid = erpc:call(Node, fun() ->
        [Pid] = [
            Pid
         || {ID, Pid, worker, _} <- supervisor:which_children(emqx_resource_manager_sup),
            ID =:= ConnectorResID
        ],
        sys:suspend(Pid),
        Pid
    end),
    on_exit(fun() -> erpc:call(Node, fun() -> catch sys:resume(Pid) end) end),
    ok.

resume_connector_resource(ConnectorResID, Config) ->
    Node = ?config(node, Config),
    erpc:call(Node, fun() ->
        [Pid] = [
            Pid
         || {ID, Pid, worker, _} <- supervisor:which_children(emqx_resource_manager_sup),
            ID =:= ConnectorResID
        ],
        sys:resume(Pid),
        ok
    end),
    ok.
