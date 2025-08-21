%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_v2_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include_lib("emqx_resource/include/emqx_resource_runtime.hrl").
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(ACTIONS_ROOT, "actions").
-define(SOURCES_ROOT, "sources").

-define(ACTION_CONNECTOR_NAME, <<"my_connector">>).
-define(SOURCE_CONNECTOR_NAME, <<"my_connector">>).

-define(CONNECTOR_IMPL, emqx_bridge_v2_dummy_connector).

-define(RESOURCE(NAME, TYPE), #{
    <<"enable">> => true,
    %<<"ssl">> => #{<<"enable">> => false},
    <<"type">> => TYPE,
    <<"name">> => NAME
}).

-define(ACTION_CONNECTOR_TYPE_STR, "kafka_producer").
-define(ACTION_CONNECTOR_TYPE, <<?ACTION_CONNECTOR_TYPE_STR>>).
-define(KAFKA_BOOTSTRAP_HOST, <<"127.0.0.1:9092">>).
-define(KAFKA_CONNECTOR(Name, BootstrapHosts), ?RESOURCE(Name, ?ACTION_CONNECTOR_TYPE)#{
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

-define(ACTIONS_CONNECTOR(Name), ?KAFKA_CONNECTOR(Name, ?KAFKA_BOOTSTRAP_HOST)).
-define(ACTIONS_CONNECTOR, ?ACTIONS_CONNECTOR(?ACTION_CONNECTOR_NAME)).

-define(MQTT_LOCAL_TOPIC, <<"mqtt/local/topic">>).
-define(BRIDGE_NAME, (atom_to_binary(?FUNCTION_NAME))).
-define(ACTION_TYPE_STR, "kafka_producer").
-define(ACTION_TYPE, <<?ACTION_TYPE_STR>>).
-define(ACTION_TYPE_2, ?SOURCE_TYPE).
-define(KAFKA_BRIDGE(Name, Connector), ?RESOURCE(Name, ?ACTION_TYPE)#{
    <<"connector">> => Connector,
    <<"fallback_actions">> => [
        #{<<"kind">> => <<"republish">>, <<"args">> => #{<<"topic">> => <<Name/binary, "/fba">>}}
    ],
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
-define(KAFKA_BRIDGE(Name), ?KAFKA_BRIDGE(Name, ?ACTION_CONNECTOR_NAME)).

-define(KAFKA_BRIDGE_UPDATE(Name, Connector),
    maps:without([<<"name">>, <<"type">>], ?KAFKA_BRIDGE(Name, Connector))
).

-define(SOURCE_TYPE_STR, "mqtt").
-define(SOURCE_TYPE, <<?SOURCE_TYPE_STR>>).
-define(SOURCE_CONNECTOR_TYPE, ?SOURCE_TYPE).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.

matrix_cases() ->
    emqx_common_test_helpers:all(?MODULE).

groups() ->
    emqx_common_test_helpers:matrix_to_groups(?MODULE, matrix_cases()).

suite() ->
    [{timetrap, {seconds, 60}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Name, Config) ->
    {NodeSpecs, Nodes} = mk_cluster(Name, Config),
    [NodePrimary | _] = Nodes,
    [
        {group, Name},
        {cluster_nodes, Nodes},
        {node, NodePrimary},
        {node_specs, NodeSpecs}
        | Config
    ];
init_per_group(cluster_later_join = Name, Config) ->
    {NodeSpecs, Nodes} = mk_cluster(Name, Config, #{join_to => undefined}),
    [NodePrimary | _] = Nodes,
    Fun = fun() -> ?ON(NodePrimary, emqx_mgmt_api_test_util:auth_header_()) end,
    emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
    [
        {group, Name},
        {cluster_nodes, Nodes},
        {node, NodePrimary},
        {node_specs, NodeSpecs}
        | Config
    ];
init_per_group(single = Group, Config) ->
    WorkDir = work_dir_random_suffix(Group, Config),
    Apps = emqx_cth_suite:start(
        app_specs_without_dashboard() ++ [emqx_mgmt_api_test_util:emqx_dashboard()],
        #{work_dir => WorkDir}
    ),
    [{group, single}, {group_apps, Apps}, {node, node()} | Config];
init_per_group(actions, Config) ->
    [
        {bridge_kind, action},
        {connector_type, ?ACTION_CONNECTOR_TYPE},
        {connector_name, ?ACTION_CONNECTOR_NAME}
        | Config
    ];
init_per_group(sources, Config) ->
    [
        {bridge_kind, source},
        {connector_type, ?SOURCE_CONNECTOR_TYPE},
        {connector_name, ?SOURCE_CONNECTOR_NAME}
        | Config
    ];
init_per_group(_Group, Config) ->
    Config.

app_specs_without_dashboard() ->
    [
        emqx_conf,
        emqx,
        emqx_auth,
        emqx_management,
        emqx_connector,
        emqx_bridge_mqtt,
        emqx_bridge,
        emqx_rule_engine
    ].

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = app_specs_without_dashboard() ++ [emqx_mgmt_api_test_util:emqx_dashboard()],
    Node2Apps = app_specs_without_dashboard(),
    WorkDir = work_dir_random_suffix(Name, Config),
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {emqx_bridge_v2_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_bridge_v2_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => WorkDir}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    {NodeSpecs, Nodes}.

end_per_group(Group, Config) when
    Group =:= cluster;
    Group =:= cluster_later_join
->
    emqx_bridge_v2_testlib:clear_auth_header_getter(),
    ok = emqx_cth_cluster:stop(?config(cluster_nodes, Config));
end_per_group(single, Config) ->
    emqx_cth_suite:stop(?config(group_apps, Config)),
    ok;
end_per_group(_Group, _Config) ->
    ok.

init_per_testcase(TestCase, Config) when
    TestCase =:= t_start_action_or_source_with_disabled_connector;
    TestCase =:= t_action_types
->
    setup_auth_header_fn(Config),
    case ?config(cluster_nodes, Config) of
        undefined ->
            init_mocks();
        Nodes ->
            [erpc:call(Node, ?MODULE, init_mocks, []) || Node <- Nodes]
    end,
    #{
        connector_config := ConnectorConfig,
        bridge_type := BridgeType,
        bridge_name := BridgeName,
        bridge_config := BridgeConfig
    } =
        case ?config(bridge_kind, Config) of
            action ->
                #{
                    connector_config => ?ACTIONS_CONNECTOR,
                    bridge_type => {action_type, ?ACTION_TYPE},
                    bridge_name => {action_name, ?ACTION_CONNECTOR_NAME},
                    bridge_config => {action_config, ?KAFKA_BRIDGE(?ACTION_CONNECTOR_NAME)}
                };
            source ->
                #{
                    connector_config => source_connector_create_config(#{}),
                    bridge_type => {source_type, ?SOURCE_TYPE},
                    bridge_name => {source_name, ?SOURCE_CONNECTOR_NAME},
                    bridge_config => {source_config, source_config_base()}
                }
        end,
    [
        {connector_config, ConnectorConfig},
        BridgeType,
        BridgeName,
        BridgeConfig
        | Config
    ];
init_per_testcase(TestCase, Config) ->
    setup_auth_header_fn(Config),
    case ?config(cluster_nodes, Config) of
        undefined ->
            init_mocks();
        Nodes ->
            [erpc:call(Node, ?MODULE, init_mocks, []) || Node <- Nodes]
    end,
    ShouldCreateConnector = not lists:member(TestCase, skip_connector_creation_test_cases()),
    case ?config(bridge_kind, Config) of
        action when ShouldCreateConnector ->
            {ok, 201, _} = request(post, uri(["connectors"]), ?ACTIONS_CONNECTOR, Config);
        source when ShouldCreateConnector ->
            {ok, 201, _} = request(
                post,
                uri(["connectors"]),
                source_connector_create_config(#{}),
                Config
            );
        _ ->
            ok
    end,
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

skip_connector_creation_test_cases() ->
    [
        t_connector_dependencies,
        t_kind_dependencies,
        t_summary,
        t_summary_inconsistent,
        t_namespaced_crud,
        t_namespaced_load_on_restart
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

get_tcp_mqtt_port(Node) ->
    {_Host, Port} = erpc:call(Node, emqx_config, get, [[listeners, tcp, default, bind]]),
    Port.

work_dir_random_suffix(Name0, CTConfig) ->
    Name = iolist_to_binary([emqx_utils_conv:bin(Name0), emqx_utils:rand_id(5)]),
    WorkDir = emqx_cth_suite:work_dir(Name, CTConfig),
    binary_to_list(WorkDir).

setup_auth_header_fn(Config) ->
    case ?config(cluster_nodes, Config) of
        undefined ->
            ok;
        [N1 | _] ->
            Fun = fun() -> ?ON(N1, emqx_mgmt_api_test_util:auth_header_()) end,
            emqx_bridge_v2_testlib:set_auth_header_getter(Fun)
    end.

init_mocks() ->
    meck:new(emqx_connector_resource, [passthrough, no_link]),
    meck:expect(emqx_connector_resource, connector_to_resource_type, 1, ?CONNECTOR_IMPL),
    meck:new(?CONNECTOR_IMPL, [non_strict, no_link]),
    meck:expect(?CONNECTOR_IMPL, resource_type, 0, dummy),
    meck:expect(?CONNECTOR_IMPL, callback_mode, 0, async_if_possible),
    meck:expect(
        ?CONNECTOR_IMPL,
        on_start,
        fun
            (<<"connector:", ?ACTION_CONNECTOR_TYPE_STR, ":bad_", _/binary>>, _C) ->
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
    meck:expect(?CONNECTOR_IMPL, on_query_async, fun(_ResId, _Req, ReplyFunAndArgs, _ConnState) ->
        emqx_resource:apply_reply_fun(ReplyFunAndArgs, ok),
        {ok, self()}
    end),
    ok.

clear_mocks(TCConfig) ->
    case ?config(cluster_nodes, TCConfig) of
        undefined ->
            meck:unload();
        Nodes ->
            [erpc:call(Node, meck, unload, []) || Node <- Nodes]
    end.

clear_resources() ->
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors().

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
                        [?global_ns, ConnectorType, ConnectorName],
                        500
                    );
                Nodes ->
                    erpc:multicall(
                        Nodes,
                        emqx_connector_resource,
                        OperationName,
                        [?global_ns, ConnectorType, ConnectorName],
                        500
                    )
            end;
        _ ->
            ok = emqx_connector_resource:OperationName(?global_ns, ConnectorType, ConnectorName)
    end.

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
request(Method, URL, Body, _Config) ->
    AuthHeader = emqx_bridge_v2_testlib:auth_header(),
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
    [_SingleOrCluster, Kind | _] = group_path(Config),
    #{api_root_key := APIRootKey} = get_common_values(Kind, <<"unused">>),
    uri(["nodes", ?config(node, Config), APIRootKey, BridgeID, Oper]);
operation_path(cluster, Oper, BridgeID, Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    #{api_root_key := APIRootKey} = get_common_values(Kind, <<"unused">>),
    uri([APIRootKey, BridgeID, Oper]).

enable_path(Enable, BridgeID) ->
    uri([?ACTIONS_ROOT, BridgeID, "enable", Enable]).

publish_message(Topic, Body, Config) ->
    Node = ?config(node, Config),
    publish_message(Topic, Body, Node, Config).

publish_message(Topic, Body, Node, _Config) ->
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
    case emqx_utils_json:safe_decode(B) of
        {ok, Term} ->
            Term;
        {error, Reason} = Error ->
            ct:pal("Failed to decode json: ~p~n~p", [Reason, B]),
            Error
    end.

group_path(Config) ->
    group_path(Config, undefined).
group_path(Config, Default) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            Default;
        Path ->
            Path
    end.

source_connector_config_base() ->
    #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    }.

source_connector_create_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    Conf0 = maps:merge(
        source_connector_config_base(),
        #{
            <<"enable">> => true,
            <<"type">> => ?SOURCE_CONNECTOR_TYPE,
            <<"name">> => ?SOURCE_CONNECTOR_NAME
        }
    ),
    maps:merge(
        Conf0,
        Overrides
    ).

source_config_base() ->
    #{
        <<"enable">> => true,
        <<"connector">> => ?SOURCE_CONNECTOR_NAME,
        <<"parameters">> =>
            #{
                <<"topic">> => <<"remote/topic">>,
                <<"qos">> => 2
            },
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"resume_interval">> => <<"15s">>
        }
    }.

mqtt_action_config_base() ->
    source_config_base().

mqtt_action_create_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    Conf0 = maps:merge(
        mqtt_action_config_base(),
        #{
            <<"enable">> => true,
            <<"type">> => ?SOURCE_TYPE
        }
    ),
    emqx_utils_maps:deep_merge(
        Conf0,
        Overrides
    ).

source_create_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    Conf0 = maps:merge(
        source_config_base(),
        #{
            <<"enable">> => true,
            <<"type">> => ?SOURCE_TYPE
        }
    ),
    maps:merge(
        Conf0,
        Overrides
    ).

source_update_config(Overrides0) ->
    Overrides = emqx_utils_maps:binary_key_map(Overrides0),
    maps:merge(
        source_config_base(),
        Overrides
    ).

get_common_values(Kind, FnName) ->
    case Kind of
        actions ->
            #{
                api_root_key => ?ACTIONS_ROOT,
                type => ?ACTION_TYPE,
                default_connector_name => ?ACTION_CONNECTOR_NAME,
                create_config_fn =>
                    fun(Overrides) ->
                        Name = maps:get(name, Overrides, FnName),
                        ConnectorName = maps:get(connector, Overrides, ?ACTION_CONNECTOR_NAME),
                        ?KAFKA_BRIDGE(Name, ConnectorName)
                    end,
                update_config_fn =>
                    fun(Overrides) ->
                        Name = maps:get(name, Overrides, FnName),
                        ConnectorName = maps:get(connector, Overrides, ?ACTION_CONNECTOR_NAME),
                        ?KAFKA_BRIDGE_UPDATE(Name, ConnectorName)
                    end,
                create_connector_config_fn =>
                    fun(Overrides) ->
                        ConnectorName = maps:get(name, Overrides, ?ACTION_CONNECTOR_NAME),
                        ?ACTIONS_CONNECTOR(ConnectorName)
                    end
            };
        sources ->
            #{
                api_root_key => ?SOURCES_ROOT,
                type => ?SOURCE_TYPE,
                default_connector_name => ?SOURCE_CONNECTOR_NAME,
                create_config_fn => fun(Overrides0) ->
                    Overrides =
                        case Overrides0 of
                            #{name := _} -> Overrides0;
                            _ -> Overrides0#{name => FnName}
                        end,
                    source_create_config(Overrides)
                end,
                update_config_fn => fun source_update_config/1,
                create_connector_config_fn => fun source_connector_create_config/1
            }
    end.

maybe_get_other_node(Config) ->
    %% In the single node test group, this simply returns the lone node.  Otherwise, it'll
    %% return a node that's not the primary one that receives API calls.
    PrimaryNode = ?config(node, Config),
    case proplists:get_value(cluster_nodes, Config, []) -- [PrimaryNode] of
        [] ->
            PrimaryNode;
        [OtherNode | _] ->
            OtherNode
    end.

list_connectors_api() ->
    Res = emqx_bridge_v2_testlib:list_connectors_http_api(),
    emqx_mgmt_api_test_util:simplify_result(Res).

get_connector_api(Type, Name) ->
    Res = emqx_bridge_v2_testlib:get_connector_api(Type, Name),
    emqx_mgmt_api_test_util:simplify_result(Res).

get_source_api(Type, Name) ->
    Res = emqx_bridge_v2_testlib:get_bridge_api(source, Type, Name),
    emqx_mgmt_api_test_util:simplify_result(Res).

get_action_api(Type, Name) ->
    Res = emqx_bridge_v2_testlib:get_bridge_api(action, Type, Name),
    emqx_mgmt_api_test_util:simplify_result(Res).

create_source_api(Name, Type, Params) ->
    Res = emqx_bridge_v2_testlib:create_kind_api([
        {bridge_kind, source},
        {source_type, Type},
        {source_name, Name},
        {source_config, Params}
    ]),
    emqx_mgmt_api_test_util:simplify_result(Res).

create_action_api(Name, Type, Params) ->
    Res = emqx_bridge_v2_testlib:create_kind_api([
        {bridge_kind, action},
        {action_type, Type},
        {action_name, Name},
        {action_config, Params}
    ]),
    emqx_mgmt_api_test_util:simplify_result(Res).

update_action_api(Name, Type, Params) ->
    Res = emqx_bridge_v2_testlib:update_bridge_api([
        {bridge_kind, action},
        {action_type, Type},
        {action_name, Name},
        {action_config, Params}
    ]),
    emqx_mgmt_api_test_util:simplify_result(Res).

update_source_api(Name, Type, Params) ->
    Res = emqx_bridge_v2_testlib:update_bridge_api([
        {bridge_kind, source},
        {source_type, Type},
        {source_name, Name},
        {source_config, Params}
    ]),
    emqx_mgmt_api_test_util:simplify_result(Res).

list_sources_api() ->
    Res = emqx_bridge_v2_testlib:list_sources_http_api(),
    emqx_mgmt_api_test_util:simplify_result(Res).

list_actions_api() ->
    Res = emqx_bridge_v2_testlib:list_actions_http_api(),
    emqx_mgmt_api_test_util:simplify_result(Res).

create_action_rule(ActionType, ActionName) ->
    RuleTopic = <<"t/", ActionName/binary>>,
    Config = [{action_name, ActionName}],
    emqx_bridge_v2_testlib:create_rule_and_action_http(ActionType, RuleTopic, Config).

create_source_rule1(SourceType, SourceName) ->
    RuleTopic = <<"t/", SourceName/binary>>,
    Config = [{action_name, <<"unused">>}],
    Id = emqx_bridge_resource:bridge_id(SourceType, SourceName),
    Opts = #{
        overrides => #{
            sql => <<"select * from \"$bridges/", Id/binary, "\"">>,
            actions => []
        }
    },
    emqx_bridge_v2_testlib:create_rule_and_action_http(SourceType, RuleTopic, Config, Opts).

create_source_rule2(SourceType, SourceName) ->
    RuleTopic = <<"t/", SourceName/binary>>,
    Config = [{action_name, <<"unused">>}],
    Id = emqx_bridge_resource:bridge_id(SourceType, SourceName),
    Opts = #{
        overrides => #{
            sql => <<"select * from \"$sources/", Id/binary, "\"">>,
            actions => []
        }
    },
    emqx_bridge_v2_testlib:create_rule_and_action_http(SourceType, RuleTopic, Config, Opts).

summary_scenarios_setup(Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    Name = <<"summary">>,
    ConnectorName = <<"summary">>,
    case Kind of
        actions ->
            %% This particular connector type serves both MQTT actions and sources.
            ConnectorType = ?SOURCE_CONNECTOR_TYPE,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_config, source_connector_create_config(#{})},
                    {connector_name, ConnectorName},
                    {connector_type, ConnectorType}
                ]),
            Type = ?ACTION_TYPE_2,
            CreateConfig = mqtt_action_create_config(#{
                <<"connector">> => ConnectorName,
                <<"tags">> => [<<"tag1">>]
            }),
            {201, _} = create_action_api(Name, Type, CreateConfig),
            Summarize = fun emqx_bridge_v2_testlib:summarize_actions_api/0;
        sources ->
            ConnectorType = ?SOURCE_CONNECTOR_TYPE,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_config, source_connector_create_config(#{})},
                    {connector_name, ConnectorName},
                    {connector_type, ConnectorType}
                ]),
            Type = ?SOURCE_TYPE,
            CreateConfig = source_create_config(#{
                <<"connector">> => ConnectorName,
                <<"tags">> => [<<"tag1">>]
            }),
            {201, _} = create_source_api(Name, Type, CreateConfig),
            Summarize = fun emqx_bridge_v2_testlib:summarize_sources_api/0
    end,
    #{
        type => Type,
        name => Name,
        config => CreateConfig,
        summarize => Summarize
    }.

get_value(Key, TCConfig) ->
    emqx_bridge_v2_testlib:get_value(Key, TCConfig).

auth_header(TCConfig) ->
    maybe
        undefined ?= ?config(auth_header, TCConfig),
        emqx_bridge_v2_testlib:auth_header()
    end.

ensure_namespaced_api_key(Namespace, TCConfig) ->
    ensure_namespaced_api_key(Namespace, _Opts = #{}, TCConfig).
ensure_namespaced_api_key(Namespace, Opts0, TCConfig) ->
    Node = get_value(node, TCConfig),
    Opts = Opts0#{namespace => Namespace},
    ?ON(Node, emqx_bridge_v2_testlib:ensure_namespaced_api_key(Opts)).

get_matrix_prop(TCConfig, Alternatives, Default) ->
    GroupPath = group_path(TCConfig, [Default]),
    case lists:filter(fun(G) -> lists:member(G, Alternatives) end, GroupPath) of
        [] ->
            Default;
        [Opt] ->
            Opt
    end.

kind_of(TCConfig) ->
    case get_matrix_prop(TCConfig, [actions, sources], actions) of
        actions ->
            action;
        sources ->
            source
    end.

conf_root_key_of(TCConfig) ->
    case proplists:get_value(conf_root_key, TCConfig) of
        undefined ->
            atom_to_binary(get_matrix_prop(TCConfig, [actions, sources], actions));
        ConfRootKey ->
            atom_to_binary(ConfRootKey)
    end.

list(TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig)]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => URL,
            auth_header => AuthHeader
        })
    end).

summary(TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        URL =
            case kind_of(TCConfig) of
                action -> emqx_mgmt_api_test_util:api_path(["actions_summary"]);
                source -> emqx_mgmt_api_test_util:api_path(["sources_summary"])
            end,
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => URL,
            auth_header => AuthHeader
        })
    end).

create(Type, Name, Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig)]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => URL,
            body => Config#{
                <<"type">> => Type,
                <<"name">> => Name
            },
            auth_header => AuthHeader
        })
    end).

get(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => URL,
            auth_header => AuthHeader
        })
    end).

get_channel_id(Namespace, Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        ConfRootKey =
            case kind_of(TCConfig) of
                action -> actions;
                source -> sources
            end,
        {ok, {_ConnResId, ChanResId}} =
            emqx_bridge_v2:get_resource_ids(Namespace, ConfRootKey, Type, Name),
        ChanResId
    end).

get_runtime(ChanResId, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, emqx_resource_cache:get_runtime(ChanResId)).

get_runtime(Namespace, Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        ConfRootKey =
            case kind_of(TCConfig) of
                action -> actions;
                source -> sources
            end,
        {ok, {_ConnResId, ChanResId}} =
            emqx_bridge_v2:get_resource_ids(Namespace, ConfRootKey, Type, Name),
        emqx_resource_cache:get_runtime(ChanResId)
    end).

update(Type, Name, Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => URL,
            body => Config,
            auth_header => AuthHeader
        })
    end).

enable(Type, Name, TCConfig) ->
    do_enable_or_disable(Type, Name, "true", TCConfig).

disable(Type, Name, TCConfig) ->
    do_enable_or_disable(Type, Name, "false", TCConfig).

do_enable_or_disable(Type, Name, Enable, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        AuthHeader = auth_header(TCConfig),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id, "enable", Enable]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => URL,
            auth_header => AuthHeader
        })
    end).

start(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        AuthHeader = auth_header(TCConfig),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id, "start"]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => URL,
            auth_header => AuthHeader
        })
    end).

start_on_node(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    NodeBin = atom_to_binary(Node),
    ?ON(Node, begin
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        AuthHeader = auth_header(TCConfig),
        URL = emqx_mgmt_api_test_util:api_path([
            "nodes", NodeBin, conf_root_key_of(TCConfig), Id, "start"
        ]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => URL,
            auth_header => AuthHeader
        })
    end).

delete(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => delete,
            url => URL,
            auth_header => AuthHeader
        })
    end).

-doc """
NOTE: We should **not** call `emqx_bridge_v2:send_message` directly in tests, as that
makes potential refactoring harder.  The correct way is to create a rule referencing the
action and send MQTT messages to the rule topic.

At the time of writing, however, we're in the process of adding namespace support, and
rule engine still does not "understand" namespaces.  After rule engine is updated, this
helper should be replaced by creating a proper rule.
""".
send_message(Namespace, Type, Name, Message, QueryOpts, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(
        Node,
        emqx_bridge_v2:send_message(Namespace, Type, Name, Message, QueryOpts)
    ).

probe(Type, Name, Config, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        URL =
            case kind_of(TCConfig) of
                action -> emqx_mgmt_api_test_util:api_path(["actions_probe"]);
                source -> emqx_mgmt_api_test_util:api_path(["sources_probe"])
            end,
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => URL,
            body => Config#{<<"type">> => Type, <<"name">> => Name},
            auth_header => AuthHeader
        })
    end).

get_metrics(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id, "metrics"]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => URL,
            auth_header => AuthHeader
        })
    end).

reset_metrics(Type, Name, TCConfig) ->
    Node = get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        Id = emqx_bridge_resource:bridge_id(Type, Name),
        URL = emqx_mgmt_api_test_util:api_path([conf_root_key_of(TCConfig), Id, "metrics", "reset"]),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => URL,
            auth_header => AuthHeader
        })
    end).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

%% We have to pretend testing a kafka bridge since at this point that's the
%% only one that's implemented.

t_bridges_lifecycle(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_bridges_lifecycle(Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    FnName = atom_to_binary(?FUNCTION_NAME),
    #{
        api_root_key := APIRootKey,
        type := Type,
        default_connector_name := DefaultConnectorName,
        create_config_fn := CreateConfigFn,
        update_config_fn := UpdateConfigFn,
        create_connector_config_fn := CreateConnectorConfigFn
    } = get_common_values(Kind, FnName),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([APIRootKey]), Config),

    {ok, 404, _} = request(get, uri([APIRootKey, "foo"]), Config),
    {ok, 404, _} = request(get, uri([APIRootKey, "kafka_producer:foo"]), Config),

    %% need a var for patterns below
    BridgeName = FnName,
    CreateRes = request_json(
        post,
        uri([APIRootKey]),
        CreateConfigFn(#{}),
        Config
    ),
    ?assertMatch(
        {ok, 201, #{
            <<"type">> := Type,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _],
            <<"connector">> := DefaultConnectorName,
            <<"parameters">> := #{},
            <<"resource_opts">> := _
        }},
        CreateRes,
        #{name => BridgeName, type => Type, connector => DefaultConnectorName}
    ),
    case Kind of
        actions ->
            ?assertMatch({ok, 201, #{<<"local_topic">> := _}}, CreateRes);
        sources ->
            ok
    end,

    %% list all bridges, assert bridge is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := Type,
                <<"name">> := BridgeName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri([APIRootKey]), Config)
    ),

    %% list all bridges, assert bridge is in it
    ?assertMatch(
        {ok, 200, [
            #{
                <<"type">> := Type,
                <<"name">> := BridgeName,
                <<"enable">> := true,
                <<"status">> := _,
                <<"node_status">> := [_ | _]
            }
        ]},
        request_json(get, uri([APIRootKey]), Config)
    ),

    %% get the bridge by id
    BridgeID = emqx_bridge_resource:bridge_id(Type, ?BRIDGE_NAME),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := Type,
            <<"name">> := BridgeName,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(get, uri([APIRootKey, BridgeID]), Config)
    ),

    ?assertMatch(
        {ok, 400, #{
            <<"code">> := <<"BAD_REQUEST">>,
            <<"message">> := _
        }},
        request_json(post, uri([APIRootKey, BridgeID, "brababbel"]), Config)
    ),

    %% update bridge config
    {ok, 201, _} = request(
        post,
        uri(["connectors"]),
        CreateConnectorConfigFn(#{name => <<"foobla">>}),
        Config
    ),
    ?assertMatch(
        {ok, 200, #{
            <<"type">> := Type,
            <<"name">> := BridgeName,
            <<"connector">> := <<"foobla">>,
            <<"enable">> := true,
            <<"status">> := _,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            put,
            uri([APIRootKey, BridgeID]),
            UpdateConfigFn(#{connector => <<"foobla">>}),
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
            uri([APIRootKey, BridgeID]),
            UpdateConfigFn(#{connector => <<"does_not_exist">>}),
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
            (?ACTIONS_CONNECTOR(<<"foobla2">>))#{
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
            uri([APIRootKey, BridgeID]),
            UpdateConfigFn(#{connector => <<"foobla2">>}),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message2)
    ),

    %% delete the bridge
    {ok, 204, <<>>} = request(delete, uri([APIRootKey, BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri([APIRootKey]), Config),

    %% try create with unknown connector name
    {ok, 400, #{
        <<"code">> := <<"BAD_REQUEST">>,
        <<"message">> := Message3
    }} =
        request_json(
            post,
            uri([APIRootKey]),
            CreateConfigFn(#{connector => <<"does_not_exist">>}),
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
            uri([APIRootKey]),
            CreateConfigFn(#{connector => <<"foobla2">>}),
            Config
        ),
    ?assertMatch(
        #{<<"reason">> := <<"connector_not_found_or_wrong_type">>},
        emqx_utils_json:decode(Message4)
    ),

    %% make sure nothing has been created above
    {ok, 200, []} = request_json(get, uri([APIRootKey]), Config),

    %% update a deleted bridge returns an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(
            put,
            uri([APIRootKey, BridgeID]),
            UpdateConfigFn(#{}),
            Config
        )
    ),

    %% deleting a non-existing bridge should result in an error
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := _
        }},
        request_json(delete, uri([APIRootKey, BridgeID]), Config)
    ),

    %% try delete unknown bridge id
    ?assertMatch(
        {ok, 404, #{
            <<"code">> := <<"NOT_FOUND">>,
            <<"message">> := <<"Invalid bridge ID", _/binary>>
        }},
        request_json(delete, uri([APIRootKey, "foo"]), Config)
    ),

    %% Try create bridge with bad characters as name
    {ok, 400, _} = request(
        post, uri([APIRootKey]), CreateConfigFn(#{name => <<"隋达"/utf8>>}), Config
    ),
    {ok, 400, _} = request(post, uri([APIRootKey]), CreateConfigFn(#{name => <<"a.b">>}), Config),
    ok.

t_broken_bridge_config(matrix) ->
    [
        [single, actions]
    ];
t_broken_bridge_config(Config) ->
    emqx_cth_suite:stop_apps([emqx_bridge]),
    BridgeName = ?BRIDGE_NAME,
    StartOps =
        #{
            config =>
                "actions {\n"
                "  "
                ?ACTION_TYPE_STR
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
                <<"type">> := ?ACTION_TYPE,
                <<"connector">> := <<"does_not_exist">>,
                <<"status">> := <<"disconnected">>,
                <<"error">> := <<"Not installed">>
            }
        ]},
        request_json(get, uri([?ACTIONS_ROOT]), Config)
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, ?BRIDGE_NAME),
    ?assertEqual(
        {ok, 204, <<>>},
        request(delete, uri([?ACTIONS_ROOT, BridgeID]), Config)
    ),

    ?assertEqual(
        {ok, 200, []},
        request_json(get, uri([?ACTIONS_ROOT]), Config)
    ),

    ok.

t_fix_broken_bridge_config(matrix) ->
    [
        [single, actions]
    ];
t_fix_broken_bridge_config(Config) ->
    emqx_cth_suite:stop_apps([emqx_bridge]),
    BridgeName = ?BRIDGE_NAME,
    StartOps =
        #{
            config =>
                "actions {\n"
                "  "
                ?ACTION_TYPE_STR
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
                <<"type">> := ?ACTION_TYPE,
                <<"connector">> := <<"does_not_exist">>,
                <<"status">> := <<"disconnected">>,
                <<"error">> := <<"Not installed">>
            }
        ]},
        request_json(get, uri([?ACTIONS_ROOT]), Config)
    ),

    BridgeID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, ?BRIDGE_NAME),
    request_json(
        put,
        uri([?ACTIONS_ROOT, BridgeID]),
        ?KAFKA_BRIDGE_UPDATE(?BRIDGE_NAME, ?ACTION_CONNECTOR_NAME),
        Config
    ),

    ?assertMatch(
        {ok, 200, #{
            <<"connector">> := ?ACTION_CONNECTOR_NAME,
            <<"status">> := <<"connected">>
        }},
        request_json(get, uri([?ACTIONS_ROOT, BridgeID]), Config)
    ),

    ok.

t_start_bridge_unknown_node(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_start_bridge_unknown_node(Config) ->
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "thisbetterbenotanatomyet", ?ACTIONS_ROOT, "kafka_producer:foo", start]),
            Config
        ),
    {ok, 404, _} =
        request(
            post,
            uri(["nodes", "undefined", ?ACTIONS_ROOT, "kafka_producer:foo", start]),
            Config
        ).

t_start_bridge_node(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_start_bridge_node(Config) ->
    do_start_bridge(node, Config).

t_start_bridge_cluster(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_start_bridge_cluster(Config) ->
    do_start_bridge(cluster, Config).

do_start_bridge(TestType, Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    Name = atom_to_binary(TestType),
    #{
        api_root_key := APIRootKey,
        type := Type,
        default_connector_name := DefaultConnectorName,
        create_config_fn := CreateConfigFn
    } = get_common_values(Kind, Name),
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([APIRootKey]), Config),

    ?assertMatch(
        {ok, 201, #{
            <<"type">> := Type,
            <<"name">> := Name,
            <<"enable">> := true,
            <<"status">> := <<"connected">>,
            <<"node_status">> := [_ | _]
        }},
        request_json(
            post,
            uri([APIRootKey]),
            CreateConfigFn(#{name => Name}),
            Config
        )
    ),

    BridgeID = emqx_bridge_resource:bridge_id(Type, Name),

    %% start again
    {ok, 204, <<>>} = request(post, {operation, TestType, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri([APIRootKey, BridgeID]), Config)
    ),
    %% start a started bridge
    {ok, 204, <<>>} = request(post, {operation, TestType, start, BridgeID}, Config),
    ?assertMatch(
        {ok, 200, #{<<"status">> := <<"connected">>}},
        request_json(get, uri([APIRootKey, BridgeID]), Config)
    ),

    {ok, 400, _} = request(post, {operation, TestType, invalidop, BridgeID}, Config),

    %% Make start bridge fail
    expect_on_all_nodes(
        ?CONNECTOR_IMPL,
        on_add_channel,
        fun(_, _, _ResId, _Channel) -> {error, <<"my_error">>} end,
        Config
    ),

    connector_operation(Config, Type, DefaultConnectorName, stop),
    connector_operation(Config, Type, DefaultConnectorName, start),

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
    {ok, 204, <<>>} = request(delete, uri([APIRootKey, BridgeID]), Config),
    {ok, 200, []} = request_json(get, uri([APIRootKey]), Config),

    %% Fail parse-id check
    {ok, 404, _} = request(post, {operation, TestType, start, <<"wreckbook_fugazi">>}, Config),
    %% Looks ok but doesn't exist
    {ok, 404, _} = request(post, {operation, TestType, start, <<"webhook:cptn_hook">>}, Config),
    ok.

t_bridges_probe(matrix) ->
    [
        [single, actions]
    ];
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

t_cascade_delete_actions(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_cascade_delete_actions(Config) ->
    %% assert we there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ACTIONS_ROOT]), Config),
    %% then we add a a bridge, using POST
    %% POST /actions/ will create a bridge
    BridgeID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, ?BRIDGE_NAME),
    {ok, 201, _} = request(
        post,
        uri([?ACTIONS_ROOT]),
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
        uri([?ACTIONS_ROOT, BridgeID]) ++ "?also_delete_dep_actions=true",
        Config
    ),
    {ok, 200, []} = request_json(get, uri([?ACTIONS_ROOT]), Config),
    ?assertMatch(
        {ok, 200, #{<<"actions">> := []}},
        request_json(get, uri(["rules", RuleId]), Config)
    ),
    {ok, 204, <<>>} = request(delete, uri(["rules", RuleId]), Config),

    {ok, 201, _} = request(
        post,
        uri([?ACTIONS_ROOT]),
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
    {ok, 400, Body} = request(
        delete,
        uri([?ACTIONS_ROOT, BridgeID]),
        Config
    ),
    ?assertMatch(#{<<"rules">> := [_ | _]}, emqx_utils_json:decode(Body)),
    {ok, 200, [_]} = request_json(get, uri([?ACTIONS_ROOT]), Config),
    %% Cleanup
    {ok, 204, _} = request(
        delete,
        uri([?ACTIONS_ROOT, BridgeID]) ++ "?also_delete_dep_actions=true",
        Config
    ),
    {ok, 200, []} = request_json(get, uri([?ACTIONS_ROOT]), Config).

t_action_types(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_action_types(Config) ->
    Res = request_json(get, uri(["action_types"]), Config),
    ?assertMatch({ok, 200, _}, Res),
    {ok, 200, Types} = Res,
    ?assert(is_list(Types), #{types => Types}),
    ?assert(lists:all(fun is_binary/1, Types), #{types => Types}),
    ok.

t_bad_name(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_bad_name(Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    Name = <<"_bad_name">>,
    #{
        api_root_key := APIRootKey,
        create_config_fn := CreateConfigFn
    } = get_common_values(Kind, Name),
    Res = request_json(
        post,
        uri([APIRootKey]),
        CreateConfigFn(#{}),
        Config
    ),
    ?assertMatch({ok, 400, #{<<"message">> := _}}, Res),
    {ok, 400, #{<<"message">> := Msg0}} = Res,
    Msg = emqx_utils_json:decode(Msg0),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := <<"Invalid name format.", _/binary>>
        },
        Msg
    ),
    ok.

t_metrics(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_metrics(Config) ->
    {ok, 200, []} = request_json(get, uri([?ACTIONS_ROOT]), Config),

    ActionName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri([?ACTIONS_ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),

    ActionID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, ActionName),

    ?assertMatch(
        {ok, 200, #{
            <<"metrics">> := #{
                <<"matched">> := 0,
                <<"queuing_bytes">> := 0
            },
            <<"node_metrics">> := [
                #{
                    <<"metrics">> := #{
                        <<"matched">> := 0,
                        <<"queuing_bytes">> := 0
                    }
                }
                | _
            ]
        }},
        request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config)
    ),

    {ok, 200, Bridge} = request_json(get, uri([?ACTIONS_ROOT, ActionID]), Config),
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
            request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config)
        )
    ),

    %% check for absence of metrics when listing all bridges
    {ok, 200, Bridges} = request_json(get, uri([?ACTIONS_ROOT]), Config),
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

t_reset_metrics(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_reset_metrics(Config) ->
    %% assert there's no bridges at first
    {ok, 200, []} = request_json(get, uri([?ACTIONS_ROOT]), Config),

    ActionName = ?BRIDGE_NAME,
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri([?ACTIONS_ROOT]),
            ?KAFKA_BRIDGE(?BRIDGE_NAME),
            Config
        )
    ),
    ActionID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, ActionName),

    Body = <<"my msg">>,
    OtherNode = maybe_get_other_node(Config),
    _ = publish_message(?MQTT_LOCAL_TOPIC, Body, OtherNode, Config),
    ?retry(
        _Sleep0 = 200,
        _Retries0 = 20,
        ?assertMatch(
            {ok, 200, #{
                <<"metrics">> := #{<<"matched">> := 1},
                <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
            }},
            request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config)
        )
    ),

    {ok, 204, <<>>} = request(put, uri([?ACTIONS_ROOT, ActionID, "metrics", "reset"]), Config),

    Res = ?retry(
        _Sleep0 = 200,
        _Retries0 = 20,
        begin
            Res0 = request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config),
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"matched">> := 0},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
                }},
                Res0
            ),
            Res0
        end
    ),
    {ok, 200, #{<<"node_metrics">> := NodeMetrics}} = Res,
    ?assert(
        lists:all(
            fun(#{<<"metrics">> := #{<<"matched">> := Matched}}) ->
                Matched == 0
            end,
            NodeMetrics
        ),
        #{node_metrics => NodeMetrics}
    ),

    ok.

t_cluster_later_join_metrics(matrix) ->
    [
        [cluster_later_join, actions]
    ];
t_cluster_later_join_metrics(Config) ->
    [PrimaryNode, OtherNode | _] = ?config(cluster_nodes, Config),
    Name = ?BRIDGE_NAME,
    ActionParams = ?KAFKA_BRIDGE(Name),
    ActionID = emqx_bridge_resource:bridge_id(?ACTION_TYPE, Name),
    ?check_trace(
        begin
            %% Create a bridge on only one of the nodes.
            ?assertMatch(
                {ok, 201, _}, request_json(post, uri([?ACTIONS_ROOT]), ActionParams, Config)
            ),
            %% Pre-condition.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
                }},
                request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config)
            ),
            %% Now join the other node join with the api node.
            ok = erpc:call(OtherNode, ekka, join, [PrimaryNode]),
            %% Check metrics; shouldn't crash even if the bridge is not
            %% ready on the node that just joined the cluster.
            ?assertMatch(
                {ok, 200, #{
                    <<"metrics">> := #{<<"success">> := _},
                    <<"node_metrics">> := [#{<<"metrics">> := #{}} | _]
                }},
                request_json(get, uri([?ACTIONS_ROOT, ActionID, "metrics"]), Config)
            ),
            ok
        end,
        []
    ),
    ok.

t_raw_config_response_defaults(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_raw_config_response_defaults(Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    Name = atom_to_binary(?FUNCTION_NAME),
    #{
        api_root_key := APIRootKey,
        create_config_fn := CreateConfigFn
    } = get_common_values(Kind, Name),
    Params = maps:remove(<<"enable">>, CreateConfigFn(#{})),
    ?assertMatch(
        {ok, 201, #{<<"enable">> := true}},
        request_json(
            post,
            uri([APIRootKey]),
            Params,
            Config
        )
    ),
    ok.

t_older_version_nodes_in_cluster(matrix) ->
    [
        [cluster, actions],
        [cluster, sources]
    ];
t_older_version_nodes_in_cluster(Config) ->
    [_, Kind | _] = group_path(Config),
    PrimaryNode = ?config(node, Config),
    OtherNode = maybe_get_other_node(Config),
    ?assertNotEqual(OtherNode, PrimaryNode),
    Name = atom_to_binary(?FUNCTION_NAME),
    ?check_trace(
        begin
            #{api_root_key := APIRootKey} = get_common_values(Kind, Name),
            erpc:call(PrimaryNode, fun() ->
                meck:new(emqx_bpapi, [no_history, passthrough, no_link]),
                meck:expect(emqx_bpapi, supported_version, fun(N, Api) ->
                    case N =:= OtherNode of
                        true -> 1;
                        false -> meck:passthrough([N, Api])
                    end
                end)
            end),
            erpc:call(OtherNode, fun() ->
                meck:new(emqx_bridge_v2, [no_history, passthrough, no_link]),
                meck:expect(emqx_bridge_v2, list, fun(_ConfRootKey) ->
                    error(should_not_be_called)
                end)
            end),
            ?assertMatch(
                {ok, 200, _},
                request_json(
                    get,
                    uri([APIRootKey]),
                    Config
                )
            ),
            ok
        end,
        []
    ),

    ok.

t_start_action_or_source_with_disabled_connector(matrix) ->
    [
        [single, actions],
        [single, sources]
    ];
t_start_action_or_source_with_disabled_connector(Config) ->
    ok = emqx_bridge_v2_testlib:t_start_action_or_source_with_disabled_connector(Config),
    ok.

%% Verifies that listing connectors return the actions and sources that depend on the
%% connector
t_connector_dependencies(matrix) ->
    [
        [single, actions],
        [single, sources]
    ];
t_connector_dependencies(Config) when is_list(Config) ->
    ?check_trace(
        begin
            %% This particular source type happens to serve both actions and sources, a
            %% nice edge case for this test.
            ActionType = ?SOURCE_TYPE,
            ConnectorType = ?SOURCE_CONNECTOR_TYPE,
            ConnectorName = <<"c">>,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_config, source_connector_create_config(#{})},
                    {connector_name, ConnectorName},
                    {connector_type, ConnectorType}
                ]),
            ?assertMatch(
                {200, [
                    #{
                        <<"actions">> := [],
                        <<"sources">> := []
                    }
                ]},
                list_connectors_api()
            ),
            ?assertMatch(
                {200, #{
                    <<"actions">> := [],
                    <<"sources">> := []
                }},
                get_connector_api(ConnectorType, ConnectorName)
            ),

            SourceName1 = <<"s1">>,
            {201, _} = create_source_api(
                SourceName1,
                ?SOURCE_TYPE,
                source_create_config(#{
                    <<"connector">> => ConnectorName
                })
            ),
            ?assertMatch(
                {200, [
                    #{
                        <<"actions">> := [],
                        <<"sources">> := [SourceName1]
                    }
                ]},
                list_connectors_api()
            ),
            ?assertMatch(
                {200, #{
                    <<"actions">> := [],
                    <<"sources">> := [SourceName1]
                }},
                get_connector_api(ConnectorType, ConnectorName)
            ),

            ActionName1 = <<"a1">>,
            {201, _} = create_action_api(
                ActionName1,
                ActionType,
                mqtt_action_create_config(#{
                    <<"connector">> => ConnectorName
                })
            ),
            ?assertMatch(
                {200, [
                    #{
                        <<"actions">> := [ActionName1],
                        <<"sources">> := [SourceName1]
                    }
                ]},
                list_connectors_api()
            ),
            ?assertMatch(
                {200, #{
                    <<"actions">> := [ActionName1],
                    <<"sources">> := [SourceName1]
                }},
                get_connector_api(ConnectorType, ConnectorName)
            ),

            ok
        end,
        []
    ),
    ok.

%% Verifies that listing actions/sources return the rules that depend on them.
t_kind_dependencies(matrix) ->
    [
        [single, actions],
        [single, sources]
    ];
t_kind_dependencies(Config) when is_list(Config) ->
    ?check_trace(
        begin
            %% This particular source type happens to serve both actions and sources, a
            %% nice edge case for this test.
            ActionType = ?SOURCE_TYPE,
            SourceType = ?SOURCE_TYPE,
            ConnectorType = ?SOURCE_CONNECTOR_TYPE,
            ConnectorName = <<"c">>,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_config, source_connector_create_config(#{})},
                    {connector_name, ConnectorName},
                    {connector_type, ConnectorType}
                ]),

            ActionName1 = <<"a1">>,
            {201, _} = create_action_api(
                ActionName1,
                ActionType,
                mqtt_action_create_config(#{
                    <<"connector">> => ConnectorName
                })
            ),
            ?assertMatch(
                {200, [#{<<"rules">> := []}]},
                list_actions_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := []}},
                get_action_api(ActionType, ActionName1)
            ),

            {ok, #{<<"id">> := RuleId1}} = create_action_rule(ActionType, ActionName1),

            ?assertMatch(
                {200, [#{<<"rules">> := [RuleId1]}]},
                list_actions_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := [RuleId1]}},
                get_action_api(ActionType, ActionName1)
            ),
            ?assertMatch(
                {200, []},
                list_sources_api()
            ),

            SourceName1 = <<"s1">>,
            {201, _} = create_source_api(
                SourceName1,
                ?SOURCE_TYPE,
                source_create_config(#{
                    <<"connector">> => ConnectorName
                })
            ),
            ?assertMatch(
                {200, [#{<<"rules">> := []}]},
                list_sources_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := []}},
                get_source_api(SourceType, SourceName1)
            ),
            %% Action remains untouched
            ?assertMatch(
                {200, [#{<<"rules">> := [RuleId1]}]},
                list_actions_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := [RuleId1]}},
                get_action_api(ActionType, ActionName1)
            ),

            %% using "$bridges/..." hookpoint
            {ok, #{<<"id">> := RuleId2}} = create_source_rule1(SourceType, SourceName1),
            ?assertMatch(
                {200, [#{<<"rules">> := [RuleId2]}]},
                list_sources_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := [RuleId2]}},
                get_source_api(SourceType, SourceName1)
            ),
            %% Action remains untouched
            ?assertMatch(
                {200, [#{<<"rules">> := [RuleId1]}]},
                list_actions_api()
            ),

            %% using "$sources/..." hookpoint
            {ok, #{<<"id">> := RuleId3}} = create_source_rule2(SourceType, SourceName1),
            ?assertMatch(
                {200, [#{<<"rules">> := [RuleId1]}]},
                list_actions_api()
            ),
            Rules = lists:sort([RuleId2, RuleId3]),
            ?assertMatch(
                {200, [#{<<"rules">> := Rules}]},
                list_sources_api()
            ),
            ?assertMatch(
                {200, #{<<"rules">> := Rules}},
                get_source_api(SourceType, SourceName1)
            ),

            ok
        end,
        []
    ),
    ok.

t_summary(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_summary(Config) when is_list(Config) ->
    Opts = summary_scenarios_setup(Config),
    #{
        summarize := Summarize,
        name := Name,
        type := Type
    } = Opts,
    ?assertMatch(
        {200, [
            #{
                <<"enable">> := true,
                <<"name">> := Name,
                <<"type">> := Type,
                <<"description">> := _,
                <<"created_at">> := _,
                <<"last_modified_at">> := _,
                <<"node_status">> := [
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"connected">>,
                        <<"status_reason">> := <<"">>
                    }
                    | _
                ],
                <<"rules">> := [],
                <<"status">> := <<"connected">>,
                <<"status_reason">> := <<"">>
            }
        ]},
        Summarize()
    ),
    ok.

t_summary_inconsistent(matrix) ->
    [
        [cluster, actions],
        [cluster, sources]
    ];
t_summary_inconsistent(Config) when is_list(Config) ->
    Opts = summary_scenarios_setup(Config),
    [_N1, N2 | _] = ?config(cluster_nodes, Config),
    ?ON(N2, begin
        ok = meck:new(emqx_bridge_v2_api, [no_history, passthrough, no_link]),
        ok = meck:expect(emqx_bridge_v2_api, summary_v8, fun(Namespace, ConfRootKey) ->
            Res = meck:passthrough([Namespace, ConfRootKey]),
            lists:map(
                fun(R) ->
                    R#{
                        status := disconnected,
                        status_reason := <<"mocked failure">>
                    }
                end,
                Res
            )
        end)
    end),
    #{
        summarize := Summarize,
        name := Name,
        type := Type
    } = Opts,
    ?assertMatch(
        {200, [
            #{
                <<"enable">> := true,
                <<"name">> := Name,
                <<"type">> := Type,
                <<"description">> := _,
                <<"created_at">> := _,
                <<"last_modified_at">> := _,
                <<"node_status">> := [
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"disconnected">>,
                        <<"status_reason">> := <<"mocked failure">>
                    },
                    #{
                        <<"node">> := _,
                        <<"status">> := <<"connected">>,
                        <<"status_reason">> := <<"">>
                    }
                    | _
                ],
                <<"rules">> := [],
                <<"status">> := <<"inconsistent">>,
                <<"status_reason">> := <<"mocked failure">>
            }
        ]},
        Summarize()
    ),
    ok.

%% Checks that we can delete an action/source when a source/action with the same name and
%% connector is referenced by a rule.
t_delete_when_dual_has_dependencies(matrix) ->
    [
        [single, actions],
        [single, sources]
    ];
t_delete_when_dual_has_dependencies(Config) when is_list(Config) ->
    [_SingleOrCluster, Kind | _] = group_path(Config),
    %% This particular source type happens to serve both actions and sources
    ConnectorType = ?SOURCE_CONNECTOR_TYPE,
    ConnectorName = <<"c">>,
    {ok, {{_, 201, _}, _, _}} =
        emqx_bridge_v2_testlib:create_connector_api([
            {connector_config, source_connector_create_config(#{})},
            {connector_name, ConnectorName},
            {connector_type, ConnectorType}
        ]),
    ActionName = <<"same_name">>,
    ActionType = ?SOURCE_TYPE,
    ActionConfig = mqtt_action_create_config(#{<<"connector">> => ConnectorName}),
    {201, _} = create_action_api(ActionName, ActionType, ActionConfig),
    SourceName = ActionName,
    SourceType = ?SOURCE_TYPE,
    SourceConfig = source_create_config(#{<<"connector">> => ConnectorName}),
    {201, _} = create_source_api(SourceName, SourceType, SourceConfig),

    %% We'll use `Kind' to choose which kind will have dependencies.
    %% We then attempt to delete the kind which is dual to `Kind'.
    case Kind of
        actions ->
            {ok, _} = create_action_rule(ActionType, ActionName),
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:delete_kind_api(source, SourceType, SourceName)
            );
        sources ->
            {ok, _} = create_source_rule1(SourceType, SourceName),
            ?assertMatch(
                {204, _},
                emqx_bridge_v2_testlib:delete_kind_api(action, ActionType, ActionName)
            )
    end,

    ok.

%% Checks that we return configured fallback actions in API responses for a single action
%% and for summary.
t_fallback_actions_returned_info(matrix) ->
    [
        [single, actions],
        [cluster, actions]
    ];
t_fallback_actions_returned_info(Config) ->
    Opts = summary_scenarios_setup(Config),
    #{
        summarize := Summarize,
        config := ActionConfig0,
        name := ReferencedName,
        type := Type
    } = Opts,
    ReferencingName = <<"action_that_uses_fallback">>,
    ActionConfig = emqx_utils_maps:deep_merge(
        ActionConfig0,
        #{
            <<"fallback_actions">> => [
                #{<<"kind">> => <<"reference">>, <<"type">> => Type, <<"name">> => ReferencedName}
            ]
        }
    ),
    ?assertMatch(
        {201, #{
            <<"fallback_actions">> := [
                #{
                    <<"kind">> := <<"reference">>,
                    <<"tags">> := [<<"tag1">>]
                }
            ]
        }},
        create_action_api(ReferencingName, Type, ActionConfig)
    ),
    ?assertMatch(
        {200, #{
            <<"fallback_actions">> := [
                #{
                    <<"kind">> := <<"reference">>,
                    <<"tags">> := [<<"tag1">>]
                }
            ]
        }},
        update_action_api(
            ReferencingName,
            Type,
            maps:without([<<"type">>, <<"name">>], ActionConfig)
        )
    ),
    ?assertMatch(
        {200, [
            #{
                <<"name">> := ReferencingName,
                <<"tags">> := _,
                <<"referenced_as_fallback_action_by">> := []
            },
            #{
                <<"name">> := ReferencedName,
                <<"tags">> := _,
                <<"referenced_as_fallback_action_by">> := [
                    #{
                        <<"type">> := Type,
                        <<"name">> := ReferencingName
                    }
                ]
            }
        ]},
        Summarize()
    ),
    ok.

-doc """
Smoke tests for CRUD operations on namespaced connectors.

  - Namespaced users should only see and be able to mutate resources in their namespaces.

""".
t_namespaced_crud(matrix) ->
    [
        [single, actions],
        [single, sources],
        [cluster, actions],
        [cluster, sources]
    ];
t_namespaced_crud(TCConfig0) when is_list(TCConfig0) ->
    clear_mocks(TCConfig0),
    Node = get_value(node, TCConfig0),
    {ok, APIKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    TCConfig = [{api_key, APIKey} | TCConfig0],

    NoNamespace = ?global_ns,
    AuthHeaderGlobal = emqx_common_test_http:auth_header(APIKey),
    TCConfigGlobal = [{auth_header, AuthHeaderGlobal} | TCConfig],
    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],
    NS2 = <<"ns2">>,
    AuthHeaderNS2 = ensure_namespaced_api_key(NS2, TCConfig),
    TCConfigNS2 = [{auth_header, AuthHeaderNS2} | TCConfig],

    AuthHeaderViewerNS1 = ensure_namespaced_api_key(
        NS1, #{name => <<"viewer">>, role => viewer}, TCConfig
    ),
    TCConfigViewerNS1 = [{auth_header, AuthHeaderViewerNS1} | TCConfig],

    %% setup connectors for each namespace
    MQTTPort = get_tcp_mqtt_port(Node),
    Server = <<"127.0.0.1:", (integer_to_binary(MQTTPort))/binary>>,
    ConnectorType = <<"mqtt">>,
    ConnectorName = <<"conn">>,
    ConnectorConfigGlobal = source_connector_create_config(#{
        <<"description">> => <<"global">>,
        <<"server">> => Server
    }),
    ConnectorConfigNS1 = source_connector_create_config(#{
        <<"description">> => <<"ns1">>,
        <<"server">> => Server
    }),
    ConnectorConfigNS2 = source_connector_create_config(#{
        <<"description">> => <<"ns2">>,
        <<"server">> => Server
    }),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigGlobal, TCConfigGlobal
        ),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigNS1, TCConfigNS1
        ),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigNS2, TCConfigNS2
        ),

    %% no actions/sources at this moment
    ?assertMatch({200, []}, list(TCConfigGlobal)),
    ?assertMatch({200, []}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),

    ?assertMatch({200, []}, summary(TCConfigGlobal)),
    ?assertMatch({200, []}, summary(TCConfigNS1)),
    ?assertMatch({200, []}, summary(TCConfigNS2)),

    %% Creating action/source with same name globally and in each NS should work.
    Type = <<"mqtt">>,
    Name1 = <<"channel1">>,
    ConfigGlobal = source_update_config(#{
        <<"description">> => <<"global">>,
        <<"connector">> => ConnectorName
    }),
    ConfigNS1 = source_update_config(#{
        <<"description">> => <<"ns1">>,
        <<"connector">> => ConnectorName
    }),
    ConfigNS2 = source_update_config(#{
        <<"description">> => <<"ns2">>,
        <<"connector">> => ConnectorName
    }),

    ?assertMatch(
        {201, #{<<"description">> := <<"global">>, <<"status">> := <<"connected">>}},
        create(Type, Name1, ConfigGlobal, TCConfigGlobal)
    ),
    ?assertMatch(
        {201, #{<<"description">> := <<"ns1">>, <<"status">> := <<"connected">>}},
        create(Type, Name1, ConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"description">> := <<"ns2">>, <<"status">> := <<"connected">>}},
        create(Type, Name1, ConfigNS2, TCConfigNS2)
    ),

    ?assertMatch({200, [#{<<"description">> := <<"global">>}]}, list(TCConfigGlobal)),
    ?assertMatch({200, [#{<<"description">> := <<"ns1">>}]}, list(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := <<"ns2">>}]}, list(TCConfigNS2)),

    ?assertMatch({200, [#{<<"description">> := <<"global">>}]}, summary(TCConfigGlobal)),
    ?assertMatch({200, [#{<<"description">> := <<"ns1">>}]}, summary(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := <<"ns2">>}]}, summary(TCConfigNS2)),

    ?assertMatch(
        {200, #{<<"description">> := <<"global">>}},
        get(Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"description">> := NS1}},
        get(Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"description">> := NS2}},
        get(Type, Name1, TCConfigNS2)
    ),

    %% Checking resource state directly to ensure their independency.
    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NoNamespace, Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NS1, Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NS2, Type, Name1, TCConfigNS2)
    ),

    %% Updating each one should be independent
    ?assertMatch(
        {200, #{<<"description">> := <<"updated global">>}},
        update(
            Type,
            Name1,
            emqx_utils_maps:deep_merge(
                ConfigGlobal,
                #{
                    <<"description">> => <<"updated global">>,
                    <<"parameters">> => #{<<"qos">> => 0}
                }
            ),
            TCConfigGlobal
        )
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns1">>}},
        update(
            Type,
            Name1,
            emqx_utils_maps:deep_merge(
                ConfigNS1,
                #{
                    <<"description">> => <<"updated ns1">>,
                    <<"parameters">> => #{<<"qos">> => 1}
                }
            ),
            TCConfigNS1
        )
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns2">>}},
        update(
            Type,
            Name1,
            emqx_utils_maps:deep_merge(
                ConfigNS2,
                #{
                    <<"description">> => <<"updated ns2">>,
                    <<"parameters">> => #{<<"qos">> => 2}
                }
            ),
            TCConfigNS2
        )
    ),

    ?assertMatch({200, [#{<<"description">> := <<"updated global">>}]}, list(TCConfigGlobal)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns1">>}]}, list(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns2">>}]}, list(TCConfigNS2)),

    ?assertMatch({200, [#{<<"description">> := <<"updated global">>}]}, summary(TCConfigGlobal)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns1">>}]}, summary(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns2">>}]}, summary(TCConfigNS2)),

    ?assertMatch(
        {200, #{<<"description">> := <<"updated global">>}},
        get(Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns1">>}},
        get(Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns2">>}},
        get(Type, Name1, TCConfigNS2)
    ),

    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NoNamespace, Type, Name1, TCConfig)
    ),
    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NS1, Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {ok, #rt{
            st_err = #{status := ?status_connected},
            channel_status = ?status_connected
        }},
        get_runtime(NS2, Type, Name1, TCConfigNS2)
    ),

    %% Enable/Disable/Start operations
    ?assertMatch({204, _}, disable(Type, Name1, TCConfigGlobal)),
    ?assertMatch({204, _}, disable(Type, Name1, TCConfigNS1)),
    ?assertMatch({204, _}, disable(Type, Name1, TCConfigNS2)),

    ?assertMatch({204, _}, enable(Type, Name1, TCConfigGlobal)),
    ?assertMatch({204, _}, enable(Type, Name1, TCConfigNS1)),
    ?assertMatch({204, _}, enable(Type, Name1, TCConfigNS2)),

    ?assertMatch({204, _}, start(Type, Name1, TCConfig)),
    ?assertMatch({204, _}, start(Type, Name1, TCConfigNS1)),
    ?assertMatch({204, _}, start(Type, Name1, TCConfigNS2)),

    ?assertMatch({204, _}, start_on_node(Type, Name1, TCConfig)),
    ?assertMatch({204, _}, start_on_node(Type, Name1, TCConfigNS1)),
    ?assertMatch({204, _}, start_on_node(Type, Name1, TCConfigNS2)),

    %% Create one extra namespaced action/source
    Name2 = <<"conn2">>,
    ConfigNS1B = source_update_config(#{
        <<"description">> => <<"another ns1">>,
        <<"connector">> => ConnectorName
    }),
    ?assertMatch(
        {201, #{<<"description">> := <<"another ns1">>}},
        create(Type, Name2, ConfigNS1B, TCConfigNS1)
    ),
    ?assertMatch({200, [_]}, list(TCConfigGlobal)),
    ?assertMatch({200, [_, _]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),
    ?assertMatch({200, [_]}, summary(TCConfigGlobal)),
    ?assertMatch(
        {200, [#{<<"status">> := <<"connected">>}, #{<<"status">> := <<"connected">>}]},
        summary(TCConfigNS1)
    ),
    ?assertMatch({200, [_]}, summary(TCConfigNS2)),
    ?assertMatch(
        {200, #{<<"description">> := <<"another ns1">>}},
        get(Type, Name2, TCConfigNS1)
    ),
    ?assertMatch({404, _}, get(Type, Name2, TCConfigGlobal)),
    ?assertMatch({404, _}, get(Type, Name2, TCConfigNS2)),

    %% Viewer cannot mutate anything, only read
    ?assertMatch({200, [_, _]}, list(TCConfigViewerNS1)),
    ?assertMatch({200, #{}}, get(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({200, _}, get_metrics(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, create(Type, Name1, ConfigNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, update(Type, Name1, ConfigNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, delete(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, start(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, start_on_node(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, disable(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, enable(Type, Name1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, probe(Type, Name1, ConfigNS1, TCConfigViewerNS1)),
    ?assertMatch({403, _}, reset_metrics(Type, Name1, TCConfigViewerNS1)),

    %% Probe
    ?assertMatch({204, _}, probe(Type, Name1, ConfigGlobal, TCConfigGlobal)),
    ?assertMatch({204, _}, probe(Type, Name1, ConfigNS1, TCConfigNS1)),
    ?assertMatch({204, _}, probe(Type, Name1, ConfigNS2, TCConfigNS2)),

    %% Send messages; only applicable to actions
    maybe
        action ?= get_value(bridge_kind, TCConfig0),
        #{<<"parameters">> := #{<<"topic">> := RemoteTopic}} = ConfigGlobal,
        {ok, C} = emqtt:start_link(#{port => MQTTPort, proto_ver => v5}),
        {ok, _} = emqtt:connect(C),
        {ok, _, _} = emqtt:subscribe(C, RemoteTopic, [{qos, 2}]),
        QueryOpts = #{},
        Message = #{payload => <<"hello">>},
        ok = send_message(NoNamespace, Type, Name1, Message, QueryOpts, TCConfigGlobal),
        ok = send_message(NS1, Type, Name1, Message, QueryOpts, TCConfigNS1),
        ok = send_message(NS2, Type, Name1, Message, QueryOpts, TCConfigNS2),
        %% Each action has different QoSes.
        ?assertReceive({publish, #{qos := 0}}),
        ?assertReceive({publish, #{qos := 1}}),
        ?assertReceive({publish, #{qos := 2}}),
        ?assertNotReceive({publish, _})
    end,

    InitialMatchedCount =
        case get_value(bridge_kind, TCConfig0) of
            action -> 1;
            source -> 0
        end,

    %% Metrics
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigNS2)
    ),

    ?assertMatch({204, _}, reset_metrics(Type, Name1, TCConfigGlobal)),

    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := 0}}},
        get_metrics(Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigNS2)
    ),

    ?assertMatch({204, _}, reset_metrics(Type, Name1, TCConfigNS1)),

    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := 0}}},
        get_metrics(Type, Name1, TCConfigGlobal)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := 0}}},
        get_metrics(Type, Name1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"metrics">> := #{<<"matched">> := InitialMatchedCount}}},
        get_metrics(Type, Name1, TCConfigNS2)
    ),

    %% Delete

    %% Let's create _a_ rule so that it gets enumerated when trying to find referencing
    %% rules.  Otherwise, we might miss some code paths where the resource ids are
    %% attempted to be parsed.
    {201, _} = emqx_bridge_v2_testlib:create_rule_api2(
        #{
            <<"enable">> => true,
            <<"sql">> => <<"select * from bah">>,
            <<"actions">> => [#{<<"function">> => <<"console">>}]
        },
        #{auth_header => AuthHeaderGlobal}
    ),

    ChanIdGlobal = get_channel_id(NoNamespace, Type, Name1, TCConfigGlobal),
    ChanIdNS1 = get_channel_id(NS1, Type, Name1, TCConfigNS1),
    ChanIdNS2 = get_channel_id(NS2, Type, Name1, TCConfigNS2),

    ?assertMatch({204, _}, delete(Type, Name1, TCConfigGlobal)),
    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, [_, _]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),
    %% Note: we retry because removal of channel from cache is async
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, #rt{channel_status = ?NO_CHANNEL}},
            get_runtime(ChanIdGlobal, TCConfigGlobal)
        )
    ),
    ?assertMatch({ok, _}, get_runtime(ChanIdNS1, TCConfigNS1)),
    ?assertMatch({ok, _}, get_runtime(ChanIdNS2, TCConfigNS1)),

    ?assertMatch({204, _}, delete(Type, Name1, TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigGlobal)),
    ?assertMatch({200, [_]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),

    ?assertMatch({204, _}, delete(Type, Name1, TCConfigNS2)),
    ?assertMatch({200, []}, list(TCConfigGlobal)),
    ?assertMatch({200, [_]}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),
    ?assertMatch(
        {ok, #rt{channel_status = ?NO_CHANNEL}},
        get_runtime(ChanIdGlobal, TCConfigGlobal)
    ),
    %% Note: we retry because removal of channel from cache is async
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, #rt{channel_status = ?NO_CHANNEL}},
            get_runtime(ChanIdNS1, TCConfigNS1)
        )
    ),
    ?retry(
        200,
        20,
        ?assertMatch(
            {ok, #rt{channel_status = ?NO_CHANNEL}},
            get_runtime(ChanIdNS2, TCConfigNS2)
        )
    ),

    ?assertMatch({204, _}, delete(Type, Name2, TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigGlobal)),
    ?assertMatch({200, []}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),

    ok.

-doc """
Checks that we correctly load and unload connectors already in the config when a node
restarts.
""".
t_namespaced_load_on_restart(matrix) ->
    [
        [cluster, actions],
        [cluster, sources]
    ];
t_namespaced_load_on_restart(TCConfig0) when is_list(TCConfig0) ->
    clear_mocks(TCConfig0),
    N1 = get_value(node, TCConfig0),
    [N1Spec | _] = get_value(node_specs, TCConfig0),
    {ok, APIKey} = erpc:call(N1, emqx_common_test_http, create_default_app, []),
    TCConfig = [{api_key, APIKey} | TCConfig0],

    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],

    MQTTPort = get_tcp_mqtt_port(N1),
    Server = <<"127.0.0.1:", (integer_to_binary(MQTTPort))/binary>>,
    ConnectorType = <<"mqtt">>,
    ConnectorName = <<"conn">>,
    ConnectorConfigNS1 = source_connector_create_config(#{
        <<"description">> => <<"ns1">>,
        <<"server">> => Server
    }),
    {201, #{<<"status">> := <<"connected">>}} =
        emqx_connector_api_SUITE:create(
            ConnectorType, ConnectorName, ConnectorConfigNS1, TCConfigNS1
        ),

    Type = <<"mqtt">>,
    Name = <<"channel1">>,
    ConfigNS1 = source_update_config(#{
        <<"description">> => <<"ns1">>,
        <<"connector">> => ConnectorName
    }),
    {201, #{<<"status">> := <<"connected">>}} =
        create(Type, Name, ConfigNS1, TCConfigNS1),

    ChanIdNS1 = get_channel_id(NS1, Type, Name, TCConfigNS1),

    [N1] = emqx_cth_cluster:restart([N1Spec]),

    ?assertMatch({200, [#{<<"status">> := <<"connected">>}]}, list(TCConfigNS1)),
    ?assertMatch(
        {ok, #rt{channel_status = ?status_connected}},
        get_runtime(ChanIdNS1, TCConfigNS1)
    ),

    ok.
