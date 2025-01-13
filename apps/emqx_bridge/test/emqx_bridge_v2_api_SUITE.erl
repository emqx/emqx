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

-module(emqx_bridge_v2_api_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(emqx_mgmt_api_test_util, [uri/1]).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/test_macros.hrl").

-define(ACTIONS_ROOT, "actions").
-define(SOURCES_ROOT, "sources").

-define(ACTION_CONNECTOR_NAME, <<"my_connector">>).
-define(SOURCE_CONNECTOR_NAME, <<"my_connector">>).

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

-define(APPSPECS, [
    emqx_conf,
    emqx,
    emqx_auth,
    emqx_management,
    emqx_connector,
    emqx_bridge,
    emqx_rule_engine
]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

-if(?EMQX_RELEASE_EDITION == ee).
%% For now we got only kafka implementing `bridge_v2` and that is enterprise only.
all() ->
    All0 = emqx_common_test_helpers:all(?MODULE),
    All = All0 -- matrix_cases(),
    Groups = lists:map(fun({G, _, _}) -> {group, G} end, groups()),
    Groups ++ All.
-else.
all() ->
    [].
-endif.

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
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config),
    [{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config];
init_per_group(cluster_later_join = Name, Config) ->
    Nodes = [NodePrimary | _] = mk_cluster(Name, Config, #{join_to => undefined}),
    Fun = fun() -> ?ON(NodePrimary, emqx_mgmt_api_test_util:auth_header_()) end,
    emqx_bridge_v2_testlib:set_auth_header_getter(Fun),
    [{group, Name}, {cluster_nodes, Nodes}, {node, NodePrimary} | Config];
init_per_group(single = Group, Config) ->
    WorkDir = work_dir_random_suffix(Group, Config),
    Apps = emqx_cth_suite:start(
        ?APPSPECS ++ [emqx_mgmt_api_test_util:emqx_dashboard()],
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

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [emqx_mgmt_api_test_util:emqx_dashboard()],
    Node2Apps = ?APPSPECS,
    WorkDir = work_dir_random_suffix(Name, Config),
    emqx_cth_cluster:start(
        [
            {emqx_bridge_v2_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_bridge_v2_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => WorkDir}
    ).

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
        t_summary_inconsistent
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

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

-define(CONNECTOR_IMPL, emqx_bridge_v2_dummy_connector).
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
    case emqx_utils_json:safe_decode(B, [return_maps]) of
        {ok, Term} ->
            Term;
        {error, Reason} = Error ->
            ct:pal("Failed to decode json: ~p~n~p", [Reason, B]),
            Error
    end.

group_path(Config) ->
    case emqx_common_test_helpers:group_path(Config) of
        [] ->
            undefined;
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
            CreateConfig = mqtt_action_create_config(#{<<"connector">> => ConnectorName}),
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
            CreateConfig = source_create_config(#{<<"connector">> => ConnectorName}),
            {201, _} = create_source_api(Name, Type, CreateConfig),
            Summarize = fun emqx_bridge_v2_testlib:summarize_sources_api/0
    end,
    #{
        type => Type,
        name => Name,
        summarize => Summarize
    }.

%%------------------------------------------------------------------------------
%% Testcases
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
    ?assertMatch(#{<<"rules">> := [_ | _]}, emqx_utils_json:decode(Body, [return_maps])),
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
    Msg = emqx_utils_json:decode(Msg0, [return_maps]),
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

%% Verifies that we return thrown messages as is to the API.
t_thrown_messages(matrix) ->
    [
        [single, actions],
        [single, sources]
    ];
t_thrown_messages(Config) when is_list(Config) ->
    meck:expect(?CONNECTOR_IMPL, on_remove_channel, fun(_ConnResId, ConnState, _ActionResid) ->
        timer:sleep(20_000),
        {ok, ConnState}
    end),
    ?check_trace(
        begin
            [_SingleOrCluster, Kind | _] = group_path(Config),
            ConnectorType = ?SOURCE_CONNECTOR_TYPE,
            ConnectorName = <<"c">>,
            {ok, {{_, 201, _}, _, _}} =
                emqx_bridge_v2_testlib:create_connector_api([
                    {connector_config, source_connector_create_config(#{})},
                    {connector_name, ConnectorName},
                    {connector_type, ConnectorType}
                ]),
            do_t_thrown_messages(Kind, Config, ConnectorName),
            meck:expect(?CONNECTOR_IMPL, on_remove_channel, 3, {ok, connector_state}),
            ok
        end,
        []
    ),
    ok.

do_t_thrown_messages(actions, _Config, ConnectorName) ->
    Name = <<"a1">>,
    %% MQTT
    Type = ?SOURCE_TYPE,
    CreateConfig = mqtt_action_create_config(#{
        <<"connector">> => ConnectorName
    }),
    {201, _} = create_action_api(
        Name,
        Type,
        CreateConfig
    ),
    UpdateConfig = maps:remove(<<"type">>, CreateConfig),
    ?assertMatch(
        {503, #{
            <<"message">> :=
                #{<<"reason">> := <<"Timed out trying to remove", _/binary>>}
        }},
        update_action_api(
            Name,
            Type,
            UpdateConfig
        )
    ),
    ok;
do_t_thrown_messages(sources, _Config, ConnectorName) ->
    Name = <<"s1">>,
    Type = ?SOURCE_TYPE,
    CreateConfig = source_create_config(#{
        <<"connector">> => ConnectorName
    }),
    {201, _} = create_source_api(
        Name,
        Type,
        CreateConfig
    ),
    UpdateConfig = maps:remove(<<"type">>, CreateConfig),
    ?assertMatch(
        {503, #{
            <<"message">> :=
                #{<<"reason">> := <<"Timed out trying to remove", _/binary>>}
        }},
        update_source_api(
            Name,
            Type,
            UpdateConfig
        )
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
                <<"enabled">> := true,
                <<"name">> := Name,
                <<"type">> := Type,
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
        ok = meck:expect(emqx_bridge_v2_api, summary_from_local_node_v7, fun(ConfRootKey) ->
            Res = meck:passthrough([ConfRootKey]),
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
                <<"enabled">> := true,
                <<"name">> := Name,
                <<"type">> := Type,
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
