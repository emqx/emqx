%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/emqx_config.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(CONNECTOR_IMPL, dummy_connector_impl).
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
    {emqx, #{
        before_start => fun(App, AppConfig) ->
            %% We need this in the tests because `emqx_cth_suite` does not start apps in
            %% the exact same way as the release works: in the release,
            %% `emqx_enterprise_schema` is the root schema that knows all root keys.  In
            %% CTH, we need to manually load the schema below so that when
            %% `emqx_config:init_load` runs and encounters a namespaced root key, it knows
            %% the schema module for it.
            emqx_config:init_load(emqx_connector_schema, <<"">>),
            ok = emqx_schema_hooks:inject_from_modules([?MODULE, emqx_connector_schema]),
            emqx_cth_suite:inhibit_config_loader(App, AppConfig)
        end
    }},
    emqx_conf,
    emqx_auth,
    emqx_connector,
    emqx_bridge_http,
    emqx_bridge,
    emqx_rule_engine,
    emqx_management
]).

-define(APPSPEC_DASHBOARD,
    {emqx_dashboard, "dashboard.listeners.http { enable = true, bind = 18083 }"}
).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

suite() ->
    [{timetrap, {seconds, 60}}].

all() ->
    [
        {group, single},
        {group, cluster}
    ].

groups() ->
    AllTCs = emqx_common_test_helpers:all(?MODULE),
    SingleOnlyTests = [
        t_connectors_probe,
        t_fail_delete_with_action,
        t_actions_field,
        t_update_with_failed_validation,
        t_create_with_failed_root_validation
    ],
    ClusterOnlyTests = [
        t_inconsistent_state,
        t_namespaced_load_on_restart
    ],
    [
        {single, [], AllTCs -- ClusterOnlyTests},
        {cluster, [], AllTCs -- SingleOnlyTests}
    ].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_group(cluster = Name, Config) ->
    {NodeSpecs, Nodes = [NodePrimary | _]} = mk_cluster(Name, Config),
    init_api([
        {group, Name},
        {cluster_nodes, Nodes},
        {node_specs, NodeSpecs},
        {node, NodePrimary}
        | Config
    ]);
init_per_group(Name, Config) ->
    WorkDir = filename:join(?config(priv_dir, Config), Name),
    Apps = emqx_cth_suite:start(?APPSPECS ++ [?APPSPEC_DASHBOARD], #{work_dir => WorkDir}),
    init_api([{group, single}, {group_apps, Apps}, {node, node()} | Config]).

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

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

init_api(Config) ->
    Node = ?config(node, Config),
    {ok, ApiKey} = erpc:call(Node, emqx_common_test_http, create_default_app, []),
    [{api_key, ApiKey} | Config].

mk_cluster(Name, Config) ->
    mk_cluster(Name, Config, #{}).

mk_cluster(Name, Config, Opts) ->
    Node1Apps = ?APPSPECS ++ [?APPSPEC_DASHBOARD],
    Node2Apps = ?APPSPECS,
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {emqx_connector_api_SUITE_1, Opts#{role => core, apps => Node1Apps}},
            {emqx_connector_api_SUITE_2, Opts#{role => core, apps => Node2Apps}}
        ],
        #{work_dir => filename:join(?config(priv_dir, Config), Name)}
    ),
    Nodes = emqx_cth_cluster:start(NodeSpecs),
    {NodeSpecs, Nodes}.

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
    emqx_bridge_v2_testlib:delete_all_rules(),
    emqx_bridge_v2_testlib:delete_all_bridges_and_connectors(),
    ok.

clear_mocks(TCConfig) ->
    case ?config(cluster_nodes, TCConfig) of
        undefined ->
            meck:unload();
        Nodes ->
            [erpc:call(Node, meck, unload, []) || Node <- Nodes]
    end.

%% `emqx_schema_hooks' callback
injected_fields() ->
    #{
        'connectors.validators' => [fun ?MODULE:dummy_validator/1]
    }.

dummy_validator(RootRawConf) ->
    case persistent_term:get({?MODULE, validator}, undefined) of
        Fn when is_function(Fn, 1) ->
            Fn(RootRawConf);
        _ ->
            ok
    end.

set_validator(Fn) when is_function(Fn, 1) ->
    persistent_term:put({?MODULE, validator}, Fn).

clear_validator() ->
    persistent_term:erase({?MODULE, validator}).

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
    AuthHeader = auth_header(Config),
    Opts = #{compatible_mode => true, httpc_req_opts => [{body_format, binary}]},
    emqx_mgmt_api_test_util:request_api(Method, URL, [], AuthHeader, Body, Opts).

auth_header(TCConfig) ->
    maybe
        undefined ?= ?config(auth_header, TCConfig),
        APIKey = emqx_bridge_v2_testlib:get_value(api_key, TCConfig),
        emqx_common_test_http:auth_header(APIKey)
    end.

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
    case emqx_utils_json:safe_decode(B) of
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

http_connector_config() ->
    http_connector_config(_Overrides = #{}).
http_connector_config(Overrides) ->
    Params0 = emqx_bridge_http_v2_SUITE:make_connector_config([{http_server, #{port => 18083}}]),
    Params = emqx_utils_maps:deep_merge(Params0, Overrides),
    emqx_bridge_v2_testlib:parse_and_check_connector(<<"http">>, <<"x">>, Params).

ensure_namespaced_api_key(Namespace, TCConfig) ->
    ensure_namespaced_api_key(Namespace, _Opts = #{}, TCConfig).
ensure_namespaced_api_key(Namespace, Opts0, TCConfig) ->
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    Opts = Opts0#{namespace => Namespace},
    ?ON(Node, emqx_bridge_v2_testlib:ensure_namespaced_api_key(Opts)).

get_resource(Namespace, Type, Name, TCConfig) ->
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        ConnResId = emqx_connector_resource:resource_id(Namespace, Type, Name),
        emqx_resource:get_instance(ConnResId)
    end).

list(TCConfig) ->
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["connectors"]),
            auth_header => AuthHeader
        })
    end).

get(Type, Name, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => get,
            url => emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
            auth_header => AuthHeader
        })
    end).

create(Type, Name, Config, TCConfig) ->
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => emqx_mgmt_api_test_util:api_path(["connectors"]),
            body => Config#{
                <<"type">> => Type,
                <<"name">> => Name
            },
            auth_header => AuthHeader
        })
    end).

update(Type, Name, Config, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
            body => Config,
            auth_header => AuthHeader
        })
    end).

delete(Type, Name, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => delete,
            url => emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId]),
            auth_header => AuthHeader
        })
    end).

start(Type, Name, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId, "start"]),
            auth_header => AuthHeader
        })
    end).

start_on_node(Type, Name, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    NodeBin = atom_to_binary(Node),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => post,
            url => emqx_mgmt_api_test_util:api_path(
                ["nodes", NodeBin, "connectors", ConnectorId, "start"]
            ),
            auth_header => AuthHeader
        })
    end).

enable(Type, Name, TCConfig) ->
    do_enable_or_disable(Type, Name, "true", TCConfig).

disable(Type, Name, TCConfig) ->
    do_enable_or_disable(Type, Name, "false", TCConfig).

do_enable_or_disable(Type, Name, Enable, TCConfig) ->
    ConnectorId = emqx_connector_resource:connector_id(Type, Name),
    Node = emqx_bridge_v2_testlib:get_value(node, TCConfig),
    ?ON(Node, begin
        AuthHeader = auth_header(TCConfig),
        emqx_bridge_v2_testlib:simple_request(#{
            method => put,
            url => emqx_mgmt_api_test_util:api_path(["connectors", ConnectorId, "enable", Enable]),
            auth_header => AuthHeader
        })
    end).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

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
                        Node,
                        emqx_connector_resource,
                        stop,
                        [?global_ns, ?CONNECTOR_TYPE, Name],
                        500
                    );
                cluster ->
                    Nodes = ?config(cluster_nodes, Config),
                    [{ok, ok}, {ok, ok}] = erpc:multicall(
                        Nodes,
                        emqx_connector_resource,
                        stop,
                        [?global_ns, ?CONNECTOR_TYPE, Name],
                        500
                    )
            end;
        _ ->
            ok = emqx_connector_resource:stop(?global_ns, ?CONNECTOR_TYPE, Name)
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
    Msg = emqx_utils_json:decode(Msg0),
    ?assertMatch(#{<<"kind">> := <<"validation_error">>}, Msg),
    ok.

%% Checks that we correctly handle `throw({bad_ssl_config, _})' from
%% `emqx_connector_ssl:convert_certs' and massage the error message accordingly.
t_create_with_bad_tls_files(Config) ->
    ConnectorName = atom_to_binary(?FUNCTION_NAME),
    Conf0 = ?KAFKA_CONNECTOR(ConnectorName),
    Conf = Conf0#{
        <<"ssl">> => #{
            <<"enable">> => true,
            <<"cacertfile">> => <<"bad_pem_file">>
        }
    },
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
    ConnectorResID = emqx_connector_resource:resource_id(
        ?global_ns, ?CONNECTOR_TYPE, ?CONNECTOR_NAME
    ),
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

%% Checks that we return a readable error when we attempt to update a connector and its
%% validation fails.
t_update_with_failed_validation(Config) ->
    Params = ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
    ?assertMatch(
        {ok, 201, _},
        request_json(
            post,
            uri(["connectors"]),
            Params,
            Config
        )
    ),
    BadParams0 = emqx_utils_maps:deep_merge(
        Params,
        #{<<"bootstrap_hosts">> => <<"a:b:123:a">>}
    ),
    BadParams = maps:without([<<"type">>, <<"name">>], BadParams0),
    ConnectorID = emqx_connector_resource:connector_id(?CONNECTOR_TYPE, ?CONNECTOR_NAME),
    %% Has to be a validator that returns `{error, _}' instead of throwing stuff.
    on_exit(fun() -> meck:unload() end),
    ok = meck:new(emqx_schema, [passthrough]),
    MockedError = <<"mocked negative validator response">>,
    ok = meck:expect(emqx_schema, servers_validator, fun(_, _) ->
        fun(_) ->
            {error, MockedError}
        end
    end),
    {ok, 400, ResBin} = request(
        put,
        uri(["connectors", ConnectorID]),
        BadParams,
        Config
    ),
    #{<<"message">> := MessageBin} = emqx_utils_json:decode(ResBin),
    ct:pal("error message:\n  ~s", [MessageBin]),
    Message = emqx_utils_json:decode(MessageBin),
    ?assertMatch(
        #{
            <<"kind">> := <<"validation_error">>,
            <<"reason">> := MockedError
        },
        Message
    ),
    ok.

%% Checks that we return a readable error when we attempt to create a connector and a
%% global, root validation fails.
t_create_with_failed_root_validation(Config) ->
    on_exit(fun clear_validator/0),
    ErrorMsg = <<"mocked root error">>,
    set_validator(fun(_RootConf) -> {error, ErrorMsg} end),
    Params = ?KAFKA_CONNECTOR(?CONNECTOR_NAME),
    {ok, 400, #{<<"message">> := MsgBin}} =
        request_json(
            post,
            uri(["connectors"]),
            Params,
            Config
        ),
    Message = emqx_utils_json:decode(MsgBin),
    %% `value' shouldn't contain the whole connectors map, as it may be huge.  We focus on
    %% the config that was part of the request.
    ?assertMatch(
        #{
            <<"reason">> := ErrorMsg,
            <<"kind">> := <<"validation_error">>,
            <<"path">> := <<"connectors">>,
            <<"value">> := #{<<"bootstrap_hosts">> := _}
        },
        Message
    ),
    ok.

-doc """
Smoke tests for CRUD operations on namespaced connectors.

  - Namespaced users should only see and be able to mutate resources in their namespaces.

""".
t_namespaced_crud(TCConfig) ->
    clear_mocks(TCConfig),
    NoNamespace = ?global_ns,
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

    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, []}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),

    ConnectorType = <<"http">>,
    ConnectorName1 = <<"conn1">>,
    ConnectorConfigGlobal1 = http_connector_config(#{<<"description">> => <<"global">>}),
    ConnectorConfigNS1 = http_connector_config(#{<<"description">> => NS1}),
    ConnectorConfigNS2 = http_connector_config(#{<<"description">> => NS2}),

    %% Creating connector with same name globally and in each NS should work.
    ?assertMatch(
        {201, #{<<"description">> := <<"global">>}},
        create(ConnectorType, ConnectorName1, ConnectorConfigGlobal1, TCConfig)
    ),
    ?assertMatch(
        {201, #{<<"description">> := NS1}},
        create(ConnectorType, ConnectorName1, ConnectorConfigNS1, TCConfigNS1)
    ),
    ?assertMatch(
        {201, #{<<"description">> := NS2}},
        create(ConnectorType, ConnectorName1, ConnectorConfigNS2, TCConfigNS2)
    ),

    ?assertMatch({200, [#{<<"description">> := <<"global">>}]}, list(TCConfig)),
    ?assertMatch({200, [#{<<"description">> := NS1}]}, list(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := NS2}]}, list(TCConfigNS2)),

    ?assertMatch(
        {200, #{<<"description">> := <<"global">>}},
        get(ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {200, #{<<"description">> := NS1}},
        get(ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"description">> := NS2}},
        get(ConnectorType, ConnectorName1, TCConfigNS2)
    ),

    %% Checking resource state directly to ensure their independency.
    ?assertMatch(
        {ok, _, #{config := #{description := <<"global">>}}},
        get_resource(NoNamespace, ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {ok, _, #{config := #{description := NS1}}},
        get_resource(NS1, ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {ok, _, #{config := #{description := NS2}}},
        get_resource(NS2, ConnectorType, ConnectorName1, TCConfigNS2)
    ),

    %% Updating each one should be independent
    ?assertMatch(
        {200, #{<<"description">> := <<"updated global">>}},
        update(
            ConnectorType,
            ConnectorName1,
            ConnectorConfigGlobal1#{<<"description">> => <<"updated global">>},
            TCConfig
        )
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns1">>}},
        update(
            ConnectorType,
            ConnectorName1,
            ConnectorConfigGlobal1#{<<"description">> => <<"updated ns1">>},
            TCConfigNS1
        )
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns2">>}},
        update(
            ConnectorType,
            ConnectorName1,
            ConnectorConfigGlobal1#{<<"description">> => <<"updated ns2">>},
            TCConfigNS2
        )
    ),

    ?assertMatch({200, [#{<<"description">> := <<"updated global">>}]}, list(TCConfig)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns1">>}]}, list(TCConfigNS1)),
    ?assertMatch({200, [#{<<"description">> := <<"updated ns2">>}]}, list(TCConfigNS2)),

    ?assertMatch(
        {200, #{<<"description">> := <<"updated global">>}},
        get(ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns1">>}},
        get(ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {200, #{<<"description">> := <<"updated ns2">>}},
        get(ConnectorType, ConnectorName1, TCConfigNS2)
    ),

    ?assertMatch(
        {ok, _, #{config := #{description := <<"updated global">>}}},
        get_resource(NoNamespace, ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {ok, _, #{config := #{description := <<"updated ns1">>}}},
        get_resource(NS1, ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {ok, _, #{config := #{description := <<"updated ns2">>}}},
        get_resource(NS2, ConnectorType, ConnectorName1, TCConfigNS2)
    ),

    %% Enable/Disable/Start operations
    ?assertMatch({204, _}, disable(ConnectorType, ConnectorName1, TCConfig)),
    ?assertMatch({204, _}, disable(ConnectorType, ConnectorName1, TCConfigNS1)),
    ?assertMatch({204, _}, disable(ConnectorType, ConnectorName1, TCConfigNS2)),

    ?assertMatch({204, _}, enable(ConnectorType, ConnectorName1, TCConfig)),
    ?assertMatch({204, _}, enable(ConnectorType, ConnectorName1, TCConfigNS1)),
    ?assertMatch({204, _}, enable(ConnectorType, ConnectorName1, TCConfigNS2)),

    ?assertMatch({204, _}, start(ConnectorType, ConnectorName1, TCConfig)),
    ?assertMatch({204, _}, start(ConnectorType, ConnectorName1, TCConfigNS1)),
    ?assertMatch({204, _}, start(ConnectorType, ConnectorName1, TCConfigNS2)),

    ?assertMatch({204, _}, start_on_node(ConnectorType, ConnectorName1, TCConfig)),
    ?assertMatch({204, _}, start_on_node(ConnectorType, ConnectorName1, TCConfigNS1)),
    ?assertMatch({204, _}, start_on_node(ConnectorType, ConnectorName1, TCConfigNS2)),

    %% Create one extra namespaced connector
    ConnectorName2 = <<"conn2">>,
    ConnectorConfigNS1B = http_connector_config(#{<<"description">> => <<"another ns1">>}),
    ?assertMatch(
        {201, #{<<"description">> := <<"another ns1">>}},
        create(ConnectorType, ConnectorName2, ConnectorConfigNS1B, TCConfigNS1)
    ),
    ?assertMatch({200, [_]}, list(TCConfig)),
    ?assertMatch({200, [_, _]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),
    ?assertMatch(
        {200, #{<<"description">> := <<"another ns1">>}},
        get(ConnectorType, ConnectorName2, TCConfigNS1)
    ),
    ?assertMatch({404, _}, get(ConnectorType, ConnectorName2, TCConfig)),
    ?assertMatch({404, _}, get(ConnectorType, ConnectorName2, TCConfigNS2)),

    %% Viewer cannot mutate anything, only read
    ?assertMatch({200, [_, _]}, list(TCConfigViewerNS1)),
    ?assertMatch({200, #{}}, get(ConnectorType, ConnectorName1, TCConfigViewerNS1)),
    ?assertMatch(
        {403, _},
        create(ConnectorType, ConnectorName1, ConnectorConfigNS1, TCConfigViewerNS1)
    ),
    ?assertMatch(
        {403, _},
        update(ConnectorType, ConnectorName1, ConnectorConfigNS1, TCConfigViewerNS1)
    ),
    ?assertMatch(
        {403, _},
        delete(ConnectorType, ConnectorName1, TCConfigViewerNS1)
    ),
    ?assertMatch(
        {403, _},
        start(ConnectorType, ConnectorName1, TCConfigViewerNS1)
    ),
    ?assertMatch(
        {403, _},
        disable(ConnectorType, ConnectorName1, TCConfigViewerNS1)
    ),
    ?assertMatch(
        {403, _},
        enable(ConnectorType, ConnectorName1, TCConfigViewerNS1)
    ),

    %% Delete
    ?assertMatch({204, _}, delete(ConnectorType, ConnectorName1, TCConfig)),
    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, [_, _]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),
    ?assertMatch(
        {error, not_found},
        get_resource(NoNamespace, ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {ok, _, _},
        get_resource(NS1, ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {ok, _, _},
        get_resource(NS2, ConnectorType, ConnectorName1, TCConfigNS1)
    ),

    ?assertMatch({204, _}, delete(ConnectorType, ConnectorName1, TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, [_]}, list(TCConfigNS1)),
    ?assertMatch({200, [_]}, list(TCConfigNS2)),

    ?assertMatch({204, _}, delete(ConnectorType, ConnectorName1, TCConfigNS2)),
    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, [_]}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),
    ?assertMatch(
        {error, not_found},
        get_resource(NoNamespace, ConnectorType, ConnectorName1, TCConfig)
    ),
    ?assertMatch(
        {error, not_found},
        get_resource(NS1, ConnectorType, ConnectorName1, TCConfigNS1)
    ),
    ?assertMatch(
        {error, not_found},
        get_resource(NS2, ConnectorType, ConnectorName1, TCConfigNS2)
    ),

    ?assertMatch({204, _}, delete(ConnectorType, ConnectorName2, TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfig)),
    ?assertMatch({200, []}, list(TCConfigNS1)),
    ?assertMatch({200, []}, list(TCConfigNS2)),

    ok.

-doc """
Checks that we correctly load and unload connectors already in the config when a node
restarts.
""".
t_namespaced_load_on_restart(TCConfig) ->
    [N1Spec | _] = emqx_bridge_v2_testlib:get_value(node_specs, TCConfig),
    [N1 | _] = emqx_bridge_v2_testlib:get_value(cluster_nodes, TCConfig),

    NS1 = <<"ns1">>,
    AuthHeaderNS1 = ensure_namespaced_api_key(NS1, TCConfig),
    TCConfigNS1 = [{auth_header, AuthHeaderNS1} | TCConfig],

    ConnectorType = <<"http">>,
    ConnectorName1 = <<"conn1">>,
    ConnectorConfigNS1 = http_connector_config(#{<<"description">> => NS1}),

    ?assertMatch(
        {201, #{<<"description">> := NS1, <<"status">> := <<"connected">>}},
        create(ConnectorType, ConnectorName1, ConnectorConfigNS1, TCConfigNS1)
    ),

    [N1] = emqx_cth_cluster:restart([N1Spec]),

    ?assertMatch({200, [#{<<"status">> := <<"connected">>}]}, list(TCConfigNS1)),
    ConnResId = emqx_connector_resource:resource_id(NS1, ConnectorType, ConnectorName1),
    ?assertMatch({ok, _, _}, ?ON(N1, emqx_resource:get_instance(ConnResId))),

    ok.
