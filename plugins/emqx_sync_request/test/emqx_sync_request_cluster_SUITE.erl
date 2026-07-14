%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_sync_request_cluster_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(REQ_PAYLOAD, <<"{\"cmd\":\"reboot\"}">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    try
        Package = emqx_sync_request_SUITE:plugin_package(),
        {ok, PackageBin} = file:read_file(Package),
        NameVsn = filename:basename(Package, ".tar.gz"),
        [
            {plugin_name_vsn, NameVsn},
            {plugin_package_bin, PackageBin}
            | Config
        ]
    catch
        error:{plugin_package_build_failed, _Package, Output} ->
            ct:log("plugin_package build failed: ~s", [Output]),
            {skip, "Run 'make compile-emqx-enterprise' first to build plugin dependencies."}
    end.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TestCase, Config) ->
    WorkDir = emqx_cth_suite:work_dir(TestCase, Config),
    Nodes = emqx_cth_cluster:start(cluster_specs(), #{work_dir => WorkDir}),
    ok = install_and_start_plugin_on_nodes(Nodes, Config),
    [{nodes, Nodes} | Config].

end_per_testcase(_TestCase, Config) ->
    Nodes = ?config(nodes, Config),
    ok = cleanup_plugin_on_nodes(Nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes).

t_request_on_one_node_receives_response_from_another_node(Config) ->
    Nodes = [HttpNode, ResponderNode] = ?config(nodes, Config),
    Parent = self(),
    ReqTopic = <<"sync_request/cluster/request">>,
    RespTopic = <<"sync_request/cluster/response">>,
    ReqId = <<"cluster-request-id">>,
    RespPayload = <<"cluster-response">>,
    {ok, Responder} = start_v5_responder_on_node(
        ResponderNode,
        <<"sync_request_cluster_responder">>,
        ReqTopic,
        fun(_Client, #{properties := Props, payload := Payload}) ->
            Parent ! {cluster_request_seen, Payload},
            {
                maps:get('Response-Topic', Props),
                #{'Correlation-Data' => maps:get('Correlation-Data', Props)},
                RespPayload
            }
        end
    ),
    try
        ok = emqx_cth_cluster:sync_routes(Nodes),
        Host = dashboard_host(HttpNode),
        Auth = erpc:call(HttpNode, emqx_mgmt_api_test_util, auth_header_, []),
        Body = emqx_sync_request_SUITE:request_body(
            ReqTopic, RespTopic, ReqId, #{timeout => <<"5s">>}
        ),
        {Status, ResponseMap} = emqx_sync_request_SUITE:do_http_request(Host, Auth, Body),
        ?assertEqual(200, Status),
        emqx_sync_request_SUITE:assert_response_payload(ResponseMap, RespPayload),
        ?assertReceive({cluster_request_seen, ?REQ_PAYLOAD}, 5000)
    after
        emqx_sync_request_SUITE:stop_client(Responder)
    end.

t_request_conflicts_when_exact_subscribers_exist_on_multiple_nodes(Config) ->
    Nodes = [HttpNode, ResponderNode] = ?config(nodes, Config),
    Parent = self(),
    ReqTopic = <<"sync_request/cluster/conflict/request">>,
    RespTopic = <<"sync_request/cluster/conflict/response">>,
    {ok, Responder1} = start_blackhole_responder_on_node(
        HttpNode,
        <<"sync_request_cluster_conflict_responder_1">>,
        ReqTopic,
        fun(Payload) -> Parent ! {cluster_conflict_request_seen, Payload} end
    ),
    {ok, Responder2} = start_blackhole_responder_on_node(
        ResponderNode,
        <<"sync_request_cluster_conflict_responder_2">>,
        ReqTopic,
        fun(Payload) -> Parent ! {cluster_conflict_request_seen, Payload} end
    ),
    try
        ok = emqx_cth_cluster:sync_routes(Nodes),
        Host = dashboard_host(HttpNode),
        Auth = erpc:call(HttpNode, emqx_mgmt_api_test_util, auth_header_, []),
        Body = emqx_sync_request_SUITE:request_body(
            ReqTopic, RespTopic, <<"cluster-conflict-request-id">>, #{timeout => <<"100ms">>}
        ),
        {Status, ResponseMap} = emqx_sync_request_SUITE:do_http_request(Host, Auth, Body),
        ?assertEqual(409, Status),
        ?assertMatch(
            #{
                <<"code">> := <<"CONFLICT">>,
                <<"message">> :=
                    <<"The request topic has a shared subscription or more than one exact subscriber.">>
            },
            ResponseMap
        ),
        ?assertNotReceive({cluster_conflict_request_seen, _}, 200)
    after
        emqx_sync_request_SUITE:stop_client(Responder1),
        emqx_sync_request_SUITE:stop_client(Responder2)
    end.

cluster_specs() ->
    Apps = [
        {emqx_conf, #{
            schema_mod => emqx_enterprise_schema,
            config => cluster_broker_config()
        }},
        {emqx, #{config => cluster_broker_config()}},
        {emqx_plugins, #{config => #{plugins => #{install_dir => "plugins"}}}}
    ],
    [
        {emqx_sync_request_cluster1, #{
            role => core,
            apps => Apps ++
                [
                    emqx_management,
                    cluster_dashboard()
                ]
        }},
        {emqx_sync_request_cluster2, #{role => core, apps => Apps}}
    ].

cluster_dashboard() ->
    {emqx_dashboard, #{
        config => "dashboard.listeners.http { enable = true, bind = 10183 }",
        before_start => fun() ->
            true = os:putenv("SCHEMA_MOD", "emqx_enterprise_schema"),
            {ok, _} = emqx_common_test_http:create_default_app()
        end
    }}.

cluster_broker_config() ->
    #{
        listeners => #{
            tcp => #{
                default => #{
                    enable_authn => false
                }
            }
        }
    }.

dashboard_host(Node) ->
    Port = erpc:call(Node, ranch, get_port, ['http:dashboard']),
    "http://127.0.0.1:" ++ integer_to_list(Port).

install_and_start_plugin_on_nodes(Nodes, Config) ->
    lists:foreach(
        fun(Node) -> ok = install_and_start_plugin_on_node(Node, Config) end,
        Nodes
    ).

install_and_start_plugin_on_node(Node, Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    PackageBin = ?config(plugin_package_bin, Config),
    Sha256 = binary:encode_hex(crypto:hash(sha256, PackageBin), lowercase),
    ok = erpc:call(Node, emqx_plugins, write_package, [NameVsn, PackageBin]),
    ok = erpc:call(Node, emqx_plugins, allow_installation, [NameVsn, Sha256]),
    ok = erpc:call(Node, emqx_plugins, ensure_installed, [NameVsn, fresh_install]),
    ok = erpc:call(Node, emqx_plugins, ensure_started, [NameVsn]).

cleanup_plugin_on_nodes(Nodes, Config) ->
    lists:foreach(
        fun(Node) -> ok = cleanup_plugin_on_node(Node, Config) end,
        Nodes
    ).

cleanup_plugin_on_node(Node, Config) ->
    NameVsn = ?config(plugin_name_vsn, Config),
    _ = erpc:call(Node, emqx_plugins, ensure_stopped, [NameVsn]),
    _ = erpc:call(Node, emqx_plugins, ensure_disabled, [NameVsn]),
    _ = erpc:call(Node, emqx_plugins, ensure_uninstalled, [NameVsn]),
    _ = erpc:call(Node, emqx_plugins, delete_package, [NameVsn]),
    _ = erpc:call(Node, emqx_plugins, forget_allowed_installation, [NameVsn]),
    ok.

start_v5_responder_on_node(Node, ClientId, ReqTopic, OnRequest) ->
    MsgHandler = #{
        publish => fun(Msg) ->
            Client = self(),
            case OnRequest(Client, Msg) of
                {RespTopic, RespProps, RespPayload} ->
                    emqx_sync_request_SUITE:publish_from_handler(
                        Client, RespTopic, RespProps, RespPayload, maps:get(qos, Msg, ?QOS_0)
                    );
                noreply ->
                    ok
            end
        end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    },
    start_subscriber_on_node(Node, ClientId, v5, ReqTopic, MsgHandler).

start_blackhole_responder_on_node(Node, ClientId, ReqTopic, OnRequest) ->
    MsgHandler = #{
        publish => fun(#{payload := Payload}) ->
            OnRequest(Payload),
            ok
        end,
        puback => fun(_Ack) -> ok end,
        disconnected => fun(_Reason) -> ok end
    },
    start_subscriber_on_node(Node, ClientId, v5, ReqTopic, MsgHandler).

start_subscriber_on_node(Node, ClientId, ProtoVer, ReqTopic, MsgHandler) ->
    {ok, Client} = emqx_sync_request_SUITE:start_client(ClientId, ProtoVer, MsgHandler, [
        {port, emqx_cth_cluster:get_tcp_mqtt_port(Node)}
    ]),
    {ok, _Props, [?QOS_0]} = emqtt:subscribe(Client, ReqTopic, ?QOS_0),
    {ok, Client}.
