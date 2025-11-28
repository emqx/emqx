%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/asserts.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("typerefl/include/types.hrl").
-include("emqx_mt.hrl").
-include_lib("../../emqx_prometheus/include/emqx_prometheus.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(NEW_CLIENTID(),
    iolist_to_binary("c-" ++ atom_to_list(?FUNCTION_NAME) ++ "-" ++ integer_to_list(?LINE))
).
-define(NEW_USERNAME(), iolist_to_binary("u-" ++ atom_to_list(?FUNCTION_NAME))).

-define(WAIT_FOR_DOWN(Pid, Timeout),
    (fun() ->
        receive
            {'DOWN', _, process, P, Reason} when Pid =:= P ->
                Reason
        after Timeout ->
            erlang:error(timeout)
        end
    end)()
).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(ON_ALL(NODES, BODY), erpc:multicall(NODES, fun() -> BODY end)).

-define(ALARM, <<"invalid_namespaced_configs">>).

-define(tcp, tcp).
-define(ws, ws).
-define(quic, quic).
-define(socket, socket).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(app_specs(), #{work_dir => emqx_cth_suite:work_dir(Config)}),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(Case, Config) ->
    snabbkaffe:start_trace(),
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    snabbkaffe:stop(),
    emqx_common_test_helpers:call_janitor(),
    ?MODULE:Case({'end', Config}),
    ok.

app_specs() ->
    [
        emqx,
        {emqx_conf, "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]"},
        emqx_mt,
        emqx_management
    ].

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

injected_fields() ->
    #{
        'roots.high' => [{foo, hoconsc:mk(hoconsc:ref(?MODULE, foo), #{})}]
    }.

fields(foo) ->
    [{bar, hoconsc:mk(persistent_term:get({?MODULE, bar_type}, binary()), #{})}].

connect(ClientId, Username) ->
    connect(#{clientid => ClientId, username => Username}).

connect(Opts0) ->
    DefaultOpts = #{proto_ver => v5},
    Opts = maps:merge(DefaultOpts, Opts0),
    {ok, Pid} = emqtt:start_link(Opts),
    monitor(process, Pid),
    unlink(Pid),
    ConnectFn = maps:get(connect_fn, Opts, fun emqtt:connect/1),
    case ConnectFn(Pid) of
        {ok, _} ->
            Pid;
        {error, _Reason} = E ->
            catch emqtt:stop(Pid),
            receive
                {'DOWN', _, process, Pid, _, _} -> ok
            after 3000 ->
                exit(Pid, kill)
            end,
            erlang:error(E)
    end.

setup_corrupt_namespace_scenario(TestCase, TCConfig) ->
    {ok, Agent} = emqx_utils_agent:start_link(_BarType0 = binary()),
    SetType = fun() ->
        BarType = emqx_utils_agent:get(Agent),
        persistent_term:put({?MODULE, bar_type}, BarType)
    end,
    AppSpecs = [
        {emqx_conf, #{
            before_start =>
                fun(App, AppCfg) ->
                    SetType(),
                    ok = emqx_config:add_allowed_namespaced_config_root(<<"foo">>),
                    ok = emqx_schema_hooks:inject_from_modules([?MODULE]),
                    emqx_cth_suite:inhibit_config_loader(App, AppCfg)
                end
        }},
        emqx,
        emqx_management,
        {emqx_mt, #{
            after_start =>
                fun() ->
                    {ok, _} = emqx:update_config(
                        [mqtt, client_attrs_init],
                        [
                            #{
                                <<"expression">> => <<"username">>,
                                <<"set_as_attr">> => <<"tns">>
                            }
                        ]
                    )
                end
        }}
    ],
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(
        [
            {corrupt_namespace_cfg1, #{
                apps => AppSpecs ++ [emqx_mgmt_api_test_util:emqx_dashboard()]
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    ct:pal("starting cluster"),
    Nodes = [N1] = emqx_cth_cluster:start(NodeSpecs),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    ct:timetrap({seconds, 15}),
    ct:pal("cluster started"),
    ?assertMatch([_], ?ON(N1, emqx:get_config([mqtt, client_attrs_init]))),
    #{
        nodes => Nodes,
        node_specs => NodeSpecs,
        schema_agent => Agent,
        set_type_fn => SetType
    }.

apply_jq(Transformation, NssToConfigs) ->
    maps:map(
        fun(_Ns, CfgIn) ->
            CfgInBin = emqx_utils_json:encode(CfgIn),
            {ok, [CfgOutBin]} = jq:process_json(Transformation, CfgInBin),
            #{} = emqx_utils_json:decode(CfgOutBin)
        end,
        NssToConfigs
    ).

restart_node(Node, NodeSpec) ->
    %% N.B. For some reason, even using `shutdown => 5_000`, mnesia does not seem to
    %% correctly sync/flush data to disk when restarting the peer node.  We call
    %% `mnesia:sync_log` here to force it to sync data so that it's correctly loaded when
    %% the peer restarts.  Without this, the table is empty after the restart...
    ?ON(Node, ok = mnesia:sync_log()),
    [Node] = emqx_cth_cluster:restart([NodeSpec]),
    ok.

connection_type_of(TCConfig) ->
    emqx_common_test_helpers:get_matrix_prop(TCConfig, [?tcp, ?ws, ?quic, ?socket], ?tcp).

%% Assumes local node is the SUT.
connect_opts_of(TCConfig) ->
    case connection_type_of(TCConfig) of
        ?tcp ->
            #{connect_fn => fun emqtt:connect/1};
        ?ws ->
            {_, Port} = emqx:get_config([listeners, ws, default, bind]),
            #{
                connect_fn => fun emqtt:ws_connect/1,
                hosts => [{"127.0.0.1", Port}],
                ws_transport_options => [
                    {protocols, [http]},
                    {transport, tcp}
                ]
            };
        ?quic ->
            {listener, {LType, LName}} = lists:keyfind(listener, 1, TCConfig),
            {_, Port} = emqx:get_config([listeners, LType, LName, bind]),
            #{
                connect_fn => fun emqtt:quic_connect/1,
                hosts => [{"127.0.0.1", Port}],
                ssl => true,
                ssl_opts => [{verify, verify_none}, {alpn, ["mqtt"]}]
            };
        ?socket ->
            {listener, {LType, LName}} = lists:keyfind(listener, 1, TCConfig),
            {_, Port} = emqx:get_config([listeners, LType, LName, bind]),
            #{
                connect_fn => fun emqtt:connect/1,
                port => Port
            }
    end.

assert_client_connection_consistent(ClientPid, TCConfig) ->
    ExpectedMod =
        case connection_type_of(TCConfig) of
            ?tcp -> emqx_connection;
            ?ws -> emqx_ws_connection;
            ?quic -> emqx_connection;
            ?socket -> emqx_socket_connection
        end,
    ?assertEqual(ExpectedMod, emqx_cth_broker:connection_info(connmod, ClientPid)),
    case connection_type_of(TCConfig) of
        ?quic ->
            ?assertEqual(quic, emqx_cth_broker:connection_info(socktype, ClientPid));
        _ ->
            ok
    end,
    ok.

reset_global_metrics() ->
    lists:foreach(
        fun({Name, _Val}) ->
            emqx_metrics:set_global(Name, 0)
        end,
        emqx_metrics:all_global()
    ).

generate_tls_certs(TCConfig) ->
    PrivDir = ?config(priv_dir, TCConfig),
    CertDir = filename:join(PrivDir, "tls"),
    ok = filelib:ensure_path(CertDir),
    CertKeyRoot = emqx_cth_tls:gen_cert(#{key => ec, issuer => root}),
    CertKeyServer = emqx_cth_tls:gen_cert(#{
        key => ec,
        issuer => CertKeyRoot,
        extensions => #{subject_alt_name => [{ip, {127, 0, 0, 1}}]}
    }),
    {CertfileCA, _} = emqx_cth_tls:write_cert(CertDir, CertKeyRoot),
    {Certfile, Keyfile} = emqx_cth_tls:write_cert(CertDir, CertKeyServer),
    #{
        <<"cacertfile">> => CertfileCA,
        <<"certfile">> => Certfile,
        <<"keyfile">> => Keyfile
    }.

setup_namespaced_metrics_channel_scenario(TCConfig) ->
    %% Explicit namespace
    Namespace = <<"explicit_ns">>,
    ok = emqx_mt_config:create_managed_ns(Namespace),
    Listener =
        case connection_type_of(TCConfig) of
            ?socket ->
                %% Socket listener
                SocketLName = socket,
                SocketLPort = emqx_common_test_helpers:select_free_port(tcp),
                SocektLConfig = #{
                    <<"bind">> => iolist_to_binary(
                        emqx_listeners:format_bind({"127.0.0.1", SocketLPort})
                    ),
                    <<"tcp_backend">> => <<"socket">>
                },
                {ok, _} = emqx:update_config(
                    [listeners, tcp, SocketLName], {create, SocektLConfig}
                ),
                {tcp, SocketLName};
            ?quic ->
                %% Quic listener
                QuicLName = quic,
                QuicLPort = emqx_common_test_helpers:select_free_port(quic),
                QuicSSLOpts = generate_tls_certs(TCConfig),
                QuicLConfig = #{
                    <<"bind">> => iolist_to_binary(
                        emqx_listeners:format_bind({"127.0.0.1", QuicLPort})
                    ),
                    <<"ssl_options">> => QuicSSLOpts
                },
                {ok, _} = emqx:update_config([listeners, quic, QuicLName], {create, QuicLConfig}),
                {quic, QuicLName};
            _ ->
                undefined
        end,
    reset_global_metrics(),
    [
        {explicit_ns, Namespace},
        {listener, Listener}
        | TCConfig
    ].

teardown_namespaced_metrics_channel_scenario(TCConfig) ->
    maybe
        {LType, LName} ?= ?config(listener, TCConfig),
        ok = emqx_listeners:stop_listener(emqx_listeners:listener_id(LType, LName)),
        {ok, _} = emqx:remove_config([listeners, LType, LName])
    end,
    Namespace = ?config(explicit_ns, TCConfig),
    ok = emqx_mt_config:delete_managed_ns(Namespace),
    %% Wait for cleanup
    ?retry(250, 10, ?assertNot(emqx_mt_state:is_tombstoned(Namespace))),
    reset_global_metrics(),
    delete_all_namespaces(),
    ok.

delete_all_namespaces() ->
    do_delete_all_namespaces(?MIN_NS).

do_delete_all_namespaces(LastNs) ->
    case emqx_mt:list_ns(LastNs, 100) of
        [] ->
            ok;
        Nss ->
            lists:foreach(fun emqx_mt_config:delete_managed_ns/1, Nss),
            NewLastNs = lists:last(Nss),
            do_delete_all_namespaces(NewLastNs)
    end.

assert_namespaced_metrics_channel_explicit(Namespace, ClientId, TCConfig, ClientSubPubFn) ->
    %% Fresh namespace with no metrics.
    ?assertEqual(0, emqx_metrics:val_global('messages.publish')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.publish')),
    ?assertEqual(0, emqx_metrics:val_global('messages.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.received')),
    ?assertEqual(0, emqx_metrics:val_global('bytes.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.received')),
    ?assertEqual(0, emqx_metrics:val_global('bytes.sent')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.sent')),
    ?assertEqual(0, emqx_metrics:val_global('packets.sent')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'packets.sent')),
    ?assertEqual(0, emqx_metrics:val_global('packets.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'packets.received')),

    %% Connect the client and trigger some activity.
    Opts0 = #{clientid => ClientId, username => Namespace},
    Opts1 = connect_opts_of(TCConfig),
    Opts = maps:merge(Opts1, Opts0),
    C1 = connect(Opts),
    %% Sanity check
    assert_client_connection_consistent(C1, TCConfig),
    Topic = <<"t">>,
    ClientSubPubFn(C1, Topic),
    ?assertReceive({publish, _}),
    %% Both global and namespaced metrics should be bumped.
    ?assertEqual(1, emqx_metrics:val_global('messages.publish')),
    ?assertEqual(1, emqx_metrics:val(Namespace, 'messages.publish')),
    ?assertEqual(1, emqx_metrics:val_global('messages.received')),
    ?assertEqual(1, emqx_metrics:val(Namespace, 'messages.received')),
    ?assert(0 < emqx_metrics:val_global('bytes.received')),
    ?assert(0 < emqx_metrics:val(Namespace, 'bytes.received')),
    ?assert(0 < emqx_metrics:val_global('bytes.sent')),
    ?assert(0 < emqx_metrics:val(Namespace, 'bytes.sent')),
    ?assert(0 < emqx_metrics:val_global('packets.sent')),
    ?assert(0 < emqx_metrics:val(Namespace, 'packets.sent')),
    ?assert(0 < emqx_metrics:val_global('packets.received')),
    ?assert(0 < emqx_metrics:val(Namespace, 'packets.received')),
    ok.

assert_namespaced_metrics_channel_implicit(Namespace, ClientId, TCConfig, ClientSubPubFn) ->
    %% Fresh namespace with no metrics.
    ?assertEqual(0, emqx_metrics:val_global('messages.publish')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.publish')),
    ?assertEqual(0, emqx_metrics:val_global('messages.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.received')),
    ?assertEqual(0, emqx_metrics:val_global('bytes.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.received')),
    ?assertEqual(0, emqx_metrics:val_global('bytes.sent')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.sent')),
    ?assertEqual(0, emqx_metrics:val_global('packets.sent')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'packets.sent')),

    %% Connect the client and trigger some activity.
    Opts0 = #{clientid => ClientId, username => Namespace},
    Opts1 = connect_opts_of(TCConfig),
    Opts = maps:merge(Opts1, Opts0),
    C1 = connect(Opts),
    %% Sanity check
    assert_client_connection_consistent(C1, TCConfig),
    Topic = <<"t">>,
    ClientSubPubFn(C1, Topic),
    ?assertReceive({publish, _}),
    %% Only global metrics should be bumped.
    ?assertEqual(1, emqx_metrics:val_global('messages.publish')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.publish')),
    ?assertEqual(1, emqx_metrics:val_global('messages.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'messages.received')),
    ?assert(0 < emqx_metrics:val_global('bytes.received')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.received')),
    ?assert(0 < emqx_metrics:val_global('bytes.sent')),
    ?assertEqual(0, emqx_metrics:val(Namespace, 'bytes.sent')),
    ok.

mk_cluster(TestCase, #{n := NumNodes} = _Opts, TCConfig) ->
    AppSpecs = [
        {emqx_conf,
            "mqtt.client_attrs_init = [{expression = username, set_as_attr = tns}]\n"
            "authentication = [{mechanism = password_based, backend = built_in_database}]"},
        emqx_auth_mnesia,
        emqx_auth,
        emqx_mt,
        {emqx_prometheus, "prometheus.namespaced_metrics_limiter.rate = infinity"},
        emqx_management
    ],
    MkDashApp = fun(N) ->
        Port = 18083 + N - 1,
        PortStr = integer_to_list(Port),
        [
            emqx_mgmt_api_test_util:emqx_dashboard(
                "dashboard.listeners.http.bind = " ++ PortStr
            )
        ]
    end,
    NodeSpecs0 = lists:map(
        fun(N) ->
            Name = mk_node_name(TestCase, N),
            {Name, #{apps => AppSpecs ++ MkDashApp(N)}}
        end,
        lists:seq(1, NumNodes)
    ),
    Nodes = emqx_cth_cluster:start(
        NodeSpecs0,
        #{work_dir => emqx_cth_suite:work_dir(TestCase, TCConfig)}
    ),
    on_exit(fun() -> ok = emqx_cth_cluster:stop(Nodes) end),
    Nodes.

mk_node_name(TestCase, N) ->
    Name0 = iolist_to_binary([atom_to_binary(TestCase), "_", integer_to_binary(N)]),
    binary_to_atom(Name0).

get_prometheus_ns_stats(Namespace, Mode, Format) ->
    Headers =
        case Format of
            json -> [{"accept", "application/json"}];
            prometheus -> []
        end,
    QueryString = uri_string:compose_query(
        lists:flatten([
            {"mode", atom_to_binary(Mode)},
            [{"ns", Namespace} || Namespace /= all]
        ])
    ),
    URL = emqx_mgmt_api_test_util:api_path(["prometheus", "namespaced_stats"]),
    {Status, Response} = emqx_mgmt_api_test_util:simple_request(#{
        method => get,
        url => URL,
        extra_headers => Headers,
        query_params => QueryString,
        auth_header => {"no", "auth"}
    }),
    case Format of
        json ->
            {Status, Response};
        prometheus when Status == 200 ->
            {Status, parse_prometheus(Response)};
        prometheus ->
            {Status, Response}
    end.

parse_prometheus(RawData) ->
    lists:foldl(
        fun
            (<<"#", _/binary>>, Acc) ->
                Acc;
            (Line, Acc) ->
                {Name, Labels, Value} = parse_prometheus_line(Line),
                maps:update_with(
                    Name,
                    fun(Old) -> Old#{Labels => Value} end,
                    #{Labels => Value},
                    Acc
                )
        end,
        #{},
        binary:split(iolist_to_binary(RawData), <<"\n">>, [global, trim_all])
    ).

parse_prometheus_line(Line) ->
    RE = <<"(?<name>[a-z0-9A-Z_]+)(\\{(?<labels>[^)]*)\\})? *(?<value>[0-9]+(\\.[0-9]+)?)">>,
    {match, [Name, Labels0, Value0]} = re:run(
        Line, RE, [{capture, [<<"name">>, <<"labels">>, <<"value">>], binary}]
    ),
    Labels = parse_prometheus_labels(Labels0),
    Value =
        try
            binary_to_float(Value0)
        catch
            error:badarg ->
                binary_to_integer(Value0)
        end,
    {Name, Labels, Value}.

parse_prometheus_labels(<<"">>) ->
    #{};
parse_prometheus_labels(Labels) ->
    lists:foldl(
        fun(Label, Acc) ->
            [K, V0] = binary:split(Label, <<"=">>),
            V = binary:replace(V0, <<"\"">>, <<"">>, [global]),
            Acc#{K => V}
        end,
        #{},
        binary:split(Labels, <<",">>, [global])
    ).

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_connect_disconnect({init, Config}) ->
    Config;
t_connect_disconnect({'end', _Config}) ->
    ok;
t_connect_disconnect(_Config) ->
    ClientId = ?NEW_CLIENTID(),
    Username = ?NEW_USERNAME(),
    Pid = connect(ClientId, Username),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Username)),
    ?assertEqual({error, not_found}, emqx_mt:count_clients(<<"unknown">>)),
    ?assertEqual({ok, [ClientId]}, emqx_mt:list_clients(Username)),
    ?assertEqual({error, not_found}, emqx_mt:list_clients(<<"unknown">>)),
    ?assertEqual([Username], emqx_mt:list_ns()),
    ok = emqtt:stop(Pid),
    ?assertMatch(
        {ok, #{tns := Username, clientid := ClientId}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_proc_deleted},
            3000
        )
    ),
    ok.

t_session_limit_exceeded({init, Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1),
    Config;
t_session_limit_exceeded({'end', _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(infinity);
t_session_limit_exceeded(_Config) ->
    Ns = ?NEW_USERNAME(),
    C1 = ?NEW_CLIENTID(),
    C2 = ?NEW_CLIENTID(),
    Pid1 = connect(C1, Ns),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns)),
    %% two reasons may race
    try
        {ok, _} = connect(C2, Ns),
        ct:fail("unexpected success")
    catch
        error:{error, {quota_exceeded, _}} ->
            ok;
        exit:{shutdown, quota_exceeded} ->
            ok
    end,
    ok = emqtt:stop(Pid1).

%% if a client reconnects, it should not consume the session quota
t_session_reconnect({init, Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(1),
    Config;
t_session_reconnect({'end', _Config}) ->
    emqx_mt_config:tmp_set_default_max_sessions(infinity);
t_session_reconnect(_Config) ->
    Ns = ?NEW_USERNAME(),
    C1 = ?NEW_CLIENTID(),
    Pid1 = connect(C1, Ns),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_added},
            3000
        )
    ),
    ?assertEqual({ok, 1}, emqx_mt:count_clients(Ns)),
    {Pid2, {ok, #{tns := Ns, clientid := C1, proc := CPid2}}} =
        ?wait_async_action(
            connect(C1, Ns),
            #{?snk_kind := multi_tenant_client_added},
            3000
        ),
    R = ?WAIT_FOR_DOWN(Pid1, 3000),
    ?assertMatch({shutdown, {disconnected, ?RC_SESSION_TAKEN_OVER, _}}, R),
    ok = emqtt:stop(Pid2),
    _ = ?WAIT_FOR_DOWN(Pid2, 3000),
    ?assertMatch(
        {ok, #{tns := Ns, clientid := C1}},
        ?block_until(
            #{?snk_kind := multi_tenant_client_proc_deleted, proc := CPid2},
            3000
        )
    ),
    %% ok = emqx_mt_state:evict_ccache(Ns),
    ?assertEqual({ok, 0}, emqx_mt:count_clients(Ns)),
    ok.

%% Verifies that we initialize existing limiter groups when booting up the node.
t_initialize_limiter_groups({init, Config}) ->
    ClusterSpec = [{mt_initialize1, #{apps => app_specs()}}],
    ClusterOpts = #{
        work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
        shutdown => 5_000
    },
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(ClusterSpec, ClusterOpts),
    Cluster = emqx_cth_cluster:start(NodeSpecs),
    [{cluster, Cluster}, {node_specs, NodeSpecs} | Config];
t_initialize_limiter_groups({'end', Config}) ->
    Cluster = ?config(cluster, Config),
    ok = emqx_cth_cluster:stop(Cluster),
    ok;
t_initialize_limiter_groups(Config) when is_list(Config) ->
    [N] = ?config(cluster, Config),
    [NodeSpec] = ?config(node_specs, Config),
    %% Setup namespace with limiters
    Params1 = emqx_mt_api_SUITE:tenant_limiter_params(),
    Params2 = emqx_mt_api_SUITE:client_limiter_params(),
    Params = emqx_utils_maps:deep_merge(Params1, Params2),
    Ns = atom_to_binary(?FUNCTION_NAME),
    ?ON(N, begin
        ok = emqx_mt_config:create_managed_ns(Ns),
        {ok, _} = emqx_mt_config:update_managed_ns_config(Ns, Params)
    end),
    %% Restart node
    ok = restart_node(N, NodeSpec),
    %% Client should connect fine
    ?check_trace(
        begin
            C1 = ?NEW_CLIENTID(),
            {ok, Pid1} = emqtt:start_link(#{
                username => Ns,
                clientid => C1,
                proto_ver => v5,
                port => emqx_mt_api_SUITE:get_mqtt_tcp_port(N)
            }),
            {ok, _} = emqtt:connect(Pid1),
            emqtt:stop(Pid1)
        end,
        fun(Trace) ->
            ?assertEqual([], ?of_kind(["hook_callback_exception"], Trace)),
            ok
        end
    ),
    ok.

-doc """
When (re)starting a node, it's possible that due to a bug or to intentionally
non-backwards compatible schema changes, we have persisted namespaced configs that are
invalid in mnesia.

This test attempts to emulate that situation, and verify that we _can_ start the node
normally, but such namespace will be "unavailable" in the sense that some operations such
as MQTT clients attempting to connect or data integrations will not work until the
configuration is manually fixed.

Note that this is just an emulation of such situation, because here in tests we don't
start the peer node exactly how it happens in a release.  For a real test, see the boot
tests that exist outside CT in this repo.
""".
t_namespaced_bad_config_during_start({init, Config}) ->
    Config;
t_namespaced_bad_config_during_start({'end', _Config}) ->
    ok;
t_namespaced_bad_config_during_start(Config) when is_list(Config) ->
    Ns = <<"some_namespace">>,
    #{
        nodes := [N1 | _],
        node_specs := [N1Spec | _],
        schema_agent := Agent
    } = setup_corrupt_namespace_scenario(?FUNCTION_NAME, Config),
    ct:pal("seeding namespace"),
    ?ON(
        N1,
        ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(
            emqx_schema, Ns
        )
    ),
    ct:pal("seeded namespace"),
    ct:pal("updating config that will become \"corrupt\" later on"),
    {ok, #{
        config := #{bar := <<"hello">>},
        namespace := Ns,
        raw_config := #{<<"bar">> := <<"hello">>}
    }} =
        ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => <<"hello">>}, #{namespace => Ns})),
    ct:pal("injecting failure"),
    ok = emqx_utils_agent:set(Agent, _BarType1 = integer()),
    ct:pal("restarting node; should not fail to start"),
    {[N1], {ok, _}} =
        ?wait_async_action(
            emqx_cth_cluster:restart([N1Spec]),
            #{?snk_kind := "corrupt_ns_checker_started"}
        ),
    ?assertMatch(#{<<"foo">> := _}, ?ON(N1, emqx_config:get_namespace_config_errors(Ns))),
    Alarms1 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    ?assertMatch(
        [
            #{
                name := ?ALARM,
                message := <<"Namespaces with invalid configurations">>,
                details := #{
                    problems :=
                        #{
                            Ns :=
                                #{<<"foo">> := #{<<"kind">> := <<"validation_error">>}}
                        }
                }
            }
        ],
        [A || A = #{name := ?ALARM} <- Alarms1]
    ),
    %% Using the same value (now invalid)
    ?assertMatch(
        {error, #{reason := "Unable to parse integer value"}},
        ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => <<"hello">>}, #{namespace => Ns}))
    ),
    ?assertMatch([_], ?ON(N1, emqx:get_config([mqtt, client_attrs_init]))),
    ClientId = ?NEW_CLIENTID(),
    Port1 = emqx_mt_api_SUITE:get_mqtt_tcp_port(N1),
    ?assertError(
        {error, {server_unavailable, _}},
        connect(#{
            clientid => ClientId,
            username => Ns,
            port => Port1
        })
    ),
    %% With valid values of new type; should succeed and heal the node
    ?assertMatch(
        {{ok, #{namespace := Ns, config := #{bar := 1}, raw_config := #{<<"bar">> := 1}}}, {ok, _}},
        ?wait_async_action(
            ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => 1}, #{namespace => Ns})),
            #{?snk_kind := "corrupt_ns_checker_checked", ns := Ns}
        )
    ),
    ?assertMatch(undefined, ?ON(N1, emqx_config:get_namespace_config_errors(Ns))),
    KeyPath = [foo, bar],
    ?assertMatch(1, ?ON(N1, emqx:get_namespaced_config(Ns, KeyPath))),
    ?assertMatch(1, ?ON(N1, emqx:get_raw_namespaced_config(Ns, KeyPath))),
    Alarms2 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    ?assertMatch([], [A || A = #{name := ?ALARM} <- Alarms2]),
    ok.

-doc """
Verifies the behavior of the bulk fix API for corrupt namespaced configurations.

When (re)starting a node, it's possible that due to a bug or to intentionally
non-backwards compatible schema changes, we have persisted namespaced configs that are
invalid in mnesia.

Assuming that such problems will happen to multiple namespaces in a similar way (due to
bugs or intentional breaking changes), we have introduced this API which uses `jq`
programs to apply the same transformation to multiple configurations without the need to
manually go through each one.
""".
t_namespaced_bad_config_bulk_fix({init, TCConfig}) ->
    TCConfig;
t_namespaced_bad_config_bulk_fix({'end', _TCConfig}) ->
    ok;
t_namespaced_bad_config_bulk_fix(TCConfig) when is_list(TCConfig) ->
    #{
        nodes := [N1 | _] = Nodes,
        node_specs := NodeSpecs,
        schema_agent := Agent
    } = setup_corrupt_namespace_scenario(?FUNCTION_NAME, TCConfig),
    Namespaces = [<<"ns", (integer_to_binary(I))/binary>> || I <- lists:seq(1, 4)],
    {CorruptNamespaces, OtherNamespaces} = lists:split(2, Namespaces),
    ct:pal("seeding namespaces"),
    ?ON(
        N1,
        lists:foreach(
            fun(Ns) ->
                {204, _} = emqx_mt_api_SUITE:create_managed_ns(Ns),
                ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(
                    emqx_schema, Ns
                )
            end,
            Namespaces
        )
    ),
    ct:pal("seeded namespaces"),
    ct:pal("updating config that will become \"corrupt\" later on"),
    ?ON(
        N1,
        lists:foreach(
            fun(Ns) ->
                {ok, #{
                    config := #{bar := <<"hello">>},
                    namespace := Ns,
                    raw_config := #{<<"bar">> := <<"hello">>}
                }} =
                    ?ON(
                        N1, emqx_conf:update([foo], #{<<"bar">> => <<"hello">>}, #{namespace => Ns})
                    )
            end,
            CorruptNamespaces
        )
    ),
    ct:pal("injecting failure"),
    ok = emqx_utils_agent:set(Agent, _BarType1 = integer()),
    ct:pal("restarting node; should not fail to start"),
    {Nodes, {ok, _}} =
        ?wait_async_action(
            emqx_cth_cluster:restart(NodeSpecs),
            #{?snk_kind := "corrupt_ns_checker_started"}
        ),
    %% Adding new, valid value to other namespaces
    ?ON(
        N1,
        lists:foreach(
            fun(Ns) ->
                {ok, #{
                    config := #{bar := 1},
                    namespace := Ns,
                    raw_config := #{<<"bar">> := 1}
                }} =
                    ?ON(N1, emqx_conf:update([foo], #{<<"bar">> => 1}, #{namespace => Ns}))
            end,
            OtherNamespaces
        )
    ),
    Alarms1 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    [
        #{
            name := ?ALARM,
            message := <<"Namespaces with invalid configurations">>,
            details := #{problems := Problems1}
        }
    ] = [A || A = #{name := ?ALARM} <- Alarms1],
    ?assertEqual(lists:sort(CorruptNamespaces), lists:sort(maps:keys(Problems1))),

    GlobalAuthHeader = ?ON(N1, emqx_mgmt_api_test_util:auth_header_()),
    emqx_mt_api_SUITE:put_auth_header(GlobalAuthHeader),

    {200, BadNssToConfigs} = emqx_mt_api_SUITE:bulk_export_ns_configs(#{
        <<"namespaces">> => CorruptNamespaces
    }),
    ?assertEqual(lists:sort(CorruptNamespaces), lists:sort(maps:keys(BadNssToConfigs))),

    %% Unknown namespaces
    ?assertMatch(
        {400, #{<<"message">> := <<"unknown_namespaces">>, <<"unknown">> := [<<"unknown_ns">>]}},
        emqx_mt_api_SUITE:bulk_export_ns_configs(#{
            <<"namespaces">> => [<<"unknown_ns">>]
        })
    ),

    %% Lets try to fix the namespaces with the wrong transformations; should fail.
    %%   - bad type
    ?assertMatch(
        {400, #{
            <<"message">> := <<"errors_importing_configurations">>,
            <<"errors">> := #{
                <<"ns1">> := #{
                    <<"reason">> := <<"bad_resulting_configuration">>,
                    <<"details">> := [#{<<"kind">> := <<"validation_error">>}]
                },
                <<"ns2">> := #{
                    <<"reason">> := <<"bad_resulting_configuration">>,
                    <<"details">> := [#{<<"kind">> := <<"validation_error">>}]
                }
            }
        }},
        emqx_mt_api_SUITE:bulk_import_ns_configs(#{
            <<"configs">> => apply_jq(
                <<".foo.bar=\"still_wrong_type\"">>,
                BadNssToConfigs
            ),
            <<"dry_run">> => false
        })
    ),
    %%   - resulting config has unknown keys
    ?assertMatch(
        {400, #{
            <<"message">> := <<"errors_importing_configurations">>,
            <<"errors">> := #{
                <<"ns1">> := #{
                    <<"reason">> := <<"bad_resulting_configuration">>,
                    <<"details">> := [#{<<"kind">> := <<"validation_error">>}]
                },
                <<"ns2">> := #{
                    <<"reason">> := <<"bad_resulting_configuration">>,
                    <<"details">> := [#{<<"kind">> := <<"validation_error">>}]
                }
            }
        }},
        emqx_mt_api_SUITE:bulk_import_ns_configs(#{
            <<"configs">> => apply_jq(
                <<".foo.bar=123 | .foo.unknown_key=true">>,
                BadNssToConfigs
            ),
            <<"dry_run">> => false
        })
    ),
    %% Now, lets fix the namespaces in bulk.
    %% First a dry-run; shouldn't do anything
    ?assertMatch(
        {200, #{
            <<"ns1">> := #{<<"foo">> := #{<<"bar">> := 123}},
            <<"ns2">> := #{<<"foo">> := #{<<"bar">> := 123}}
        }},
        emqx_mt_api_SUITE:bulk_import_ns_configs(#{
            <<"configs">> => apply_jq(
                <<".foo.bar=123">>,
                BadNssToConfigs
            ),
            <<"dry_run">> => true
        })
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertMatch(
                <<"hello">>,
                ?ON(N1, emqx:get_raw_namespaced_config(Ns, [<<"foo">>, <<"bar">>])),
                #{ns => Ns}
            )
        end,
        CorruptNamespaces
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertMatch(
                1,
                ?ON(N1, emqx:get_raw_namespaced_config(Ns, [<<"foo">>, <<"bar">>])),
                #{ns => Ns}
            )
        end,
        OtherNamespaces
    ),
    %% Now for real.
    ?assertMatch(
        {200, #{
            <<"ns1">> := #{<<"foo">> := #{<<"bar">> := 123}},
            <<"ns2">> := #{<<"foo">> := #{<<"bar">> := 123}}
        }},
        emqx_mt_api_SUITE:bulk_import_ns_configs(#{
            <<"configs">> => apply_jq(
                <<".foo.bar=123">>,
                BadNssToConfigs
            ),
            <<"dry_run">> => false
        })
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertMatch(
                123,
                ?ON(N1, emqx:get_raw_namespaced_config(Ns, [<<"foo">>, <<"bar">>])),
                #{ns => Ns}
            )
        end,
        CorruptNamespaces
    ),
    lists:foreach(
        fun(Ns) ->
            ?assertMatch(
                1,
                ?ON(N1, emqx:get_raw_namespaced_config(Ns, [<<"foo">>, <<"bar">>])),
                #{ns => Ns}
            )
        end,
        OtherNamespaces
    ),
    Alarms2 = ?ON(N1, emqx_alarm:get_alarms(activated)),
    ?assertEqual(
        [],
        [A || A = #{name := ?ALARM} <- Alarms2]
    ),
    ok.

-doc """
Verifies that we initialize metrics when creating a new managed namespace and when loading
existing namespaces while restarting a node.
""".
t_namespaced_metrics({init, Config}) ->
    ClusterSpec = [
        {ns_metrics1, #{apps => app_specs()}},
        {ns_metrics2, #{apps => app_specs()}}
    ],
    ClusterOpts = #{
        work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
        shutdown => 5_000
    },
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(ClusterSpec, ClusterOpts),
    Cluster = emqx_cth_cluster:start(NodeSpecs),
    [{cluster, Cluster}, {node_specs, NodeSpecs} | Config];
t_namespaced_metrics({'end', Config}) ->
    Cluster = ?config(cluster, Config),
    ok = emqx_cth_cluster:stop(Cluster),
    ok;
t_namespaced_metrics(Config) when is_list(Config) ->
    [N1, N2] = ?config(cluster, Config),
    [_, NSpec2] = ?config(node_specs, Config),
    Ns = atom_to_binary(?FUNCTION_NAME),
    %% Before namespace exists, we don't manipulate its metrics.
    ?assertError({exception, badarg, _}, ?ON(N1, emqx_metrics:inc(Ns, 'messages.received'))),
    ?assertError({exception, badarg, _}, ?ON(N2, emqx_metrics:inc(Ns, 'messages.received'))),
    ?ON(N1, ok = emqx_mt_config:create_managed_ns(Ns)),
    %% Can manipulate namespace metrics on both nodes now.
    ?assertEqual(ok, ?ON(N1, emqx_metrics:inc(Ns, 'messages.received'))),
    ?assertEqual(1, ?ON(N1, emqx_metrics:val(Ns, 'messages.received'))),
    ?assertEqual(ok, ?ON(N2, emqx_metrics:inc(Ns, 'messages.received', 2))),
    ?assertEqual(2, ?ON(N2, emqx_metrics:val(Ns, 'messages.received'))),
    %% Restart node; should re-register namespace metrics.
    ok = restart_node(N2, NSpec2),
    ?assertEqual(ok, ?ON(N2, emqx_metrics:inc(Ns, 'messages.received', 3))),
    ?assertEqual(3, ?ON(N2, emqx_metrics:val(Ns, 'messages.received'))),
    %% Delete the managed namespace; should drop namespace metrics.
    ?ON(N1, ok = emqx_mt_config:delete_managed_ns(Ns)),
    %% Wait for cleanup
    ?retry(250, 10, ?assertNot(?ON(N2, emqx_mt_state:is_tombstoned(Ns)))),
    ?assertError({exception, badarg, _}, ?ON(N1, emqx_metrics:inc(Ns, 'messages.received'))),
    ?assertError({exception, badarg, _}, ?ON(N2, emqx_metrics:inc(Ns, 'messages.received'))),
    ok.

-doc """
Verifies that we bump **both** global and namespaced metrics for a channel that belongs to
an **explicit** namespace.
""".
t_namespaced_metrics_channel_explicit() ->
    [{matrix, true}].
t_namespaced_metrics_channel_explicit(matrix) ->
    [[?tcp], [?quic], [?ws], [?socket]];
t_namespaced_metrics_channel_explicit({init, TCConfig}) ->
    setup_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_explicit({'end', TCConfig}) ->
    teardown_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_explicit(TCConfig) when is_list(TCConfig) ->
    Namespace = ?config(explicit_ns, TCConfig),
    ClientId = ?NEW_CLIENTID(),
    ClientSubPubFn = fun(Client, Topic) ->
        {ok, _, _} = emqtt:subscribe(Client, Topic, [{qos, 1}]),
        emqtt:publish(Client, Topic, <<"hey1">>, [{qos, 1}])
    end,
    assert_namespaced_metrics_channel_explicit(Namespace, ClientId, TCConfig, ClientSubPubFn),
    ok.

-doc """
Verifies that we bump **only** global metrics for a channel that belongs to
an **implicit** namespace.
""".
t_namespaced_metrics_channel_implicit() ->
    [{matrix, true}].
t_namespaced_metrics_channel_implicit(matrix) ->
    [[?tcp], [?quic], [?ws], [?socket]];
t_namespaced_metrics_channel_implicit({init, TCConfig}) ->
    setup_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_implicit({'end', TCConfig}) ->
    teardown_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_implicit(TCConfig) when is_list(TCConfig) ->
    Namespace = ?NEW_USERNAME(),
    ClientId = ?NEW_CLIENTID(),
    ClientSubPubFn = fun(Client, Topic) ->
        {ok, _, _} = emqtt:subscribe(Client, Topic, [{qos, 1}]),
        emqtt:publish(Client, Topic, <<"hey1">>, [{qos, 1}])
    end,
    assert_namespaced_metrics_channel_implicit(Namespace, ClientId, TCConfig, ClientSubPubFn),
    ok.

-doc """
Same as `t_namespaced_metrics_channel_explicit`, but using quic multi streams.  A separate
case due to the different APIs used.
""".
t_namespaced_metrics_channel_explicit_quic() ->
    [{matrix, true}].
t_namespaced_metrics_channel_explicit_quic(matrix) ->
    [[?quic]];
t_namespaced_metrics_channel_explicit_quic({init, TCConfig}) ->
    setup_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_explicit_quic({'end', TCConfig}) ->
    teardown_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_explicit_quic(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 6}),
    Namespace = ?config(explicit_ns, TCConfig),
    ClientId = ?NEW_CLIENTID(),
    ClientSubPubFn = fun(Client, Topic) ->
        {ok, _, _} = emqtt:subscribe_via(Client, {new_data_stream, []}, #{}, [
            {Topic, [{qos, 1}]}
        ]),
        {ok, PubVia} = emqtt:start_data_stream(Client, []),
        emqtt:publish_via(Client, PubVia, Topic, #{}, <<"hey1">>, [{qos, 1}])
    end,
    assert_namespaced_metrics_channel_explicit(Namespace, ClientId, TCConfig, ClientSubPubFn),
    ok.

-doc """
Same as `t_namespaced_metrics_channel_implicit`, but using quic multi streams.  A separate
case due to the different APIs used.
""".
t_namespaced_metrics_channel_implicit_quic() ->
    [{matrix, true}].
t_namespaced_metrics_channel_implicit_quic(matrix) ->
    [[?quic]];
t_namespaced_metrics_channel_implicit_quic({init, TCConfig}) ->
    setup_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_implicit_quic({'end', TCConfig}) ->
    teardown_namespaced_metrics_channel_scenario(TCConfig);
t_namespaced_metrics_channel_implicit_quic(TCConfig) when is_list(TCConfig) ->
    ct:timetrap({seconds, 6}),
    Namespace = ?NEW_USERNAME(),
    ClientId = ?NEW_CLIENTID(),
    ClientSubPubFn = fun(Client, Topic) ->
        {ok, _, _} = emqtt:subscribe_via(Client, {new_data_stream, []}, #{}, [
            {Topic, [{qos, 1}]}
        ]),
        {ok, PubVia} = emqtt:start_data_stream(Client, []),
        emqtt:publish_via(Client, PubVia, Topic, #{}, <<"hey1">>, [{qos, 1}])
    end,
    assert_namespaced_metrics_channel_implicit(Namespace, ClientId, TCConfig, ClientSubPubFn),
    ok.

t_namespaced_metrics_prometheus({init, TCConfig}) ->
    Nodes = mk_cluster(?FUNCTION_NAME, #{n => 2}, TCConfig),
    ?ON_ALL(Nodes, begin
        meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
        meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end)
    end),
    [{nodes, Nodes} | TCConfig];
t_namespaced_metrics_prometheus({'end', _TCConfig}) ->
    ok;
t_namespaced_metrics_prometheus(TCConfig) when is_list(TCConfig) ->
    [N | _] = ?config(nodes, TCConfig),
    Namespace = <<"explicit_ns">>,
    ok = ?ON(N, emqx_mt_config:create_managed_ns(Namespace)),
    Namespace2 = <<"other_explicit_ns">>,
    ok = ?ON(N, emqx_mt_config:create_managed_ns(Namespace2)),

    %% Generate some traffic for namespaced metrics
    ?ON(
        N,
        {ok, _} = emqx_authn_chains:add_user(
            'mqtt:global',
            <<"password_based:built_in_database">>,
            #{
                user_id => Namespace,
                namespace => Namespace,
                password => Namespace
            }
        )
    ),

    Port = emqx_mt_api_SUITE:get_mqtt_tcp_port(N),
    ClientId = ?NEW_CLIENTID(),
    Opts0 = #{
        clientid => ClientId,
        username => Namespace,
        password => Namespace,
        port => Port
    },
    Opts1 = connect_opts_of(TCConfig),
    Opts = maps:merge(Opts1, Opts0),
    C1 = connect(Opts),
    Topic = <<"t">>,
    {ok, _, _} = emqtt:subscribe(C1, Topic, [{qos, 1}]),
    emqtt:publish(C1, Topic, <<"hey!">>, [{qos, 1}]),
    ?assertReceive({publish, _}),

    AuthzRule = #{
        <<"permission">> => <<"allow">>,
        <<"action">> => <<"publish">>,
        <<"topic">> => <<"t">>,
        <<"listener_re">> => <<"^tcp:">>
    },
    ?ON(N, begin
        emqx_authz_mnesia:store_rules(Namespace, all, [AuthzRule]),
        emqx_authz_mnesia:store_rules(Namespace, {username, <<"user1">>}, [AuthzRule]),
        emqx_authz_mnesia:store_rules(Namespace, {clientid, <<"client1">>}, [AuthzRule])
    end),

    ?ON(N, begin
        lists:foreach(
            fun(I) ->
                IBin = integer_to_binary(I),
                {ok, _} = emqx_authn_chains:add_user(
                    'mqtt:global',
                    <<"password_based:built_in_database">>,
                    #{
                        user_id => <<"u", IBin/binary>>,
                        password => <<"p">>,
                        namespace => Namespace
                    }
                )
            end,
            lists:seq(1, 3)
        )
    end),

    %% Check prometheus
    NsLabel0A = #{<<"node">> => atom_to_binary(N), <<"namespace">> => Namespace},
    NsLabel0B = #{<<"node">> => atom_to_binary(N), <<"namespace">> => Namespace2},
    {200, Metrics0} = get_prometheus_ns_stats(
        all, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, prometheus
    ),
    SampleMetrics = [
        <<"emqx_messages_received">>,
        <<"emqx_messages_sent">>,
        <<"emqx_packets_received">>,
        <<"emqx_packets_sent">>,
        <<"emqx_sessions_count">>,
        <<"emqx_authz_builtin_record_count">>,
        <<"emqx_authn_builtin_record_count">>
    ],
    ?assertMatch(
        #{
            <<"emqx_messages_received">> := #{NsLabel0A := 1, NsLabel0B := 0},
            <<"emqx_messages_sent">> := #{NsLabel0A := 1, NsLabel0B := 0},
            <<"emqx_packets_received">> := #{NsLabel0A := N1, NsLabel0B := 0},
            <<"emqx_packets_sent">> := #{NsLabel0A := N2, NsLabel0B := 0},
            <<"emqx_sessions_count">> := #{NsLabel0A := 1, NsLabel0B := 0},
            %% Namespaces without any rules at all don't show up
            <<"emqx_authz_builtin_record_count">> := #{NsLabel0A := 3},
            <<"emqx_authn_builtin_record_count">> := #{NsLabel0A := 4}
        } when N1 > 0 andalso N2 > 0,
        Metrics0,
        #{sample => maps:with(SampleMetrics, Metrics0)}
    ),
    %% Filtering a single namespace
    {200, Metrics1} =
        get_prometheus_ns_stats(Namespace, ?PROM_DATA_MODE__ALL_NODES_UNAGGREGATED, prometheus),
    ?assertMatch(
        #{
            <<"emqx_messages_received">> := #{NsLabel0A := 1} = M1,
            <<"emqx_messages_sent">> := #{NsLabel0A := 1},
            <<"emqx_packets_received">> := #{NsLabel0A := N1},
            <<"emqx_packets_sent">> := #{NsLabel0A := N2},
            <<"emqx_sessions_count">> := #{NsLabel0A := 1} = M2,
            <<"emqx_authz_builtin_record_count">> := #{NsLabel0A := 3} = M3,
            <<"emqx_authn_builtin_record_count">> := #{NsLabel0A := 4} = M4
        } when
            N1 > 0 andalso N2 > 0 andalso
                not is_map_key(NsLabel0B, M1) andalso
                not is_map_key(NsLabel0B, M2) andalso
                not is_map_key(NsLabel0B, M3) andalso
                not is_map_key(NsLabel0B, M4),
        Metrics1,
        #{sample => maps:with(SampleMetrics, Metrics1)}
    ),

    NsLabel1A = #{<<"namespace">> => Namespace},
    NsLabel1B = #{<<"namespace">> => Namespace2},
    {200, Metrics2} = get_prometheus_ns_stats(
        all, ?PROM_DATA_MODE__ALL_NODES_AGGREGATED, prometheus
    ),
    ?assertMatch(
        #{
            <<"emqx_messages_received">> := #{NsLabel1A := 1, NsLabel1B := 0},
            <<"emqx_messages_sent">> := #{NsLabel1A := 1, NsLabel1B := 0},
            <<"emqx_packets_received">> := #{NsLabel1A := N1, NsLabel1B := 0},
            <<"emqx_packets_sent">> := #{NsLabel1A := N2, NsLabel1B := 0},
            <<"emqx_sessions_count">> := #{NsLabel1A := 1, NsLabel1B := 0},
            %% Namespaces without any rules at all don't show up
            <<"emqx_authz_builtin_record_count">> := #{NsLabel1A := 3},
            <<"emqx_authn_builtin_record_count">> := #{NsLabel1A := 4}
        } when N1 > 0 andalso N2 > 0,
        Metrics2,
        #{sample => maps:with(SampleMetrics, Metrics2)}
    ),

    %% Unknown namespace
    lists:foreach(
        fun(Mode) ->
            ?assertMatch(
                {200, _},
                get_prometheus_ns_stats(<<"unknown_ns">>, Mode, prometheus)
            )
        end,
        ?PROM_DATA_MODES
    ),

    %% Quick checking that all combinations do not crash (i.e., return 200 and not 500)
    lists:foreach(
        fun({Ns, Mode}) ->
            ?assertMatch(
                {200, _},
                get_prometheus_ns_stats(Ns, Mode, prometheus),
                #{ns => Ns, mode => Mode}
            )
        end,
        [
            {Ns, Mode}
         || Ns <- [all, Namespace, Namespace2],
            Mode <- ?PROM_DATA_MODES
        ]
    ),

    %% We don't support json
    lists:foreach(
        fun({Ns, Mode}) ->
            ?assertMatch(
                {400, _},
                get_prometheus_ns_stats(Ns, Mode, json),
                #{ns => Ns, mode => Mode}
            )
        end,
        [
            {Ns, Mode}
         || Ns <- [all, Namespace, Namespace2],
            Mode <- ?PROM_DATA_MODES
        ]
    ),

    ok.

-doc """
Verifies that we rate limit requests to the namespaced metrics scraping endpoint.

We limit requests that attempt to retrieve data from all namespaces.  Requests for a
single namespace are not rate limited.
""".
t_namespaced_metrics_prometheus_rate_limit({init, TCConfig}) ->
    Nodes = mk_cluster(?FUNCTION_NAME, #{n => 1}, TCConfig),
    ?ON_ALL(Nodes, begin
        meck:new(emqx_license_checker, [non_strict, passthrough, no_link]),
        meck:expect(emqx_license_checker, expiry_epoch, fun() -> 1859673600 end)
    end),
    [{nodes, Nodes} | TCConfig];
t_namespaced_metrics_prometheus_rate_limit({'end', _TCConfig}) ->
    ok;
t_namespaced_metrics_prometheus_rate_limit(TCConfig) when is_list(TCConfig) ->
    [N | _] = ?config(nodes, TCConfig),
    Namespace = <<"rate_limited">>,
    ok = ?ON(N, emqx_mt_config:create_managed_ns(Namespace)),

    %% Exercising propagated post config update hook
    RawPromConf0 = ?ON(N, emqx_config:get_raw([prometheus])),
    RawPromConf = emqx_utils_maps:deep_put(
        [<<"namespaced_metrics_limiter">>, <<"rate">>],
        RawPromConf0,
        <<"1/s">>
    ),
    {{ok, _}, {ok, _}} =
        ?wait_async_action(
            ?ON(
                N,
                emqx_conf:update(
                    [prometheus],
                    RawPromConf,
                    #{override_to => cluster}
                )
            ),
            #{?snk_kind := "prometheus_api_limiter_updated"},
            5_000
        ),

    Mode = ?PROM_DATA_MODE__NODE,

    %% Requesting specific namespace is not rate limited.
    lists:foreach(
        fun(_) ->
            ?assertMatch({200, _}, get_prometheus_ns_stats(Namespace, Mode, prometheus))
        end,
        lists:seq(1, 10)
    ),

    %% Requesting all namespaces is rate limited.
    ?retry(100, 5, ?assertMatch({429, _}, get_prometheus_ns_stats(all, Mode, prometheus))),

    ok.
