%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(DS_SHARD, <<"local">>).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    TCApps = emqx_cth_suite:start(
        app_specs(),
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{tc_apps, TCApps} | Config].

end_per_suite(Config) ->
    TCApps = ?config(tc_apps, Config),
    emqx_cth_suite:stop(TCApps),
    ok.

init_per_testcase(t_session_subscription_idempotency = TC, Config) ->
    Cluster = cluster(#{n => 1}),
    ClusterOpts = #{work_dir => emqx_cth_suite:work_dir(TC, Config)},
    NodeSpecs = emqx_cth_cluster:mk_nodespecs(Cluster, ClusterOpts),
    Nodes = emqx_cth_cluster:start(Cluster, ClusterOpts),
    [
        {cluster, Cluster},
        {node_specs, NodeSpecs},
        {cluster_opts, ClusterOpts},
        {nodes, Nodes}
        | Config
    ];
init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(t_session_subscription_idempotency, Config) ->
    Nodes = ?config(nodes, Config),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

cluster(#{n := N}) ->
    Node1 = ds_SUITE1,
    Spec = #{
        role => core,
        join_to => emqx_cth_cluster:node_name(Node1),
        apps => app_specs()
    },
    [
        {Node1, Spec}
        | lists:map(
            fun(M) ->
                Name = binary_to_atom(<<"ds_SUITE", (integer_to_binary(M))/binary>>),
                {Name, Spec}
            end,
            lists:seq(2, N)
        )
    ].

app_specs() ->
    [
        emqx_durable_storage,
        {emqx, #{
            before_start => fun() ->
                emqx_app:set_config_loader(?MODULE)
            end,
            config => #{persistent_session_store => #{ds => true}},
            override_env => [{boot_modules, [broker, listeners]}]
        }}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

get_all_iterator_ids(Node) ->
    Fn = fun(K, _V, Acc) -> [K | Acc] end,
    erpc:call(Node, fun() ->
        emqx_ds_storage_layer:foldl_iterator_prefix(?DS_SHARD, <<>>, Fn, [])
    end).

get_session_iterators(Node, ClientId) ->
    erpc:call(Node, fun() ->
        [ConnPid] = emqx_cm:lookup_channels(ClientId),
        emqx_connection:info({channel, {session, iterators}}, sys:get_state(ConnPid))
    end).

wait_nodeup(Node) ->
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        pong = net_adm:ping(Node)
    ).

wait_gen_rpc_down(_NodeSpec = #{apps := Apps}) ->
    #{override_env := Env} = proplists:get_value(gen_rpc, Apps),
    Port = proplists:get_value(tcp_server_port, Env),
    ?retry(
        _Sleep0 = 500,
        _Attempts0 = 50,
        false = emqx_common_test_helpers:is_tcp_server_available("127.0.0.1", Port)
    ).

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_session_subscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := persistent_session_ds_iterator_added},
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_open_iterators, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() ->
                ?tp(will_restart_node, #{}),
                ct:pal("restarting node ~p", [Node1]),
                true = monitor_node(Node1, true),
                ok = erpc:call(Node1, init, restart, []),
                receive
                    {nodedown, Node1} ->
                        ok
                after 10_000 ->
                    ct:fail("node ~p didn't stop", [Node1])
                end,
                ct:pal("waiting for nodeup ~p", [Node1]),
                wait_nodeup(Node1),
                wait_gen_rpc_down(Node1Spec),
                ct:pal("restarting apps on ~p", [Node1]),
                Apps = maps:get(apps, Node1Spec),
                ok = erpc:call(Node1, emqx_cth_suite, load_apps, [Apps]),
                _ = erpc:call(Node1, emqx_cth_suite, start_apps, [Apps, Node1Spec]),
                %% have to re-inject this so that we may stop the node succesfully at the
                %% end....
                ok = emqx_cth_cluster:set_node_opts(Node1, Node1Spec),
                ct:pal("node ~p restarted", [Node1]),
                ?tp(restarted_node, #{}),
                ok
            end),

            ct:pal("starting 1"),
            {ok, Client0} = emqtt:start_link([
                {port, Port},
                {clientid, ClientId},
                {proto_ver, v5}
            ]),
            {ok, _} = emqtt:connect(Client0),
            ct:pal("subscribing 1"),
            process_flag(trap_exit, true),
            catch emqtt:subscribe(Client0, SubTopicFilter, qos2),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 0 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ct:pal("starting 2"),
            {ok, Client1} = emqtt:start_link([
                {port, Port},
                {clientid, ClientId},
                {proto_ver, v5}
            ]),
            {ok, _} = emqtt:connect(Client1),
            ct:pal("subscribing 2"),
            {ok, _, [2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),
            SessionIterators = get_session_iterators(Node1, ClientId),

            ok = emqtt:stop(Client1),

            #{session_iterators => SessionIterators}
        end,
        fun(Res, Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            #{session_iterators := SessionIterators} = Res,
            %% Exactly one iterator should have been opened.
            ?assertEqual(1, map_size(SessionIterators), #{iterators => SessionIterators}),
            ?assertMatch(#{SubTopicFilter := _}, SessionIterators),
            ?assertMatch({ok, [_]}, get_all_iterator_ids(Node1)),
            ?assertMatch(
                {_IsNew = false, ClientId},
                erpc:call(Node1, emqx_ds, session_open, [ClientId])
            ),
            ok
        end
    ),
    ok.
