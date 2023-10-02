%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_ds_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("stdlib/include/assert.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").

-define(DEFAULT_KEYSPACE, default).
-define(DS_SHARD_ID, <<"local">>).
-define(DS_SHARD, {?DEFAULT_KEYSPACE, ?DS_SHARD_ID}).
-define(ITERATOR_REF_TAB, emqx_ds_iterator_ref).

-import(emqx_common_test_helpers, [on_exit/1]).

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

init_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency
->
    Cluster = cluster(#{n => 1}),
    ClusterOpts = #{work_dir => emqx_cth_suite:work_dir(TestCase, Config)},
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

end_per_testcase(TestCase, Config) when
    TestCase =:= t_session_subscription_idempotency;
    TestCase =:= t_session_unsubscription_idempotency
->
    Nodes = ?config(nodes, Config),
    emqx_common_test_helpers:call_janitor(60_000),
    ok = emqx_cth_cluster:stop(Nodes),
    ok;
end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(60_000),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

cluster(#{n := N}) ->
    Spec = #{role => core, apps => app_specs()},
    lists:map(
        fun(M) ->
            Name = list_to_atom("ds_SUITE" ++ integer_to_list(M)),
            {Name, Spec}
        end,
        lists:seq(1, N)
    ).

app_specs() ->
    [
        emqx_durable_storage,
        {emqx, "persistent_session_store = {ds = true}"}
    ].

get_mqtt_port(Node, Type) ->
    {_IP, Port} = erpc:call(Node, emqx_config, get, [[listeners, Type, default, bind]]),
    Port.

get_all_iterator_refs(Node) ->
    erpc:call(Node, mnesia, dirty_all_keys, [?ITERATOR_REF_TAB]).

get_all_iterator_ids(Node) ->
    Fn = fun(K, _V, Acc) -> [K | Acc] end,
    erpc:call(Node, fun() ->
        emqx_ds_storage_layer:foldl_iterator_prefix(?DS_SHARD, <<>>, Fn, [])
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

start_client(Opts0 = #{}) ->
    Defaults = #{
        proto_ver => v5,
        properties => #{'Session-Expiry-Interval' => 300}
    },
    Opts = maps:to_list(emqx_utils_maps:deep_merge(Defaults, Opts0)),
    {ok, Client} = emqtt:start_link(Opts),
    on_exit(fun() -> catch emqtt:stop(Client) end),
    Client.

%%------------------------------------------------------------------------------
%% Testcases
%%------------------------------------------------------------------------------

t_non_persistent_session_subscription(_Config) ->
    ClientId = atom_to_binary(?FUNCTION_NAME),
    SubTopicFilter = <<"t/#">>,
    ?check_trace(
        begin
            ?tp(notice, "starting", #{}),
            Client = start_client(#{
                clientid => ClientId,
                properties => #{'Session-Expiry-Interval' => 0}
            }),
            {ok, _} = emqtt:connect(Client),
            ?tp(notice, "subscribing", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client, SubTopicFilter, qos2),
            IteratorRefs = get_all_iterator_refs(node()),
            IteratorIds = get_all_iterator_ids(node()),

            ok = emqtt:stop(Client),

            #{
                iterator_refs => IteratorRefs,
                iterator_ids => IteratorIds
            }
        end,
        fun(Res, Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            #{
                iterator_refs := IteratorRefs,
                iterator_ids := IteratorIds
            } = Res,
            ?assertEqual([], IteratorRefs),
            ?assertEqual({ok, []}, IteratorIds),
            ok
        end
    ),
    ok.

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
                ?tp(notice, "restarting node", #{node => Node1}),
                true = monitor_node(Node1, true),
                ok = erpc:call(Node1, init, restart, []),
                receive
                    {nodedown, Node1} ->
                        ok
                after 10_000 ->
                    ct:fail("node ~p didn't stop", [Node1])
                end,
                ?tp(notice, "waiting for nodeup", #{node => Node1}),
                wait_nodeup(Node1),
                wait_gen_rpc_down(Node1Spec),
                ?tp(notice, "restarting apps", #{node => Node1}),
                Apps = maps:get(apps, Node1Spec),
                ok = erpc:call(Node1, emqx_cth_suite, load_apps, [Apps]),
                _ = erpc:call(Node1, emqx_cth_suite, start_apps, [Apps, Node1Spec]),
                %% have to re-inject this so that we may stop the node succesfully at the
                %% end....
                ok = emqx_cth_cluster:set_node_opts(Node1, Node1Spec),
                ok = snabbkaffe:forward_trace(Node1),
                ?tp(notice, "node restarted", #{node => Node1}),
                ?tp(restarted_node, #{}),
                ok
            end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:subscribe(Client0, SubTopicFilter, qos2),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 0 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),

            ok = emqtt:stop(Client1),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            %% Exactly one iterator should have been opened.
            SubTopicFilterWords = emqx_topic:words(SubTopicFilter),
            ?assertEqual([{ClientId, SubTopicFilterWords}], get_all_iterator_refs(Node1)),
            ?assertMatch({ok, [_]}, get_all_iterator_ids(Node1)),
            ?assertMatch(
                {ok, #{}, #{SubTopicFilterWords := #{}}},
                erpc:call(Node1, emqx_persistent_session_ds, session_open, [ClientId])
            )
        end
    ),
    ok.

%% Check that we close the iterators before deleting the iterator id entry.
t_session_unsubscription_idempotency(Config) ->
    [Node1Spec | _] = ?config(node_specs, Config),
    [Node1] = ?config(nodes, Config),
    Port = get_mqtt_port(Node1, tcp),
    SubTopicFilter = <<"t/+">>,
    ClientId = <<"myclientid">>,
    ?check_trace(
        begin
            ?force_ordering(
                #{?snk_kind := persistent_session_ds_close_iterators, ?snk_span := {complete, _}},
                _NEvents0 = 1,
                #{?snk_kind := will_restart_node},
                _Guard0 = true
            ),
            ?force_ordering(
                #{?snk_kind := restarted_node},
                _NEvents1 = 1,
                #{?snk_kind := persistent_session_ds_iterator_delete, ?snk_span := start},
                _Guard1 = true
            ),

            spawn_link(fun() ->
                ?tp(will_restart_node, #{}),
                ?tp(notice, "restarting node", #{node => Node1}),
                true = monitor_node(Node1, true),
                ok = erpc:call(Node1, init, restart, []),
                receive
                    {nodedown, Node1} ->
                        ok
                after 10_000 ->
                    ct:fail("node ~p didn't stop", [Node1])
                end,
                ?tp(notice, "waiting for nodeup", #{node => Node1}),
                wait_nodeup(Node1),
                wait_gen_rpc_down(Node1Spec),
                ?tp(notice, "restarting apps", #{node => Node1}),
                Apps = maps:get(apps, Node1Spec),
                ok = erpc:call(Node1, emqx_cth_suite, load_apps, [Apps]),
                _ = erpc:call(Node1, emqx_cth_suite, start_apps, [Apps, Node1Spec]),
                %% have to re-inject this so that we may stop the node succesfully at the
                %% end....
                ok = emqx_cth_cluster:set_node_opts(Node1, Node1Spec),
                ok = snabbkaffe:forward_trace(Node1),
                ?tp(notice, "node restarted", #{node => Node1}),
                ?tp(restarted_node, #{}),
                ok
            end),

            ?tp(notice, "starting 1", #{}),
            Client0 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client0),
            ?tp(notice, "subscribing 1", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client0, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 1", #{}),
            process_flag(trap_exit, true),
            catch emqtt:unsubscribe(Client0, SubTopicFilter),
            receive
                {'EXIT', {shutdown, _}} ->
                    ok
            after 0 -> ok
            end,
            process_flag(trap_exit, false),

            {ok, _} = ?block_until(#{?snk_kind := restarted_node}, 15_000),
            ?tp(notice, "starting 2", #{}),
            Client1 = start_client(#{port => Port, clientid => ClientId}),
            {ok, _} = emqtt:connect(Client1),
            ?tp(notice, "subscribing 2", #{}),
            {ok, _, [?RC_GRANTED_QOS_2]} = emqtt:subscribe(Client1, SubTopicFilter, qos2),
            ?tp(notice, "unsubscribing 2", #{}),
            {{ok, _, [?RC_SUCCESS]}, {ok, _}} =
                ?wait_async_action(
                    emqtt:unsubscribe(Client1, SubTopicFilter),
                    #{
                        ?snk_kind := persistent_session_ds_iterator_delete,
                        ?snk_span := {complete, _}
                    },
                    15_000
                ),

            ok = emqtt:stop(Client1),

            ok
        end,
        fun(Trace) ->
            ct:pal("trace:\n  ~p", [Trace]),
            %% No iterators remaining
            ?assertEqual([], get_all_iterator_refs(Node1)),
            ?assertEqual({ok, []}, get_all_iterator_ids(Node1)),
            ok
        end
    ),
    ok.
