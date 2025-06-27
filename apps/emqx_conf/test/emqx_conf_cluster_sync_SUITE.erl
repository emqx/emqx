%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_cluster_sync_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_conf.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(_TestCase, Config) ->
    Config.

end_per_testcase(_TestCase, _Config) ->
    emqx_common_test_helpers:call_janitor(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

injected_fields() ->
    #{
        'config.allowed_namespaced_roots' => [<<"sysmon">>]
    }.

fake_mfa(TnxId, Node, MFA) ->
    Func = fun() ->
        MFARec = #cluster_rpc_mfa{
            tnx_id = TnxId,
            mfa = MFA,
            initiator = Node,
            created_at = erlang:localtime()
        },
        ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
        ok = emqx_cluster_rpc:commit(Node, TnxId)
    end,
    {atomic, ok} = mria:transaction(?CLUSTER_RPC_SHARD, Func, []),
    ok.

mk_cluster_spec(Opts) ->
    Conf = #{
        listeners => #{
            tcp => #{default => <<"marked_for_deletion">>},
            ssl => #{default => <<"marked_for_deletion">>},
            ws => #{default => <<"marked_for_deletion">>},
            wss => #{default => <<"marked_for_deletion">>}
        }
    },
    Apps = [
        {emqx, #{config => Conf}},
        {emqx_conf, #{config => Conf}}
    ],
    [
        {emqx_conf_cluster_sync_SUITE1, Opts#{role => core, apps => Apps}},
        {emqx_conf_cluster_sync_SUITE2, Opts#{role => core, apps => Apps}}
    ].

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_fix(Config) ->
    Cluster = mk_cluster_spec(#{}),
    Nodes =
        [Node1, Node2] = emqx_cth_cluster:start(
            Cluster, #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),

    ?ON(Node1, ?assertMatch({atomic, []}, emqx_cluster_rpc:status())),
    ?ON(Node2, ?assertMatch({atomic, []}, emqx_cluster_rpc:status())),
    ?ON(Node1, emqx_conf_proto_v4:update([<<"mqtt">>], #{<<"max_topic_levels">> => 100}, #{})),
    ?assertEqual(100, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertEqual(100, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),
    ?ON(
        Node1,
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    ),
    %% fix normal, nothing changed
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    %% fix inconsistent_key. tnx_id is the same, so nothing changed.
    emqx_conf_proto_v4:update(Node1, [<<"mqtt">>], #{<<"max_topic_levels">> => 99}, #{}),
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 1},
                #{node := Node2, tnx_id := 1}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(100, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id_key. tnx_id and key are updated.
    ?ON(Node1, fake_mfa(2, Node1, {?MODULE, undef, []})),
    %% 2 -> fake_mfa, 3-> mark_begin_log, 4-> mqtt 5 -> zones
    ?ON(Node2, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 5},
                #{node := Node2, tnx_id := 5}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(99, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id. tnx_id is updated.
    {ok, _} = ?ON(
        Node1, emqx_conf_proto_v4:update([<<"mqtt">>], #{<<"max_topic_levels">> => 98}, #{})
    ),
    ?ON(Node2, fake_mfa(7, Node2, {?MODULE, undef1, []})),
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 8},
                #{node := Node2, tnx_id := 8}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ?assertMatch(98, emqx_conf_proto_v4:get_config(Node1, [mqtt, max_topic_levels])),
    ?assertMatch(98, emqx_conf_proto_v4:get_config(Node2, [mqtt, max_topic_levels])),
    %% unchanged
    ?ON(Node1, begin
        ok = emqx_conf_cli:admins(["fix"]),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := 8},
                #{node := Node2, tnx_id := 8}
            ]},
            emqx_cluster_rpc:status()
        )
    end),
    ok.

%% Checks that (checked) namespaced config is loaded when a node (re)starts.  Also, checks
%% that we don't execute mnesia transactions to re-insert the same config when replicating
%% cluster RPC entries.
t_namespaced_config_restart(Config) ->
    Ns = <<"some_namespace">>,
    AppSpecs = [
        {emqx, #{
            before_start =>
                fun(App, AppOpts) ->
                    ok = emqx_schema_hooks:inject_from_modules([?MODULE]),
                    emqx_cth_suite:inhibit_config_loader(App, AppOpts)
                end
        }},
        emqx_conf
    ],
    NodeSpecs =
        [_NSpec1, NSpec2] = emqx_cth_cluster:mk_nodespecs(
            [
                {namespace_cfg_restart1, #{apps => AppSpecs}},
                {namespace_cfg_restart2, #{apps => AppSpecs}}
            ],
            #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    ct:pal("starting cluster"),
    Nodes = [N1, N2] = emqx_cth_cluster:start(NodeSpecs),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    ct:pal("cluster started"),
    ct:pal("seeding namespace"),
    ?ON(
        N1,
        ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(
            emqx_schema, Ns
        )
    ),
    ct:pal("seeded namespace"),
    ?check_trace(
        #{timetrap => 15_000},
        begin
            KeyPath = [sysmon],
            %% Checked configs must exist on both nodes
            ?assertMatch(#{}, ?ON(N1, emqx_config:get_namespaced(KeyPath, Ns))),
            ?assertMatch(#{os := #{}}, ?ON(N2, emqx_config:get_namespaced(KeyPath, Ns))),
            ?ON(N1, begin
                OriginalValRaw = emqx_config:get_raw_namespaced(KeyPath, Ns),
                Val1 = emqx_utils_maps:deep_put(
                    [<<"os">>, <<"cpu_check_interval">>], OriginalValRaw, <<"666s">>
                ),
                Opts = #{namespace => Ns},
                ct:pal("will update config"),
                ?assertMatch(
                    {ok, #{
                        config := #{os := #{cpu_check_interval := 666_000}},
                        raw_config := #{<<"os">> := #{<<"cpu_check_interval">> := <<"666s">>}}
                    }},
                    emqx_conf:update(KeyPath, Val1, Opts)
                ),
                ct:pal("config updated"),
                ok
            end),
            %% Same raw config seen from all nodes.
            ?assertMatch(
                #{<<"os">> := #{<<"cpu_check_interval">> := <<"666s">>}},
                ?ON(N1, emqx_config:get_raw_namespaced(KeyPath, Ns))
            ),
            ?assertMatch(
                #{<<"os">> := #{<<"cpu_check_interval">> := <<"666s">>}},
                ?ON(N2, emqx_config:get_raw_namespaced(KeyPath, Ns))
            ),
            ?assertMatch(
                #{os := #{cpu_check_interval := 666_000}},
                ?ON(N1, emqx_config:get_namespaced(KeyPath, Ns))
            ),
            ?assertMatch(
                #{os := #{cpu_check_interval := 666_000}},
                ?ON(N2, emqx_config:get_namespaced(KeyPath, Ns))
            ),
            %% Now we restart `N2`.  It should reconstruct the persistent term
            %% configuration for the namespace.
            ct:pal("restarting node 2"),
            [N2] = emqx_cth_cluster:restart([NSpec2]),
            ct:pal("restarted node 2"),
            ?assertMatch(
                #{os := #{cpu_check_interval := 666_000}},
                ?ON(N2, emqx_config:get_namespaced(KeyPath, Ns))
            ),
            ok
        end,
        fun(Trace) ->
            %% Should not trigger transactions when replicating RPC entries.
            ?assertMatch([_], ?of_kind("emqx_config_save_configs_namespaced_transaction", Trace)),
            ok
        end
    ),
    ok.
