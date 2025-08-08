%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_cluster_sync_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include_lib("typerefl/include/types.hrl").
-include_lib("emqx/include/emqx_config.hrl").
-include("emqx_conf.hrl").

%%------------------------------------------------------------------------------
%% Defs
%%------------------------------------------------------------------------------

-import(emqx_common_test_helpers, [on_exit/1]).

-define(NS, <<"some_namespace">>).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

-define(global_namespace, global_namespace).
-define(namespaced, namespaced).

%%------------------------------------------------------------------------------
%% CT boilerplate
%%------------------------------------------------------------------------------

all() ->
    emqx_common_test_helpers:all_with_matrix(?MODULE).

groups() ->
    emqx_common_test_helpers:groups_with_matrix(?MODULE).

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
        'config.allowed_namespaced_roots' => [<<"sysmon">>, <<"mqtt">>]
    }.

fake_mfa(TxId, Node, MFA) ->
    Func = fun() ->
        MFARec = #cluster_rpc_mfa{
            tnx_id = TxId,
            mfa = MFA,
            initiator = Node,
            created_at = erlang:localtime()
        },
        ok = mnesia:write(?CLUSTER_MFA, MFARec, write),
        ok = emqx_cluster_rpc:commit(Node, TxId)
    end,
    {atomic, ok} = mria:transaction(?CLUSTER_RPC_SHARD, Func, []),
    ok.

mk_cluster_spec(Opts) ->
    BaseAppSpec = #{
        before_start =>
            fun(App, AppOpts) ->
                ok = emqx_schema_hooks:inject_from_modules([?MODULE]),
                emqx_cth_suite:inhibit_config_loader(App, AppOpts)
            end
    },
    Conf = #{
        listeners => #{
            tcp => #{default => <<"marked_for_deletion">>},
            ssl => #{default => <<"marked_for_deletion">>},
            ws => #{default => <<"marked_for_deletion">>},
            wss => #{default => <<"marked_for_deletion">>}
        }
    },
    Apps = [
        {emqx, BaseAppSpec#{config => Conf}},
        {emqx_conf, BaseAppSpec#{config => Conf}}
    ],
    [
        {emqx_conf_cluster_sync_SUITE1, Opts#{role => core, apps => Apps}},
        {emqx_conf_cluster_sync_SUITE2, Opts#{role => core, apps => Apps}}
    ].

cli_admin(?global_ns, Cmd, Args) ->
    emqx_conf_cli:admins([Cmd | Args]);
cli_admin(Namespace, Cmd, Args) when is_binary(Namespace) ->
    emqx_conf_cli:admins([Cmd, "--namespace", str(Namespace) | Args]).

str(X) -> emqx_utils_conv:str(X).

namespace_of(TCConfig) ->
    case
        emqx_common_test_helpers:get_matrix_prop(
            TCConfig, [?global_namespace, ?namespaced], ?global_namespace
        )
    of
        ?global_namespace -> ?global_ns;
        ?namespaced -> ?NS
    end.

status() ->
    %% Due to seeding the test node, there're might be some initial MFAs.
    {atomic, Status} = emqx_cluster_rpc:status(),
    lists:filter(
        fun
            (#{mfa := {emqx_config, seed_defaults_for_all_roots_namespaced, _}}) ->
                false;
            (_) ->
                true
        end,
        Status
    ).

%% bump expected transaction id if running namespaced test, since these have a seed
%% transaction during setup.
bump_if_namespaced(?global_ns, TxId) ->
    TxId;
bump_if_namespaced(Namespace, TxId) when is_binary(Namespace) ->
    TxId + 1.

%%------------------------------------------------------------------------------
%% Test cases
%%------------------------------------------------------------------------------

t_fix() ->
    [{matrix, true}].
t_fix(matrix) ->
    [[?global_namespace], [?namespaced]];
t_fix(Config) when is_list(Config) ->
    Namespace = namespace_of(Config),
    UpdateOpts =
        case Namespace of
            ?global_ns ->
                #{};
            _ ->
                #{namespace => Namespace}
        end,
    Cluster = mk_cluster_spec(#{}),
    Nodes =
        [Node1, Node2] = emqx_cth_cluster:start(
            Cluster, #{work_dir => emqx_cth_suite:work_dir(?FUNCTION_NAME, Config)}
        ),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    maybe
        true ?= is_binary(Namespace),
        ?ON(
            Node1,
            ok = emqx_common_test_helpers:seed_defaults_for_all_roots_namespaced_cluster(
                emqx_schema, Namespace
            )
        )
    end,

    ?ON(Node1, ?assertMatch([], status())),
    ?ON(Node2, ?assertMatch([], status())),
    ?ON(
        Node1, emqx_conf_proto_v5:update([<<"mqtt">>], #{<<"max_topic_levels">> => 100}, UpdateOpts)
    ),
    ?assertEqual(100, emqx_conf_proto_v5:get_config(Node1, Namespace, [mqtt, max_topic_levels])),
    ?assertEqual(100, emqx_conf_proto_v5:get_config(Node2, Namespace, [mqtt, max_topic_levels])),
    TxId1 = bump_if_namespaced(Namespace, 1),
    ?ON(
        Node1,
        ?assertMatch(
            [
                #{node := Node1, tnx_id := TxId1},
                #{node := Node2, tnx_id := TxId1}
            ],
            status()
        )
    ),
    %% fix normal, nothing changed
    ?ON(Node1, begin
        ok = cli_admin(Namespace, "fix", []),
        ?assertMatch(
            [
                #{node := Node1, tnx_id := TxId1},
                #{node := Node2, tnx_id := TxId1}
            ],
            status()
        )
    end),
    %% fix inconsistent_key. tnx_id is the same, so nothing changed.
    emqx_conf_proto_v5:update(Node1, [<<"mqtt">>], #{<<"max_topic_levels">> => 99}, UpdateOpts),
    ?ON(Node1, begin
        ok = cli_admin(Namespace, "fix", []),
        ?assertMatch(
            [
                #{node := Node1, tnx_id := TxId1},
                #{node := Node2, tnx_id := TxId1}
            ],
            status()
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v5:get_config(Node1, Namespace, [mqtt, max_topic_levels])),
    ?assertMatch(100, emqx_conf_proto_v5:get_config(Node2, Namespace, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id_key. tnx_id and key are updated.
    ?ON(Node1, fake_mfa(TxId1 + 1, Node1, {?MODULE, undef, []})),
    %% 2 -> fake_mfa, 3-> mark_begin_log, 4-> mqtt 5 -> zones
    TxId2 = 5,
    ?ON(Node2, begin
        ok = cli_admin(Namespace, "fix", []),
        ?assertMatch(
            [
                #{node := Node1, tnx_id := TxId2},
                #{node := Node2, tnx_id := TxId2}
            ],
            status(),
            #{expected_tx_id => TxId2}
        )
    end),
    ?assertMatch(99, emqx_conf_proto_v5:get_config(Node1, Namespace, [mqtt, max_topic_levels])),
    ?assertMatch(99, emqx_conf_proto_v5:get_config(Node2, Namespace, [mqtt, max_topic_levels])),

    %% fix inconsistent_tnx_id. tnx_id is updated.
    {ok, _} = ?ON(
        Node1, emqx_conf_proto_v5:update([<<"mqtt">>], #{<<"max_topic_levels">> => 98}, UpdateOpts)
    ),
    ?ON(Node2, fake_mfa(TxId2 + 2, Node2, {?MODULE, undef1, []})),
    TxId3 = 8,
    ?ON(Node1, begin
        ok = cli_admin(Namespace, "fix", []),
        ?assertMatch(
            [
                #{node := Node1, tnx_id := TxId3},
                #{node := Node2, tnx_id := TxId3}
            ],
            status(),
            #{expected_tx_id => TxId3}
        )
    end),
    ?assertMatch(98, emqx_conf_proto_v5:get_config(Node1, Namespace, [mqtt, max_topic_levels])),
    ?assertMatch(98, emqx_conf_proto_v5:get_config(Node2, Namespace, [mqtt, max_topic_levels])),
    %% unchanged
    ?ON(Node1, begin
        ok = cli_admin(Namespace, "fix", []),
        ?assertMatch(
            {atomic, [
                #{node := Node1, tnx_id := TxId3},
                #{node := Node2, tnx_id := TxId3}
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
