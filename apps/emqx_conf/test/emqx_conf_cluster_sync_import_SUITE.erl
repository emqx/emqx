%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_cluster_sync_import_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).
-define(RAW_DIFF_RULE_ID, <<"cluster_sync_raw_diff_rule">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

suite() ->
    [{timetrap, {seconds, 180}}].

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

t_status_after_import_then_join_ignores_raw_only_diffs(Config) ->
    WorkDir = emqx_cth_suite:work_dir(?FUNCTION_NAME, Config),
    AppSpecs = import_join_app_specs(),
    ClusterSpec = [
        {cluster_sync_import1, #{role => core, apps => AppSpecs}},
        {cluster_sync_import2, #{role => core, apps => AppSpecs}}
    ],
    [N1Spec, N2Spec] = emqx_cth_cluster:mk_nodespecs(
        ClusterSpec,
        #{
            work_dir => WorkDir,
            start_apps_timeout => 60_000
        }
    ),
    [N1, N2] = Nodes = [maps:get(name, N1Spec), maps:get(name, N2Spec)],
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),

    [N1] = emqx_cth_cluster:start([N1Spec]),
    BackupFile = make_rule_engine_backup(Config),
    ImportRes = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(ImportRes, ?ON(N1, emqx_mgmt_data_backup:import_local(BackupFile))),
    wait_imported_rule(N1),

    [N2] = emqx_cth_cluster:start([N2Spec]),
    emulate_emqx_conf_init_load(N2, N1),
    wait_clustered(Nodes),
    wait_same_tnx_id(Nodes),
    wait_imported_rule(N2),

    N1Rule = get_imported_rule(N1, emqx_conf, get, [rule_engine, rules]),
    N2Rule = get_imported_rule(N2, emqx_conf, get, [rule_engine, rules]),
    ?assertEqual(N1Rule, N2Rule),

    emulate_sparse_import_raw_conf(N1),
    N1RawRule = get_imported_rule(N1, emqx_conf, get_raw, [rule_engine, rules]),
    N2RawRule = get_imported_rule(N2, emqx_conf, get_raw, [rule_engine, rules]),
    ?assertNotEqual(N1RawRule, N2RawRule),

    Output = capture_cluster_sync_status(N1),
    ?assertEqual(
        nomatch,
        binary:match(Output, <<"has been updated">>),
        #{output => Output}
    ),
    ?assertEqual(
        nomatch,
        binary:match(Output, <<"inconsistent keys">>),
        #{output => Output}
    ),
    ?assertNotEqual(
        nomatch,
        binary:match(Output, <<"All configuration synchronized">>),
        #{output => Output}
    ),
    ok.

import_join_app_specs() ->
    [
        {emqx, #{
            override_env => [{boot_modules, []}],
            config => #{
                listeners => #{
                    tcp => #{default => <<"marked_for_deletion">>},
                    ssl => #{default => <<"marked_for_deletion">>},
                    ws => #{default => <<"marked_for_deletion">>},
                    wss => #{default => <<"marked_for_deletion">>}
                }
            }
        }},
        {emqx_conf, #{config => #{}}},
        emqx_management,
        emqx_rule_engine
    ].

make_rule_engine_backup(Config) ->
    PrivDir = ?config(priv_dir, Config),
    BackupName = filename:join(PrivDir, "cluster-sync-raw-diff-import.tar.gz"),
    BackupRoot = filename:basename(BackupName, ".tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(
            #{
                edition => emqx_release:edition(),
                version => emqx_release:version()
            },
            #{}
        )
    ),
    ClusterHocon = unicode:characters_to_binary(hocon_pp:do(rule_engine_import_conf(), #{})),
    ok = erl_tar:create(
        BackupName,
        [
            {filename:join(BackupRoot, "META.hocon"), Meta},
            {filename:join(BackupRoot, "cluster.hocon"), ClusterHocon}
        ],
        [compressed]
    ),
    BackupName.

rule_engine_import_conf() ->
    #{
        <<"rule_engine">> => #{
            <<"rules">> => #{
                ?RAW_DIFF_RULE_ID => sparse_rule_raw()
            }
        }
    }.

sparse_rule_raw() ->
    #{
        <<"description">> => <<"">>,
        <<"sql">> => <<"SELECT * FROM \"t/#\"">>,
        <<"actions">> => [
            #{
                <<"function">> => <<"republish">>,
                <<"args">> => #{
                    <<"topic">> => <<"cluster/sync/${topic}">>
                }
            }
        ]
    }.

get_imported_rule(Node, Module, Function, Path) ->
    Rules = ?ON(Node, erlang:apply(Module, Function, [Path])),
    get_rule_from_map(Rules).

get_rule_from_map(Rules) ->
    case maps:find(?RAW_DIFF_RULE_ID, Rules) of
        {ok, Rule} ->
            Rule;
        error ->
            RuleIdAtom = binary_to_existing_atom(?RAW_DIFF_RULE_ID, utf8),
            maps:get(RuleIdAtom, Rules)
    end.

wait_imported_rule(Node) ->
    wait_until(
        fun() ->
            has_imported_rule(Node)
        end,
        #{action => wait_imported_rule, node => Node}
    ).

has_imported_rule(Node) ->
    Rules = ?ON(Node, emqx_conf:get([rule_engine, rules])),
    maps:is_key(?RAW_DIFF_RULE_ID, Rules) orelse
        maps:is_key(binary_to_existing_atom(?RAW_DIFF_RULE_ID, utf8), Rules).

%% Emulate `emqx_conf_app:init_load/1`; the CT cluster helper does not
%% automatically replay this boot-time sync after a node joins.
emulate_emqx_conf_init_load(NewNode, SeedNode) ->
    ?ON(SeedNode, ok = emqx_config_backup_manager:flush()),
    ?ON(NewNode, begin
        {ok, _TnxId} = emqx_conf_app:sync_cluster_conf(),
        DataDir = emqx:data_dir(),
        Path = filename:join([DataDir, "configs", "cluster.hocon"]),
        {ok, Contents0} = file:read_file(Path),
        ok = file:write_file(Path, [
            Contents0,
            <<"node.cookie = cookie\n">>,
            <<"node.data_dir = \"">>,
            DataDir,
            <<"\"\n">>
        ]),
        SchemaMod = emqx_conf:schema_module(),
        ok = application:stop(emqx_rule_engine),
        ok = emqx_config:init_load(SchemaMod),
        ok = application:start(emqx_rule_engine)
    end).

%% `release-60` may materialize defaults during the import path already.  Keep
%% the original regression shape by making the importer node retain the sparse
%% user-provided raw rule while leaving its checked config untouched.
emulate_sparse_import_raw_conf(Node) ->
    ?ON(Node, emqx_config:put_raw([rule_engine, rules, ?RAW_DIFF_RULE_ID], sparse_rule_raw())).

wait_clustered(Nodes) ->
    wait_until(
        fun() ->
            lists:all(
                fun(Node) ->
                    lists:sort(Nodes) =:= lists:sort(?ON(Node, mria:running_nodes()))
                end,
                Nodes
            )
        end,
        #{action => wait_clustered, nodes => Nodes}
    ).

wait_same_tnx_id(Nodes) ->
    [Node | _] = Nodes,
    wait_until(
        fun() ->
            case ?ON(Node, emqx_cluster_rpc:status()) of
                {atomic, Status} ->
                    TnxIds = lists:usort([TnxId || #{tnx_id := TnxId} <- Status]),
                    length(TnxIds) =:= 1;
                _ ->
                    false
            end
        end,
        #{action => wait_same_tnx_id, nodes => Nodes}
    ).

wait_until(Fun, Context) ->
    wait_until(Fun, Context, 30).

wait_until(_Fun, Context, 0) ->
    error({timeout, Context});
wait_until(Fun, Context, Retries) ->
    case Fun() of
        true ->
            ok;
        false ->
            timer:sleep(500),
            wait_until(Fun, Context, Retries - 1)
    end.

capture_cluster_sync_status(Node) ->
    ?ON(Node, begin
        ok = meck:new(emqx_ctl, [passthrough, no_link]),
        try
            ok = emqx_conf_cli:admins(["status"]),
            History = meck:history(emqx_ctl),
            unicode:characters_to_binary([
                format_ctl_output(Fun, Args)
             || {_Pid, {emqx_ctl, Fun, Args}, _Result} <- History,
                Fun =:= print orelse Fun =:= warning
            ])
        after
            meck:unload(emqx_ctl)
        end
    end).

format_ctl_output(_Fun, [Format]) ->
    Format;
format_ctl_output(_Fun, [Format, Args]) ->
    io_lib:format(Format, Args);
format_ctl_output(_Fun, Args) ->
    io_lib:format("~p", [Args]).
