%%--------------------------------------------------------------------
%% Copyright (c) 2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_data_backup_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(BOOTSTRAP_BACKUP, "emqx-export-test-bootstrap-ce.tar.gz").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    [application:load(App) || App <- apps_to_start() ++ apps_to_load()],
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(t_import_on_cluster, Config) ->
    %% Don't import listeners to avoid port conflicts
    %% when the same conf will be imported to another cluster
    meck:new(emqx_mgmt_listeners_conf, [passthrough]),
    meck:new(emqx_gateway_conf, [passthrough]),
    meck:expect(
        emqx_mgmt_listeners_conf,
        import_config,
        1,
        {ok, #{changed => [], root_key => listeners}}
    ),
    meck:expect(
        emqx_gateway_conf,
        import_config,
        1,
        {ok, #{changed => [], root_key => gateway}}
    ),
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(t_verify_imported_mnesia_tab_on_cluster, Config) ->
    [{cluster, cluster(Config)} | setup(Config)];
init_per_testcase(t_mnesia_bad_tab_schema, Config) ->
    meck:new(emqx_mgmt_data_backup, [passthrough]),
    meck:expect(emqx_mgmt_data_backup, mnesia_tabs_to_backup, 0, [data_backup_test]),
    setup(Config);
init_per_testcase(_TestCase, Config) ->
    setup(Config).

end_per_testcase(t_import_on_cluster, Config) ->
    cleanup_cluster(?config(cluster, Config)),
    cleanup(Config),
    meck:unload(emqx_mgmt_listeners_conf),
    meck:unload(emqx_gateway_conf);
end_per_testcase(t_verify_imported_mnesia_tab_on_cluster, Config) ->
    cleanup_cluster(?config(cluster, Config)),
    cleanup(Config);
end_per_testcase(t_mnesia_bad_tab_schema, Config) ->
    cleanup(Config),
    meck:unload(emqx_mgmt_data_backup);
end_per_testcase(_TestCase, Config) ->
    cleanup(Config).

t_empty_export_import(_Config) ->
    ExpRawConf = emqx:get_raw_config([]),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)),
    ?assertEqual(ExpRawConf, emqx:get_raw_config([])),
    %% idempotent update assert
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)),
    ?assertEqual(ExpRawConf, emqx:get_raw_config([])).

t_cluster_hocon_export_import(Config) ->
    RawConfBeforeImport = emqx:get_raw_config([]),
    BootstrapFile = filename:join(?config(data_dir, Config), ?BOOTSTRAP_BACKUP),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(BootstrapFile)),
    RawConfAfterImport = emqx:get_raw_config([]),
    ?assertNotEqual(RawConfBeforeImport, RawConfAfterImport),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)),
    ?assertEqual(RawConfAfterImport, emqx:get_raw_config([])),
    %% idempotent update assert
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)),
    ?assertEqual(RawConfAfterImport, emqx:get_raw_config([])),
    %% lookup file inside <data_dir>/backup
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(filename:basename(FileName))).

t_ee_to_ce_backup(Config) ->
    case emqx_release:edition() of
        ce ->
            EEBackupFileName = filename:join(?config(priv_dir, Config), "export-backup-ee.tar.gz"),
            Meta = unicode:characters_to_binary(
                hocon_pp:do(#{edition => ee, version => emqx_release:version()}, #{})
            ),
            ok = erl_tar:create(
                EEBackupFileName,
                [
                    {"export-backup-ee/cluster.hocon", <<>>},
                    {"export-backup-ee/META.hocon", Meta}
                ],
                [compressed]
            ),
            ExpReason = ee_to_ce_backup,
            ?assertEqual(
                {error, ExpReason}, emqx_mgmt_data_backup:import(EEBackupFileName)
            ),
            %% Must be translated to a readable string
            ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ExpReason));
        ee ->
            %% Don't fail if the test is run with emqx-enterprise profile
            ok
    end.

t_no_backup_file(_Config) ->
    ExpReason = not_found,
    ?assertEqual(
        {error, not_found}, emqx_mgmt_data_backup:import("no_such_backup.tar.gz")
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ExpReason)).

t_bad_backup_file(Config) ->
    BadFileName = filename:join(?config(priv_dir, Config), "export-bad-backup-tar-gz"),
    ok = file:write_file(BadFileName, <<>>),
    NoMetaFileName = filename:join(?config(priv_dir, Config), "export-no-meta.tar.gz"),
    ok = erl_tar:create(NoMetaFileName, [{"export-no-meta/cluster.hocon", <<>>}], [compressed]),
    BadArchiveDirFileName = filename:join(?config(priv_dir, Config), "export-bad-dir.tar.gz"),
    ok = erl_tar:create(
        BadArchiveDirFileName,
        [
            {"tmp/cluster.hocon", <<>>},
            {"export-bad-dir-inside/META.hocon", <<>>},
            {"/export-bad-dir-inside/mnesia/test_tab", <<>>}
        ],
        [compressed]
    ),
    InvalidEditionFileName = filename:join(
        ?config(priv_dir, Config), "export-invalid-edition.tar.gz"
    ),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(#{edition => "test", version => emqx_release:version()}, #{})
    ),
    ok = erl_tar:create(
        InvalidEditionFileName,
        [
            {"export-invalid-edition/cluster.hocon", <<>>},
            {"export-invalid-edition/META.hocon", Meta}
        ],
        [compressed]
    ),
    InvalidVersionFileName = filename:join(
        ?config(priv_dir, Config), "export-invalid-version.tar.gz"
    ),
    Meta1 = unicode:characters_to_binary(
        hocon_pp:do(#{edition => emqx_release:edition(), version => "test"}, #{})
    ),
    ok = erl_tar:create(
        InvalidVersionFileName,
        [
            {"export-invalid-version/cluster.hocon", <<>>},
            {"export-invalid-version/META.hocon", Meta1}
        ],
        [compressed]
    ),
    BadFileNameReason = bad_backup_name,
    NoMetaReason = missing_backup_meta,
    BadArchiveDirReason = bad_archive_dir,
    InvalidEditionReason = invalid_edition,
    InvalidVersionReason = invalid_version,
    ?assertEqual({error, BadFileNameReason}, emqx_mgmt_data_backup:import(BadFileName)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(BadFileNameReason)),
    ?assertEqual({error, NoMetaReason}, emqx_mgmt_data_backup:import(NoMetaFileName)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(NoMetaReason)),
    ?assertEqual(
        {error, BadArchiveDirReason},
        emqx_mgmt_data_backup:import(BadArchiveDirFileName)
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(BadArchiveDirReason)),
    ?assertEqual(
        {error, InvalidEditionReason},
        emqx_mgmt_data_backup:import(InvalidEditionFileName)
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(InvalidEditionReason)),
    ?assertEqual(
        {error, InvalidVersionReason},
        emqx_mgmt_data_backup:import(InvalidVersionFileName)
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(InvalidVersionReason)).

t_future_version(Config) ->
    CurrentVersion = list_to_binary(emqx_release:version()),
    [_, _ | Patch] = string:split(CurrentVersion, ".", all),
    {ok, {MajorInt, MinorInt}} = emqx_mgmt_data_backup:parse_version_no_patch(CurrentVersion),
    FutureMajorVersion = recompose_version(MajorInt + 1, MinorInt, Patch),
    FutureMinorVersion = recompose_version(MajorInt, MinorInt + 1, Patch),
    [MajorMeta, MinorMeta] =
        [
            unicode:characters_to_binary(
                hocon_pp:do(#{edition => emqx_release:edition(), version => V}, #{})
            )
         || V <- [FutureMajorVersion, FutureMinorVersion]
        ],
    MajorFileName = filename:join(?config(priv_dir, Config), "export-future-major-ver.tar.gz"),
    MinorFileName = filename:join(?config(priv_dir, Config), "export-future-minor-ver.tar.gz"),
    ok = erl_tar:create(
        MajorFileName,
        [
            {"export-future-major-ver/cluster.hocon", <<>>},
            {"export-future-major-ver/META.hocon", MajorMeta}
        ],
        [compressed]
    ),
    ok = erl_tar:create(
        MinorFileName,
        [
            {"export-future-minor-ver/cluster.hocon", <<>>},
            {"export-future-minor-ver/META.hocon", MinorMeta}
        ],
        [compressed]
    ),
    ExpMajorReason = {unsupported_version, FutureMajorVersion},
    ExpMinorReason = {unsupported_version, FutureMinorVersion},
    ?assertEqual({error, ExpMajorReason}, emqx_mgmt_data_backup:import(MajorFileName)),
    ?assertEqual({error, ExpMinorReason}, emqx_mgmt_data_backup:import(MinorFileName)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ExpMajorReason)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ExpMinorReason)).

t_bad_config(Config) ->
    BadConfigFileName = filename:join(?config(priv_dir, Config), "export-bad-config-backup.tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(#{edition => emqx_release:edition(), version => emqx_release:version()}, #{})
    ),
    BadConfigMap = #{
        <<"listeners">> =>
            #{
                <<"bad-type">> =>
                    #{<<"bad-name">> => #{<<"bad-field">> => <<"bad-val">>}}
            }
    },
    BadConfig = unicode:characters_to_binary(hocon_pp:do(BadConfigMap, #{})),
    ok = erl_tar:create(
        BadConfigFileName,
        [
            {"export-bad-config-backup/cluster.hocon", BadConfig},
            {"export-bad-config-backup/META.hocon", Meta}
        ],
        [compressed]
    ),
    Res = emqx_mgmt_data_backup:import(BadConfigFileName),
    ?assertMatch({error, #{kind := validation_error}}, Res).

t_import_on_cluster(Config) ->
    %% Randomly chosen config key to verify import result additionally
    ?assertEqual([], emqx:get_config([authentication])),
    BootstrapFile = filename:join(?config(data_dir, Config), ?BOOTSTRAP_BACKUP),
    ExpImportRes = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(ExpImportRes, emqx_mgmt_data_backup:import(BootstrapFile)),
    ImportedAuthnConf = emqx:get_config([authentication]),
    ?assertMatch([_ | _], ImportedAuthnConf),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, Cwd} = file:get_cwd(),
    AbsFilePath = filename:join(Cwd, FileName),
    [CoreNode1, _CoreNode2, ReplicantNode] = NodesList = ?config(cluster, Config),
    ReplImportReason = not_core_node,
    ?assertEqual(
        {error, ReplImportReason},
        rpc:call(ReplicantNode, emqx_mgmt_data_backup, import, [AbsFilePath])
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ReplImportReason)),
    [?assertEqual([], rpc:call(N, emqx, get_config, [[authentication]])) || N <- NodesList],
    ?assertEqual(
        ExpImportRes,
        rpc:call(CoreNode1, emqx_mgmt_data_backup, import, [AbsFilePath])
    ),
    [
        ?assertEqual(
            authn_ids(ImportedAuthnConf),
            authn_ids(rpc:call(N, emqx, get_config, [[authentication]]))
        )
     || N <- NodesList
    ].

t_verify_imported_mnesia_tab_on_cluster(Config) ->
    UsersToExport = users(<<"user_to_export_">>),
    UsersBeforeImport = users(<<"user_before_import_">>),
    [{ok, _} = emqx_dashboard_admin:add_user(U, U, U) || U <- UsersToExport],
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, Cwd} = file:get_cwd(),
    AbsFilePath = filename:join(Cwd, FileName),

    [CoreNode1, CoreNode2, ReplicantNode] = NodesList = ?config(cluster, Config),

    [
        {ok, _} = rpc:call(CoreNode1, emqx_dashboard_admin, add_user, [U, U, U])
     || U <- UsersBeforeImport
    ],

    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        rpc:call(CoreNode1, emqx_mgmt_data_backup, import, [AbsFilePath])
    ),

    [Tab] = emqx_dashboard_admin:backup_tables(),
    AllUsers = lists:sort(mnesia:dirty_all_keys(Tab) ++ UsersBeforeImport),
    [
        ?assertEqual(
            AllUsers,
            lists:sort(rpc:call(N, mnesia, dirty_all_keys, [Tab]))
        )
     || N <- [CoreNode1, CoreNode2]
    ],

    %% Give some extra time to replicant to import data...
    timer:sleep(3000),
    ?assertEqual(AllUsers, lists:sort(rpc:call(ReplicantNode, mnesia, dirty_all_keys, [Tab]))),

    [rpc:call(N, ekka, leave, []) || N <- lists:reverse(NodesList)],
    [emqx_common_test_helpers:stop_slave(N) || N <- NodesList].

t_mnesia_bad_tab_schema(_Config) ->
    OldAttributes = [id, name, description],
    ok = create_test_tab(OldAttributes),
    ok = mria:dirty_write({data_backup_test, <<"id">>, <<"old_name">>, <<"old_description">>}),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {atomic, ok} = mnesia:delete_table(data_backup_test),
    NewAttributes = [id, name, description, new_field],
    ok = create_test_tab(NewAttributes),
    NewRec =
        {data_backup_test, <<"id">>, <<"new_name">>, <<"new_description">>, <<"new_field_value">>},
    ok = mria:dirty_write(NewRec),
    ?assertEqual(
        {ok, #{
            db_errors =>
                #{data_backup_test => {error, {"Backup traversal failed", different_table_schema}}},
            config_errors => #{}
        }},
        emqx_mgmt_data_backup:import(FileName)
    ),
    ?assertEqual([NewRec], mnesia:dirty_read(data_backup_test, <<"id">>)),
    ?assertEqual([<<"id">>], mnesia:dirty_all_keys(data_backup_test)).

t_read_files(_Config) ->
    DataDir = emqx:data_dir(),
    %% Relative "data" path is set in init_per_testcase/2, asserting it must be safe
    ?assertEqual("data", DataDir),
    {ok, Cwd} = file:get_cwd(),
    AbsDataDir = filename:join(Cwd, DataDir),
    FileBaseName = "t_read_files_tmp_file",
    TestFileAbsPath = iolist_to_binary(filename:join(AbsDataDir, FileBaseName)),
    TestFilePath = iolist_to_binary(filename:join(DataDir, FileBaseName)),
    TestFileContent = <<"test_file_content">>,
    ok = file:write_file(TestFileAbsPath, TestFileContent),

    RawConf = #{
        <<"test_rootkey">> => #{
            <<"test_field">> => <<"test_field_path">>,
            <<"abs_data_dir_path_file">> => TestFileAbsPath,
            <<"rel_data_dir_path_file">> => TestFilePath,
            <<"path_outside_data_dir">> => <<"/tmp/some-file">>
        }
    },

    RawConf1 = emqx_utils_maps:deep_put(
        [<<"test_rootkey">>, <<"abs_data_dir_path_file">>], RawConf, TestFileContent
    ),
    ExpectedConf = emqx_utils_maps:deep_put(
        [<<"test_rootkey">>, <<"rel_data_dir_path_file">>], RawConf1, TestFileContent
    ),
    ?assertEqual(ExpectedConf, emqx_mgmt_data_backup:read_data_files(RawConf)).

%%------------------------------------------------------------------------------
%% Internal test helpers
%%------------------------------------------------------------------------------

setup(Config) ->
    %% avoid port conflicts if the cluster is started
    AppHandler = fun
        (emqx_dashboard) ->
            ok = emqx_config:put([dashboard, listeners, http, bind], 0);
        (_) ->
            ok
    end,
    ok = emqx_common_test_helpers:start_apps(apps_to_start(), AppHandler),
    PrevDataDir = application:get_env(emqx, data_dir),
    application:set_env(emqx, data_dir, "data"),
    [{previous_emqx_data_dir, PrevDataDir} | Config].

cleanup(Config) ->
    emqx_common_test_helpers:stop_apps(apps_to_start()),
    case ?config(previous_emqx_data_dir, Config) of
        undefined ->
            application:unset_env(emqx, data_dir);
        {ok, Val} ->
            application:set_env(emqx, data_dir, Val)
    end.

cleanup_cluster(ClusterNodes) ->
    [rpc:call(N, ekka, leave, []) || N <- lists:reverse(ClusterNodes)],
    [emqx_common_test_helpers:stop_slave(N) || N <- ClusterNodes].

users(Prefix) ->
    [
        <<Prefix/binary, (integer_to_binary(abs(erlang:unique_integer())))/binary>>
     || _ <- lists:seq(1, 10)
    ].

authn_ids(AuthnConf) ->
    lists:sort([emqx_authentication:authenticator_id(Conf) || Conf <- AuthnConf]).

recompose_version(MajorInt, MinorInt, Patch) ->
    unicode:characters_to_list(
        [integer_to_list(MajorInt + 1), $., integer_to_list(MinorInt), $. | Patch]
    ).

cluster(Config) ->
    PrivDataDir = ?config(priv_dir, Config),
    [{Core1, Core1Opts}, {Core2, Core2Opts}, {Replicant, ReplOpts}] =
        emqx_common_test_helpers:emqx_cluster(
            [
                {core, data_backup_core1},
                {core, data_backup_core2},
                {replicant, data_backup_replicant}
            ],
            #{
                priv_data_dir => PrivDataDir,
                schema_mod => emqx_conf_schema,
                apps => apps_to_start(),
                load_apps => apps_to_start() ++ apps_to_load(),
                env => [{mria, db_backend, rlog}],
                load_schema => true,
                start_autocluster => true,
                listener_ports => [],
                conf => [{[dashboard, listeners, http, bind], 0}],
                env_handler =>
                    fun(_) ->
                        application:set_env(emqx, boot_modules, [broker, router])
                    end
            }
        ),
    Node1 = emqx_common_test_helpers:start_slave(Core1, Core1Opts),
    Node2 = emqx_common_test_helpers:start_slave(Core2, Core2Opts),
    #{conf := _ReplConf, env := ReplEnv} = ReplOpts,
    ClusterDiscovery = {static, [{seeds, [Node1, Node2]}]},
    ReplOpts1 = maps:remove(
        join_to,
        ReplOpts#{
            env => [{ekka, cluster_discovery, ClusterDiscovery} | ReplEnv],
            env_handler => fun(_) ->
                application:set_env(emqx, boot_modules, [broker, router]),
                application:set_env(
                    ekka,
                    cluster_discovery,
                    ClusterDiscovery
                )
            end
        }
    ),
    ReplNode = emqx_common_test_helpers:start_slave(Replicant, ReplOpts1),
    [Node1, Node2, ReplNode].

create_test_tab(Attributes) ->
    ok = mria:create_table(data_backup_test, [
        {type, set},
        {rlog_shard, data_backup_test_shard},
        {storage, disc_copies},
        {record_name, data_backup_test},
        {attributes, Attributes},
        {storage_properties, [
            {ets, [
                {read_concurrency, true},
                {write_concurrency, true}
            ]}
        ]}
    ]),
    ok = mria:wait_for_tables([data_backup_test]).

apps_to_start() ->
    [
        emqx,
        emqx_conf,
        emqx_psk,
        emqx_management,
        emqx_dashboard,
        emqx_authz,
        emqx_authn,
        emqx_rule_engine,
        emqx_retainer,
        emqx_prometheus,
        emqx_modules,
        emqx_gateway,
        emqx_exhook,
        emqx_bridge,
        emqx_auto_subscribe
    ].

apps_to_load() ->
    [
        emqx_gateway_lwm2m,
        emqx_gateway_coap,
        emqx_gateway_exproto,
        emqx_gateway_stomp,
        emqx_gateway_mqttsn
    ].
