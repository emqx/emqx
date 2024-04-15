%%--------------------------------------------------------------------
%% Copyright (c) 2023-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx_utils/include/emqx_message.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-define(ROLE_SUPERUSER, <<"administrator">>).
-define(ROLE_API_SUPERUSER, <<"administrator">>).
-define(BOOTSTRAP_BACKUP, "emqx-export-test-bootstrap-ce.tar.gz").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_Config) ->
    ok.

init_per_testcase(TC = t_import_on_cluster, Config) ->
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
    [{cluster, cluster(TC, Config)} | setup(TC, Config)];
init_per_testcase(TC = t_verify_imported_mnesia_tab_on_cluster, Config) ->
    [{cluster, cluster(TC, Config)} | setup(TC, Config)];
init_per_testcase(t_mnesia_bad_tab_schema, Config) ->
    meck:new(emqx_mgmt_data_backup, [passthrough]),
    meck:expect(TC = emqx_mgmt_data_backup, mnesia_tabs_to_backup, 0, [?MODULE]),
    setup(TC, Config);
init_per_testcase(TC, Config) ->
    setup(TC, Config).

end_per_testcase(t_import_on_cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config),
    meck:unload(emqx_mgmt_listeners_conf),
    meck:unload(emqx_gateway_conf);
end_per_testcase(t_verify_imported_mnesia_tab_on_cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
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

t_cluster_hocon_import_mqtt_subscribers_retainer_messages(Config) ->
    case emqx_release:edition() of
        ce ->
            ok;
        ee ->
            FNameEmqx44 = "emqx-export-4.4.24-retainer-mqttsub.tar.gz",
            BackupFile = filename:join(?config(data_dir, Config), FNameEmqx44),
            Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
            ?assertEqual(Exp, emqx_mgmt_data_backup:import(BackupFile)),
            RawConfAfterImport = emqx:get_raw_config([]),
            %% verify that MQTT sources are imported
            ?assertMatch(
                #{<<"sources">> := #{<<"mqtt">> := Sources}} when map_size(Sources) > 0,
                RawConfAfterImport
            ),
            %% verify that retainer messages are imported
            ?assertMatch(
                {ok, [#message{payload = <<"test-payload">>}]},
                emqx_retainer:read_message(<<"test-retained-message/1">>)
            ),
            %% Export and import again
            {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
            ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)),
            ?assertEqual(RawConfAfterImport, emqx:get_raw_config([]))
    end,
    ok.

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
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(filename:basename(FileName))),

    %% backup data migration test
    ?assertMatch([_, _, _], ets:tab2list(emqx_app)),
    ?assertMatch(
        {ok, #{name := <<"key_to_export2">>, role := ?ROLE_API_SUPERUSER}},
        emqx_mgmt_auth:read(<<"key_to_export2">>)
    ),
    ok.

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
    [{ok, _} = emqx_dashboard_admin:add_user(U, U, ?ROLE_SUPERUSER, U) || U <- UsersToExport],
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, Cwd} = file:get_cwd(),
    AbsFilePath = filename:join(Cwd, FileName),

    [CoreNode1, CoreNode2, ReplicantNode] = ?config(cluster, Config),

    [
        {ok, _} = rpc:call(CoreNode1, emqx_dashboard_admin, add_user, [U, U, ?ROLE_SUPERUSER, U])
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
    ?assertEqual(AllUsers, lists:sort(rpc:call(ReplicantNode, mnesia, dirty_all_keys, [Tab]))).

backup_tables() ->
    [data_backup_test].

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

setup(TC, Config) ->
    WorkDir = filename:join(emqx_cth_suite:work_dir(TC, Config), local),
    Started = emqx_cth_suite:start(apps_to_start(), #{work_dir => WorkDir}),
    [{suite_apps, Started} | Config].

cleanup(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

users(Prefix) ->
    [
        <<Prefix/binary, (integer_to_binary(abs(erlang:unique_integer())))/binary>>
     || _ <- lists:seq(1, 10)
    ].

authn_ids(AuthnConf) ->
    lists:sort([emqx_authn_chains:authenticator_id(Conf) || Conf <- AuthnConf]).

recompose_version(MajorInt, MinorInt, Patch) ->
    unicode:characters_to_list(
        [integer_to_list(MajorInt + 1), $., integer_to_list(MinorInt), $. | Patch]
    ).

cluster(TC, Config) ->
    Nodes = emqx_cth_cluster:start(
        [
            {data_backup_core1, #{role => core, apps => apps_to_start()}},
            {data_backup_core2, #{role => core, apps => apps_to_start()}},
            {data_backup_replicant, #{role => replicant, apps => apps_to_start()}}
        ],
        #{work_dir => emqx_cth_suite:work_dir(TC, Config)}
    ),
    Nodes.

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
        {emqx, #{override_env => [{boot_modules, [broker]}]}},
        {emqx_conf, #{config => #{dashboard => #{listeners => #{http => #{bind => <<"0">>}}}}}},
        emqx_psk,
        emqx_management,
        emqx_dashboard,
        emqx_auth,
        emqx_auth_http,
        emqx_auth_jwt,
        emqx_auth_mnesia,
        emqx_auth_mongodb,
        emqx_auth_mysql,
        emqx_auth_postgresql,
        emqx_auth_redis,
        emqx_rule_engine,
        emqx_retainer,
        emqx_prometheus,
        emqx_modules,
        emqx_gateway,
        emqx_exhook,
        emqx_bridge_http,
        emqx_bridge,
        emqx_auto_subscribe,

        % loaded only
        emqx_gateway_lwm2m,
        emqx_gateway_coap,
        emqx_gateway_exproto,
        emqx_gateway_stomp,
        emqx_gateway_mqttsn
    ].
