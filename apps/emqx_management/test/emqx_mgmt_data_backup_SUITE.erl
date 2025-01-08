%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_common_test_helpers, [on_exit/1]).

-define(ROLE_SUPERUSER, <<"administrator">>).
-define(ROLE_API_SUPERUSER, <<"administrator">>).
-define(BOOTSTRAP_BACKUP, "emqx-export-test-bootstrap-ce.tar.gz").

-define(CACERT, <<
    "-----BEGIN CERTIFICATE-----\n"
    "MIIDUTCCAjmgAwIBAgIJAPPYCjTmxdt/MA0GCSqGSIb3DQEBCwUAMD8xCzAJBgNV\n"
    "BAYTAkNOMREwDwYDVQQIDAhoYW5nemhvdTEMMAoGA1UECgwDRU1RMQ8wDQYDVQQD\n"
    "DAZSb290Q0EwHhcNMjAwNTA4MDgwNjUyWhcNMzAwNTA2MDgwNjUyWjA/MQswCQYD\n"
    "VQQGEwJDTjERMA8GA1UECAwIaGFuZ3pob3UxDDAKBgNVBAoMA0VNUTEPMA0GA1UE\n"
    "AwwGUm9vdENBMIIBIjANBgkqhkiG9w0BAQEFAAOCAQ8AMIIBCgKCAQEAzcgVLex1\n"
    "EZ9ON64EX8v+wcSjzOZpiEOsAOuSXOEN3wb8FKUxCdsGrsJYB7a5VM/Jot25Mod2\n"
    "juS3OBMg6r85k2TWjdxUoUs+HiUB/pP/ARaaW6VntpAEokpij/przWMPgJnBF3Ur\n"
    "MjtbLayH9hGmpQrI5c2vmHQ2reRZnSFbY+2b8SXZ+3lZZgz9+BaQYWdQWfaUWEHZ\n"
    "uDaNiViVO0OT8DRjCuiDp3yYDj3iLWbTA/gDL6Tf5XuHuEwcOQUrd+h0hyIphO8D\n"
    "tsrsHZ14j4AWYLk1CPA6pq1HIUvEl2rANx2lVUNv+nt64K/Mr3RnVQd9s8bK+TXQ\n"
    "KGHd2Lv/PALYuwIDAQABo1AwTjAdBgNVHQ4EFgQUGBmW+iDzxctWAWxmhgdlE8Pj\n"
    "EbQwHwYDVR0jBBgwFoAUGBmW+iDzxctWAWxmhgdlE8PjEbQwDAYDVR0TBAUwAwEB\n"
    "/zANBgkqhkiG9w0BAQsFAAOCAQEAGbhRUjpIred4cFAFJ7bbYD9hKu/yzWPWkMRa\n"
    "ErlCKHmuYsYk+5d16JQhJaFy6MGXfLgo3KV2itl0d+OWNH0U9ULXcglTxy6+njo5\n"
    "CFqdUBPwN1jxhzo9yteDMKF4+AHIxbvCAJa17qcwUKR5MKNvv09C6pvQDJLzid7y\n"
    "E2dkgSuggik3oa0427KvctFf8uhOV94RvEDyqvT5+pgNYZ2Yfga9pD/jjpoHEUlo\n"
    "88IGU8/wJCx3Ds2yc8+oBg/ynxG8f/HmCC1ET6EHHoe2jlo8FpU/SgGtghS1YL30\n"
    "IWxNsPrUP+XsZpBJy/mvOhE5QXo6Y35zDqqj8tI7AGmAWu22jg==\n"
    "-----END CERTIFICATE-----"
>>).
-define(GLOBAL_AUTHN_CHAIN, 'mqtt:global').
-define(ON(NODE, BODY), erpc:call(NODE, fun() -> BODY end)).

all() ->
    case emqx_cth_suite:skip_if_oss() of
        false ->
            emqx_common_test_helpers:all(?MODULE);
        True ->
            True
    end.

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
    meck:expect(TC = emqx_mgmt_data_backup, modules_with_mnesia_tabs_to_backup, 0, [?MODULE]),
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

t_import_retained_messages(Config) ->
    FName = "emqx-export-ce-retained-msgs-test.tar.gz",
    BackupFile = filename:join(?config(data_dir, Config), FName),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(BackupFile)),
    %% verify that retainer messages are imported
    ?assertMatch(
        {ok, [#message{payload = <<"Hi 1!!!">>}]},
        emqx_retainer:read_message(<<"t/backup-retainer/test1">>)
    ),
    ?assertMatch(
        {ok, [#message{payload = <<"Hi 5!!!">>}]},
        emqx_retainer:read_message(<<"t/backup-retainer/test5">>)
    ),

    %% verify that messages are re-indexed
    ?assertMatch(
        {ok, _, [
            #message{payload = <<"Hi 5!!!">>},
            #message{payload = <<"Hi 4!!!">>},
            #message{payload = <<"Hi 3!!!">>},
            #message{payload = <<"Hi 2!!!">>},
            #message{payload = <<"Hi 1!!!">>}
        ]},
        emqx_retainer:page_read(<<"t/backup-retainer/#">>, 1, 5)
    ),
    %% Export and import again
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(FileName)).

t_export_ram_retained_messages(_Config) ->
    {ok, _} = emqx_retainer:update_config(
        #{
            <<"enable">> => true,
            <<"backend">> => #{<<"storage_type">> => <<"ram">>}
        }
    ),
    ?assertEqual(ram_copies, mnesia:table_info(emqx_retainer_message, storage_type)),
    Topic = <<"t/backup_test_export_retained_ram/1">>,
    Payload = <<"backup_test_retained_ram">>,
    Msg = emqx_message:make(
        <<"backup_test">>,
        ?QOS_0,
        Topic,
        Payload,
        #{retain => true},
        #{}
    ),
    _ = emqx_broker:publish(Msg),
    {ok, #{filename := BackupFileName}} = emqx_mgmt_data_backup:export(),
    ok = emqx_retainer:delete(Topic),
    ?assertEqual({ok, []}, emqx_retainer:read_message(Topic)),
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        emqx_mgmt_data_backup:import(BackupFileName)
    ),
    ?assertMatch({ok, [#message{payload = Payload}]}, emqx_retainer:read_message(Topic)).

t_export_cloud_subset(Config) ->
    setup_t_export_cloud_subset_scenario(),
    Opts = #{
        raw_conf_transform => fun(RawConf) ->
            maps:with(
                [
                    <<"connectors">>,
                    <<"actions">>,
                    <<"sources">>,
                    <<"rule_engine">>,
                    <<"schema_registry">>
                ],
                RawConf
            )
        end,
        mnesia_table_filter => fun(TableName) ->
            lists:member(
                TableName,
                [
                    %% mnesia builtin authn
                    emqx_authn_mnesia,
                    emqx_authn_scram_mnesia,
                    %% mnesia builtin authz
                    emqx_acl,
                    %% banned
                    emqx_banned,
                    emqx_banned_rules
                ]
            )
        end
    },
    {ok, #{filename := BackupFileName}} = emqx_mgmt_data_backup:export(Opts),
    #{
        cluster_hocon := RawHocon,
        mnesia_tables := Tables
    } = inspect_backup(BackupFileName),
    {ok, Hocon} = hocon:binary(RawHocon),
    ?assertEqual(
        lists:sort([
            <<"connectors">>,
            <<"actions">>,
            <<"sources">>,
            <<"rule_engine">>,
            <<"schema_registry">>
        ]),
        lists:sort(maps:keys(Hocon))
    ),
    ?assertEqual(
        lists:sort([
            <<"emqx_authn_mnesia">>,
            <<"emqx_authn_scram_mnesia">>,
            <<"emqx_acl">>,
            <<"emqx_banned">>,
            <<"emqx_banned_rules">>
        ]),
        lists:sort(maps:keys(Tables))
    ),
    %% Using a fresh cluster to avoid dirty environment.
    Nodes = [N1 | _] = cluster(?FUNCTION_NAME, Config),
    on_exit(fun() -> emqx_cth_cluster:stop(Nodes) end),
    ?ON(
        N1,
        ?assertEqual(
            {ok, #{db_errors => #{}, config_errors => #{}}},
            emqx_mgmt_data_backup:import(BackupFileName)
        )
    ),
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
    ?assertMatch([_, _, _, _], ets:tab2list(emqx_app)),
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

t_cluster_links(_Config) ->
    case emqx_release:edition() of
        ce ->
            %% Only available in EMQX Enterprise
            ok;
        ee ->
            Link = #{
                <<"name">> => <<"emqxcl_backup_test">>,
                <<"server">> => <<"emqx.emqxcl_backup_test.host:41883">>,
                <<"topics">> => [<<"#">>],
                <<"ssl">> => #{<<"enable">> => true, <<"cacertfile">> => ?CACERT}
            },
            {ok, [RawLink]} = emqx_cluster_link_config:update([Link]),
            {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
            {ok, []} = emqx_cluster_link_config:update([]),
            #{<<"ssl">> := #{<<"cacertfile">> := CertPath}} = RawLink,
            _ = file:delete(CertPath),
            ?assertEqual(
                {ok, #{db_errors => #{}, config_errors => #{}}},
                emqx_mgmt_data_backup:import(FileName)
            ),
            [
                #{
                    <<"name">> := <<"emqxcl_backup_test">>,
                    <<"ssl">> := #{<<"cacertfile">> := CertPath1}
                }
            ] = emqx:get_raw_config([cluster, links]),
            ?assertEqual({ok, ?CACERT}, file:read_file(CertPath1))
    end.

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

    {_Name, [Tab]} = emqx_dashboard_admin:backup_tables(),
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
    {<<"mocked_test">>, [data_backup_test]}.

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
    Started = emqx_cth_suite:start(apps_to_start(TC), #{work_dir => WorkDir}),
    [{suite_apps, Started} | Config].

cleanup(Config) ->
    emqx_common_test_helpers:call_janitor(),
    emqx_cth_suite:stop(?config(suite_apps, Config)).

setup_t_export_cloud_subset_scenario() ->
    ok = emqx_dashboard:stop_listeners(),
    {ok, _} = emqx_conf:update(
        [dashboard, listeners, http, bind],
        18083,
        #{override_to => cluster}
    ),
    ok = emqx_dashboard:start_listeners(),
    ready = emqx_dashboard_listener:regenerate_minirest_dispatch(),
    {ok, _} = emqx_license:update_key(<<"default">>),
    {ok, _} = emqx_dashboard_admin:add_user(
        <<"some_admin">>,
        <<"Adminpass123">>,
        ?ROLE_SUPERUSER,
        <<"shouldn't be exported">>
    ),
    {ok, _} = emqx_authn_chains:create_authenticator(
        ?GLOBAL_AUTHN_CHAIN,
        #{
            mechanism => password_based,
            backend => built_in_database,
            enable => true,
            user_id_type => username,
            password_hash_algorithm => #{
                name => plain,
                salt_position => suffix
            }
        }
    ),
    {ok, _} = emqx_authn_chains:add_user(
        ?GLOBAL_AUTHN_CHAIN,
        <<"password_based:built_in_database">>,
        #{
            user_id => <<"user1">>,
            password => <<"user1pass">>
        }
    ),
    ok = emqx_authz_mnesia:store_rules(
        {username, <<"user2">>},
        [
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"publish">>,
                <<"topic">> => <<"t">>
            },
            #{
                <<"permission">> => <<"allow">>,
                <<"action">> => <<"subscribe">>,
                <<"topic">> => <<"t/+">>
            }
        ]
    ),
    ConnectorName1 = <<"source1">>,
    ConnectorConfig1 = #{
        <<"enable">> => true,
        <<"description">> => <<"my connector">>,
        <<"pool_size">> => 3,
        <<"proto_ver">> => <<"v5">>,
        <<"server">> => <<"127.0.0.1:1883">>,
        <<"resource_opts">> => #{
            <<"health_check_interval">> => <<"15s">>,
            <<"start_after_created">> => true,
            <<"start_timeout">> => <<"5s">>
        }
    },
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api([
            {connector_type, <<"mqtt">>},
            {connector_name, ConnectorName1},
            {connector_config, ConnectorConfig1}
        ])
    ),
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api([
            {bridge_kind, source},
            {source_type, <<"mqtt">>},
            {source_name, <<"source1">>},
            {source_config, #{
                <<"enable">> => true,
                <<"connector">> => ConnectorName1,
                <<"parameters">> =>
                    #{
                        <<"topic">> => <<"remote/topic">>,
                        <<"qos">> => 2
                    },
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"15s">>,
                    <<"resume_interval">> => <<"15s">>
                }
            }}
        ])
    ),
    ConnectorName2 = <<"action1">>,
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_connector_api([
            {connector_type, <<"mqtt">>},
            {connector_name, ConnectorName2},
            {connector_config, ConnectorConfig1}
        ])
    ),
    {201, _} = emqx_bridge_v2_testlib:simplify_result(
        emqx_bridge_v2_testlib:create_kind_api([
            {bridge_kind, action},
            {action_type, <<"mqtt">>},
            {action_name, <<"action1">>},
            {action_config, #{
                <<"enable">> => true,
                <<"connector">> => ConnectorName1,
                <<"parameters">> =>
                    #{
                        <<"topic">> => <<"local/topic">>,
                        <<"qos">> => 2
                    },
                <<"resource_opts">> => #{
                    <<"health_check_interval">> => <<"15s">>,
                    <<"resume_interval">> => <<"15s">>
                }
            }}
        ])
    ),
    {ok, _} = emqx_bridge_v2_testlib:create_rule_api(#{
        sql => <<"select * from '$bridges/mqtt:source1'">>,
        actions => []
    }),
    {ok, _} = emqx_bridge_v2_testlib:create_rule_api(#{
        sql => <<"select * from 't/action'">>,
        actions => [<<"mqtt:action1">>]
    }),
    {201, _} = emqx_mgmt_api_test_util:simple_request(
        post,
        emqx_mgmt_api_test_util:api_path(["cluster", "links"]),
        #{
            <<"name">> => <<"clink1">>,
            <<"clientid">> => <<"linkclientid">>,
            <<"password">> => <<"my secret password">>,
            <<"pool_size">> => 1,
            <<"server">> => <<"emqxcl_2.nohost:31883">>,
            <<"topics">> => [<<"t/test-topic">>, <<"t/test/#">>]
        }
    ),
    {201, _} = emqx_mgmt_api_test_util:simple_request(
        post,
        emqx_mgmt_api_test_util:api_path(["schema_registry"]),
        #{
            <<"name">> => <<"schema1">>,
            <<"type">> => <<"json">>,
            <<"source">> => emqx_utils_json:encode(#{
                properties => #{
                    foo => #{},
                    bar => #{}
                },
                required => [<<"foo">>]
            })
        }
    ),
    ok.

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

apps_to_start(t_cluster_links) ->
    case emqx_release:edition() of
        ee -> apps_to_start() ++ [emqx_cluster_link];
        ce -> []
    end;
apps_to_start(t_export_cloud_subset) ->
    case emqx_release:edition() of
        ee -> apps_to_start() ++ [emqx_schema_registry, emqx_cluster_link];
        ce -> []
    end;
apps_to_start(_TC) ->
    apps_to_start().

apps_to_start() ->
    [
        {emqx, #{override_env => [{boot_modules, [broker]}]}},
        {emqx_conf, #{config => #{}}},
        {emqx_license, ""},
        emqx_psk,
        emqx_management,
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
        emqx_mgmt_api_test_util:emqx_dashboard("dashboard.listeners.http.bind = 0"),

        % loaded only
        emqx_gateway_lwm2m,
        emqx_gateway_coap,
        emqx_gateway_exproto,
        emqx_gateway_stomp,
        emqx_gateway_mqttsn
    ].

inspect_backup(Filepath) ->
    {ok, Contents} = erl_tar:extract(Filepath, [compressed, memory]),
    [{_, Meta}] = filter_matching(Contents, <<"/META.hocon$">>),
    [{_, ClusterHocon}] = filter_matching(Contents, <<"/cluster.hocon$">>),
    MnesiaTables0 = filter_matching(Contents, <<"/mnesia/.*$">>),
    MnesiaTables1 = lists:map(
        fun({Path, FileContents}) ->
            {strip_prefix(Path, 2), FileContents}
        end,
        MnesiaTables0
    ),
    #{
        meta => Meta,
        cluster_hocon => ClusterHocon,
        mnesia_tables => maps:from_list(MnesiaTables1)
    }.

filter_matching(FileTree, RE) ->
    lists:filter(
        fun({Path, _FileContents}) ->
            match =:= re:run(Path, RE, [{capture, none}])
        end,
        FileTree
    ).

strip_prefix(Path, N) ->
    Segments = filename:split(Path),
    iolist_to_binary(filename:join(lists:nthtail(N, Segments))).
