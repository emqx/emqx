%%--------------------------------------------------------------------
%% Copyright (c) 2023-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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
        2,
        {ok, #{changed => [], root_key => listeners}}
    ),
    meck:expect(
        emqx_gateway_conf,
        import_config,
        2,
        {ok, #{changed => [], root_key => gateway}}
    ),
    [{cluster, cluster(TC, Config)} | setup(TC, Config)];
init_per_testcase(TC = t_verify_imported_mnesia_tab_on_cluster, Config) ->
    [{cluster, cluster(TC, Config)} | setup(TC, Config)];
init_per_testcase(TC = t_import_file_authz_source_on_cluster, Config) ->
    %% Don't import listeners to avoid port conflicts when running in a cluster.
    meck:new(emqx_mgmt_listeners_conf, [passthrough]),
    meck:new(emqx_gateway_conf, [passthrough]),
    meck:expect(
        emqx_mgmt_listeners_conf,
        import_config,
        2,
        {ok, #{changed => [], root_key => listeners}}
    ),
    meck:expect(
        emqx_gateway_conf,
        import_config,
        2,
        {ok, #{changed => [], root_key => gateway}}
    ),
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
end_per_testcase(t_import_file_authz_source_on_cluster, Config) ->
    emqx_cth_cluster:stop(?config(cluster, Config)),
    cleanup(Config),
    meck:unload(emqx_mgmt_listeners_conf),
    meck:unload(emqx_gateway_conf);
end_per_testcase(t_mnesia_bad_tab_schema, Config) ->
    cleanup(Config),
    meck:unload(emqx_mgmt_data_backup);
end_per_testcase(_TestCase, Config) ->
    cleanup(Config).

t_empty_export_import(_Config) ->
    ExpRawConf = emqx:get_raw_config([]),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),
    ?assertEqual(ExpRawConf, emqx:get_raw_config([])),
    %% idempotent update assert
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),
    ?assertEqual(ExpRawConf, emqx:get_raw_config([])).

t_cluster_hocon_import_mqtt_subscribers_retainer_messages(Config) ->
    FNameEmqx44 = "emqx-export-4.4.24-retainer-mqttsub.tar.gz",
    BackupFile = filename:join(?config(data_dir, Config), FNameEmqx44),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import_local(BackupFile)),
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
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),
    ?assertEqual(
        normalize_checked_config(RawConfAfterImport),
        normalize_checked_config(emqx:get_raw_config([])),
        #{
            diff => deep_diff_maps(
                normalize_checked_config(emqx:get_raw_config([])),
                normalize_checked_config(RawConfAfterImport)
            )
        }
    ),
    ok.

t_import_retained_messages(Config) ->
    FName = "emqx-export-ce-retained-msgs-test.tar.gz",
    BackupFile = filename:join(?config(data_dir, Config), FName),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import_local(BackupFile)),
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
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))).

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
        emqx_mgmt_data_backup:import(basename(BackupFileName))
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
            emqx_mgmt_data_backup:import_local(BackupFileName)
        )
    ),
    ok.

t_cluster_hocon_export_import(Config) ->
    RawConfBeforeImport = emqx:get_raw_config([]),
    BootstrapFile = filename:join(?config(data_dir, Config), ?BOOTSTRAP_BACKUP),
    Exp = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(Exp, emqx_mgmt_data_backup:import_local(BootstrapFile)),
    RawConfAfterImport = emqx:get_raw_config([]),
    ?assertNotEqual(RawConfBeforeImport, RawConfAfterImport),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),
    ?assertEqual(
        normalize_checked_config(RawConfAfterImport),
        normalize_checked_config(emqx:get_raw_config([])),
        #{
            diff => deep_diff_maps(
                normalize_checked_config(RawConfAfterImport),
                normalize_checked_config(emqx:get_raw_config([]))
            )
        }
    ),
    %% idempotent update assert
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),
    ?assertEqual(
        normalize_checked_config(RawConfAfterImport),
        normalize_checked_config(emqx:get_raw_config([]))
    ),
    %% lookup file inside <data_dir>/backup
    ?assertEqual(Exp, emqx_mgmt_data_backup:import(basename(FileName))),

    %% backup data migration test
    ?assertMatch([_, _, _, _], ets:tab2list(emqx_app)),
    ?assertMatch(
        {ok, #{name := <<"key_to_export2">>, role := ?ROLE_API_SUPERUSER}},
        emqx_mgmt_auth:read(<<"key_to_export2">>)
    ),
    ok.

t_tar_outside_backup_dir(Config) ->
    BackupFileName = filename:join(?config(priv_dir, Config), "tar_outside_backup_dir.tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(#{edition => emqx_release:edition(), version => emqx_release:version()}, #{})
    ),
    ok = erl_tar:create(
        BackupFileName,
        [
            {"tar_outside_backup_dir/../../cluster.hocon", <<>>},
            {"tar_outside_backup_dir/META.hocon", Meta}
        ],
        [compressed]
    ),
    {error, Message} = emqx_mgmt_data_backup:import_local(BackupFileName),
    ?assert(
        string:str(Message, "The path points above the current working directory") > 0
    ).

t_unsafe_backup_name(Config) ->
    {ok, #{filename := Filename}} = emqx_mgmt_data_backup:export(),
    ?assert(filelib:is_regular(Filename)),
    BackupFileName = filename:join(?config(priv_dir, Config), "...tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(#{edition => emqx_release:edition(), version => emqx_release:version()}, #{})
    ),
    ok = erl_tar:create(
        BackupFileName,
        [
            {"../cluster.hocon", <<>>},
            {"../META.hocon", Meta}
        ],
        [compressed]
    ),
    {ok, BackupFileContent} = file:read_file(BackupFileName),
    ?assertEqual(
        {error, unsafe_backup_name},
        emqx_mgmt_data_backup:upload(filename:basename(BackupFileName), BackupFileContent)
    ),
    ?assert(filelib:is_regular(Filename)).

t_no_backup_file(_Config) ->
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(not_found)),
    ?assertEqual(
        {error, not_found}, emqx_mgmt_data_backup:import("no_such_backup.tar.gz")
    ),
    ?assertEqual(
        {error, not_found}, emqx_mgmt_data_backup:import_local("/no_such_backup.tar.gz")
    ).

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
    ?assertEqual({error, BadFileNameReason}, emqx_mgmt_data_backup:import_local(BadFileName)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(BadFileNameReason)),
    ?assertEqual({error, NoMetaReason}, emqx_mgmt_data_backup:import_local(NoMetaFileName)),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(NoMetaReason)),
    ?assertEqual(
        {error, BadArchiveDirReason},
        emqx_mgmt_data_backup:import_local(BadArchiveDirFileName)
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(BadArchiveDirReason)),
    ?assertEqual(
        {error, InvalidEditionReason},
        emqx_mgmt_data_backup:import_local(InvalidEditionFileName)
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(InvalidEditionReason)),
    ?assertEqual(
        {error, InvalidVersionReason},
        emqx_mgmt_data_backup:import_local(InvalidVersionFileName)
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
    ?assertEqual({error, ExpMajorReason}, emqx_mgmt_data_backup:import_local(MajorFileName)),
    ?assertEqual({error, ExpMinorReason}, emqx_mgmt_data_backup:import_local(MinorFileName)),
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
    Res = emqx_mgmt_data_backup:import_local(BadConfigFileName),
    ?assertMatch({error, #{kind := validation_error}}, Res).

%% Regression test: when the imported cluster.hocon contains only a sub-set of
%% a root that has required fields (e.g. a partial `node' section without
%% `node.cookie'), the schema check must still succeed by falling back to the
%% running node's value for the missing fields.
t_import_cluster_hocon_partial_node(Config) ->
    BackupFileName = filename:join(?config(priv_dir, Config), "export-partial-node-backup.tar.gz"),
    Meta = unicode:characters_to_binary(
        hocon_pp:do(#{edition => emqx_release:edition(), version => emqx_release:version()}, #{})
    ),
    %% Cluster.hocon contains only a subset of `node' fields; `node.cookie' is
    %% intentionally omitted to mimic an export produced when only a non-cookie
    %% field of `node' was overridden.
    PartialNodeMap = #{
        <<"node">> => #{
            <<"global_gc_interval">> => <<"15m">>
        }
    },
    PartialConf = unicode:characters_to_binary(hocon_pp:do(PartialNodeMap, #{})),
    ok = erl_tar:create(
        BackupFileName,
        [
            {"export-partial-node-backup/cluster.hocon", PartialConf},
            {"export-partial-node-backup/META.hocon", Meta}
        ],
        [compressed]
    ),
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        emqx_mgmt_data_backup:import_local(BackupFileName)
    ).

t_cluster_links(_Config) ->
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
        emqx_mgmt_data_backup:import(basename(FileName))
    ),
    [
        #{
            <<"name">> := <<"emqxcl_backup_test">>,
            <<"ssl">> := #{<<"cacertfile">> := CertPath1}
        }
    ] = emqx:get_raw_config([cluster, links]),
    ?assertEqual({ok, ?CACERT}, file:read_file(CertPath1)).

-doc """
The link's `password` field is typed `emqx_schema_secret`; the raw config
on disk stores it verbatim, but the runtime config wraps it in an
`emqx_secret` closure. This case verifies a full export → wipe → import
round-trip preserves the original secret value (as observed via
`emqx_secret:unwrap/1`).

Also inspects the actual exported archive bytes to document how the
secret travels in the backup file.
""".
t_cluster_link_password_round_trip(_Config) ->
    %% Long, distinctive value so any truncation/mangling shows up obviously.
    Password = <<"d3adb33fc4f3-pw-r0undtr1p-t3st-EMQX-2026-05-26-aaa">>,
    LinkName = <<"clink_pw_roundtrip">>,
    Link = #{
        <<"name">> => LinkName,
        <<"server">> => <<"emqx.clink_pw.host:41883">>,
        <<"clientid">> => <<"clink-pw-client">>,
        <<"password">> => Password,
        <<"topics">> => [<<"t/pw/#">>]
    },
    {ok, [_]} = emqx_cluster_link_config:update([Link]),
    [#{password := WrappedBefore}] = emqx:get_config([cluster, links]),
    ?assertEqual(Password, emqx_secret:unwrap(WrappedBefore)),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, []} = emqx_cluster_link_config:update([]),
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        emqx_mgmt_data_backup:import(basename(FileName))
    ),
    [#{name := LinkName, password := WrappedAfter}] = emqx:get_config([cluster, links]),
    ?assertEqual(Password, emqx_secret:unwrap(WrappedAfter)),
    %% Inspect the archive: the secret is *not* encrypted nor file-referenced
    %% by export, it ends up in cluster.hocon as the literal value the user
    %% supplied. Encoding the current behaviour so a future change to
    %% encrypt/redact secrets-at-rest in backups breaks this assertion loudly.
    #{cluster_hocon := RawHocon} = inspect_backup(FileName),
    ?assertNotEqual(nomatch, binary:match(RawHocon, Password)).

-doc """
Round-trips multiple distinct links and verifies all are restored with
fields intact and in the original list order.

Order matters: cluster-link uses first-link-matched semantics, so a
re-ordered import would silently change message routing.

As a bonus, updates one link post-import and asserts the others are
untouched.
""".
t_cluster_link_multi_round_trip(_Config) ->
    L1Name = <<"clink_multi_a">>,
    L2Name = <<"clink_multi_b">>,
    L3Name = <<"clink_multi_c">>,
    L1 = #{
        <<"name">> => L1Name,
        <<"server">> => <<"emqx.a.host:41883">>,
        <<"clientid">> => <<"client-a">>,
        <<"topics">> => [<<"a/#">>]
    },
    L2 = #{
        <<"name">> => L2Name,
        <<"server">> => <<"emqx.b.host:41884">>,
        <<"clientid">> => <<"client-b">>,
        <<"topics">> => [<<"b/topic">>, <<"b/other/#">>],
        <<"ssl">> => #{<<"enable">> => true, <<"cacertfile">> => ?CACERT}
    },
    L3 = #{
        <<"name">> => L3Name,
        <<"server">> => <<"emqx.c.host:41885">>,
        <<"clientid">> => <<"client-c">>,
        <<"topics">> => [<<"c/#">>],
        <<"pool_size">> => 4
    },
    Links = [L1, L2, L3],
    {ok, RawBefore} = emqx_cluster_link_config:update(Links),
    ?assertEqual(
        [L1Name, L2Name, L3Name],
        [maps:get(<<"name">>, L) || L <- RawBefore]
    ),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    %% Clean up SSL side-files so the import has to re-create them.
    lists:foreach(
        fun
            (#{<<"ssl">> := #{<<"cacertfile">> := P}}) -> _ = file:delete(P);
            (_) -> ok
        end,
        RawBefore
    ),
    {ok, []} = emqx_cluster_link_config:update([]),
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        emqx_mgmt_data_backup:import(basename(FileName))
    ),
    RawAfter = emqx:get_raw_config([cluster, links]),
    ?assertEqual(
        [L1Name, L2Name, L3Name],
        [maps:get(<<"name">>, L) || L <- RawAfter]
    ),
    ?assertMatch(
        [
            #{<<"name">> := L1Name, <<"server">> := <<"emqx.a.host:41883">>},
            #{
                <<"name">> := L2Name,
                <<"server">> := <<"emqx.b.host:41884">>,
                <<"topics">> := [<<"b/topic">>, <<"b/other/#">>],
                <<"ssl">> := #{<<"enable">> := true, <<"cacertfile">> := _}
            },
            #{
                <<"name">> := L3Name,
                <<"server">> := <<"emqx.c.host:41885">>,
                <<"pool_size">> := 4
            }
        ],
        RawAfter
    ),
    [_, #{<<"ssl">> := #{<<"cacertfile">> := CertPath}} | _] = RawAfter,
    ?assertEqual({ok, ?CACERT}, file:read_file(CertPath)),
    %% Bonus: update one link, the others must be untouched.
    L2Updated = L2#{<<"pool_size">> => 9},
    {ok, _} = emqx_cluster_link_config:update_link(L2Updated),
    [
        #{<<"name">> := L1Name, <<"server">> := <<"emqx.a.host:41883">>},
        #{<<"name">> := L2Name, <<"pool_size">> := 9},
        #{<<"name">> := L3Name, <<"pool_size">> := 4}
    ] = emqx:get_raw_config([cluster, links]).

-doc """
`ps_actor_incarnation` is a per-link integer set by the config handler
(`maybe_increment_ps_actor_incr/2` in `emqx_cluster_link_config`) and
used as a globally synchronised sequence so persistent-routes actors
agree on the next incarnation after each config change. It is a
schema field with importance `?IMPORTANCE_HIDDEN` and is settable via
raw config.

This case verifies the value is preserved across an export/wipe/import
round-trip — i.e. the imported live config does not reset back to the
schema default of 0 — so persistent-routes actors after import resume
with the same generation as before export.
""".
t_cluster_link_ps_actor_incarnation_preserved(_Config) ->
    LinkName = <<"clink_ps_actor">>,
    Incarnation = 7,
    Link = #{
        <<"name">> => LinkName,
        <<"server">> => <<"emqx.ps_actor.host:41883">>,
        <<"topics">> => [<<"ps/#">>],
        <<"ps_actor_incarnation">> => Incarnation
    },
    {ok, _} = emqx_cluster_link_config:update([Link]),
    ?assertMatch(
        [#{name := LinkName, ps_actor_incarnation := Incarnation}],
        emqx:get_config([cluster, links])
    ),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, []} = emqx_cluster_link_config:update([]),
    ?assertEqual(
        [],
        emqx:get_config([cluster, links])
    ),
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        emqx_mgmt_data_backup:import(basename(FileName))
    ),
    ?assertMatch(
        [#{name := LinkName, ps_actor_incarnation := Incarnation}],
        emqx:get_config([cluster, links])
    ).

t_import_on_cluster(Config) ->
    %% Randomly chosen config key to verify import result additionally
    ?assertEqual([], emqx:get_config([authentication])),
    BootstrapFile = filename:join(?config(data_dir, Config), ?BOOTSTRAP_BACKUP),
    ExpImportRes = {ok, #{db_errors => #{}, config_errors => #{}}},
    ?assertEqual(ExpImportRes, emqx_mgmt_data_backup:import_local(BootstrapFile)),
    ImportedAuthnConf = emqx:get_config([authentication]),
    ?assertMatch([_ | _], ImportedAuthnConf),
    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),
    {ok, Cwd} = file:get_cwd(),
    AbsFilePath = filename:join(Cwd, FileName),
    [CoreNode1, _CoreNode2, ReplicantNode] = NodesList = ?config(cluster, Config),
    ReplImportReason = not_core_node,
    ?assertEqual(
        {error, ReplImportReason},
        rpc:call(ReplicantNode, emqx_mgmt_data_backup, import_local, [AbsFilePath])
    ),
    ?assertMatch([_ | _], emqx_mgmt_data_backup:format_error(ReplImportReason)),
    [?assertEqual([], rpc:call(N, emqx, get_config, [[authentication]])) || N <- NodesList],
    ?assertEqual(
        ExpImportRes,
        rpc:call(CoreNode1, emqx_mgmt_data_backup, import_local, [AbsFilePath])
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
        rpc:call(CoreNode1, emqx_mgmt_data_backup, import_local, [AbsFilePath])
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

%% Regression: importing a backup that contains an authz `file` source used to
%% strip inline `rules` on the importer node and ship only `path` across the
%% cluster. Peer nodes had no such file on disk and failed to apply the change
%% with `failed_to_read_acl_file` / `cluster_rpc_apply_failed`. The fix keeps
%% `rules` inline in the cluster-bound config so each peer writes its own
%% local copy of acl.conf during pre_config_update.
t_import_file_authz_source_on_cluster(Config) ->
    [Core1, Core2, _Replicant] = ?config(cluster, Config),
    BackupRules = <<"{allow, {username, \"backup_user\"}, all, [\"#\"]}.\n">>,
    BackupSource = #{
        <<"type">> => <<"file">>,
        <<"enable">> => true,
        <<"rules">> => BackupRules
    },
    %% Set the file source via the safe rules-bearing API path so that
    %% both nodes already have a local acl.conf before export.
    {ok, _} = ?ON(
        Core1,
        emqx_conf:update(
            [authorization, sources],
            {replace, [BackupSource]},
            #{override_to => cluster}
        )
    ),
    {ok, #{filename := FileName}} = ?ON(Core1, emqx_mgmt_data_backup:export()),
    {ok, Cwd} = ?ON(Core1, file:get_cwd()),
    AbsPath = filename:join(Cwd, FileName),
    %% Change authz to something different so re-import triggers a real
    %% cluster_rpc update on both nodes.
    OtherSource = #{
        <<"type">> => <<"file">>,
        <<"enable">> => true,
        <<"rules">> => <<"{deny, all}.\n">>
    },
    {ok, _} = ?ON(
        Core1,
        emqx_conf:update(
            [authorization, sources],
            {replace, [OtherSource]},
            #{override_to => cluster}
        )
    ),
    %% Delete acl.conf on Core2 so that the import actually has to recreate
    %% the file from the replicated `rules`. Without this, the bug could
    %% silently leave the stale OtherSource content in place on Core2 and
    %% only the content assertion below would catch it.
    ok = ?ON(Core2, file:delete(emqx_authz_file:acl_conf_file())),
    %% Import — pre-fix this triggered cluster_rpc_apply_failed on Core2
    %% because the importer used to ship `path` instead of `rules`.
    ?assertEqual(
        {ok, #{db_errors => #{}, config_errors => #{}}},
        ?ON(Core1, emqx_mgmt_data_backup:import_local(AbsPath))
    ),
    %% Each node must have written its own copy of acl.conf locally with
    %% the imported `rules` content. (The stored config's `path` field is
    %% legitimately node-local because each node resolves the file under
    %% its own `data_dir`; we don't assert raw-config equality across nodes.)
    [
        ?assertMatch(
            {ok, BackupRules},
            ?ON(N, file:read_file(emqx_authz_file:acl_conf_file()))
        )
     || N <- [Core1, Core2]
    ],
    %% cluster_rpc must be fully synced (both nodes at the same tnx_id) and
    %% the cluster_rpc_apply_failed alarm must not be active on either node.
    {atomic, StatusOnCore1} = ?ON(Core1, emqx_cluster_rpc:status()),
    TnxIds = lists:usort([T || #{tnx_id := T} <- StatusOnCore1]),
    ?assertMatch([_Single], TnxIds, #{status => StatusOnCore1}),
    [
        ?assertEqual(
            [],
            [
                A
             || A <- ?ON(N, emqx_alarm:get_alarms(activated)),
                cluster_rpc_apply_failed =:= maps:get(name, A, undefined)
            ],
            #{node => N}
        )
     || N <- [Core1, Core2]
    ],
    ok.

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
        emqx_mgmt_data_backup:import(basename(FileName))
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
            {data_backup_core1, #{role => core, apps => apps_to_start(TC)}},
            {data_backup_core2, #{role => core, apps => apps_to_start(TC)}},
            {data_backup_replicant, #{role => replicant, apps => apps_to_start(TC)}}
        ],
        #{
            work_dir => emqx_cth_suite:work_dir(TC, Config),
            start_apps_timeout => 60_000
        }
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
    apps_to_start() ++ [emqx_cluster_link];
apps_to_start(t_cluster_link_password_round_trip) ->
    apps_to_start() ++ [emqx_cluster_link];
apps_to_start(t_cluster_link_multi_round_trip) ->
    apps_to_start() ++ [emqx_cluster_link];
apps_to_start(t_cluster_link_ps_actor_incarnation_preserved) ->
    apps_to_start() ++ [emqx_cluster_link];
apps_to_start(t_export_cloud_subset) ->
    apps_to_start() ++ [emqx_schema_registry, emqx_cluster_link];
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

remove_time_based_fields(RawConf0) ->
    %% These fields are dynamically injected when config updates happen.
    StripBridgeDates =
        fun(TypeToNameAndConf) ->
            maps:map(
                fun(_Type, NameToConf) ->
                    maps:map(
                        fun(_Name, Conf) ->
                            maps:without([<<"created_at">>, <<"last_modified_at">>], Conf)
                        end,
                        NameToConf
                    )
                end,
                TypeToNameAndConf
            )
        end,
    RawConf1 = emqx_utils_maps:update_if_present(<<"actions">>, StripBridgeDates, RawConf0),
    emqx_utils_maps:update_if_present(<<"sources">>, StripBridgeDates, RawConf1).

%% The process of checking the config also converts it, so we remove dynamic (time-based)
%% fields, as well as normalize potentially injected fields during conversion.
normalize_checked_config(RawConf0) ->
    RawConf1 = remove_time_based_fields(RawConf0),
    RawConf2 = emqx_utils_maps:update_if_present(
        <<"retainer">>,
        fun do_normalize_checked_config_retainer/1,
        RawConf1
    ),
    emqx_utils_maps:update_if_present(
        <<"prometheus">>,
        fun do_normalize_checked_config_prometheus/1,
        RawConf2
    ).

do_normalize_checked_config_retainer(Cfg) ->
    %% Retainer injects flow control based on `delivery_rate`.
    maps:remove(<<"flow_control">>, Cfg).

do_normalize_checked_config_prometheus(Cfg0) ->
    %% Apparently, these fields are added/removed when converting
    Cfg1 = maps:remove(<<"latency_buckets">>, Cfg0),
    emqx_utils_maps:deep_remove([<<"push_gateway">>, <<"method">>], Cfg1).

basename(FilePath) ->
    filename:basename(FilePath).

deep_diff_maps(#{} = ExpectedMap, #{} = ActualMap) ->
    Diff0 = emqx_utils_maps:diff_maps(ExpectedMap, ActualMap),
    Diff = maps:remove(identical, Diff0),
    maps:update_with(
        changed,
        fun(#{} = InnerDiffs) ->
            maps:map(
                fun
                    (_K, {#{} = XActual, #{} = XExpected}) ->
                        deep_diff_maps(XExpected, XActual);
                    (_K, Tuple) ->
                        Tuple
                end,
                InnerDiffs
            )
        end,
        Diff
    ).
