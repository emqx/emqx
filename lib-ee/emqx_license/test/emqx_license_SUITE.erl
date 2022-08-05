%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_config:save_schema_mod_and_names(emqx_license_schema),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    ok.

init_per_testcase(Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    set_invalid_license_file(Case),
    Paths = set_override_paths(Case),
    Config0 = setup_test(Case, Config),
    Paths ++ Config0 ++ Config.

end_per_testcase(Case, Config) ->
    restore_valid_license_file(Case),
    clean_overrides(Case, Config),
    teardown_test(Case, Config),
    ok.

set_override_paths(TestCase) when
    TestCase =:= t_change_from_file_to_key;
    TestCase =:= t_change_from_key_to_file
->
    LocalOverridePath = filename:join([
        "/tmp",
        "local-" ++ atom_to_list(TestCase) ++ ".conf"
    ]),
    ClusterOverridePath = filename:join([
        "/tmp",
        "local-" ++ atom_to_list(TestCase) ++ ".conf"
    ]),
    application:set_env(emqx, local_override_conf_file, LocalOverridePath),
    application:set_env(emqx, cluster_override_conf_file, ClusterOverridePath),
    [
        {local_override_path, LocalOverridePath},
        {cluster_override_path, ClusterOverridePath}
    ];
set_override_paths(_TestCase) ->
    [].

clean_overrides(TestCase, Config) when
    TestCase =:= t_change_from_file_to_key;
    TestCase =:= t_change_from_key_to_file
->
    LocalOverridePath = ?config(local_override_path, Config),
    ClusterOverridePath = ?config(cluster_override_path, Config),
    file:delete(LocalOverridePath),
    file:delete(ClusterOverridePath),
    application:unset_env(emqx, local_override_conf_file),
    application:unset_env(emqx, cluster_override_conf_file),
    ok;
clean_overrides(_TestCase, _Config) ->
    ok.

setup_test(TestCase, Config) when
    TestCase =:= t_update_file_cluster_backup
->
    DataDir = ?config(data_dir, Config),
    {LicenseKey, _License} = mk_license(
        [
            %% license format version
            "220111",
            %% license type
            "0",
            %% customer type
            "10",
            %% customer name
            "Foo",
            %% customer email
            "contact@foo.com",
            %% deplayment name
            "bar-deployment",
            %% start date
            "20220111",
            %% days
            "100000",
            %% max connections
            "19"
        ]
    ),
    Cluster = emqx_common_test_helpers:emqx_cluster(
        [core, core],
        [
            {apps, [emqx_conf, emqx_license]},
            {load_schema, false},
            {schema_mod, emqx_enterprise_conf_schema},
            {env_handler, fun
                (emqx) ->
                    emqx_config:save_schema_mod_and_names(emqx_enterprise_conf_schema),
                    %% emqx_config:save_schema_mod_and_names(emqx_license_schema),
                    application:set_env(emqx, boot_modules, []),
                    application:set_env(
                        emqx,
                        data_dir,
                        filename:join([
                            DataDir,
                            TestCase,
                            node()
                        ])
                    ),
                    ok;
                (emqx_conf) ->
                    emqx_config:save_schema_mod_and_names(emqx_enterprise_conf_schema),
                    %% emqx_config:save_schema_mod_and_names(emqx_license_schema),
                    application:set_env(
                        emqx,
                        data_dir,
                        filename:join([
                            DataDir,
                            TestCase,
                            node()
                        ])
                    ),
                    ok;
                (emqx_license) ->
                    LicensePath = filename:join(emqx_license:license_dir(), "emqx.lic"),
                    filelib:ensure_dir(LicensePath),
                    ok = file:write_file(LicensePath, LicenseKey),
                    LicConfig = #{type => file, file => LicensePath},
                    emqx_config:put([license], LicConfig),
                    RawConfig = #{<<"type">> => file, <<"file">> => LicensePath},
                    emqx_config:put_raw([<<"license">>], RawConfig),
                    ok = persistent_term:put(
                        emqx_license_test_pubkey,
                        emqx_license_test_lib:public_key_pem()
                    ),
                    ok;
                (_) ->
                    ok
            end}
        ]
    ),
    Nodes = [emqx_common_test_helpers:start_slave(Name, Opts) || {Name, Opts} <- Cluster],
    [{nodes, Nodes}, {cluster, Cluster}, {old_license, LicenseKey}];
setup_test(_TestCase, _Config) ->
    [].

teardown_test(TestCase, Config) when
    TestCase =:= t_update_file_cluster_backup
->
    Nodes = ?config(nodes, Config),
    lists:foreach(
        fun(N) ->
            LicenseDir = erpc:call(N, emqx_license, license_dir, []),
            {ok, _} = emqx_common_test_helpers:stop_slave(N),
            ok = file:del_dir_r(LicenseDir),
            ok
        end,
        Nodes
    ),
    ok;
teardown_test(_TestCase, _Config) ->
    ok.

set_invalid_license_file(t_read_license_from_invalid_file) ->
    Config = #{type => file, file => "/invalid/file"},
    emqx_config:put([license], Config);
set_invalid_license_file(_) ->
    ok.

restore_valid_license_file(t_read_license_from_invalid_file) ->
    Config = #{type => file, file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config);
restore_valid_license_file(_) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{type => file, file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config),
    RawConfig = #{<<"type">> => file, <<"file">> => emqx_license_test_lib:default_license()},
    emqx_config:put_raw([<<"license">>], RawConfig);
set_special_configs(_) ->
    ok.

assert_on_nodes(Nodes, RunFun, CheckFun) ->
    Res = [{N, erpc:call(N, RunFun)} || N <- Nodes],
    lists:foreach(CheckFun, Res).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_update_file(_Config) ->
    ?assertMatch(
        {error, enoent},
        emqx_license:update_file("/unknown/path")
    ),

    ok = file:write_file("license_with_invalid_content.lic", <<"bad license">>),
    ?assertMatch(
        {error, [_ | _]},
        emqx_license:update_file("license_with_invalid_content.lic")
    ),

    ?assertMatch(
        {ok, #{}},
        emqx_license:update_file(emqx_license_test_lib:default_license())
    ).

t_update_file_cluster_backup(Config) ->
    OldLicenseKey = ?config(old_license, Config),
    Nodes = [N1 | _] = ?config(nodes, Config),

    %% update the license file for the cluster
    {NewLicenseKey, NewDecodedLicense} = mk_license(
        [
            %% license format version
            "220111",
            %% license type
            "0",
            %% customer type
            "10",
            %% customer name
            "Foo",
            %% customer email
            "contact@foo.com",
            %% deplayment name
            "bar-deployment",
            %% start date
            "20220111",
            %% days
            "100000",
            %% max connections
            "190"
        ]
    ),
    NewLicensePath = "tmp_new_license.lic",
    ok = file:write_file(NewLicensePath, NewLicenseKey),
    {ok, _} = erpc:call(N1, emqx_license, update_file, [NewLicensePath]),

    assert_on_nodes(
        Nodes,
        fun() ->
            Conf = emqx_conf:get([license]),
            emqx_license:read_license(Conf)
        end,
        fun({N, Res}) ->
            ?assertMatch({ok, _}, Res, #{node => N}),
            {ok, License} = Res,
            ?assertEqual(NewDecodedLicense, License, #{node => N})
        end
    ),

    assert_on_nodes(
        Nodes,
        fun() ->
            LicenseDir = emqx_license:license_dir(),
            file:list_dir(LicenseDir)
        end,
        fun({N, Res}) ->
            ?assertMatch({ok, _}, Res, #{node => N}),
            {ok, DirContents} = Res,
            %% the now current license
            ?assert(lists:member("emqx.lic", DirContents), #{node => N, dir_contents => DirContents}),
            %% the backed up old license
            ?assert(
                lists:any(
                    fun
                        ("emqx.lic." ++ Suffix) -> lists:suffix(".backup", Suffix);
                        (_) -> false
                    end,
                    DirContents
                ),
                #{node => N, dir_contents => DirContents}
            )
        end
    ),

    assert_on_nodes(
        Nodes,
        fun() ->
            LicenseDir = emqx_license:license_dir(),
            {ok, DirContents} = file:list_dir(LicenseDir),
            [BackupLicensePath0] = [
                F
             || "emqx.lic." ++ F <- DirContents, lists:suffix(".backup", F)
            ],
            BackupLicensePath = "emqx.lic." ++ BackupLicensePath0,
            {ok, BackupLicense} = file:read_file(filename:join(LicenseDir, BackupLicensePath)),
            {ok, NewLicense} = file:read_file(filename:join(LicenseDir, "emqx.lic")),
            #{
                backup => BackupLicense,
                new => NewLicense
            }
        end,
        fun({N, #{backup := BackupLicense, new := NewLicense}}) ->
            ?assertEqual(OldLicenseKey, BackupLicense, #{node => N}),
            ?assertEqual(NewLicenseKey, NewLicense, #{node => N})
        end
    ),

    %% uploading the same license twice should not generate extra backups.
    {ok, _} = erpc:call(N1, emqx_license, update_file, [NewLicensePath]),

    assert_on_nodes(
        Nodes,
        fun() ->
            LicenseDir = emqx_license:license_dir(),
            {ok, DirContents} = file:list_dir(LicenseDir),
            [F || "emqx.lic." ++ F <- DirContents, lists:suffix(".backup", F)]
        end,
        fun({N, Backups}) ->
            ?assertMatch([_], Backups, #{node => N})
        end
    ),

    ok.

t_update_value(_Config) ->
    ?assertMatch(
        {error, [_ | _]},
        emqx_license:update_key("invalid.license")
    ),

    {ok, LicenseValue} = file:read_file(emqx_license_test_lib:default_license()),

    ?assertMatch(
        {ok, #{}},
        emqx_license:update_key(LicenseValue)
    ).

t_read_license_from_invalid_file(_Config) ->
    ?assertMatch(
        {error, enoent},
        emqx_license:read_license()
    ).

t_check_exceeded(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ok = lists:foreach(
        fun(_) ->
            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C)
        end,
        lists:seq(1, 12)
    ),

    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_check_ok(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            "0",
            "10",
            "Foo",
            "contact@foo.com",
            "bar",
            "20220111",
            "100000",
            "10"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ok = lists:foreach(
        fun(_) ->
            {ok, C} = emqtt:start_link(),
            {ok, _} = emqtt:connect(C)
        end,
        lists:seq(1, 11)
    ),

    ?assertEqual(
        {ok, #{}},
        emqx_license:check(#{}, #{})
    ).

t_check_expired(_Config) ->
    {_, License} = mk_license(
        [
            "220111",
            %% Official customer
            "1",
            %% Small customer
            "0",
            "Foo",
            "contact@foo.com",
            "bar",
            %% Expired long ago
            "20211101",
            "10",
            "10"
        ]
    ),
    #{} = emqx_license_checker:update(License),

    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_check_not_loaded(_Config) ->
    ok = emqx_license_checker:purge(),
    ?assertEqual(
        {stop, {error, ?RC_QUOTA_EXCEEDED}},
        emqx_license:check(#{}, #{})
    ).

t_change_from_file_to_key(_Config) ->
    %% precondition
    ?assertMatch(#{file := _}, emqx_conf:get([license])),

    OldConf = emqx_conf:get_raw([]),

    %% this saves updated config to `{cluster,local}-overrrides.conf'
    {ok, LicenseValue} = file:read_file(emqx_license_test_lib:default_license()),
    {ok, _NewConf} = emqx_license:update_key(LicenseValue),

    %% assert that `{cluster,local}-overrides.conf' merge correctly
    ?assertEqual(ok, emqx_config:init_load(emqx_license_schema, OldConf, #{})),

    ok.

t_change_from_key_to_file(_Config) ->
    Config = #{type => key, key => <<"some key">>},
    emqx_config:put([license], Config),
    RawConfig = #{<<"type">> => key, <<"key">> => <<"some key">>},
    emqx_config:put_raw([<<"license">>], RawConfig),

    %% precondition
    ?assertMatch(#{type := key, key := _}, emqx_conf:get([license])),
    OldConf = emqx_conf:get_raw([]),

    %% this saves updated config to `{cluster,local}-overrrides.conf'
    {ok, _NewConf} = emqx_license:update_file(emqx_license_test_lib:default_license()),

    %% assert that `{cluster,local}-overrides.conf' merge correctly
    ?assertEqual(ok, emqx_config:init_load(emqx_license_schema, OldConf, #{})),

    ok.

%%------------------------------------------------------------------------------
%% Helpers
%%------------------------------------------------------------------------------

mk_license(Fields) ->
    EncodedLicense = emqx_license_test_lib:make_license(Fields),
    {ok, License} = emqx_license_parser:parse(
        EncodedLicense,
        emqx_license_test_lib:public_key_pem()
    ),
    {EncodedLicense, License}.
