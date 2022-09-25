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
    Paths = set_override_paths(Case),
    Config0 = setup_test(Case, Config),
    Paths ++ Config0 ++ Config.

end_per_testcase(Case, Config) ->
    clean_overrides(Case, Config),
    teardown_test(Case, Config),
    ok.

set_override_paths(_TestCase) ->
    [].

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
            {schema_mod, emqx_ee_conf_schema},
            {env_handler, fun
                (emqx) ->
                    emqx_config:save_schema_mod_and_names(emqx_ee_conf_schema),
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
                    emqx_config:save_schema_mod_and_names(emqx_ee_conf_schema),
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

teardown_test(_TestCase, _Config) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{key => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config),
    RawConfig = #{<<"key">> => emqx_license_test_lib:default_license()},
    emqx_config:put_raw([<<"license">>], RawConfig);
set_special_configs(_) ->
    ok.

assert_on_nodes(Nodes, RunFun, CheckFun) ->
    Res = [{N, erpc:call(N, RunFun)} || N <- Nodes],
    lists:foreach(CheckFun, Res).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_update_value(_Config) ->
    ?assertMatch(
        {error, [_ | _]},
        emqx_license:update_key("invalid.license")
    ),

    LicenseValue = emqx_license_test_lib:default_license(),

    ?assertMatch(
        {ok, #{}},
        emqx_license:update_key(LicenseValue)
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
