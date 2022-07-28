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
    Paths ++ Config.

end_per_testcase(Case, Config) ->
    restore_valid_license_file(Case),
    clean_overrides(Case, Config),
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

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_update_file(_Config) ->
    ?assertMatch(
        {error, {invalid_license_file, enoent}},
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
    License = mk_license(
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
    License = mk_license(
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
    License = mk_license(
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
    License.
