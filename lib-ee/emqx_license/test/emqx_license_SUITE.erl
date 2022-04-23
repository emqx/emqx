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
    Config.

end_per_testcase(Case, _Config) ->
    restore_valid_license_file(Case),
    ok.

set_invalid_license_file(t_read_license_from_invalid_file) ->
    Config = #{file => "/invalid/file"},
    emqx_config:put([license], Config);
set_invalid_license_file(_) ->
    ok.

restore_valid_license_file(t_read_license_from_invalid_file) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config);
restore_valid_license_file(_) ->
    ok.

set_special_configs(emqx_license) ->
    Config = #{file => emqx_license_test_lib:default_license()},
    emqx_config:put([license], Config),
    RawConfig = #{<<"file">> => emqx_license_test_lib:default_license()},
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
