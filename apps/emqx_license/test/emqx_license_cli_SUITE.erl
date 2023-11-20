%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    _ = application:load(emqx_conf),
    emqx_config:save_schema_mod_and_names(emqx_license_schema),
    emqx_common_test_helpers:start_apps([emqx_license], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_common_test_helpers:stop_apps([emqx_license]),
    persistent_term:erase(emqx_license_test_pubkey),
    ok.

set_special_configs(emqx_license) ->
    LicenseKey = default_license(),
    Config = #{
        key => LicenseKey, connection_low_watermark => 0.75, connection_high_watermark => 0.8
    },
    emqx_config:put([license], Config),

    RawConfig = #{
        <<"key">> => LicenseKey,
        <<"connection_low_watermark">> => <<"75%">>,
        <<"connection_high_watermark">> => <<"80%">>
    },
    emqx_config:put_raw([<<"license">>], RawConfig),

    ok = persistent_term:put(
        emqx_license_test_pubkey,
        emqx_license_test_lib:public_key_pem()
    );
set_special_configs(_) ->
    ok.

init_per_testcase(t_unset, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    MeckLicenseValues = #{
        license_format => "220111",
        license_type => "0",
        customer_type => "10",
        name => "Default Meck License",
        email => "contact@meck.com",
        deployment => "meck-deployment",
        start_date => "20220111",
        days => "100000",
        max_connections => "100"
    },
    DefaultMeckLicenseKey = emqx_license_test_lib:make_license(MeckLicenseValues),
    ok = meck:new(emqx_license_schema, [passthrough, no_history]),
    ok = meck:expect(emqx_license_schema, default_license, fun() -> DefaultMeckLicenseKey end),
    [{default_license_key, DefaultMeckLicenseKey} | Config];
init_per_testcase(_Case, Config) ->
    {ok, _} = emqx_cluster_rpc:start_link(node(), emqx_cluster_rpc, 1000),
    Config.

end_per_testcase(t_unset, _Config) ->
    ok = meck:unload(emqx_license_schema),
    {ok, _} = reset_license(),
    ok;
end_per_testcase(_Case, _Config) ->
    {ok, _} = reset_license(),
    ok.

%%------------------------------------------------------------------------------
%% Helper fns
%%------------------------------------------------------------------------------

default_license() ->
    emqx_license_test_lib:make_license(#{max_connections => "100"}).

reset_license() ->
    emqx_license:update_key(default_license()).

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_help(_Config) ->
    _ = emqx_license_cli:license([]).

t_info(_Config) ->
    _ = emqx_license_cli:license(["info"]).

t_update(_Config) ->
    LicenseValue = emqx_license_test_lib:default_license(),
    _ = emqx_license_cli:license(["update", LicenseValue]),
    _ = emqx_license_cli:license(["reload"]),
    _ = emqx_license_cli:license(["update", "Invalid License Value"]).

t_unset(Config) ->
    ?check_trace(
        begin
            _ = emqx_license_cli:license(["unset"]),
            License = maps:get(key, emqx:get_config([license])),
            Default = ?config(default_license_key, Config),
            ?assertEqual(License, Default)
        end,
        fun(Trace) ->
            ?assertMatch(
                [#{operation_method := "cli"}],
                ?of_kind(unset_to_default_evaluation_license_key, Trace)
            )
        end
    ),
    ok.

t_conf_update(_Config) ->
    LicenseKey = emqx_license_test_lib:make_license(#{max_connections => "123"}),
    Conf = #{
        <<"connection_high_watermark">> => <<"50%">>,
        <<"connection_low_watermark">> => <<"45%">>,
        <<"key">> => LicenseKey
    },
    ?assertMatch({ok, _}, emqx:update_config([license], Conf)),
    ?assertEqual(
        #{
            connection_high_watermark => 0.5,
            connection_low_watermark => 0.45,
            key => LicenseKey
        },
        emqx:get_config([license])
    ),
    ?assertMatch(
        #{max_connections := 123},
        maps:from_list(emqx_license_checker:dump())
    ),
    ok.
