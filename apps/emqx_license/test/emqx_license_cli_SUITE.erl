%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => emqx_license_test_lib:default_license()}}
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    emqx_license_test_lib:mock_parser(),
    Config.

end_per_testcase(_Case, _Config) ->
    emqx_license_test_lib:unmock_parser(),
    ok.

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
            dynamic_max_connections => 25,
            key => LicenseKey
        },
        emqx:get_config([license])
    ),
    ?assertMatch(
        #{max_connections := 123},
        maps:from_list(emqx_license_checker:dump())
    ),
    ok.
