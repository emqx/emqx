%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_cli_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx_license.hrl").

-define(CAPTURE(Expr), emqx_common_test_helpers:capture_io_format(fun() -> Expr end)).

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

init_per_testcase(Case, Config) ->
    emqx_license_test_lib:mock_parser(),
    ?MODULE:Case({init, Config}).

end_per_testcase(Case, Config) ->
    ?MODULE:Case({'end', Config}),
    emqx_license_test_lib:unmock_parser(),
    ok.

%%------------------------------------------------------------------------------
%% Tests
%%------------------------------------------------------------------------------

t_help({init, Config}) ->
    Config;
t_help({'end', _Config}) ->
    ok;
t_help(_Config) ->
    _ = emqx_license_cli:license([]).

t_info({init, Config}) ->
    Config;
t_info({'end', _Config}) ->
    ok;
t_info(_Config) ->
    _ = emqx_license_cli:license(["info"]).

t_update({init, Config}) ->
    Config;
t_update({'end', _Config}) ->
    ok;
t_update(_Config) ->
    LicenseValue = emqx_license_test_lib:default_license(),
    _ = emqx_license_cli:license(["update", LicenseValue]),
    _ = emqx_license_cli:license(["reload"]),
    _ = emqx_license_cli:license(["update", "Invalid License Value"]).

t_update_with_invalid_license_value({init, Config}) ->
    _ = emqx_license_cli:license(["update", "evaluation"]),
    meck:new(emqx, [passthrough, no_history]),
    meck:expect(emqx, cluster_nodes, fun(all) -> [node(), node()] end),
    Config;
t_update_with_invalid_license_value({'end', _Config}) ->
    meck:unload(emqx),
    ok;
t_update_with_invalid_license_value(_Config) ->
    %% do not allow setting to "default" license key or any community license key
    Key1 = <<"default">>,
    C = integer_to_list(?COMMUNITY),
    T = integer_to_list(?TRIAL),
    Key2 = emqx_license_test_lib:make_license(#{max_sessions => "100", license_type => C}),
    Key3 = emqx_license_test_lib:make_license(#{max_sessions => "100", license_type => T}),
    Err = <<"Error: single_node_license_not_allowed_when_clusterd\n">>,
    ?assertEqual({ok, [Err]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key1)]))),
    ?assertEqual({ok, [Err]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key2)]))),
    ?assertEqual(
        {ok, [<<"ok\n">>]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key3)]))
    ),
    ok.

t_conf_update({init, Config}) ->
    Config;
t_conf_update({'end', _Config}) ->
    ok;
t_conf_update(_Config) ->
    LicenseKey = emqx_license_test_lib:make_license(#{max_sessions => "123"}),
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
            dynamic_max_connections => ?DEFAULT_MAX_SESSIONS_CTYPE3,
            key => LicenseKey
        },
        emqx:get_config([license])
    ),
    Dump = maps:from_list(emqx_license_checker:dump()),
    ?assertMatch(
        #{
            max_sessions := 123
        },
        Dump
    ),
    ok.
