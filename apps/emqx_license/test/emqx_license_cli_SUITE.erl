%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-doc "Print usage when no subcommand is given.".
t_help({init, Config}) ->
    Config;
t_help({'end', _Config}) ->
    ok;
t_help(_Config) ->
    _ = emqx_license_cli:license([]).

-doc "Show license info.".
t_info({init, Config}) ->
    Config;
t_info({'end', _Config}) ->
    ok;
t_info(_Config) ->
    _ = emqx_license_cli:license(["info"]).

-doc "History command shows monthly plain-text and JSON output.".
t_history({init, Config}) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm),
    %% Use explicit UTC to avoid host-timezone dependence.
    emqx_config:put([license, high_watermark_timezone], <<"+00:00">>),
    ok = emqx_license_session_hwm:observe(1_776_520_385_000, 25),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:observe(1_778_889_600_000, 18),
    ok = emqx_license_session_hwm:sync(),
    Config;
t_history({'end', _Config}) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm);
t_history(_Config) ->
    {ok, Out} = ?CAPTURE(emqx_license_cli:license(["history"])),
    OutBin = iolist_to_binary(Out),
    ?assertMatch(
        {match, _},
        re:run(OutBin, <<"period=2026-05 high_watermark=18 observed_at=\\S+">>)
    ),
    {ok, JsonOut} = ?CAPTURE(emqx_license_cli:license(["history", "--json"])),
    [Json] = JsonOut,
    Decoded = emqx_utils_json:decode(Json),
    ?assertMatch(
        #{
            <<"period">> := <<"monthly">>,
            <<"count">> := 2,
            <<"data">> := [#{<<"period">> := <<"2026-05">>} | _]
        },
        Decoded
    ).

-doc "Empty history prints a clear message in text mode and empty array in JSON.".
t_history_empty({init, Config}) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm),
    Config;
t_history_empty({'end', _Config}) ->
    ok;
t_history_empty(_Config) ->
    {ok, Out} = ?CAPTURE(emqx_license_cli:license(["history"])),
    ?assertMatch([<<"No session high-watermark history recorded.\n">>], Out),
    {ok, JsonOut} = ?CAPTURE(emqx_license_cli:license(["history", "--json"])),
    [Json] = JsonOut,
    Decoded = emqx_utils_json:decode(Json),
    ?assertMatch(#{<<"count">> := 0, <<"data">> := []}, Decoded).

-doc "Daily mode output shows daily period labels.".
t_history_daily({init, Config}) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm),
    emqx_config:put([license, high_watermark_timezone], <<"+00:00">>),
    ok = emqx_license_session_hwm:observe(1_776_520_385_000, 25),
    ok = emqx_license_session_hwm:sync(),
    Config;
t_history_daily({'end', _Config}) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm);
t_history_daily(_Config) ->
    {ok, Out} = ?CAPTURE(emqx_license_cli:license(["history", "--period", "daily"])),
    OutBin = iolist_to_binary(Out),
    ?assertMatch(
        {match, _},
        re:run(OutBin, <<"period=2026-04-18 high_watermark=25 observed_at=\\S+">>)
    ).

-doc "Invalid arguments to history command produce error messages.".
t_history_bad_args({init, Config}) ->
    Config;
t_history_bad_args({'end', _Config}) ->
    ok;
t_history_bad_args(_Config) ->
    {ok, Out1} = ?CAPTURE(emqx_license_cli:license(["history", "--period", "yearly"])),
    ?assertMatch([<<"Error: bad_history_period\n">> | _], Out1),
    {ok, Out2} = ?CAPTURE(emqx_license_cli:license(["history", "0"])),
    ?assertMatch([<<"Error: bad_history_argument\n">> | _], Out2).

-doc "Update license via CLI.".
t_update({init, Config}) ->
    Config;
t_update({'end', _Config}) ->
    ok;
t_update(_Config) ->
    LicenseValue = emqx_license_test_lib:default_license(),
    _ = emqx_license_cli:license(["update", LicenseValue]),
    _ = emqx_license_cli:license(["reload"]),
    _ = emqx_license_cli:license(["update", "Invalid License Value"]).

-doc "Reject single-node license keys in multi-node cluster.".
t_update_with_invalid_license_value({init, Config}) ->
    _ = emqx_license_cli:license(["update", "evaluation"]),
    meck:new(emqx, [passthrough, no_history]),
    meck:expect(emqx, cluster_nodes, fun(running) -> [node(), node()] end),
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
    Err = <<"Error: SINGLE_NODE_LICENSE\n">>,
    ?assertEqual({ok, [Err]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key1)]))),
    ?assertEqual({ok, [Err]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key2)]))),
    ?assertEqual(
        {ok, [<<"ok\n">>]}, ?CAPTURE(emqx_license_cli:license(["update", binary_to_list(Key3)]))
    ),
    ok.

-doc "Config update via emqx:update_config persists license settings.".
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
            high_watermark_timezone => system,
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
