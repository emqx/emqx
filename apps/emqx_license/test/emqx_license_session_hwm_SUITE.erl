%%--------------------------------------------------------------------
%% Copyright (c) 2022-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_session_hwm_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include("emqx_license.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_conf,
            {emqx_license, #{
                config => #{license => #{key => ?DEFAULT_EVALUATION_LICENSE_KEY}}
            }}
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, Apps} | Config].

end_per_suite(Config) ->
    ok = emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(_Case, Config) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm),
    %% Use an explicit timezone in all tests to avoid host-timezone dependence.
    emqx_config:put([license, high_watermark_timezone], <<"+00:00">>),
    Config.

end_per_testcase(_Case, _Config) ->
    {atomic, ok} = mria:clear_table(emqx_license_session_hwm).

-doc "A single day produces one row; lower observations do not reduce the peak.".
t_daily_high_watermark(_Config) ->
    %% 1_776_520_385_000 = 2026-04-18T13:53:05Z
    Ts = 1_776_520_385_000,
    ok = emqx_license_session_hwm:observe(Ts, 10),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:observe(Ts + 10, 9),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:observe(Ts + 20, 12),
    ok = emqx_license_session_hwm:sync(),
    [#{period := <<"2026-04-18">>, high_watermark := 12}] =
        emqx_license_session_hwm:list_history(daily, 10).

-doc "Day rollover produces separate rows for each day.".
t_day_rollover(_Config) ->
    %% 2026-04-18T19:00:00Z
    Day1 = 1_776_538_800_000,
    %% 2026-04-19T00:00:00Z (next day)
    Day2 = 1_776_556_800_000,
    ok = emqx_license_session_hwm:observe(Day1, 10),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:observe(Day2, 5),
    ok = emqx_license_session_hwm:sync(),
    [
        #{period := <<"2026-04-19">>, high_watermark := 5},
        #{period := <<"2026-04-18">>, high_watermark := 10}
    ] = emqx_license_session_hwm:list_history(daily, 10).

-doc "Monthly query folds multiple daily rows into month-level peaks.".
t_monthly_fold(_Config) ->
    %% 2026-04-07T04:00:00Z
    ok = emqx_license_session_hwm:observe(1_775_534_400_000, 10),
    ok = emqx_license_session_hwm:sync(),
    %% 2026-04-18T04:00:00Z
    ok = emqx_license_session_hwm:observe(1_776_484_800_000, 25),
    ok = emqx_license_session_hwm:sync(),
    %% 2026-05-16T00:00:00Z (different month)
    ok = emqx_license_session_hwm:observe(1_778_889_600_000, 18),
    ok = emqx_license_session_hwm:sync(),
    [
        #{period := <<"2026-05">>, high_watermark := 18},
        #{period := <<"2026-04">>, high_watermark := 25}
    ] =
        emqx_license_session_hwm:list_history(monthly, 10).

-doc "Monthly fold tie-break: when two days have the same peak, later observed_at wins.".
t_monthly_fold_tiebreak(_Config) ->
    %% Two days in the same month with the same peak.
    %% 2025-04-10T12:00:00Z
    Day1 = 1_744_286_400_000,
    %% 2025-04-15T12:00:00Z
    Day2 = 1_744_718_400_000,
    ok = emqx_license_session_hwm:observe(Day1, 100),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:observe(Day2, 100),
    ok = emqx_license_session_hwm:sync(),
    [#{period := <<"2025-04">>, high_watermark := 100, observed_at := ObservedAt}] =
        emqx_license_session_hwm:list_history(monthly, 10),
    %% The later observation should win the tie-break.
    ?assertNotEqual(nomatch, binary:match(ObservedAt, <<"2025-04-15">>)).

-doc "A +02:00 timezone shifts a late-UTC timestamp into the next day.".
t_timezone_daily_boundary(_Config) ->
    emqx_config:put([license, high_watermark_timezone], <<"+02:00">>),
    %% 2026-04-18T22:30:00Z => 2026-04-19 in +02:00
    ok = emqx_license_session_hwm:observe(1_776_551_400_000, 20),
    ok = emqx_license_session_hwm:sync(),
    [#{period := <<"2026-04-19">>, high_watermark := 20}] =
        emqx_license_session_hwm:list_history(daily, 10).

-doc "GC removes rows older than 24 months while retaining recent ones.".
t_gc_retains_recent_months(_Config) ->
    %% 2024-03-30T00:00:00Z — should be GC'd when current month is 2026-04.
    ok = emqx_license_session_hwm:observe(1_711_756_800_000, 1),
    ok = emqx_license_session_hwm:sync(),
    %% 2025-05-18T01:46:40Z
    ok = emqx_license_session_hwm:observe(1_747_532_800_000, 2),
    ok = emqx_license_session_hwm:sync(),
    %% 2026-04-18T00:00:00Z
    ok = emqx_license_session_hwm:observe(1_776_470_400_000, 3),
    ok = emqx_license_session_hwm:sync(),
    ok = emqx_license_session_hwm:gc(),
    Rows = emqx_license_session_hwm:list_history(daily, 10),
    ?assertEqual([<<"2026-04-18">>, <<"2025-05-18">>], [maps:get(period, R) || R <- Rows]).

-doc "Data survives application stop/start because the table uses disc_copies.".
t_persist_after_restart(_Config) ->
    %% 1_776_520_385_000 = 2026-04-18T13:53:05Z
    Ts = 1_776_520_385_000,
    ok = emqx_license_session_hwm:observe(Ts, 15),
    ok = emqx_license_session_hwm:sync(),
    ok = application:stop(emqx_license),
    ok = application:start(emqx_license),
    [#{period := <<"2026-04-18">>, high_watermark := 15}] =
        emqx_license_session_hwm:list_history(daily, 10).

-doc "list_history returns empty list when no data exists.".
t_empty_history(_Config) ->
    ?assertEqual([], emqx_license_session_hwm:list_history(daily, 10)),
    ?assertEqual([], emqx_license_session_hwm:list_history(monthly, 10)).

-doc "list_history limit parameter caps the number of returned rows.".
t_list_history_limit(_Config) ->
    %% Insert 3 days of data.
    %% 2026-04-18T00:00:00Z
    ok = emqx_license_session_hwm:observe(1_776_470_400_000, 10),
    ok = emqx_license_session_hwm:sync(),
    %% 2026-04-19T00:00:00Z
    ok = emqx_license_session_hwm:observe(1_776_556_800_000, 20),
    ok = emqx_license_session_hwm:sync(),
    %% 2026-04-20T00:00:00Z
    ok = emqx_license_session_hwm:observe(1_776_643_200_000, 30),
    ok = emqx_license_session_hwm:sync(),
    ?assertEqual(3, length(emqx_license_session_hwm:list_history(daily, 10))),
    ?assertEqual(2, length(emqx_license_session_hwm:list_history(daily, 2))),
    ?assertEqual(1, length(emqx_license_session_hwm:list_history(daily, 1))).
