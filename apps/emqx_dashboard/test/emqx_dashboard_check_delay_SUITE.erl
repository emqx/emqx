%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_check_delay_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include("emqx_dashboard.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").

-define(DELAY, emqx_dashboard_check_delay).
-define(USERNAME, <<"delay_admin">>).
-define(PASSWORD, <<"delayP@ss1">>).
-define(WRONG_PASSWORD, <<"wrongP@ss1">>).
-define(SAMPLE_US, 50_000).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(apps, Config)).

init_per_testcase(_Case, Config) ->
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_login_lock:cleanup_all(),
    {ok, _} = emqx_dashboard_admin:add_user(
        ?USERNAME, ?PASSWORD, ?ROLE_SUPERUSER, <<"check delay test user">>
    ),
    ok = ?DELAY:clear_sample(),
    ok = meck:new(?DELAY, [passthrough]),
    Config.

end_per_testcase(_Case, _Config) ->
    ok = meck:unload(?DELAY),
    ok = ?DELAY:clear_sample().

t_check_delay_for_missing_user(_) ->
    ok = set_fresh_sample(?SAMPLE_US),
    ?assertEqual(
        {error, <<"username_not_found">>},
        emqx_dashboard_admin:check(<<"no_such_user">>, ?PASSWORD)
    ),
    [DelayUs] = delays(),
    ?assert(DelayUs >= ?SAMPLE_US),
    ?assert(DelayUs =< ?SAMPLE_US + max_extra_us(?SAMPLE_US)).

t_check_delay_after_password_check(_) ->
    ok = set_fresh_sample(?SAMPLE_US),
    ?assertEqual(
        {error, <<"password_error">>},
        emqx_dashboard_admin:check(?USERNAME, ?WRONG_PASSWORD)
    ),
    ?assertMatch({ok, _}, emqx_dashboard_admin:check(?USERNAME, ?PASSWORD)),
    Delays = delays(),
    ?assertEqual(2, length(Delays)),
    lists:foreach(
        fun(DelayUs) -> ?assert(DelayUs =< max_extra_us(?SAMPLE_US)) end,
        Delays
    ).

t_check_delay_sample_refresh(_) ->
    ?assertEqual(undefined, ?DELAY:sample()),
    {error, _} = emqx_dashboard_admin:check(?USERNAME, ?WRONG_PASSWORD),
    {SampleUs1, TakenAt1} = Sample1 = ?DELAY:sample(),
    ?assert(SampleUs1 > 0),
    %% A fresh sample is kept.
    {error, _} = emqx_dashboard_admin:check(?USERNAME, ?WRONG_PASSWORD),
    {ok, _} = emqx_dashboard_admin:check(?USERNAME, ?PASSWORD),
    ?assertEqual(Sample1, ?DELAY:sample()),
    %% A sample older than the maximum age is replaced after the next check.
    OldTakenAt = TakenAt1 - ?DELAY:max_age_ms() - 1,
    ok = ?DELAY:set_sample(SampleUs1, OldTakenAt),
    {error, _} = emqx_dashboard_admin:check(?USERNAME, ?WRONG_PASSWORD),
    {_SampleUs2, TakenAt2} = ?DELAY:sample(),
    ?assert(TakenAt2 > OldTakenAt).

t_check_delay_sample_excludes_delay(_) ->
    %% Make every delay a fixed 20 ms, then refresh the sample a few times.
    %% The sample plus the delay must fit in the time of the whole check.
    FixedDelayMs = 20,
    ok = meck:expect(?DELAY, sleep_us, fun(_) -> timer:sleep(FixedDelayMs) end),
    lists:foreach(
        fun(_) ->
            ok = ?DELAY:set_sample(?SAMPLE_US, stale_taken_at()),
            T0 = erlang:monotonic_time(microsecond),
            {error, _} = emqx_dashboard_admin:check(?USERNAME, ?WRONG_PASSWORD),
            ElapsedUs = erlang:monotonic_time(microsecond) - T0,
            {SampleUs, _} = ?DELAY:sample(),
            ct:pal("sample_us=~p elapsed_us=~p", [SampleUs, ElapsedUs]),
            ?assert(SampleUs + FixedDelayMs * 1000 =< ElapsedUs)
        end,
        lists:seq(1, 3)
    ).

t_check_delay_without_sample(_) ->
    ?assertEqual(undefined, ?DELAY:sample()),
    T0 = erlang:monotonic_time(microsecond),
    {error, _} = emqx_dashboard_admin:check(<<"no_such_user">>, ?PASSWORD),
    ElapsedUs = erlang:monotonic_time(microsecond) - T0,
    {SampleUs, _} = ?DELAY:sample(),
    ?assert(SampleUs > 0),
    %% The first request runs one hash to take the sample, then adds the
    %% random extra delay.
    ?assert(ElapsedUs >= SampleUs),
    [DelayUs] = delays(),
    ?assert(DelayUs =< max_extra_us(SampleUs)).

t_check_delay_for_legacy_hash(_) ->
    {atomic, ok} = mria:sync_transaction(?DASHBOARD_SHARD, fun() ->
        [User] = mnesia:wread({?ADMIN, ?USERNAME}),
        mnesia:write(User#?ADMIN{pwdhash = <<"abcd", 0:256>>})
    end),
    %% A stale sample: a measured check would replace it.
    TakenAt = stale_taken_at(),
    ok = ?DELAY:set_sample(?SAMPLE_US, TakenAt),
    ?assertEqual(
        {error, <<"password_error">>},
        emqx_dashboard_admin:check(?USERNAME, ?PASSWORD)
    ),
    [DelayUs] = delays(),
    ?assert(DelayUs >= ?SAMPLE_US),
    ?assert(DelayUs =< ?SAMPLE_US + max_extra_us(?SAMPLE_US)),
    ?assertEqual({?SAMPLE_US, TakenAt}, ?DELAY:sample()).

t_api_secret_check_no_delay(_) ->
    TakenAt = stale_taken_at(),
    ok = ?DELAY:set_sample(?SAMPLE_US, TakenAt),
    ok = meck:reset(?DELAY),
    Secret = <<"api-secret-for-delay-test">>,
    Hash = emqx_dashboard_admin:hash_api_secret(Secret),
    ?assertEqual(ok, emqx_dashboard_admin:verify_hash(Secret, Hash)),
    ?assertEqual(error, emqx_dashboard_admin:verify_hash(<<"other">>, Hash)),
    ?assertEqual([], meck:history(?DELAY)),
    ?assertEqual({?SAMPLE_US, TakenAt}, ?DELAY:sample()).

t_check_delay_sample_clamped(_) ->
    MaxUs = ?DELAY:max_sample_us(),
    ok = ?DELAY:record_sample(MaxUs * 10),
    ?assertMatch({MaxUs, _}, ?DELAY:sample()),
    ok = ?DELAY:clear_sample(),
    ok = ?DELAY:measure(fun() -> timer:sleep(MaxUs div 1000 + 100) end),
    ?assertMatch({MaxUs, _}, ?DELAY:sample()).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

set_fresh_sample(SampleUs) ->
    ?DELAY:set_sample(SampleUs, erlang:monotonic_time(millisecond)).

stale_taken_at() ->
    erlang:monotonic_time(millisecond) - ?DELAY:max_age_ms() - 1.

max_extra_us(SampleUs) ->
    SampleUs * ?DELAY:extra_percent() div 100.

%% The delays requested through `sleep_us/1', in call order.
delays() ->
    [DelayUs || {_Pid, {?DELAY, sleep_us, [DelayUs]}, _Result} <- meck:history(?DELAY)].
