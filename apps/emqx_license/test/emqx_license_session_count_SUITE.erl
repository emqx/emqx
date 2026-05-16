%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_license_session_count_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_license.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    Apps = emqx_cth_suite:start(
        [
            {emqx, #{override_env => [{boot_modules, [broker]}]}},
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
    %% Wipe any callbacks left over (gateway registers one if loaded).
    application:set_env(emqx_license, session_count_callbacks, #{}),
    Config.

end_per_testcase(_Case, _Config) ->
    application:set_env(emqx_license, session_count_callbacks, #{}),
    ok.

%% Callbacks used in the tests
ten() -> 10.
seven() -> 7.
throws() -> throw(something_else).

-doc "sum_callbacks adds every healthy callback's return value.".
t_sum_healthy(_Config) ->
    ok = emqx_license_session_count:register_callback(a, fun ?MODULE:ten/0),
    ok = emqx_license_session_count:register_callback(b, fun ?MODULE:seven/0),
    ?assertEqual(17, emqx_license_session_count:sum_callbacks()).

-doc "Callbacks raising error:undef are silently skipped (feature off case).".
t_skips_undef(_Config) ->
    ok = emqx_license_session_count:register_callback(a, fun ?MODULE:ten/0),
    ok = emqx_license_session_count:register_callback(
        missing, fun emqx_license_nonexistent_module:zero/0
    ),
    ?check_trace(
        ?assertEqual(10, emqx_license_session_count:sum_callbacks()),
        fun(Trace) ->
            ?assertEqual(
                [],
                ?of_kind(license_session_count_callback_crash, Trace)
            )
        end
    ).

-doc "Other crashes are logged and the broken callback contributes 0.".
t_logs_crash(_Config) ->
    ok = emqx_license_session_count:register_callback(a, fun ?MODULE:ten/0),
    ok = emqx_license_session_count:register_callback(bad, fun ?MODULE:throws/0),
    ?check_trace(
        ?assertEqual(10, emqx_license_session_count:sum_callbacks()),
        fun(Trace) ->
            ?assertMatch(
                [#{callback := bad, class := throw, reason := something_else} | _],
                ?of_kind(license_session_count_callback_crash, Trace)
            )
        end
    ).

-doc "unregister_callback removes the entry; sum reflects the remaining callbacks.".
t_unregister(_Config) ->
    ok = emqx_license_session_count:register_callback(a, fun ?MODULE:ten/0),
    ok = emqx_license_session_count:register_callback(b, fun ?MODULE:seven/0),
    ok = emqx_license_session_count:unregister_callback(a),
    ?assertEqual(7, emqx_license_session_count:sum_callbacks()),
    ok = emqx_license_session_count:unregister_callback(b),
    ?assertEqual(0, emqx_license_session_count:sum_callbacks()),
    %% Removing an absent callback is a no-op.
    ok = emqx_license_session_count:unregister_callback(never_registered),
    ?assertEqual(0, emqx_license_session_count:sum_callbacks()).
