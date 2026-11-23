%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_dashboard_login_lock_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-import(
    emqx_mgmt_api_test_util,
    [
        uri/1,
        request_api/6
    ]
).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("emqx/include/emqx.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").
-include("emqx_dashboard.hrl").

-define(USERNAME, <<"admin">>).
-define(PASSWORD, <<"public_www1">>).
-define(WRONG_PASSWORD, <<"wrong_password">>).
-define(GOOD_TOTP, <<"123456">>).
-define(BAD_TOTP, <<"654321">>).

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    SuiteApps = emqx_cth_suite:start(
        [
            emqx_conf,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{suite_apps, SuiteApps} | Config].

end_per_suite(Config) ->
    emqx_cth_suite:stop(?config(suite_apps, Config)).

init_per_testcase(TestCase, Config) ->
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        ?USERNAME, ?PASSWORD, ?ROLE_SUPERUSER, <<"simple_description">>
    ),
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 1),
    maybe_setup_mfa(TestCase),
    Config.

end_per_testcase(TestCase, Config) ->
    maybe_teardown_mfa(TestCase),
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 600),
    emqx_dashboard_login_lock:cleanup_all(),
    Config.

t_login_lock(_) ->
    %% 5 unsuccessful logins should lead to login lock
    ok = lists:foreach(
        fun(_I) ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
                api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
            )
        end,
        lists:seq(1, 5)
    ),

    %% check that login is locked, even with correct password
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        api_post([login], #{username => ?USERNAME, password => ?PASSWORD})
    ),

    %% wait for lock to be released
    ct:sleep(1500),

    %% check that login is successful after lock is released
    ?assertMatch(
        {ok, _},
        api_post([login], #{username => ?USERNAME, password => ?PASSWORD})
    ),
    ok.

t_cancel_lock_with_successful_login(_) ->
    %% make 4 unsuccessful logins, one less than the lock threshold
    ok = lists:foreach(
        fun(_I) ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
                api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
            )
        end,
        lists:seq(1, 4)
    ),

    %% make one successful login to cancel the lock
    ?assertMatch(
        {ok, _},
        api_post([login], #{username => ?USERNAME, password => ?PASSWORD})
    ),
    ?assertEqual(0, failed_attempt_record_count()),

    %% now unsuccessful logins should be allowed again
    ok = lists:foreach(
        fun(_I) ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
                api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
            )
        end,
        lists:seq(1, 4)
    ).

t_cancel_lock_with_cli(_Config) ->
    %% Use a long lock duration so the lock cannot expire between locking and the
    %% assertions below under a slow CI (the suite default is 1s, kept short only
    %% for `t_login_lock' which tests lock release).
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 600),
    %% make 5 unsuccessful logins to lock the account
    ok = lists:foreach(
        fun(_I) ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
                api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
            )
        end,
        lists:seq(1, 5)
    ),

    %% check that login is locked
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        api_post([login], #{username => ?USERNAME, password => ?PASSWORD})
    ),

    %% password reset should unlock the account
    emqx_dashboard_cli:admins(["passwd", "admin", "new_password"]),

    %% check that login is successful after password change
    ?assertMatch(
        {ok, _},
        api_post([login], #{username => ?USERNAME, password => <<"new_password">>})
    ).

t_cleanup(_) ->
    %% Short interval, so the attempt records this case asserts on are
    %% cleaned up quickly. The lock deadline is truncated to whole seconds,
    %% so a 1s lock can expire before the assertion below; keep it long.
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 60),
    emqx_config:put([dashboard, unsuccessful_login_interval], 1),

    %% make 5 unsuccessful logins to lock the account
    ok = lists:foreach(
        fun(_I) ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
                api_post([login], #{username => ?USERNAME, password => <<"wrong_password">>})
            )
        end,
        lists:seq(1, 5)
    ),

    %% check that login is locked
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        api_post([login], #{username => ?USERNAME, password => ?PASSWORD})
    ),

    %% check that tables are empty
    ?retry(
        _Inteval = 500,
        _Attempts = 4,
        ?assertEqual(0, failed_attempt_record_count())
    ).

t_no_record_for_invalid_username(_) ->
    %% Check that no record is created for invalid username
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
        api_post([login], #{username => <<"invalid_username">>, password => <<"wrong_password">>})
    ),
    ?assertEqual(0, failed_attempt_record_count()).

t_cleanup_many(_) ->
    %% This test checks that batch accumulation and deletion works properly
    %% during cleanup.

    %% Create 2222 interleaving records, 1111 outdated and 1111 actual
    lists:foreach(
        fun(I) ->
            Username = <<"user_", (integer_to_binary(I))/binary, "_a">>,
            emqx_dashboard_login_lock:register_unsuccessful_login(Username)
        end,
        lists:seq(1, 1111)
    ),
    NowUs =
        erlang:system_time(microsecond) +
            erlang:convert_time_unit(
                emqx_config:get([dashboard, unsuccessful_login_interval]), second, microsecond
            ),
    ct:sleep(1),
    lists:foreach(
        fun(I) ->
            Username = <<"user_", (integer_to_binary(I))/binary, "_b">>,
            emqx_dashboard_login_lock:register_unsuccessful_login(Username)
        end,
        lists:seq(1, 1111)
    ),
    ct:sleep(100),
    ?assertEqual(2222, failed_attempt_record_count()),

    %% Cleanup all outdated records
    ok = emqx_dashboard_login_lock:cleanup_all(NowUs),
    ct:sleep(100),

    %% Check that only the actual records are left
    ?assertEqual(1111, failed_attempt_record_count()),

    %% Check that no-op cleanup also works
    ok = emqx_dashboard_login_lock:cleanup_all(NowUs),
    ct:sleep(100),
    ?assertEqual(1111, failed_attempt_record_count()).

t_mfa_wrong_code_counts_as_unsuccessful_login(_) ->
    repeat(max_attempts(), fun() ->
        ?assertMatch(
            {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
            mfa_login(?PASSWORD, ?BAD_TOTP)
        )
    end),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        mfa_login(?PASSWORD, ?GOOD_TOTP)
    ).

t_mfa_wrong_password_and_code_counts_as_unsuccessful_login(_) ->
    repeat(max_attempts(), fun() ->
        ?assertMatch(
            {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
            mfa_login(?WRONG_PASSWORD, ?BAD_TOTP)
        )
    end),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        mfa_login(?PASSWORD, ?GOOD_TOTP)
    ).

t_mfa_missing_code_not_counted(_) ->
    repeat(max_attempts() + 1, fun() ->
        ?assertMatch(
            {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
            mfa_login(?PASSWORD, undefined)
        ),
        ?assertMatch(
            {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
            mfa_login(?WRONG_PASSWORD, undefined)
        )
    end),
    ?assertEqual(0, failed_attempt_record_count()),
    ?assertMatch({ok, _}, mfa_login(?PASSWORD, ?GOOD_TOTP)).

t_mfa_wrong_code_and_wrong_password_share_limit(_) ->
    5 = max_attempts(),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
        mfa_login(?WRONG_PASSWORD, ?GOOD_TOTP)
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
        mfa_login(?PASSWORD, ?BAD_TOTP)
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_USERNAME_OR_PWD">>}},
        mfa_login(?WRONG_PASSWORD, ?GOOD_TOTP)
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
        mfa_login(?WRONG_PASSWORD, ?BAD_TOTP)
    ),
    ?assertEqual(4, failed_attempt_record_count()),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
        mfa_login(?PASSWORD, ?BAD_TOTP)
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        mfa_login(?PASSWORD, ?GOOD_TOTP)
    ).

t_mfa_successful_login_resets_wrong_code_count(_) ->
    BelowLimit = max_attempts() - 1,
    WrongCodes = fun() ->
        repeat(BelowLimit, fun() ->
            ?assertMatch(
                {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
                mfa_login(?PASSWORD, ?BAD_TOTP)
            )
        end)
    end,
    WrongCodes(),
    ?assertMatch({ok, _}, mfa_login(?PASSWORD, ?GOOD_TOTP)),
    %% The reset deletes the attempt records asynchronously.
    ?retry(100, 20, ?assertEqual(0, failed_attempt_record_count())),
    WrongCodes(),
    ?assertEqual(BelowLimit, failed_attempt_record_count()),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
        mfa_login(?PASSWORD, ?BAD_TOTP)
    ),
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"LOGIN_LOCKED">>}},
        mfa_login(?PASSWORD, ?GOOD_TOTP)
    ).

t_mfa_wrong_password_and_code_is_one_attempt(_) ->
    ?assertMatch(
        {error, 401, #{<<"code">> := <<"BAD_MFA_TOKEN">>}},
        mfa_login(?WRONG_PASSWORD, ?BAD_TOTP)
    ),
    ?assertEqual(1, failed_attempt_record_count()).

%%--------------------------------------------------------------------
%% Helpers
%%--------------------------------------------------------------------

api_post(Path, Data) ->
    case request_api(post, uri(Path), [], noauth_header(), Data, #{return_all => true}) of
        {ok, ResponseBody} ->
            Res =
                case emqx_utils_json:safe_decode(ResponseBody) of
                    {ok, Decoded} -> Decoded;
                    {error, _} -> ResponseBody
                end,
            {ok, Res};
        {error, {{_Status, Code, _Message}, _Headers, Body}} ->
            {error, Code, emqx_utils_json:decode(Body)}
    end.

noauth_header() ->
    emqx_common_test_http:auth_header("invalid", "password").

failed_attempt_record_count() ->
    length(mnesia:dirty_all_keys(login_attempts)).

max_attempts() ->
    emqx_config:get([dashboard, unsuccessful_login_max_attempts]).

repeat(N, Fun) ->
    lists:foreach(fun(_) -> Fun() end, lists:seq(1, N)).

mfa_login(Password, undefined) ->
    api_post([login], #{username => ?USERNAME, password => Password});
mfa_login(Password, MfaToken) ->
    api_post([login], #{username => ?USERNAME, password => Password, mfa_token => MfaToken}).

is_mfa_case(TestCase) ->
    lists:prefix("t_mfa_", atom_to_list(TestCase)).

maybe_setup_mfa(TestCase) ->
    case is_mfa_case(TestCase) of
        true ->
            %% A long lock, so that it cannot expire before the assertions.
            emqx_config:put([dashboard, unsuccessful_login_lock_duration], 600),
            ok = meck:new(pot, [passthrough, no_history]),
            ok = meck:expect(pot, valid_totp, fun(Token, _) -> Token =:= ?GOOD_TOTP end),
            {ok, ok} = emqx_dashboard_admin:set_mfa_state(?USERNAME, #{
                mechanism => totp,
                secret => <<"JBSWY3DPEHPK3PXP">>,
                first_verify_ts => erlang:system_time(second)
            }),
            ok;
        false ->
            ok
    end.

maybe_teardown_mfa(TestCase) ->
    case is_mfa_case(TestCase) of
        true -> ok = meck:unload(pot);
        false -> ok
    end.
