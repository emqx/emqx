%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

init_per_testcase(_TestCase, Config) ->
    mnesia:clear_table(?ADMIN),
    emqx_dashboard_admin:add_user(
        ?USERNAME, ?PASSWORD, ?ROLE_SUPERUSER, <<"simple_description">>
    ),
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 1),
    Config.

end_per_testcase(_TestCase, Config) ->
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

t_check_cleanup(_) ->
    %% Set small timeouts for records
    emqx_config:put([dashboard, unsuccessful_login_lock_duration], 1),
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
        ?assertMatch(
            #{failed_attempt_records := 0},
            emqx_dashboard_login_lock:info()
        )
    ).

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
