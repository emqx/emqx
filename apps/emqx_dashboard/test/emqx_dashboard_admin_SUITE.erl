%%--------------------------------------------------------------------
%% Copyright (c) 2020-2024 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_dashboard_admin_SUITE).

-compile(nowarn_export_all).
-compile(export_all).

-include("emqx_dashboard.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

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
    Apps = ?config(apps, Config),
    emqx_cth_suite:stop(Apps),
    ok.

end_per_testcase(_, _Config) ->
    All = emqx_dashboard_admin:all_users(),
    [emqx_dashboard_admin:remove_user(Name) || #{username := Name} <- All].

t_check_user(_) ->
    Username = <<"admin1">>,
    Password = <<"public_1">>,
    BadUsername = <<"admin_bad">>,
    BadPassword = <<"public_bad">>,
    EmptyUsername = <<>>,
    EmptyPassword = <<>>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, _} = emqx_dashboard_admin:check(Username, Password),
    {error, <<"password_error">>} = emqx_dashboard_admin:check(Username, BadPassword),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(BadUsername, Password),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(BadUsername, BadPassword),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:check(EmptyUsername, Password),
    {error, <<"password_error">>} = emqx_dashboard_admin:check(Username, EmptyPassword),
    {error, <<"username_not_provided">>} = emqx_dashboard_admin:check(undefined, Password),
    {error, <<"password_not_provided">>} = emqx_dashboard_admin:check(Username, undefined),
    ok.

t_add_user(_) ->
    AddUser = <<"add_user">>,
    AddPassword = <<"add_password">>,
    AddDescription = <<"add_description">>,

    BadAddUser = <<"***add_user_bad">>,

    %% add success. not return password
    {ok, NewUser} = emqx_dashboard_admin:add_user(
        AddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription
    ),
    AddUser = maps:get(username, NewUser),
    AddDescription = maps:get(description, NewUser),
    false = maps:is_key(password, NewUser),

    %% add again
    {error, <<"username_already_exists">>} =
        emqx_dashboard_admin:add_user(AddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription),

    %% add bad username
    BadNameError =
        <<"Bad Username. Only upper and lower case letters, numbers and underscores are supported">>,
    {error, BadNameError} = emqx_dashboard_admin:add_user(
        BadAddUser, AddPassword, ?ROLE_SUPERUSER, AddDescription
    ),
    ok.

t_lookup_user(_) ->
    LookupUser = <<"lookup_user">>,
    LookupPassword = <<"lookup_password">>,
    LookupDescription = <<"lookup_description">>,

    BadLookupUser = <<"***lookup_user_bad">>,

    {ok, _} =
        emqx_dashboard_admin:add_user(
            LookupUser, LookupPassword, ?ROLE_SUPERUSER, LookupDescription
        ),
    %% lookup success. not return password
    [#emqx_admin{username = LookupUser, description = LookupDescription}] =
        emqx_dashboard_admin:lookup_user(LookupUser),

    [] = emqx_dashboard_admin:lookup_user(BadLookupUser),
    ok.

t_all_users(_) ->
    Username = <<"admin_all">>,
    Password = <<"public_2">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    All = emqx_dashboard_admin:all_users(),
    ?assert(erlang:length(All) >= 1),
    ok.

t_delete_user(_) ->
    DeleteUser = <<"delete_user">>,
    DeletePassword = <<"delete_password">>,
    DeleteDescription = <<"delete_description">>,

    DeleteBadUser = <<"delete_user_bad">>,

    {ok, _NewUser} =
        emqx_dashboard_admin:add_user(
            DeleteUser, DeletePassword, ?ROLE_SUPERUSER, DeleteDescription
        ),
    {ok, ok} = emqx_dashboard_admin:remove_user(DeleteUser),
    %% remove again
    {error, <<"username_not_found">>} = emqx_dashboard_admin:remove_user(DeleteUser),
    {error, <<"username_not_found">>} = emqx_dashboard_admin:remove_user(DeleteBadUser),
    ok.

t_update_user(_) ->
    UpdateUser = <<"update_user">>,
    UpdatePassword = <<"update_password">>,
    UpdateDescription = <<"update_description">>,

    NewDesc = <<"new_description">>,

    BadUpdateUser = <<"update_user_bad">>,

    {ok, _} = emqx_dashboard_admin:add_user(
        UpdateUser, UpdatePassword, ?ROLE_SUPERUSER, UpdateDescription
    ),
    {ok, NewUserInfo} =
        emqx_dashboard_admin:update_user(UpdateUser, ?ROLE_SUPERUSER, NewDesc),
    UpdateUser = maps:get(username, NewUserInfo),
    NewDesc = maps:get(description, NewUserInfo),

    {error, <<"username_not_found">>} = emqx_dashboard_admin:update_user(
        BadUpdateUser, ?ROLE_SUPERUSER, NewDesc
    ),
    ok.

t_change_password(_) ->
    User = <<"change_user">>,
    OldPassword = <<"change_password">>,
    Description = <<"change_description">>,

    NewPassword = <<"new_password">>,
    NewBadPassword = <<"public">>,

    BadChangeUser = <<"change_user_bad">>,

    {ok, _} = emqx_dashboard_admin:add_user(User, OldPassword, ?ROLE_SUPERUSER, Description),

    {ok, ok} = emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),
    %% change pwd again
    {error, <<"password_error">>} =
        emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),

    {error, <<"The range of password length is 8~64">>} =
        emqx_dashboard_admin:change_password(User, NewPassword, NewBadPassword),

    {error, <<"username_not_found">>} =
        emqx_dashboard_admin:change_password(BadChangeUser, OldPassword, NewPassword),
    ok.

t_clean_token(_) ->
    Username = <<"admin_token">>,
    Password = <<"public_www1">>,
    NewPassword = <<"public_www2">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, #{token := Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    FakePath = erlang:list_to_binary(emqx_dashboard_swagger:relative_uri("/fake")),
    FakeReq = #{method => <<"GET">>, path => FakePath},
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% change password
    {ok, _} = emqx_dashboard_admin:change_password(Username, Password, NewPassword),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, Token),
    %% remove user
    {ok, #{token := Token2}} = emqx_dashboard_admin:sign_token(Username, NewPassword),
    {ok, Username} = emqx_dashboard_admin:verify_token(FakeReq, Token2),
    {ok, _} = emqx_dashboard_admin:remove_user(Username),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(FakeReq, Token2),
    ok.

t_password_expired(_) ->
    Username = <<"t_password_expired">>,
    Password = <<"public_www1">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, ?ROLE_SUPERUSER, <<"desc">>),
    {ok, #{token := _Token}} = emqx_dashboard_admin:sign_token(Username, Password),
    [#?ADMIN{extra = #{password_ts := PwdTS}} = User] = emqx_dashboard_admin:lookup_user(Username),
    PwdTS2 = PwdTS - 86400 * 2,
    emqx_dashboard_admin:unsafe_update_user(User#?ADMIN{extra = #{password_ts => PwdTS2}}),
    SignResult = emqx_dashboard_admin:sign_token(Username, Password),
    ?assertMatch({ok, #{password_expire_in_seconds := X}} when X =< -86400, SignResult),
    ok.
