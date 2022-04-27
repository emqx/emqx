%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("emqx/include/http_api.hrl").
-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    mria:start(),
    application:load(emqx_dashboard),
    emqx_common_test_helpers:start_apps([emqx_conf, emqx_dashboard], fun set_special_configs/1),
    Config.

set_special_configs(emqx_dashboard) ->
    emqx_dashboard_api_test_helpers:set_default_config(),
    ok;
set_special_configs(_) ->
    ok.

end_per_suite(Config) ->
    end_suite(),
    Config.

end_per_testcase(_, _Config) ->
    All = emqx_dashboard_admin:all_users(),
    [emqx_dashboard_admin:remove_user(Name) || #{username := Name} <- All].

end_suite() ->
    application:unload(emqx_management),
    emqx_common_test_helpers:stop_apps([emqx_dashboard]).

t_check_user(_) ->
    Username = <<"admin1">>,
    Password = <<"public">>,
    BadUsername = <<"admin_bad">>,
    BadPassword = <<"public_bad">>,
    EmptyUsername = <<>>,
    EmptyPassword = <<>>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, <<"desc">>),
    ok = emqx_dashboard_admin:check(Username, Password),
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
    {ok, NewUser} = emqx_dashboard_admin:add_user(AddUser, AddPassword, AddDescription),
    AddUser = maps:get(username, NewUser),
    AddDescription = maps:get(description, NewUser),
    false = maps:is_key(password, NewUser),

    %% add again
    {error, <<"username_already_exist">>} =
        emqx_dashboard_admin:add_user(AddUser, AddPassword, AddDescription),

    %% add bad username
    BadNameError =
        <<"Bad Username. Only upper and lower case letters, numbers and underscores are supported">>,
    {error, BadNameError} = emqx_dashboard_admin:add_user(BadAddUser, AddPassword, AddDescription),
    ok.

t_lookup_user(_) ->
    LookupUser = <<"lookup_user">>,
    LookupPassword = <<"lookup_password">>,
    LookupDescription = <<"lookup_description">>,

    BadLookupUser = <<"***lookup_user_bad">>,

    {ok, _} =
        emqx_dashboard_admin:add_user(LookupUser, LookupPassword, LookupDescription),
    %% lookup success. not return password
    [#emqx_admin{username = LookupUser, description = LookupDescription}] =
        emqx_dashboard_admin:lookup_user(LookupUser),

    [] = emqx_dashboard_admin:lookup_user(BadLookupUser),
    ok.

t_all_users(_) ->
    Username = <<"admin_all">>,
    Password = <<"public">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, <<"desc">>),
    All = emqx_dashboard_admin:all_users(),
    ?assert(erlang:length(All) >= 1),
    ok.

t_delete_user(_) ->
    DeleteUser = <<"delete_user">>,
    DeletePassword = <<"delete_password">>,
    DeleteDescription = <<"delete_description">>,

    DeleteBadUser = <<"delete_user_bad">>,

    {ok, _NewUser} =
        emqx_dashboard_admin:add_user(DeleteUser, DeletePassword, DeleteDescription),
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

    {ok, _} = emqx_dashboard_admin:add_user(UpdateUser, UpdatePassword, UpdateDescription),
    {ok, NewUserInfo} =
        emqx_dashboard_admin:update_user(UpdateUser, NewDesc),
    UpdateUser = maps:get(username, NewUserInfo),
    NewDesc = maps:get(description, NewUserInfo),

    {error, <<"username_not_found">>} = emqx_dashboard_admin:update_user(BadUpdateUser, NewDesc),
    ok.

t_change_password(_) ->
    User = <<"change_user">>,
    OldPassword = <<"change_password">>,
    Description = <<"change_description">>,

    NewPassword = <<"new_password">>,

    BadChangeUser = <<"change_user_bad">>,

    {ok, _} = emqx_dashboard_admin:add_user(User, OldPassword, Description),

    {ok, ok} = emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),
    %% change pwd again
    {error, <<"password_error">>} =
        emqx_dashboard_admin:change_password(User, OldPassword, NewPassword),

    {error, <<"username_not_found">>} =
        emqx_dashboard_admin:change_password(BadChangeUser, OldPassword, NewPassword),
    ok.

t_clean_token(_) ->
    Username = <<"admin_token">>,
    Password = <<"public">>,
    NewPassword = <<"public1">>,
    {ok, _} = emqx_dashboard_admin:add_user(Username, Password, <<"desc">>),
    {ok, Token} = emqx_dashboard_admin:sign_token(Username, Password),
    ok = emqx_dashboard_admin:verify_token(Token),
    %% change password
    {ok, _} = emqx_dashboard_admin:change_password(Username, Password, NewPassword),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(Token),
    %% remove user
    {ok, Token2} = emqx_dashboard_admin:sign_token(Username, NewPassword),
    ok = emqx_dashboard_admin:verify_token(Token2),
    {ok, _} = emqx_dashboard_admin:remove_user(Username),
    timer:sleep(5),
    {error, not_found} = emqx_dashboard_admin:verify_token(Token2),
    ok.
