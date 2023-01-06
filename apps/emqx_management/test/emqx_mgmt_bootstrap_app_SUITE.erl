%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mgmt_bootstrap_app_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:boot_modules(all),
    application:load(emqx_modules),
    application:load(emqx_modules_spec),
    application:load(emqx_management),
    application:stop(emqx_rule_engine),
    ekka_mnesia:start(),
    emqx_ct_helpers:start_apps([]),
    Config.

end_per_suite(_) ->
    ok = application:unset_env(emqx_management, bootstrap_apps_file),
    emqx_mgmt_auth:clear_bootstrap_apps(),
    emqx_ct_helpers:stop_apps([]),
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_ok(_) ->
    application:stop(emqx_management),
    Bin = <<"test-1:secret-1\ntest-2:secret-2">>,
    File = "./bootstrap_apps.txt",
    ok = file:write_file(File, Bin),
    emqx_mgmt_auth:clear_bootstrap_apps(),
    application:set_env(emqx_management, bootstrap_apps_file, File),
    {ok, _} = application:ensure_all_started(emqx_management),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"secret-1">>)),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"secret-2">>)),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"secret-1">>)),

    %% load twice to check if the table is unchanged.
    application:stop(emqx_management),
    Bin1 = <<"test-1:new-secret-1\ntest-2:new-secret-2">>,
    ok = file:write_file(File, Bin1),
    application:set_env(emqx_management, bootstrap_apps_file, File),
    {ok, _} = application:ensure_all_started(emqx_management),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"secret-1">>)),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"secret-2">>)),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"new-secret-1">>)),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"new-secret-2">>)),
    application:stop(emqx_management).

t_bootstrap_user_file_not_found(_) ->
    File = "./bootstrap_apps_not_exist.txt",
    check_load_failed(File),
    ok.

t_load_invalid_username_failed(_) ->
    Bin = <<"test-1:password-1\ntest&2:password-2">>,
    File = "./bootstrap_apps.txt",
    ok = file:write_file(File, Bin),
    check_load_failed(File),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"password-1">>)),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test&2">>, <<"password-2">>)),
    ok.

t_load_invalid_format_failed(_) ->
    Bin = <<"test-1:password-1\ntest-2 password-2">>,
    File = "./bootstrap_apps.txt",
    ok = file:write_file(File, Bin),
    check_load_failed(File),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"password-1">>)),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"password-2">>)),
    ok.

check_load_failed(File) ->
    emqx_mgmt_auth:clear_bootstrap_apps(),
    application:stop(emqx_management),
    application:set_env(emqx_management, bootstrap_apps_file, File),
    ?assertMatch({error, _}, application:ensure_all_started(emqx_management)),
    ?assertNot(lists:member(emqx_management, application:which_applications())).
