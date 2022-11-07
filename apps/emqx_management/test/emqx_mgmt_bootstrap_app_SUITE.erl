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
    _ = mnesia:clear_table(mqtt_app),
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
    _ = mnesia:clear_table(mqtt_app),
    application:set_env(emqx_management, bootstrap_apps_file, File),
    {ok, _} = application:ensure_all_started(emqx_management),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-1">>, <<"secret-1">>)),
    ?assert(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"secret-2">>)),
    ?assertNot(emqx_mgmt_auth:is_authorized(<<"test-2">>, <<"secret-1">>)),
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
    ok.

t_load_invalid_format_failed(_) ->
    Bin = <<"test-1:password-1\ntest-2password-2">>,
    File = "./bootstrap_apps.txt",
    ok = file:write_file(File, Bin),
    check_load_failed(File),
    ok.

check_load_failed(File) ->
    _ = mnesia:clear_table(mqtt_app),
    application:stop(emqx_management),
    application:set_env(emqx_management, bootstrap_apps_file, File),
    ?assertMatch({error, _}, application:ensure_all_started(emqx_management)),
    ?assertNot(lists:member(emqx_management, application:which_applications())),
    ?assertEqual(0, mnesia:table_info(mqtt_app, size)).
