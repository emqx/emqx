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

-module(emqx_dashboard_admin_bootstrap_user).

-compile(export_all).
-compile(nowarn_export_all).
-import(emqx_dashboard_SUITE, [http_post/2]).

-include_lib("common_test/include/ct.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx/include/emqx.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    Config.

end_per_suite(_) ->
    ok.

%%--------------------------------------------------------------------
%% Test cases
%%--------------------------------------------------------------------

t_load_ok(_) ->
    Bin = <<"test-1:password-1\ntest-2:password-2">>,
    File = "./bootstrap_users.txt",
    ok = file:write_file(File, Bin),
    _ = mnesia:clear_table(emqx_admin),
    application:set_env(emqx_dashboard, bootstrap_users_file, File),
    emqx_ct_helpers:start_apps([emqx_dashboard]),
    ?assertEqual(#{<<"code">> => 0}, check_auth(<<"test-1">>, <<"password-1">>)),
    ?assertEqual(#{<<"code">> => 0}, check_auth(<<"test-2">>, <<"password-2">>)),
    ?assertEqual(#{<<"message">> => <<"Username/Password error">>},
        check_auth(<<"test-2">>, <<"password-1">>)),
    emqx_ct_helpers:stop_apps([emqx_dashboard]).

t_bootstrap_user_file_not_found(_) ->
    File = "./bootstrap_users_not_exist.txt",
    check_load_failed(File),
    ok.

t_load_invalid_username_failed(_) ->
    Bin = <<"test-1:password-1\ntest&2:password-2">>,
    File = "./bootstrap_users.txt",
    ok = file:write_file(File, Bin),
    check_load_failed(File),
    ok.

t_load_invalid_format_failed(_) ->
    Bin = <<"test-1:password-1\ntest-2password-2">>,
    File = "./bootstrap_users.txt",
    ok = file:write_file(File, Bin),
    check_load_failed(File),
    ok.

check_load_failed(File) ->
    _ = mnesia:clear_table(emqx_admin),
    application:set_env(emqx_dashboard, bootstrap_users_file, File),
    ?assertError(_, emqx_ct_helpers:start_apps([emqx_dashboard])),
    ?assertNot(lists:member(emqx_dashboard, application:which_applications())),
    ?assertEqual(0, mnesia:table_info(mqtt_admin, size)).


check_auth(Username, Password) ->
    {ok, Res} = http_post("auth", #{<<"username">> => Username, <<"password">> => Password}),
    json(Res).

json(Data) ->
    {ok, Jsx} = emqx_json:safe_decode(Data, [return_maps]), Jsx.
