%%--------------------------------------------------------------------
%% Copyright (c) 2022-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_auth_mnesia_data_export_import_SUITE).

-compile([export_all, nowarn_export_all]).

-ifdef(EMQX_ENTERPRISE).

-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_modules/include/emqx_modules.hrl").

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    _ = application:load(emqx_modules_spec),
    emqx_ct_helpers:start_apps([emqx_rule_engine, emqx_modules,
                                emqx_management, emqx_dashboard]),
    Cfg.

end_per_suite(Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_dashboard, emqx_management,
                               emqx_modules, emqx_rule_engine]),
    Cfg.

get_data_path() ->
    emqx_ct_helpers:deps_path(emqx_management, "test/emqx_auth_mnesia_data_export_import_SUITE_data/").

import(FilePath, _Version) ->
    ok = emqx_mgmt_data_backup:import(get_data_path() ++ "/" ++ FilePath, <<"{}">>),
    [_] = lists:filter(
            fun(#module{type = mnesia_authentication}) -> true;
               (_) -> false
            end, emqx_modules_registry:get_modules()),
    ?assertNotEqual(0, ets:info(emqx_user, size)),
    ?assertNotEqual(0, ets:info(emqx_acl, size)).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

t_importee427(_) ->
    import("ee427.json", ee427),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_all_users_and_acl().

t_importee430(_) ->
    import("ee435.json", ee435),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_all_users_and_acl().

t_import_test(_) ->
    SimpleAdmin = <<"simpleAdmin">>,
    SimplePassword = <<"simplepassword">>,
    SimplePasswordHash = emqx_dashboard_admin:hash(SimplePassword),

    Admins = [<<"Admin1">>, <<"Admin2">>, <<"Admin3">>, <<"Admin4">>, <<"Admin5">>],
    Passwords = [<<"password1">>, <<"PAssword2">>,<<"3&*)dkdKlkd">>,<<"&*qwl4kd>">>,<<"PASSWORD5D">>],

    %% add some users
    add_admins(Admins, Passwords),
    %% Allow force import simple password.
    ok = emqx_dashboard_admin:force_add_user(SimpleAdmin, SimplePasswordHash, <<"test">>),

    ct:pal("1111~p~n", [ets:info(mqtt_admin)]),
    ct:pal("~p~n", [ets:tab2list(mqtt_admin)]),
    check_admins_ok(Admins, Passwords),

    {ok, #{filename := FileName}} = emqx_mgmt_data_backup:export(),

    remove_admins(Admins),
    ok = emqx_dashboard_admin:remove_user(SimpleAdmin),
    ct:pal("0000~n"),
    check_admins_failed(Admins, Passwords),
    {error, _} = emqx_dashboard_admin:check(SimpleAdmin, SimplePassword),

    ok = emqx_mgmt_data_backup:import(FileName, <<"{}">>),
    ct:pal("2222~n"),
    check_admins_ok(Admins, Passwords),
    ok = emqx_dashboard_admin:check(SimpleAdmin, SimplePassword),

    remove_admins(Admins),
    ok = emqx_dashboard_admin:remove_user(SimpleAdmin),

    remove_all_users_and_acl(),
    ok.

add_admins(Admins, Passwords) ->
    lists:foreach(
        fun({Admin, Password}) ->
            ok = emqx_dashboard_admin:add_user(Admin, Password, <<"test">>)
        end, lists:zip(Admins, Passwords)),
    ok.

check_admins_ok(Admins, Passwords) ->
    lists:foreach(
        fun({Admin, Password}) ->
            ?assertMatch(ok, emqx_dashboard_admin:check(Admin, Password), {Admin, Password})
        end, lists:zip(Admins, Passwords)),
    ok.

check_admins_failed(Admins, Passwords) ->
    lists:foreach(
        fun({Admin, Password}) ->
            ?assertMatch({error, _}, emqx_dashboard_admin:check(Admin, Password), {Admin, Password})
        end, lists:zip(Admins, Passwords)),
    ok.

remove_admins(Admins) ->
    lists:foreach(
        fun(Admin) ->
            ok = emqx_dashboard_admin:remove_user(Admin)
        end, Admins),
    ok.

remove_all_users_and_acl() ->
    mnesia:delete_table(emqx_user),
    mnesia:delete_table(emqx_acl).

-else.

%% opensource edition

all() -> [].

-endif.
