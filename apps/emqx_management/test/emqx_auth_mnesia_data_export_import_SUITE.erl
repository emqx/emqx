%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_modules/include/emqx_modules.hrl").
-compile([export_all, nowarn_export_all]).

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
    dbg:tracer(),dbg:p(all,call),
    dbg:tpl(emqx_mgmt_data_backup,import_auth_mnesia,x),
    dbg:tp(emqx_modules_registry,get_modules,x),
    dbg:tp(emqx_module_internal_acl,on_module_create,x),
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
-ifdef(EMQX_ENTERPRISE).

t_importee430(_) ->
    import("ee435.json", ee435),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_all_users_and_acl().

-endif.

remove_all_users_and_acl() ->
    mnesia:delete_table(emqx_user),
    mnesia:delete_table(emqx_acl).
