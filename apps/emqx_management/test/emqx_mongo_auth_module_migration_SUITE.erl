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

-module(emqx_mongo_auth_module_migration_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

-ifdef(EMQX_ENTERPRISE).
-include_lib("emqx_modules/include/emqx_modules.hrl").
-endif.

all() ->
    emqx_ct:all(?MODULE).

-ifdef(EMQX_ENTERPRISE).

init_per_suite(Config) ->
    application:load(emqx_modules_spec),
    emqx_ct_helpers:start_apps([emqx_management, emqx_modules]),
    Config.

end_per_suite(_Config) ->
    emqx_ct_helpers:stop_apps([emqx_modules, emqx_management]),
    application:unload(emqx_modules_spec),
    ok.

t_import_4_2(Config) ->
    ?assertMatch(ok, import("e4.2.8.json", Config)),
    timer:sleep(100),

    MongoAuthNModule = emqx_modules_registry:find_module_by_type(mongo_authentication),
    ?assertNotEqual(not_found, MongoAuthNModule),
    ?assertMatch(#module{config = #{<<"srv_record">> := _}}, MongoAuthNModule),
    delete_modules().

t_import_4_3(Config) ->
    ?assertMatch(ok, import("e4.3.5.json", Config)),
    timer:sleep(100),

    MongoAuthNModule = emqx_modules_registry:find_module_by_type(mongo_authentication),
    ?assertNotEqual(not_found, MongoAuthNModule),
    ?assertMatch(#module{config = #{<<"srv_record">> := _}}, MongoAuthNModule),
    delete_modules().

import(File, Config) ->
    Filename = filename:join(proplists:get_value(data_dir, Config), File),
    {ok, Content} = file:read_file(Filename),
    BackupFile = filename:join(emqx:get_env(data_dir), File),
    ok = file:write_file(BackupFile, Content),
    emqx_mgmt_data_backup:import(File, "{}").

delete_modules() ->
    [emqx_modules_registry:remove_module(Mod) || Mod <-  emqx_modules_registry:get_modules()].

-endif.
