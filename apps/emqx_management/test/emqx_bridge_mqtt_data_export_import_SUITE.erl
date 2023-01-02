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

-module(emqx_bridge_mqtt_data_export_import_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-compile([export_all, nowarn_export_all]).

% -define(EMQX_ENTERPRISE, true).

%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    ekka_mnesia:start(),
    ok = emqx_dashboard_admin:mnesia(boot),
    application:load(emqx_modules),
    application:load(emqx_bridge_mqtt),
    ekka_mnesia:start(),
    emqx_dashboard_admin:mnesia(boot),
    emqx_ct_helpers:start_apps([emqx_rule_engine, emqx_management]),
    application:ensure_all_started(emqx_dashboard),
    ok = emqx_rule_engine:load_providers(),
    Cfg.

end_per_suite(Cfg) ->
    emqx_mgmt_data_backup:delete_all_backup_file(),
    application:stop(emqx_dashboard),
    emqx_ct_helpers:stop_apps([emqx_management, emqx_rule_engine]),
    Cfg.

get_data_path() ->
    emqx_ct_helpers:deps_path(emqx_management, "test/emqx_bridge_mqtt_data_export_import_SUITE_data/").

import(FilePath0, Version) ->
    Filename = filename:basename(FilePath0),
    FilePath = filename:join([get_data_path(), FilePath0]),
    {ok, Bin} = file:read_file(FilePath),
    ok = emqx_mgmt_data_backup:upload_backup_file(Filename, Bin),
    timer:sleep(500),
    lists:foreach(fun(#resource{id = Id, config = Config} = _Resource) ->
        case Id of
            <<"bridge">> ->
                test_utils:resource_is_alive(Id),
                handle_config(Config, Version, bridge);
            <<"rpc">> ->
                test_utils:resource_is_alive(Id),
                handle_config(Config, Version, rpc);
            _ -> ok
        end
    end, emqx_rule_registry:get_resources()).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-ifndef(EMQX_ENTERPRISE).

t_import420(_) ->
    import("420.json", 420),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_import430(_) ->
    import("430.json", 430),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_import409(_) ->
    import("409.json", 409),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_import415(_) ->
    import("415.json", 415),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().


handle_config(Config, 420, bridge) ->
    ?assertEqual(false, maps:get(<<"ssl">>, Config));

handle_config(Config, 430, bridge) ->
    ?assertEqual(false, maps:get(<<"ssl">>, Config));

handle_config(Config, 420, rpc) ->
    handle_config(Config, 430, rpc);

handle_config(Config, 409, rpc) ->
    handle_config(Config, 420, rpc);

handle_config(Config, 415, rpc) ->
    handle_config(Config, 420, rpc);

handle_config(Config, 409, bridge) ->
    handle_config(Config, 420, bridge);

handle_config(Config, 415, bridge) ->
    handle_config(Config, 420, bridge);

handle_config(Config, 430, rpc) ->
    ?assertEqual(<<"test@127.0.0.1">>, maps:get(<<"address">>, Config)),
    ?assertEqual(32, maps:get(<<"batch_size">>, Config)),
    ?assertEqual(<<"off">>, maps:get(<<"disk_cache">>, Config)),
    ?assertEqual(<<"bridge/emqx/${node}/">>, maps:get(<<"mountpoint">>, Config)),
    ?assertEqual(<<"30s">>, maps:get(<<"reconnect_interval">>, Config)),
    ?assertEqual(8, maps:get(<<"pool_size">>, Config));

handle_config(_, _, _) -> ok.

-endif.

-ifdef(EMQX_ENTERPRISE).

t_importee4010(_) ->
    import("ee4010.json", ee4010),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_importee410(_) ->
    import("ee410.json", ee410),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_importee411(_) ->
    import("ee411.json", ee411),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_importee420(_) ->
    import("ee420.json", ee420),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_importee425(_) ->
    import("ee425.json", ee425),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

t_importee430(_) ->
    import("ee430.json", ee430),
    {ok, _} = emqx_mgmt_data_backup:export(),
    remove_resources().

%%--------------------------------------------------------------------
%% handle_config
%%--------------------------------------------------------------------

handle_config(Config, ee4010, Id) ->
    handle_config(Config, ee430, Id);

handle_config(Config, ee410, Id) ->
    handle_config(Config, ee430, Id);

handle_config(Config, ee411, Id) ->
    handle_config(Config, ee430, Id);

handle_config(Config, ee420, Id) ->
    handle_config(Config, ee430, Id);

handle_config(Config, ee425, Id) ->
    handle_config(Config, ee430, Id);

handle_config(Config, ee430, bridge) ->
    ?assertEqual(false, maps:get(<<"ssl">>, Config));

handle_config(Config, ee430, rpc) ->
    ?assertEqual(<<"off">>, maps:get(<<"disk_cache">>, Config));

handle_config(Config, ee435, Id) ->
    handle_config(Config, ee430, Id).
-endif.

remove_resources() ->
    timer:sleep(500),
    lists:foreach(fun(#resource{id = Id}) ->
        emqx_rule_engine:delete_resource(Id)
    end, emqx_rule_registry:get_resources()),
    timer:sleep(500).
