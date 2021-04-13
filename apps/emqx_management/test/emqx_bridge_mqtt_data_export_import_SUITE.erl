%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    ok = emqx_rule_engine:load_providers(),
    emqx_ct_helpers:start_apps([emqx_web_hook,
                                emqx_bridge_mqtt,
                                emqx_rule_engine,
                                emqx_modules,
                                emqx_management,
                                emqx_dashboard]),
    Cfg.

end_per_suite(Cfg) ->
    emqx_ct_helpers:stop_apps([emqx_dashboard,
                               emqx_management,
                               emqx_modules,
                               emqx_rule_engine,
                               emqx_bridge_mqtt,
                               emqx_web_hook]),
    Cfg.

get_data_path() ->
    emqx_ct_helpers:deps_path(emqx_management, "test/emqx_bridge_mqtt_data_export_import_SUITE_data/").

remove_resource(Id) ->
    emqx_rule_registry:remove_resource(Id),
    emqx_rule_registry:remove_resource_params(Id).

import(FilePath, Version) ->
    ok = emqx_mgmt_data_backup:import(get_data_path() ++ "/" ++ FilePath, <<"{}">>),
    lists:foreach(fun(#resource{id = Id, config = Config} = _Resource) ->
        timer:sleep(1000),
        case Id of
            <<"bridge">> ->
                test_utils:resource_is_alive(Id),
                handle_config(Config, Version, bridge),
                remove_resource(Id);
            <<"rpc">> ->
                test_utils:resource_is_alive(Id),
                handle_config(Config, Version, rpc),
                remove_resource(Id);
            _ -> ok
        end
    end, emqx_rule_registry:get_resources()).

%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

-ifndef(EMQX_ENTERPRISE).

t_import420(_) ->
    import("420.json", 420),
    {ok, _} = emqx_mgmt_data_backup:export().

t_import430(_) ->
    import("430.json", 430),
    {ok, _} = emqx_mgmt_data_backup:export().

t_import409(_) ->
    import("409.json", 409),
    {ok, _} = emqx_mgmt_data_backup:export().

t_import415(_) ->
    import("415.json", 415),
    {ok, _} = emqx_mgmt_data_backup:export().


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
    {ok, _} = emqx_mgmt_data_backup:export().

t_importee410(_) ->
    import("ee410.json", ee410),
    {ok, _} = emqx_mgmt_data_backup:export().

t_importee411(_) ->
    import("ee411.json", ee411),
    {ok, _} = emqx_mgmt_data_backup:export().

t_importee420(_) ->
    import("ee420.json", ee420),
    {ok, _} = emqx_mgmt_data_backup:export().

t_importee425(_) ->
    import("ee425.json", ee425),
    {ok, _} = emqx_mgmt_data_backup:export().

t_importee430(_) ->
    import("ee430.json", ee430),
    {ok, _} = emqx_mgmt_data_backup:export().

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