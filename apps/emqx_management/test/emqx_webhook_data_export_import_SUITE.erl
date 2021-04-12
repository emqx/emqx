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

-module(emqx_webhook_data_export_import_SUITE).
-include_lib("eunit/include/eunit.hrl").
-include_lib("emqx_rule_engine/include/rule_engine.hrl").
-compile([export_all, nowarn_export_all]).
%%--------------------------------------------------------------------
%% Setups
%%--------------------------------------------------------------------
all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Cfg) ->
    emqx_ct_helpers:start_apps([emqx_web_hook,
                                emqx_bridge_mqtt,
                                emqx_rule_engine,
                                emqx_modules,
                                emqx_management,
                                emqx_dashboard]),
    ok = ekka_mnesia:start(),
    ok = emqx_rule_registry:mnesia(boot),
    ok = emqx_rule_engine:load_providers(),
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
    emqx_ct_helpers:deps_path(emqx_management, "test/emqx_webhook_data_export_import_SUITE_data/").
%%--------------------------------------------------------------------
%% Cases
%%--------------------------------------------------------------------

handle_config(Config, 409) ->
    handle_config(Config, 422);

handle_config(Config, 415) ->
    handle_config(Config, 422);

handle_config(Config, 422) ->
    ?assertEqual(<<"http://www.emqx.io">>, maps:get(<<"url">>, Config)),
    ?assertEqual(<<"POST">>, maps:get(<<"method">>, Config)),
    ?assertEqual(#{"k" => "v"}, maps:get(<<"headers">>, Config));

handle_config(Config, 423) ->
    ?assertEqual(<<"http://www.emqx.io">>, maps:get(<<"url">>, Config)),
    ?assertEqual(<<"POST">>, maps:get(<<"method">>, Config)),
    ?assertEqual("application/json", maps:get(<<"content_type">>, Config)),
    ?assertEqual(#{"k" => "v"}, maps:get(<<"headers">>, Config));

handle_config(Config, 425) ->
    ?assertEqual(<<"http://www.emqx.io">>, maps:get(<<"url">>, Config)),
    ?assertEqual(<<"POST">>, maps:get(<<"method">>, Config)),
    ?assertEqual(#{"k" => "v"}, maps:get(<<"headers">>, Config)),
    ?assertEqual(5, maps:get(<<"connect_timeout">>, Config)),
    ?assertEqual(5, maps:get(<<"request_timeout">>, Config)),
    ?assertEqual(8, maps:get(<<"pool_size">>, Config));

handle_config(Config, 430) ->
    ?assertEqual(<<"http://www.emqx.io">>, maps:get(<<"url">>, Config)),
    ?assertEqual(<<"POST">>, maps:get(<<"method">>, Config)),
    ?assertEqual(#{"k" => "v"}, maps:get(<<"headers">>, Config)),
    ?assertEqual("5s", maps:get(<<"connect_timeout">>, Config)),
    ?assertEqual("5s", maps:get(<<"request_timeout">>, Config)),
    ?assertEqual(false, maps:get(<<"verify">>, Config)),
    ?assertEqual(true, is_map(maps:get(<<"cacertfile">>, Config))),
    ?assertEqual(true, is_map(maps:get(<<"certfile">>, Config))),
    ?assertEqual(true, is_map(maps:get(<<"keyfile">>, Config))),
    ?assertEqual(8, maps:get(<<"pool_size">>, Config));

handle_config(_, _) -> ok.

remove_resource(Id) ->
    emqx_rule_registry:remove_resource(Id),
    emqx_rule_registry:remove_resource_params(Id).

import(FilePath, Version) ->
    Overrides = emqx_json:encode(#{<<"auth.mnesia.as">> => atom_to_binary(clientid)}),
    ok = emqx_mgmt_data_backup:import(get_data_path() ++ "/" ++ FilePath, Overrides),
    lists:foreach(fun(#resource{id = Id, config = Config} = _Resource) ->
        case Id of
            "webhook" ->
            handle_config(Config, Version),
            remove_resource(Id);
            _ -> ok
        end
    end, emqx_rule_registry:get_resources()).

t_import422(_) ->
    import("422.json", 422),
    {ok, _} = emqx_mgmt_data_backup:export().

t_import423(_) ->
    import("423.json", 423),
    {ok, _} = emqx_mgmt_data_backup:export().

t_import425(_) ->
    import("425.json", 425),
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
