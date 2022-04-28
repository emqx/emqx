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

-module(emqx_node_rebalance_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_helpers,
        [request_api/3,
         auth_header_/0,
         api_path/1]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent, emqx_node_rebalance, emqx_management]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_node_rebalance, emqx_eviction_agent]),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation:stop().

t_status(_Config) ->

    ?assertMatch(
        #{<<"status">> := <<"disabled">>},
        api_get(["node_rebalance/status"])),

    ok = emqx_node_rebalance_evacuation:start(
           #{conn_evict_rate => 10,
             server_reference => <<"srv">>}),

    ?assertMatch(
       #{<<"status">> := <<"enabled">>,
         <<"connection_eviction_rate">> := 10,
         <<"goal">> := 0,
         <<"stats">> := #{
                          <<"initial_connected">> := _,
                          <<"current_connected">> := _
                         }},
        api_get(["node_rebalance/status"])),

    ok = emqx_node_rebalance_evacuation:stop(),

    ?assertMatch(
        #{<<"status">> := <<"disabled">>},
        api_get(["node_rebalance/status"])).

api_get(Path) ->
    {ok, ResponseBody} = request_api(get, api_path(Path), auth_header_()),
    jiffy:decode(list_to_binary(ResponseBody), [return_maps]).
