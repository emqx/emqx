%%--------------------------------------------------------------------
%% Copyright (c) 2020-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_stats_api_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    ekka_mnesia:start(),
    emqx_mgmt_auth:mnesia(boot),
    emqx_ct_helpers:start_apps([emqx_management], fun set_special_configs/1),
    Config.

end_per_suite(_) ->
    emqx_ct_helpers:stop_apps([emqx_management]).

set_special_configs(emqx_management) ->
    emqx_config:put([emqx_management], #{listeners => [#{protocol => http, port => 8081}],
        applications =>[#{id => "admin", secret => "public"}]}),
    ok;
set_special_configs(_App) ->
    ok.

t_stats_api(_) ->
    StatsPath = emqx_mgmt_api_test_util:api_path(["stats"]),
    SystemStats = emqx_mgmt:get_stats(),
    {ok, StatsResponse} = emqx_mgmt_api_test_util:request_api(get, StatsPath),
    Stats = emqx_json:decode(StatsResponse, [return_maps]),
    ?assertEqual(erlang:length(maps:keys(SystemStats)), erlang:length(maps:keys(Stats))),
    Fun =
        fun(Key) ->
            ?assertEqual(maps:get(Key, SystemStats), maps:get(atom_to_binary(Key, utf8), Stats))
        end,
    lists:foreach(Fun, maps:keys(SystemStats)).
