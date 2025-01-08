%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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
-module(emqx_mgmt_api_stats_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").

all() ->
    emqx_common_test_helpers:all(?MODULE).

init_per_suite(Config) ->
    meck:expect(emqx, running_nodes, 0, [node(), 'fake@node']),
    Apps = emqx_cth_suite:start(
        [
            emqx,
            emqx_management,
            emqx_mgmt_api_test_util:emqx_dashboard()
        ],
        #{work_dir => emqx_cth_suite:work_dir(Config)}
    ),
    [{apps, Apps} | Config].

end_per_suite(Config) ->
    Apps = proplists:get_value(apps, Config),
    meck:unload(emqx),
    emqx_cth_suite:stop(Apps),
    ok.

t_stats_api(_) ->
    S = emqx_mgmt_api_test_util:api_path(["stats?aggregate=false"]),
    {ok, S1} = emqx_mgmt_api_test_util:request_api(get, S),
    [Stats1] = emqx_utils_json:decode(S1, [return_maps]),
    SystemStats1 = emqx_mgmt:get_stats(),
    Fun1 =
        fun(Key) ->
            ?assertEqual(maps:get(Key, SystemStats1), maps:get(atom_to_binary(Key, utf8), Stats1)),
            ?assertNot(is_map_key(<<"durable_subscriptions.count">>, Stats1), #{stats => Stats1})
        end,
    lists:foreach(Fun1, maps:keys(SystemStats1)),
    StatsPath = emqx_mgmt_api_test_util:api_path(["stats?aggregate=true"]),
    SystemStats = emqx_mgmt:get_stats(),
    {ok, StatsResponse} = emqx_mgmt_api_test_util:request_api(get, StatsPath),
    Stats = emqx_utils_json:decode(StatsResponse, [return_maps]),
    ?assertEqual(erlang:length(maps:keys(SystemStats)), erlang:length(maps:keys(Stats))),
    Fun =
        fun(Key) ->
            ?assertEqual(maps:get(Key, SystemStats), maps:get(atom_to_binary(Key, utf8), Stats))
        end,
    lists:foreach(Fun, maps:keys(SystemStats)).
