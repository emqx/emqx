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

-module(emqx_node_rebalance_cli_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent, emqx_node_rebalance]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_node_rebalance, emqx_eviction_agent]),
    Config.

init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    Config.

end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation:stop().

t_evacuation(_Config) ->
    %% usage
    ok = emqx_node_rebalance_cli:cli(["foobar"]),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),

    %% start with invalid args
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--foo-bar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--conn-evict-rate", "foobar"])),

    ?assert(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--conn-evict-rate", "10",
                                     "--redirect-to", "srv"])),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),

    ?assertMatch(
       {enabled, #{}},
       emqx_node_rebalance_evacuation:status()),

    %% already enabled
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--conn-evict-rate", "10",
                                     "--redirect-to", "srv"])),

    %% stop
    ok = emqx_node_rebalance_cli:cli(["stop"]),

    ?assertEqual(
       disabled,
       emqx_node_rebalance_evacuation:status()).
