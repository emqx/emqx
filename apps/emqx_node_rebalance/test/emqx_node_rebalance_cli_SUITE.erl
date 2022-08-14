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

-import(emqx_eviction_agent_test_helpers,
        [emqtt_connect_many/1]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent, emqx_node_rebalance]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_node_rebalance, emqx_eviction_agent]),
    Config.


init_per_testcase(t_rebalance, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    Node = emqx_node_helpers:start_slave(
             evacuate1,
             #{start_apps => [emqx, emqx_eviction_agent, emqx_node_rebalance]}),
    [{evacuate_node, Node} | Config];
init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop(),
    Config.

end_per_testcase(t_rebalance, Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop(),
    _ = emqx_node_helpers:stop_slave(?config(evacuate_node, Config));
end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation:stop(),
    _ = emqx_node_rebalance:stop().

t_evacuation(_Config) ->
    %% usage
    ok = emqx_node_rebalance_cli:cli(["foobar"]),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status", atom_to_list(node())]),

    %% start with invalid args
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--foo-bar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--conn-evict-rate", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--sess-evict-rate", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation", "--wait-takeover", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--migrate-to", "nonexistent@node"])),
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--migrate-to", ""])),
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--unknown-arg"])),
    ?assert(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--conn-evict-rate", "10",
                                     "--sess-evict-rate", "10",
                                     "--wait-takeover", "10",
                                     "--migrate-to", atom_to_list(node()),
                                     "--redirect-to", "srv"])),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status", atom_to_list(node())]),

    ?assertMatch(
       {enabled, #{}},
       emqx_node_rebalance_evacuation:status()),

    %% already enabled
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--evacuation",
                                     "--conn-evict-rate", "10",
                                     "--redirect-to", "srv"])),

    %% stop
    true = emqx_node_rebalance_cli:cli(["stop"]),

    false = emqx_node_rebalance_cli:cli(["stop"]),

    ?assertEqual(
       disabled,
       emqx_node_rebalance_evacuation:status()).

t_rebalance(Config) ->
    %% start with invalid args
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--foo-bar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--conn-evict-rate", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--abs-conn-threshold", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--rel-conn-threshold", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--sess-evict-rate", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--abs-sess-threshold", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--rel-sess-threshold", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--wait-takeover", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start", "--wait-health-check", "foobar"])),

    ?assertNot(
        emqx_node_rebalance_cli:cli(["start",
                                     "--nodes", "nonexistent@node"])),
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start",
                                     "--nodes", ""])),
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start",
                                     "--nodes", atom_to_list(?config(evacuate_node, Config))])),
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start",
                                     "--unknown-arg"])),

    _ = emqtt_connect_many(20),

    ?assert(
        emqx_node_rebalance_cli:cli(["start",
                                     "--conn-evict-rate", "10",
                                     "--abs-conn-threshold", "10",
                                     "--rel-conn-threshold", "1.1",
                                     "--sess-evict-rate", "10",
                                     "--abs-sess-threshold", "10",
                                     "--rel-sess-threshold", "1.1",
                                     "--wait-takeover", "10",
                                     "--nodes", atom_to_list(node()) ++ ","
                                                ++ atom_to_list(?config(evacuate_node, Config))
                                    ])),

    %% status
    ok = emqx_node_rebalance_cli:cli(["status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status"]),
    ok = emqx_node_rebalance_cli:cli(["node-status", atom_to_list(node())]),

    ?assertMatch(
       {enabled, #{}},
       emqx_node_rebalance:status()),

    %% already enabled
    ?assertNot(
        emqx_node_rebalance_cli:cli(["start"])),

    %% stop
    true = emqx_node_rebalance_cli:cli(["stop"]),

    false = emqx_node_rebalance_cli:cli(["stop"]),

    ?assertEqual(
       disabled,
       emqx_node_rebalance:status()).
