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

-module(emqx_node_rebalance_agent_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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
    Node = emqx_node_helpers:start_slave(
             evacuate1,
             #{start_apps => [emqx, emqx_eviction_agent, emqx_node_rebalance]}),
    [{evacuate_node, Node} | Config].

end_per_testcase(_Case, Config) ->
    _ = emqx_node_helpers:stop_slave(?config(evacuate_node, Config)),
    _ = emqx_node_rebalance_evacuation:stop().

t_enable_disable(_Config) ->
    ?assertEqual(
       disabled,
       emqx_node_rebalance_agent:status()),

    ?assertEqual(
       ok,
       emqx_node_rebalance_agent:enable(self())),

    ?assertEqual(
       {error, already_enabled},
       emqx_node_rebalance_agent:enable(self())),

    ?assertEqual(
       {enabled, self()},
       emqx_node_rebalance_agent:status()),

    ?assertEqual(
       {error, invalid_coordinator},
       emqx_node_rebalance_agent:disable(spawn_link(fun() -> ok end))),

    ?assertEqual(
       ok,
       emqx_node_rebalance_agent:disable(self())),

    ?assertEqual(
       {error, already_disabled},
       emqx_node_rebalance_agent:disable(self())),

    ?assertEqual(
       disabled,
       emqx_node_rebalance_agent:status()).

t_enable_egent_busy(_Config) ->
    ok = emqx_eviction_agent:enable(rebalance_test, undefined),

    ?assertEqual(
       {error, eviction_agent_busy},
       emqx_node_rebalance_agent:enable(self())),

    ok = emqx_eviction_agent:disable(rebalance_test).

% The following tests verify that emqx_node_rebalance_agent correctly links
% coordinator process with emqx_eviction_agent-s.

t_rebalance_agent_coordinator_fail(Config) ->
    process_flag(trap_exit, true),

    Node = ?config(evacuate_node, Config),


    CoordinatorPid = spawn_link(
                       fun() ->
                               receive
                                done -> ok
                               end
                       end),

    ?assertEqual(
       disabled,
       rpc:call(Node, emqx_eviction_agent, status, [])),

    ?assertEqual(
       ok,
       rpc:call(Node, emqx_node_rebalance_agent, enable, [CoordinatorPid])),

    ?assertMatch(
       {enabled, _},
       rpc:call(Node, emqx_eviction_agent, status, [])),

    EvictionAgentPid = rpc:call(Node, erlang, whereis, [emqx_eviction_agent]),
    true = link(EvictionAgentPid),

    true = exit(CoordinatorPid, kill),

    receive
        {'EXIT', EvictionAgentPid, _} -> true
    after
        1000 -> ?assert(false, "emqx_eviction_agent did not exit")
    end.

t_rebalance_agent_fail(Config) ->
    process_flag(trap_exit, true),

    Node = ?config(evacuate_node, Config),

    CoordinatorPid = spawn_link(
                       fun() ->
                               receive
                                done -> ok
                               end
                       end),

    ?assertEqual(
       ok,
       rpc:call(Node, emqx_node_rebalance_agent, enable, [CoordinatorPid])),

    EvictionAgentPid = rpc:call(Node, erlang, whereis, [emqx_eviction_agent]),
    true = exit(EvictionAgentPid, kill),

    receive
        {'EXIT', CoordinatorPid, _} -> true
    after
        1000 -> ?assert(false, "emqx_eviction_agent did not exit")
    end.

t_unknown_messages(_Config) ->
    Pid = whereis(emqx_node_rebalance_agent),

    ok = gen_server:cast(Pid, unknown),

    Pid ! unknown,

    ignored = gen_server:call(Pid, unknown).
