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

-module(emqx_node_rebalance_evacuation_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

-import(emqx_eviction_agent_test_helpers,
        [emqtt_connect/0, emqtt_connect/2, emqtt_try_connect/0]).

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
             #{start_apps => [emqx, emqx_eviction_agent]}),
    [{evacuate_node, Node} | Config].

end_per_testcase(_Case, Config) ->
    _ = emqx_node_helpers:stop_slave(?config(evacuate_node, Config)),
    _ = emqx_node_rebalance_evacuation:stop().

t_agent_busy(Config) ->

    ok = emqx_eviction_agent:enable(other_rebalance, undefined),

    ?assertEqual(
       {error, eviction_agent_busy},
       emqx_node_rebalance_evacuation:start(opts(Config))),

    emqx_eviction_agent:disable(other_rebalance).


t_already_started(Config) ->
    ok = emqx_node_rebalance_evacuation:start(opts(Config)),

    ?assertEqual(
       {error, already_started},
       emqx_node_rebalance_evacuation:start(opts(Config))),

    ok = emqx_node_rebalance_evacuation:stop().

t_not_started(_Config) ->
    ?assertEqual(
       {error, not_started},
       emqx_node_rebalance_evacuation:stop()).

t_start(Config) ->
    process_flag(trap_exit, true),

    ok = emqx_node_rebalance_evacuation:start(opts(Config)),
    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),
    ok = emqx_node_rebalance_evacuation:stop().

t_persistence(Config) ->
    process_flag(trap_exit, true),

    ok = emqx_node_rebalance_evacuation:start(opts(Config)),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),

    ok = supervisor:terminate_child(emqx_node_rebalance_sup, emqx_node_rebalance_evacuation),
    {ok, _} = supervisor:restart_child(emqx_node_rebalance_sup, emqx_node_rebalance_evacuation),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),
    ?assertMatch(
       {enabled, #{conn_evict_rate := 10}},
       emqx_node_rebalance_evacuation:status()),

    ok = emqx_node_rebalance_evacuation:stop().

t_conn_evicted(Config) ->
    process_flag(trap_exit, true),

    {ok, C} = emqtt_connect(),

    ?check_trace(
       ?wait_async_action(
          emqx_node_rebalance_evacuation:start(opts(Config)),
          #{?snk_kind := node_evacuation_evict_conn},
          1000),
       fun(_Result, _Trace) -> ok end),

    ct:sleep(100),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),

    ?assertNot(
       is_process_alive(C)).

t_migrate_to(Config) ->
    ?assertEqual(
       [?config(evacuate_node, Config)],
       emqx_node_rebalance_evacuation:migrate_to(undefined)),

    ?assertEqual(
       [],
       emqx_node_rebalance_evacuation:migrate_to(['unknown@node'])),

    rpc:call(?config(evacuate_node, Config),
             emqx_eviction_agent,
             enable,
             [test_rebalance, undefined]),

    ?assertEqual(
       [],
       emqx_node_rebalance_evacuation:migrate_to(undefined)).

t_session_evicted(Config) ->
    process_flag(trap_exit, true),

    {ok, C} = emqtt_connect(<<"client_with_sess">>, false),

    ?check_trace(
       ?wait_async_action(
          emqx_node_rebalance_evacuation:start(opts(Config)),
          #{?snk_kind := node_evacuation_evict_sess_over},
          5000),
       fun(_Result, Trace) ->
        ?assertMatch(
           [_ | _],
           ?of_kind(node_evacuation_evict_sess_over, Trace))
       end),

    receive
        {'EXIT', C, {disconnected, ?RC_USE_ANOTHER_SERVER, _}} -> ok
    after 1000 ->
              ?assert(false, "Connection not evicted")
    end,

    [ChannelPid] = emqx_cm_registry:lookup_channels(<<"client_with_sess">>),

    ?assertEqual(
       ?config(evacuate_node, Config),
       node(ChannelPid)).

t_unknown_messages(Config) ->
    ok = emqx_node_rebalance_evacuation:start(opts(Config)),

    whereis(emqx_node_rebalance_evacuation) ! unknown,

    gen_server:cast(emqx_node_rebalance_evacuation, unknown),

    ?assertEqual(
       ignored,
       gen_server:call(emqx_node_rebalance_evacuation, unknown)),

    ok = emqx_node_rebalance_evacuation:stop().

opts(Config) ->
    #{
      server_reference => <<"srv">>,
      conn_evict_rate => 10,
      sess_evict_rate => 10,
      wait_takeover => 1,
      migrate_to => [?config(evacuate_node, Config)]
     }.
