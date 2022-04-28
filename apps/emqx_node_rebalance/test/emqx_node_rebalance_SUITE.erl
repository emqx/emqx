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

-module(emqx_node_rebalance_SUITE).

-compile(export_all).
-compile(nowarn_export_all).

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/emqx_mqtt.hrl").
-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").
-include_lib("snabbkaffe/include/snabbkaffe.hrl").

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

init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance:stop(),
    Node = emqx_node_helpers:start_slave(
             recipient1,
             #{start_apps => [emqx, emqx_eviction_agent, emqx_node_rebalance]}),
    [{recipient_node, Node} | Config].

end_per_testcase(_Case, Config) ->
    _ = emqx_node_helpers:stop_slave(?config(recipient_node, Config)),
    _ = emqx_node_rebalance:stop().

t_rebalance(Config) ->
    process_flag(trap_exit, true),
    RecipientNode = ?config(recipient_node, Config),

    Nodes = [node(), RecipientNode],

    _Conns = emqtt_connect_many(500),

    Opts = #{conn_evict_rate => 10,
             sess_evict_rate => 10,
             evict_interval => 10,
             abs_conn_threshold => 50,
             abs_sess_threshold => 50,
             rel_conn_threshold => 1.0,
             rel_sess_threshold => 1.0,
             wait_health_check => 0.01,
             wait_takeover => 0.01,
             nodes => Nodes
            },

    ?check_trace(
       ?wait_async_action(
          emqx_node_rebalance:start(Opts),
          #{?snk_kind := emqx_node_rebalance_evict_sess_over},
          10000),
       fun({ok, _}, Trace) ->
        ?assertMatch(
           [_ | _],
           ?of_kind(emqx_node_rebalance_evict_sess_over, Trace))
       end),

    DonorConnCount = emqx_eviction_agent:connection_count(),
    DonorSessCount = emqx_eviction_agent:session_count(),
    DonorDSessCount = emqx_eviction_agent:session_count(disconnected),

    RecipientConnCount = rpc:call(RecipientNode, emqx_eviction_agent, connection_count, []),
    RecipientSessCount = rpc:call(RecipientNode, emqx_eviction_agent, session_count, []),
    RecipientDSessCount = rpc:call(RecipientNode, emqx_eviction_agent, session_count, [disconnected]),

    ct:pal("Donor: conn=~p, sess=~p, dsess=~p",
             [DonorConnCount, DonorSessCount, DonorDSessCount]),
    ct:pal("Recipient: conn=~p, sess=~p, dsess=~p",
             [RecipientConnCount, RecipientSessCount, RecipientDSessCount]),

    ?assert(DonorConnCount - 50 =< RecipientConnCount),
    ?assert(DonorDSessCount - 50 =< RecipientDSessCount).

t_rebalance_node_crash(Config) ->
    process_flag(trap_exit, true),
    RecipientNode = ?config(recipient_node, Config),

    Nodes = [node(), RecipientNode],

    _Conns = emqtt_connect_many(50),

    Opts = #{conn_evict_rate => 10,
             sess_evict_rate => 10,
             evict_interval => 10,
             abs_conn_threshold => 50,
             abs_sess_threshold => 50,
             rel_conn_threshold => 1.0,
             rel_sess_threshold => 1.0,
             wait_health_check => 0.01,
             wait_takeover => 0.01,
             nodes => Nodes
            },

    ok = emqx_node_rebalance:start(Opts),

    ?check_trace(
       ?wait_async_action(
          emqx_node_helpers:stop_slave(?config(recipient_node, Config)),
          #{?snk_kind := emqx_node_rebalance_started},
          1000),
       fun(_Result, _Trace) -> ok end),

    ?assertEqual(
       disabled,
       emqx_node_rebalance:status()).

t_no_need_to_rebalance(_Config) ->
    process_flag(trap_exit, true),

    ?assertEqual(
        {error, nothing_to_balance},
        emqx_node_rebalance:start(#{})),

    _Conns = emqtt_connect_many(50),

    ?assertEqual(
        {error, nothing_to_balance},
        emqx_node_rebalance:start(#{})).

t_unknown_mesages(Config) ->
    process_flag(trap_exit, true),
    RecipientNode = ?config(recipient_node, Config),

    Nodes = [node(), RecipientNode],

    _Conns = emqtt_connect_many(500),

    Opts = #{wait_health_check => 100,
             abs_conn_threshold => 50,
             nodes => Nodes
            },

    Pid = whereis(emqx_node_rebalance),

    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),
    ?assertEqual(
       ignored,
       gen_server:call(Pid, unknown)),

    ok = emqx_node_rebalance:start(Opts),

    Pid ! unknown,
    ok = gen_server:cast(Pid, unknown),
    ?assertEqual(
       ignored,
       gen_server:call(Pid, unknown)).

t_available_nodes(Config) ->
    rpc:call(?config(recipient_node, Config),
             emqx_eviction_agent,
             enable,
             [test_rebalance, undefined]),
    ?assertEqual(
       [node()],
       emqx_node_rebalance:available_nodes(
         [node(), ?config(recipient_node, Config)])).
