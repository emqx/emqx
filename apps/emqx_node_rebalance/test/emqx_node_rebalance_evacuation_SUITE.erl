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
    Config.

end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance_evacuation:stop().

t_agent_busy(_Config) ->

    ok = emqx_eviction_agent:enable(foo, undefined),

    ?assertEqual(
       {error, eviction_agent_busy},
       emqx_node_rebalance_evacuation:start(opts())),

    emqx_eviction_agent:disable(foo).


t_already_started(_Config) ->
    ok = emqx_node_rebalance_evacuation:start(opts()),

    ?assertEqual(
       {error, already_started},
       emqx_node_rebalance_evacuation:start(opts())),

    ok = emqx_node_rebalance_evacuation:stop().

t_not_started(_Config) ->
    ?assertEqual(
       {error, not_started},
       emqx_node_rebalance_evacuation:stop()).

t_start(_Config) ->
    process_flag(trap_exit, true),

    ok = emqx_node_rebalance_evacuation:start(opts()),
    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),
    ok = emqx_node_rebalance_evacuation:stop().

t_persistence(_Config) ->
    process_flag(trap_exit, true),

    ok = emqx_node_rebalance_evacuation:start(opts()),

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

t_status_evict(_Config) ->
    process_flag(trap_exit, true),

    {ok, C} = emqtt_connect(),

    ?check_trace(
       ?wait_async_action(
          emqx_node_rebalance_evacuation:start(opts()),
          #{?snk_kind := node_evacuation_evict},
          1000),
       fun(_Result, _Trace) -> ok end),

    ct:sleep(100),

    ?assertMatch(
       {error, {use_another_server, #{}}},
       emqtt_try_connect()),

    ?assertNot(
       is_process_alive(C)).

opts() ->
    #{
      server_reference => <<"srv">>,
      conn_evict_rate => 10
     }.


emqtt_connect() ->
    {ok, C} = emqtt:start_link(
                [{clientid, <<"client1">>},
                 {clean_start, true},
                 {proto_ver, v5},
                 {properties, #{'Session-Expiry-Interval' => 60}}
                ]),
    case emqtt:connect(C) of
        {ok, _} -> {ok, C};
        {error, _} = Error -> Error
    end.

emqtt_try_connect() ->
    case emqtt_connect() of
        {ok, C} ->
            emqtt:disconnect(C),
            ok;
        {error, _} = Error -> Error
    end.
