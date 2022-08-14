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

-include_lib("eunit/include/eunit.hrl").
-include_lib("common_test/include/ct.hrl").

-import(emqx_mgmt_api_test_helpers,
        [request_api/3,
         request_api/5,
         auth_header_/0,
         api_path/1]).

-import(emqx_eviction_agent_test_helpers,
        [emqtt_connect_many/1]).

all() ->
    emqx_ct:all(?MODULE).

init_per_suite(Config) ->
    emqx_ct_helpers:start_apps([emqx_eviction_agent, emqx_node_rebalance, emqx_management]),
    Config.

end_per_suite(Config) ->
    emqx_ct_helpers:stop_apps([emqx_management, emqx_node_rebalance, emqx_eviction_agent]),
    Config.

init_per_testcase(Case, Config)
  when Case =:= t_start_evacuation_validation
       orelse Case =:= t_start_rebalance_validation
       orelse Case =:= t_start_stop_rebalance ->
    _ = emqx_node_rebalance:stop(),
    _ = emqx_node_rebalance_evacuation:stop(),
    Node = emqx_node_helpers:start_slave(
             recipient1,
             #{start_apps => [emqx, emqx_eviction_agent, emqx_node_rebalance]}),
    [{recipient_node, Node} | Config];
init_per_testcase(_Case, Config) ->
    _ = emqx_node_rebalance:stop(),
    _ = emqx_node_rebalance_evacuation:stop(),
    Config.

end_per_testcase(Case, Config)
  when Case =:= t_start_evacuation_validation
       orelse Case =:= t_start_rebalance_validation
       orelse Case =:= t_start_stop_rebalance ->
    _ = emqx_node_helpers:stop_slave(?config(recipient_node, Config)),
    _ = emqx_node_rebalance:stop(),
    _ = emqx_node_rebalance_evacuation:stop();
end_per_testcase(_Case, _Config) ->
    _ = emqx_node_rebalance:stop(),
    _ = emqx_node_rebalance_evacuation:stop().

t_start_evacuation_validation(Config) ->
    BadOpts = [#{conn_evict_rate => <<"conn">>},
               #{sess_evict_rate => <<"sess">>},
               #{redirect_to => 123},
               #{wait_takeover => <<"wait">>},
               #{migrate_to => []},
               #{migrate_to => <<"migrate_to">>},
               #{migrate_to => [<<"bad_node">>]},
               #{migrate_to => [<<"bad_node">>, atom_to_binary(node())]},
               #{unknown => <<"Value">>}
              ],
    lists:foreach(
      fun(Opts) ->
              ?assertMatch(
                 {ok, #{}},
                 api_post(["load_rebalance", atom_to_list(node()), "evacuation", "start"],
                          Opts)),

              ?assertMatch(
                 {ok, #{<<"status">> := <<"disabled">>}},
                 api_get(["load_rebalance", "status"]))

      end,
      BadOpts),
    ?assertMatch(
       {ok, #{}},
       api_post(["load_rebalance", "bad@node", "evacuation", "start"],
                #{})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
       api_get(["load_rebalance", "status"])),

    ?assertMatch(
       {ok, #{}},
       api_post(["load_rebalance", atom_to_list(node()), "evacuation", "start"],
                #{conn_evict_rate => 10,
                  sess_evict_rate => 10,
                  wait_takeover => 10,
                  redirect_to => <<"srv">>,
                  migrate_to => [atom_to_binary(?config(recipient_node, Config))]})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"enabled">>}},
       api_get(["load_rebalance", "status"])).


t_start_rebalance_validation(Config) ->
    BadOpts = [#{conn_evict_rate => <<"conn">>},
               #{sess_evict_rate => <<"sess">>},
               #{abs_conn_threshold => <<"act">>},
               #{rel_conn_threshold => <<"rct">>},
               #{abs_sess_threshold => <<"act">>},
               #{rel_sess_threshold => <<"rct">>},
               #{wait_takeover => <<"wait">>},
               #{wait_health_check => <<"wait">>},
               #{nodes => <<"nodes">>},
               #{nodes => []},
               #{nodes => [<<"bad_node">>]},
               #{nodes => [<<"bad_node">>, atom_to_binary(node())]},
               #{unknown => <<"Value">>}
              ],
    lists:foreach(
      fun(Opts) ->
              ?assertMatch(
                 {ok, #{}},
                 api_post(["load_rebalance", atom_to_list(node()), "start"],
                          Opts)),

              ?assertMatch(
                 {ok, #{<<"status">> := <<"disabled">>}},
                 api_get(["load_rebalance", "status"]))

      end,
      BadOpts),
    ?assertMatch(
       {ok, #{}},
       api_post(["load_rebalance", "bad@node", "start"],
                #{})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
       api_get(["load_rebalance", "status"])),

    _Conns = emqtt_connect_many(50),

    ?assertMatch(
       {ok, #{}},
       api_post(["load_rebalance", atom_to_list(node()), "start"],
                #{conn_evict_rate => 10,
                  sess_evict_rate => 10,
                  wait_takeover => 10,
                  wait_health_check => 10,
                  abs_conn_threshold => 10,
                  rel_conn_threshold => 1.001,
                  abs_sess_threshold => 10,
                  rel_sess_threshold => 1.001,
                  nodes => [atom_to_binary(?config(recipient_node, Config)),
                            atom_to_binary(node())]})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"enabled">>}},
       api_get(["load_rebalance", "status"])).

t_start_stop_evacuation(_Config) ->

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])),

    ?assertMatch(
       {ok, #{}},
        api_post(["load_rebalance", atom_to_list(node()), "evacuation", "start"],
                 #{conn_evict_rate => 10,
                   sess_evict_rate => 20})),

    ?assertMatch(
       {ok, #{<<"state">> := _,
              <<"process">> := <<"evacuation">>,
              <<"connection_eviction_rate">> := 10,
              <<"session_eviction_rate">> := 20,
              <<"connection_goal">> := 0,
              <<"session_goal">> := 0,
              <<"stats">> := #{
                               <<"initial_connected">> := _,
                               <<"current_connected">> := _,
                               <<"initial_sessions">> := _,
                               <<"current_sessions">> := _
                              }}},
        api_get(["load_rebalance", "status"])),

    ?assertMatch(
       {ok, #{<<"rebalances">> := #{},
              <<"evacuations">> :=
              #{<<"test@127.0.0.1">> := #{<<"state">> := _,
                                          <<"connection_eviction_rate">> := 10,
                                          <<"session_eviction_rate">> := 20,
                                          <<"connection_goal">> := 0,
                                          <<"session_goal">> := 0,
                                          <<"stats">> := #{
                                                           <<"initial_connected">> := _,
                                                           <<"current_connected">> := _,
                                                           <<"initial_sessions">> := _,
                                                           <<"current_sessions">> := _
                                                          }
                                         }
               }}},
        api_get(["load_rebalance", "global_status"])),

    ?assertMatch(
       {ok, #{}},
        api_post(["load_rebalance", atom_to_list(node()), "evacuation", "stop"],
                 #{})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])),

    ?assertMatch(
       {ok, #{<<"evacuations">> := #{}, <<"rebalances">> := #{}}},
        api_get(["load_rebalance", "global_status"])).

t_start_stop_rebalance(Config) ->

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])),

    _Conns = emqtt_connect_many(100),

    ?assertMatch(
       {ok, #{}},
        api_post(["load_rebalance", atom_to_list(node()), "start"],
                 #{conn_evict_rate => 10,
                   sess_evict_rate => 20,
                   abs_conn_threshold => 10})),

    ?assertMatch(
       {ok, #{<<"state">> := _,
              <<"process">> := <<"rebalance">>,
              <<"coordinator_node">> := _,
              <<"connection_eviction_rate">> := 10,
              <<"session_eviction_rate">> := 20,
              <<"stats">> := #{
                               <<"initial_connected">> := _,
                               <<"current_connected">> := _,
                               <<"initial_sessions">> := _,
                               <<"current_sessions">> := _
                              }}},
        api_get(["load_rebalance", "status"])),

    DonorNode = atom_to_binary(node()),
    RecipientNode = atom_to_binary(?config(recipient_node, Config)),

    ?assertMatch(
       {ok, #{<<"evacuations">> := #{},
              <<"rebalances">> :=
              #{<<"test@127.0.0.1">> := #{<<"state">> := _,
                                          <<"coordinator_node">> := _,
                                          <<"connection_eviction_rate">> := 10,
                                          <<"session_eviction_rate">> := 20,
                                          <<"donors">> := [DonorNode],
                                          <<"recipients">> := [RecipientNode]
                                         }
               }}},
        api_get(["load_rebalance", "global_status"])),

    ?assertMatch(
       {ok, #{}},
        api_post(["load_rebalance", atom_to_list(node()), "stop"],
                 #{})),

    ?assertMatch(
       {ok, #{<<"status">> := <<"disabled">>}},
        api_get(["load_rebalance", "status"])),

    ?assertMatch(
       {ok, #{<<"evacuations">> := #{}, <<"rebalances">> := #{}}},
        api_get(["load_rebalance", "global_status"])).

t_availability_check(_Config) ->

    ?assertMatch(
       {ok, #{}},
       api_get(["load_rebalance", "availability_check"])),

    ok = emqx_node_rebalance_evacuation:start(#{}),

    ?assertMatch(
       {error, {_, 503, _}},
       api_get(["load_rebalance", "availability_check"])),

    ok = emqx_node_rebalance_evacuation:stop(),

    ?assertMatch(
       {ok, #{}},
       api_get(["load_rebalance", "availability_check"])).

api_get(Path) ->
    case request_api(get, api_path(Path), auth_header_()) of
        {ok, ResponseBody} ->
            {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
        {error, _} = Error -> Error
    end.

api_post(Path, Data) ->
    case request_api(post, api_path(Path), [], auth_header_(), Data) of
        {ok, ResponseBody} ->
            {ok, jiffy:decode(list_to_binary(ResponseBody), [return_maps])};
        {error, _} = Error -> Error
    end.
