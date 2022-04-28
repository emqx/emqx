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

-module(emqx_node_rebalance_api).

-import(minirest,  [return/1]).

-rest_api(#{name   => load_rebalance_status,
            method => 'GET',
            path   => "/load_rebalance/status",
            func   => status,
            descr  => "Get load rebalance status"}).

-rest_api(#{name   => load_rebalance_global_status,
            method => 'GET',
            path   => "/load_rebalance/global_status",
            func   => global_status,
            descr  => "Get status of all rebalance/evacuation processes across the cluster"}).

-rest_api(#{name   => load_rebalance_availability_check,
            method => 'GET',
            path   => "/load_rebalance/availability_check",
            func   => availability_check,
            descr  => "Node rebalance availability check"}).

-rest_api(#{name   => load_rebalance_start,
            method => 'POST',
            path   => "/load_rebalance/:bin:node/start",
            func   => rebalance_start,
            descr  => "Start rebalancing with the node as coordinator"}).

-rest_api(#{name   => load_rebalance_stop,
            method => 'POST',
            path   => "/load_rebalance/:bin:node/stop",
            func   => rebalance_stop,
            descr  => "Stop rebalancing coordinated by the node"}).

-rest_api(#{name   => load_rebalance_evacuation_start,
            method => 'POST',
            path   => "/load_rebalance/:bin:node/evacuation/start",
            func   => rebalance_evacuation_start,
            descr  => "Start evacuation on a node "}).

-rest_api(#{name   => load_rebalance_evacuation_stop,
            method => 'POST',
            path   => "/load_rebalance/:bin:node/evacuation/stop",
            func   => rebalance_evacuation_stop,
            descr  => "Stop evacuation on the node"}).

-export([status/2,
         availability_check/2,
         global_status/2,

         rebalance_evacuation_start/2,
         rebalance_evacuation_stop/2,
         rebalance_start/2,
         rebalance_stop/2
        ]).

status(_Bindings, _Params) ->
    case emqx_node_rebalance_status:local_status() of
        disabled ->
            {ok, #{status => disabled}};
        {rebalance, Stats} ->
            {ok, format_status(rebalance, Stats)};
        {evacuation, Stats} ->
            {ok, format_status(evacuation, Stats)}
    end.

global_status(_Bindings, _Params) ->
    #{evacuations := Evacuations,
      rebalances := Rebalances} = emqx_node_rebalance_status:global_status(),
      {ok, #{evacuations => maps:from_list(Evacuations),
             rebalances => maps:from_list(Rebalances)}}.

availability_check(_Bindings, _Params) ->
    case emqx_eviction_agent:status() of
        disabled ->
            {200, #{}};
        {enabled, _Stats} ->
            {503, #{}}
    end.

rebalance_evacuation_start(#{node := NodeBin}, Params) ->
    validated(
      fun() ->
              {Node, Opts} = validate_evacuation(NodeBin, params(Params)),
              rpc(Node, emqx_node_rebalance_evacuation, start, [Opts])
      end).

rebalance_evacuation_stop(#{node := NodeBin}, _Params) ->
    validated(
      fun() ->
              Node = parse_node(NodeBin),
              rpc(Node, emqx_node_rebalance_evacuation, stop, [])
      end).

rebalance_start(#{node := NodeBin}, Params) ->
    validated(
      fun() ->
              {Node, Opts} = validate_rebalance(NodeBin, params(Params)),
              rpc(Node, emqx_node_rebalance, start, [Opts])
      end).

rebalance_stop(#{node := NodeBin}, _Params) ->
    validated(
      fun() ->
              Node = parse_node(NodeBin),
              rpc(Node, emqx_node_rebalance, stop, [])
      end).

rpc(Node, M, F, A) ->
    case rpc:call(Node, M, F, A) of
        ok -> return({ok, []});
        {error, Error} ->
            return({error, 400, io_lib:format("~p", [Error])});
        {badrpc, _} ->
            return({error, 400, io_lib:format("Error communicating with node ~p", [Node])});
        Unknown ->
            return({error, 400, io_lib:format("Unrecognized rpc result from node ~p: ~p",
                                              [Node, Unknown])})
    end.

format_status(Process, Stats) ->
    Stats#{process => Process, status => enabled}.

validate_evacuation(Node, Params) ->
    NodeToEvacuate = parse_node(Node),
    OptList = lists:map(
                fun validate_evacuation_param/1,
                Params),
    {NodeToEvacuate, maps:from_list(OptList)}.

validate_rebalance(Node, Params) ->
    CoordinatorNode = parse_node(Node),
    OptList = lists:map(
                fun validate_rebalance_param/1,
                Params),
    {CoordinatorNode, maps:from_list(OptList)}.

validate_evacuation_param({<<"conn_evict_rate">>, Value}) ->
    validate_pos_int(conn_evict_rate, Value);
validate_evacuation_param({<<"sess_evict_rate">>, Value}) ->
    validate_pos_int(sess_evict_rate, Value);
validate_evacuation_param({<<"redirect_to">>, Value}) ->
    validate_binary(server_reference, Value);
validate_evacuation_param({<<"wait_takeover">>, Value}) ->
    validate_pos_int(wait_takeover, Value);
validate_evacuation_param({<<"migrate_to">>, Value}) ->
    validate_nodes(migrate_to, Value);
validate_evacuation_param(Value) ->
    validation_error(io_lib:format("Unknown evacuation param: ~p", [Value])).

validate_rebalance_param({<<"wait_health_check">>, Value}) ->
    validate_pos_int(wait_health_check, Value);
validate_rebalance_param({<<"conn_evict_rate">>, Value}) ->
    validate_pos_int(conn_evict_rate, Value);
validate_rebalance_param({<<"sess_evict_rate">>, Value}) ->
    validate_pos_int(sess_evict_rate, Value);
validate_rebalance_param({<<"abs_conn_threshold">>, Value}) ->
    validate_pos_int(abs_conn_threshold, Value);
validate_rebalance_param({<<"rel_conn_threshold">>, Value}) ->
    validate_fraction(rel_conn_threshold, Value);
validate_rebalance_param({<<"abs_sess_threshold">>, Value}) ->
    validate_pos_int(abs_sess_threshold, Value);
validate_rebalance_param({<<"rel_sess_threshold">>, Value}) ->
    validate_fraction(rel_sess_threshold, Value);
validate_rebalance_param({<<"wait_takeover">>, Value}) ->
    validate_pos_int(wait_takeover, Value);
validate_rebalance_param({<<"nodes">>, Value}) ->
    validate_nodes(nodes, Value);
validate_rebalance_param(Value) ->
    validation_error(io_lib:format("Unknown rebalance param: ~p", [Value])).

validate_binary(Name, Value) when is_binary(Value) ->
    {Name, Value};
validate_binary(Name, _Value) ->
    validation_error("invalid string in " ++ atom_to_list(Name)).

validate_pos_int(Name, Value) ->
    case is_integer(Value) andalso Value > 0 of
        true -> {Name, Value};
        false ->
            validation_error("invalid " ++  atom_to_list(Name) ++ " value")
    end.

validate_fraction(Name, Value) ->
    case is_number(Value) andalso Value > 1.0 of
        true -> {Name, Value};
        false ->
            validation_error("invalid " ++  atom_to_list(Name) ++ " value")
    end.

validate_nodes(Name, NodeList) when is_list(NodeList) ->
    Nodes = lists:map(
              fun parse_node/1,
              NodeList),
    case emqx_node_rebalance_evacuation:available_nodes(Nodes) of
        [] ->
            validation_error(io_lib:format("no available nodes list in ~p: ~p", [Name, Nodes]));
        Nodes ->
            {Name, Nodes};
        OtherNodes ->
            validation_error(
              io_lib:format("unavailable nodes in ~p: ~p",
                            [Name, Nodes -- OtherNodes]))
    end;
validate_nodes(Name, Nodes) ->
    validation_error(io_lib:format("invalid node list in ~p: ~p", [Name, Nodes])).

validated(Fun) ->
    try
        Fun()
    catch throw:{validation_error, Error} ->
              return({error, 400, iolist_to_binary(Error)})
    end.

validation_error(Error) ->
    throw({validation_error, Error}).

parse_node(Bin) when is_binary(Bin) ->
    try
        binary_to_existing_atom(Bin)
    catch
        error:badarg ->
            validation_error("invalid node: " ++ [Bin])
    end.

params([{}]) -> [];
params(Params) -> Params.
