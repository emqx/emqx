%%--------------------------------------------------------------------
%% Copyright (c) 2017-2021 EMQ Technologies Co., Ltd. All Rights Reserved.
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

%% @doc wrap gen_rpc?
-module(emqx_rpc).

-export([ call/4
        , call/5
        , cast/4
        , cast/5
        , multicall/4
        , multicall/5
        ]).

-compile({inline,
          [ rpc_node/1
          , rpc_nodes/1
          ]}).

-define(RPC, gen_rpc).

-define(DefaultClientNum, 1).

call(Node, Mod, Fun, Args) ->
    filter_result(?RPC:call(rpc_node(Node), Mod, Fun, Args)).

call(Key, Node, Mod, Fun, Args) ->
    filter_result(?RPC:call(rpc_node({Key, Node}), Mod, Fun, Args)).

multicall(Nodes, Mod, Fun, Args) ->
    filter_result(?RPC:multicall(rpc_nodes(Nodes), Mod, Fun, Args)).

multicall(Key, Nodes, Mod, Fun, Args) ->
    filter_result(?RPC:multicall(rpc_nodes([{Key, Node} || Node <- Nodes]), Mod, Fun, Args)).

cast(Node, Mod, Fun, Args) ->
    filter_result(?RPC:cast(rpc_node(Node), Mod, Fun, Args)).

cast(Key, Node, Mod, Fun, Args) ->
    filter_result(?RPC:cast(rpc_node({Key, Node}), Mod, Fun, Args)).

rpc_node(Node) when is_atom(Node) ->
    {Node, rand:uniform(max_client_num())};
rpc_node({Key, Node}) when is_atom(Node) ->
    {Node, erlang:phash2(Key, max_client_num()) + 1}.

rpc_nodes(Nodes) ->
    rpc_nodes(Nodes, []).

rpc_nodes([], Acc) ->
    Acc;
rpc_nodes([Node | Nodes], Acc) ->
    rpc_nodes(Nodes, [rpc_node(Node) | Acc]).

filter_result({Error, Reason})
  when Error =:= badrpc; Error =:= badtcp ->
    {badrpc, Reason};
filter_result(Delivery) ->
    Delivery.

max_client_num() ->
    emqx_config:get([gen_rpc, tcp_client_num]).
