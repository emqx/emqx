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

%% Weighted directed graph.
%%
%% Purely functional, built on top of a single `gb_tree`.
%% Weights are currently assumed to be non-negative numbers, hovewer
%% presumably anything that is ≥ 0 should work (but won't typecheck 🥲).

-module(emqx_wdgraph).

-export([new/0]).
-export([insert_edge/5]).
-export([find_edge/3]).
-export([get_edges/2]).

-export([fold/3]).

-export([find_shortest_path/3]).

-export_type([t/0]).
-export_type([t/2]).
-export_type([weight/0]).

-type gnode() :: term().
-type weight() :: _NonNegative :: number().
-type label() :: term().

-opaque t() :: t(gnode(), label()).
-opaque t(Node, Label) :: gb_trees:tree({Node}, [{Node, weight(), Label}]).

%%

-spec new() -> t(_, _).
new() ->
    gb_trees:empty().

%% Add an edge.
%% Nodes are not expected to exist beforehand, and created lazily.
%% There could be only one edge between each pair of nodes, this function
%% replaces any existing edge in the graph.
-spec insert_edge(Node, Node, weight(), Label, t(Node, Label)) -> t(Node, Label).
insert_edge(From, To, Weight, EdgeLabel, G) ->
    Edges = tree_lookup({From}, G, []),
    EdgesNext = lists:keystore(To, 1, Edges, {To, Weight, EdgeLabel}),
    tree_update({From}, EdgesNext, G).

%% Find exising edge between two nodes, if any.
-spec find_edge(Node, Node, t(Node, Label)) -> {weight(), Label} | false.
find_edge(From, To, G) ->
    Edges = tree_lookup({From}, G, []),
    case lists:keyfind(To, 1, Edges) of
        {To, Weight, Label} ->
            {Weight, Label};
        false ->
            false
    end.

%% Get all edges from the given node.
-spec get_edges(Node, t(Node, Label)) -> [{Node, weight(), Label}].
get_edges(Node, G) ->
    tree_lookup({Node}, G, []).

-spec fold(FoldFun, Acc, t(Node, Label)) -> Acc when
    FoldFun :: fun((Node, _Edge :: {Node, weight(), Label}, Acc) -> Acc).
fold(FoldFun, Acc, G) ->
    fold_iterator(FoldFun, Acc, gb_trees:iterator(G)).

fold_iterator(FoldFun, AccIn, It) ->
    case gb_trees:next(It) of
        {{Node}, Edges = [_ | _], ItNext} ->
            AccNext = lists:foldl(
                fun(Edge = {_To, _Weight, _Label}, Acc) ->
                    FoldFun(Node, Edge, Acc)
                end,
                AccIn,
                Edges
            ),
            fold_iterator(FoldFun, AccNext, ItNext);
        none ->
            AccIn
    end.

% Find the shortest path between two nodes, if any. If the path exists, return list
% of edge labels along that path.
% This is a Dijkstra shortest path algorithm. It is one-way right now, for
% simplicity sake.
-spec find_shortest_path(Node, Node, t(Node, Label)) -> [Label] | {false, _StoppedAt :: Node}.
find_shortest_path(From, To, G1) ->
    % NOTE
    % If `From` and `To` are the same node, then path is `[]` even if this
    % node does not exist in the graph.
    G2 = set_cost(From, 0, [], G1),
    case find_shortest_path(From, 0, To, G2) of
        {true, G3} ->
            construct_path(From, To, [], G3);
        {false, Last} ->
            {false, Last}
    end.

find_shortest_path(Node, Cost, Target, G1) ->
    Edges = get_edges(Node, G1),
    G2 = update_neighbours(Node, Cost, Edges, G1),
    case take_queued(G2) of
        {Target, _NextCost, G3} ->
            {true, G3};
        {Next, NextCost, G3} ->
            find_shortest_path(Next, NextCost, Target, G3);
        none ->
            {false, Node}
    end.

construct_path(From, From, Acc, _) ->
    Acc;
construct_path(From, To, Acc, G) ->
    {Prev, Label} = get_label(To, G),
    construct_path(From, Prev, [Label | Acc], G).

update_neighbours(Node, NodeCost, Edges, G1) ->
    lists:foldl(
        fun(Edge, GAcc) -> update_neighbour(Node, NodeCost, Edge, GAcc) end,
        G1,
        Edges
    ).

update_neighbour(Node, NodeCost, {Neighbour, Weight, Label}, G) ->
    case is_visited(G, Neighbour) of
        false ->
            CurrentCost = get_cost(Neighbour, G),
            case NodeCost + Weight of
                NeighCost when NeighCost < CurrentCost ->
                    set_cost(Neighbour, NeighCost, {Node, Label}, G);
                _ ->
                    G
            end;
        true ->
            G
    end.

get_cost(Node, G) ->
    case tree_lookup({Node, cost}, G, inf) of
        {Cost, _Label} ->
            Cost;
        inf ->
            inf
    end.

get_label(Node, G) ->
    {_Cost, Label} = gb_trees:get({Node, cost}, G),
    Label.

set_cost(Node, Cost, Label, G1) ->
    G3 =
        case tree_lookup({Node, cost}, G1, inf) of
            {CostWas, _Label} ->
                {true, G2} = gb_trees:take({queued, CostWas, Node}, G1),
                gb_trees:insert({queued, Cost, Node}, true, G2);
            inf ->
                gb_trees:insert({queued, Cost, Node}, true, G1)
        end,
    G4 = tree_update({Node, cost}, {Cost, Label}, G3),
    G4.

take_queued(G1) ->
    It = gb_trees:iterator_from({queued, 0, 0}, G1),
    case gb_trees:next(It) of
        {{queued, Cost, Node} = Index, true, _It} ->
            {Node, Cost, gb_trees:delete(Index, G1)};
        _ ->
            none
    end.

is_visited(G, Node) ->
    case tree_lookup({Node, cost}, G, inf) of
        inf ->
            false;
        {Cost, _Label} ->
            not tree_lookup({queued, Cost, Node}, G, false)
    end.

tree_lookup(Index, Tree, Default) ->
    case gb_trees:lookup(Index, Tree) of
        {value, V} ->
            V;
        none ->
            Default
    end.

tree_update(Index, Value, Tree) ->
    case gb_trees:is_defined(Index, Tree) of
        true ->
            gb_trees:update(Index, Value, Tree);
        false ->
            gb_trees:insert(Index, Value, Tree)
    end.
