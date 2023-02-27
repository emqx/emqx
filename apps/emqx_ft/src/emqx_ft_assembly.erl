%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_ft_assembly).

-export([new/1]).
-export([append/3]).
-export([update/1]).

-export([status/1]).
-export([filemeta/1]).
-export([coverage/1]).
-export([properties/1]).

-export_type([t/0]).

-type filemeta() :: emqx_ft:filemeta().
-type filefrag() :: emqx_ft_storage_fs:filefrag().
-type filefrag(T) :: emqx_ft_storage_fs:filefrag(T).
-type segmentinfo() :: emqx_ft_storage_fs:segmentinfo().

-record(asm, {
    status :: status(),
    coverage :: coverage() | undefined,
    properties :: properties() | undefined,
    meta :: orddict:orddict(
        filemeta(),
        {node(), filefrag({filemeta, filemeta()})}
    ),
    segs :: gb_trees:tree(
        {emqx_ft:offset(), _Locality, _MEnd, node()},
        [filefrag({segment, segmentinfo()})]
    ),
    size :: emqx_ft:bytes()
}).

-type status() ::
    {incomplete, {missing, _}}
    | complete
    | {error, {inconsistent, _}}.

-type coverage() :: [{node(), filefrag({segment, segmentinfo()})}].

-type properties() :: #{
    %% Node where "most" of the segments are located.
    dominant => node()
}.

-opaque t() :: #asm{}.

-spec new(emqx_ft:bytes()) -> t().
new(Size) ->
    #asm{
        status = {incomplete, {missing, filemeta}},
        meta = orddict:new(),
        segs = gb_trees:empty(),
        size = Size
    }.

-spec append(t(), node(), filefrag() | [filefrag()]) -> t().
append(Asm, Node, Fragments) when is_list(Fragments) ->
    lists:foldl(fun(F, AsmIn) -> append(AsmIn, Node, F) end, Asm, Fragments);
append(Asm, Node, Fragment = #{fragment := {filemeta, _}}) ->
    append_filemeta(Asm, Node, Fragment);
append(Asm, Node, Segment = #{fragment := {segment, _}}) ->
    append_segmentinfo(Asm, Node, Segment).

-spec update(t()) -> t().
update(Asm) ->
    case status(meta, Asm) of
        {complete, _Meta} ->
            case status(coverage, Asm) of
                {complete, Coverage, Props} ->
                    Asm#asm{
                        status = complete,
                        coverage = Coverage,
                        properties = Props
                    };
                Status ->
                    Asm#asm{status = Status}
            end;
        Status ->
            Asm#asm{status = Status}
    end.

-spec status(t()) -> status().
status(#asm{status = Status}) ->
    Status.

-spec filemeta(t()) -> filemeta().
filemeta(Asm) ->
    case status(meta, Asm) of
        {complete, Meta} -> Meta;
        _Other -> undefined
    end.

-spec coverage(t()) -> coverage() | undefined.
coverage(#asm{coverage = Coverage}) ->
    Coverage.

properties(#asm{properties = Properties}) ->
    Properties.

status(meta, #asm{meta = Meta}) ->
    status(meta, orddict:to_list(Meta));
status(meta, [{Meta, {_Node, _Frag}}]) ->
    {complete, Meta};
status(meta, []) ->
    {incomplete, {missing, filemeta}};
status(meta, [_M1, _M2 | _] = Metas) ->
    {error, {inconsistent, [Frag#{node => Node} || {_, {Node, Frag}} <- Metas]}};
status(coverage, #asm{segs = Segments, size = Size}) ->
    case coverage(Segments, Size) of
        Coverage when is_list(Coverage) ->
            {complete, Coverage, #{
                dominant => dominant(Coverage)
            }};
        Missing = {missing, _} ->
            {incomplete, Missing}
    end.

append_filemeta(Asm, Node, Fragment = #{fragment := {filemeta, Meta}}) ->
    Asm#asm{
        meta = orddict:store(Meta, {Node, Fragment}, Asm#asm.meta)
    }.

append_segmentinfo(Asm, _Node, #{fragment := {segment, #{size := 0}}}) ->
    % NOTE
    % Empty segments are valid but meaningless for coverage.
    Asm;
append_segmentinfo(Asm, Node, Fragment = #{fragment := {segment, Info}}) ->
    Offset = maps:get(offset, Info),
    Size = maps:get(size, Info),
    End = Offset + Size,
    Segs = add_edge(Asm#asm.segs, Offset, End, locality(Node) * Size, {Node, Fragment}),
    Asm#asm{
        % TODO
        % In theory it's possible to have two segments with same offset + size on
        % different nodes but with differing content. We'd need a checksum to
        % be able to disambiguate them though.
        segs = Segs
    }.

coverage(Segs, Size) ->
    find_shortest_path(Segs, 0, Size).

find_shortest_path(G1, From, To) ->
    % NOTE
    % This is a Dijkstra shortest path algorithm implemented on top of `gb_trees`.
    % It is one-way right now, for simplicity sake.
    G2 = set_cost(G1, From, 0, []),
    case find_shortest_path(G2, From, 0, To) of
        {found, G3} ->
            construct_path(G3, From, To, []);
        {error, Last} ->
            % NOTE: this is actually just an estimation of what is missing.
            {missing, {segment, Last, emqx_maybe:define(find_successor(G2, Last), To)}}
    end.

find_shortest_path(G1, Node, Cost, Target) ->
    Edges = get_edges(G1, Node),
    G2 = update_neighbours(G1, Node, Cost, Edges),
    case take_queued(G2) of
        {Target, _NextCost, G3} ->
            {found, G3};
        {Next, NextCost, G3} ->
            find_shortest_path(G3, Next, NextCost, Target);
        none ->
            {error, Node}
    end.

construct_path(_G, From, From, Acc) ->
    Acc;
construct_path(G, From, To, Acc) ->
    {Prev, Label} = get_label(G, To),
    construct_path(G, From, Prev, [Label | Acc]).

update_neighbours(G1, Node, NodeCost, Edges) ->
    lists:foldl(
        fun({Neighbour, Weight, Label}, GAcc) ->
            case is_visited(GAcc, Neighbour) of
                false ->
                    NeighCost = NodeCost + Weight,
                    CurrentCost = get_cost(GAcc, Neighbour),
                    case NeighCost < CurrentCost of
                        true ->
                            set_cost(GAcc, Neighbour, NeighCost, {Node, Label});
                        false ->
                            GAcc
                    end;
                true ->
                    GAcc
            end
        end,
        G1,
        Edges
    ).

add_edge(G, Node, ToNode, WeightIn, EdgeLabel) ->
    Edges = tree_lookup({Node}, G, []),
    case lists:keyfind(ToNode, 1, Edges) of
        {ToNode, Weight, _} when Weight =< WeightIn ->
            % NOTE
            % Discarding any edges with higher weight here. This is fine as long as we
            % optimize for locality.
            G;
        _ ->
            EdgesNext = lists:keystore(ToNode, 1, Edges, {ToNode, WeightIn, EdgeLabel}),
            tree_update({Node}, EdgesNext, G)
    end.

get_edges(G, Node) ->
    tree_lookup({Node}, G, []).

get_cost(G, Node) ->
    tree_lookup({Node, cost}, G, inf).

get_label(G, Node) ->
    gb_trees:get({Node, label}, G).

set_cost(G1, Node, Cost, Label) ->
    G3 =
        case tree_lookup({Node, cost}, G1, inf) of
            CostWas when CostWas /= inf ->
                {true, G2} = gb_trees:take({queued, CostWas, Node}, G1),
                tree_update({queued, Cost, Node}, true, G2);
            inf ->
                tree_update({queued, Cost, Node}, true, G1)
        end,
    G4 = tree_update({Node, cost}, Cost, G3),
    G5 = tree_update({Node, label}, Label, G4),
    G5.

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
        Cost ->
            not tree_lookup({queued, Cost, Node}, G, false)
    end.

find_successor(G, Node) ->
    case gb_trees:next(gb_trees:iterator_from({Node}, G)) of
        {{Node}, _, It} ->
            case gb_trees:next(It) of
                {{Successor}, _, _} ->
                    Successor;
                _ ->
                    undefined
            end;
        {{Successor}, _, _} ->
            Successor;
        _ ->
            undefined
    end.

tree_lookup(Index, Tree, Default) ->
    case gb_trees:lookup(Index, Tree) of
        {value, V} ->
            V;
        none ->
            Default
    end.

tree_update(Index, Value, Tree) ->
    case gb_trees:take_any(Index, Tree) of
        {_, TreeNext} ->
            gb_trees:insert(Index, Value, TreeNext);
        error ->
            gb_trees:insert(Index, Value, Tree)
    end.

dominant(Coverage) ->
    % TODO: needs improvement, better defined _dominance_, maybe some score
    Freqs = frequencies(fun({Node, Segment}) -> {Node, segsize(Segment)} end, Coverage),
    maxfreq(Freqs, node()).

frequencies(Fun, List) ->
    lists:foldl(
        fun(E, Acc) ->
            {K, N} = Fun(E),
            maps:update_with(K, fun(M) -> M + N end, N, Acc)
        end,
        #{},
        List
    ).

maxfreq(Freqs, Init) ->
    {_, Max} = maps:fold(
        fun
            (F, N, {M, _MF}) when N > M -> {N, F};
            (_F, _N, {M, MF}) -> {M, MF}
        end,
        {0, Init},
        Freqs
    ),
    Max.

locality(Node) when Node =:= node() ->
    % NOTE
    % This should prioritize locally available segments over those on remote nodes.
    0;
locality(_RemoteNode) ->
    1.

segsize(#{fragment := {segment, Info}}) ->
    maps:get(size, Info).

-ifdef(TEST).

-include_lib("eunit/include/eunit.hrl").

incomplete_new_test() ->
    ?assertEqual(
        {incomplete, {missing, filemeta}},
        status(update(new(42)))
    ).

incomplete_test() ->
    ?assertEqual(
        {incomplete, {missing, filemeta}},
        status(
            update(
                append(new(142), node(), [
                    segment(p1, 0, 42),
                    segment(p1, 42, 100)
                ])
            )
        )
    ).

consistent_test() ->
    Asm1 = append(new(42), n1, [filemeta(m1, "blarg")]),
    Asm2 = append(Asm1, n2, [segment(s2, 0, 42)]),
    Asm3 = append(Asm2, n3, [filemeta(m3, "blarg")]),
    ?assertMatch({complete, _}, status(meta, Asm3)).

inconsistent_test() ->
    Asm1 = append(new(42), node(), [segment(s1, 0, 42)]),
    Asm2 = append(Asm1, n1, [filemeta(m1, "blarg")]),
    Asm3 = append(Asm2, n2, [segment(s2, 0, 42), filemeta(m1, "blorg")]),
    Asm4 = append(Asm3, n3, [filemeta(m3, "blarg")]),
    ?assertMatch(
        {error,
            {inconsistent, [
                % blarg < blorg
                #{node := n3, path := m3, fragment := {filemeta, #{name := "blarg"}}},
                #{node := n2, path := m1, fragment := {filemeta, #{name := "blorg"}}}
            ]}},
        status(meta, Asm4)
    ).

simple_coverage_test() ->
    Node = node(),
    Segs = [
        {node42, segment(n1, 20, 30)},
        {Node, segment(n2, 0, 10)},
        {Node, segment(n3, 50, 50)},
        {Node, segment(n4, 10, 10)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n2}},
                {Node, #{path := n4}},
                {node42, #{path := n1}},
                {Node, #{path := n3}}
            ],
            #{dominant := Node}},
        status(coverage, Asm)
    ).

redundant_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 20)},
        {node1, segment(n2, 0, 10)},
        {Node, segment(n3, 20, 40)},
        {node2, segment(n4, 10, 10)},
        {node2, segment(n5, 50, 20)},
        {node3, segment(n6, 20, 20)},
        {Node, segment(n7, 50, 10)},
        {node1, segment(n8, 40, 10)}
    ],
    Asm = append_many(new(70), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n1}},
                {node3, #{path := n6}},
                {node1, #{path := n8}},
                {node2, #{path := n5}}
            ],
            #{dominant := _}},
        status(coverage, Asm)
    ).

redundant_coverage_prefer_local_test() ->
    Node = node(),
    Segs = [
        {node1, segment(n1, 0, 20)},
        {Node, segment(n2, 0, 10)},
        {Node, segment(n3, 10, 10)},
        {node2, segment(n4, 20, 20)},
        {Node, segment(n5, 30, 10)},
        {Node, segment(n6, 20, 10)}
    ],
    Asm = append_many(new(40), Segs),
    ?assertMatch(
        {complete,
            [
                {Node, #{path := n2}},
                {Node, #{path := n3}},
                {Node, #{path := n6}},
                {Node, #{path := n5}}
            ],
            #{dominant := Node}},
        status(coverage, Asm)
    ).

missing_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 10)},
        {node1, segment(n3, 10, 20)},
        {Node, segment(n2, 0, 20)},
        {node2, segment(n4, 50, 50)},
        {Node, segment(n5, 40, 60)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertEqual(
        {incomplete, {missing, {segment, 30, 40}}},
        status(coverage, Asm)
    ).

missing_end_coverage_test() ->
    Node = node(),
    Segs = [
        {Node, segment(n1, 0, 15)},
        {node1, segment(n3, 10, 10)}
    ],
    Asm = append_many(new(20), Segs),
    ?assertEqual(
        {incomplete, {missing, {segment, 15, 20}}},
        status(coverage, Asm)
    ).

missing_coverage_with_redudancy_test() ->
    Segs = [
        {node(), segment(n1, 0, 10)},
        {node(), segment(n2, 0, 20)},
        {node42, segment(n3, 10, 20)},
        {node43, segment(n4, 10, 50)},
        {node(), segment(n5, 40, 60)}
    ],
    Asm = append_many(new(100), Segs),
    ?assertEqual(
        % {incomplete, {missing, {segment, 50, 60}}}, ???
        {incomplete, {missing, {segment, 60, 100}}},
        status(coverage, Asm)
    ).

append_many(Asm, List) ->
    lists:foldl(
        fun({Node, Frag}, Acc) -> append(Acc, Node, Frag) end,
        Asm,
        List
    ).

filemeta(Path, Name) ->
    #{
        path => Path,
        fragment =>
            {filemeta, #{
                name => Name
            }}
    }.

segment(Path, Offset, Size) ->
    #{
        path => Path,
        fragment =>
            {segment, #{
                offset => Offset,
                size => Size
            }}
    }.

-endif.
