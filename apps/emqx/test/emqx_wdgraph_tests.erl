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

-module(emqx_wdgraph_tests).

-include_lib("eunit/include/eunit.hrl").

empty_test_() ->
    G = emqx_wdgraph:new(),
    [
        ?_assertEqual([], emqx_wdgraph:get_edges(foo, G)),
        ?_assertEqual(false, emqx_wdgraph:find_edge(foo, bar, G))
    ].

edges_nodes_test_() ->
    G1 = emqx_wdgraph:new(),
    G2 = emqx_wdgraph:insert_edge(foo, bar, 42, "fancy", G1),
    G3 = emqx_wdgraph:insert_edge(bar, baz, 1, "cheapest", G2),
    G4 = emqx_wdgraph:insert_edge(bar, foo, 0, "free", G3),
    G5 = emqx_wdgraph:insert_edge(foo, bar, 100, "luxury", G4),
    [
        ?_assertEqual({42, "fancy"}, emqx_wdgraph:find_edge(foo, bar, G2)),
        ?_assertEqual({100, "luxury"}, emqx_wdgraph:find_edge(foo, bar, G5)),
        ?_assertEqual([{bar, 100, "luxury"}], emqx_wdgraph:get_edges(foo, G5)),

        ?_assertEqual({1, "cheapest"}, emqx_wdgraph:find_edge(bar, baz, G5)),
        ?_assertEqual([{baz, 1, "cheapest"}, {foo, 0, "free"}], emqx_wdgraph:get_edges(bar, G5))
    ].

fold_test_() ->
    G1 = emqx_wdgraph:new(),
    G2 = emqx_wdgraph:insert_edge(foo, bar, 42, "fancy", G1),
    G3 = emqx_wdgraph:insert_edge(bar, baz, 1, "cheapest", G2),
    G4 = emqx_wdgraph:insert_edge(bar, foo, 0, "free", G3),
    G5 = emqx_wdgraph:insert_edge(foo, bar, 100, "luxury", G4),
    [
        ?_assertEqual(
            % 100 + 0 + 1
            101,
            emqx_wdgraph:fold(fun(_From, {_, Weight, _}, Acc) -> Weight + Acc end, 0, G5)
        ),
        ?_assertEqual(
            [bar, baz, foo],
            lists:usort(
                emqx_wdgraph:fold(fun(From, {To, _, _}, Acc) -> [From, To | Acc] end, [], G5)
            )
        )
    ].

nonexistent_nodes_path_test_() ->
    G1 = emqx_wdgraph:new(),
    G2 = emqx_wdgraph:insert_edge(foo, bar, 42, "fancy", G1),
    G3 = emqx_wdgraph:insert_edge(bar, baz, 1, "cheapest", G2),
    [
        ?_assertEqual(
            {false, nosuchnode},
            emqx_wdgraph:find_shortest_path(nosuchnode, baz, G3)
        ),
        ?_assertEqual(
            [],
            emqx_wdgraph:find_shortest_path(nosuchnode, nosuchnode, G3)
        )
    ].

nonexistent_path_test_() ->
    G1 = emqx_wdgraph:new(),
    G2 = emqx_wdgraph:insert_edge(foo, bar, 42, "fancy", G1),
    G3 = emqx_wdgraph:insert_edge(baz, boo, 1, "cheapest", G2),
    G4 = emqx_wdgraph:insert_edge(boo, last, 3.5, "change", G3),
    [
        ?_assertEqual(
            {false, last},
            emqx_wdgraph:find_shortest_path(baz, foo, G4)
        ),
        ?_assertEqual(
            {false, bar},
            emqx_wdgraph:find_shortest_path(foo, last, G4)
        )
    ].

shortest_path_test() ->
    G1 = emqx_wdgraph:new(),
    G2 = emqx_wdgraph:insert_edge(foo, bar, 42, "fancy", G1),
    G3 = emqx_wdgraph:insert_edge(bar, baz, 1, "cheapest", G2),
    G4 = emqx_wdgraph:insert_edge(baz, last, 0, "free", G3),
    G5 = emqx_wdgraph:insert_edge(bar, last, 100, "luxury", G4),
    G6 = emqx_wdgraph:insert_edge(bar, foo, 0, "comeback", G5),
    ?assertEqual(
        ["fancy", "cheapest", "free"],
        emqx_wdgraph:find_shortest_path(foo, last, G6)
    ).
