%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(ekka_ring).

-include_lib("mria/include/mria.hrl").

-export([
    find_node/2,
    find_nodes/2,
    find_nodes/3
]).

-ifdef(TEST).
-compile(export_all).
-compile(nowarn_export_all).
-endif.

-type key() :: term().
-type ring() :: list(member()).

-export_type([key/0, ring/0]).

%% trunc(math:pow(2, 32) - 1))
-define(BASE, 4294967295).

-spec find_node(key(), ring()) -> node().
find_node(Key, Ring) ->
    (next_member(phash(Key), Ring))#member.node.

next_member(Hash, Ring = [Head | _]) ->
    next_member(Hash, Head, Ring).

next_member(_Hash, Head, []) ->
    Head;
next_member(Hash, _Head, [M = #member{hash = MHash} | _]) when
    MHash >= Hash
->
    M;
next_member(Hash, Head, [_ | Ring]) ->
    next_member(Hash, Head, Ring).

-spec find_nodes(key(), ring()) -> list(node()).
find_nodes(Key, Ring) ->
    Count = min(quorum(Ring), length(Ring)),
    find_nodes(Key, Count, Ring).

-spec find_nodes(key(), pos_integer(), ring()) -> list(node()).
find_nodes(Key, Count, Ring) ->
    [N || #member{node = N} <- next_members(phash(Key), Count, Ring)].

next_members(_Hash, Count, Ring) when Count >= length(Ring) ->
    Ring;
next_members(Hash, Count, Ring) ->
    {Left, Right} = split_ring(Hash, Ring),
    case length(Right) >= Count of
        true ->
            lists:sublist(Right, 1, Count);
        false ->
            lists:append(Right, lists:sublist(Left, 1, Count - length(Right)))
    end.

split_ring(Hash, Ring) ->
    split_ring(Hash, Ring, [], []).

split_ring(_Hash, [], Left, Right) ->
    {lists:reverse(Left), lists:reverse(Right)};
split_ring(Hash, [M = #member{hash = MHash} | Ring], Left, Right) ->
    case Hash =< MHash of
        true -> split_ring(Hash, Ring, Left, [M | Right]);
        false -> split_ring(Hash, Ring, [M | Left], Right)
    end.

quorum(Ring) ->
    case length(Ring) div 2 + 1 of
        N when N > 3 -> 3;
        N -> N
    end.

phash(Key) -> erlang:phash2(Key, ?BASE).
