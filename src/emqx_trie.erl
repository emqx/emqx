%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_trie).

-include("emqx.hrl").

%% Mnesia bootstrap
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie API
-export([insert/1, match/1, lookup/1, delete/1]).

%% Tables
-define(TRIE, emqx_trie).
-define(TRIE_NODE, emqx_trie_node).

%%--------------------------------------------------------------------
%% Mnesia Bootstrap
%%--------------------------------------------------------------------

%% @doc Create or replicate trie tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Trie Table
    ok = ekka_mnesia:create_table(?TRIE, [
                {ram_copies, [node()]},
                {record_name, trie},
                {attributes, record_info(fields, trie)}]),
    %% Trie Node Table
    ok = ekka_mnesia:create_table(?TRIE_NODE, [
                {ram_copies, [node()]},
                {record_name, trie_node},
                {attributes, record_info(fields, trie_node)}]);

mnesia(copy) ->
    %% Copy Trie Table
    ok = ekka_mnesia:copy_table(?TRIE),
    %% Copy Trie Node Table
    ok = ekka_mnesia:copy_table(?TRIE_NODE).

%%--------------------------------------------------------------------
%% Trie API
%%--------------------------------------------------------------------

%% @doc Insert a topic into the trie
-spec(insert(Topic :: topic()) -> ok).
insert(Topic) when is_binary(Topic) ->
    case mnesia:read(?TRIE_NODE, Topic) of
        [#trie_node{topic = Topic}] ->
            ok;
        [TrieNode = #trie_node{topic = undefined}] ->
            write_trie_node(TrieNode#trie_node{topic = Topic});
        [] ->
            %% Add trie path
            lists:foreach(fun add_path/1, emqx_topic:triples(Topic)),
            %% Add last node
            write_trie_node(#trie_node{node_id = Topic, topic = Topic})
    end.

%% @doc Find trie nodes that match the topic
-spec(match(Topic :: topic()) -> list(MatchedTopic :: topic())).
match(Topic) when is_binary(Topic) ->
    TrieNodes = match_node(root, emqx_topic:words(Topic)),
    [Name || #trie_node{topic = Name} <- TrieNodes, Name =/= undefined].

%% @doc Lookup a trie node
-spec(lookup(NodeId :: binary()) -> [#trie_node{}]).
lookup(NodeId) ->
    mnesia:read(?TRIE_NODE, NodeId).

%% @doc Delete a topic from the trie
-spec(delete(Topic :: topic()) -> ok).
delete(Topic) when is_binary(Topic) ->
    case mnesia:read(?TRIE_NODE, Topic) of
        [#trie_node{edge_count = 0}] ->
            mnesia:delete({?TRIE_NODE, Topic}),
            delete_path(lists:reverse(emqx_topic:triples(Topic)));
        [TrieNode] ->
            write_trie_node(TrieNode#trie_node{topic = undefined});
        [] ->
            ok
    end.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

%% @private
%% @doc Add a path to the trie.
add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id = Node, word = Word},
    case mnesia:read(?TRIE_NODE, Node) of
        [TrieNode = #trie_node{edge_count = Count}] ->
            case mnesia:wread({?TRIE, Edge}) of
                [] ->
                    write_trie_node(TrieNode#trie_node{edge_count = Count+1}),
                    write_trie(#trie{edge = Edge, node_id = Child});
                [_] ->
                    ok
            end;
        [] ->
            write_trie_node(#trie_node{node_id = Node, edge_count = 1}),
            write_trie(#trie{edge = Edge, node_id = Child})
    end.

%% @private
%% @doc Match node with word or '+'.
match_node(root, [<<"$SYS">>|Words]) ->
    match_node(<<"$SYS">>, Words, []);

match_node(NodeId, Words) ->
    match_node(NodeId, Words, []).

match_node(NodeId, [], ResAcc) ->
    mnesia:read(?TRIE_NODE, NodeId) ++ 'match_#'(NodeId, ResAcc);

match_node(NodeId, [W|Words], ResAcc) ->
    lists:foldl(fun(WArg, Acc) ->
        case mnesia:read(?TRIE, #trie_edge{node_id = NodeId, word = WArg}) of
            [#trie{node_id = ChildId}] -> match_node(ChildId, Words, Acc);
            [] -> Acc
        end
    end, 'match_#'(NodeId, ResAcc), [W, '+']).

%% @private
%% @doc Match node with '#'.
'match_#'(NodeId, ResAcc) ->
    case mnesia:read(?TRIE, #trie_edge{node_id = NodeId, word = '#'}) of
        [#trie{node_id = ChildId}] ->
            mnesia:read(?TRIE_NODE, ChildId) ++ ResAcc;
        [] ->
            ResAcc
    end.

%% @private
%% @doc Delete paths from the trie.
delete_path([]) ->
    ok;
delete_path([{NodeId, Word, _} | RestPath]) ->
    mnesia:delete({?TRIE, #trie_edge{node_id = NodeId, word = Word}}),
    case mnesia:read(?TRIE_NODE, NodeId) of
        [#trie_node{edge_count = 1, topic = undefined}] ->
            mnesia:delete({?TRIE_NODE, NodeId}),
            delete_path(RestPath);
        [TrieNode = #trie_node{edge_count = 1, topic = _}] ->
            write_trie_node(TrieNode#trie_node{edge_count = 0});
        [TrieNode = #trie_node{edge_count = C}] ->
            write_trie_node(TrieNode#trie_node{edge_count = C-1});
        [] ->
            mnesia:abort({node_not_found, NodeId})
    end.

write_trie(Trie) ->
    mnesia:write(?TRIE, Trie, write).

write_trie_node(TrieNode) ->
    mnesia:write(?TRIE_NODE, TrieNode, write).

