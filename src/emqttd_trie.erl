%%--------------------------------------------------------------------
%% Copyright (c) 2012-2016 Feng Lee <feng@emqtt.io>.
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

%% @doc MQTT Topic Trie:
%% [Trie](http://en.wikipedia.org/wiki/Trie)
%% @end
-module(emqttd_trie).

-include("emqttd_trie.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie API
-export([insert/1, match/1, delete/1, lookup/1]).

%%--------------------------------------------------------------------
%% Mnesia Callbacks
%%--------------------------------------------------------------------

%% @doc Create or Replicate trie tables.
-spec(mnesia(boot | copy) -> ok).
mnesia(boot) ->
    %% Trie Table
    ok = emqttd_mnesia:create_table(trie, [
                {ram_copies, [node()]},
                {record_name, trie},
                {attributes, record_info(fields, trie)}]),
    %% Trie Node Table
    ok = emqttd_mnesia:create_table(trie_node, [
                {ram_copies, [node()]},
                {record_name, trie_node},
                {attributes, record_info(fields, trie_node)}]);

mnesia(copy) ->
    %% Copy Trie Table
    ok = emqttd_mnesia:copy_table(trie),
    %% Copy Trie Node Table
    ok = emqttd_mnesia:copy_table(trie_node).

%%--------------------------------------------------------------------
%% Trie API
%%--------------------------------------------------------------------

%% @doc Insert topic to trie
-spec(insert(Topic :: binary()) -> ok).
insert(Topic) when is_binary(Topic) ->
    case mnesia:read(trie_node, Topic) of
    [#trie_node{topic=Topic}] ->
        ok;
    [TrieNode=#trie_node{topic=undefined}] ->
        mnesia:write(TrieNode#trie_node{topic=Topic});
    [] ->
        %add trie path
        lists:foreach(fun add_path/1, emqttd_topic:triples(Topic)),
        %add last node
        mnesia:write(#trie_node{node_id=Topic, topic=Topic})
    end.

%% @doc Find trie nodes that match topic
-spec(match(Topic :: binary()) -> list(MatchedTopic :: binary())).
match(Topic) when is_binary(Topic) ->
    TrieNodes = match_node(root, emqttd_topic:words(Topic)),
    [Name || #trie_node{topic=Name} <- TrieNodes, Name =/= undefined].

%% @doc Lookup a Trie Node
-spec(lookup(NodeId :: binary()) -> [#trie_node{}]).
lookup(NodeId) ->
    mnesia:read(trie_node, NodeId).

%% @doc Delete topic from trie
-spec(delete(Topic :: binary()) -> ok).
delete(Topic) when is_binary(Topic) ->
    case mnesia:read(trie_node, Topic) of
    [#trie_node{edge_count=0}] -> 
        mnesia:delete({trie_node, Topic}),
        delete_path(lists:reverse(emqttd_topic:triples(Topic)));
    [TrieNode] ->
        mnesia:write(TrieNode#trie_node{topic = undefined});
    [] ->
        ok    
    end.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% @private
%% @doc Add path to trie tree.
add_path({Node, Word, Child}) ->
    Edge = #trie_edge{node_id=Node, word=Word},
    case mnesia:read(trie_node, Node) of
    [TrieNode = #trie_node{edge_count=Count}] ->
        case mnesia:wread({trie, Edge}) of
        [] -> 
            mnesia:write(TrieNode#trie_node{edge_count=Count+1}),
            mnesia:write(#trie{edge=Edge, node_id=Child});
        [_] -> 
            ok
        end;
    [] ->
        mnesia:write(#trie_node{node_id=Node, edge_count=1}),
        mnesia:write(#trie{edge=Edge, node_id=Child})
    end.

%% @private
%% @doc Match node with word or '+'.
match_node(root, [<<"$SYS">>|Words]) ->
    match_node(<<"$SYS">>, Words, []);

match_node(NodeId, Words) ->
    match_node(NodeId, Words, []).

match_node(NodeId, [], ResAcc) ->
    mnesia:read(trie_node, NodeId) ++ 'match_#'(NodeId, ResAcc);

match_node(NodeId, [W|Words], ResAcc) ->
    lists:foldl(fun(WArg, Acc) ->
        case mnesia:read(trie, #trie_edge{node_id=NodeId, word=WArg}) of
        [#trie{node_id=ChildId}] -> match_node(ChildId, Words, Acc);
        [] -> Acc
        end
    end, 'match_#'(NodeId, ResAcc), [W, '+']).

%% @private
%% @doc Match node with '#'.
'match_#'(NodeId, ResAcc) ->
    case mnesia:read(trie, #trie_edge{node_id=NodeId, word = '#'}) of
    [#trie{node_id=ChildId}] ->
        mnesia:read(trie_node, ChildId) ++ ResAcc;
    [] ->
        ResAcc
    end.

%% @private
%% @doc Delete paths from trie tree.
delete_path([]) ->
    ok;
delete_path([{NodeId, Word, _} | RestPath]) ->
    mnesia:delete({trie, #trie_edge{node_id=NodeId, word=Word}}),
    case mnesia:read(trie_node, NodeId) of
    [#trie_node{edge_count=1, topic=undefined}] -> 
        mnesia:delete({trie_node, NodeId}),
        delete_path(RestPath);
    [TrieNode=#trie_node{edge_count=1, topic=_}] -> 
        mnesia:write(TrieNode#trie_node{edge_count=0});
    [TrieNode=#trie_node{edge_count=C}] ->
        mnesia:write(TrieNode#trie_node{edge_count=C-1});
    [] ->
        throw({notfound, NodeId}) 
    end.

