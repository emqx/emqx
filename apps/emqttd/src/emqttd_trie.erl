%%%-----------------------------------------------------------------------------
%%% @Copyright (C) 2012-2015, Feng Lee <feng@emqtt.io>
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc
%%% MQTT Topic Trie Tree.
%%%
%%% [Trie](http://en.wikipedia.org/wiki/Trie)
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_trie).

-author('feng@emqtt.io').

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% Trie API
-export([insert/1, find/1, delete/1]).

-type node_id() :: binary() | atom().

-record(trie_node, {
    node_id        	:: node_id(),
    edge_count = 0  :: non_neg_integer(),
    topic    		:: binary() | undefined
}).

-record(trie_edge, {
    node_id :: node_id(),
    word    :: binary() | atom()
}).

-record(trie, {
    edge    :: #trie_edge{},
    node_id :: node_id()
}).

%%%=============================================================================
%%% Mnesia Callbacks
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Create trie tables.
%%
%% @end
%%------------------------------------------------------------------------------
-spec mnesia(boot | copy) -> ok.
mnesia(boot) ->
    %% trie tree tables
    ok = emqttd_mnesia:create_table(trie, [
                {ram_copies, [node()]},
                {record_name, trie},
                {attributes, record_info(fields, trie)}]),
    ok = emqttd_mnesia:create_table(trie_node, [
                {ram_copies, [node()]},
                {record_name, trie_node},
                {attributes, record_info(fields, trie_node)}]);

%%------------------------------------------------------------------------------
%% @doc
%% Replicate trie tables.
%%
%% @end
%%------------------------------------------------------------------------------
mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(trie),
    ok = emqttd_mnesia:copy_table(trie_node).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Insert topic to trie tree.
%%
%% @end
%%------------------------------------------------------------------------------
-spec insert(Topic :: binary()) -> ok.
insert(Topic) when is_binary(Topic) ->
	case mnesia:read(trie_node, Topic) of
	[#trie_node{topic=Topic}] ->
        ok;
	[TrieNode=#trie_node{topic=undefined}] ->
		mnesia:write(TrieNode#trie_node{topic=Topic});
	[] ->
		%add trie path
		[add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
		%add last node
		mnesia:write(#trie_node{node_id=Topic, topic=Topic})
	end.

%%------------------------------------------------------------------------------
%% @doc
%% Find trie nodes that match topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec find(Topic :: binary()) -> list(MatchedTopic :: binary()).
find(Topic) when is_binary(Topic) ->
    TrieNodes = match_node(root, emqtt_topic:words(Topic), []),
    [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined].

%%------------------------------------------------------------------------------
%% @doc
%% Delete topic from trie tree.
%%
%% @end
%%------------------------------------------------------------------------------
-spec delete(Topic :: binary()) -> ok.
delete(Topic) when is_binary(Topic) ->
	case mnesia:read(trie_node, Topic) of
	[#trie_node{edge_count=0}] -> 
		mnesia:delete({trie_node, Topic}),
		delete_path(lists:reverse(emqtt_topic:triples(Topic)));
	[TrieNode] ->
		mnesia:write(TrieNode#trie_node{topic=Topic});
	[] ->
	    ok	
	end.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Add path to trie tree.
%%
%% @end
%%------------------------------------------------------------------------------
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
	
%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Match node with word or '+'.
%%
%% @end
%%------------------------------------------------------------------------------
match_node(NodeId, [], ResAcc) ->
	mnesia:read(trie_node, NodeId) ++ 'match_#'(NodeId, ResAcc);

match_node(NodeId, [W|Words], ResAcc) ->
	lists:foldl(fun(WArg, Acc) ->
		case mnesia:read(trie, #trie_edge{node_id=NodeId, word=WArg}) of
		[#trie{node_id=ChildId}] -> match_node(ChildId, Words, Acc);
		[] -> Acc
		end
	end, 'match_#'(NodeId, ResAcc), [W, '+']).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Match node with '#'.
%%
%% @end
%%------------------------------------------------------------------------------
'match_#'(NodeId, ResAcc) ->
	case mnesia:read(trie, #trie_edge{node_id=NodeId, word = '#'}) of
	[#trie{node_id=ChildId}] ->
		mnesia:read(trie_node, ChildId) ++ ResAcc;	
	[] ->
		ResAcc
	end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Delete paths from trie tree.
%%
%% @end
%%------------------------------------------------------------------------------
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

