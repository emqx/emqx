%% The contents of this file are subject to the Mozilla Public License
%% Version 1.1 (the "License"); you may not use this file except in
%% compliance with the License. You may obtain a copy of the License
%% at http://www.mozilla.org/MPL/
%%
%% Software distributed under the License is distributed on an "AS IS"
%% basis, WITHOUT WARRANTY OF ANY KIND, either express or implied. See
%% the License for the specific language governing rights and
%% limitations under the License.
%%
%% Developer of the eMQTT Code is <ery.lee@gmail.com>
%% Copyright (c) 2012 Ery Lee.  All rights reserved.
%%
-module(emqtt_router).

-include("emqtt.hrl").

-include("emqtt_frame.hrl").

-include_lib("stdlib/include/qlc.hrl").

-export([start_link/0]).

-export([topics/0,
		subscribe/2,
		unsubscribe/2,
		publish/1,
		publish/2,
		route/2,
		match/1,
		down/1]).

-behaviour(gen_server).

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
		terminate/2,
		code_change/3]).

-record(state, {}).

start_link() ->
	gen_server2:start_link({local, ?MODULE}, ?MODULE, [], []).

topics() ->
	mnesia:dirty_all_keys(topic).

subscribe({Topic, Qos}, Client) when is_pid(Client) ->
	gen_server2:call(?MODULE, {subscribe, {Topic, Qos}, Client}).

unsubscribe(Topic, Client) when is_list(Topic) and is_pid(Client) ->
	gen_server2:cast(?MODULE, {unsubscribe, Topic, Client}).

publish(Msg=#mqtt_msg{topic=Topic}) ->
	publish(Topic, Msg).

%publish to cluster node.
publish(Topic, Msg) when is_list(Topic) and is_record(Msg, mqtt_msg) ->
	lists:foreach(fun(#topic{name=Name, node=Node}) ->
		case Node == node() of
		true -> route(Name, Msg);
		false -> rpc:call(Node, ?MODULE, route, [Name, Msg])
		end
	end, match(Topic)).

%route locally, should only be called by publish
route(Topic, Msg) ->
	[Client ! {route, Msg} || #subscriber{client=Client} <- ets:lookup(subscriber, Topic)].

match(Topic) when is_list(Topic) ->
	TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqtt_topic:words(Topic)]),
    Names = [Name || #trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
	lists:flatten([mnesia:dirty_read(topic, Name) || Name <- Names]).

down(Client) when is_pid(Client) ->
	gen_server2:cast(?MODULE, {down, Client}).

init([]) ->
	mnesia:create_table(trie, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, trie)}]),
	mnesia:add_table_copy(trie, node(), ram_copies),
	mnesia:create_table(trie_node, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, trie_node)}]),
	mnesia:add_table_copy(trie_node, node(), ram_copies),
	mnesia:create_table(topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(topic, node(), ram_copies),
	ets:new(subscriber, [bag, named_table, {keypos, 2}]),
	?INFO_MSG("emqtt_router is started."),
	{ok, #state{}}.

handle_call({subscribe, {Topic, Qos}, Client}, _From, State) ->
	case mnesia:transaction(fun trie_add/1, [Topic]) of
	{atomic, _} ->	
		ets:insert(subscriber, #subscriber{topic=Topic, qos=Qos, client=Client}),
		emqtt_retained:send(Topic, Client),
		{reply, ok, State};
	{aborted, Reason} ->
		{reply, {error, Reason}, State}
	end;

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({unsubscribe, Topic, Client}, State) ->
	ets:match_delete(subscriber, #subscriber{topic=Topic, client=Client, _='_'}),
	try_remove_topic(Topic),
	{noreply, State};

handle_cast({down, Client}, State) ->
	case ets:match_object(subscriber, #subscriber{client=Client, _='_'}) of
	[] -> ignore;
	Subs -> 
		[ets:delete_object(subscriber, Sub) || Sub <- Subs],
		[try_remove_topic(Topic) || #subscriber{topic=Topic} <- Subs]
	end,
	{noreply, State};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
	ok.

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%% ------------------------------------------------------------------------
%% internal functions
%% ------------------------------------------------------------------------
try_remove_topic(Name) ->
	case ets:member(subscriber, Name) of
	false -> 
		Topic = emqtt_topic:new(Name),
		Fun = fun() -> 
			mnesia:delete_object(topic, Topic),
			case mnesia:read(topic, Topic) of
			[] -> trie_delete(Name);		
			_ -> ignore
			end
		end,
		mnesia:transaction(Fun);
	true -> 
		ok
	end.

trie_add(Topic) ->
	mnesia:write(emqtt_topic:new(Topic)),
	case mnesia:read(trie_node, Topic) of
	[TrieNode=#trie_node{topic=undefined}] ->
		mnesia:write(TrieNode#trie_node{topic=Topic});
	[#trie_node{topic=Topic}] ->
		ignore;
	[] ->
		%add trie path
		[trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
		%add last node
		mnesia:write(#trie_node{node_id=Topic, topic=Topic})
	end.

trie_delete(Topic) ->
	case mnesia:read(trie_node, Topic) of
	[#trie_node{edge_count=0}] -> 
		mnesia:delete({trie_node, Topic}),
		trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
	[TrieNode] ->
		mnesia:write(TrieNode#trie_node{topic=Topic});
	[] ->
		ignore
	end.
	
trie_match(Words) ->
	trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
	mnesia:read(trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
	lists:foldl(fun(WArg, Acc) ->
		case mnesia:read(trie, #trie_edge{node_id=NodeId, word=WArg}) of
		[#trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
		[] -> Acc
		end
	end, 'trie_match_#'(NodeId, ResAcc), [W, "+"]).

'trie_match_#'(NodeId, ResAcc) ->
	case mnesia:read(trie, #trie_edge{node_id=NodeId, word="#"}) of
	[#trie{node_id=ChildId}] ->
		mnesia:read(trie_node, ChildId) ++ ResAcc;	
	[] ->
		ResAcc
	end.

trie_add_path({Node, Word, Child}) ->
	Edge = #trie_edge{node_id=Node, word=Word},
	case mnesia:read(trie_node, Node) of
	[TrieNode = #trie_node{edge_count=Count}] ->
		case mnesia:read(trie, Edge) of
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

trie_delete_path([{NodeId, Word, _} | RestPath]) ->
	Edge = #trie_edge{node_id=NodeId, word=Word},
	mnesia:delete({trie, Edge}),
	case mnesia:read(trie_node, NodeId) of
	[#trie_node{edge_count=1, topic=undefined}] -> 
		mnesia:delete({trie_node, NodeId}),
		trie_delete_path(RestPath);
	[TrieNode=#trie_node{edge_count=1, topic=_}] -> 
		mnesia:write(TrieNode#trie_node{edge_count=0});
	[TrieNode=#trie_node{edge_count=C}] ->
		mnesia:write(TrieNode#trie_node{edge_count=C-1});
	[] ->
		throw({notfound, NodeId}) 
	end.

