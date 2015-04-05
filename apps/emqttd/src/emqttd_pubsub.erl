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
%%% emqttd core pubsub.
%%%
%%% TODO: should not use gen_server:call to create, subscribe topics...
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub).

-author('feng@emqtt.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-include("emqttd.hrl").

-include("emqttd_topic.hrl").

-include("emqttd_packet.hrl").

-include_lib("stdlib/include/qlc.hrl").

%% API Exports 

-export([start_link/0, getstats/0]).

-export([topics/0,
        create/1,
		subscribe/1,
		unsubscribe/1,
		publish/1,
		publish/2,
        %local node
		dispatch/2, 
		match/1]).

%% gen_server Function Exports

-export([init/1,
		handle_call/3,
		handle_cast/2,
		handle_info/2,
        terminate/2,
		code_change/3]).

-record(state, {max_subs = 0}).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc
%% Start Pubsub.
%%
%% @end
%%------------------------------------------------------------------------------
-spec start_link() -> {ok, pid()} | ignore | {error, any()}.
start_link() ->
    gen_server:start_link({local, ?SERVER}, ?MODULE, [], []).

%%------------------------------------------------------------------------------
%% @doc
%% Get stats of PubSub.
%%
%% @end
%%------------------------------------------------------------------------------
-spec getstats() -> [{atom(), non_neg_integer()}].
getstats() ->
    gen_server:call(?SERVER, getstats).

%%------------------------------------------------------------------------------
%% @doc
%% All Topics.
%%
%% @end
%%------------------------------------------------------------------------------
-spec topics() -> list(binary()).
topics() ->
	mnesia:dirty_all_keys(topic).

%%------------------------------------------------------------------------------
%% @doc
%% Create static topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec create(binary()) -> {atomic,  Reason :: any()} |  {aborted, Reason :: any()}.
create(Topic) when is_binary(Topic) -> 
    {atomic, ok} = mnesia:transaction(fun trie_add/1, [Topic]), ok.

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe Topic or Topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec subscribe({binary(), mqtt_qos()} | list()) -> {ok, list(mqtt_qos())}.
subscribe({Topic, Qos}) when is_binary(Topic) ->
    case subscribe([{Topic, Qos}]) of
        {ok, [GrantedQos]} -> {ok, GrantedQos};
        {error, Error} -> {error, Error}
    end;
subscribe(Topics = [{_Topic, _Qos}|_]) ->
    subscribe(Topics, self(), []).

subscribe([], _SubPid, Acc) ->
    {ok, lists:reverse(Acc)};
%%TODO: check this function later.
subscribe([{Topic, Qos}|Topics], SubPid, Acc) ->
    Subscriber = #topic_subscriber{topic=Topic, qos = Qos, subpid=SubPid},
    F = fun() -> trie_add(Topic), mnesia:write(Subscriber) end,
    case mnesia:transaction(F) of
        {atomic, ok} -> subscribe(Topics, SubPid, [Qos|Acc]);
        Error -> {error, Error}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Unsubscribe Topic or Topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe([Topic]);

unsubscribe(Topics = [Topic|_]) when is_list(Topics) and is_binary(Topic) ->
    unsubscribe(Topics, self()).

%%TODO: check this function later.
unsubscribe(Topics, SubPid) ->
    F = fun() -> 
        Subscribers = mnesia:index_read(topic_subscriber, SubPid, #topic_subscriber.subpid),
        lists:foreach(fun(Sub = #topic_subscriber{topic = Topic}) -> 
                              case lists:member(Topic, Topics) of
                                 true -> mneisa:delete_object(Sub);
                                 false -> ok
                              end 
                      end, Subscribers) 
        %TODO: try to remove topic??? if topic is dynamic...
        %%try_remove_topic(Topic)
        end,
    {atomic, _} = mneisa:transaction(F), ok.

%%------------------------------------------------------------------------------
%% @doc
%% Publish to cluster node.
%%
%% @end
%%------------------------------------------------------------------------------
-spec publish(Msg :: mqtt_message()) -> ok.
publish(Msg=#mqtt_message{topic=Topic}) ->
	publish(Topic, Msg).

-spec publish(Topic :: binary(), Msg :: mqtt_message()) -> any().
publish(Topic, Msg) when is_binary(Topic) ->
    Count =
	lists:foldl(fun(#topic{name=Name, node=Node}, Acc) ->
        case Node =:= node() of
            true -> dispatch(Name, Msg) + Acc;
            false -> rpc:call(Node, ?MODULE, dispatch, [Name, Msg]) + Acc
        end
	end, 0, match(Topic)),
    dropped(Count =:= 0).

%%------------------------------------------------------------------------------
%% @doc
%% Dispatch Locally. Should only be called by publish.
%%
%% @end
%%------------------------------------------------------------------------------
-spec dispatch(Topic :: binary(), Msg :: mqtt_message()) -> non_neg_integer().
dispatch(Topic, Msg = #mqtt_message{qos = Qos}) when is_binary(Topic) ->
    Subscribers = mnesia:dirty_read(topic_subscriber, Topic),
    lists:foreach(
        fun(#topic_subscriber{qos = SubQos, subpid=SubPid}) ->
                Msg1 = if
                    Qos > SubQos -> Msg#mqtt_message{qos = SubQos};
                    true -> Msg
                end,
                SubPid ! {dispatch, {self(), Msg1}}
            end, Subscribers), 
    length(Subscribers).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Match topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec match(Topic :: binary()) -> [topic()].
match(Topic) when is_binary(Topic) ->
	TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqttd_topic:words(Topic)]),
    Names = [Name || #topic_trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
	lists:flatten([mnesia:dirty_read(topic, Name) || Name <- Names]).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
    %% trie and topic tables, will be copied by all nodes.
	mnesia:create_table(topic_trie_node, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, topic_trie_node)}]),
	mnesia:add_table_copy(topic_trie_node, node(), ram_copies),
	mnesia:create_table(topic_trie, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, topic_trie)}]),
	mnesia:add_table_copy(topic_trie, node(), ram_copies),
	mnesia:create_table(topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(topic, node(), ram_copies),
    mnesia:subscribe({table, topic, simple}),
    %% local table, not shared with other table
    mnesia:create_table(topic_subscriber, [
		{type, bag},
		{record_name, topic_subscriber},
		{ram_copies, [node()]},
		{attributes, record_info(fields, topic_subscriber)},
        {index, [subpid]},
        {local_content, true}]),
    mnesia:subscribe({table, topic_subscriber, simple}),
	{ok, #state{}}.

handle_call(getstats, _From, State = #state{max_subs = Max}) ->
    Stats = [{'topics/count', mnesia:table_info(topic, size)},
             {'subscribers/count', mnesia:table_info(topic_subscriber, size)},
             {'subscribers/max', Max}],
    {reply, Stats, State};

handle_call(Req, _From, State) ->
    lager:error("Bad Req: ~p", [Req]),
	{reply, error, State}.

handle_cast(Msg, State) ->
    lager:error("Bad Msg: ~p", [Msg]),
	{noreply, State}.

%% a new record has been written.
handle_info({mnesia_table_event, {write, #topic_subscriber{subpid = Pid}, _ActivityId}}, State) ->
    erlang:monitor(process, Pid),
    {noreply, setstats(State)};

%% {write, #topic{}, _ActivityId}
%% {delete_object, _OldRecord, _ActivityId}
%% {delete, {Tab, Key}, ActivityId}
handle_info({mnesia_table_event, _Event}, State) ->
    {noreply, setstats(State)};

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State) ->
    F = fun() -> 
            %%TODO: how to read with write lock?
            [mnesia:delete_object(Sub) || Sub <- mnesia:index_read(topic_subscriber, DownPid, #topic_subscriber.subpid)]
            %%TODO: try to remove dynamic topics without subscribers
            %% [try_remove_topic(Topic) || #topic_subscriber{topic=Topic} <- Subs]
        end,
    case catch mnesia:transaction(F) of
        {atomic, _} -> ok;
        {aborted, Reason} -> lager:error("Failed to delete 'DOWN' subscriber ~p: ~p", [DownPid, Reason])
    end,        
	{noreply, setstats(State)};

handle_info(Info, State) ->
    lager:error("Bad Info: ~p", [Info]),
	{noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, topic, simple}),
    mnesia:unsubscribe({table, topic_subscriber, simple}),
    %%TODO: clear topics belongs to this node???
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

%%try_remove_topic(Name) when is_binary(Name) ->
%%	case ets:member(topic_subscriber, Name) of
%%	false -> 
%%		Topic = emqttd_topic:new(Name),
%%		Fun = fun() -> 
%%			mnesia:delete_object(Topic),
%%			case mnesia:read(topic, Name) of
%%			[] -> trie_delete(Name);		
%%			_ -> ignore
%%			end
%%		end,
%%		mnesia:transaction(Fun);
%%	true -> 
%%		ok
%%	end.

trie_add(Topic) when is_binary(Topic) ->
	mnesia:write(emqttd_topic:new(Topic)),
	case mnesia:read(topic_trie_node, Topic) of
	[TrieNode=#topic_trie_node{topic=undefined}] ->
		mnesia:write(TrieNode#topic_trie_node{topic=Topic});
	[#topic_trie_node{topic=Topic}] ->
        ok;
	[] ->
		%add trie path
		[trie_add_path(Triple) || Triple <- emqttd_topic:triples(Topic)],
		%add last node
		mnesia:write(#topic_trie_node{node_id=Topic, topic=Topic})
	end.

trie_delete(Topic) when is_binary(Topic) ->
	case mnesia:read(topic_trie_node, Topic) of
	[#topic_trie_node{edge_count=0}] -> 
		mnesia:delete({topic_trie_node, Topic}),
		trie_delete_path(lists:reverse(emqttd_topic:triples(Topic)));
	[TrieNode] ->
		mnesia:write(TrieNode#topic_trie_node{topic=Topic});
	[] ->
		ignore
	end.
	
trie_match(Words) ->
	trie_match(root, Words, []).

trie_match(NodeId, [], ResAcc) ->
	mnesia:read(topic_trie_node, NodeId) ++ 'trie_match_#'(NodeId, ResAcc);

trie_match(NodeId, [W|Words], ResAcc) ->
	lists:foldl(fun(WArg, Acc) ->
		case mnesia:read(topic_trie, #topic_trie_edge{node_id=NodeId, word=WArg}) of
		[#topic_trie{node_id=ChildId}] -> trie_match(ChildId, Words, Acc);
		[] -> Acc
		end
	end, 'trie_match_#'(NodeId, ResAcc), [W, '+']).

'trie_match_#'(NodeId, ResAcc) ->
	case mnesia:read(topic_trie, #topic_trie_edge{node_id=NodeId, word = '#'}) of
	[#topic_trie{node_id=ChildId}] ->
		mnesia:read(topic_trie_node, ChildId) ++ ResAcc;	
	[] ->
		ResAcc
	end.

trie_add_path({Node, Word, Child}) ->
	Edge = #topic_trie_edge{node_id=Node, word=Word},
	case mnesia:read(topic_trie_node, Node) of
	[TrieNode = #topic_trie_node{edge_count=Count}] ->
		case mnesia:read(topic_trie, Edge) of
		[] -> 
			mnesia:write(TrieNode#topic_trie_node{edge_count=Count+1}),
			mnesia:write(#topic_trie{edge=Edge, node_id=Child});
		[_] -> 
			ok
		end;
	[] ->
		mnesia:write(#topic_trie_node{node_id=Node, edge_count=1}),
		mnesia:write(#topic_trie{edge=Edge, node_id=Child})
	end.

trie_delete_path([]) ->
	ok;
trie_delete_path([{NodeId, Word, _} | RestPath]) ->
	Edge = #topic_trie_edge{node_id=NodeId, word=Word},
	mnesia:delete({topic_trie, Edge}),
	case mnesia:read(topic_trie_node, NodeId) of
	[#topic_trie_node{edge_count=1, topic=undefined}] -> 
		mnesia:delete({topic_trie_node, NodeId}),
		trie_delete_path(RestPath);
	[TrieNode=#topic_trie_node{edge_count=1, topic=_}] -> 
		mnesia:write(TrieNode#topic_trie_node{edge_count=0});
	[TrieNode=#topic_trie_node{edge_count=C}] ->
		mnesia:write(TrieNode#topic_trie_node{edge_count=C-1});
	[] ->
		throw({notfound, NodeId}) 
	end.

setstats(State = #state{max_subs = Max}) ->
    emqttd_broker:setstat('topics/count', mnesia:table_info(topic, size)),
    SubCount = mnesia:table_info(topic_subscriber, size),
    emqttd_broker:setstat('subscribers/count', SubCount),
    if
        SubCount > Max ->
            emqttd_broker:setstat('subscribers/max', SubCount),
            State#state{max_subs = SubCount};
        true -> 
            State
    end.

dropped(true) ->
    emqttd_metrics:inc('messages/dropped');
dropped(false) ->
    ok.

