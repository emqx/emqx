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
%%% emqtt core pubsub.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqtt_pubsub).

-author('feng@emqtt.io').

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-include("emqtt.hrl").

-include("emqtt_topic.hrl").

-include("emqtt_packet.hrl").

-include_lib("stdlib/include/qlc.hrl").

%% API Exports 

-export([start_link/0, getstats/0]).

-export([topics/0,
        create/1,
		subscribe/2,
		unsubscribe/2,
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
create(Topic) -> 
    gen_server:call(?SERVER, {create, Topic}).

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe Topic or Topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec subscribe({binary(), mqtt_qos()} | list(), pid()) -> {ok, list(mqtt_qos())}.
subscribe({Topic, Qos}, SubPid) when is_binary(Topic) and is_pid(SubPid) ->
    subscribe([{Topic, Qos}], SubPid);

subscribe(Topics, SubPid) when is_list(Topics) and is_pid(SubPid) ->
    gen_server:call(?SERVER, {subscribe, Topics, SubPid}).

%%------------------------------------------------------------------------------
%% @doc
%% Unsubscribe Topic or Topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary()), pid()) -> ok.
unsubscribe(Topic, SubPid) when is_binary(Topic) and is_pid(SubPid) ->
    unsubscribe([Topic], SubPid);

unsubscribe(Topics, SubPid) when is_list(Topics) and is_pid(SubPid) ->
	gen_server:cast(?SERVER, {unsubscribe, Topics, SubPid}).

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
	lists:foreach(fun(#topic{name=Name, node=Node}) ->
        case Node =:= node() of
		true -> dispatch(Name, Msg);
		false -> rpc:call(Node, ?MODULE, dispatch, [Name, Msg])
		end
	end, match(Topic)).

%%TODO: dispatch counts....

%%------------------------------------------------------------------------------
%% @doc
%% Dispatch Locally. Should only be called by publish.
%%
%% @end
%%------------------------------------------------------------------------------
dispatch(Topic, Msg = #mqtt_message{qos = Qos}) when is_binary(Topic) ->
    lists:foreach(fun(#topic_subscriber{qos = SubQos, subpid=SubPid}) -> 
        Msg1 = if
            Qos > SubQos -> Msg#mqtt_message{qos = SubQos};
            true -> Msg
        end,
        SubPid ! {dispatch, {self(), Msg1}}
    end, ets:lookup(topic_subscriber, Topic)).

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Match topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec match(Topic :: binary()) -> [topic()].
match(Topic) when is_binary(Topic) ->
	TrieNodes = mnesia:async_dirty(fun trie_match/1, [emqtt_topic:words(Topic)]),
    Names = [Name || #topic_trie_node{topic=Name} <- TrieNodes, Name=/= undefined],
	lists:flatten([mnesia:dirty_read(topic, Name) || Name <- Names]).

%% ------------------------------------------------------------------
%% gen_server Function Definitions
%% ------------------------------------------------------------------

init([]) ->
	mnesia:create_table(topic_trie, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, topic_trie)}]),
	mnesia:create_table(topic_trie_node, [
		{ram_copies, [node()]},
		{attributes, record_info(fields, topic_trie_node)}]),
	mnesia:create_table(topic, [
		{type, bag},
		{record_name, topic},
		{ram_copies, [node()]}, 
		{attributes, record_info(fields, topic)}]),
	mnesia:add_table_copy(topic_trie, node(), ram_copies),
	mnesia:add_table_copy(topic_trie_node, node(), ram_copies),
	mnesia:add_table_copy(topic, node(), ram_copies),
	ets:new(topic_subscriber, [bag, named_table, {keypos, 2}]),
	{ok, #state{}}.

handle_call(getstats, _From, State = #state{max_subs = Max}) ->
    Stats = [{'topics/count', mnesia:table_info(topic, size)},
             {'subscribers/count', ets:info(topic_subscriber, size)},
             {'subscribers/max', Max}],
    {reply, Stats, State};

handle_call({create, Topic}, _From, State) ->
	Result = mnesia:transaction(fun trie_add/1, [Topic]),
    {reply, Result, setstats(State)};

handle_call({subscribe, Topics, SubPid}, _From, State) ->
    Result = [subscribe_topic({Topic, Qos}, SubPid) || {Topic, Qos} <- Topics],
    Reply = 
    case [Err || Err = {error, _} <- Result] of
        [] -> {ok, [Qos || {ok, Qos} <- Result]};
        Errors -> hd(Errors)
    end,
    {reply, Reply, setstats(State)};

handle_call(Req, _From, State) ->
	{stop, {badreq, Req}, State}.

handle_cast({unsubscribe, Topics, SubPid}, State) ->
    lists:foreach(fun(Topic) -> 
        ets:match_delete(topic_subscriber, #topic_subscriber{topic=Topic, qos ='_', subpid=SubPid}),
        try_remove_topic(Topic)
    end, Topics),
	{noreply, setstats(State)};

handle_cast(Msg, State) ->
	{stop, {badmsg, Msg}, State}.

handle_info({'DOWN', Mon, _Type, _Object, _Info}, State) ->
	case get({submon, Mon}) of
	undefined ->
		lager:error("unexpected 'DOWN': ~p", [Mon]);
	SubPid ->
		erase({submon, Mon}),
		erase({subscriber, SubPid}),
		Subs = ets:match_object(topic_subscriber, #topic_subscriber{subpid=SubPid, _='_'}),
		[ets:delete_object(topic_subscriber, Sub) || Sub <- Subs],
		[try_remove_topic(Topic) || #topic_subscriber{topic=Topic} <- Subs]
	end,
	{noreply, setstats(State)};

handle_info(Info, State) ->
	{stop, {badinfo, Info}, State}.

terminate(_Reason, _State) ->
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
subscribe_topic({Topic, Qos}, SubPid) ->
	case mnesia:transaction(fun trie_add/1, [Topic]) of
	{atomic, _} ->	
		case get({subscriber, SubPid}) of
		undefined -> 
            %%TODO: refactor later...
			MonRef = erlang:monitor(process, SubPid),
			put({subcriber, SubPid}, MonRef),
			put({submon, MonRef}, SubPid);
		_ ->
			already_monitored
		end,
        %% remove duplicated subscribers
        try_remove_subscriber({Topic, Qos}, SubPid),
		ets:insert(topic_subscriber, #topic_subscriber{topic=Topic, qos = Qos, subpid=SubPid}),
        %TODO: GrantedQos??
        {ok, Qos};
	{aborted, Reason} ->
		{error, Reason}
	end.

try_remove_subscriber({Topic, Qos}, SubPid) ->
    case ets:lookup(topic_subscriber, Topic) of
        [] -> 
            not_found;
        Subs ->
            DupSubs = [Sub || Sub = #topic_subscriber{qos = OldQos, subpid = OldPid} 
                                    <- Subs, Qos =/= OldQos, OldPid =:= SubPid],
            case DupSubs of
                [] -> ok;
                [DupSub] -> 
                    lager:warning("PubSub: remove duplicated subscriber ~p", [DupSub]), 
                    ets:delete(topic_subscriber, DupSub)
            end
    end.

try_remove_topic(Name) when is_binary(Name) ->
	case ets:member(topic_subscriber, Name) of
	false -> 
		Topic = emqtt_topic:new(Name),
		Fun = fun() -> 
			mnesia:delete_object(Topic),
			case mnesia:read(topic, Name) of
			[] -> trie_delete(Name);		
			_ -> ignore
			end
		end,
		mnesia:transaction(Fun);
	true -> 
		ok
	end.

trie_add(Topic) when is_binary(Topic) ->
	mnesia:write(emqtt_topic:new(Topic)),
	case mnesia:read(topic_trie_node, Topic) of
	[TrieNode=#topic_trie_node{topic=undefined}] ->
		mnesia:write(TrieNode#topic_trie_node{topic=Topic});
	[#topic_trie_node{topic=Topic}] ->
        {atomic, already_exist};
	[] ->
		%add trie path
		[trie_add_path(Triple) || Triple <- emqtt_topic:triples(Topic)],
		%add last node
		mnesia:write(#topic_trie_node{node_id=Topic, topic=Topic})
	end.

trie_delete(Topic) when is_binary(Topic) ->
	case mnesia:read(topic_trie_node, Topic) of
	[#topic_trie_node{edge_count=0}] -> 
		mnesia:delete({topic_trie_node, Topic}),
		trie_delete_path(lists:reverse(emqtt_topic:triples(Topic)));
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
    emqtt_broker:setstat('topics/count', mnesia:table_info(topic, size)),
    SubCount = ets:info(topic_subscriber, size),
    emqtt_broker:setstat('subscribers/count', SubCount),
    if
        SubCount > Max ->
            emqtt_broker:setstat('subscribers/max', SubCount),
            State#state{max_subs = SubCount};
        true -> 
            State
    end.


