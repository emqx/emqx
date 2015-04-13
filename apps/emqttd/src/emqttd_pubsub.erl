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
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub).

-author('feng@emqtt.io').

-include("emqttd.hrl").

-include("emqttd_topic.hrl").

-include("emqttd_packet.hrl").

-include_lib("stdlib/include/qlc.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

%% API Exports 

-export([start_link/0, getstats/0]).

-export([create/1,
         subscribe/1, unsubscribe/1,
         publish/1, publish/2,
         %local node
         dispatch/2, match/1]).

%% gen_server Function Exports

-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {}).

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
    [{'topics/count', mnesia:table_info(topic, size)},
     {'subscribers/count', mnesia:table_info(topic_subscriber, size)},
     {'subscribers/max', emqttd_broker:getstat('subscribers/max')}].

%%------------------------------------------------------------------------------
%% @doc
%% Create static topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec create(binary()) -> ok.
create(Topic) when is_binary(Topic) ->
    {atomic, ok} = mnesia:transaction(fun add_topic/1, [emqttd_topic:new(Topic)]), ok.

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe topics
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
subscribe([{Topic, Qos}|Topics], SubPid, Acc) ->
    TopicObj = emqttd_topic:new(Topic),
    Subscriber = #topic_subscriber{topic = Topic, qos = Qos, subpid = SubPid},
    F = fun() -> trie_add(TopicObj), mnesia:write(Subscriber) end,
    case mnesia:transaction(F) of
        {atomic, ok} -> 
            subscribe(Topics, SubPid, [Qos|Acc]);
        Error -> 
            {error, Error}
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
    mnesia:subscribe({table, topic, simple}),
    %% trie and topic tables, will be copied by all nodes.
    mnesia:subscribe({table, topic_subscriber, simple}),
	{ok, #state{}}.

handle_call(Req, _From, State) ->
    lager:error("Bad Req: ~p", [Req]),
	{reply, error, State}.

handle_cast(Msg, State) ->
    lager:error("Bad Msg: ~p", [Msg]),
	{noreply, State}.

%% a new record has been written.
handle_info({mnesia_table_event, {write, #topic_subscriber{subpid = Pid}, _ActivityId}}, State) ->
    %%TODO: rewrite...
    erlang:monitor(process, Pid),
    upstats(subscriber),
    {noreply, State};
%% TODO:...

handle_info({mnesia_table_event, {write, #topic{}, _ActivityId}}, State) ->
    upstats(topic),
    {noreply, State};

%% {write, #topic{}, _ActivityId}
%% {delete_object, _OldRecord, _ActivityId}
%% {delete, {Tab, Key}, ActivityId}
handle_info({mnesia_table_event, _Event}, State) ->
    upstats(),
    {noreply, State};

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
    upstats(),
	{noreply, State};

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
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
%%
add_topic(Topic = #topic{name = Name, node = Node}) ->
    case mnesia:wread(topic, Name) of
        [] ->
            trie_add(Name),
            mnesia:write(Topic);
        Topics  -> 
            case lists:member(Topic, Topics) of
                true -> ok;
                false -> mnesia:write(Topic)
            end
    end.


upstats() ->
    upstats(topic), upstats(subscribe). 

upstats(topic) ->
    emqttd_broker:setstat('topics/count', mnesia:table_info(topic, size));

upstats(subscribe) ->
    emqttd_broker:setstats('subscribers/count',
                           'subscribers/max',
                           mnesia:table_info(topic_subscriber, size)).

dropped(true) ->
    emqttd_metrics:inc('messages/dropped');
dropped(false) ->
    ok.

