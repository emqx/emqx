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
%%% emqttd pubsub.
%%%
%%% @end
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub).

-author('feng@emqtt.io').

-include_lib("emqtt/include/emqtt.hrl").

-include("emqttd.hrl").

-behaviour(gen_server).

-define(SERVER, ?MODULE).

-define(SUBACK_ERR, 128).

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Exports 
-export([start_link/0]).

-export([create/1,
         subscribe/1, subscribe/2,
         unsubscribe/1,
         publish/1, publish/2,
         %local node
         dispatch/2, match/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {submap :: map()}).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================
mnesia(boot) ->
    %% topic table
    ok = emqttd_mnesia:create_table(topic, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_topic},
                {attributes, record_info(fields, mqtt_topic)}]),
    %% local subscriber table, not shared with other nodes 
    ok = emqttd_mnesia:create_table(subscriber, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_subscriber},
                {attributes, record_info(fields, mqtt_subscriber)},
                {index, [subpid]},
                {local_content, true}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(topic),
    ok = emqttd_mnesia:copy_table(subscriber).

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
%% Create topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec create(binary()) -> ok.
create(Topic) when is_binary(Topic) ->
    Record = #mqtt_topic{topic = Topic, node = node()},
    {atomic, ok} = mnesia:transaction(fun insert_topic/1, [Record]), ok.

%%------------------------------------------------------------------------------
%% @doc
%% Subscribe topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec subscribe({Topic, Qos} | list({Topic, Qos})) -> {ok, Qos | list(Qos)} when 
    Topic   :: binary(),
    Qos     :: mqtt_qos().
subscribe(Topics = [{_Topic, _Qos} | _]) ->
    {ok, lists:map(fun({Topic, Qos}) ->
            case subscribe(Topic, Qos) of
                {ok, GrantedQos} -> 
                    GrantedQos;
                Error -> 
                    lager:error("Failed to subscribe '~s': ~p", [Topic, Error]), 
                    ?SUBACK_ERR
            end
        end, Topics)}.

-spec subscribe(Topic :: binary(), Qos :: mqtt_qos()) -> {ok, Qos :: mqtt_qos()}.
subscribe(Topic, Qos) when is_binary(Topic) andalso ?IS_QOS(Qos) ->
    TopicRecord = #mqtt_topic{topic = Topic, node = node()},
    Subscriber = #mqtt_subscriber{topic = Topic, qos = Qos, subpid = self()},
    F = fun() ->
            case insert_topic(TopicRecord) of
               ok -> insert_subscriber(Subscriber);
               Error -> Error
            end
        end,
    case mnesia:transaction(F) of
        {atomic, ok} -> 
            {ok, Qos};
        {aborted, Reason} -> 
            {error, Reason}
    end.

%%------------------------------------------------------------------------------
%% @doc
%% Unsubscribe Topic or Topics
%%
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    SubPid = self(),
    TopicRecord = #mqtt_topic{topic = Topic, node = node()},
    F = fun() ->
        %%TODO record name...
        Pattern = #mqtt_subscriber{topic = Topic, _ = '_', subpid = SubPid},
        [mnesia:delete_object(Sub) || Sub <- mnesia:match_object(Pattern)],
        try_remove_topic(TopicRecord)
    end,
    {atomic, _} = mneisa:transaction(F), ok;

unsubscribe(Topics = [Topic|_]) when is_binary(Topic) ->
    lists:foreach(fun(T) -> unsubscribe(T) end, Topics).

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
	lists:foreach(fun(#mqtt_topic{topic=Name, node=Node}) ->
        case Node =:= node() of
            true -> dispatch(Name, Msg);
            false -> rpc:cast(Node, ?MODULE, dispatch, [Name, Msg])
        end
	end, match(Topic)).

%%------------------------------------------------------------------------------
%% @doc
%% Dispatch Locally. Should only be called by publish.
%%
%% @end
%%------------------------------------------------------------------------------
-spec dispatch(Topic :: binary(), Msg :: mqtt_message()) -> non_neg_integer().
dispatch(Topic, Msg = #mqtt_message{qos = Qos}) when is_binary(Topic) ->
    case mnesia:dirty_read(subscriber, Topic) of
        [] -> 
            %%TODO: not right when clusted...
            setstats(dropped);
        Subscribers ->
            lists:foreach(
                fun(#mqtt_subscriber{qos = SubQos, subpid=SubPid}) ->
                        Msg1 = if
                            Qos > SubQos -> Msg#mqtt_message{qos = SubQos};
                            true -> Msg
                        end,
                        SubPid ! {dispatch, {self(), Msg1}}
                end, Subscribers)
    end.

%%------------------------------------------------------------------------------
%% @doc
%% @private
%% Match topic.
%%
%% @end
%%------------------------------------------------------------------------------
-spec match(Topic :: binary()) -> [mqtt_topic()].
match(Topic) when is_binary(Topic) ->
	MatchedTopics = mnesia:async_dirty(fun emqttd_trie:find/1, [Topic]),
	lists:flatten([mnesia:dirty_read(topic, Name) || Name <- MatchedTopics]).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([]) ->
    %%TODO: really need?
    process_flag(priority, high),
    process_flag(min_heap_size, 1024*1024),
    mnesia:subscribe({table, topic, simple}),
    mnesia:subscribe({table, subscriber, simple}),
    {ok, #state{submap = maps:new()}}.

handle_call(Req, _From, State) ->
    lager:error("Bad Request: ~p", [Req]),
	{reply, {error, badreq}, State}.

handle_cast(Msg, State) ->
    lager:error("Bad Msg: ~p", [Msg]),
	{noreply, State}.

handle_info({mnesia_table_event, {write, #mqtt_subscriber{subpid = Pid}, _ActivityId}},
            State = #state{submap = SubMap}) ->
    NewSubMap =
    case maps:is_key(Pid, SubMap) of
        false ->
            maps:put(Pid, erlang:monitor(process, Pid), SubMap);
        true ->
            SubMap
    end,
    setstats(subscribers),
    {noreply, State#state{submap = NewSubMap}};

handle_info({mnesia_table_event, {write, #mqtt_topic{}, _ActivityId}}, State) ->
    %%TODO: this is not right when clusterd.
    setstats(topics),
    {noreply, State};

%% {write, #topic{}, _ActivityId}
%% {delete_object, _OldRecord, _ActivityId}
%% {delete, {Tab, Key}, ActivityId}
handle_info({mnesia_table_event, _Event}, State) ->
    setstats(topics),
    setstats(subscribers),
    {noreply, State};

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State = #state{submap = SubMap}) ->
    case maps:is_key(DownPid, SubMap) of
        true ->
            Node = node(),
            F = fun() -> 
                    Subscribers = mnesia:index_read(subscriber, DownPid, #mqtt_subscriber.subpid),
                    lists:foreach(fun(Sub = #mqtt_subscriber{topic = Topic}) ->
                                mnesia:delete_object(subscriber, Sub, write),
                                try_remove_topic(#mqtt_topic{topic = Topic, node = Node})
                        end, Subscribers)
            end,
            NewState =
            case catch mnesia:transaction(F) of
                {atomic, _} ->
                    State#state{submap = maps:remove(DownPid, SubMap)};
                {aborted, Reason} ->
                    lager:error("Failed to delete 'DOWN' subscriber ~p: ~p", [DownPid, Reason]),
                    State
            end,
            setstats(topics), setstats(subscribers),
            {noreply, NewState};
        false ->
            lager:error("Unexpected 'DOWN' from ~p", [DownPid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
	{noreply, State}.

terminate(_Reason, _State) ->
    mnesia:unsubscribe({table, topic, simple}),
    mnesia:unsubscribe({table, subscriber, simple}),
    %%TODO: clear topics belongs to this node???
    ok.

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================
insert_topic(Record = #mqtt_topic{topic = Topic}) ->
    case mnesia:wread({topic, Topic}) of
        [] ->
            ok = emqttd_trie:insert(Topic),
            mnesia:write(topic, Record, write);
        Records ->
            case lists:member(Record, Records) of
                true -> ok;
                false -> mnesia:write(topic, Record, write)
            end
    end.

insert_subscriber(Subscriber) ->
    mnesia:write(subscriber, Subscriber, write).

try_remove_topic(Record = #mqtt_topic{topic = Topic}) ->
    case mnesia:read({subscriber, Topic}) of
        [] ->
            mnesia:delete_object(topic, Record, write),
            case mnesia:read(topic, Topic) of
                [] -> emqttd_trie:delete(Topic);		
                _ -> ok
            end;
         _ -> 
            ok
 	end.

setstats(topics) ->
    emqttd_broker:setstat('topics/count', mnesia:table_info(topic, size));

setstats(subscribers) ->
    emqttd_broker:setstats('subscribers/count',
                           'subscribers/max',
                           mnesia:table_info(subscriber, size));
setstats(dropped) ->
    emqttd_metrics:inc('messages/dropped').

