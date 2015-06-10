%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include_lib("emqtt/include/emqtt.hrl").

-include_lib("emqtt/include/emqtt_packet.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

-behaviour(gen_server).

%% API Exports 
-export([start_link/2]).

-export([create/1,
         subscribe/1,
         unsubscribe/1,
         publish/2,
         %local node
         dispatch/2, match/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-define(POOL, pubsub).

-record(state, {id, submap :: map()}).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================
mnesia(boot) ->
    %% p2p queue table
    ok = emqttd_mnesia:create_table(queue, [
                {type, set},
                {ram_copies, [node()]},
                {record_name, mqtt_queue},
                {attributes, record_info(fields, mqtt_queue)}]),
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
                {index, [pid]},
                {local_content, true}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(queue),
    ok = emqttd_mnesia:copy_table(topic),
    ok = emqttd_mnesia:copy_table(subscriber).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start one pubsub server
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Id, Opts) -> {ok, pid()} | ignore | {error, any()} when
    Id   :: pos_integer(),
    Opts :: list().
start_link(Id, Opts) ->
    gen_server:start_link(?MODULE, [Id, Opts], []).

%%------------------------------------------------------------------------------
%% @doc Create topic. Notice That this transaction is not protected by pubsub pool
%% @end
%%------------------------------------------------------------------------------
-spec create(Topic :: binary()) -> ok | {error, Error :: any()}.
create(<<"$Q/", _Queue/binary>>) ->
    %% protecte from queue
    {error, cannot_create_queue};
create(Topic) when is_binary(Topic) ->
    TopicR = #mqtt_topic{topic = Topic, node = node()},
    case mnesia:transaction(fun add_topic/1, [TopicR]) of
        {atomic, ok} ->
            setstats(topics), ok;
        {aborted, Error} ->
            {error, Error}
    end.

%%------------------------------------------------------------------------------
%% @doc Subscribe topic
%% @end
%%------------------------------------------------------------------------------
-spec subscribe({Topic, Qos} | list({Topic, Qos})) -> {ok, Qos | list(Qos)} | {error, any()} when
    Topic   :: binary(),
    Qos     :: mqtt_qos().
subscribe({Topic, Qos}) when is_binary(Topic) andalso ?IS_QOS(Qos) ->
    call({subscribe, self(), Topic, Qos});

subscribe(Topics = [{_Topic, _Qos} | _]) ->
    call({subscribe, self(), Topics}).

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topic or Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    cast({unsubscribe, self(), Topic});

unsubscribe(Topics = [Topic|_]) when is_binary(Topic) ->
    cast({unsubscribe, self(), Topics}).

call(Req) ->
    Pid = gproc_pool:pick_worker(?POOL, self()),
    gen_server:call(Pid, Req, infinity).

cast(Msg) ->
    Pid = gproc_pool:pick_worker(?POOL, self()),
    gen_server:cast(Pid, Msg).

%%------------------------------------------------------------------------------
%% @doc Publish to cluster nodes
%% @end
%%------------------------------------------------------------------------------
-spec publish(From :: mqtt_clientid() | atom(), Msg :: mqtt_message()) -> ok.
publish(From, #mqtt_message{topic=Topic} = Msg) ->
    trace(publish, From, Msg),
    %% Retain message first. Don't create retained topic.
    case emqttd_msg_store:retain(Msg) of
        ok ->
            %TODO: why unset 'retain' flag?
            publish(From, Topic, emqtt_message:unset_flag(Msg));
        ignore ->
            publish(From, Topic, Msg)
     end.

publish(From, <<"$Q/", _/binary>> = Queue, #mqtt_message{qos = Qos} = Msg) ->
    lists:foreach(
        fun(#mqtt_queue{subpid = SubPid, qos = SubQos}) -> 
            Msg1 = if
                Qos > SubQos -> Msg#mqtt_message{qos = SubQos};
                true -> Msg
            end,
            SubPid ! {dispatch, {self(), Msg1}}
        end, mnesia:dirty_read(queue, Queue));
    
publish(_From, Topic, Msg) when is_binary(Topic) ->
	lists:foreach(fun(#mqtt_topic{topic=Name, node=Node}) ->
        case Node =:= node() of
            true -> dispatch(Name, Msg);
            false -> rpc:cast(Node, ?MODULE, dispatch, [Name, Msg])
        end
	end, match(Topic)).

%%------------------------------------------------------------------------------
%% @doc Dispatch message locally. should only be called by publish.
%% @end
%%------------------------------------------------------------------------------
-spec dispatch(Topic :: binary(), Msg :: mqtt_message()) -> non_neg_integer().
dispatch(Topic, #mqtt_message{qos = Qos} = Msg ) when is_binary(Topic) ->
    Subscribers = mnesia:dirty_read(subscriber, Topic),
    setstats(dropped, Subscribers =:= []), %%TODO:...
    lists:foreach(
        fun(#mqtt_subscriber{qos = SubQos, pid=SubPid}) ->
                Msg1 = if
                    Qos > SubQos -> Msg#mqtt_message{qos = SubQos};
                    true -> Msg
                end,
                SubPid ! {dispatch, {self(), Msg1}}
            end, Subscribers), 
    length(Subscribers).

-spec match(Topic :: binary()) -> [mqtt_topic()].
match(Topic) when is_binary(Topic) ->
	MatchedTopics = mnesia:async_dirty(fun emqttd_trie:find/1, [Topic]),
	lists:append([mnesia:dirty_read(topic, Name) || Name <- MatchedTopics]).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Id, _Opts]) ->
    process_flag(min_heap_size, 1024*1024),
    gproc_pool:connect_worker(pubsub, {?MODULE, Id}),
    %%TODO: gb_trees to replace maps?
    {ok, #state{id = Id, submap = maps:new()}}.

handle_call({subscribe, SubPid, Topics}, _From, State) ->
    TopicSubs = lists:map(fun({<<"$Q/", _/binary>> = Queue, Qos}) ->
                            #mqtt_queue{name = Queue, subpid = SubPid, qos = Qos};
                             ({Topic, Qos}) ->
                            {#mqtt_topic{topic = Topic, node = node()},
                             #mqtt_subscriber{topic = Topic, qos = Qos, pid = SubPid}}
                end, Topics),
    F = fun() ->
            lists:map(fun(QueueR) when is_record(QueueR, mqtt_queue) ->
                        add_queue(QueueR);
                         (TopicSub) ->
                        add_subscriber(TopicSub)
                end, TopicSubs)
        end,
    case mnesia:transaction(F) of
        {atomic, _Result} ->
            setstats(all),
            NewState = monitor_subscriber(SubPid, State),
            %%TODO: grant all qos
            {reply, {ok, [Qos || {_Topic, Qos} <- Topics]}, NewState};
        {aborted, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call({subscribe, SubPid, <<"$Q/", _/binary>> = Queue, Qos}, _From, State) ->
    case mnesia:dirty_read(queue, Queue) of
        [OldQueueR] -> lager:error("Queue is overwrited by ~p: ~p", [SubPid, OldQueueR]);
        [] -> ok
    end,
    QueueR = #mqtt_queue{name = Queue, subpid = SubPid, qos = Qos},
    case mnesia:transaction(fun add_queue/1, [QueueR]) of
        {atomic, ok} ->
            setstats(queues),
            {reply, {ok, Qos}, monitor_subscriber(SubPid, State)};
        {aborted, Error} -> 
            {reply, {error, Error}, State}
    end;

handle_call({subscribe, SubPid, Topic, Qos}, _From, State) ->
    TopicR = #mqtt_topic{topic = Topic, node = node()},
    Subscriber = #mqtt_subscriber{topic = Topic, qos = Qos, pid = SubPid},
    case mnesia:transaction(fun add_subscriber/1, [{TopicR, Subscriber}]) of
        {atomic, ok} -> 
            setstats(all),
            {reply, {ok, Qos}, monitor_subscriber(SubPid, State)};
        {aborted, Error} -> 
            {reply, {error, Error}, State}
    end;

handle_call(Req, _From, State) ->
    lager:error("Bad Request: ~p", [Req]),
	{reply, {error, badreq}, State}.

handle_cast({unsubscribe, SubPid, Topics}, State) when is_list(Topics) ->

    TopicSubs = lists:map(fun(<<"$Q/", _/binary>> = Queue) ->
                                #mqtt_queue{name = Queue, subpid = SubPid};
                             (Topic) ->
                                {#mqtt_topic{topic = Topic, node = node()},
                                 #mqtt_subscriber{topic = Topic, _ = '_', pid = SubPid}}
                end, Topics),
    F = fun() ->
            lists:foreach(
                fun(QueueR) when is_record(QueueR, mqtt_queue) ->
                        remove_queue(QueueR);
                    (TopicSub) ->
                        remove_subscriber(TopicSub)
                end, TopicSubs) 
        end,
    case mnesia:transaction(F) of
        {atomic, _} -> ok;
        {aborted, Error} -> lager:error("unsubscribe ~p error: ~p", [Topics, Error])
    end,
    setstats(all),
    {noreply, State};

handle_cast({unsubscribe, SubPid, <<"$Q/", _/binary>> = Queue}, State) ->
    QueueR = #mqtt_queue{name = Queue, subpid = SubPid},
    case mnesia:transaction(fun remove_queue/1, [QueueR]) of
        {atomic, _} -> 
            setstats(queues);
        {aborted, Error} -> 
            lager:error("unsubscribe queue ~s error: ~p", [Queue, Error])
    end,
    {noreply, State};

handle_cast({unsubscribe, SubPid, Topic}, State) ->
    TopicR = #mqtt_topic{topic = Topic, node = node()},
    Subscriber = #mqtt_subscriber{topic = Topic, _ = '_', pid = SubPid},
    case mnesia:transaction(fun remove_subscriber/1, [{TopicR, Subscriber}]) of
        {atomic, _} -> ok;
        {aborted, Error} -> lager:error("unsubscribe ~s error: ~p", [Topic, Error])
    end,
    setstats(all),
    {noreply, State};

handle_cast(Msg, State) ->
    lager:error("Bad Msg: ~p", [Msg]),
	{noreply, State}.

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State = #state{submap = SubMap}) ->
    case maps:is_key(DownPid, SubMap) of
        true ->
            Node = node(),
            F = fun() -> 
                    %% remove queue...
                    Queues = mnesia:match_object(queue, #mqtt_queue{subpid = DownPid, _ = '_'}, write),
                    lists:foreach(fun(QueueR) ->
                                mnesia:delete_object(queue, QueueR, write) 
                        end, Queues),

                    %% remove subscribers...
                    Subscribers = mnesia:index_read(subscriber, DownPid, #mqtt_subscriber.pid),
                    lists:foreach(fun(Sub = #mqtt_subscriber{topic = Topic}) ->
                                mnesia:delete_object(subscriber, Sub, write),
                                try_remove_topic(#mqtt_topic{topic = Topic, node = Node})
                        end, Subscribers)
            end,
            case catch mnesia:transaction(F) of
                {atomic, _} -> ok;
                {aborted, Reason} ->
                    lager:error("Failed to delete 'DOWN' subscriber ~p: ~p", [DownPid, Reason])
            end,
            setstats(all),
            {noreply, State#state{submap = maps:remove(DownPid, SubMap)}};
        false ->
            lager:error("Unexpected 'DOWN' from ~p", [DownPid]),
            {noreply, State}
    end;

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
	{noreply, State}.

terminate(_Reason, _State) ->
    TopicR = #mqtt_topic{_ = '_', node = node()},
    F = fun() ->
            [mnesia:delete_object(topic, R, write) || R <- mnesia:match_object(topic, TopicR, write)]
            %%TODO: remove trie??
        end,
    mnesia:transaction(F),
    setstats(all).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

add_queue(QueueR) ->
    mnesia:write(queue, QueueR, write).

add_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    case mnesia:wread({topic, Topic}) of
        [] ->
            ok = emqttd_trie:insert(Topic),
            mnesia:write(topic, TopicR, write);
        Records ->
            case lists:member(TopicR, Records) of
                true -> ok;
                false -> mnesia:write(topic, TopicR, write)
            end
    end.

%% Fix issue #53 - Remove Overlapping Subscriptions
add_subscriber({TopicR, Subscriber = #mqtt_subscriber{topic = Topic, qos = Qos, pid = SubPid}})
                when is_record(TopicR, mqtt_topic) ->
    case add_topic(TopicR) of
        ok ->
            OverlapSubs = [Sub || Sub = #mqtt_subscriber{topic = SubTopic, qos = SubQos}
                                    <- mnesia:index_read(subscriber, SubPid, #mqtt_subscriber.pid),
                                        SubTopic =:= Topic, SubQos =/= Qos],

            %% remove overlapping subscribers
            if
                length(OverlapSubs) =:= 0 -> ok;
                true ->
                    lager:warning("Remove overlapping subscribers: ~p", [OverlapSubs]),
                    [mnesia:delete_object(subscriber, OverlapSub, write) || OverlapSub <- OverlapSubs]
            end,

            %% insert subscriber
            mnesia:write(subscriber, Subscriber, write);
        Error -> 
            Error
    end.

monitor_subscriber(SubPid, State = #state{submap = SubMap}) ->
    NewSubMap = case maps:is_key(SubPid, SubMap) of
        false ->
            maps:put(SubPid, erlang:monitor(process, SubPid), SubMap);
        true ->
            SubMap
    end,
    State#state{submap = NewSubMap}.

remove_queue(#mqtt_queue{name = Name, subpid = Pid}) ->
    case mnesia:wread({queue, Name}) of
        [R = #mqtt_queue{subpid = Pid}] -> 
            mnesia:delete(queue, R, write);
        _ ->
            ok
    end.

remove_subscriber({TopicR, Subscriber}) when is_record(TopicR, mqtt_topic) ->
    [mnesia:delete_object(subscriber, Sub, write) || 
        Sub <- mnesia:match_object(subscriber, Subscriber, write)],
    try_remove_topic(TopicR).

try_remove_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    case mnesia:read({subscriber, Topic}) of
        [] ->
            mnesia:delete_object(topic, TopicR, write),
            case mnesia:read(topic, Topic) of
                [] -> emqttd_trie:delete(Topic);		
                _ -> ok
            end;
         _ -> 
            ok
 	end.

%%%=============================================================================
%%% Stats functions
%%%=============================================================================

setstats(all) ->
    [setstats(Stat) || Stat <- [queues, topics, subscribers]];

setstats(queues) ->
    emqttd_stats:setstats('queues/count', 'queues/max',
                           mnesia:table_info(queue, size));

setstats(topics) ->
    emqttd_stats:setstats('topics/count', 'topics/max',
                          mnesia:table_info(topic, size));
setstats(subscribers) ->
    emqttd_stats:setstats('subscribers/count', 'subscribers/max',
                           mnesia:table_info(subscriber, size)).

%%TODO: queue dropped?
setstats(dropped, false) ->
    ignore;
setstats(dropped, true) ->
    emqttd_metrics:inc('messages/dropped').


%%%=============================================================================
%%% Trace functions
%%%=============================================================================

trace(publish, From, _Msg) when is_atom(From) ->
    %%dont' trace broker publish
    ignore;

trace(publish, From, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:info([{client, From}, {topic, Topic}],
                    "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

