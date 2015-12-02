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
%%% @doc emqttd pubsub
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%
%%%-----------------------------------------------------------------------------
-module(emqttd_pubsub).

-behaviour(gen_server2).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

%% Mnesia Callbacks
-export([mnesia/1]).

-boot_mnesia({mnesia, [boot]}).
-copy_mnesia({mnesia, [copy]}).

%% API Exports 
-export([start_link/3]).

-export([create/1, subscribe/1, subscribe/2, unsubscribe/1, publish/1]).

%% Local node
-export([match/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id}).

-define(ROUTER, emqttd_router).

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
    %% subscription table
    ok = emqttd_mnesia:create_table(subscription, [
                {type, bag},
                {ram_copies, [node()]},
                {record_name, mqtt_subscription},
                {attributes, record_info(fields, mqtt_subscription)}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(topic),
    ok = emqttd_mnesia:copy_table(subscription).

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start one pubsub server
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Pool, Id, Opts) -> {ok, pid()} | ignore | {error, any()} when
    Pool :: atom(),
    Id   :: pos_integer(),
    Opts :: list(tuple()).
start_link(Pool, Id, Opts) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Pool, Id, Opts], []).

name(Id) ->
    list_to_atom("emqttd_pubsub_" ++ integer_to_list(Id)).

%%------------------------------------------------------------------------------
%% @doc Create topic. Notice That this transaction is not protected by pubsub pool
%% @end
%%------------------------------------------------------------------------------
-spec create(Topic :: binary()) -> ok | {error, Error :: any()}.
create(Topic) when is_binary(Topic) ->
    case mnesia:transaction(fun add_topic/1, [#mqtt_topic{topic = Topic, node = node()}]) of
        {atomic, ok}     -> setstats(topics), ok;
        {aborted, Error} -> {error, Error}
    end.

%%------------------------------------------------------------------------------
%% @doc Subscribe Topic
%% @end
%%------------------------------------------------------------------------------
-spec subscribe(Topic, Qos) -> {ok, Qos} when
     Topic :: binary(),
     Qos   :: mqtt_qos() | mqtt_qos_name().
subscribe(Topic, Qos) ->
    %%TODO:...
    subscribe([{Topic, Qos}]).

-spec subscribe({Topic, Qos} | list({Topic, Qos})) ->
    {ok, Qos | list(Qos)} | {error, any()} when
    Topic   :: binary(),
    Qos     :: mqtt_qos() | mqtt_qos_name().
subscribe({Topic, Qos}) when is_binary(Topic) andalso (?IS_QOS(Qos) orelse is_atom(Qos)) ->
    %%TODO:...
    subscribe([{Topic, Qos}]);

subscribe(TopicTable0 = [{_Topic, _Qos} | _]) ->
    Self = self(),
    TopicTable = [{Topic, ?QOS_I(Qos)} || {Topic, Qos} <- TopicTable0],
    ?ROUTER:add_routes(TopicTable, Self),
    PubSub = gproc_pool:pick_worker(pubsub, Self),
    SubReq = {subscribe, Self, TopicTable},
    gen_server2:call(PubSub, SubReq, infinity).

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topic or Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe([Topic]);

unsubscribe(Topics = [Topic|_]) when is_binary(Topic) ->
    Self = self(),
    ?ROUTER:delete_routes(Topics, Self),
    PubSub = gproc_pool:pick_worker(pubsub, Self),
    gen_server2:cast(PubSub, {unsubscribe, Self, Topics}).

%%------------------------------------------------------------------------------
%% @doc Publish to cluster nodes
%% @end
%%------------------------------------------------------------------------------
-spec publish(Msg :: mqtt_message()) -> ok.
publish(Msg = #mqtt_message{from = From}) ->
    trace(publish, From, Msg),
    Msg1 = #mqtt_message{topic = Topic}
               = emqttd_broker:foldl_hooks('message.publish', [], Msg),

    %% Retain message first. Don't create retained topic.
    case emqttd_retainer:retain(Msg1) of
        ok ->
            %% TODO: why unset 'retain' flag?
            publish(Topic, emqttd_message:unset_flag(Msg1));
        ignore ->
            publish(Topic, Msg1)
     end.

publish(Topic, Msg) when is_binary(Topic) ->
	lists:foreach(fun(#mqtt_topic{topic=Name, node=Node}) ->
                rpc:cast(Node, ?ROUTER, route, [Name, Msg])
        end, match(Topic)).

%%TODO: Benchmark and refactor...
-spec match(Topic :: binary()) -> [mqtt_topic()].
match(Topic) when is_binary(Topic) ->
	MatchedTopics = mnesia:async_dirty(fun emqttd_trie:match/1, [Topic]),
	lists:append([mnesia:dirty_read(topic, Name) || Name <- MatchedTopics]).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Pool, Id, _Opts]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id}}.

%%TODO: clientId???
handle_call({subscribe, _SubPid, TopicTable}, _From, State) ->
    Records = [#mqtt_topic{topic = Topic, node = node()} || {Topic, _Qos} <- TopicTable],
    case mnesia:transaction(fun() -> [add_topic(Record) || Record <- Records] end) of
        {atomic, _Result} ->
            {reply, {ok, [Qos || {_Topic, Qos} <- TopicTable]}, setstats(State)};
        {aborted, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call(Req, _From, State) ->
    lager:error("Bad Request: ~p", [Req]),
	{reply, {error, badreq}, State}.

%%TODO: clientId???
handle_cast({unsubscribe, SubPid, Topics}, State) when is_list(Topics) ->
    {noreply, State};

handle_cast(Msg, State) ->
    lager:error("Bad Msg: ~p", [Msg]),
	{noreply, State}.

handle_info(Info, State) ->
    lager:error("Unexpected Info: ~p", [Info]),
	{noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id), setstats(all).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

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

setstats(State) ->
    emqttd_stats:setstats('topics/count', 'topics/max',
                          mnesia:table_info(topic, size)), State.

%%%=============================================================================
%%% Trace functions
%%%=============================================================================

trace(publish, From, _Msg) when is_atom(From) ->
    %%dont' trace broker publish
    ignore;

trace(publish, From, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:info([{client, From}, {topic, Topic}],
                    "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

