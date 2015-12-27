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
-export([start_link/4]).

-export([create/2, lookup/2, subscribe/1, subscribe/2,
         unsubscribe/1, unsubscribe/2, publish/1, delete/2]).

%% Subscriptions API

%% Local node
-export([match/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-ifdef(TEST).
-compile(export_all).
-endif.

-record(state, {pool, id, statsfun}).

-define(ROUTER, emqttd_router).

-define(HELPER, emqttd_pubsub_helper).

%%%=============================================================================
%%% Mnesia callbacks
%%%=============================================================================
mnesia(boot) ->
    ok = create_table(topic, ram_copies),
    if_subscription(fun(RamOrDisc) ->
                      ok = create_table(subscription, RamOrDisc)
                    end);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(topic),
    %% Only one disc_copy???
    if_subscription(fun(_RamOrDisc) ->
                      ok = emqttd_mnesia:copy_table(subscription)
                    end).

%% Topic Table
create_table(topic, RamOrDisc) ->
    emqttd_mnesia:create_table(topic, [
            {type, bag},
            {RamOrDisc, [node()]},
            {record_name, mqtt_topic},
            {attributes, record_info(fields, mqtt_topic)}]);

%% Subscription Table
create_table(subscription, RamOrDisc) ->
    emqttd_mnesia:create_table(subscription, [
            {type, bag},
            {RamOrDisc, [node()]},
            {record_name, mqtt_subscription},
            {attributes, record_info(fields, mqtt_subscription)},
            {storage_properties, [{ets, [compressed]},
                                  {dets, [{auto_save, 5000}]}]}]).

if_subscription(Fun) ->
    case env(subscription) of
        disc      -> Fun(disc_copies);
        ram       -> Fun(ram_copies);
        false     -> ok;
        undefined -> ok
    end.

env(Key) ->
    case get({pubsub, Key}) of
        undefined ->
            cache_env(Key);
        Val ->
            Val
    end.

cache_env(Key) ->
    Val = emqttd_opts:g(Key, emqttd_broker:env(pubsub)),
    put({pubsub, Key}, Val),
    Val.

%%%=============================================================================
%%% API
%%%=============================================================================

%%------------------------------------------------------------------------------
%% @doc Start one pubsub server
%% @end
%%------------------------------------------------------------------------------
-spec start_link(Pool, Id, StatsFun, Opts) -> {ok, pid()} | ignore | {error, any()} when
    Pool     :: atom(),
    Id       :: pos_integer(),
    StatsFun :: fun(),
    Opts     :: list(tuple()).
start_link(Pool, Id, StatsFun, Opts) ->
    gen_server2:start_link({local, name(Id)}, ?MODULE, [Pool, Id, StatsFun, Opts], []).

name(Id) ->
    list_to_atom("emqttd_pubsub_" ++ integer_to_list(Id)).

%%------------------------------------------------------------------------------
%% @doc Create Topic or Subscription.
%% @end
%%------------------------------------------------------------------------------
-spec create(topic | subscription, binary()) -> ok | {error, any()}.
create(topic, Topic) when is_binary(Topic) ->
    Record = #mqtt_topic{topic = Topic, node = node()},
    case mnesia:transaction(fun add_topic/1, [Record]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end;

create(subscription, {SubId, Topic, Qos}) when is_binary(SubId) andalso is_binary(Topic) ->
    case mnesia:transaction(fun add_subscription/2, [SubId, {Topic, Qos}]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end.

%%------------------------------------------------------------------------------
%% @doc Lookup Topic or Subscription.
%% @end
%%------------------------------------------------------------------------------
-spec lookup(topic | subscription, binary()) -> list().
lookup(topic, Topic) ->
    mnesia:dirty_read(topic, Topic);

lookup(subscription, ClientId) ->
    mnesia:dirty_read(subscription, ClientId).

%%------------------------------------------------------------------------------
%% @doc Delete Topic or Subscription.
%% @end
%%------------------------------------------------------------------------------
delete(topic, _Topic) ->
    {error, unsupported};

delete(subscription, ClientId) when is_binary(ClientId) ->
    mnesia:dirty_delete({subscription, ClientId});

delete(subscription, {ClientId, Topic}) when is_binary(ClientId) ->
    mnesia:async_dirty(fun remove_subscriptions/2, [ClientId, [Topic]]).

%%------------------------------------------------------------------------------
%% @doc Subscribe Topics
%% @end
%%------------------------------------------------------------------------------
-spec subscribe({Topic, Qos} | list({Topic, Qos})) ->
    {ok, Qos | list(Qos)} | {error, any()} when
    Topic   :: binary(),
    Qos     :: mqtt_qos() | mqtt_qos_name().
subscribe({Topic, Qos}) ->
    subscribe([{Topic, Qos}]);
subscribe(TopicTable) when is_list(TopicTable) ->
    call({subscribe, {undefined, self()}, fixqos(TopicTable)}).

-spec subscribe(ClientId, {Topic, Qos} | list({Topic, Qos})) ->
    {ok, Qos | list(Qos)} | {error, any()} when
    ClientId :: binary(),
    Topic    :: binary(),
    Qos      :: mqtt_qos() | mqtt_qos_name().
subscribe(ClientId, {Topic, Qos}) when is_binary(ClientId) ->
    subscribe(ClientId, [{Topic, Qos}]);
subscribe(ClientId, TopicTable) when is_binary(ClientId) andalso is_list(TopicTable) ->
    call({subscribe, {ClientId, self()}, fixqos(TopicTable)}).

fixqos(TopicTable) ->
    [{Topic, ?QOS_I(Qos)} || {Topic, Qos} <- TopicTable].

call(Request) ->
    PubSub = gproc_pool:pick_worker(pubsub, self()),
    gen_server2:call(PubSub, Request, infinity).

%%------------------------------------------------------------------------------
%% @doc Unsubscribe Topic or Topics
%% @end
%%------------------------------------------------------------------------------
-spec unsubscribe(binary() | list(binary())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe([Topic]);
unsubscribe(Topics = [Topic|_]) when is_binary(Topic) ->
    cast({unsubscribe, {undefined, self()}, Topics}).

-spec unsubscribe(binary(), binary() | list(binary())) -> ok.
unsubscribe(ClientId, Topic) when is_binary(ClientId) andalso is_binary(Topic) ->
    unsubscribe(ClientId, [Topic]);
unsubscribe(ClientId, Topics = [Topic|_]) when is_binary(Topic) ->
    cast({unsubscribe, {ClientId, self()}, Topics}).

cast(Msg) ->
    PubSub = gproc_pool:pick_worker(pubsub, self()),
    gen_server2:cast(PubSub, Msg).

%%------------------------------------------------------------------------------
%% @doc Publish to cluster nodes
%% @end
%%------------------------------------------------------------------------------
-spec publish(Msg :: mqtt_message()) -> ok.
publish(Msg = #mqtt_message{from = From}) ->
    trace(publish, From, Msg),
    Msg1 = #mqtt_message{topic = To}
               = emqttd_broker:foldl_hooks('message.publish', [], Msg),

    %% Retain message first. Don't create retained topic.
    case emqttd_retainer:retain(Msg1) of
        ok ->
            %% TODO: why unset 'retain' flag?
            publish(To, emqttd_message:unset_flag(Msg1));
        ignore ->
            publish(To, Msg1)
     end.

publish(To, Msg) ->
    lists:foreach(fun(#mqtt_topic{topic = Topic, node = Node}) ->
                    case Node =:= node() of
                        true  -> ?ROUTER:route(Topic, Msg);
                        false -> rpc:cast(Node, ?ROUTER, route, [Topic, Msg])
                    end
                  end, match(To)).

%%------------------------------------------------------------------------------
%% @doc Match Topic Name with Topic Filters
%% @end
%%------------------------------------------------------------------------------
-spec match(binary()) -> [mqtt_topic()].
match(To) ->
    MatchedTopics = mnesia:async_dirty(fun emqttd_trie:match/1, [To]),
    %% ets:lookup for topic table will be replicated.
    lists:append([ets:lookup(topic, Topic) || Topic <- MatchedTopics]).

%%%=============================================================================
%%% gen_server callbacks
%%%=============================================================================

init([Pool, Id, StatsFun, Opts]) ->
    ?ROUTER:init(Opts),
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, statsfun = StatsFun}}.

handle_call({subscribe, {SubId, SubPid}, TopicTable}, _From,
            State = #state{statsfun = StatsFun}) ->

    Topics = [Topic || {Topic, _Qos} <- TopicTable],

    %% Add routes first
    ?ROUTER:add_routes(Topics, SubPid),

    %% Insert topic records to global topic table
    Records = [#mqtt_topic{topic = Topic, node = node()} || Topic <- Topics],

    case mnesia:transaction(fun add_topics/1, [Records]) of
        {atomic, _} ->
            StatsFun(topic),
            if_subscription(
                fun(_) ->
                    %% Add subscriptions
                    Args = [fun add_subscriptions/2, [SubId, TopicTable]],
                    emqttd_pooler:async_submit({mnesia, async_dirty, Args}),
                    StatsFun(subscription)
                end),
            %% Grant all qos...
            {reply, {ok, [Qos || {_Topic, Qos} <- TopicTable]}, State};
        {aborted, Error} ->
            {reply, {error, Error}, State}
    end;

handle_call(Req, _From, State) ->
   ?UNEXPECTED_REQ(Req, State).

handle_cast({unsubscribe, {SubId, SubPid}, Topics}, State = #state{statsfun = StatsFun}) ->
    %% Delete routes first
    ?ROUTER:delete_routes(Topics, SubPid),

    %% Remove subscriptions
    if_subscription(
        fun(_) ->
            Args = [fun remove_subscriptions/2, [SubId, Topics]],
            emqttd_pooler:async_submit({mnesia, async_dirty, Args}),
            StatsFun(subscription)
        end),
    {noreply, State};

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State) ->

    Routes = ?ROUTER:lookup_routes(DownPid),

    %% Delete all routes of the process
    ?ROUTER:delete_routes(DownPid),

    ?HELPER:aging([Topic || Topic <- Routes, not ?ROUTER:has_route(Topic)]),

    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%%=============================================================================
%%% Internal functions
%%%=============================================================================

add_topics(Records) ->
    lists:foreach(fun add_topic/1, Records).

add_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    case mnesia:wread({topic, Topic}) of
        [] ->
            ok = emqttd_trie:insert(Topic),
            mnesia:write(topic, TopicR, write);
        Records ->
            case lists:member(TopicR, Records) of
                true  -> ok;
                false -> mnesia:write(topic, TopicR, write)
            end
    end.

add_subscriptions(undefined, _TopicTable) ->
    ok;
add_subscriptions(SubId, TopicTable) ->
    lists:foreach(fun({Topic, Qos}) ->
                    add_subscription(SubId, {Topic, Qos})
                  end,TopicTable).

add_subscription(SubId, {Topic, Qos}) ->
    Subscription = #mqtt_subscription{subid = SubId, topic = Topic, qos = Qos},
    Pattern = #mqtt_subscription{subid = SubId, topic = Topic, qos = '_'},
    Records = mnesia:match_object(subscription, Pattern, write),
    case lists:member(Subscription, Records) of
        true  ->
            ok;
        false ->
            [delete_subscription(Record) || Record <- Records],
            insert_subscription(Subscription)
    end.

insert_subscription(Record) ->
    mnesia:write(subscription, Record, write).

remove_subscriptions(undefined, _Topics) ->
    ok;
remove_subscriptions(SubId, Topics) ->
    lists:foreach(fun(Topic) ->
         Pattern = #mqtt_subscription{subid = SubId, topic = Topic, qos = '_'},
         Records = mnesia:match_object(subscription, Pattern, write),
         lists:foreach(fun delete_subscription/1, Records)
     end, Topics).

delete_subscription(Record) ->
    mnesia:delete_object(subscription, Record, write).

%%%=============================================================================
%%% Trace Functions
%%%=============================================================================

trace(publish, From, _Msg) when is_atom(From) ->
    %% Dont' trace '$SYS' publish
    ignore;

trace(publish, From, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:info([{client, From}, {topic, Topic}],
               "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

