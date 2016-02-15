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
         publish/1, unsubscribe/1, unsubscribe/2, delete/2]).

%% Local node
-export([match/1]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, statsfun}).

-define(ROUTER, emqttd_router).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------
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
    Val = proplists:get_value(Key, emqttd_broker:env(pubsub)),
    put({pubsub, Key}, Val),
    Val.

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

%% @doc Start one pubsub server
-spec start_link(Pool, Id, StatsFun, Opts) -> {ok, pid()} | ignore | {error, any()} when
    Pool     :: atom(),
    Id       :: pos_integer(),
    StatsFun :: fun((atom()) -> any()),
    Opts     :: list(tuple()).
start_link(Pool, Id, StatsFun, Opts) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)},
                           ?MODULE, [Pool, Id, StatsFun, Opts], []).

%% @doc Create Topic or Subscription.
-spec create(topic, emqttd_topic:topic()) -> ok | {error, any()};
            (subscription, {binary(), binary(), mqtt_qos()}) -> ok | {error, any()}.
create(topic, Topic) when is_binary(Topic) ->
    Record = #mqtt_topic{topic = Topic, node = node()},
    case mnesia:transaction(fun add_topic/1, [Record]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end;

create(subscription, {SubId, Topic, Qos}) when is_binary(SubId) andalso is_binary(Topic) ->
    case mnesia:transaction(fun add_subscription/2, [SubId, {Topic, ?QOS_I(Qos)}]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end.

%% @doc Lookup Topic or Subscription.
-spec lookup(topic, emqttd_topic:topic()) -> list(mqtt_topic());
            (subscription, binary())      -> list(mqtt_subscription()).
lookup(topic, Topic) when is_binary(Topic) ->
    mnesia:dirty_read(topic, Topic);

lookup(subscription, SubId) when is_binary(SubId) ->
    mnesia:dirty_read(subscription, SubId).

%% @doc Delete Topic or Subscription.
-spec delete(topic, emqttd_topic:topic()) -> ok | {error, any()};
            (subscription, binary() | {binary(), emqttd_topic:topic()}) -> ok.
delete(topic, _Topic) ->
    {error, unsupported};

delete(subscription, SubId) when is_binary(SubId) ->
    mnesia:dirty_delete({subscription, SubId});

delete(subscription, {SubId, Topic}) when is_binary(SubId) andalso is_binary(Topic) ->
    mnesia:async_dirty(fun remove_subscriptions/2, [SubId, [Topic]]).

%% @doc Subscribe Topics
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

%% @doc Unsubscribe Topic or Topics
-spec unsubscribe(emqttd_topic:topic() | list(emqttd_topic:topic())) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe([Topic]);
unsubscribe(Topics = [Topic|_]) when is_binary(Topic) ->
    cast({unsubscribe, {undefined, self()}, Topics}).

-spec unsubscribe(binary(), emqttd_topic:topic() | list(emqttd_topic:topic())) -> ok.
unsubscribe(ClientId, Topic) when is_binary(ClientId) andalso is_binary(Topic) ->
    unsubscribe(ClientId, [Topic]);
unsubscribe(ClientId, Topics = [Topic|_]) when is_binary(Topic) ->
    cast({unsubscribe, {ClientId, self()}, Topics}).

call(Request) ->
    gen_server2:call(pick(self()), Request, infinity).

cast(Msg) ->
    gen_server2:cast(pick(self()), Msg).

pick(Self) -> gproc_pool:pick_worker(pubsub, Self).

%% @doc Publish to cluster nodes
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

%% @doc Match Topic Name with Topic Filters
-spec match(emqttd_topic:topic()) -> [mqtt_topic()].
match(To) ->
    Matched = mnesia:async_dirty(fun emqttd_trie:match/1, [To]),
    %% ets:lookup for topic table will be replicated to all nodes.
    lists:append([ets:lookup(topic, Topic) || Topic <- [To | Matched]]).

%%--------------------------------------------------------------------
%% gen_server callbacks
%%--------------------------------------------------------------------

init([Pool, Id, StatsFun, _Opts]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, statsfun = StatsFun}}.

handle_call({subscribe, {SubId, SubPid}, TopicTable}, _From,
            State = #state{statsfun = StatsFun}) ->

    %% Monitor SubPid first
    try_monitor(SubPid),

    %% Topics
    Topics = [Topic || {Topic, _Qos} <- TopicTable],

    NewTopics = Topics -- reverse_routes(SubPid),

    %% Add routes
    ?ROUTER:add_routes(NewTopics, SubPid),

    insert_reverse_routes(SubPid, NewTopics),

    StatsFun(reverse_route),

    %% Insert topic records to mnesia
    Records = [#mqtt_topic{topic = Topic, node = node()} || Topic <- NewTopics],

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

    delete_reverse_routes(SubPid, Topics),

    StatsFun(reverse_route),

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

handle_info({'DOWN', _Mon, _Type, DownPid, _Info}, State = #state{statsfun = StatsFun}) ->

    Topics = reverse_routes(DownPid),

    ?ROUTER:delete_routes(Topics, DownPid),

    delete_reverse_routes(DownPid),

    StatsFun(reverse_route),

    {noreply, State, hibernate};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

add_topics(Records) ->
    lists:foreach(fun add_topic/1, Records).

add_topic(TopicR = #mqtt_topic{topic = Topic}) ->
    case mnesia:wread({topic, Topic}) of
        [] ->
            case emqttd_topic:wildcard(Topic) of
                true  -> emqttd_trie:insert(Topic);
                false -> ok
            end,
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
        true ->
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

reverse_routes(SubPid) ->
    case ets:member(reverse_route, SubPid) of
        true  ->
            try ets:lookup_element(reverse_route, SubPid, 2) catch error:badarg -> [] end;
        false ->
            []
    end.

insert_reverse_routes(SubPid, Topics) ->
    ets:insert(reverse_route, [{SubPid, Topic} || Topic <- Topics]).

delete_reverse_routes(SubPid, Topics) ->
    lists:foreach(fun(Topic) ->
                ets:delete_object(reverse_route, {SubPid, Topic})
        end, Topics).

delete_reverse_routes(SubPid) ->
    ets:delete(reverse_route, SubPid).

try_monitor(SubPid) ->
    case ets:member(reverse_route, SubPid) of
        true  -> ignore;
        false -> erlang:monitor(process, SubPid)
    end.

%%--------------------------------------------------------------------
%% Trace Functions
%%--------------------------------------------------------------------

trace(publish, From, _Msg) when is_atom(From) ->
    %% Dont' trace '$SYS' publish
    ignore;

trace(publish, From, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:info([{client, From}, {topic, Topic}],
               "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

