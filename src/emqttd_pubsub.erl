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
-export([start_link/3, create_topic/1, lookup_topic/1]).

-export([subscribe/2, unsubscribe/2, publish/2, dispatch/2,
         async_subscribe/2, async_unsubscribe/2]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env}).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(topic, [
                {ram_copies, [node()]},
                {record_name, mqtt_topic},
                {attributes, record_info(fields, mqtt_topic)}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(topic).

%%--------------------------------------------------------------------
%% Start PubSub
%%--------------------------------------------------------------------

%% @doc Start one pubsub
-spec(start_link(Pool, Id, Env) -> {ok, pid()} | ignore | {error, any()} when
      Pool :: atom(),
      Id   :: pos_integer(),
      Env  :: list(tuple())).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%% @doc Create a Topic.
-spec(create_topic(binary()) -> ok | {error, any()}).
create_topic(Topic) when is_binary(Topic) ->
    case mnesia:transaction(fun add_topic_/2, [Topic, [static]]) of
        {atomic, ok}     -> ok;
        {aborted, Error} -> {error, Error}
    end.

%% @doc Lookup a Topic.
-spec(lookup_topic(binary()) -> list(mqtt_topic())).
lookup_topic(Topic) when is_binary(Topic) ->
    mnesia:dirty_read(topic, Topic).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe a Topic
-spec(subscribe(binary(), pid()) -> ok).
subscribe(Topic, SubPid) when is_binary(Topic) ->
    call(pick(Topic), {subscribe, Topic, SubPid}).

%% @doc Asynchronous Subscribe
-spec(async_subscribe(binary(), pid()) -> ok).
async_subscribe(Topic, SubPid) when is_binary(Topic) ->
    cast(pick(Topic), {subscribe, Topic, SubPid}).

%% @doc Publish message to Topic.
-spec(publish(binary(), any()) -> any()).
publish(Topic, Msg) ->
    lists:foreach(
        fun(#mqtt_route{topic = To, node = Node}) when Node =:= node() ->
            ?MODULE:dispatch(To, Msg);
           (#mqtt_route{topic = To, node = Node}) ->
            rpc:cast(Node, ?MODULE, dispatch, [To, Msg])
        end, emqttd_router:lookup(Topic)).

%% @doc Dispatch Message to Subscribers
-spec(dispatch(binary(), mqtt_message()) -> ok).
dispatch(Queue = <<"$queue/", _Q/binary>>, Msg) ->
    case subscribers(Queue) of
        [] ->
            dropped(Queue);
        [SubPid] ->
            SubPid ! {dispatch, Queue, Msg};
        SubPids ->
            Idx = crypto:rand_uniform(1, length(SubPids) + 1),
            SubPid = lists:nth(Idx, SubPids),
            SubPid ! {dispatch, Queue, Msg}
    end;

dispatch(Topic, Msg) ->
    case subscribers(Topic) of
        [] ->
            dropped(Topic);
        [SubPid] ->
            SubPid ! {dispatch, Topic, Msg};
        SubPids ->
            lists:foreach(fun(SubPid) ->
                SubPid ! {dispatch, Topic, Msg}
            end, SubPids)
    end.

%% @private
%% @doc Find all subscribers
subscribers(Topic) ->
    case ets:member(subscriber, Topic) of
        true -> %% faster then lookup?
            try ets:lookup_element(subscriber, Topic, 2) catch error:badarg -> [] end;
        false ->
            []
    end.

%% @private
%% @doc Ingore $SYS Messages.
dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqttd_metrics:inc('messages/dropped').

%% @doc Unsubscribe
-spec(unsubscribe(binary(), pid()) -> ok).
unsubscribe(Topic, SubPid) when is_binary(Topic) ->
    call(pick(Topic), {unsubscribe, Topic, SubPid}).

%% @doc Asynchronous Unsubscribe
-spec(async_unsubscribe(binary(), pid()) -> ok).
async_unsubscribe(Topic, SubPid)  when is_binary(Topic) ->
    cast(pick(Topic), {unsubscribe, Topic, SubPid}).

call(PubSub, Req) when is_pid(PubSub) ->
    gen_server2:call(PubSub, Req, infinity).

cast(PubSub, Msg) when is_pid(PubSub) ->
    gen_server2:cast(PubSub, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(pubsub, Topic).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env}}.

handle_call({subscribe, Topic, SubPid}, _From, State) ->
    add_subscriber_(Topic, SubPid),
	{reply, ok, setstats(State)};

handle_call({unsubscribe, Topic, SubPid}, _From, State) ->
    del_subscriber_(Topic, SubPid),
	{reply, ok, setstats(State)};

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, Topic, SubPid}, State) ->
    add_subscriber_(Topic, SubPid),
	{noreply, setstats(State)};

handle_cast({unsubscribe, Topic, SubPid}, State) ->
    del_subscriber_(Topic, SubPid),
	{noreply, setstats(State)};

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

add_subscriber_(Topic, SubPid) ->
    case ets:member(subscriber, Topic) of
        false ->
            mnesia:transaction(fun add_topic_route_/2, [Topic, node()]),
            setstats(topic);
        true ->
            ok
    end,
    ets:insert(subscriber, {Topic, SubPid}).

del_subscriber_(Topic, SubPid) ->
    ets:delete_object(subscriber, {Topic, SubPid}),
    case ets:lookup(subscriber, Topic) of
        [] ->
            mnesia:transaction(fun del_topic_route_/2, [Topic, node()]),
            setstats(topic);
        [_|_] ->
            ok
    end.

add_topic_route_(Topic, Node) ->
    add_topic_(Topic), emqttd_router:add_route(Topic, Node).

add_topic_(Topic) ->
    add_topic_(Topic, []).

add_topic_(Topic, Flags) ->
    Record = #mqtt_topic{topic = Topic, flags = Flags},
    case mnesia:wread({topic, Topic}) of
        []  -> mnesia:write(topic, Record, write);
        [_] -> ok
    end.

del_topic_route_(Topic, Node) ->
    emqttd_router:del_route(Topic, Node), del_topic_(Topic).

del_topic_(Topic) ->
    case emqttd_router:has_route(Topic) of
        true  -> ok;
        false -> do_del_topic_(Topic)
    end.

do_del_topic_(Topic) ->
    case mnesia:wread({topic, Topic}) of
        [#mqtt_topic{flags = []}] ->
            mnesia:delete(topic, Topic, write);
        _ ->
            ok
    end.

setstats(State) when is_record(State, state) ->
    setstats(subscriber), State;

setstats(topic) ->
    emqttd_stats:setstats('topics/count', 'topics/max', mnesia:table_info(topic, size));

setstats(subscriber) ->
    emqttd_stats:setstats('subscribers/count', 'subscribers/max', ets:info(subscriber, size)).

