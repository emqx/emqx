%%--------------------------------------------------------------------
%% Copyright (c) 2013-2017 EMQ Enterprise, Inc. (http://emqtt.io)
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

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_internal.hrl").

-export([start_link/3]).

%% PubSub API.
-export([subscribe/3, async_subscribe/3, publish/2, unsubscribe/3,
         async_unsubscribe/3, subscribers/1]).

-export([dispatch/2]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env}).

-define(PUBSUB, ?MODULE).

-define(is_local(Options), lists:member(local, Options)).

%%--------------------------------------------------------------------
%% Start PubSub
%%--------------------------------------------------------------------

-spec(start_link(atom(), pos_integer(), list()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe to a Topic
-spec(subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
subscribe(Topic, Subscriber, Options) ->
    call(pick(Topic), {subscribe, Topic, Subscriber, Options}).

-spec(async_subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_subscribe(Topic, Subscriber, Options) ->
    cast(pick(Topic), {subscribe, Topic, Subscriber, Options}).

%% @doc Publish MQTT Message to Topic.
-spec(publish(binary(), mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Topic, Msg) ->
    route(lists:append(emqttd_router:match(Topic),
                       emqttd_router:match_local(Topic)), delivery(Msg)).

route([], #mqtt_delivery{message = #mqtt_message{topic = Topic}}) ->
    dropped(Topic), ignore;

%% Dispatch on the local node.
route([#mqtt_route{topic = To, node = Node}],
      Delivery = #mqtt_delivery{flows = Flows}) when Node =:= node() ->
    dispatch(To, Delivery#mqtt_delivery{flows = [{route, Node, To} | Flows]});

%% Forward to other nodes
route([#mqtt_route{topic = To, node = Node}], Delivery = #mqtt_delivery{flows = Flows}) ->
    forward(Node, To, Delivery#mqtt_delivery{flows = [{route, Node, To}|Flows]});

route(Routes, Delivery) ->
    {ok, lists:foldl(fun(Route, Acc) ->
                    {ok, Acc1} = route([Route], Acc), Acc1
            end, Delivery, Routes)}.

delivery(Msg) -> #mqtt_delivery{sender = self(), message = Msg, flows = []}.

%% @doc Forward message to another node...
forward(Node, To, Delivery) ->
    rpc:cast(Node, ?PUBSUB, dispatch, [To, Delivery]), {ok, Delivery}.

%% @doc Dispatch Message to Subscribers.
-spec(dispatch(binary(), mqtt_delivery()) -> mqtt_delivery()).
dispatch(Topic, Delivery = #mqtt_delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            dropped(Topic), {ok, Delivery};
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg),
            {ok, Delivery#mqtt_delivery{flows = [{dispatch, Topic, 1}|Flows]}};
        Subscribers ->
            Flows1 = [{dispatch, Topic, length(Subscribers)} | Flows],
            lists:foreach(fun(Sub) -> dispatch(Sub, Topic, Msg) end, Subscribers),
            {ok, Delivery#mqtt_delivery{flows = Flows1}}
    end.

%%TODO: Is SubPid aliving???
dispatch(SubPid, Topic, Msg) when is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg};
dispatch({SubId, SubPid}, Topic, Msg) when is_binary(SubId), is_pid(SubPid) ->
    SubPid ! {dispatch, Topic, Msg};
dispatch({{share, _Share}, [Sub]}, Topic, Msg) ->
    dispatch(Sub, Topic, Msg);
dispatch({{share, _Share}, []}, _Topic, _Msg) ->
    ok;
dispatch({{share, _Share}, Subs}, Topic, Msg) -> %% round-robbin?
    dispatch(lists:nth(rand:uniform(length(Subs)), Subs), Topic, Msg).

subscribers(Topic) ->
    group_by_share(try ets:lookup_element(mqtt_subscriber, Topic, 2) catch error:badarg -> [] end).

group_by_share([]) -> [];

group_by_share(Subscribers) ->
    {Subs1, Shares1} =
    lists:foldl(fun({share, Share, Sub}, {Subs, Shares}) ->
                    {Subs, dict:append({share, Share}, Sub, Shares)};
                   (Sub, {Subs, Shares}) ->
                    {[Sub|Subs], Shares}
                end, {[], dict:new()}, Subscribers),
    lists:append(Subs1, dict:to_list(Shares1)).

%% @private
%% @doc Ingore $SYS Messages.
dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqttd_metrics:inc('messages/dropped').

%% @doc Unsubscribe
-spec(unsubscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
unsubscribe(Topic, Subscriber, Options) ->
    call(pick(Topic), {unsubscribe, Topic, Subscriber, Options}).

-spec(async_unsubscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_unsubscribe(Topic, Subscriber, Options) ->
    cast(pick(Topic), {unsubscribe, Topic, Subscriber, Options}).

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
    {ok, #state{pool = Pool, id = Id, env = Env},
     hibernate, {backoff, 2000, 2000, 20000}}.

handle_call({subscribe, Topic, Subscriber, Options}, _From, State) ->
    add_subscriber(Topic, Subscriber, Options),
    reply(ok, setstats(State));

handle_call({unsubscribe, Topic, Subscriber, Options}, _From, State) ->
    del_subscriber(Topic, Subscriber, Options),
    reply(ok, setstats(State));

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, Topic, Subscriber, Options}, State) ->
    add_subscriber(Topic, Subscriber, Options),
    noreply(setstats(State));

handle_cast({unsubscribe, Topic, Subscriber, Options}, State) ->
    del_subscriber(Topic, Subscriber, Options),
    noreply(setstats(State));

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internel Functions
%%--------------------------------------------------------------------

add_subscriber(Topic, Subscriber, Options) ->
    Share = proplists:get_value(share, Options),
    case ?is_local(Options) of
        false -> add_global_subscriber(Share, Topic, Subscriber);
        true  -> add_local_subscriber(Share, Topic, Subscriber)
    end.

add_global_subscriber(Share, Topic, Subscriber) ->
    case ets:member(mqtt_subscriber, Topic) and emqttd_router:has_route(Topic) of
        true  -> ok;
        false -> emqttd_router:add_route(Topic)
    end,
    ets:insert(mqtt_subscriber, {Topic, shared(Share, Subscriber)}).

add_local_subscriber(Share, Topic, Subscriber) ->
    (not ets:member(mqtt_subscriber, {local, Topic})) andalso emqttd_router:add_local_route(Topic),
    ets:insert(mqtt_subscriber, {{local, Topic}, shared(Share, Subscriber)}).

del_subscriber(Topic, Subscriber, Options) ->
    Share = proplists:get_value(share, Options),
    case ?is_local(Options) of
        false -> del_global_subscriber(Share, Topic, Subscriber);
        true  -> del_local_subscriber(Share, Topic, Subscriber)
    end.

del_global_subscriber(Share, Topic, Subscriber) ->
    ets:delete_object(mqtt_subscriber, {Topic, shared(Share, Subscriber)}),
    (not ets:member(mqtt_subscriber, Topic)) andalso emqttd_router:del_route(Topic).

del_local_subscriber(Share, Topic, Subscriber) ->
    ets:delete_object(mqtt_subscriber, {{local, Topic}, shared(Share, Subscriber)}),
    (not ets:member(mqtt_subscriber, {local, Topic})) andalso emqttd_router:del_local_route(Topic).

shared(undefined, Subscriber) ->
    Subscriber;
shared(Share, Subscriber) ->
    {share, Share, Subscriber}.

setstats(State) ->
    emqttd_stats:setstats('subscribers/count', 'subscribers/max', ets:info(mqtt_subscriber, size)),
    State.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

noreply(State) ->
    {noreply, State, hibernate}.

