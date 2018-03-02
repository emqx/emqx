%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Enterprise, Inc. All Rights Reserved.
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

-module(emqx_pubsub).

-behaviour(gen_server).

-include("emqx.hrl").

-include("emqx_mqtt.hrl").

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
    gen_server:start_link(?MODULE, [Pool, Id, Env], [{hibernate_after, 10000}]).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe to a Topic
-spec(subscribe(binary(), emqx:subscriber(), [emqx:suboption()]) -> ok).
subscribe(Topic, Subscriber, Options) ->
    call(pick(Topic), {subscribe, Topic, Subscriber, Options}).

-spec(async_subscribe(binary(), emqx:subscriber(), [emqx:suboption()]) -> ok).
async_subscribe(Topic, Subscriber, Options) ->
    cast(pick(Topic), {subscribe, Topic, Subscriber, Options}).

%% @doc Publish MQTT Message to a Topic
-spec(publish(binary(), mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Topic, Msg) ->
    route(lists:append(emqx_router:match(Topic),
                       emqx_router:match_local(Topic)), delivery(Msg)).

route([], #mqtt_delivery{message = Msg}) ->
    emqx_hooks:run('message.dropped', [undefined, Msg]),
    dropped(Msg#mqtt_message.topic), ignore;

%% Dispatch on the local node.
route([#route{topic = To, node = Node}],
      Delivery = #mqtt_delivery{flows = Flows}) when Node =:= node() ->
    dispatch(To, Delivery#mqtt_delivery{flows = [{route, Node, To} | Flows]});

%% Forward to other nodes
route([#route{topic = To, node = Node}], Delivery = #mqtt_delivery{flows = Flows}) ->
    forward(Node, To, Delivery#mqtt_delivery{flows = [{route, Node, To}|Flows]});

route(Routes, Delivery) ->
    {ok, lists:foldl(fun(Route, Acc) ->
                    {ok, Acc1} = route([Route], Acc), Acc1
            end, Delivery, Routes)}.

delivery(Msg) -> #mqtt_delivery{sender = self(), message = Msg, flows = []}.

%% @doc Forward message to another node...
forward(Node, To, Delivery) ->
    emqx_rpc:cast(Node, ?PUBSUB, dispatch, [To, Delivery]), {ok, Delivery}.

%% @doc Dispatch Message to Subscribers.
-spec(dispatch(binary(), mqtt_delivery()) -> mqtt_delivery()).
dispatch(Topic, Delivery = #mqtt_delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            emqx_hooks:run('message.dropped', [undefined, Msg]),
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
    emqx_metrics:inc('messages/dropped').

%% @doc Unsubscribe
-spec(unsubscribe(binary(), emqx:subscriber(), [emqx:suboption()]) -> ok).
unsubscribe(Topic, Subscriber, Options) ->
    call(pick(Topic), {unsubscribe, Topic, Subscriber, Options}).

-spec(async_unsubscribe(binary(), emqx:subscriber(), [emqx:suboption()]) -> ok).
async_unsubscribe(Topic, Subscriber, Options) ->
    cast(pick(Topic), {unsubscribe, Topic, Subscriber, Options}).

call(PubSub, Req) when is_pid(PubSub) ->
    gen_server:call(PubSub, Req, infinity).

cast(PubSub, Msg) when is_pid(PubSub) ->
    gen_server:cast(PubSub, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(pubsub, Topic).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    gproc_pool:connect_worker(Pool, {Pool, Id}),
    {ok, #state{pool = Pool, id = Id, env = Env}, hibernate}.

handle_call({subscribe, Topic, Subscriber, Options}, _From, State) ->
    add_subscriber(Topic, Subscriber, Options),
    reply(ok, setstats(State));

handle_call({unsubscribe, Topic, Subscriber, Options}, _From, State) ->
    del_subscriber(Topic, Subscriber, Options),
    reply(ok, setstats(State));

handle_call(Req, _From, State) ->
    lager:error("[~s] Unexpected Call: ~p", [?MODULE, Req]),
    {reply, ignore, State}.

handle_cast({subscribe, Topic, Subscriber, Options}, State) ->
    add_subscriber(Topic, Subscriber, Options),
    noreply(setstats(State));

handle_cast({unsubscribe, Topic, Subscriber, Options}, State) ->
    del_subscriber(Topic, Subscriber, Options),
    noreply(setstats(State));

handle_cast(Msg, State) ->
    lager:error("[~s] Unexpected Cast: ~p", [?MODULE, Msg]),
    {noreply, State}.

handle_info(Info, State) ->
    lager:error("[~s] Unexpected Info: ~p", [?MODULE, Info]),
    {noreply, State}.

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    gproc_pool:disconnect_worker(Pool, {Pool, Id}).

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
    case ets:member(mqtt_subscriber, Topic) and emqx_router:has_route(Topic) of
        true  -> ok;
        false -> emqx_router:add_route(Topic)
    end,
    ets:insert(mqtt_subscriber, {Topic, shared(Share, Subscriber)}).

add_local_subscriber(Share, Topic, Subscriber) ->
    (not ets:member(mqtt_subscriber, {local, Topic})) andalso emqx_router:add_local_route(Topic),
    ets:insert(mqtt_subscriber, {{local, Topic}, shared(Share, Subscriber)}).

del_subscriber(Topic, Subscriber, Options) ->
    Share = proplists:get_value(share, Options),
    case ?is_local(Options) of
        false -> del_global_subscriber(Share, Topic, Subscriber);
        true  -> del_local_subscriber(Share, Topic, Subscriber)
    end.

del_global_subscriber(Share, Topic, Subscriber) ->
    ets:delete_object(mqtt_subscriber, {Topic, shared(Share, Subscriber)}),
    (not ets:member(mqtt_subscriber, Topic)) andalso emqx_router:del_route(Topic).

del_local_subscriber(Share, Topic, Subscriber) ->
    ets:delete_object(mqtt_subscriber, {{local, Topic}, shared(Share, Subscriber)}),
    (not ets:member(mqtt_subscriber, {local, Topic})) andalso emqx_router:del_local_route(Topic).

shared(undefined, Subscriber) ->
    Subscriber;
shared(Share, Subscriber) ->
    {share, Share, Subscriber}.

setstats(State) ->
    emqx_stats:setstats('subscribers/count', 'subscribers/max', ets:info(mqtt_subscriber, size)),
    State.

reply(Reply, State) ->
    {reply, Reply, State}.

noreply(State) ->
    {noreply, State}.

