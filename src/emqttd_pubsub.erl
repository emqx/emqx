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

-include("emqttd_internal.hrl").

%% API Exports
-export([start_link/3, subscribe/2, unsubscribe/2, publish/2,
         async_subscribe/2, async_unsubscribe/2]).

-export([subscribers/1]).

%% gen_server.
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env}).

-define(PUBSUB, ?MODULE).

-spec(start_link(atom(), pos_integer(), [tuple()]) -> {ok, pid()} | ignore | {error, any()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

-spec(subscribe(binary(), emqttd:subscriber()) -> ok).
subscribe(Topic, Subscriber) ->
    call(pick(Topic), {subscribe, Topic, Subscriber}).

-spec(async_subscribe(binary(), emqttd:subscriber()) -> ok).
async_subscribe(Topic, Subscriber) ->
    cast(pick(Topic), {subscribe, Topic, Subscriber}).

-spec(publish(binary(), any()) -> {ok, mqtt_delivery()} | ignore).
publish(Topic, Msg) ->
    route(emqttd_router:match(Topic), delivery(Msg)).

route([], _Delivery) ->
    ignore;

%% Dispatch on the local node
route([#mqtt_route{topic = To, node = Node}],
      Delivery = #mqtt_delivery{flows = Flows}) when Node =:= node() ->
    dispatch(To, Delivery#mqtt_delivery{flows = [{route, Node, To} | Flows]});

%% Forward to other nodes
route([#mqtt_route{topic = To, node = Node}], Delivery = #mqtt_delivery{flows = Flows}) ->
    forward(Node, To, Delivery#mqtt_delivery{flows = [{route, Node, To}|Flows]});

route(Routes, Delivery) ->
    {ok, lists:foldl(fun(Route, DelAcc) ->
                    {ok, DelAcc1} = route([Route], DelAcc), DelAcc1
            end, Delivery, Routes)}.

delivery(Msg) -> #mqtt_delivery{sender = self(), message = Msg, flows = []}.

%% @doc Forward message to another node...
forward(Node, To, Delivery) ->
    rpc:cast(Node, ?PUBSUB, dispatch, [To, Delivery]), {ok, Delivery}.

%% @doc Dispatch Message to Subscribers
-spec(dispatch(binary(), mqtt_delivery()) -> mqtt_delivery()).
dispatch(Topic, Delivery = #mqtt_delivery{message = Msg, flows = Flows}) ->
    case subscribers(Topic) of
        [] ->
            dropped(Topic), {ok, Delivery};
        [Sub] -> %% optimize?
            dispatch(Sub, Topic, Msg),
            {ok, Delivery#mqtt_delivery{flows = [{dispatch, Topic, 1} | Flows]}};
        Subscribers ->
            Flows1 = [{dispatch, Topic, length(Subscribers)} | Flows],
            lists:foreach(fun(Sub) -> dispatch(Sub, Topic, Msg) end, Subscribers),
            {ok, Delivery#mqtt_delivery{flows = Flows1}}
    end.

dispatch(Pid, Topic, Msg) when is_pid(Pid) ->
    Pid ! {dispatch, Topic, Msg};
dispatch(SubId, Topic, Msg) when is_binary(SubId) ->
    emqttd_sm:dispatch(SubId, Topic, Msg).

subscribers(Topic) ->
    try ets:lookup_element(mqtt_subscriber, Topic, 2) catch error:badarg -> [] end.

%% @private
%% @doc Ingore $SYS Messages.
dropped(<<"$SYS/", _/binary>>) ->
    ok;
dropped(_Topic) ->
    emqttd_metrics:inc('messages/dropped').

-spec(unsubscribe(binary(), emqttd:subscriber()) -> ok).
unsubscribe(Topic, Subscriber) ->
    call(pick(Topic), {unsubscribe, Topic, Subscriber}).

-spec(async_unsubscribe(binary(), emqttd:subscriber()) -> ok).
async_unsubscribe(Topic, Subscriber) ->
    cast(pick(Topic), {unsubscribe, Topic, Subscriber}).

call(Server, Req) ->
    gen_server2:call(Server, Req, infinity).

cast(Server, Msg) ->
    gen_server2:cast(Server, Msg).

pick(Topic) ->
    gproc_pool:pick_worker(pubsub, Topic).

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env}}.

handle_call({subscribe, Topic, Subscriber}, _From, State) ->
    add_subscriber_(Topic, Subscriber),
    {reply, ok, setstats(State)};

handle_call({unsubscribe, Topic, Subscriber}, _From, State) ->
    del_subscriber_(Topic, Subscriber), 
    {reply, ok, setstats(State)};

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, Topic, Subscriber}, State) ->
    add_subscriber_(Topic, Subscriber),
    {noreply, setstats(State)};

handle_cast({unsubscribe, Topic, Subscriber}, State) ->
    del_subscriber_(Topic, Subscriber),
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
%% Internel Functions
%%--------------------------------------------------------------------

add_subscriber_(Topic, Subscriber) ->
    (not ets:member(mqtt_subscriber, Topic))
        andalso emqttd_router:add_route(Topic),
    ets:insert(mqtt_subscriber, {Topic, Subscriber}).

del_subscriber_(Topic, Subscriber) ->
    ets:delete_object(mqtt_subscriber, {Topic, Subscriber}),
    (not ets:member(mqtt_subscriber, Topic))
        andalso emqttd_router:del_route(Topic).

setstats(State) ->
    emqttd_stats:setstats('subscribers/count', 'subscribers/max',
                          ets:info(mqtt_subscriber, size)), State.

