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

-module(emqttd_server).

-behaviour(gen_server2).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-include("emqttd_internal.hrl").

-export([start_link/3]).

%% PubSub API.
-export([subscribe/1, subscribe/2, subscribe/3, publish/1,
         unsubscribe/1, unsubscribe/2]).

%% Async PubSub API.
-export([async_subscribe/1, async_subscribe/2, async_subscribe/3,
         async_unsubscribe/1, async_unsubscribe/2]).

%% Management API.
-export([setqos/3, subscriptions/1, subscribers/1, subscribed/2]).

%% Debug API
-export([dump/0]).

%% gen_server Callbacks
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env, subids :: map(), submon :: emqttd_pmon:pmon()}).

%% @doc Start the server
-spec(start_link(atom(), pos_integer(), list()) -> {ok, pid()} | ignore | {error, term()}).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe to a Topic.
-spec(subscribe(binary()) -> ok | {error, term()}).
subscribe(Topic) when is_binary(Topic) ->
    subscribe(Topic, self()).

-spec(subscribe(binary(), emqttd:subscriber()) -> ok | {error, term()}).
subscribe(Topic, Subscriber) when is_binary(Topic) ->
    subscribe(Topic, Subscriber, []).

-spec(subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) ->
      ok | {error, term()}).
subscribe(Topic, Subscriber, Options) when is_binary(Topic) ->
    call(pick(Subscriber), {subscribe, Topic, with_subpid(Subscriber), Options}).

%% @doc Subscribe to a Topic asynchronously.
-spec(async_subscribe(binary()) -> ok).
async_subscribe(Topic) when is_binary(Topic) ->
    async_subscribe(Topic, self()).

-spec(async_subscribe(binary(), emqttd:subscriber()) -> ok).
async_subscribe(Topic, Subscriber) when is_binary(Topic) ->
    async_subscribe(Topic, Subscriber, []).

-spec(async_subscribe(binary(), emqttd:subscriber(), [emqttd:suboption()]) -> ok).
async_subscribe(Topic, Subscriber, Options) when is_binary(Topic) ->
    cast(pick(Subscriber), {subscribe, Topic, with_subpid(Subscriber), Options}).

%% @doc Publish message to Topic.
-spec(publish(mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Msg = #mqtt_message{from = From}) ->
    trace(publish, From, Msg),
    case emqttd_hooks:run('message.publish', [], Msg) of
        {ok, Msg1 = #mqtt_message{topic = Topic}} ->
            emqttd_pubsub:publish(Topic, Msg1);
        {stop, Msg1} ->
            lager:warning("Stop publishing: ~s", [emqttd_message:format(Msg1)]),
            ignore
    end.

%% @private
trace(publish, From, _Msg) when is_atom(From) ->
    %% Dont' trace '$SYS' publish
    ignore;
trace(publish, {ClientId, Username}, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:debug([{client, ClientId}, {topic, Topic}],
                "~s/~s PUBLISH to ~s: ~p", [ClientId, Username, Topic, Payload]);
trace(publish, From, #mqtt_message{topic = Topic, payload = Payload}) ->
    lager:debug([{client, From}, {topic, Topic}],
                "~s PUBLISH to ~s: ~p", [From, Topic, Payload]).

%% @doc Unsubscribe
-spec(unsubscribe(binary()) -> ok | {error, term()}).
unsubscribe(Topic) when is_binary(Topic) ->
    unsubscribe(Topic, self()).

%% @doc Unsubscribe
-spec(unsubscribe(binary(), emqttd:subscriber()) -> ok | {error, term()}).
unsubscribe(Topic, Subscriber) when is_binary(Topic) ->
    call(pick(Subscriber), {unsubscribe, Topic, with_subpid(Subscriber)}).

%% @doc Async Unsubscribe
-spec(async_unsubscribe(binary()) -> ok).
async_unsubscribe(Topic) when is_binary(Topic) ->
    async_unsubscribe(Topic, self()).

-spec(async_unsubscribe(binary(), emqttd:subscriber()) -> ok).
async_unsubscribe(Topic, Subscriber) when is_binary(Topic) ->
    cast(pick(Subscriber), {unsubscribe, Topic, with_subpid(Subscriber)}).

-spec(setqos(binary(), emqttd:subscriber(), mqtt_qos()) -> ok).
setqos(Topic, Subscriber, Qos) when is_binary(Topic) ->
    call(pick(Subscriber), {setqos, Topic, with_subpid(Subscriber), Qos}).

with_subpid(SubPid) when is_pid(SubPid) ->
    SubPid;
with_subpid(SubId) when is_binary(SubId) ->
    {SubId, self()};
with_subpid({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    {SubId, SubPid}.

-spec(subscriptions(emqttd:subscriber()) -> [{emqttd:subscriber(), binary(), list(emqttd:suboption())}]).
subscriptions(SubPid) when is_pid(SubPid) ->
    with_subproperty(ets:lookup(mqtt_subscription, SubPid));

subscriptions(SubId) when is_binary(SubId) ->
    with_subproperty(ets:match_object(mqtt_subscription, {{SubId, '_'}, '_'}));

subscriptions({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    with_subproperty(ets:lookup(mqtt_subscription, {SubId, SubPid})).

with_subproperty({Subscriber, {share, _Share, Topic}}) ->
    with_subproperty({Subscriber, Topic});
with_subproperty({Subscriber, Topic}) ->
    {Subscriber, Topic, ets:lookup_element(mqtt_subproperty, {Topic, Subscriber}, 2)};
with_subproperty(Subscriptions) when is_list(Subscriptions) ->
    [with_subproperty(Subscription) || Subscription <- Subscriptions].

-spec(subscribers(binary()) -> list(emqttd:subscriber())).
subscribers(Topic) when is_binary(Topic) ->
    emqttd_pubsub:subscribers(Topic).

-spec(subscribed(binary(), emqttd:subscriber()) -> boolean()).
subscribed(Topic, SubPid) when is_binary(Topic), is_pid(SubPid) ->
    ets:member(mqtt_subproperty, {Topic, SubPid});
subscribed(Topic, SubId) when is_binary(Topic), is_binary(SubId) ->
    length(ets:match_object(mqtt_subproperty, {{Topic, {SubId, '_'}}, '_'}, 1)) == 1;
subscribed(Topic, {SubId, SubPid}) when is_binary(Topic), is_binary(SubId), is_pid(SubPid) ->
    ets:member(mqtt_subproperty, {Topic, {SubId, SubPid}}).

call(Server, Req) ->
    gen_server2:call(Server, Req, infinity).

cast(Server, Msg) when is_pid(Server) ->
    gen_server2:cast(Server, Msg).

pick(SubPid) when is_pid(SubPid) ->
    gproc_pool:pick_worker(server, SubPid);
pick(SubId) when is_binary(SubId) ->
    gproc_pool:pick_worker(server, SubId);
pick({SubId, SubPid}) when is_binary(SubId), is_pid(SubPid) ->
    pick(SubId).

dump() ->
    [{Tab, ets:tab2list(Tab)} || Tab <- [mqtt_subproperty, mqtt_subscription, mqtt_subscriber]].

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    State = #state{pool = Pool, id = Id, env = Env,
                   subids = #{}, submon = emqttd_pmon:new()},
    {ok, State, hibernate, {backoff, 2000, 2000, 20000}}.

handle_call({subscribe, Topic, Subscriber, Options}, _From, State) ->
    case do_subscribe(Topic, Subscriber, Options, State) of
        {ok, NewState} -> reply(ok, setstats(NewState));
        {error, Error} -> reply({error, Error}, State)
    end;

handle_call({unsubscribe, Topic, Subscriber}, _From, State) ->
    case do_unsubscribe(Topic, Subscriber, State) of
        {ok, NewState} -> reply(ok, setstats(NewState));
        {error, Error} -> reply({error, Error}, State)
    end;

handle_call({setqos, Topic, Subscriber, Qos}, _From, State) ->
    Key = {Topic, Subscriber},
    case ets:lookup(mqtt_subproperty, Key) of
        [{_, Opts}] ->
            Opts1 = lists:ukeymerge(1, [{qos, Qos}], Opts),
            ets:insert(mqtt_subproperty, {Key, Opts1}),
            reply(ok, State);
        [] ->
            reply({error, {subscription_not_found, Topic}}, State)
    end;

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast({subscribe, Topic, Subscriber, Options}, State) ->
    case do_subscribe(Topic, Subscriber, Options, State) of
        {ok, NewState}  -> noreply(setstats(NewState));
        {error, _Error} -> noreply(State)
    end;

handle_cast({unsubscribe, Topic, Subscriber}, State) ->
    case do_unsubscribe(Topic, Subscriber, State) of
        {ok, NewState}  -> noreply(setstats(NewState));
        {error, _Error} -> noreply(State)
    end;

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #state{subids = SubIds}) ->
    case maps:find(DownPid, SubIds) of
        {ok, SubId} ->
            clean_subscriber({SubId, DownPid});
        error ->
            clean_subscriber(DownPid)
    end,
    noreply(setstats(demonitor_subscriber(DownPid, State)));

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
	{ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

do_subscribe(Topic, Subscriber, Options, State) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [] ->
            emqttd_pubsub:async_subscribe(Topic, Subscriber, Options),
            Share = proplists:get_value(share, Options),
            add_subscription(Share, Subscriber, Topic),
            ets:insert(mqtt_subproperty, {{Topic, Subscriber}, Options}),
            {ok, monitor_subscriber(Subscriber, State)};
        [_] ->
            {error, {already_subscribed, Topic}}
    end.

add_subscription(undefined, Subscriber, Topic) ->
    ets:insert(mqtt_subscription, {Subscriber, Topic});
add_subscription(Share, Subscriber, Topic) ->
    ets:insert(mqtt_subscription, {Subscriber, {share, Share, Topic}}).

monitor_subscriber(SubPid, State = #state{submon = SubMon}) when is_pid(SubPid) ->
    State#state{submon = SubMon:monitor(SubPid)};
monitor_subscriber({SubId, SubPid}, State = #state{subids = SubIds, submon = SubMon}) ->
    State#state{subids = maps:put(SubPid, SubId, SubIds), submon = SubMon:monitor(SubPid)}.

do_unsubscribe(Topic, Subscriber, State) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [{_, Options}] ->
            emqttd_pubsub:async_unsubscribe(Topic, Subscriber, Options),
            Share = proplists:get_value(share, Options),
            del_subscription(Share, Subscriber, Topic),
            ets:delete(mqtt_subproperty, {Topic, Subscriber}),
            {ok, State};
        [] ->
            {error, {subscription_not_found, Topic}}
    end.

del_subscription(undefined, Subscriber, Topic) ->
    ets:delete_object(mqtt_subscription, {Subscriber, Topic});
del_subscription(Share, Subscriber, Topic) ->
    ets:delete_object(mqtt_subscription, {Subscriber, {share, Share, Topic}}).

clean_subscriber(Subscriber) ->
    lists:foreach(fun({_, {share, Share, Topic}}) ->
                      clean_subscriber(Share, Subscriber, Topic);
                     ({_, Topic}) ->
                      clean_subscriber(undefined, Subscriber, Topic)
        end, ets:lookup(mqtt_subscription, Subscriber)),
    ets:delete(mqtt_subscription, Subscriber).

clean_subscriber(Share, Subscriber, Topic) ->
    case ets:lookup(mqtt_subproperty, {Topic, Subscriber}) of
        [] ->
            %% TODO:....???
            Options = if Share == undefined -> []; true -> [{share, Share}] end,
            emqttd_pubsub:async_unsubscribe(Topic, Subscriber, Options);
        [{_, Options}] ->
            emqttd_pubsub:async_unsubscribe(Topic, Subscriber, Options),
            ets:delete(mqtt_subproperty, {Topic, Subscriber})
    end.

demonitor_subscriber(SubPid, State = #state{subids = SubIds, submon = SubMon}) ->
    State#state{subids = maps:remove(SubPid, SubIds), submon = SubMon:demonitor(SubPid)}.

setstats(State) ->
    emqttd_stats:setstats('subscriptions/count', 'subscriptions/max',
                          ets:info(mqtt_subscription, size)), State.

reply(Reply, State) ->
    {reply, Reply, State, hibernate}.

noreply(State) ->
    {noreply, State, hibernate}.

