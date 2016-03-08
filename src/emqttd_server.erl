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

-module(emqttd_server).

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

%% PubSub API
-export([subscribe/1, subscribe/3, publish/1, unsubscribe/1, unsubscribe/3,
         update_subscription/4]).

%% gen_server Function Exports
-export([init/1, handle_call/3, handle_cast/2, handle_info/2,
         terminate/2, code_change/3]).

-record(state, {pool, id, env, monitors}).

%%--------------------------------------------------------------------
%% Mnesia callbacks
%%--------------------------------------------------------------------

mnesia(boot) ->
    ok = emqttd_mnesia:create_table(subscription, [
                {type, bag},
                {ram_copies, [node()]},
                {local_content, true}, %% subscription table is local
                {record_name, mqtt_subscription},
                {attributes, record_info(fields, mqtt_subscription)}]);

mnesia(copy) ->
    ok = emqttd_mnesia:copy_table(subscription).

%%--------------------------------------------------------------------
%% Start server
%%--------------------------------------------------------------------

%% @doc Start a Server
-spec start_link(Pool, Id, Env) -> {ok, pid()} | ignore | {error, any()} when
    Pool :: atom(),
    Id   :: pos_integer(),
    Env  :: list(tuple()).
start_link(Pool, Id, Env) ->
    gen_server2:start_link({local, ?PROC_NAME(?MODULE, Id)}, ?MODULE, [Pool, Id, Env], []).

%%--------------------------------------------------------------------
%% PubSub API
%%--------------------------------------------------------------------

%% @doc Subscribe a Topic
-spec subscribe(binary()) -> ok.
subscribe(Topic) when is_binary(Topic) ->
    From = self(), call(server(From), {subscribe, From, Topic}).

%% @doc Subscribe from a MQTT session.
-spec subscribe(binary(), binary(), mqtt_qos()) -> ok.
subscribe(ClientId, Topic, Qos) ->
    From = self(), call(server(From), {subscribe, From, ClientId, Topic, ?QOS_I(Qos)}).

%% @doc Update a subscription.
-spec update_subscription(binary(), binary(), mqtt_qos(), mqtt_qos()) -> ok.
update_subscription(ClientId, Topic, OldQos, NewQos) ->
    call(server(self()), {update_subscription, ClientId, Topic, ?QOS_I(OldQos), ?QOS_I(NewQos)}).

%% @doc Publish a Message
-spec publish(Msg :: mqtt_message()) -> ok.
publish(Msg = #mqtt_message{from = From}) ->
    trace(publish, From, Msg),
    Msg1 = #mqtt_message{topic = Topic}
               = emqttd_broker:foldl_hooks('message.publish', [], Msg),
    %% Retain message first. Don't create retained topic.
    Msg2 = case emqttd_retainer:retain(Msg1) of
               ok     -> emqttd_message:unset_flag(Msg1);
               ignore -> Msg1
           end,
    emqttd_pubsub:publish(Topic, Msg2).

%% @doc Unsubscribe a Topic
-spec unsubscribe(binary()) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    From = self(), call(server(From), {unsubscribe, From, Topic}).

%% @doc Unsubscribe a Topic from a MQTT session
-spec unsubscribe(binary(), binary(), mqtt_qos()) -> ok.
unsubscribe(ClientId, Topic, Qos) ->
    From = self(), call(server(From), {unsubscribe, From, ClientId, Topic, Qos}).

call(Server, Req) ->
    gen_server2:call(Server, Req, infinity).

server(From) ->
    gproc_pool:pick_worker(server, From).

%%--------------------------------------------------------------------
%% gen_server Callbacks
%%--------------------------------------------------------------------

init([Pool, Id, Env]) ->
    ?GPROC_POOL(join, Pool, Id),
    {ok, #state{pool = Pool, id = Id, env = Env, monitors = dict:new()}}.

handle_call({subscribe, SubPid, ClientId, Topic, Qos}, _From, State) ->
    add_subscription_(ClientId, Topic, Qos),
    set_subscription_stats(),
    do_subscribe_(SubPid, Topic),
    ok(monitor_subscriber_(ClientId, SubPid, State));

handle_call({subscribe, SubPid, Topic}, _From, State) ->
    do_subscribe_(SubPid, Topic),
    ok(monitor_subscriber_(undefined, SubPid, State));

handle_call({update_subscription, ClientId, Topic, OldQos, NewQos}, _From, State) ->
    OldSub = #mqtt_subscription{subid = ClientId, topic = Topic, qos = OldQos},
    NewSub = #mqtt_subscription{subid = ClientId, topic = Topic, qos = NewQos},
    mnesia:transaction(fun update_subscription_/2, [OldSub, NewSub]),
    set_subscription_stats(), ok(State);

handle_call({unsubscribe, SubPid, ClientId, Topic, Qos}, From, State) ->
    del_subscription_(ClientId, Topic, Qos),
    set_subscription_stats(),
    handle_call({unsubscribe, SubPid, Topic}, From, State);

handle_call({unsubscribe, SubPid, Topic}, _From, State) ->
    emqttd_pubsub:unsubscribe(Topic, SubPid),
    ets:delete_object(subscribed, {SubPid, Topic}),
    ok(State);

handle_call(Req, _From, State) ->
    ?UNEXPECTED_REQ(Req, State).

handle_cast(Msg, State) ->
    ?UNEXPECTED_MSG(Msg, State).

handle_info({'DOWN', _MRef, process, DownPid, _Reason}, State = #state{monitors = Monitors}) ->
    %% unsubscribe
    lists:foreach(fun({_, Topic}) ->
                emqttd_pubsub:async_unsubscribe(Topic, DownPid)
        end, ets:lookup(subscribed, DownPid)),
    ets:delete(subscribed, DownPid),

    %% clean subscriptions
    case dict:find(DownPid, Monitors) of
        {ok, {undefined, _}} -> ok;
        {ok, {ClientId,  _}} -> mnesia:dirty_delete(subscription, ClientId);
        error                -> ok
    end,
    {noreply, State#state{monitors = dict:erase(DownPid, Monitors)}};

handle_info(Info, State) ->
    ?UNEXPECTED_INFO(Info, State).

terminate(_Reason, #state{pool = Pool, id = Id}) ->
    ?GPROC_POOL(leave, Pool, Id).

code_change(_OldVsn, State, _Extra) ->
    {ok, State}.

%%--------------------------------------------------------------------
%% Internal Functions
%%--------------------------------------------------------------------

%% @private
%% @doc Add a subscription.
-spec add_subscription_(binary(), binary(), mqtt_qos()) -> ok.
add_subscription_(ClientId, Topic, Qos) ->
    add_subscription_(#mqtt_subscription{subid = ClientId, topic = Topic, qos = Qos}).

-spec add_subscription_(mqtt_subscription()) -> ok.
add_subscription_(Subscription) when is_record(Subscription, mqtt_subscription) ->
    mnesia:dirty_write(subscription, Subscription).

update_subscription_(OldSub, NewSub) ->
    mnesia:delete_object(subscription, OldSub, write),
    mnesia:write(subscription, NewSub, write).

%% @private
%% @doc Delete a subscription
-spec del_subscription_(binary(), binary(), mqtt_qos()) -> ok.
del_subscription_(ClientId, Topic, Qos) ->
    del_subscription_(#mqtt_subscription{subid = ClientId, topic = Topic, qos = Qos}).

del_subscription_(Subscription) when is_record(Subscription, mqtt_subscription) ->
    mnesia:dirty_delete_object(subscription, Subscription).

%% @private
%% @doc Call pubsub to subscribe
do_subscribe_(SubPid, Topic) ->
    case ets:match(subscribed, {SubPid, Topic}) of
        [] ->
            emqttd_pubsub:subscribe(Topic, SubPid),
            ets:insert(subscribed, {SubPid, Topic});
        [_] ->
            false
    end.

monitor_subscriber_(ClientId, SubPid, State = #state{monitors = Monitors}) ->
    case dict:find(SubPid, Monitors) of
        {ok, _} ->
            State;
        error ->
            MRef = erlang:monitor(process, SubPid),
            State#state{monitors = dict:store(SubPid, {ClientId, MRef}, Monitors)}
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

%%--------------------------------------------------------------------
%% Subscription Statistics
%%--------------------------------------------------------------------

set_subscription_stats() ->
    emqttd_stats:setstats('subscriptions/count', 'subscriptions/max', mnesia:table_info(subscription, size)).

%%--------------------------------------------------------------------

ok(State) -> {reply, ok, State}.

