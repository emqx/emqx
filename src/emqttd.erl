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

-module(emqttd).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([start/0, env/1, env/2, is_running/1]).

%% PubSub API
-export([create/2, lookup/2, publish/1, subscribe/1, subscribe/3,
         unsubscribe/1, unsubscribe/3]).

%% Hooks API
-export([hook/4, hook/3, unhook/2, run_hooks/3]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, environment, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqttd application.
-spec(start() -> ok | {error, any()}).
start() -> application:start(?APP).

%% @doc Group environment
-spec(env(Group :: atom()) -> list()).
env(Group) -> application:get_env(?APP, Group, []).

%% @doc Get environment
-spec(env(Group :: atom(), Name :: atom()) -> undefined | any()).
env(Group, Name) -> proplists:get_value(Name, env(Group)).

%% @doc Is running?
-spec(is_running(node()) -> boolean()).
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

%%--------------------------------------------------------------------
%% PubSub APIs that wrap emqttd_server, emqttd_pubsub
%%--------------------------------------------------------------------

%% @doc Lookup Topic or Subscription
-spec(lookup(topic, binary()) -> [mqtt_topic()];
            (subscription, binary()) -> [mqtt_subscription()]).
lookup(topic, Topic) when is_binary(Topic) ->
    emqttd_pubsub:lookup_topic(Topic);

lookup(subscription, ClientId) when is_binary(ClientId) ->
    emqttd_server:lookup_subscription(ClientId).

%% @doc Create a Topic or Subscription
-spec(create(topic | subscription, binary()) -> ok | {error, any()}).
create(topic, Topic) when is_binary(Topic) ->
    emqttd_pubsub:create_topic(Topic);

create(subscription, {ClientId, Topic, Qos}) ->
    Subscription = #mqtt_subscription{subid = ClientId, topic = Topic, qos = ?QOS_I(Qos)},
    emqttd_backend:add_subscription(Subscription).

%% @doc Publish MQTT Message
-spec(publish(mqtt_message()) -> ok).
publish(Msg) when is_record(Msg, mqtt_message) ->
    emqttd_server:publish(Msg), ok.

%% @doc Subscribe
-spec(subscribe(binary()) -> ok;
               ({binary(), binary(), mqtt_qos()}) -> ok).
subscribe(Topic) when is_binary(Topic) ->
    emqttd_server:subscribe(Topic);
subscribe({ClientId, Topic, Qos}) ->
    subscribe(ClientId, Topic, Qos).

-spec(subscribe(binary(), binary(), mqtt_qos()) -> {ok, mqtt_qos()}).
subscribe(ClientId, Topic, Qos) ->
    emqttd_server:subscribe(ClientId, Topic, Qos).

%% @doc Unsubscribe
-spec(unsubscribe(binary()) -> ok).
unsubscribe(Topic) when is_binary(Topic) ->
    emqttd_server:unsubscribe(Topic).

-spec(unsubscribe(binary(), binary(), mqtt_qos()) -> ok).
unsubscribe(ClientId, Topic, Qos) ->
    emqttd_server:unsubscribe(ClientId, Topic, Qos).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(hook(atom(), function(), list(any())) -> ok | {error, any()}).
hook(Hook, Function, InitArgs) ->
    emqttd_hook:add(Hook, Function, InitArgs).

-spec(hook(atom(), function(), list(any()), integer()) -> ok | {error, any()}).
hook(Hook, Function, InitArgs, Priority) ->
    emqttd_hook:add(Hook, Function, InitArgs, Priority).

-spec(unhook(atom(), function()) -> ok | {error, any()}).
unhook(Hook, Function) ->
    emqttd_hook:delete(Hook, Function).

-spec(run_hooks(atom(), list(any()), any()) -> {ok | stop, any()}).
run_hooks(Hook, Args, Acc) ->
    emqttd_hook:run(Hook, Args, Acc).

