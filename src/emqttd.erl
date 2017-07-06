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

%% @doc EMQ Main Module.

-module(emqttd).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([start/0, env/1, env/2, is_running/1, stop/0]).

%% PubSub API
-export([subscribe/1, subscribe/2, subscribe/3, publish/1,
         unsubscribe/1, unsubscribe/2]).

%% PubSub Management API
-export([setqos/3, topics/0, subscriptions/1, subscribers/1,
         is_subscribed/2, subscriber_down/1]).

%% Hooks API
-export([hook/4, hook/3, unhook/2, run_hooks/2, run_hooks/3]).

%% Debug API
-export([dump/0]).

-type(subscriber() :: pid() | binary()).

-type(suboption() :: local | {qos, non_neg_integer()} | {share, {'$queue' | binary()}}).

-type(pubsub_error() :: {error, {already_subscribed, binary()}
                              | {subscription_not_found, binary()}}).

-export_type([subscriber/0, suboption/0, pubsub_error/0]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, environment, configuration, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqttd application.
-spec(start() -> ok | {error, any()}).
start() -> application:start(?APP).

%% @doc Stop emqttd application.
-spec(stop() -> ok | {error, any()}).
stop() -> application:stop(?APP).

%% @doc Environment
-spec(env(Key:: atom()) -> {ok, any()} | undefined).
env(Key) -> application:get_env(?APP, Key).

%% @doc Get environment
-spec(env(Key:: atom(), Default:: any()) -> undefined | any()).
env(Key, Default) -> application:get_env(?APP, Key, Default).

%% @doc Is running?
-spec(is_running(node()) -> boolean()).
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

%%--------------------------------------------------------------------
%% PubSub APIs
%%--------------------------------------------------------------------

%% @doc Subscribe
-spec(subscribe(iodata()) -> ok | {error, any()}).
subscribe(Topic) ->
    subscribe(Topic, self()).

-spec(subscribe(iodata(), subscriber()) -> ok | {error, any()}).
subscribe(Topic, Subscriber) ->
    subscribe(Topic, Subscriber, []).

-spec(subscribe(iodata(), subscriber(), [suboption()]) -> ok | pubsub_error()).
subscribe(Topic, Subscriber, Options) ->
    emqttd_server:subscribe(iolist_to_binary(Topic), Subscriber, Options).

%% @doc Publish MQTT Message
-spec(publish(mqtt_message()) -> {ok, mqtt_delivery()} | ignore).
publish(Msg) ->
    emqttd_server:publish(Msg).

%% @doc Unsubscribe
-spec(unsubscribe(iodata()) -> ok | pubsub_error()).
unsubscribe(Topic) ->
    unsubscribe(Topic, self()).

-spec(unsubscribe(iodata(), subscriber()) -> ok | pubsub_error()).
unsubscribe(Topic, Subscriber) ->
    emqttd_server:unsubscribe(iolist_to_binary(Topic), Subscriber).

-spec(setqos(binary(), subscriber(), mqtt_qos()) -> ok).
setqos(Topic, Subscriber, Qos) ->
    emqttd_server:setqos(iolist_to_binary(Topic), Subscriber, Qos).

-spec(topics() -> [binary()]).
topics() -> emqttd_router:topics().

-spec(subscribers(iodata()) -> list(subscriber())).
subscribers(Topic) ->
    emqttd_server:subscribers(iolist_to_binary(Topic)).

-spec(subscriptions(subscriber()) -> [{binary(), binary(), list(suboption())}]).
subscriptions(Subscriber) ->
    emqttd_server:subscriptions(Subscriber).

-spec(is_subscribed(iodata(), subscriber()) -> boolean()).
is_subscribed(Topic, Subscriber) ->
    emqttd_server:is_subscribed(iolist_to_binary(Topic), Subscriber).

-spec(subscriber_down(subscriber()) -> ok).
subscriber_down(Subscriber) ->
    emqttd_server:subscriber_down(Subscriber).

%%--------------------------------------------------------------------
%% Hooks API
%%--------------------------------------------------------------------

-spec(hook(atom(), function() | {emqttd_hooks:hooktag(), function()}, list(any()))
      -> ok | {error, any()}).
hook(Hook, TagFunction, InitArgs) ->
    emqttd_hooks:add(Hook, TagFunction, InitArgs).

-spec(hook(atom(), function() | {emqttd_hooks:hooktag(), function()}, list(any()), integer())
      -> ok | {error, any()}).
hook(Hook, TagFunction, InitArgs, Priority) ->
    emqttd_hooks:add(Hook, TagFunction, InitArgs, Priority).

-spec(unhook(atom(), function() | {emqttd_hooks:hooktag(), function()})
      -> ok | {error, any()}).
unhook(Hook, TagFunction) ->
    emqttd_hooks:delete(Hook, TagFunction).

-spec(run_hooks(atom(), list(any())) -> ok | stop).
run_hooks(Hook, Args) ->
    emqttd_hooks:run(Hook, Args).

-spec(run_hooks(atom(), list(any()), any()) -> {ok | stop, any()}).
run_hooks(Hook, Args, Acc) ->
    emqttd_hooks:run(Hook, Args, Acc).

%%--------------------------------------------------------------------
%% Debug
%%--------------------------------------------------------------------

dump() -> lists:append([emqttd_server:dump(), emqttd_router:dump()]).

