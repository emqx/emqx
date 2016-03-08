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

-export([create/2, publish/1, subscribe/1, subscribe/3,
         unsubscribe/1, unsubscribe/3]).

-define(APP, ?MODULE).

%%--------------------------------------------------------------------
%% Bootstrap, environment, is_running...
%%--------------------------------------------------------------------

%% @doc Start emqttd application.
-spec start() -> ok | {error, any()}.
start() -> application:start(?APP).

%% @doc Group environment
-spec env(Group :: atom()) -> list().
env(Group) -> application:get_env(?APP, Group, []).

%% @doc Get environment
-spec env(Group :: atom(), Name :: atom()) -> undefined | any().
env(Group, Name) -> proplists:get_value(Name, env(Group)).

%% @doc Is running?
-spec is_running(node()) -> boolean().
is_running(Node) ->
    case rpc:call(Node, erlang, whereis, [?APP]) of
        {badrpc, _}          -> false;
        undefined            -> false;
        Pid when is_pid(Pid) -> true
    end.

%%--------------------------------------------------------------------
%% PubSub APIs that wrap emqttd_server, emqttd_pubsub
%%--------------------------------------------------------------------

%% @doc Create a Topic
create(topic, Topic) when is_binary(Topic) ->
    emqttd_pubsub:create_topic(Topic).

%% @doc Publish MQTT Message
-spec publish(mqtt_message()) -> ok.
publish(Msg) when is_record(Msg, mqtt_message) ->
    emqttd_server:publish(Msg).

%% @doc Subscribe
-spec subscribe(binary()) -> ok.
subscribe(Topic) when is_binary(Topic) ->
    emqttd_server:subscribe(Topic).

-spec subscribe(binary(), binary(), mqtt_qos()) -> {ok, mqtt_qos()}.
subscribe(ClientId, Topic, Qos) ->
    emqttd_server:subscribe(ClientId, Topic, Qos).

%% @doc Unsubscribe
-spec unsubscribe(binary()) -> ok.
unsubscribe(Topic) when is_binary(Topic) ->
    emqttd_server:unsubscribe(Topic).

-spec unsubscribe(binary(), binary(), mqtt_qos()) -> ok.
unsubscribe(ClientId, Topic, Qos) ->
    emqttd_server:unsubscribe(ClientId, Topic, Qos).

