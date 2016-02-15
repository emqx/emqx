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

%% @doc Subscription from Broker Side
-module(emqttd_mod_subscription).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([load/1, client_connected/3, unload/1]).

-record(state, {topics, stored = false}).

load(Opts) ->
    Topics = [{iolist_to_binary(Topic), QoS} || {Topic, QoS} <- Opts, ?IS_QOS(QoS)],
    State = #state{topics = Topics, stored = lists:member(stored, Opts)},
    emqttd_broker:hook('client.connected', {?MODULE, client_connected},
                       {?MODULE, client_connected, [State]}),
    ok.

client_connected(?CONNACK_ACCEPT, #mqtt_client{client_id  = ClientId,
                                               client_pid = ClientPid,
                                               username   = Username},
                 #state{topics = Topics, stored = Stored}) ->
    Replace = fun(Topic) -> rep(<<"$u">>, Username, rep(<<"$c">>, ClientId, Topic)) end,
    TopicTable = with_stored(Stored, ClientId, [{Replace(Topic), Qos} || {Topic, Qos} <- Topics]),
    emqttd_client:subscribe(ClientPid, TopicTable);

client_connected(_ConnAck, _Client, _State) -> ok.

with_stored(false, _ClientId, TopicTable) ->
    TopicTable;
with_stored(true, ClientId, TopicTable) ->
    Fun = fun(#mqtt_subscription{topic = Topic, qos = Qos}) -> {Topic, Qos} end,
    emqttd_opts:merge([Fun(Sub) || Sub <- emqttd_pubsub:lookup(subscription, ClientId)], TopicTable).

unload(_Opts) ->
    emqttd_broker:unhook('client.connected', {?MODULE, client_connected}).

rep(<<"$c">>, ClientId, Topic) ->
    emqttd_topic:feed_var(<<"$c">>, ClientId, Topic);
rep(<<"$u">>, undefined, Topic) ->
    Topic;
rep(<<"$u">>, Username, Topic) ->
    emqttd_topic:feed_var(<<"$u">>, Username, Topic).

