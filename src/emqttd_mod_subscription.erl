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

-export([load/1, on_client_connected/3, unload/1]).

-record(state, {topics, backend = false}).

load(Opts) ->
    Topics = [{iolist_to_binary(Topic), QoS} || {Topic, QoS} <- Opts, ?IS_QOS(QoS)],
    State = #state{topics = Topics, backend = lists:member(backend, Opts)},
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [State]).

on_client_connected(?CONNACK_ACCEPT, Client = #mqtt_client{client_id  = ClientId,
                                                           client_pid = ClientPid,
                                                           username   = Username},
                    #state{topics = Topics, backend = Backend}) ->

    Replace = fun(Topic) -> rep(<<"$u">>, Username, rep(<<"$c">>, ClientId, Topic)) end,
    TopicTable = [{Replace(Topic), Qos} || {Topic, Qos} <- with_backend(Backend, ClientId, Topics)],
    emqttd_client:subscribe(ClientPid, TopicTable),
    {ok, Client};

on_client_connected(_ConnAck, _Client, _State) ->
    ok.

with_backend(false, _ClientId, TopicTable) ->
    TopicTable;
with_backend(true, ClientId, TopicTable) ->
    Fun = fun(#mqtt_subscription{topic = Topic, qos = Qos}) -> {Topic, Qos} end,
    emqttd_opts:merge([Fun(Sub) || Sub <- emqttd_backend:lookup_subscriptions(ClientId)], TopicTable).

unload(_Opts) ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3).

rep(<<"$c">>, ClientId, Topic) ->
    emqttd_topic:feed_var(<<"$c">>, ClientId, Topic);
rep(<<"$u">>, undefined, Topic) ->
    Topic;
rep(<<"$u">>, Username, Topic) ->
    emqttd_topic:feed_var(<<"$u">>, Username, Topic).

