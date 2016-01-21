%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2016 eMQTT.IO, All Rights Reserved.
%%%
%%% Permission is hereby granted, free of charge, to any person obtaining a copy
%%% of this software and associated documentation files (the "Software"), to deal
%%% in the Software without restriction, including without limitation the rights
%%% to use, copy, modify, merge, publish, distribute, sublicense, and/or sell
%%% copies of the Software, and to permit persons to whom the Software is
%%% furnished to do so, subject to the following conditions:
%%%
%%% The above copyright notice and this permission notice shall be included in all
%%% copies or substantial portions of the Software.
%%%
%%% THE SOFTWARE IS PROVIDED "AS IS", WITHOUT WARRANTY OF ANY KIND, EXPRESS OR
%%% IMPLIED, INCLUDING BUT NOT LIMITED TO THE WARRANTIES OF MERCHANTABILITY,
%%% FITNESS FOR A PARTICULAR PURPOSE AND NONINFRINGEMENT. IN NO EVENT SHALL THE
%%% AUTHORS OR COPYRIGHT HOLDERS BE LIABLE FOR ANY CLAIM, DAMAGES OR OTHER
%%% LIABILITY, WHETHER IN AN ACTION OF CONTRACT, TORT OR OTHERWISE, ARISING FROM,
%%% OUT OF OR IN CONNECTION WITH THE SOFTWARE OR THE USE OR OTHER DEALINGS IN THE
%%% SOFTWARE.
%%%-----------------------------------------------------------------------------
%%% @doc Subscription from Broker Side
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%-----------------------------------------------------------------------------
-module(emqttd_mod_subscription).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([load/1, client_connected/3, unload/1]).

-record(state, {topics, stored = false}).

-ifdef(TEST).
-compile(export_all).
-endif.

load(Opts) ->
    Topics = [{iolist_to_binary(Topic), QoS} || {Topic, QoS} <- Opts, ?IS_QOS(QoS)],
    State = #state{topics = Topics, stored = lists:member(stored, Opts)},
    emqttd_broker:hook('client.connected', {?MODULE, client_connected},
                       {?MODULE, client_connected, [State]}),
    {ok, State}.

client_connected(?CONNACK_ACCEPT, #mqtt_client{client_id  = ClientId,
                                               client_pid = ClientPid,
                                               username   = Username},
                 #state{topics = Topics, stored = Stored}) ->
    Replace = fun(Topic) -> rep(<<"$u">>, Username, rep(<<"$c">>, ClientId, Topic)) end,
    TopicTable = with_stored(Stored, ClientId, [{Replace(Topic), Qos} || {Topic, Qos} <- Topics]),
    emqttd_client:subscribe(ClientPid, TopicTable).

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

