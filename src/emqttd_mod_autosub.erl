%%%-----------------------------------------------------------------------------
%%% Copyright (c) 2012-2015 eMQTT.IO, All Rights Reserved.
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
%%% @doc emqttd auto subscribe module.
%%%
%%% @author Feng Lee <feng@emqtt.io>
%%%
%%%-----------------------------------------------------------------------------
-module(emqttd_mod_autosub).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-include("emqttd_protocol.hrl").

-export([load/1, client_connected/3, unload/1]).

-record(state, {topics}).

load(Opts) ->
    Topics = [{list_to_binary(Topic), Qos} || {Topic, Qos} <- Opts, 0 =< Qos, Qos =< 2],
    emqttd_broker:hook('client.connected', {?MODULE, client_connected},
                       {?MODULE, client_connected, [Topics]}),
    {ok, #state{topics = Topics}}.

client_connected(?CONNACK_ACCEPT, #mqtt_client{client_id = ClientId,
                                               client_pid = ClientPid,
                                               username = Username}, Topics) ->
    F = fun(Topic) ->
            Topic1 = emqttd_topic:feed_var(<<"$c">>, ClientId, Topic),
            if
                Username =:= undefined -> Topic1;
                true -> emqttd_topic:feed_var(<<"$u">>, Username, Topic1)
            end
    end,
    emqttd_client:subscribe(ClientPid, [{F(Topic), Qos} || {Topic, Qos} <- Topics]);

client_connected(_ConnAck, _Client, _Topics) ->
    ignore.

unload(_Opts) ->
    emqttd_broker:unhook('client.connected', {?MODULE, client_connected}).

