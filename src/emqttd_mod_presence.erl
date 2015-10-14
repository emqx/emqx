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
%%% @doc
%%% emqttd presence management module.
%%%
%%% @end
%%%-----------------------------------------------------------------------------

-module(emqttd_mod_presence).

-author("Feng Lee <feng@emqtt.io>").

-include("emqttd.hrl").

-behaviour(emqttd_gen_mod).

-export([load/1, unload/1]).

-export([client_connected/3, client_disconnected/3]).

load(Opts) ->
    emqttd_broker:hook('client.connected', {?MODULE, client_connected},
                        {?MODULE, client_connected, [Opts]}),
    emqttd_broker:hook('client.disconnected', {?MODULE, client_disconnected},
                        {?MODULE, client_disconnected, [Opts]}),
    {ok, Opts}.

client_connected(ConnAck, #mqtt_client{client_id  = ClientId,
                                       username   = Username,
                                       peername   = {IpAddress, _},
                                       clean_sess = CleanSess,
                                       proto_ver  = ProtoVer}, Opts) ->
    Sess = case CleanSess of
        true -> false;
        false -> true
    end,
    Json = mochijson2:encode([{clientid, ClientId},
                              {username, Username},
                              {ipaddress, list_to_binary(emqttd_net:ntoa(IpAddress))},
                              {session, Sess},
                              {protocol, ProtoVer},
                              {connack, ConnAck},
                              {ts, emqttd_util:now_to_secs()}]),
    Msg = emqttd_message:make(presence,
                              proplists:get_value(qos, Opts, 0),
                              topic(connected, ClientId),
                              iolist_to_binary(Json)),
    emqttd_pubsub:publish(Msg).

client_disconnected(Reason, ClientId, Opts) ->
    Json = mochijson2:encode([{clientid, ClientId},
                              {reason, reason(Reason)},
                              {ts, emqttd_util:now_to_secs()}]),
    Msg = emqttd_message:make(presence,
                              proplists:get_value(qos, Opts, 0),
                              topic(disconnected, ClientId),
                              iolist_to_binary(Json)),
    emqttd_pubsub:publish(Msg).

unload(_Opts) ->
    emqttd_broker:unhook('client.connected', {?MODULE, client_connected}),
    emqttd_broker:unhook('client.disconnected', {?MODULE, client_disconnected}).

topic(connected, ClientId) ->
    emqttd_topic:systop(list_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqttd_topic:systop(list_to_binary(["clients/", ClientId, "/disconnected"])).

reason(Reason) when is_atom(Reason)    -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.


