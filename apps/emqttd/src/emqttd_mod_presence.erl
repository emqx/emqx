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

-include_lib("emqtt/include/emqtt.hrl").

-include("emqttd.hrl").

-export([load/1, unload/1]).

-export([client_connected/3, client_disconnected/3]).

load(Opts) ->
    emqttd_broker:hook(client_connected, {?MODULE, client_connected}, {?MODULE, client_connected, [Opts]}),
    emqttd_broker:hook(client_disconnected, {?MODULE, client_disconnected}, {?MODULE, client_disconnected, [Opts]}),
    {ok, Opts}.

client_connected(ConnAck, #mqtt_client{clientid   = ClientId,
                                       username   = Username,
                                       ipaddress  = IpAddress,
                                       clean_sess = CleanSess,
                                       proto_ver  = ProtoVer}, Opts) ->
    Sess = case CleanSess of
        true -> false;
        false -> true
    end,
    Json = mochijson2:encode([{username, Username},
                              {ipaddress, list_to_binary(emqttd_net:ntoa(IpAddress))},
                              {session, Sess},
                              {protocol, ProtoVer},
                              {connack, ConnAck},
                              {ts, emqttd_vm:timestamp()}]),
    Message = #mqtt_message{qos     = proplists:get_value(qos, Opts, 0),
                            topic   = topic(connected, ClientId),
                            payload = iolist_to_binary(Json)},
    emqttd_pubsub:publish(presence, Message).

client_disconnected(Reason, ClientId, Opts) ->
    Json = mochijson2:encode([{reason, reason(Reason)}, {ts, emqttd_vm:timestamp()}]),
    emqttd_pubsub:publish(presence, #mqtt_message{qos     = proplists:get_value(qos, Opts, 0),
                                                  topic   = topic(disconnected, ClientId),
                                                  payload = iolist_to_binary(Json)}).

unload(_Opts) ->
    emqttd_broker:unhook(client_connected, {?MODULE, client_connected}),
    emqttd_broker:unhook(client_disconnected, {?MODULE, client_disconnected}).


topic(connected, ClientId) ->
    emqtt_topic:systop(list_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqtt_topic:systop(list_to_binary(["clients/", ClientId, "/disconnected"])).

reason(Reason) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

