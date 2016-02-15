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

%% @doc emqttd presence management module
-module(emqttd_mod_presence).

-behaviour(emqttd_gen_mod).

-include("emqttd.hrl").

-export([load/1, unload/1]).

-export([client_connected/3, client_disconnected/3]).

load(Opts) ->
    emqttd_broker:hook('client.connected', {?MODULE, client_connected},
                        {?MODULE, client_connected, [Opts]}),
    emqttd_broker:hook('client.disconnected', {?MODULE, client_disconnected},
                        {?MODULE, client_disconnected, [Opts]}),
    ok.

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
                              {ts, emqttd_time:now_to_secs()}]),
    Msg = emqttd_message:make(presence,
                              proplists:get_value(qos, Opts, 0),
                              topic(connected, ClientId),
                              iolist_to_binary(Json)),
    emqttd_pubsub:publish(Msg).

client_disconnected(Reason, ClientId, Opts) ->
    Json = mochijson2:encode([{clientid, ClientId},
                              {reason, reason(Reason)},
                              {ts, emqttd_time:now_to_secs()}]),
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

