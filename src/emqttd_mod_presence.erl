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

-export([on_client_connected/3, on_client_disconnected/3]).

load(Opts) ->
    emqttd:hook('client.connected', fun ?MODULE:on_client_connected/3, [Opts]),
    emqttd:hook('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Opts]).

on_client_connected(ConnAck, Client = #mqtt_client{client_id  = ClientId,
                                                   username   = Username,
                                                   peername   = {IpAddr, _},
                                                   clean_sess = CleanSess,
                                                   proto_ver  = ProtoVer}, Opts) ->
    Json = mochijson2:encode([{clientid, ClientId},
                              {username, Username},
                              {ipaddress, list_to_binary(emqttd_net:ntoa(IpAddr))},
                              {session, sess(CleanSess)},
                              {protocol, ProtoVer},
                              {connack, ConnAck},
                              {ts, emqttd_time:now_to_secs()}]),
    Msg = message(qos(Opts), topic(connected, ClientId), Json),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)),
    {ok, Client}.

on_client_disconnected(Reason, ClientId, Opts) ->
    Json = mochijson2:encode([{clientid, ClientId},
                              {reason, reason(Reason)},
                              {ts, emqttd_time:now_to_secs()}]),
    Msg = message(qos(Opts), topic(disconnected, ClientId), Json),
    emqttd:publish(emqttd_message:set_flag(sys, Msg)).

unload(_Opts) ->
    emqttd:unhook('client.connected', fun ?MODULE:on_client_connected/3),
    emqttd:unhook('client.disconnected', fun ?MODULE:on_client_disconnected/3).

sess(false) -> true;
sess(true)  -> false.

qos(Opts) -> proplists:get_value(qos, Opts, 0).

message(Qos, Topic, Json) ->
    emqttd_message:make(presence, Qos, Topic, iolist_to_binary(Json)).

topic(connected, ClientId) ->
    emqttd_topic:systop(list_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqttd_topic:systop(list_to_binary(["clients/", ClientId, "/disconnected"])).

reason(Reason) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

