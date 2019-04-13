%% Copyright (c) 2013-2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_presence).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").

%% APIs
-export([ on_client_connected/4
        , on_client_disconnected/3
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        ]).

-define(ATTR_KEYS, [clean_start, proto_ver, proto_name, keepalive]).

%%------------------------------------------------------------------------------
%% APIs
%%------------------------------------------------------------------------------

load(Env) ->
    emqx_hooks:add('client.connected',    fun ?MODULE:on_client_connected/4, [Env]),
    emqx_hooks:add('client.disconnected', fun ?MODULE:on_client_disconnected/3, [Env]).

on_client_connected(#{client_id := ClientId,
                      username  := Username,
                      peername  := {IpAddr, _}}, ConnAck, ConnAttrs, Env) ->
    Attrs = maps:filter(fun(K, _) ->
                                lists:member(K, ?ATTR_KEYS)
                        end, ConnAttrs),
    case emqx_json:safe_encode(Attrs#{clientid => ClientId,
                                      username => Username,
                                      ipaddress => iolist_to_binary(esockd_net:ntoa(IpAddr)),
                                      connack => ConnAck,
                                      ts => os:system_time(second)
                                     }) of
        {ok, Payload} ->
            emqx:publish(message(qos(Env), topic(connected, ClientId), Payload));
        {error, Reason} ->
            ?LOG(error, "[Presence] Encoding connected event error: ~p", [Reason])
    end.

on_client_disconnected(#{client_id := ClientId, username := Username}, Reason, Env) ->
    case emqx_json:safe_encode([{clientid, ClientId},
                                {username, Username},
                                {reason, reason(Reason)},
                                {ts, os:system_time(second)}]) of
        {ok, Payload} ->
            emqx_broker:publish(message(qos(Env), topic(disconnected, ClientId), Payload));
        {error, Reason} ->
            ?LOG(error, "[Presence] Encoding disconnected event error: ~p", [Reason])
    end.

unload(_Env) ->
    emqx_hooks:del('client.connected',    fun ?MODULE:on_client_connected/4),
    emqx_hooks:del('client.disconnected', fun ?MODULE:on_client_disconnected/3).

message(QoS, Topic, Payload) ->
    emqx_message:set_flag(
      sys, emqx_message:make(
             ?MODULE, QoS, Topic, iolist_to_binary(Payload))).

topic(connected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/disconnected"])).

qos(Env) -> proplists:get_value(qos, Env, 0).

reason(Reason) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.
