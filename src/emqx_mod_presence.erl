%%--------------------------------------------------------------------
%% Copyright (c) 2019 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mod_presence).

-behaviour(emqx_gen_mod).

-include("emqx.hrl").
-include("logger.hrl").

-logger_header("[Presence]").

%% APIs
-export([ on_client_connected/4
        , on_client_disconnected/3
        ]).

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        ]).

%%--------------------------------------------------------------------
%% APIs
%%--------------------------------------------------------------------

load(_Env) ->
    ok.
    %% emqx_hooks:add('client.connected',    {?MODULE, on_client_connected, [Env]}),
    %% emqx_hooks:add('client.disconnected', {?MODULE, on_client_disconnected, [Env]}).

on_client_connected(#{client_id := ClientId,
                      username  := Username,
                      peername  := {IpAddr, _}
                     }, ConnAck,
                    #{session    := Session,
                      proto_name := ProtoName,
                      proto_ver  := ProtoVer,
                      keepalive  := Keepalive
                     }, Env) ->
    case emqx_json:safe_encode(maps:merge(#{clientid => ClientId,
                               username => Username,
                               ipaddress => iolist_to_binary(esockd_net:ntoa(IpAddr)),
                               proto_name => ProtoName,
                               proto_ver => ProtoVer,
                               keepalive => Keepalive,
                               connack => ConnAck,
                               ts => erlang:system_time(millisecond)
                               }, maps:with([clean_start, expiry_interval], Session))) of
        {ok, Payload} ->
            emqx:publish(message(qos(Env), topic(connected, ClientId), Payload));
        {error, Reason} ->
            ?LOG(error, "Encoding connected event error: ~p", [Reason])
    end.




on_client_disconnected(#{client_id := ClientId,
                         username := Username}, Reason, Env) ->
    case emqx_json:safe_encode(#{clientid => ClientId,
                                 username => Username,
                                 reason => reason(Reason),
                                 ts => erlang:system_time(millisecond)
                                }) of
        {ok, Payload} ->
            emqx_broker:publish(message(qos(Env), topic(disconnected, ClientId), Payload));
        {error, Reason} ->
            ?LOG(error, "Encoding disconnected event error: ~p", [Reason])
    end.

unload(_Env) ->
    emqx_hooks:del('client.connected',    {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}).

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

