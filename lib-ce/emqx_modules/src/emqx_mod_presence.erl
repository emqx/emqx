%%--------------------------------------------------------------------
%% Copyright (c) 2020-2023 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/emqx.hrl").
-include_lib("emqx/include/logger.hrl").

-logger_header("[Presence]").

%% emqx_gen_mod callbacks
-export([ load/1
        , unload/1
        , description/0
        ]).

-export([ on_client_connected/3
        , on_client_disconnected/4
        ]).

-ifdef(TEST).
-export([reason/1]).
-endif.

load(Env) ->
    emqx_hooks:put('client.connected',    {?MODULE, on_client_connected, [Env]}),
    emqx_hooks:put('client.disconnected', {?MODULE, on_client_disconnected, [Env]}).

unload(_Env) ->
    emqx_hooks:del('client.connected',    {?MODULE, on_client_connected}),
    emqx_hooks:del('client.disconnected', {?MODULE, on_client_disconnected}).

description() ->
    "EMQX Presence Module".
%%--------------------------------------------------------------------
%% Callbacks
%%--------------------------------------------------------------------

on_client_connected(ClientInfo = #{clientid := ClientId}, ConnInfo, Env) ->
    Presence = common_infos(ClientInfo, ConnInfo),
    NPresence = Presence#{
                  connack         => 0, %% XXX: connack will be removed in 5.0
                  keepalive       => maps:get(keepalive, ConnInfo, 0),
                  clean_start     => maps:get(clean_start, ConnInfo, true),
                  expiry_interval => maps:get(expiry_interval, ConnInfo, 0)
                 },
    case emqx_json:safe_encode(NPresence) of
        {ok, Payload} ->
            emqx_broker:safe_publish(
              make_msg(qos(Env), topic(connected, ClientId), Payload));
        {error, _Reason} ->
            ?LOG(error, "Failed to encode 'connected' presence: ~p", [NPresence])
    end.

on_client_disconnected(ClientInfo = #{clientid := ClientId},
                       Reason, ConnInfo = #{disconnected_at := DisconnectedAt}, Env) ->

    Presence = common_infos(ClientInfo, ConnInfo),
    NPresence = Presence#{
                  reason => reason(Reason),
                  disconnected_at => DisconnectedAt
                 },
    case emqx_json:safe_encode(NPresence) of
        {ok, Payload} ->
            emqx_broker:safe_publish(
              make_msg(qos(Env), topic(disconnected, ClientId), Payload));
        {error, _Reason} ->
            ?LOG(error, "Failed to encode 'disconnected' presence: ~p", [NPresence])
    end.

%%--------------------------------------------------------------------
%% Helper functions
%%--------------------------------------------------------------------

common_infos(
  _ClientInfo = #{clientid := ClientId,
                  username := Username,
                  peerhost := PeerHost,
                  sockport := SockPort
                 },
  _ConnInfo = #{proto_name := ProtoName,
                proto_ver := ProtoVer,
                connected_at := ConnectedAt
               }) ->
    #{clientid => ClientId,
      username => Username,
      ipaddress => ntoa(PeerHost),
      sockport => SockPort,
      proto_name => ProtoName,
      proto_ver => ProtoVer,
      connected_at => ConnectedAt,
      ts => erlang:system_time(millisecond)
     }.

make_msg(QoS, Topic, Payload) ->
    emqx_message:set_flag(
      sys, emqx_message:make(
             ?MODULE, QoS, Topic, iolist_to_binary(Payload))).

topic(connected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/connected"]));
topic(disconnected, ClientId) ->
    emqx_topic:systop(iolist_to_binary(["clients/", ClientId, "/disconnected"])).

qos(Env) -> proplists:get_value(qos, Env, 0).

-compile({inline, [reason/1]}).
reason(Reason) when is_atom(Reason) -> Reason;
reason({shutdown, Reason}) when is_atom(Reason) -> Reason;
reason({Error, _}) when is_atom(Error) -> Error;
reason(_) -> internal_error.

-compile({inline, [ntoa/1]}).
ntoa(IpAddr) -> iolist_to_binary(inet:ntoa(IpAddr)).

