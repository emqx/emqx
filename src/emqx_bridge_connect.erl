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

-module(emqx_bridge_connect).

-export([start/2]).

-export_type([config/0, connection/0]).

-optional_callbacks([ensure_subscribed/3, ensure_unsubscribed/2]).

%% map fields depend on implementation
-type config() :: map().
-type connection() :: term().
-type conn_ref() :: term().
-type batch() :: emqx_protal:batch().
-type ack_ref() :: emqx_bridge:ack_ref().
-type topic() :: emqx_topic:topic().
-type qos() :: emqx_mqtt_types:qos().

-include("logger.hrl").

%% establish the connection to remote node/cluster
%% protal worker (the caller process) should be expecting
%% a message {disconnected, conn_ref()} when disconnected.
-callback start(config()) -> {ok, conn_ref(), connection()} | {error, any()}.

%% send to remote node/cluster
%% bridge worker (the caller process) should be expecting
%% a message {batch_ack, reference()} when batch is acknowledged by remote node/cluster
-callback send(connection(), batch()) -> {ok, ack_ref()} | {error, any()}.

%% called when owner is shutting down.
-callback stop(conn_ref(), connection()) -> ok.

-callback ensure_subscribed(connection(), topic(), qos()) -> ok.

-callback ensure_unsubscribed(connection(), topic()) -> ok.

start(Module, Config) ->
    case Module:start(Config) of
        {ok, Ref, Conn} ->
            {ok, Ref, Conn};
        {error, Reason} ->
            Config1 = obfuscate(Config),
            ?LOG(error, "[Bridge connect] Failed to connect with module=~p\n"
                 "config=~p\nreason:~p", [Module, Config1, Reason]),
            {error, Reason}
    end.

obfuscate(Map) ->
    maps:fold(fun(K, V, Acc) ->
                      case is_sensitive(K) of
                          true -> [{K, '***'} | Acc];
                          false -> [{K, V} | Acc]
                      end
              end, [], Map).

is_sensitive(password) -> true;
is_sensitive(_) -> false.
