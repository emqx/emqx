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

-module(emqx_bridge_connect).

-export([start/2]).

-export_type([config/0, connection/0]).

-optional_callbacks([ensure_subscribed/3, ensure_unsubscribed/2]).

%% map fields depend on implementation
-type(config() :: map()).
-type(connection() :: term()).
-type(batch() :: emqx_protal:batch()).
-type(ack_ref() :: emqx_bridge_worker:ack_ref()).
-type(topic() :: emqx_topic:topic()).
-type(qos() :: emqx_mqtt_types:qos()).

-include_lib("emqx/include/logger.hrl").

-logger_header("[Bridge Connect]").

%% establish the connection to remote node/cluster
%% protal worker (the caller process) should be expecting
%% a message {disconnected, conn_ref()} when disconnected.
-callback start(config()) -> {ok, connection()} | {error, any()}.

%% send to remote node/cluster
%% bridge worker (the caller process) should be expecting
%% a message {batch_ack, reference()} when batch is acknowledged by remote node/cluster
-callback send(connection(), batch()) -> {ok, ack_ref()} | {ok, integer()} | {error, any()}.

%% called when owner is shutting down.
-callback stop(connection()) -> ok.

-callback ensure_subscribed(connection(), topic(), qos()) -> ok.

-callback ensure_unsubscribed(connection(), topic()) -> ok.

start(Module, Config) ->
    case Module:start(Config) of
        {ok, Conn} ->
            {ok, Conn};
        {error, Reason} ->
            ?LOG_SENSITIVE(error, "Failed to connect with module=~p\n"
                           "config=~p\nreason:~p", [Module, Config, Reason]),
            {error, Reason}
    end.
