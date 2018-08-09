%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All rights reserved.
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

-module(emqx_ws_connection_sup).

-behavior(supervisor).

-export([start_link/0, start_connection/2]).

-export([init/1]).

-spec(start_link() -> {ok, pid()}).
start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%% @doc Start a MQTT/WebSocket Connection.
-spec(start_connection(pid(), cowboy_req:req()) -> {ok, pid()}).
start_connection(WsPid, Req) ->
    supervisor:start_child(?MODULE, [WsPid, Req]).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %%TODO: Cannot upgrade the environments, Use zone?
    Env = lists:append(emqx_config:get_env(client, []), emqx_config:get_env(protocol, [])),
    {ok, {{simple_one_for_one, 0, 1},
           [{ws_connection, {emqx_ws_connection, start_link, [Env]},
             temporary, 5000, worker, [emqx_ws_connection]}]}}.

