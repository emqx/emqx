%%--------------------------------------------------------------------
%% Copyright (c) 2017-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_mcp_gateway_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    emqx_mcp_server_name_manager:init_tables(),
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 3,
        period => 10
    },
    ServerNameManager = #{
        id => emqx_mcp_server_name_manager,
        start => {emqx_mcp_server_name_manager, start_link, []},
        restart => permanent,
        type => worker
    },
    Dispatcher = #{
        id => emqx_mcp_server_dispatcher,
        start => {emqx_mcp_server_dispatcher, start_link, []},
        restart => permanent,
        type => worker
    },
    ServerSup = #{
        id => emqx_mcp_server_sup,
        start => {emqx_mcp_server_sup, start_link, []},
        restart => permanent,
        type => supervisor
    },
    ChildSpecs = [ServerSup, Dispatcher, ServerNameManager],
    {ok, {SupFlags, ChildSpecs}}.
