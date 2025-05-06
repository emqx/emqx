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

-module(emqx_mcp_server_sup).
-include("emqx_mcp_gateway.hrl").

-behaviour(supervisor).
%% API
-export([start_link/0, start_child/1]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlags, [worker_spec()]}}.

-spec start_child(emqx_mcp_server:config()) -> supervisor:startchild_ret().
start_child(Conf) ->
    supervisor:start_child(?MODULE, [Conf]).

worker_spec() ->
    #{
        id => emqx_mcp_server,
        start => {emqx_mcp_server, start_link, []},
        restart => temporary,
        shutdown => 3_000,
        type => worker
    }.
