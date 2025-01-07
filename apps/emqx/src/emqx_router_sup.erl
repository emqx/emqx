%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_router_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    %% Init and log routing table type
    ok = mria:wait_for_tables(
        emqx_trie:create_trie() ++
            emqx_router:create_tables() ++
            emqx_router_helper:create_tables()
    ),
    ok = emqx_router:init_schema(),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% Router helper
    Helper = #{
        id => helper,
        start => {emqx_router_helper, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_router_helper]
    },
    HelperPostStart = #{
        id => helper_post_start,
        start => {emqx_router_helper, post_start, []},
        restart => transient,
        shutdown => brutal_kill,
        type => worker
    },
    %% Router pool
    RouterPool = emqx_pool_sup:spec([
        router_pool,
        hash,
        {emqx_router, start_link, []}
    ]),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 100
    },
    {ok, {SupFlags, [Helper, HelperPostStart, RouterPool]}}.
