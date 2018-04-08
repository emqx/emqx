%%--------------------------------------------------------------------
%% Copyright (c) 2013-2018 EMQ Inc. All Rights Reserved.
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
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    StatsFun = emqx_stats:statsfun('routes/count', 'routes/max'),

    %% Router helper
    Helper = {router_helper, {emqx_router_helper, start_link, [StatsFun]},
              permanent, 5000, worker, [emqx_router_helper]},

    %% Router pool
    PoolSup = emqx_pool_sup:spec(router_pool,
                                  [router, hash, emqx_sys:schedulers(),
                                   {emqx_router, start_link, []}]),

    {ok, {{one_for_all, 0, 3600}, [Helper, PoolSup]}}.

