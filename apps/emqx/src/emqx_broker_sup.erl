%%--------------------------------------------------------------------
%% Copyright (c) 2020 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-module(emqx_broker_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Broker pool
    PoolSize = emqx_vm:schedulers() * 2,
    BrokerPool = emqx_pool_sup:spec([broker_pool, hash, PoolSize,
                                     {emqx_broker, start_link, []}]),

    %% Shared subscription
    SharedSub = #{id => shared_sub,
                  start => {emqx_shared_sub, start_link, []},
                  restart => permanent,
                  shutdown => 2000,
                  type => worker,
                  modules => [emqx_shared_sub]},

    %% Broker helper
    Helper = #{id => helper,
               start => {emqx_broker_helper, start_link, []},
               restart => permanent,
               shutdown => 2000,
               type => worker,
               modules => [emqx_broker_helper]},

    {ok, {{one_for_all, 0, 1}, [BrokerPool, SharedSub, Helper]}}.

