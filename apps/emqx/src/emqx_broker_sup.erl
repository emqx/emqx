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

-module(emqx_broker_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([get_broker_pool_workers/0]).

-export([init/1]).

-define(broker_pool, broker_pool).

start_link() ->
    ok = mria:wait_for_tables(emqx_shared_sub:create_tables()),
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

get_broker_pool_workers() ->
    try
        lists:map(fun({_Name, Pid}) -> Pid end, gproc_pool:active_workers(?broker_pool))
    catch
        _:_ ->
            []
    end.

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    %% Broker pool
    ok = emqx_broker:create_tabs(),
    PoolSize = emqx:get_config([node, broker_pool_size], emqx_vm:schedulers() * 2),
    BrokerPool = emqx_pool_sup:spec(broker_pool_sup, permanent, [
        ?broker_pool,
        hash,
        PoolSize,
        {emqx_broker, start_link, []}
    ]),

    SyncerPool = emqx_pool_sup:spec(syncer_pool_sup, [
        router_syncer_pool,
        hash,
        PoolSize,
        {emqx_router_syncer, start_link_pooled, []}
    ]),

    %% Shared subscription
    SharedSub = #{
        id => shared_sub,
        start => {emqx_shared_sub, start_link, []},
        restart => permanent,
        shutdown => 2000,
        type => worker,
        modules => [emqx_shared_sub]
    },

    %% Broker helper
    Helper = #{
        id => helper,
        start => {emqx_broker_helper, start_link, []},
        restart => permanent,
        shutdown => 2000,
        type => worker,
        modules => [emqx_broker_helper]
    },

    %% exclusive subscription
    ExclusiveSub = #{
        id => exclusive_subscription,
        start => {emqx_exclusive_subscription, start_link, []},
        restart => permanent,
        shutdown => 2000,
        type => worker,
        modules => [emqx_exclusive_subscription]
    },

    {ok, {{one_for_all, 0, 1}, [SyncerPool, BrokerPool, SharedSub, Helper, ExclusiveSub]}}.
