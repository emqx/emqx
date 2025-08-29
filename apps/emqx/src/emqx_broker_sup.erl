%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_broker_sup).

-include("emqx_instr.hrl").

-behaviour(supervisor).

-export([start_link/0]).
-export([get_broker_pool_workers/0]).

-export([init/1]).

-define(broker_pool, broker_pool).
-define(dispatcher_pool, dispatcher_pool).

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
    ok = emqx_broker:init_config(),
    %% broker pool manages subscriptions
    %% dispatcher pool dispatches messgess
    %% the two pool should share same config
    PoolSize = emqx:get_config([node, broker_pool_size], emqx_vm:schedulers() * 2),
    BrokerPool = emqx_pool_sup:spec(broker_pool_sup, permanent, [
        ?broker_pool,
        hash,
        PoolSize,
        {emqx_broker, start_link, []}
    ]),
    DispatcherPool = emqx_pool_sup:spec(dispatcher_pool_sup, permanent, [
        ?dispatcher_pool,
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
    SharedSubPostStart = #{
        id => shared_sub_post_start,
        start => {emqx_shared_sub, post_start, []},
        restart => transient,
        shutdown => brutal_kill,
        type => worker
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

    MetricsWorker = emqx_metrics_worker:child_spec(
        metrics_worker,
        ?BROKER_INSTR_METRICS_WORKER,
        ?BROKER_INSTR_METRICS_DECL
    ),

    {ok,
        {{one_for_all, 0, 1}, [
            MetricsWorker,
            SyncerPool,
            BrokerPool,
            DispatcherPool,
            SharedSub,
            SharedSubPostStart,
            Helper,
            ExclusiveSub
        ]}}.
