%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_resource_sup).

-include("emqx_resource.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ok = emqx_resource_cache:new(),
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 10},
    Metrics = emqx_metrics_worker:child_spec(?RES_METRICS),
    CacheCleaner = #{
        id => emqx_resource_cache_cleaner,
        start => {emqx_resource_cache_cleaner, start_link, []},
        restart => permanent,
        shutdown => 5_000,
        type => worker,
        modules => [emqx_resource_cache_cleaner]
    },
    ResourceManager =
        #{
            id => emqx_resource_manager_sup,
            start => {emqx_resource_manager_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_resource_manager_sup]
        },
    WorkerSup = #{
        id => emqx_resource_buffer_worker_sup,
        start => {emqx_resource_buffer_worker_sup, start_link, []},
        restart => permanent,
        shutdown => infinity,
        type => supervisor
    },
    {ok, {SupFlags, [Metrics, CacheCleaner, ResourceManager, WorkerSup]}}.
