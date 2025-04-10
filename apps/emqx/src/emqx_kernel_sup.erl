%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_kernel_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok, {
        {one_for_one, 10, 100},
        %% always start emqx_config_handler first to load the emqx.conf to emqx_config
        [
            child_spec(emqx_config_handler, worker),
            child_spec(emqx_config_backup_manager, worker),
            child_spec(emqx_pool_sup, supervisor, [
                emqx:get_config([node, generic_pool_size], emqx_vm:schedulers())
            ]),
            child_spec(emqx_hooks, worker),
            child_spec(emqx_stats, worker),
            child_spec(emqx_metrics, worker),
            child_spec(emqx_ocsp_cache, worker),
            child_spec(emqx_crl_cache, worker),
            child_spec(emqx_tls_lib_sup, supervisor),
            child_spec(emqx_log_throttler, worker),
            child_spec(emqx_bpapi_replicant_checker, worker)
        ]
    }}.

child_spec(M, Type) ->
    child_spec(M, Type, []).

child_spec(M, worker, Args) ->
    #{
        id => M,
        start => {M, start_link, Args},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [M]
    };
child_spec(M, supervisor, Args) ->
    #{
        id => M,
        start => {M, start_link, Args},
        restart => permanent,
        shutdown => infinity,
        type => supervisor,
        modules => [M]
    }.
