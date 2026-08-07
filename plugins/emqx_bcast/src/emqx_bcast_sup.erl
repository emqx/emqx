%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).
-export([restart_deliver_pool/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

restart_deliver_pool(PoolSize) ->
    _ = supervisor:terminate_child(?MODULE, bcast_deliver_pool_sup),
    _ = supervisor:delete_child(?MODULE, bcast_deliver_pool_sup),
    {ok, _} = supervisor:start_child(?MODULE, pool_spec(PoolSize)),
    ok.

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 3600},
    Config = persistent_term:get({emqx_bcast, config}, #{}),
    PoolSize = maps:get(delivery_pool_size, Config, erlang:system_info(schedulers)),
    Children = [
        pool_spec(PoolSize),
        #{
            id => emqx_bcast_cleanup,
            start => {emqx_bcast_cleanup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_cleanup]
        }
    ],
    {ok, {SupFlags, Children}}.

pool_spec(PoolSize) ->
    emqx_pool_sup:spec(bcast_deliver_pool_sup, permanent, [
        emqx_bcast_deliver_pool,
        round_robin,
        PoolSize,
        {emqx_pool, start_link, []}
    ]).
