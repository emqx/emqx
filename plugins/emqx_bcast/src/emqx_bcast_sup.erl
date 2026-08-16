%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_sup).

-behaviour(supervisor).

-include("emqx_bcast.hrl").

-export([start_link/0, init/1]).
-export([restart_pools/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

restart_pools(PoolSize) ->
    lists:foreach(
        fun(ChildId) -> restart_pool_child(ChildId, PoolSize) end,
        [bcast_pull_pool_sup, bcast_ack_pool_sup, bcast_pull_server_pool_sup]
    ).

restart_pool_child(ChildId, PoolSize) ->
    case lists:keymember(ChildId, 1, supervisor:which_children(?MODULE)) of
        true ->
            _ = supervisor:terminate_child(?MODULE, ChildId),
            _ = supervisor:delete_child(?MODULE, ChildId),
            {ok, _} = supervisor:start_child(?MODULE, pool_spec(ChildId, PoolSize)),
            ok;
        false ->
            ok
    end.

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 3600},
    PoolSize = pool_size(),
    Core = emqx_bcast:is_core(),
    Children =
        [
            pool_spec(bcast_pull_pool_sup, PoolSize),
            pool_spec(bcast_ack_pool_sup, PoolSize)
        ] ++
        core_pool_specs(Core, PoolSize) ++
        [
            #{
                id => emqx_bcast_pull_pool,
                start => {emqx_bcast_pull_pool, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_bcast_pull_pool]
            },
            #{
                id => emqx_bcast_ack_pool,
                start => {emqx_bcast_ack_pool, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_bcast_ack_pool]
            }
        ] ++
        core_children(Core),
    {ok, {SupFlags, Children}}.

core_pool_specs(true, PoolSize) ->
    [pool_spec(bcast_pull_server_pool_sup, PoolSize)];
core_pool_specs(false, _PoolSize) ->
    [].

core_children(true) ->
    [
        #{
            id => emqx_bcast_pull_server_pool,
            start => {emqx_bcast_pull_server_pool, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_pull_server_pool]
        },
        #{
            id => emqx_bcast_cleanup,
            start => {emqx_bcast_cleanup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_cleanup]
        }
    ];
core_children(false) ->
    [].

pool_size() ->
    Config = persistent_term:get({?APP, config}, #{}),
    maps:get(delivery_pool_size, Config, erlang:system_info(schedulers)).

pool_spec(ChildId, PoolSize) ->
    PoolName =
        case ChildId of
            bcast_pull_pool_sup -> emqx_bcast_pull_worker_pool;
            bcast_ack_pool_sup -> emqx_bcast_ack_worker_pool;
            bcast_pull_server_pool_sup -> emqx_bcast_pull_server_worker_pool
        end,
    emqx_pool_sup:spec(ChildId, permanent, [
        PoolName,
        round_robin,
        PoolSize,
        {emqx_pool, start_link, []}
    ]).
