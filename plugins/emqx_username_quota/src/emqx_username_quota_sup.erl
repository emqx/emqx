%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_username_quota_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    ok = emqx_username_quota_state:create_tables(),
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 10
    },
    PoolModule = emqx_username_quota_pool,
    PoolType = hash,
    MFA = {PoolModule, start_link, []},
    SupArgs = [PoolModule, PoolType, MFA],
    ChildSpecs = [
        emqx_pool_sup:spec(emqx_username_quota_pool_sup, SupArgs),
        #{
            id => emqx_username_quota_cluster_watch,
            start => {emqx_username_quota_cluster_watch, start_link, []},
            type => worker,
            restart => transient,
            shutdown => 1_000
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
