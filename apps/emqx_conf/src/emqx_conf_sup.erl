%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_conf_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 100
    },
    ChildSpecs =
        [
            child_spec(emqx_cluster_rpc, []),
            child_spec(emqx_cluster_rpc_cleaner, [])
        ],
    {ok, {SupFlags, ChildSpecs}}.

child_spec(Mod, Args) ->
    #{
        id => Mod,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    }.
