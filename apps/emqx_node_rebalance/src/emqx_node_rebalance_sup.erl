%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_node_rebalance_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Childs = [
        child_spec(emqx_node_rebalance_purge, []),
        child_spec(emqx_node_rebalance_evacuation, []),
        child_spec(emqx_node_rebalance_agent, []),
        child_spec(emqx_node_rebalance, [])
    ],
    {ok, {
        #{strategy => one_for_one, intensity => 10, period => 3600},
        Childs
    }}.

child_spec(Mod, Args) ->
    #{
        id => Mod,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [Mod]
    }.
