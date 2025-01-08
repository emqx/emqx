%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_eviction_agent_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    Childs = [
        child_spec(worker, emqx_eviction_agent, []),
        child_spec(supervisor, emqx_eviction_agent_conn_sup, [])
    ],
    {ok, {
        #{strategy => one_for_one, intensity => 10, period => 3600},
        Childs
    }}.

child_spec(Type, Mod, Args) ->
    #{
        id => Mod,
        start => {Mod, start_link, Args},
        restart => permanent,
        shutdown => 5000,
        type => Type,
        modules => [Mod]
    }.
