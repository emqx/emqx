%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_agent_skill_registry,
            start => {emqx_agent_skill_registry, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_agent_skill_registry]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
