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
        },
        #{
            id => emqx_agent_sess_sup,
            start => {emqx_agent_sess_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_agent_sess_sup]
        },
        #{
            id => emqx_agent_pipeline_registry,
            start => {emqx_agent_pipeline_registry, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_agent_pipeline_registry]
        },
        #{
            id => emqx_agent_pipeline_mgr,
            start => {emqx_agent_pipeline_mgr, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_agent_pipeline_mgr]
        },
        #{
            id => emqx_agent_pipeline_sup,
            start => {emqx_agent_pipeline_sup, start_link, []},
            restart => permanent,
            shutdown => infinity,
            type => supervisor,
            modules => [emqx_agent_pipeline_sup]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
