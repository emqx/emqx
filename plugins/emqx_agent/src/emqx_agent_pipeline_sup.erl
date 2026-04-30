%%--------------------------------------------------------------------
%% Copyright (c) 2025-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

%% Supervisor for pipeline instances.
%%
%% Pipeline instances are lazy-started: one is created whenever a trigger
%% event arrives for a registered pipeline definition.  Instances complete
%% naturally (normal exit after idle timeout), hence restart => transient.

-module(emqx_agent_pipeline_sup).

-behaviour(supervisor).

-export([start_link/0, start_pipeline/2]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_pipeline(map(), map()) -> {ok, pid()} | {error, term()}.
start_pipeline(Def, TriggerEvent) ->
    supervisor:start_child(?MODULE, [Def, TriggerEvent]).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpec = #{
        id => emqx_agent_pipeline,
        start => {emqx_agent_pipeline, start_link, []},
        restart => temporary,
        shutdown => 30_000,
        type => worker,
        modules => [emqx_agent_pipeline]
    },
    {ok, {SupFlags, [ChildSpec]}}.
