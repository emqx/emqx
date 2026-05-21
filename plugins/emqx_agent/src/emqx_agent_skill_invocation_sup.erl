%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_skill_invocation_sup).

-moduledoc """
Supervisor for skill invocation workers.

Each invocation is a short-lived temporary worker that executes a
skill's handle_invoke/2 with a configurable timeout.
""".

-behaviour(supervisor).

-export([start_link/0, start_invocation/6]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_invocation(binary(), binary(), module(), map(), map(), non_neg_integer()) ->
    {ok, pid()} | {error, term()}.
start_invocation(Type, SkillId, Module, Context, Request, Timeout) ->
    supervisor:start_child(?MODULE, [Type, SkillId, Module, Context, Request, Timeout]).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpec = #{
        id => emqx_agent_skill_invocation,
        start => {emqx_agent_skill_invocation, start_link, []},
        restart => temporary,
        shutdown => 5000,
        type => worker,
        modules => [emqx_agent_skill_invocation]
    },
    {ok, {SupFlags, [ChildSpec]}}.
