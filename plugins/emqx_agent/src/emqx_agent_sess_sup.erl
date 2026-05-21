%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_agent_sess_sup).

-moduledoc """
Supervisor for session gen_servers.

Sessions are lazy: started on the first message arriving at
sess/in/<SID>/. They terminate normally once the LLM loop finishes
and there is nothing left to process, so restart => transient.
""".

-behaviour(supervisor).

-export([start_link/0, start_session/2]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

-spec start_session(binary(), boolean()) -> {ok, pid()} | {error, term()}.
start_session(Sid, Persistent) ->
    supervisor:start_child(?MODULE, [Sid, Persistent]).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpec = #{
        id => emqx_agent_session,
        start => {emqx_agent_session, start_link, []},
        restart => transient,
        shutdown => 30000,
        type => worker,
        modules => [emqx_agent_session]
    },
    {ok, {SupFlags, [ChildSpec]}}.
