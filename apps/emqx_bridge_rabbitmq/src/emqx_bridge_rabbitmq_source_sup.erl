%%--------------------------------------------------------------------
%% Copyright (c) 2024-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_bridge_rabbitmq_source_sup).

-behaviour(supervisor).
%% API
-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link(?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 10
    },
    {ok, {SupFlags, [worker_spec()]}}.

worker_spec() ->
    Mod = emqx_bridge_rabbitmq_source_worker,
    #{
        id => Mod,
        start => {Mod, start_link, []},
        restart => transient,
        shutdown => brutal_kill,
        type => worker,
        modules => [Mod]
    }.
