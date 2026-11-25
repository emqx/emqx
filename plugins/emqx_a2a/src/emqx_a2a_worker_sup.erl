%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_worker_sup).

-moduledoc """
Dynamic supervisor for A2A agent worker processes.
""".

-behaviour(supervisor).

-export([start_link/0, start_worker/1, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

start_worker(Args) ->
    supervisor:start_child(?MODULE, [Args]).

init([]) ->
    SupFlags = #{
        strategy => simple_one_for_one,
        intensity => 100,
        period => 10
    },
    ChildSpec = #{
        id => emqx_a2a_worker,
        start => {emqx_a2a_worker, start_link, []},
        type => worker,
        restart => temporary,
        shutdown => 30_000
    },
    {ok, {SupFlags, [ChildSpec]}}.
