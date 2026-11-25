%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_a2a_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    ok = emqx_a2a_store:create_tables(),
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_a2a_store,
            start => {emqx_a2a_store, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        },
        #{
            id => emqx_a2a_worker_sup,
            start => {emqx_a2a_worker_sup, start_link, []},
            type => supervisor,
            restart => permanent,
            shutdown => infinity
        },
        #{
            id => emqx_a2a_session_mgr,
            start => {emqx_a2a_session_mgr, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 5_000
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
