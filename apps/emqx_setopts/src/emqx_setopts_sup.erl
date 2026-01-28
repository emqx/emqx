%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_setopts_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

-define(SERVER, ?MODULE).

-doc """
Start the setopts supervisor.
""".
start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_setopts,
            start => {emqx_setopts, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_setopts]
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
