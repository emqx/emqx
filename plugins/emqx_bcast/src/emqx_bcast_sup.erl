%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bcast_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{strategy => one_for_one, intensity => 10, period => 3600},
    Children = [
        #{
            id => emqx_bcast_cleanup,
            start => {emqx_bcast_cleanup, start_link, []},
            restart => permanent,
            shutdown => 5000,
            type => worker,
            modules => [emqx_bcast_cleanup]
        }
    ],
    {ok, {SupFlags, Children}}.
