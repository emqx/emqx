%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_maptabs_sup).

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
    Server = #{
        id => emqx_maptabs_server,
        start => {emqx_maptabs_server, start_link, []},
        type => worker,
        modules => [emqx_maptabs_server],
        restart => permanent,
        shutdown => 5000
    },
    {ok, {SupFlags, [Server]}}.
