%%--------------------------------------------------------------------
%% Copyright (c) 2023-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_tls_lib_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

%%--------------------------------------------------------------------
%% Supervisor callbacks
%%--------------------------------------------------------------------

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 100,
        period => 10
    },
    GC = #{
        id => emqx_tls_certfile_gc,
        start => {emqx_tls_certfile_gc, start_link, []},
        restart => permanent,
        shutdown => brutal_kill,
        type => worker
    },
    {ok, {SupFlags, [GC]}}.
