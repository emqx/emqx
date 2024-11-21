%%--------------------------------------------------------------------
%% Copyright (c) 2024 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mt_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(SERVER, ?MODULE).

start_link() ->
    supervisor:start_link({local, ?SERVER}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_all,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_mt,
            start => {emqx_mt, start_link, []},
            type => worker,
            restart => transient,
            shutdown => 1000
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
