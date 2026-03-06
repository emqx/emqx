%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_unsgov_sup).

-behaviour(supervisor).

-export([start_link/0, init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 10
    },
    ChildSpecs = [
        #{
            id => emqx_unsgov_metrics,
            start => {emqx_unsgov_metrics, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 1_000
        },
        #{
            id => emqx_unsgov_store,
            start => {emqx_unsgov_store, start_link, []},
            type => worker,
            restart => permanent,
            shutdown => 1_000
        }
    ],
    {ok, {SupFlags, ChildSpecs}}.
