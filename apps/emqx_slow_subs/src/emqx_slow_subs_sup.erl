%%--------------------------------------------------------------------
%% Copyright (c) 2021-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_slow_subs_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    emqx_slow_subs:init_tab(),
    {ok,
        {{one_for_one, 10, 3600}, [
            #{
                id => st_statistics,
                start => {emqx_slow_subs, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_slow_subs]
            }
        ]}}.
