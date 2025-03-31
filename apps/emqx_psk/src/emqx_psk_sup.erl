%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_psk_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    {ok,
        {{one_for_one, 10, 3600}, [
            #{
                id => emqx_psk,
                start => {emqx_psk, start_link, []},
                restart => permanent,
                shutdown => 5000,
                type => worker,
                modules => [emqx_psk]
            }
        ]}}.
