%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_dashboard_sup).

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

-define(CHILD(I, ShutDown), {I, {I, start_link, []}, permanent, ShutDown, worker, [I]}).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    %% supervisor owns the cache table
    ok = emqx_dashboard_desc_cache:init(),
    {ok,
        {{one_for_one, 5, 100}, [
            ?CHILD(emqx_dashboard_listener, brutal_kill),
            ?CHILD(emqx_dashboard_token, 5000),
            ?CHILD(emqx_dashboard_monitor, 5000)
        ]}}.
