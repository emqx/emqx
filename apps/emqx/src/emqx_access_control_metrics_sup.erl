%%--------------------------------------------------------------------
%% Copyright (c) 2018-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_access_control_metrics_sup).

-include("emqx.hrl").

-behaviour(supervisor).

-export([start_link/0]).

-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    AccessControlMetrics = emqx_metrics_worker:child_spec(
        ?MODULE,
        ?ACCESS_CONTROL_METRICS_WORKER,
        [
            {'client.authenticate', [{hist, total_latency}]},
            {'client.authorize', [{hist, total_latency}]}
        ]
    ),
    {ok,
        {
            {one_for_one, 10, 100},
            [AccessControlMetrics]
        }}.
