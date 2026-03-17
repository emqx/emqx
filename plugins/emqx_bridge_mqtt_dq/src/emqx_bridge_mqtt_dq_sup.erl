%%--------------------------------------------------------------------
%% Copyright (c) 2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------
-module(emqx_bridge_mqtt_dq_sup).

-behaviour(supervisor).

-export([start_link/0]).
-export([init/1]).

start_link() ->
    supervisor:start_link({local, ?MODULE}, ?MODULE, []).

init([]) ->
    SupFlags = #{
        strategy => one_for_one,
        intensity => 10,
        period => 60
    },
    Metrics = #{
        id => emqx_bridge_mqtt_dq_metrics,
        start => {emqx_bridge_mqtt_dq_metrics, start_link, []},
        restart => permanent,
        shutdown => 5000,
        type => worker,
        modules => [emqx_bridge_mqtt_dq_metrics]
    },
    {ok, {SupFlags, [Metrics]}}.
