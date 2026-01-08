%%--------------------------------------------------------------------
%% Copyright (c) 2020-2026 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_prometheus_app).

-behaviour(application).

-include("emqx_prometheus.hrl").

%% Application callbacks
-export([
    start/2,
    stop/1
]).

start(_StartType, _StartArgs) ->
    Res = emqx_prometheus_sup:start_link(),
    emqx_prometheus_config:add_handler(),
    init_latency_metrics(),
    ok = emqx_prometheus_limiter:create_api_limiter_group(),
    Res.

stop(_State) ->
    emqx_prometheus_config:remove_handler(),
    ok = emqx_prometheus_limiter:delete_api_limiter_group(),
    ok.

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

init_latency_metrics() ->
    emqx_prometheus_auth:init_latency_metrics().
