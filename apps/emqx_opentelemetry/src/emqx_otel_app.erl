%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_otel_app).

-behaviour(application).

-export([start/2, stop/1]).
-export([configure_otel_deps/0]).

start(_StartType, _StartArgs) ->
    emqx_otel_config:add_handler(),
    ok = emqx_otel_sampler:init_tables(),
    ok = emqx_otel_config:add_otel_log_handler(),
    ok = emqx_otel_trace:ensure_traces(emqx:get_config([opentelemetry])),
    emqx_otel_sup:start_link().

stop(_State) ->
    emqx_otel_config:remove_handler(),
    _ = emqx_otel_trace:stop(),
    _ = emqx_otel_config:remove_otel_log_handler(),
    ok.

configure_otel_deps() ->
    %% default tracer and metrics are started only on demand
    ok = application:set_env(
        [
            {opentelemetry, [{start_default_tracer, false}]},
            {opentelemetry_experimental, [{start_default_metrics, false}]}
        ],
        [{persistent, true}]
    ).
