%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_mq_metrics).

-export([
    child_spec/0,
    inc/2,
    inc/3,
    get_rates/1
]).

-define(MQ_METRICS_WORKER, mq_metrics).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(
        ?MQ_METRICS_WORKER,
        ?MQ_METRICS_WORKER,
        [
            {ds, [{counter, received_messages}]}
        ]
    ).

inc(Id, Metric) ->
    emqx_metrics_worker:inc(?MQ_METRICS_WORKER, Id, Metric).

inc(Id, Metric, Val) ->
    emqx_metrics_worker:inc(?MQ_METRICS_WORKER, Id, Metric, Val).

get_rates(Id) ->
    #{rate := Rates} = emqx_metrics_worker:get_metrics(?MQ_METRICS_WORKER, Id),
    Rates.
