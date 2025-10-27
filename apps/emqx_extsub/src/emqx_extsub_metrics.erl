%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_extsub_metrics).
-include_lib("emqx/include/http_api.hrl").

-export([
    child_spec/0,
    inc/2,
    inc/1,
    observe_hist/2
]).

-export([
    get_rates/0,
    get_counters/0
]).

-export([
    print_hists/0
]).

-define(EXT_SUB_METRICS_WORKER, ext_sub_metrics).
-define(EXT_SUB_METRICS_ID, ext_sub_metrics_id).

-define(LATENCY_BUCKETS, [
    2,
    5,
    10,
    20,
    50,
    100,
    250,
    500,
    750,
    1000,
    2000,
    5000
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_WORKER, [
        {?EXT_SUB_METRICS_ID, [
            {counter, delivered_messages},
            {hist, handle_info_latency_ms, ?LATENCY_BUCKETS}
        ]}
    ]).

inc(Metric) ->
    inc(Metric, 1).

inc(Metric, Val) ->
    emqx_metrics_worker:inc(?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_ID, Metric, Val).

get_rates() ->
    #{rate := Rates} = emqx_metrics_worker:get_metrics(
        ?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_ID
    ),
    Rates.

get_counters() ->
    #{counters := Counters} = emqx_metrics_worker:get_metrics(
        ?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_ID
    ),
    Counters.

observe_hist(Metric, Val) ->
    emqx_metrics_worker:observe_hist(?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_ID, Metric, Val).

print_hists() ->
    emqx_utils_metrics:print_hists(?EXT_SUB_METRICS_WORKER, ?EXT_SUB_METRICS_ID).
