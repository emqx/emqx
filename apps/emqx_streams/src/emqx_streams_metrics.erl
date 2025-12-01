%%--------------------------------------------------------------------
%% Copyright (c) 2025 EMQ Technologies Co., Ltd. All Rights Reserved.
%%--------------------------------------------------------------------

-module(emqx_streams_metrics).

-include_lib("snabbkaffe/include/trace.hrl").

-export([
    child_spec/0,
    inc/2,
    inc/3,
    observe_hist_stream/3,
    inc_stream/2,
    inc_stream/3,
    observe_hist/3,
    set_quota_buffer_inbox_size/2
]).

-export([
    get_rates/1,
    get_counters/1,
    get_quota_buffer_inbox_size/0
]).

-export([
    print_common_hists/0,
    print_common_hists/1,
    print_flush_quota_hist/0
]).

-define(STREAMS_METRICS_WORKER, streams_metrics).

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

-define(COMMON_STREAMS_METRICS, [
    {counter, insert_errors},
    {counter, insert_ok},
    {hist, insert_latency_ms, ?LATENCY_BUCKETS}
]).

%%--------------------------------------------------------------------
%% API
%%--------------------------------------------------------------------

child_spec() ->
    emqx_metrics_worker:child_spec(
        ?STREAMS_METRICS_WORKER,
        ?STREAMS_METRICS_WORKER,
        [
            {ds, [
                {counter, received_messages},
                {counter, inserted_messages}
            ]},
            {flush_quota_index, [
                {hist, flush_latency_ms, ?LATENCY_BUCKETS},
                {counter, flush_errors}
            ]},
            {regular_limited, ?COMMON_STREAMS_METRICS},
            {regular_unlimited, ?COMMON_STREAMS_METRICS},
            {lastvalue_limited, ?COMMON_STREAMS_METRICS},
            {lastvalue_unlimited, ?COMMON_STREAMS_METRICS}
        ]
    ).

inc(Id, Metric) ->
    inc(Id, Metric, 1).

inc(Id, Metric, Val) ->
    emqx_metrics_worker:inc(?STREAMS_METRICS_WORKER, Id, Metric, Val).

get_rates(Id) ->
    #{rate := Rates} = emqx_metrics_worker:get_metrics(?STREAMS_METRICS_WORKER, Id),
    Rates.

get_counters(Id) ->
    #{counters := Counters} = emqx_metrics_worker:get_metrics(?STREAMS_METRICS_WORKER, Id),
    Counters.

observe_hist(Id, Metric, Val) ->
    emqx_metrics_worker:observe_hist(?STREAMS_METRICS_WORKER, Id, Metric, Val).

inc_stream(Stream, Metric) ->
    inc_stream(Stream, Metric, 1).

inc_stream(Stream, Metric, Val) ->
    Id = stream_metrics_id(Stream),
    inc(Id, Metric, Val).

observe_hist_stream(Stream, Metric, Val) ->
    Id = stream_metrics_id(Stream),
    observe_hist(Id, Metric, Val).

set_quota_buffer_inbox_size(WorkerId, Val) ->
    ok = emqx_metrics_worker:set_gauge(
        ?STREAMS_METRICS_WORKER, flush_quota_index, WorkerId, process_inbox_size, Val
    ).

get_quota_buffer_inbox_size() ->
    emqx_metrics_worker:get_gauge(?STREAMS_METRICS_WORKER, flush_quota_index, process_inbox_size).

print_common_hists() ->
    lists:foreach(
        fun(Id) -> emqx_utils_metrics:print_hists(?STREAMS_METRICS_WORKER, Id) end,
        [regular_limited, regular_unlimited, lastvalue_limited, lastvalue_unlimited]
    ).

print_flush_quota_hist() ->
    emqx_utils_metrics:print_hists(?STREAMS_METRICS_WORKER, flush_quota_index).

print_common_hists(Id) ->
    emqx_utils_metrics:print_hists(?STREAMS_METRICS_WORKER, Id).

%%--------------------------------------------------------------------
%% Internal functions
%%--------------------------------------------------------------------

stream_metrics_id(Stream) ->
    stream_metrics_id(emqx_streams_prop:is_lastvalue(Stream), emqx_streams_prop:is_limited(Stream)).

stream_metrics_id(true = _IsLastvalue, true = _IsLimited) ->
    lastvalue_limited;
stream_metrics_id(true = _IsLastvalue, false = _IsLimited) ->
    lastvalue_unlimited;
stream_metrics_id(false = _IsLastvalue, true = _IsLimited) ->
    regular_limited;
stream_metrics_id(false = _IsLastvalue, false = _IsLimited) ->
    regular_unlimited.
