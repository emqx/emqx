-module(emqx_resource_metrics).

-export([
    batching_change/2,
    batching_get/1,
    inflight_change/2,
    inflight_get/1,
    queuing_change/2,
    queuing_get/1,
    dropped_inc/1,
    dropped_inc/2,
    dropped_get/1,
    dropped_other_inc/1,
    dropped_other_inc/2,
    dropped_other_get/1,
    dropped_queue_full_inc/1,
    dropped_queue_full_inc/2,
    dropped_queue_full_get/1,
    dropped_queue_not_enabled_inc/1,
    dropped_queue_not_enabled_inc/2,
    dropped_queue_not_enabled_get/1,
    dropped_resource_not_found_inc/1,
    dropped_resource_not_found_inc/2,
    dropped_resource_not_found_get/1,
    dropped_resource_stopped_inc/1,
    dropped_resource_stopped_inc/2,
    dropped_resource_stopped_get/1,
    failed_inc/1,
    failed_inc/2,
    failed_get/1,
    matched_inc/1,
    matched_inc/2,
    matched_get/1,
    retried_inc/1,
    retried_inc/2,
    retried_get/1,
    retried_failed_inc/1,
    retried_failed_inc/2,
    retried_failed_get/1,
    retried_success_inc/1,
    retried_success_inc/2,
    retried_success_get/1,
    success_inc/1,
    success_inc/2,
    success_get/1
]).

-define(RES_METRICS, resource_metrics).

%% Gauges (value can go both up and down):
%% --------------------------------------

%% @doc Count of messages that are currently accumulated in memory waiting for
%% being sent in one batch
batching_change(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'batching', Val).

batching_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'batching').

%% @doc Count of messages that are currently queuing. [Gauge]
queuing_change(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'queuing', Val).

queuing_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'queuing').

%% @doc Count of messages that were sent asynchronously but ACKs are not
%% received. [Gauge]
inflight_change(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'inflight', Val).

inflight_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'inflight').

%% Counters (value can only got up):
%% --------------------------------------

%% @doc Count of messages dropped
dropped_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped').

dropped_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val).

dropped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped').

%% @doc Count of messages dropped due to other reasons
dropped_other_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other').

dropped_other_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other', Val).

dropped_other_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.other').

%% @doc Count of messages dropped because the queue was full
dropped_queue_full_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full').

dropped_queue_full_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full', Val).

dropped_queue_full_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.queue_full').

%% @doc Count of messages dropped because the queue was not enabled
dropped_queue_not_enabled_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_not_enabled').

dropped_queue_not_enabled_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_not_enabled', Val).

dropped_queue_not_enabled_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.queue_not_enabled').

%% @doc Count of messages dropped because the resource was not found
dropped_resource_not_found_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found').

dropped_resource_not_found_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found', Val).

dropped_resource_not_found_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_not_found').

%% @doc Count of messages dropped because the resource was stopped
dropped_resource_stopped_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped').

dropped_resource_stopped_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped', Val).

dropped_resource_stopped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_stopped').

%% @doc Count of how many times this bridge has been matched and queried
matched_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched').

matched_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched', Val).

matched_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'matched').

%% @doc The number of times message sends have been retried
retried_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried').

retried_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried', Val).

retried_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried').

%% @doc Count of message sends that have failed
failed_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed').

failed_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed', Val).

failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'failed').

%%% @doc Count of message sends that have failed after having been retried
retried_failed_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.failed').

retried_failed_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.failed', Val).

retried_failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.failed').

%% @doc Count messages that were sucessfully sent after at least one retry
retried_success_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.success').

retried_success_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.success', Val).

retried_success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.success').

%% @doc Count of messages that have been sent successfully
success_inc(ID) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'success').

success_inc(ID, Val) ->
    emqx_metrics_worker:inc(?RES_METRICS, ID, 'success', Val).

success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'success').
