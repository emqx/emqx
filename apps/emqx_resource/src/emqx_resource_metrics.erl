%%--------------------------------------------------------------------
%% Copyright (c) 2022 EMQ Technologies Co., Ltd. All Rights Reserved.
%%
%% Licensed under the Apache License, Version 2.0 (the "License");
%% you may not use this file except in compliance with the License.
%% You may obtain a copy of the License at
%%
%%     http://www.apache.org/licenses/LICENSE-2.0
%%
%% Unless required by applicable law or agreed to in writing, software
%% distributed under the License is distributed on an "AS IS" BASIS,
%% WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
%% See the License for the specific language governing permissions and
%% limitations under the License.
%%--------------------------------------------------------------------

-module(emqx_resource_metrics).

-export([
    events/0,
    install_telemetry_handler/1,
    uninstall_telemetry_handler/1,
    handle_telemetry_event/4
]).

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
-define(TELEMETRY_PREFIX, emqx, resource).

-spec events() -> [telemetry:event_name()].
events() ->
    [
        [?TELEMETRY_PREFIX, Event]
     || Event <- [
            batching,
            dropped_other,
            dropped_queue_full,
            dropped_queue_not_enabled,
            dropped_resource_not_found,
            dropped_resource_stopped,
            failed,
            inflight,
            matched,
            queuing,
            retried_failed,
            retried_success,
            success
        ]
    ].

-spec install_telemetry_handler(binary()) -> ok.
install_telemetry_handler(HandlerID) ->
    _ = telemetry:attach_many(
        HandlerID,
        events(),
        fun ?MODULE:handle_telemetry_event/4,
        _HandlerConfig = #{}
    ),
    ok.

-spec uninstall_telemetry_handler(binary()) -> ok.
uninstall_telemetry_handler(HandlerID) ->
    _ = telemetry:detach(HandlerID),
    ok.

handle_telemetry_event(
    [?TELEMETRY_PREFIX, Event],
    _Measurements = #{counter_inc := Val},
    _Metadata = #{resource_id := ID},
    _HandlerConfig
) ->
    case Event of
        batching ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'batching', Val);
        dropped_other ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other', Val);
        dropped_queue_full ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full', Val);
        dropped_queue_not_enabled ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_not_enabled', Val);
        dropped_resource_not_found ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found', Val);
        dropped_resource_stopped ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped', Val);
        failed ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed', Val);
        inflight ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'inflight', Val);
        matched ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched', Val);
        queuing ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'queuing', Val);
        retried_failed ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.failed', Val);
        retried_success ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'success', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'retried.success', Val);
        success ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'success', Val);
        _ ->
            ok
    end;
handle_telemetry_event(_EventName, _Measurements, _Metadata, _HandlerConfig) ->
    ok.

%% Gauges (value can go both up and down):
%% --------------------------------------

%% @doc Count of messages that are currently accumulated in memory waiting for
%% being sent in one batch
batching_change(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, batching], #{counter_inc => Val}, #{resource_id => ID}).

batching_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'batching').

%% @doc Count of messages that are currently queuing. [Gauge]
queuing_change(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, queuing], #{counter_inc => Val}, #{resource_id => ID}).

queuing_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'queuing').

%% @doc Count of messages that were sent asynchronously but ACKs are not
%% received. [Gauge]
inflight_change(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, inflight], #{counter_inc => Val}, #{resource_id => ID}).

inflight_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'inflight').

%% Counters (value can only got up):
%% --------------------------------------

%% @doc Count of messages dropped
dropped_inc(ID) ->
    dropped_inc(ID, 1).

dropped_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped], #{counter_inc => Val}, #{resource_id => ID}).

dropped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped').

%% @doc Count of messages dropped due to other reasons
dropped_other_inc(ID) ->
    dropped_other_inc(ID, 1).

dropped_other_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_other], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_other_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.other').

%% @doc Count of messages dropped because the queue was full
dropped_queue_full_inc(ID) ->
    dropped_queue_full_inc(ID, 1).

dropped_queue_full_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_queue_full], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_queue_full_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.queue_full').

%% @doc Count of messages dropped because the queue was not enabled
dropped_queue_not_enabled_inc(ID) ->
    dropped_queue_not_enabled_inc(ID, 1).

dropped_queue_not_enabled_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_queue_not_enabled], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_queue_not_enabled_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.queue_not_enabled').

%% @doc Count of messages dropped because the resource was not found
dropped_resource_not_found_inc(ID) ->
    dropped_resource_not_found_inc(ID, 1).

dropped_resource_not_found_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_resource_not_found], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_resource_not_found_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_not_found').

%% @doc Count of messages dropped because the resource was stopped
dropped_resource_stopped_inc(ID) ->
    dropped_resource_stopped_inc(ID, 1).

dropped_resource_stopped_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_resource_stopped], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_resource_stopped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_stopped').

%% @doc Count of how many times this bridge has been matched and queried
matched_inc(ID) ->
    matched_inc(ID, 1).

matched_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, matched], #{counter_inc => Val}, #{resource_id => ID}).

matched_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'matched').

%% @doc The number of times message sends have been retried
retried_inc(ID) ->
    retried_inc(ID, 1).

retried_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried], #{counter_inc => Val}, #{resource_id => ID}).

retried_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried').

%% @doc Count of message sends that have failed
failed_inc(ID) ->
    failed_inc(ID, 1).

failed_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, failed], #{counter_inc => Val}, #{resource_id => ID}).

failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'failed').

%%% @doc Count of message sends that have failed after having been retried
retried_failed_inc(ID) ->
    retried_failed_inc(ID, 1).

retried_failed_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried_failed], #{counter_inc => Val}, #{
        resource_id => ID
    }).

retried_failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.failed').

%% @doc Count messages that were sucessfully sent after at least one retry
retried_success_inc(ID) ->
    retried_success_inc(ID, 1).

retried_success_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried_success], #{counter_inc => Val}, #{
        resource_id => ID
    }).

retried_success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.success').

%% @doc Count of messages that have been sent successfully
success_inc(ID) ->
    success_inc(ID, 1).

success_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, success], #{counter_inc => Val}, #{resource_id => ID}).

success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'success').
