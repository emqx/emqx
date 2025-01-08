%%--------------------------------------------------------------------
%% Copyright (c) 2022-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-include_lib("emqx/include/logger.hrl").
-include("emqx_resource.hrl").

-export([
    events/0,
    install_telemetry_handler/1,
    uninstall_telemetry_handler/1,
    handle_telemetry_event/4
]).

-export([
    inflight_set/3,
    inflight_get/1,
    queuing_set/3,
    queuing_get/1,
    queuing_bytes_set/3,
    queuing_bytes_get/1,
    dropped_inc/1,
    dropped_inc/2,
    dropped_get/1,
    dropped_other_inc/1,
    dropped_other_inc/2,
    dropped_other_get/1,
    dropped_expired_inc/1,
    dropped_expired_inc/2,
    dropped_expired_get/1,
    dropped_queue_full_inc/1,
    dropped_queue_full_inc/2,
    dropped_queue_full_get/1,
    dropped_resource_not_found_inc/1,
    dropped_resource_not_found_inc/2,
    dropped_resource_not_found_get/1,
    dropped_resource_stopped_inc/1,
    dropped_resource_stopped_inc/2,
    dropped_resource_stopped_get/1,
    failed_inc/1,
    failed_inc/2,
    failed_get/1,
    late_reply_inc/1,
    late_reply_inc/2,
    late_reply_get/1,
    matched_inc/1,
    matched_inc/2,
    matched_get/1,
    received_inc/1,
    received_inc/2,
    received_get/1,
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

-define(TELEMETRY_PREFIX, emqx, resource).

-spec events() -> [telemetry:event_name()].
events() ->
    [
        [?TELEMETRY_PREFIX, Event]
     || Event <- [
            dropped_other,
            dropped_expired,
            dropped_queue_full,
            dropped_resource_not_found,
            dropped_resource_stopped,
            late_reply,
            failed,
            inflight,
            matched,
            queuing,
            queuing_bytes,
            received,
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
    try
        handle_counter_telemetry_event(Event, ID, Val)
    catch
        Kind:Reason:Stacktrace ->
            %% We catch errors to avoid detaching the telemetry handler function.
            %% When restarting a resource while it's under load, there might be transient
            %% failures while the metrics are not yet created.
            ?SLOG_THROTTLE(
                warning,
                ID,
                #{
                    msg => handle_resource_metrics_failed,
                    hint => "transient failures may occur when restarting a resource",
                    kind => Kind,
                    reason => Reason,
                    stacktrace => Stacktrace,
                    resource_id => ID,
                    event => Event
                },
                #{tag => ?TAG}
            ),
            ok
    end;
handle_telemetry_event(
    [?TELEMETRY_PREFIX, Event],
    _Measurements = #{gauge_set := Val},
    _Metadata = #{resource_id := ID, worker_id := WorkerID},
    _HandlerConfig
) ->
    try
        handle_gauge_telemetry_event(Event, ID, WorkerID, Val)
    catch
        Kind:Reason:Stacktrace ->
            %% We catch errors to avoid detaching the telemetry handler function.
            %% When restarting a resource while it's under load, there might be transient
            %% failures while the metrics are not yet created.
            ?SLOG_THROTTLE(
                warning,
                ID,
                #{
                    msg => handle_resource_metrics_failed,
                    hint => "transient failures may occur when restarting a resource",
                    kind => Kind,
                    reason => Reason,
                    stacktrace => Stacktrace,
                    resource_id => ID,
                    event => Event
                },
                #{tag => ?TAG}
            ),
            ok
    end;
handle_telemetry_event(_EventName, _Measurements, _Metadata, _HandlerConfig) ->
    ok.

handle_counter_telemetry_event(Event, ID, Val) ->
    case Event of
        dropped_other ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.other', Val);
        dropped_expired ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.expired', Val);
        dropped_queue_full ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.queue_full', Val);
        dropped_resource_not_found ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_not_found', Val);
        dropped_resource_stopped ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped', Val),
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'dropped.resource_stopped', Val);
        late_reply ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'late_reply', Val);
        failed ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'failed', Val);
        matched ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'matched', Val);
        received ->
            emqx_metrics_worker:inc(?RES_METRICS, ID, 'received', Val);
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
    end.

handle_gauge_telemetry_event(Event, ID, WorkerID, Val) ->
    case Event of
        inflight ->
            emqx_metrics_worker:set_gauge(?RES_METRICS, ID, WorkerID, 'inflight', Val);
        queuing ->
            emqx_metrics_worker:set_gauge(?RES_METRICS, ID, WorkerID, 'queuing', Val);
        queuing_bytes ->
            emqx_metrics_worker:set_gauge(?RES_METRICS, ID, WorkerID, 'queuing_bytes', Val);
        _ ->
            ok
    end.

%% Gauges (value can go both up and down):
%% --------------------------------------

%% @doc Count of batches of messages that are currently
%% queuing. [Gauge]
queuing_set(ID, WorkerID, Val) ->
    telemetry:execute(
        [?TELEMETRY_PREFIX, queuing],
        #{gauge_set => Val},
        #{resource_id => ID, worker_id => WorkerID}
    ).

queuing_get(ID) ->
    emqx_metrics_worker:get_gauge(?RES_METRICS, ID, 'queuing').

%% @doc Number of bytes currently queued. [Gauge]
queuing_bytes_set(ID, WorkerID, Val) ->
    telemetry:execute(
        [?TELEMETRY_PREFIX, queuing_bytes],
        #{gauge_set => Val},
        #{resource_id => ID, worker_id => WorkerID}
    ).

queuing_bytes_get(ID) ->
    emqx_metrics_worker:get_gauge(?RES_METRICS, ID, 'queuing_bytes').

%% @doc Count of batches of messages that were sent asynchronously but
%% ACKs are not yet received. [Gauge]
inflight_set(ID, WorkerID, Val) ->
    telemetry:execute(
        [?TELEMETRY_PREFIX, inflight],
        #{gauge_set => Val},
        #{resource_id => ID, worker_id => WorkerID}
    ).

inflight_get(ID) ->
    emqx_metrics_worker:get_gauge(?RES_METRICS, ID, 'inflight').

%% Counters (value can only got up):
%% --------------------------------------

%% @doc Count of messages dropped
dropped_inc(ID) ->
    dropped_inc(ID, 1).

dropped_inc(_ID, 0) ->
    ok;
dropped_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped], #{counter_inc => Val}, #{resource_id => ID}).

dropped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped').

%% @doc Count of messages dropped due to other reasons
dropped_other_inc(ID) ->
    dropped_other_inc(ID, 1).

dropped_other_inc(_ID, 0) ->
    ok;
dropped_other_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_other], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_other_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.other').

%% @doc Count of messages dropped due to being expired before being sent.
dropped_expired_inc(ID) ->
    dropped_expired_inc(ID, 1).

dropped_expired_inc(_ID, 0) ->
    ok;
dropped_expired_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_expired], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_expired_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.expired').

%% @doc Count of messages that were sent but received a late reply.
late_reply_inc(ID) ->
    late_reply_inc(ID, 1).

late_reply_inc(_ID, 0) ->
    ok;
late_reply_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, late_reply], #{counter_inc => Val}, #{
        resource_id => ID
    }).

late_reply_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'late_reply').

%% @doc Count of messages dropped because the queue was full
dropped_queue_full_inc(ID) ->
    dropped_queue_full_inc(ID, 1).

dropped_queue_full_inc(_ID, 0) ->
    ok;
dropped_queue_full_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_queue_full], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_queue_full_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.queue_full').

%% @doc Count of messages dropped because the resource was not found
dropped_resource_not_found_inc(ID) ->
    dropped_resource_not_found_inc(ID, 1).

dropped_resource_not_found_inc(_ID, 0) ->
    ok;
dropped_resource_not_found_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_resource_not_found], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_resource_not_found_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_not_found').

%% @doc Count of messages dropped because the resource was stopped
dropped_resource_stopped_inc(ID) ->
    dropped_resource_stopped_inc(ID, 1).

dropped_resource_stopped_inc(_ID, 0) ->
    ok;
dropped_resource_stopped_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, dropped_resource_stopped], #{counter_inc => Val}, #{
        resource_id => ID
    }).

dropped_resource_stopped_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'dropped.resource_stopped').

%% @doc Count of how many times this bridge has been matched and queried
matched_inc(ID) ->
    matched_inc(ID, 1).

matched_inc(_ID, 0) ->
    ok;
matched_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, matched], #{counter_inc => Val}, #{resource_id => ID}).

matched_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'matched').

%% @doc The number of messages that have been received from a bridge
received_inc(ID) ->
    received_inc(ID, 1).

received_inc(_ID, 0) ->
    ok;
received_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, received], #{counter_inc => Val}, #{resource_id => ID}).

received_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'received').

%% @doc The number of times message sends have been retried
retried_inc(ID) ->
    retried_inc(ID, 1).

retried_inc(_ID, 0) ->
    ok;
retried_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried], #{counter_inc => Val}, #{resource_id => ID}).

retried_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried').

%% @doc Count of message sends that have failed
failed_inc(ID) ->
    failed_inc(ID, 1).

failed_inc(_ID, 0) ->
    ok;
failed_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, failed], #{counter_inc => Val}, #{resource_id => ID}).

failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'failed').

%%% @doc Count of message sends that have failed after having been retried
retried_failed_inc(ID) ->
    retried_failed_inc(ID, 1).

retried_failed_inc(_ID, 0) ->
    ok;
retried_failed_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried_failed], #{counter_inc => Val}, #{
        resource_id => ID
    }).

retried_failed_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.failed').

%% @doc Count messages that were successfully sent after at least one retry
retried_success_inc(ID) ->
    retried_success_inc(ID, 1).

retried_success_inc(_ID, 0) ->
    ok;
retried_success_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, retried_success], #{counter_inc => Val}, #{
        resource_id => ID
    }).

retried_success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'retried.success').

%% @doc Count of messages that have been sent successfully
success_inc(ID) ->
    success_inc(ID, 1).

success_inc(_ID, 0) ->
    ok;
success_inc(ID, Val) ->
    telemetry:execute([?TELEMETRY_PREFIX, success], #{counter_inc => Val}, #{resource_id => ID}).

success_get(ID) ->
    emqx_metrics_worker:get(?RES_METRICS, ID, 'success').
