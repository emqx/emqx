%%--------------------------------------------------------------------
%% Copyright (c) 2020-2022 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(EMPTY_METRICS,
    ?METRICS(
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    )
).

-define(METRICS(
    Batched,
    Dropped,
    DroppedOther,
    DroppedQueueFull,
    DroppedQueueNotEnabled,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    Retried,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'batching' => Batched,
        'dropped' => Dropped,
        'dropped.other' => DroppedOther,
        'dropped.queue_full' => DroppedQueueFull,
        'dropped.queue_not_enabled' => DroppedQueueNotEnabled,
        'dropped.resource_not_found' => DroppedResourceNotFound,
        'dropped.resource_stopped' => DroppedResourceStopped,
        'matched' => Matched,
        'queuing' => Queued,
        'retried' => Retried,
        'failed' => SentFailed,
        'inflight' => SentInflight,
        'success' => SentSucc,
        rate => RATE,
        rate_last5m => RATE_5,
        rate_max => RATE_MAX,
        received => Rcvd
    }
).

-define(metrics(
    Batched,
    Dropped,
    DroppedOther,
    DroppedQueueFull,
    DroppedQueueNotEnabled,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    Retried,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'batching' := Batched,
        'dropped' := Dropped,
        'dropped.other' := DroppedOther,
        'dropped.queue_full' := DroppedQueueFull,
        'dropped.queue_not_enabled' := DroppedQueueNotEnabled,
        'dropped.resource_not_found' := DroppedResourceNotFound,
        'dropped.resource_stopped' := DroppedResourceStopped,
        'matched' := Matched,
        'queuing' := Queued,
        'retried' := Retried,
        'failed' := SentFailed,
        'inflight' := SentInflight,
        'success' := SentSucc,
        rate := RATE,
        rate_last5m := RATE_5,
        rate_max := RATE_MAX,
        received := Rcvd
    }
).

-define(METRICS_EXAMPLE, #{
    metrics => ?EMPTY_METRICS,
    node_metrics => [
        #{
            node => node(),
            metrics => ?EMPTY_METRICS
        }
    ]
}).
