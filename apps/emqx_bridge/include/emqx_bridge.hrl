%%--------------------------------------------------------------------
%% Copyright (c) 2020-2025 EMQ Technologies Co., Ltd. All Rights Reserved.
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

-define(EMPTY_METRICS_V1,
    ?METRICS_V1(
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    )
).

-define(METRICS_V1(
    Dropped,
    DroppedOther,
    DroppedExpired,
    DroppedQueueFull,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    Retried,
    LateReply,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'dropped' => Dropped,
        'dropped.other' => DroppedOther,
        'dropped.expired' => DroppedExpired,
        'dropped.queue_full' => DroppedQueueFull,
        'dropped.resource_not_found' => DroppedResourceNotFound,
        'dropped.resource_stopped' => DroppedResourceStopped,
        'matched' => Matched,
        'queuing' => Queued,
        'retried' => Retried,
        'late_reply' => LateReply,
        'failed' => SentFailed,
        'inflight' => SentInflight,
        'success' => SentSucc,
        rate => RATE,
        rate_last5m => RATE_5,
        rate_max => RATE_MAX,
        received => Rcvd
    }
).

-define(metrics_v1(
    Dropped,
    DroppedOther,
    DroppedExpired,
    DroppedQueueFull,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    Retried,
    LateReply,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'dropped' := Dropped,
        'dropped.other' := DroppedOther,
        'dropped.expired' := DroppedExpired,
        'dropped.queue_full' := DroppedQueueFull,
        'dropped.resource_not_found' := DroppedResourceNotFound,
        'dropped.resource_stopped' := DroppedResourceStopped,
        'matched' := Matched,
        'queuing' := Queued,
        'retried' := Retried,
        'late_reply' := LateReply,
        'failed' := SentFailed,
        'inflight' := SentInflight,
        'success' := SentSucc,
        rate := RATE,
        rate_last5m := RATE_5,
        rate_max := RATE_MAX,
        received := Rcvd
    }
).

-define(EMPTY_METRICS,
    ?METRICS(
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
    )
).

-define(METRICS(
    Dropped,
    DroppedOther,
    DroppedExpired,
    DroppedQueueFull,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    QueuedBytes,
    Retried,
    LateReply,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'dropped' => Dropped,
        'dropped.other' => DroppedOther,
        'dropped.expired' => DroppedExpired,
        'dropped.queue_full' => DroppedQueueFull,
        'dropped.resource_not_found' => DroppedResourceNotFound,
        'dropped.resource_stopped' => DroppedResourceStopped,
        'matched' => Matched,
        'queuing' => Queued,
        'queuing_bytes' => QueuedBytes,
        'retried' => Retried,
        'late_reply' => LateReply,
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
    Dropped,
    DroppedOther,
    DroppedExpired,
    DroppedQueueFull,
    DroppedResourceNotFound,
    DroppedResourceStopped,
    Matched,
    Queued,
    QueuedBytes,
    Retried,
    LateReply,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'dropped' := Dropped,
        'dropped.other' := DroppedOther,
        'dropped.expired' := DroppedExpired,
        'dropped.queue_full' := DroppedQueueFull,
        'dropped.resource_not_found' := DroppedResourceNotFound,
        'dropped.resource_stopped' := DroppedResourceStopped,
        'matched' := Matched,
        'queuing' := Queued,
        'queuing_bytes' := QueuedBytes,
        'retried' := Retried,
        'late_reply' := LateReply,
        'failed' := SentFailed,
        'inflight' := SentInflight,
        'success' := SentSucc,
        rate := RATE,
        rate_last5m := RATE_5,
        rate_max := RATE_MAX,
        received := Rcvd
    }
).
