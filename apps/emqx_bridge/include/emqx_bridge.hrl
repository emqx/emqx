-define(EMPTY_METRICS,
    ?METRICS(
        0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0, 0
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
    Sent,
    SentExcpt,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'batched' => Batched,
        'dropped' => Dropped,
        'dropped.other' => DroppedOther,
        'dropped.queue_full' => DroppedQueueFull,
        'dropped.queue_not_enabled' => DroppedQueueNotEnabled,
        'dropped.resource_not_found' => DroppedResourceNotFound,
        'dropped.resource_stopped' => DroppedResourceStopped,
        'matched' => Matched,
        'queued' => Queued,
        'sent' => Sent,
        'sent.exception' => SentExcpt,
        'sent.failed' => SentFailed,
        'sent.inflight' => SentInflight,
        'sent.success' => SentSucc,
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
    Sent,
    SentExcpt,
    SentFailed,
    SentInflight,
    SentSucc,
    RATE,
    RATE_5,
    RATE_MAX,
    Rcvd
),
    #{
        'batched' := Batched,
        'dropped' := Dropped,
        'dropped.other' := DroppedOther,
        'dropped.queue_full' := DroppedQueueFull,
        'dropped.queue_not_enabled' := DroppedQueueNotEnabled,
        'dropped.resource_not_found' := DroppedResourceNotFound,
        'dropped.resource_stopped' := DroppedResourceStopped,
        'matched' := Matched,
        'queued' := Queued,
        'sent' := Sent,
        'sent.exception' := SentExcpt,
        'sent.failed' := SentFailed,
        'sent.inflight' := SentInflight,
        'sent.success' := SentSucc,
        rate := RATE,
        rate_last5m := RATE_5,
        rate_max := RATE_MAX,
        received := Rcvd
    }
).
