Changed the default of `mq.auto_create` and `streams.auto_create` to disable automatic creation.

Previously, subscribing to a `$queue/...` topic created a last-value message queue, and subscribing
to a `$stream/...` topic created a last-value message stream, when the queue or stream did not exist.
Both now default to `false`, so a subscription no longer creates anything implicitly.

To keep the previous behaviour, enable automatic creation explicitly:

```
mq.auto_create.lastvalue = {}
streams.auto_create.lastvalue = {}
```

Set `regular` instead of `lastvalue` to automatically create regular queues or streams. Only one of
the two can be enabled at a time. Message queues and streams that already exist are not affected.
