Changed interfaces of queues and shared subscriptions.

For subscribing to message queues, `$queue` prefix is now used. Also, message queues become named, and the name should be specified on subscribe: `SUBSCRIBE $queue/<name>/<topic_filter>` or (`SUBSCRIBE $queue/<name>` if the queue is known to exist).

Notes:
* `$queue` prefix cannot be used for subscribing to shared subscriptions anymore.
* Queue names may contain only alphanumeric characters, underscores, hyphens, and dots.
* Previously created unnamed queues obtain the name derived from their topic filter. Their name becomes their topic filter with prepended `/`.
* `$q` prefix is still supported for subscribing to legacy queues but is considered deprecated.
