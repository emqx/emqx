Changed interfaces of streams.

For subscribing to streams, `$stream` prefix is now used. Also, streams become named, and the name should be specified on subscribe: `SUBSCRIBE $stream/<name>/<topic_filter>` or (`SUBSCRIBE $stream/<name>` if the stream is known to exist). The starting point for stream consumption is specified using `stream-offset` user subscription property.

Notes:
* Stream names may contain only alphanumeric characters, underscores, hyphens, and dots.
* Previously created unnamed streams obtain the name derived from their topic filter. Their name becomes their topic filter with prepended `/`.
* `$s` prefix is still supported for subscribing to legacy streams but is considered deprecated.
