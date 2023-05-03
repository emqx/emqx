# EMQX Modules

The application provides some minor functional modules that are not included in the MQTT
protocol standard, including "Delayed Publish", "Topic Rewrite", "Topic Metrics".


## Delayed Publish

After enabling this module, messages sent by the clients with the topic prefixed with
`$delayed/{Interval}/{Topic}` will be delayed by `{Interval}` seconds before
being published to the `{Topic}`.

More introduction about [Delayed Publish](https://www.emqx.io/docs/en/v5.0/mqtt/mqtt-delayed-publish.html).

See [Enabling/Disabling Delayed Publish via HTTP API](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1delayed/put).


## Topic Rewrite

Topic Rewrite allows users to configure rules to change the topic strings that
the client requests to subscribe or publish.

This feature is very useful when there's a need to transform between different topic structures.
For example, an old device that has already been issued and cannot
be upgraded may use old topic designs, but for some reason, we adjusted the format of topics. We can use this feature to rewrite the old topics as the new format to eliminate these differences.

More introduction about [Topic Rewrite](https://www.emqx.io/docs/en/v5.0/mqtt/mqtt-topic-rewrite.html).

See [List all rewrite rules](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1topic_rewrite/get)
and [Create or Update rewrite rules](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1topic_rewrite/put).


## Topic Metrics

Topic Metrics is used for users to specify monitoring of certain topics and to
count the number of messages, QoS distribution, and rate for all messages on that topic.

More introduction about [Topic Metrics](https://www.emqx.io/docs/en/v5.0/dashboard/diagnose.html#topic-metrics).

See HTTP API docs to [List all monitored topics](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1topic_metrics/get),
[Create topic metrics](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1topic_metrics/post)
and [Get the monitored result](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1topic_metrics~1%7Btopic%7D/get).



