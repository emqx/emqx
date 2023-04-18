# EMQX Modules

The application provide some minor functional modules that are not included in the MQTT
protocol standard, including "Delayed Publish", "Topic Rewrite", "Topic Metrics" and "Telemetry".


## Delayed Publish

After enabling this module, messages sent by the user with the prefix
`$delayed/{Interval}/{Topic}` will be delayed by `{Interval}` seconds before
being published to the `{Topic}`.

More introduction about [Delayed Publish](https://www.emqx.io/docs/en/v5.0/mqtt/mqtt-delayed-publish.html).

See [Enabling/Disabling Delayed Publish via HTTP API](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/MQTT/paths/~1mqtt~1delayed/put).


## Topic Rewrite

Topic Rewrite allows users to configure rules to change the topic strings that
the client requests to subscribe or publish.

This feature is very useful when designing topics that are compatible with different
client versions. For example, an old device that has already been issued and cannot
be upgraded may use old topic rules, but the production environment need to apply
a new design rules for the topics.

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


## Telemetry

Telemetry is used for collecting non-sensitive information about the EMQX cluster.

More introduction about [Telemetry](https://www.emqx.io/docs/en/v5.0/telemetry/telemetry.html#telemetry).

See HTTP API docs to [Enable/Disable telemetry](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1status/put),
[Get the enabled status](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1status/get)
and [Get the data of the module collected](https://www.emqx.io/docs/en/v5.0/admin/api-docs.html#tag/Telemetry/paths/~1telemetry~1data/get).
