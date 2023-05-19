# EMQX Slow Subscriptions

This application can calculate the latency (time spent) of the message to be processed and transmitted since it arrives at EMQX.

If the latency exceeds a specified threshold, this application will add the subscriber and topic information to a slow subscriptions list or update the existing record.

More introduction: [Slow Subscriptions](https://www.emqx.io/docs/en/v5.0/observability/slow-subscribers-statistics.html)

# Usage

You can add the below section into `emqx.conf` to enable this application

```yaml
slow_subs {
  enable = true
  threshold = "500ms"
  expire_interval = "300s"
  top_k_num = 10
  stats_type = whole
}
```

# Configurations

**threshold**: Latency threshold for statistics, only messages with latency exceeding this value will be collected.

Minimum value: 100ms  
Default value: 500ms

**expire_interval**: Eviction time of the record, will start the counting since the creation of the record, and the records that are not triggered again within this specified period will be removed from the rank list.

Default: 300s

**top_k_num**: Maximum number of records in the slow subscription statistics record table.

Maximum value: 1,000  
Default value: 10

**stats_type**: Calculation methods of the latency, which are
- **whole**: From the time the message arrives at EMQX until the message transmission completes
- **internal**: From when the message arrives at EMQX until when EMQX starts delivering the message
- **response**: From the time EMQX starts delivering the message until the message transmission completes

Default value: whole

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
