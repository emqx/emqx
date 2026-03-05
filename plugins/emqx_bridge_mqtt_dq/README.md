# MQTT Bridge with Disk Queue

Use this plugin to forward local MQTT messages to another MQTT broker with a
disk buffer for better resilience.

## Features

- Disk buffering per bridge.
- Automatic retry when the remote broker is unavailable.
- Topic rewrite support with `${topic}`.
- Multiple bridges in one plugin.
- Config updates apply per bridge (unchanged bridges keep running).

## How It Works

1. Match local publishes with each bridge `filter_topic`.
2. Append matching messages to disk queue partitions.
3. Publish queued messages to the remote broker.
4. Retry automatically if publish fails due to network/connectivity.
5. If a queue partition exceeds `queue.max_total_bytes`, oldest records in that partition are dropped.

## Configuration

Configure from EMQX Dashboard (recommended) or via plugin config file.

For production, start with one bridge, validate traffic, then scale out.

### Quick Start (Dashboard)

1. Enable the plugin.
2. Add one bridge under `bridges`.
3. Set `server`, `filter_topic`, and `remote_topic`.
4. Save and verify remote delivery.
5. Tune queue and pool settings only after baseline validation.

### Example

```
bridges {
  to-cloud {
    enable = true
    server = "cloud-broker.example.com:8883"
    proto_ver = "v4"
    clientid_prefix = "emqx_dq_"
    username = "bridge_user"
    password = "secret"
    clean_start = true
    keepalive_s = 60
    ssl {
      enable = true
    }
    pool_size = 4
    filter_topic = "devices/#"
    remote_topic = "fwd/${topic}"
    remote_qos = 1
    remote_retain = false
    queue {
      dir = "data/bridge_mqtt_dq/to-cloud"
      seg_bytes = "100MB"
      max_total_bytes = "1GB"
    }
  }
}
```

### Configuration Reference

#### Top Level

| Field     | Type    | Default | Description                          |
|-----------|---------|---------|--------------------------------------|
| `bridges` | map     | `{}`    | Map of bridge name to bridge configuration. |

#### Bridge (`bridges.<name>`)

| Field             | Type    | Default | Description                                                                 |
|-------------------|---------|---------|-----------------------------------------------------------------------------|
| `enable`          | boolean | `true`  | Enable or disable this bridge.                                              |
| `server`          | string  | —       | Remote MQTT broker address (`host:port`).                                   |
| `proto_ver`       | string  | `"v4"`  | MQTT protocol version: `v3`, `v4`, or `v5`.                                |
| `clientid_prefix` | string  | —       | Prefix for auto-generated MQTT client IDs.                                  |
| `username`        | string  | `""`    | Username for authentication with the remote broker.                         |
| `password`        | string  | `""`    | Password for authentication with the remote broker.                         |
| `clean_start`     | boolean | `true`  | MQTT clean start flag.                                                      |
| `keepalive_s`     | integer | `60`    | MQTT keep-alive interval in seconds.                                        |
| `ssl.enable`      | boolean | `false` | Enable SSL/TLS for the connection to the remote broker.                     |
| `ssl.sni`         | string  | server hostname | TLS Server Name Indication. Defaults to the server hostname. Set to `"disable"` to turn off SNI. |
| `pool_size`       | integer | `4`     | Number of MQTT connections to the remote broker.                            |
| `buffer_pool_size` | integer | `4`    | Number of disk queue buffer workers per bridge. See warnings below.         |
| `filter_topic`    | string  | —       | Local topic filter pattern. Supports `+` and `#` wildcards.                |
| `remote_topic`    | string  | —       | Target topic template. Use `${topic}` for the original topic.              |
| `enqueue_timeout_ms` | integer | `5000` | Max time (ms) to block waiting for disk queue confirmation. Only applies to QoS > 0; QoS 0 is always async. |
| `remote_qos`      | integer | `1`     | QoS level for publishing to the remote broker (0, 1, or 2).                |
| `remote_retain`   | boolean | `false` | Retain flag for publishing to the remote broker.                            |

#### Queue

| Field             | Type   | Default                        | Description                                      |
|-------------------|--------|--------------------------------|--------------------------------------------------|
| `queue.dir`       | string | `"data/bridge_mqtt_dq/<name>"` | Directory for disk queue segment files.           |
| `queue.seg_bytes` | string | `"100MB"`                      | Maximum size per queue segment file.              |
| `queue.max_total_bytes` | string | `"1GB"`                  | Maximum disk queue size **per partition**. Each bridge uses `buffer_pool_size` partitions (default 4), so the worst-case total disk usage is `buffer_pool_size` x this value. Oldest messages are discarded when exceeded. |

## Topic Templating

The `remote_topic` field supports the `${topic}` placeholder, which is replaced
with the original publish topic at forwarding time.

Examples:
- `remote_topic = "${topic}"` — forward with the original topic unchanged.
- `remote_topic = "forwarded/${topic}"` — prepend a prefix.
- `remote_topic = "region1/${topic}"` — add a region namespace.

`remote_topic` is applied when messages are sent out of the queue. After changing
this field, queued messages use the new template after the affected bridge restarts.

## Configuration Change Behavior

Configuration updates are applied per bridge:
- Changed bridges restart.
- Removed or disabled bridges stop.
- New bridges start.
- Unchanged bridges continue running.

The full plugin is not restarted for every config update.
However, each restarted bridge has a short handover window where matching
messages can be dropped. Apply bridge-impacting changes during low traffic.

### Before You Change Config

1. Identify which bridges are impacted.
2. Apply during a low-traffic window.
3. Monitor Dashboard status and logs for restart/reconnect errors.
4. For critical pipelines, validate end-to-end delivery after the change.

### Changing `buffer_pool_size`

The `buffer_pool_size` controls how many disk queue partitions exist per bridge.
Messages are assigned to partitions by `erlang:phash2(Topic, buffer_pool_size)`.
Changing this value has important side effects:

1. **Shrinking the pool** (e.g. 8 -> 4): partitions with indices >= new size
   are no longer consumed. Their old files remain under `queue.dir` and need
   manual cleanup.

2. **Growing the pool** (e.g. 4 -> 8): the hash space changes, so topics that
   previously mapped to partition N may now map to partition M. Messages already
   queued in the old partition are still delivered (in order, within that
   partition), but new messages for the same topic may go to a different
   partition. This breaks end-to-end per-topic ordering across the
   transition — some old messages may be delivered after new ones.

3. **Bridge-scoped drop window**: changing `buffer_pool_size` restarts that bridge,
   so in-flight matching messages can be dropped during handover.

## Operational Notes

### Persistence

Buffered messages survive:
- EMQX node restarts.
- Plugin reloads and upgrades.
- Temporary network outages to the remote broker.

### Queue Limit

When queue usage exceeds `queue.max_total_bytes` for a partition, oldest messages
in that partition are discarded to make room for new data. Warning logs are emitted.

### Pool Sizing

Each buffer worker is assigned to exactly one connector by
`BufferIndex rem pool_size`. For even load distribution:

- `buffer_pool_size` should be **greater than or equal to** `pool_size`.
- `buffer_pool_size` should be a **multiple of** `pool_size`
  (i.e. `buffer_pool_size mod pool_size = 0`).

Good examples: `pool_size = 4, buffer_pool_size = 4` (1:1),
`pool_size = 4, buffer_pool_size = 8` (2:1).

Bad example: `pool_size = 4, buffer_pool_size = 5` — connector 0 serves two
buffers while others serve one, causing uneven throughput.

If a connector drops, the buffer workers assigned to it pause and resume
automatically once the connector reconnects.

### Ordering

Per-topic ordering is preserved under stable bridge settings. If you change
`buffer_pool_size`, ordering may be temporarily affected as described above.

### Publisher ACK Behavior (QoS 1/2)

For messages matching a bridge:
- `PUBACK` (QoS 1) and `PUBREC` (QoS 2) to publishing clients may be delayed while
  EMQX waits for disk-queue enqueue confirmation (`enqueue_timeout_ms`).
- If that enqueue wait times out, EMQX still completes the client publish flow.
  The client does not receive a publish error because of disk-queue enqueue timeout.
