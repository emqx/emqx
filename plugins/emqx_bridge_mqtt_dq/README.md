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

### Config File Locations

There are two relevant config file locations:

- Bundled default file inside the installed plugin package:
  - docker install example for `0.2.0`:
    `/opt/emqx/plugins/emqx_bridge_mqtt_dq-0.2.0/emqx_bridge_mqtt_dq-0.2.0/priv/config.hocon`
  - deb/rpm install example for `0.2.0`:
    `/usr/lib/emqx/plugins/emqx_bridge_mqtt_dq-0.2.0/emqx_bridge_mqtt_dq-0.2.0/priv/config.hocon`

- Persisted plugin config file managed by EMQX after config is saved via Dashboard or API:
  - docker:
    `/opt/emqx/data/plugins/emqx_bridge_mqtt_dq/config.hocon`
  - deb/rpm:
    `/var/lib/emqx/plugins/emqx_bridge_mqtt_dq/config.hocon`

The `priv/config.hocon` file is the packaged default template. The
`data/plugins/.../config.hocon` file is the persisted plugin config location
used after EMQX saves plugin config changes.

### Quick Start (Dashboard)

1. Enable the plugin.
2. Add one reusable remote under `remotes`.
3. Add one bridge under `bridges`.
4. Set `remote`, `filter_topic`, and `remote_topic`.
5. Save and verify remote delivery.
6. Tune queue and pool settings only after baseline validation.

### Example

```
bridges {
  to-cloud {
    enable = true
    remote = cloud
    proto_ver = "v4"
    keepalive_s = 60
    pool_size = 4
    filter_topic = "devices/#"
    remote_topic = "fwd/${topic}"
    remote_qos = "${qos}"
    remote_retain = "${retain}"
    queue {
      seg_bytes = "100MB"
      max_total_bytes = "1GB"
    }
  }
}

remotes {
  cloud {
    server = "cloud-broker.example.com:8883"
    username = "bridge_user"
    password = "secret"
    ssl {
      enable = true
      verify = verify_none
      # cacertfile = "/path/to/ca.pem"
      # certfile = "/path/to/client-cert.pem"
      # keyfile = "/path/to/client-key.pem"
    }
  }
}
```

### Environment Variable Substitution

Any string value in the config file can reference an OS environment variable
using the `${EMQXDQ_*}` syntax. Only variables with the `EMQXDQ_` prefix are
resolved — other `${...}` patterns (such as `${topic}` in `remote_topic`) are
left untouched. The entire value must be the placeholder; partial interpolation
(e.g. `"prefix-${EMQXDQ_VAR}-suffix"`) is not supported.

**Limitation:** `${EMQXDQ_*}` substitution only works for config fields that
accept string values (e.g. `server`, `username`, `password`). It cannot be used
for boolean fields (`enable`), integer fields (`pool_size`, `keepalive_s`).

Example:

```
remotes {
  cloud {
    server = "${EMQXDQ_REMOTE_SERVER}"
    username = "${EMQXDQ_REMOTE_USER}"
    password = "${EMQXDQ_REMOTE_PASSWORD}"
  }
}
```

If the environment variable is not set, the plugin logs an error and keeps the
original `${EMQXDQ_...}` string as the literal value. This typically causes a
connection failure (e.g. trying to connect to `"${EMQXDQ_REMOTE_SERVER}"`),
which makes the misconfiguration visible in both logs and the status API.

> **Warning — dynamic config updates and node-local environment variables**
>
> Environment variables are resolved at config parse time on the node that
> parses the config. When you update the plugin config via the EMQX Dashboard,
> REST API, or CLI, the raw config text is persisted and then re-parsed on
> every node in the cluster. If different nodes have different values (or
> missing values) for the referenced environment variables, each node will
> resolve to a different effective config.
>
> Because of this, **avoid using `${EMQXDQ_...}` substitution with Dashboard,
> API, or CLI config updates** unless you are certain that every node in the
> cluster has the same environment variables set. For node-local secrets,
> prefer editing the config file directly and reloading the plugin, or use a
> consistent secret-injection mechanism (e.g. Kubernetes ConfigMaps/Secrets
> mounted identically on all nodes).

### Configuration Reference

#### Top Level

| Field     | Type    | Default | Description                          |
|-----------|---------|---------|--------------------------------------|
| `bridges` | map     | `{}`    | Map of bridge name to bridge configuration. |
| `remotes` | map     | `{}`    | Map of reusable remote broker definitions. |

#### Bridge (`bridges.<name>`)

| Field             | Type    | Default | Description                                                                 |
|-------------------|---------|---------|-----------------------------------------------------------------------------|
| `enable`          | boolean | `true`  | Enable or disable this bridge.                                              |
| `remote`          | string  | —       | Name of the remote broker definition under `remotes`.                       |
| `proto_ver`       | string  | `"v4"`  | MQTT protocol version: `v3`, `v4`, or `v5`.                                |
| `clientid_prefix` | string  | `"emqx-dq-<name>-"` | Prefix for auto-generated MQTT client IDs. Each connection appends a unique index (e.g. `emqx-dq-mybridge-0`). Optional — leave empty to use the default. |
| `keepalive_s`     | integer | `60`    | MQTT keep-alive interval in seconds.                                        |
| `pool_size`       | integer | `4`     | Number of MQTT connections to the remote broker.                            |
| `buffer_pool_size` | integer | `4`    | Number of disk queue buffer workers per bridge. See warnings below.         |
| `filter_topic`    | string  | —       | Local topic filter pattern. Supports `+` and `#` wildcards.                |
| `remote_topic`    | string  | —       | Target topic template. Use `${topic}` for the original topic.              |
| `enqueue_timeout_ms` | integer | `5000` | Max time (ms) to block waiting for disk queue confirmation. Only applies to QoS > 0; QoS 0 is always async. |
| `max_inflight`    | integer | `32`    | Maximum unacknowledged messages per connection to the remote broker. Controls batch pop size from disk queue and emqtt send window. |
| `remote_qos`      | string | `"${qos}"` | QoS level for publishing to the remote broker (`"0"`, `"1"`, `"2"`). The default `"${qos}"` preserves the original message's QoS. |
| `remote_retain`   | string | `"${retain}"` | Retain flag for publishing to the remote broker (`"true"`, `"false"`). The default `"${retain}"` preserves the original message's retain flag. |
| `max_publish_retries` | integer | `-1` | Number of publish retry attempts per message before dropping it. `-1` means infinite retries. Each failed PUBACK or connection loss consumes one credit. |

#### Remote (`remotes.<name>`)

| Field        | Type    | Default | Description                                             |
|--------------|---------|---------|---------------------------------------------------------|
| `server`     | string  | —       | Remote MQTT broker address (`host:port`).               |
| `username`   | string  | `""`    | Username for authentication with the remote broker.     |
| `password`   | string  | `""`    | Password for authentication with the remote broker.     |
| `ssl.enable` | boolean | `false` | Enable SSL/TLS for the connection to the remote broker. |
| `ssl.verify` | string | `verify_none` | TLS verification mode. Supported values: `verify_none`, `verify_peer`. |
| `ssl.sni`    | string  | server hostname | TLS Server Name Indication. Defaults to the server hostname. Set to `"disable"` to turn off SNI. |
| `ssl.cacertfile` | string | —    | CA certificate file used to verify the remote broker certificate. |
| `ssl.certfile` | string | —      | Client certificate file for mutual TLS authentication.  |
| `ssl.keyfile` | string | —       | Client private key file for mutual TLS authentication.  |

#### Queue

| Field             | Type   | Default                        | Description                                      |
|-------------------|--------|--------------------------------|--------------------------------------------------|
| `queue.base_dir`  | string | `"emqx_bridge_mqtt_dq"` | Base directory for disk queue segment files. The bridge name and partition index are automatically appended (i.e. `<base_dir>/<bridge_name>/<index>`). Relative paths are resolved against EMQX `data_dir`. Absolute paths are used as-is. |
| `queue_seg_bytes` | string | `"100MB"`                      | Maximum size per queue segment file.              |
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

## REST API

The plugin exposes four endpoints under the EMQX plugin API base path:

- `GET /api/v5/plugin_api/emqx_bridge_mqtt_dq/metrics` — Prometheus text format
- `GET /api/v5/plugin_api/emqx_bridge_mqtt_dq/stats` — JSON dashboard snapshot
- `GET /api/v5/plugin_api/emqx_bridge_mqtt_dq/stats/<bridge>` — one bridge only
- `GET /api/v5/plugin_api/emqx_bridge_mqtt_dq/status` — plugin/cluster health summary

All JSON endpoints return `application/json; charset=utf-8`.

The JSON APIs are cluster-aggregated. If a node is unavailable or times out
during aggregation, the API still returns best-effort data, but the response
contains explicit cluster completeness metadata.

Example:

```bash
curl -u admin:public \
  http://127.0.0.1:18083/api/v5/plugin_api/emqx_bridge_mqtt_dq/metrics
```

```bash
curl -u admin:public \
  http://127.0.0.1:18083/api/v5/plugin_api/emqx_bridge_mqtt_dq/stats
```

### `/stats` Response Shape

The `/stats` response body contains:

- `cluster`: cluster completeness and failed-node information
- `uptime_seconds`: maximum plugin uptime observed across responding nodes
- `summary`: totals across all configured bridges
- `bridges`: one entry per configured bridge

Example:

```json
{
  "cluster": {
    "complete": true,
    "responded_nodes": ["emqx@127.0.0.1"],
    "failed_nodes": [],
    "timeout_ms": 5000
  },
  "uptime_seconds": 123,
  "summary": {
    "bridge_count": 1,
    "running_bridge_count": 1,
    "buffered": 12,
    "backlog": 3,
    "inflight": 8,
    "enqueue": 1000,
    "dequeue": 995,
    "publish": 990,
    "drop": 5
  },
  "bridges": [
    {
      "name": "to-cloud",
      "config_state": "enabled",
      "runtime_state": "running",
      "status": "ok",
      "status_reason": null,
      "enqueue": 1000,
      "dequeue": 995,
      "publish": 990,
      "drop": 5,
      "retried_by_reason": {
        "connect_failed": 2,
        "reason_code": 3
      },
      "buffered": 12,
      "backlog": 3,
      "inflight": 8,
      "buffers": [
        {
          "bridge": "to-cloud",
          "index": 0,
          "status": "running",
          "buffered": 12
        }
      ],
      "connectors": [
        {
          "bridge": "to-cloud",
          "index": 0,
          "status": "connected",
          "backlog": 3,
          "inflight": 8
        }
      ]
    }
  ]
}
```

`GET /stats/<bridge>` returns:

```json
{
  "cluster": {
    "complete": true,
    "responded_nodes": ["emqx@127.0.0.1"],
    "failed_nodes": [],
    "timeout_ms": 5000
  },
  "bridge": {
    "name": "to-cloud",
    "config_state": "enabled",
    "runtime_state": "running",
    "status": "ok"
  }
}
```

If the bridge does not exist in current config, the API returns `404`.

`GET /status` returns a compact health view:

```json
{
  "plugin": "emqx_bridge_mqtt_dq",
  "cluster": {
    "complete": true,
    "responded_nodes": ["emqx@127.0.0.1"],
    "failed_nodes": [],
    "timeout_ms": 5000
  },
  "status": "ok",
  "bridge_count": 1
}
```

The `/metrics` endpoint returns Prometheus text exposition with cluster-aggregated
series such as:

- `emqx_bridge_mqtt_dq_uptime_seconds`
- `emqx_bridge_mqtt_dq_bridge_enqueue_total{bridge="..."}`
- `emqx_bridge_mqtt_dq_bridge_dequeue_total{bridge="..."}`
- `emqx_bridge_mqtt_dq_bridge_publish_total{bridge="..."}`
- `emqx_bridge_mqtt_dq_bridge_drop_total{bridge="..."}`
- `emqx_bridge_mqtt_dq_bridge_status{bridge="...",status="..."}`
- `emqx_bridge_mqtt_dq_bridge_retry_reason_total{bridge="...",reason="..."}`
- `emqx_bridge_mqtt_dq_buffer_buffered{bridge="...",index="..."}`
- `emqx_bridge_mqtt_dq_connector_backlog{bridge="...",index="..."}`
- `emqx_bridge_mqtt_dq_connector_inflight{bridge="...",index="..."}`

### Metric Semantics

#### Bridge Metrics

- `enqueue`: number of local messages accepted into the bridge enqueue path
- `dequeue`: number of messages durably removed from the local queue
- `publish`: number of messages successfully published to the remote broker
- `drop`: number of queued messages finalized as dropped
- `retried_by_reason`: retry attempts broken down by reason
- `config_state`: desired bridge state from config (`enabled` or `disabled`)
- `runtime_state`: observed worker/storage state (`running`, `degraded`, or `purged`)
- `status`: operator-facing bridge health (`ok`, `partial`, `disconnected`, `disabled`, `error`)

Current retry reasons include:

- `reason_code`: remote broker returned a non-success MQTT reason code and the message was retried
- `connect_failed`: connect or publish failure triggered a retry
- `timeout`: timeout-specific retry classification
- `connection_lost`: linked client process exited and inflight messages were salvaged for retry
- `other`: fallback bucket for unclassified retry causes

After the bridge drains fully, the counters satisfy:

- `enqueue = dequeue = publish + drop`

#### Buffer Metrics

- `buffered`: current number of messages stored in that durable queue partition
- buffer row `status`: `running` when the worker is present, `missing` otherwise

This gauge is refreshed immediately after `replayq:open/1`, so persisted on-disk
messages are visible even before new traffic arrives.

#### Connector Metrics

- `backlog`: number of messages sitting in the connector backlog queue waiting to be dispatched to `emqtt`
- `inflight`: number of messages already handed to `emqtt` and still awaiting completion
- connector row `status`: `connected`, `disconnected`, `partial`, `missing`, or `unknown`

## Configuration Change Behavior

Configuration updates are applied per bridge:
- Changed bridges restart.
- Removed bridges stop.
- Disabled bridges stop and purge their queue directory.
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

### Changing `queue.base_dir`

Changing `queue.base_dir` on an enabled bridge restarts the bridge with the new
directory. The actual queue path is `<base_dir>/<bridge_name>/<index>`. The old
directory is **not** automatically purged — it remains on disk as orphaned data.
If the old directory is no longer needed, remove it manually after verifying the
bridge is running on the new path.

### Changing `buffer_pool_size`

The `buffer_pool_size` controls how many disk queue partitions exist per bridge.
Messages are assigned to partitions by `erlang:phash2(Topic, buffer_pool_size)`.
Changing this value has important side effects:

1. **Shrinking the pool** (e.g. 8 -> 4): partitions with indices >= new size
   are no longer consumed. Their old files remain under `queue.base_dir` and
   need manual cleanup.

2. **Growing the pool** (e.g. 4 -> 8): the hash space changes, so topics that
   previously mapped to partition N may now map to partition M. Messages already
   queued in the old partition are still delivered (in order, within that
   partition), but new messages for the same topic may go to a different
   partition. This breaks end-to-end per-topic ordering across the
   transition — some old messages may be delivered after new ones.

3. **Bridge-scoped drop window**: changing `buffer_pool_size` restarts that bridge,
   so in-flight matching messages can be dropped during handover.

## Message Delivery Guarantees

This plugin provides **at-least-once** delivery under normal operation, and
**best-effort** delivery under sustained failure. Messages can be lost in the
following scenarios:

### Disk Queue Overflow

When a queue partition exceeds `queue.max_total_bytes`, the oldest messages in
that partition are silently discarded to make room for new data. A warning log
(`mqtt_dq_buffer_overflow`) is emitted periodically (not per message).

**Mitigation**: increase `queue.max_total_bytes`, increase `buffer_pool_size`
to spread load across more partitions, or reduce message throughput.

### Remote Broker Rejects a Publish

When the remote broker returns a non-success MQTT reason code in PUBACK (QoS 1)
or PUBREC (QoS 2), the connector retries the message up to 3 times. If all
retries are exhausted, the message is dropped and a warning log
(`mqtt_dq_publish_dropped`) is emitted.

Common rejection reason codes include:

| Code | Meaning (MQTT 5.0)              |
|------|---------------------------------|
| 16   | No matching subscribers         |
| 128  | Unspecified error                |
| 131  | Implementation specific error    |
| 135  | Not authorized                  |
| 144  | Topic Name invalid              |
| 145  | Packet identifier in use        |
| 151  | Quota exceeded                  |

Note: reason code 0 (Success) and 16 (No matching subscribers) are treated as
successful delivery and do not trigger retries.

**Mitigation**: check remote broker ACLs and topic policies. Review logs for
the specific reason code.

### Repeated Connection Failures

Each time the connection to the remote broker drops, all pending (not yet
acknowledged) messages lose one retry attempt. After 3 cumulative connection
failures without a successful delivery, the message is dropped.

For example, a message published during a network outage:
1. Queued locally (retry counter = 3).
2. Remote reconnects, message dispatched — remote disconnects again before ACK
   (retry counter = 2).
3. Reconnects, dispatched again — connection drops (retry counter = 1).
4. Reconnects, dispatched — rejected or connection drops (retry counter = 0).
5. Message dropped, warning logged.

**Mitigation**: investigate why the remote broker is repeatedly unreachable.
Transient network blips are handled transparently; this scenario requires
sustained instability.

### Enqueue Backpressure (QoS > 0 Local Publishes)

When a QoS 1 or 2 client publishes a message matching a bridge, the plugin
sends the message to the buffer worker's mailbox and then blocks the publishing
session process for up to `enqueue_timeout_ms` (default 5000 ms) waiting for
disk-write confirmation.

The message itself is **not lost** when this timeout fires — it is already in the
buffer worker's Erlang mailbox and will eventually be written to the disk queue.
The timeout only controls how long the local publish path blocks.

Why this matters: the `message.publish` hook runs inside the MQTT session
process. While the hook is blocking, the session cannot process other messages
from that client. If the buffer worker is slow (e.g., disk I/O stall or
high mailbox backlog), the timeout prevents one slow bridge from stalling the
client session indefinitely.

When the timeout fires:
1. The session process stops waiting and continues normally.
2. The client receives PUBACK/PUBREC as usual — no error is surfaced.
3. A warning log (`mqtt_dq_enqueue_timeout`) is emitted.
4. The message remains in the buffer worker's mailbox and is written to the disk
   queue when the worker catches up.

The risk is indirect: if buffer workers fall persistently behind, their mailbox
grows without bound, increasing memory usage. This is a sign that the bridge
cannot keep up with the incoming message rate.

**Mitigation**: increase `buffer_pool_size` to spread load, use faster storage
for `queue.base_dir`, or reduce the message rate for matched topics.

Note: QoS 0 local publishes never block — they are enqueued asynchronously with
no backpressure applied to the publishing session.

### Bridge Restart Window

When a bridge restarts (due to config change, plugin reload, or enable/disable
toggle), there is a brief window where matching messages may not be captured.

**Mitigation**: apply config changes during low-traffic periods.

### QoS 0 TCP-Level Delivery

For QoS 0 publishes to the remote broker, the connector considers delivery
successful once the message reaches the local TCP send buffer. If the remote
broker crashes after the TCP stack accepts the data but before the broker
processes it, the message may be lost without any error reported back to the
connector.

This is inherent to MQTT QoS 0 and not specific to this plugin.

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
