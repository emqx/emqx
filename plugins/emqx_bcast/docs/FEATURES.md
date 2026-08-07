# EMQX Bcast Plugin -- Feature Overview

The plugin provides product-level device message delivery on top of EMQX:
an HTTP API accepts a message and a target set of devices, and the plugin
delivers it directly to the connected client processes, bypassing
subscription state and ACL checks. Devices are addressed by
`ProductKey` + `DeviceName`, derived from the MQTT username
(`ProductKey-DeviceName`) via EMQX namespace attributes.

## Actions

### PubBroadcast

- Delivers one message to all online devices of a product.
- Offline devices do not receive the message; nothing is stored.
- Fanned out cluster-wide: every node delivers to its locally connected
  devices.
- Broadcast topic templates must not contain `${deviceName}`.

### BatchPub

- Delivers one message to an explicit device list (up to `max_device_count`,
  default 10,000 per call).
- Payload can be inline (`MessageContent`, Base64) or referenced by a
  pre-registered `MessageId` to avoid re-uploading large payloads.
- Delivery is asynchronous: the API resolves which devices are online,
  persists what must be persisted, and hands delivery to a pool of
  background workers. A `200` response means the request was accepted;
  if the delivery queue is full the request is rejected with
  `429 DeliveryQueueFull` and nothing is stored.

### RegisterMessage

- Pre-registers a payload and returns a `MessageId` for later BatchPub
  reuse, or refreshes the TTL of an existing message.
- SHA-256 content deduplication: identical content maps to the same
  `MessageId` and refreshes its TTL instead of creating a copy.
- Messages are immutable: there is no update operation.

## QoS Semantics (BatchPub)

| QoS | Online devices | Offline devices |
|-----|----------------|-----------------|
| 0 | Delivered asynchronously | Skipped, nothing stored |
| 1 | Delivered asynchronously, PUBACK tracked | Stored with TTL, replayed automatically on reconnect |

- QoS=1 stores one shared message record plus one delivery record per
  call. The delivery is deleted once all target devices acknowledge, or
  when it expires.
- `force_upgrade_qos = true` (default): QoS=1 is always delivered at
  QoS=1 regardless of the device subscription QoS. When false, the
  effective QoS is `min(publish, subscription)`; deliveries downgraded to
  QoS=0 are completed immediately because no PUBACK will arrive.

## Offline Replay

- Pending QoS=1 deliveries are indexed per device. The index is
  replicated to every cluster node, so a device reconnecting to any node
  replays its pending messages after subscribing.
- Message and delivery records are stored on a replicated Mnesia (mria)
  shard, so they survive the loss of the node that received the API call.

## Delivery Pipeline

1. Validate the request (sizes, device list, QoS, base64).
2. Resolve devices on every node to local connection pids (pure ETS
   reads). Devices not resolved anywhere are accounted as offline.
3. For QoS=1, write the message and delivery records in a single
   transaction (only after the queue capacity check passes).
4. Submit delivery tasks to the worker pool. Workers send directly to
   device pids, including remote pids, with no RPC in the worker path.

Backpressure is an atomic queue-depth counter: submissions beyond
`delivery_queue_max` are rejected before any storage write.

## Configuration

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_topic` | `/sys/broadcast/${productKey}` | PubBroadcast topic template |
| `batch_topic` | `/${productKey}/${deviceName}/user/get` | BatchPub topic template |
| `msg_ttl` | `15d` | Message and delivery record TTL |
| `cleanup_interval` | `60s` | Expiry scan interval |
| `max_device_count` | `10000` | Max devices per BatchPub call |
| `max_message_size_broadcast` | `65536` | Max PubBroadcast payload bytes |
| `max_message_size_batch` | `10240` | Max BatchPub payload bytes (binary) |
| `msg_warn_threshold` | `100000` | Warn when pending messages exceed this |
| `force_upgrade_qos` | `true` | See QoS Semantics above |
| `delivery_pool_size` | `0` | Async delivery workers; 0 = one per scheduler |
| `delivery_queue_max` | `10000` (tasks) | Max queued delivery tasks (each task = up to 200 devices). BatchPub with N online devices uses ceil(N/200) tasks. Rejected with 429 when full |

## Metrics

Prometheus text format at `GET /api/v5/plugin_api/emqx_bcast/metrics`
(registry prefix `bcast_`):

- Per-action API counters (`*_in`, `*_error`).
- BatchPub QoS=0: `targeted`, `delivered`, `skipped`.
- BatchPub QoS=1: `delivered_inline`, `stored_offline`, `wanted`,
  `acked`, `replayed`.
- Pool: `delivery_queue_depth` (gauge), `delivery_submit_rejected`
  (counter).

Delivery counters are updated by asynchronous workers and lag the API
response. See [API.md](API.md) for the full metric list and
[USAGE.md](USAGE.md) for end-to-end workflows.
