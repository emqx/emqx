# EMQX Bcast Plugin -- Feature Overview

The plugin provides product-level device message delivery on top of EMQX:
an HTTP API accepts a message and a target set of devices, and the plugin
delivers it directly to the connected client processes via internal
channels, bypassing ACL checks. Delivery is gated by the device's MQTT
subscription: only devices subscribed to the delivery topic receive the
message. Each client's subscription filters are cached in its per-client
pull-state row (maintained from the session subscribe/unsubscribe hooks,
re-read from EMQX's own subscription tables `emqx_broker:subscriptions/1`
on session resume, and used as a fallback when the cache is empty); the
plugin keeps no global subscription mirror. Devices are addressed by
`ProductKey` + `DeviceName`, derived from the MQTT username
(`ProductKey-DeviceName`) via EMQX namespace attributes.

Listener mountpoints and `namespace_as_mountpoint` are not supported:
with either configured, the topic filters in EMQX's subscription tables
are prefixed by the mountpoint, which does not match the plugin's
unmounted delivery topics.

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
- Delivery is asynchronous: all API writes are funnelled to a core node.
  QoS=1 is accepted into a bounded node-local intake queue, so a `200`
  means in-memory acceptance, not durability; the promoter then commits
  the message and delivery rows to mria, appends the per-device index and
  broadcasts a per-node trigger, and the node serving each device claims
  and delivers the entry through its pull shards. QoS=0/PubBroadcast data
  is broadcast in full to every node's pull shards. Online delivery
  completes asynchronously.

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
- Delivery QoS follows the device subscription: the effective QoS is
  `min(publish, subscription)`. Devices subscribed at QoS=0 receive the
  message at QoS=0 and the delivery is considered complete when it is
  handed to the channel process; this path is at-most-once with respect
  to the client actually observing the publish. Devices subscribed at
  QoS=1 receive it at QoS=1 and the delivery is removed once the PUBACK
  arrives.
- BatchPub QoS=1 is at-least-once: a lost ack or a reconnect/takeover race
  can redeliver a message, so clients must tolerate duplicates. QoS=0 can
  also duplicate during the session-takeover window, although delivery is
  now gated on the current session holder at send time.

## Offline Replay

- Pending QoS=1 deliveries are indexed per device in core Mnesia.
  Replicant nodes keep no delivery storage; a device reconnecting to any
  node pulls its pending messages from a core node after subscribing.
- Core nodes keep the authoritative Mnesia copies, so records survive
  the loss of the node that received the API call.

## Delivery Pipeline

1. Validate the request (sizes, device list, QoS, base64, topic template).
2. All API requests are funnelled to a core node.
3. QoS=0 / PubBroadcast: the core broadcasts the full delivery data to
   every node; each node's pull shards check online + subscription and
   deliver directly.
4. QoS=1: the request is accepted into the bounded node-local intake
   queue; the promoter drains it, commits the message and delivery rows in
   a single mria transaction (the durability point), appends the
   per-device index to the owning index shard, then broadcasts a pure
   trigger per (product, template). Each node's pull shards claim their
   devices' queue entries, deliver to the local channel and track pending
   acks.

The delivery workers themselves have no queue admission control: claim
and ack work is pooled (pool size = `delivery_pool_size`, 0 = one worker
per scheduler) and the pull shards bound each flush batch (2 ms / 500
entries for claims). The API layer separately enforces the intake queue
bound (429 `Busy`) and the pending-delivery quotas (429 `QuotaExceeded`).

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
| `max_pending_deliveries` | `10000000` | Global cap on pending QoS=1 deliveries; QoS=1 BatchPub requests that would exceed it are rejected with 429 QuotaExceeded |
| `max_pending_deliveries_per_device` | `100` | Per-device cap on pending QoS=1 deliveries (clamped 10-200); requests targeting a device over the cap are rejected with 429 QuotaExceeded and the over-limit device list |
| `delivery_pool_size` | `0` | Worker count for each delivery pool (the per-node claim pool and the core-side server pool); 0 = one per scheduler |

## Metrics

Prometheus text format at `GET /api/v5/plugin_api/emqx_bcast/metrics`
(registry prefix `bcast_`). The surface is bcast business metrics only;
EMQX's own endpoint provides system-level values (CPU/memory/connections,
broker `messages.delivered`).

- BatchPub QoS=0: `targeted`, `qos0_delivery_count`.
- BatchPub QoS=1 delivery ledger: `wanted` (durable commit anchor),
  `delivered` (actual sends incl. redeliveries), `redelivered`
  (attempt >= 2), `acked`, `auto_acked`, `ttl_expired`, `canceled`.
- BatchPub QoS=1 gauges: `intake_depth`, `queued`, `inflight`
  (live backlog).
- Request-level counters: `batch_pub_qos1_{in,enqueued,intake_rejected,promote_error}`,
  `broadcast_pub_*`, `register_message_*`.

Ledger identity (eventually consistent, cluster-summed):
`wanted = acked + auto_acked + ttl_expired + canceled + queued + inflight`.
Delivery counters are updated by asynchronous workers and lag the API
response. All counters are node-local - aggregate across all nodes. A
guarded cluster-wide metric reset endpoint exists (see [API.md](API.md)).
See [API.md](API.md) for the full metric list and [USAGE.md](USAGE.md) for
end-to-end workflows.
