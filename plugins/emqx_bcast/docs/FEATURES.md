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
- Delivery is asynchronous: all API writes are funnelled to a core node,
  core persists QoS=1 records and broadcasts a trigger (or full QoS=0 data),
  and the node serving each device pulls the delivery through the
  `pull_pool` / `pull_server_pool` pipeline. A `200` response means the
  request was accepted; online delivery completes asynchronously.

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
  message at QoS=0 and the delivery completes immediately (no PUBACK
  exists for QoS=0), devices subscribed at QoS=1 receive it at QoS=1 and
  the delivery is removed once the PUBACK arrives.

## Offline Replay

- Pending QoS=1 deliveries are indexed per device in core Mnesia.
  Replicant nodes keep no delivery storage; a device reconnecting to any
  node pulls its pending messages from a core node after subscribing.
- Core nodes keep the authoritative Mnesia copies, so records survive
  the loss of the node that received the API call.

## Delivery Pipeline

1. Validate the request (sizes, device list, QoS, base64).
2. All API requests are funnelled to a core node (F8).
3. QoS=0 / PubBroadcast: core broadcasts full deliver data to every
   node; each `pull_pool` checks online + subscription and delivers.
4. QoS=1: core writes message and delivery records in a single
   transaction, then broadcasts a pure trigger. Each `pull_pool`
   deduplicates triggers into `buffer3`, pulls from a random core,
   fills the active A/B buffer, delivers, and tracks pending acks.

There is no delivery-queue admission control: workers are pooled and
pull-side buffering provides backpressure through the 50ms/100 batch
flush policy.

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
| `delivery_pool_size` | `0` | Async workers for each of the three pools; 0 = one per scheduler |

## Metrics

Prometheus text format at `GET /api/v5/plugin_api/emqx_bcast/metrics`
(registry prefix `bcast_`):

- Per-action API counters (`batch_pub_*_in`, `broadcast_pub_in`,
  `register_message_*`).
- BatchPub QoS=0: `targeted`, `qos0_delivery_count`.
- BatchPub QoS=1: `wanted`, `delivered`, `acked`.

Delivery counters are updated by asynchronous workers and lag the API
response. `delivered` and `acked` are node-local (they count on the node
that delivers or receives the PUBACK), so aggregate them across all nodes.
See [API.md](API.md) for the full metric list and
[USAGE.md](USAGE.md) for end-to-end workflows.
