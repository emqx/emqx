# EMQX Bcast Plugin

EMQX plugin providing BatchPub, PubBroadcast, and RegisterMessage HTTP APIs for product-level device message delivery.

## Features

- **PubBroadcast** -- Broadcast messages to all online devices within a Product
- **BatchPub** -- Batch delivery to a device list (≤ 10,000 devices per call), with offline storage and message reuse
- **RegisterMessage** -- Pre-register message content and TTL refresh, with SHA-256 content deduplication

## Configure

```hocon
emqx_bcast {
    broadcast_topic = "/sys/broadcast/${productKey}"
    batch_topic = "/${productKey}/${deviceName}/user/get"
    msg_ttl = 15d
    cleanup_interval = 60s
    max_device_count = 10000
    max_message_size_broadcast = 65536    # 64 KiB
    max_message_size_batch = 10240        # 10 KiB (binary)
    max_pending_deliveries = 10000000     # global cap on pending QoS=1 deliveries
    max_pending_deliveries_per_device = 100  # per-device cap (clamped 10-200)
}
```

## API

Unified endpoint: `POST /api/v5/plugin_api/emqx_bcast/pub`

Distinguished by the `Action` field in the request body:
- `"PubBroadcast"` -- broadcast to all online devices of a product
- `"BatchPub"` -- batch delivery to a specified device list
- `"RegisterMessage"` -- pre-register message content or refresh TTL

Full API documentation: [docs/API.md](docs/API.md)
Usage guide: [docs/USAGE.md](docs/USAGE.md)
Developer guide: [docs/DEVELOPMENT.md](docs/DEVELOPMENT.md)

## Namespace Integration

Client identities map to `ProductKey` and `DeviceName` through EMQX's multi-tenancy:

```hocon
mqtt.client_attrs_init = [
  { expression = "nth(1, tokens(username, '-'))", set_as_attr = tns }
]
```

Client connection example: `username = "P1-device001"`, `clientId = "device001"`
→ `namespace = "P1"` → `ProductKey = "P1"`, `DeviceName = "device001"`

## Prometheus Metrics

Plugin metrics are exposed at a dedicated endpoint:

```
GET /api/v5/plugin_api/emqx_bcast/metrics
Content-Type: text/plain; version=0.0.4
```

The endpoint carries **bcast plugin business metrics only** (EMQX's own
Prometheus endpoint already exposes node CPU/memory/connections and broker
`messages.delivered`). QoS=1 delivery counters count **logical deliveries**
(one BatchPub request x one device) and are node-local - scrape every node
and sum for cluster totals.

| Metric | Type | Description |
|--------|------|-------------|
| `bcast_batch_pub_qos1_wanted` | C | Logical deliveries durably committed to mria (ledger base, counted at promotion, not at API acceptance) |
| `bcast_batch_pub_qos1_delivered` | C | Actual PUBLISH sends (includes redeliveries and QoS0-subscription auto sends) |
| `bcast_batch_pub_qos1_redelivered` | C | Sends whose claim attempt number was >= 2 |
| `bcast_batch_pub_qos1_acked` | C | PUBACKs matched to a pending delivery |
| `bcast_batch_pub_qos1_auto_acked` | C | Deliveries completed because the subscription QoS is 0 |
| `bcast_batch_pub_qos1_ttl_expired` | C | Deliveries abandoned at TTL expiry without confirmation |
| `bcast_batch_pub_qos1_canceled` | C | Deliveries removed by management delete/reset without confirmation |
| `bcast_batch_pub_qos1_queued` / `..._inflight` | G | Live backlog gauges (queued / claimed-not-terminal; sum over nodes) |
| `bcast_intake_depth` | G | QoS=1 intake queue depth (node-local) |
| `bcast_batch_pub_qos0_in` / `..._targeted` / `bcast_qos0_delivery_count` | C | BatchPub QoS=0 requests / targeted / delivered |
| `bcast_batch_pub_qos1_{in,enqueued,intake_rejected,promote_error}` | C | QoS=1 API requests / accepted / queue-full rejects / promotion failures |
| `bcast_broadcast_pub_in` / `..._error`, `bcast_register_message_{in,refresh,error}` | C | PubBroadcast / RegisterMessage counters |

Ledger identity (eventually consistent, cluster-summed):
`wanted = acked + auto_acked + ttl_expired + canceled + queued + inflight`.
A guarded cluster-wide metric reset endpoint exists
(`POST /api/v5/plugin_api/emqx_bcast/metrics/reset`, refuses with 409 while
pending deliveries exist). See `docs/API.md` for the full contract.
