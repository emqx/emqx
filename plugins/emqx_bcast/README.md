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

| Metric | Description |
|--------|-------------|
| `bcast_batch_pub_qos0_in` | BatchPub QoS=0 API requests |
| `bcast_batch_pub_qos0_targeted` | QoS=0 devices targeted |
| `bcast_qos0_delivery_count` | QoS=0 one-shot deliveries to online clients |
| `bcast_batch_pub_qos1_in` | BatchPub QoS=1 API requests |
| `bcast_batch_pub_qos1_wanted` | QoS=1 total wanted acks |
| `bcast_batch_pub_qos1_delivered` | QoS=1 deliveries to clients |
| `bcast_batch_pub_qos1_acked` | QoS=1 acks received |
| `bcast_broadcast_pub_in` | PubBroadcast API requests |
| `bcast_broadcast_pub_error` | PubBroadcast errors |
| `bcast_register_message_in` | RegisterMessage API requests |
| `bcast_register_message_refresh` | RegisterMessage TTL refresh |
| `bcast_register_message_error` | RegisterMessage errors |
