# EMQX IoT Plugin

EMQX 6.1 plugin providing BatchPub, PubBroadcast, and RegisterMessage HTTP APIs for IoT device message delivery.

## Features

- **PubBroadcast** -- Broadcast messages to all online devices within a Product
- **BatchPub** -- Batch delivery to a device list (≤ 10,000 devices per call), with offline storage and message reuse
- **RegisterMessage** -- Pre-register message content and TTL refresh, with SHA-256 content deduplication

## Build

```bash
cd plugins/emqx_iot
MIX_ENV=emqx-enterprise mix do deps.get, emqx.plugin
```

The plugin package is generated under `_build/emqx_enterprise/...`.

## Install

```bash
emqx ctl plugins install emqx_iot-<vsn>.tar.gz
emqx ctl plugins start emqx_iot
```

## Configure

```hocon
emqx_iot {
    broadcast_topic = "/sys/broadcast/${productKey}"
    batch_topic = "/${productKey}/${deviceName}/user/get"
    msg_ttl = 15d
    cleanup_interval = 60s
    max_device_count = 10000
    max_message_size_broadcast = 65536    # 64 KiB
    max_message_size_batch = 10240        # 10 KiB (binary)
    msg_warn_threshold = 100000
}
```

## API

Unified endpoint: `POST /api/v5/plugin_api/emqx_iot/pub`

Distinguished by the `Action` field in the request body:
- `"PubBroadcast"` -- broadcast to all online devices of a product
- `"BatchPub"` -- batch delivery to a specified device list
- `"RegisterMessage"` -- pre-register message content or refresh TTL

Full API documentation: [docs/API.md](docs/API.md)
Usage guide: [docs/USAGE.md](docs/USAGE.md)

## Namespace Integration

Client identities map to `ProductKey` and `DeviceName` through EMQX's multi-tenancy:

```hocon
mqtt.client_attrs_init = [
  { expression = "nth(1, tokens(username, '-'))", set_as_attr = tns }
]
```

Client connection example: `username = "P1-device001"`, `clientId = "device001"`
→ `namespace = "P1"` → `ProductKey = "P1"`, `DeviceName = "device001"`

## Architecture

```
API Layer (HTTP)
  ├── emqx_iot_api.erl              -- dispatch by Action
  ├── emqx_iot_pub_broadcast.erl
  ├── emqx_iot_batch_pub.erl
  └── emqx_iot_register_message.erl

ID Layer
  └── emqx_iot_id.erl              -- UUID v4 ↔ emqx_guid dual-layer mapping

Storage Layer
  └── emqx_iot_storage.erl          -- Mnesia CRUD, ACK tracking, cleanup

Device Layer
  └── emqx_iot.erl                  -- hooks, ETS device table, offline replay

Infrastructure
  ├── emqx_iot_app.erl              -- application lifecycle
  ├── emqx_iot_sup.erl              -- supervisor
  ├── emqx_iot_config.erl           -- configuration loading
  ├── emqx_iot_utils.erl            -- GUID, UUID, SHA-256, Base64, topic expansion
  ├── emqx_iot_cleanup.erl          -- scheduled expired message cleanup
  └── emqx_iot_metrics.erl          -- Prometheus counters and gauge (self-managed ETS)
```

## Prometheus Metrics

Plugin metrics are exposed at a dedicated endpoint:

```
GET /api/v5/plugin_api/emqx_iot/metrics
Content-Type: text/plain; version=0.0.4
```

Available metrics: `iot_mq_broadcast_pub_in`, `iot_mq_batch_pub_qos0_in`, `iot_mq_batch_pub_qos1_in`, `iot_mq_batch_pub_qos1_msg_wanted`, `iot_mq_batch_pub_qos1_msg_acked`, `iot_mq_batch_pub_qos1_msg_replayed`, `iot_mq_register_message_in`, etc.

## Tests

```bash
# Unit tests
MIX_ENV=emqx-enterprise-test mix test

# CT suite (Docker-based)
scripts/ct/run.sh --app plugins/emqx_iot
```
