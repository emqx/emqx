# EMQX Bcast Plugin -- Usage Guide

## Prerequisites

- EMQX 6.1.x Enterprise with a valid license
- API key with administrator role (for plugin API access)
- MQTT clients with usernames following the `ProductKey-DeviceName` convention

## Setup

### 1. EMQX Namespace Configuration

Configure `client_attrs_init` to derive the namespace from the MQTT client's username:

```hocon
mqtt.client_attrs_init = [
  { expression = "nth(1, tokens(username, '-'))", set_as_attr = tns }
]
```

With this configuration, a client connecting with `username = "P1-device001"` has:
- `namespace` (via `client_attrs.tns`) = `"P1"`
- `DeviceName` = the client's `clientId`

### 2. Install Plugin

```bash
emqx ctl plugins install emqx_bcast-<vsn>.tar.gz
emqx ctl plugins start emqx_bcast
```

### 3. Create API Key

**Option A -- Bootstrap file** (recommended for Docker):

```bash
# bootstrap-api-key.txt format: key:secret:role
echo "my_api_key:my_api_secret_min_32_chars:administrator" > bootstrap-api-key.txt

# Docker compose:
#   EMQX_API_KEY__BOOTSTRAP_FILE: /etc/emqx/bootstrap-api-key.txt
#   volumes:
#     - ./bootstrap-api-key.txt:/etc/emqx/bootstrap-api-key.txt:ro
```

**Option B -- Dashboard**:

Management → API Keys → Create

---

## Workflow Examples

### Scenario 1: Pre-register Message + Batch Delivery

This pattern optimizes bandwidth at scale -- register the payload once, then reference it across many BatchPub calls.

```bash
API_KEY="my_api_key:my_api_secret_min_32_chars"
HOST="http://127.0.0.1:18083"

# Step 1: Pre-register the message body
MSG_ID=$(curl -su "$API_KEY" \
  -X POST "$HOST/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"RegisterMessage","MessageContent":"SGVsbG8gV29ybGQ="}' \
  | python3 -c "import sys,json; print(json.load(sys.stdin)['MessageId'])")

# Step 2: BatchPub to 1000 devices (reuse MessageId, no payload transfer)
curl -su "$API_KEY" \
  -X POST "$HOST/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d "{\"Action\":\"BatchPub\",\"ProductKey\":\"P1\",\"DeviceName\":[\"D1\",\"D2\",\"D3\"],\"MessageId\":\"$MSG_ID\",\"Qos\":1}"
```

For large-scale scenarios (500,000+ devices), pre-register once and send ~50 BatchPub calls of 10,000 devices each, all sharing the same `MessageId`.

### Scenario 2: Broadcast to All Online Devices

```bash
curl -su "$API_KEY" \
  -X POST "$HOST/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"PubBroadcast","ProductKey":"P1","MessageContent":"SGVsbG8gV29ybGQ="}'
```

The message reaches all online devices of product `P1`. Offline devices do not receive the broadcast.

### Scenario 3: QoS=1 with Offline Replay

QoS=1 messages are persisted for `msg_ttl` (default 15 days). When an offline device reconnects, the plugin automatically replays pending messages in FIFO order.

```bash
# Device list includes both online and offline devices
curl -su "$API_KEY" \
  -X POST "$HOST/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["online_device","offline_device"],"MessageContent":"SGVsbG8=","Qos":1}'
```

- `online_device` -- the core broadcasts a trigger, the node serving the device pulls the delivery and waits for PUBACK
- `offline_device` -- stored in core Mnesia, pulled and delivered when the device reconnects and subscribes

The delivery record is automatically deleted once all devices have acknowledged. A `200` response means the request was accepted and the QoS=1 delivery record is stored before the response returns; actual delivery completes asynchronously. BatchPub QoS=1 is at-least-once, so clients should tolerate duplicate delivery around reconnect/takeover windows.

### Scenario 4: QoS=0 Fire-and-Forget

```bash
curl -su "$API_KEY" \
  -X POST "$HOST/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["D1","D2"],"MessageContent":"SGVsbG8=","Qos":0}'
```

Only currently online devices receive the message. No storage, no ACK tracking.

---

## Monitoring

### Real-time Metrics

```bash
curl -su "$API_KEY" "$HOST/api/v5/plugin_api/emqx_bcast/metrics"
```

### Prometheus + Grafana

1. Configure Prometheus to scrape `http://<emqx>:18083/api/v5/plugin_api/emqx_bcast/metrics`
2. Import the Grafana dashboard

Key metrics to watch:
- **`bcast_batch_pub_qos1_wanted - (bcast_batch_pub_qos1_acked + bcast_batch_pub_qos1_auto_acked)`** -- backlog of unacknowledged deliveries
- **`rate((bcast_batch_pub_qos1_acked + bcast_batch_pub_qos1_auto_acked)[5m]) / rate(bcast_batch_pub_qos1_wanted[5m])`** -- delivery success rate

---

## Configuration Reference

| Parameter | Default | Description |
|-----------|---------|-------------|
| `broadcast_topic` | `/sys/broadcast/${productKey}` | Topic template for PubBroadcast |
| `batch_topic` | `/${productKey}/${deviceName}/user/get` | Default topic template for BatchPub |
| `msg_ttl` | `15d` | Message expiry (delivery record TTL) |
| `cleanup_interval` | `60s` | How often to scan for expired records |
| `max_device_count` | `10000` | Max DeviceName list size per BatchPub call |
| `max_message_size_broadcast` | `65536` | Max PubBroadcast payload (bytes, 64 KiB) |
| `max_message_size_batch` | `10240` | Max BatchPub payload binary (bytes, 10 KiB) |
| `max_pending_deliveries` | `10000000` | Global cap on pending QoS=1 deliveries; requests exceeding it are rejected with 429 QuotaExceeded |
| `max_pending_deliveries_per_device` | `100` | Per-device cap on pending QoS=1 deliveries (clamped 10-200); requests targeting a device over the cap are rejected with 429 QuotaExceeded and the over-limit device list |
| `delivery_pool_size` | `0` | Number of async workers for each of the three pools (pull, ack and pull-server). 0 means one worker per scheduler. Changing it restarts the pools |

---

## Troubleshooting

**"Plugin API Not Found"**: Ensure the plugin is installed and started. Check `emqx ctl plugins list`.

**"BAD_API_KEY_OR_SECRET"**: Verify the API key exists and has administrator role. For bootstrap files, restart EMQX after creating the file.

**Messages not delivered to offline devices**: Check that `msg_ttl` hasn't expired. Verify the device's `ProductKey` and `DeviceName` match between the API call and MQTT client connection.

**High pending count**: If `bcast_batch_pub_qos1_wanted - (bcast_batch_pub_qos1_acked + bcast_batch_pub_qos1_auto_acked)` is growing, check that offline devices are eventually reconnecting within the TTL window. Consider increasing `msg_ttl` for longer offline tolerance.
