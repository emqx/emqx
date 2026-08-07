# EMQX Bcast Plugin -- API Reference

## General

**Endpoint**: `POST /api/v5/plugin_api/emqx_bcast/pub`

**Authentication**: HTTP Basic Auth using an EMQX API Key as username and API Secret as password.

```bash
curl -u "<api_key>:<api_secret>" -X POST "http://<host>:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" -d '{...}'
```

**Success Response**:

```json
{ "Success": true, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }
```

**Error Response**:

```json
{ "Success": false, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "Code": "ErrorCode", "ErrorMessage": "human-readable description" }
```

`RequestId` and `MessageId` are UUID v4 format.

---

## Action: PubBroadcast

Broadcasts a message to all online devices within a product. Offline devices do not receive the message; no storage is performed.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Action` | String | Yes | `"PubBroadcast"` |
| `ProductKey` | String | Yes | Target product identifier |
| `MessageContent` | String | Yes | Base64-encoded payload, max 64 KiB |
| `TopicFullName` | String | No | Custom broadcast topic. Defaults to the plugin-configured broadcast topic |

```json
// Request
{ "Action": "PubBroadcast", "ProductKey": "P1", "MessageContent": "SGVsbG8=" }

// Response
{ "Success": true, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }
```

```bash
curl -u "<api_key>:<api_secret>" \
  -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"PubBroadcast","ProductKey":"P1","MessageContent":"SGVsbG8="}'
```

---

## Action: BatchPub

Publishes messages to a specified list of devices, up to 10,000 per call (configurable). Supports `MessageContent` or `MessageId` (mutually exclusive) for specifying the message body.

> Messages are delivered directly to the target client processes via internal channels, bypassing subscription state and ACL checks.

> Delivery is asynchronous: a `200` response means the request was accepted, not that all devices have received the message. For QoS=1, messages to offline devices are stored and delivered when the device reconnects. If the internal delivery queue is full, the request is rejected with `429 DeliveryQueueFull` and nothing is stored.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Action` | String | Yes | `"BatchPub"` |
| `ProductKey` | String | Yes | Target product identifier |
| `DeviceName` | [String] | Yes | Target device list, max 10,000 (configurable), must not contain duplicates |
| `MessageContent` | String | Conditional | Base64-encoded, max 10 KiB binary (API input limit: 13,656 characters). Mutually exclusive with `MessageId` |
| `MessageId` | String | Conditional | UUID v4 format. Mutually exclusive with `MessageContent` |
| `Qos` | Integer | No | 0 (default, online only) or 1 (online + offline storage, 15-day TTL) |
| `TopicShortName` | String | No | Custom topic suffix |
| `TopicTemplateName` | String | No | Custom topic template. Higher priority than `TopicShortName` |

**QoS Behavior**:

| QoS | Online Devices | Offline Devices | Storage | ACK Tracking |
|-----|---------------|-----------------|---------|-------------|
| 0 | Deliver | Skip | None | None |
| 1 | Deliver + wait PUBACK | Store, replay on reconnect | 15-day TTL | Counter ≥ target_ack_count then delete |

```json
// Request (inline MessageContent)
{
  "Action": "BatchPub", "ProductKey": "P1",
  "DeviceName": ["D1", "D2", "D3"],
  "MessageContent": "SGVsbG8=", "Qos": 1
}

// Request (MessageId reuse)
{
  "Action": "BatchPub", "ProductKey": "P1",
  "DeviceName": ["D4", "D5", "D6"],
  "MessageId": "550e8400-e29b-41d4-a716-446655440000", "Qos": 1
}

// Response
{ "Success": true, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }
```

```bash
# QoS=0 inline content
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["D1","D2"],"MessageContent":"SGVsbG8=","Qos":0}'

# QoS=1 with MessageId reuse
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["D3"],"MessageId":"550e8400-e29b-41d4-a716-446655440000","Qos":1}'
```

**Topic Priority** (BatchPub only):

1. `TopicTemplateName` (API parameter) -- used directly, placeholder `${deviceName}` is expanded
2. `TopicShortName` (API parameter) -- appended to `/${productKey}/${deviceName}/user/${TopicShortName}`
3. Plugin config `batch_topic` template (default)

---

## Action: RegisterMessage

Pre-registers a message or refreshes an existing message's TTL. `MessageContent` and `MessageId` are mutually exclusive.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Action` | String | Yes | `"RegisterMessage"` |
| `MessageContent` | String | Conditional | Base64-encoded, max 10 KiB binary. Creates a new message. Mutually exclusive with `MessageId` |
| `MessageId` | String | Conditional | UUID v4 format. Refreshes an existing message's TTL. Mutually exclusive with `MessageContent` |

**SHA-256 Deduplication**: Identical `MessageContent` → same `MessageId`. Repeated calls refresh the TTL rather than creating duplicates.

| Input | Behavior | Response MessageId |
|-------|----------|--------------------|
| `MessageContent` (first) | Create message, TTL = now + 15d | New UUID v4 |
| `MessageContent` (duplicate) | Content dedup, refresh TTL | Existing UUID v4 |
| `MessageId` | Refresh TTL | Same UUID v4 |

```json
// Create new message
{ "Action": "RegisterMessage", "MessageContent": "SGVsbG8=" }

// Refresh existing message TTL
{ "Action": "RegisterMessage", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }

// Response
{ "Success": true, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }
```

```bash
# Create
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"RegisterMessage","MessageContent":"SGVsbG8="}'

# Refresh TTL
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_bcast/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"RegisterMessage","MessageId":"550e8400-e29b-41d4-a716-446655440000"}'
```

---

## Management API

Endpoints for inspecting and deleting registered messages and deliveries.
These are separate from the `POST /pub` data plane and use the same
authentication. Registered messages are immutable: they can only be
created (via RegisterMessage or BatchPub) and deleted, never updated.
No endpoint returns payload content.

### List Messages

```
GET /api/v5/plugin_api/emqx_bcast/messages?limit=100&offset=0
```

`limit` defaults to 100 and is capped at 1000; `offset` defaults to 0.
Messages are returned newest first.

```json
{
  "Success": true,
  "RequestId": "...",
  "TotalCount": 42,
  "Messages": [
    { "MessageId": "...", "CreatedAt": 1753200000, "ExpiresAt": 1754496000, "PayloadSize": 128 }
  ]
}
```

### Get Message

```
GET /api/v5/plugin_api/emqx_bcast/messages/:messageId
```

Returns the same fields as the list, plus `DeliveryCount` (number of
deliveries referencing the message). 404 `MessageNotFound` if unknown.

### Delete Message

```
DELETE /api/v5/plugin_api/emqx_bcast/messages/:messageId
```

Deletes the message record and cascade-deletes all deliveries referencing
it together with their replay index entries. 404 `MessageNotFound` if
unknown.

### Get Delivery

```
GET /api/v5/plugin_api/emqx_bcast/deliveries/:deliveryId
```

```json
{
  "Success": true,
  "RequestId": "...",
  "DeliveryId": "...",
  "MessageId": "...",
  "ProductKey": "...",
  "DeviceNames": ["device-1", "device-2"],
  "TargetCount": 2,
  "PendingCount": 1,
  "CreatedAt": 1753200000,
  "ExpiresAt": 1754496000
}
```

`PendingCount` is the number of target devices that have not yet
acknowledged. 404 `DeliveryNotFound` if unknown.

### Query Deliveries by Device

```
GET /api/v5/plugin_api/emqx_bcast/deliveries?product_key=PK&device_name=DN
```

Returns all pending deliveries targeting the given device as a
`Deliveries` array with the same fields as above. Both query parameters
are required; missing either returns 400 `InvalidParams`.

### Delete Delivery

```
DELETE /api/v5/plugin_api/emqx_bcast/deliveries/:deliveryId
```

Removes the delivery record and its replay index entries. Delivery tasks
already queued in the async pool are not recalled; acknowledgements
arriving afterwards are ignored as duplicates. 404 `DeliveryNotFound` if
unknown.

---

## Error Codes

| Code | HTTP | Description |
|------|------|-------------|
| `InvalidProductKey` | 400 | ProductKey does not exist or is invalid |
| `InvalidDeviceName` | 400 | DeviceName list contains invalid entries |
| `DeviceCountExceeded` | 400 | DeviceName exceeds the configurable limit (default 10,000) |
| `DuplicateDeviceName` | 400 | DeviceName list contains duplicates |
| `MessageTooLarge` | 400 | MessageContent exceeds size limit |
| `InvalidBase64` | 400 | MessageContent Base64 decoding failed |
| `InvalidTopicTemplate` | 400 | TopicTemplateName format is invalid |
| `MessageNotFound` | 400/404 | MessageId does not exist (400 for refresh or reuse on `POST /pub`; 404 on management endpoints) |
| `MessageIdContentConflict` | 400 | Both MessageContent and MessageId provided, or neither |
| `InvalidQos` | 400 | Qos value is not 0 or 1 |
| `MissingAction` | 400 | Request body does not contain an Action field |
| `UnknownAction` | 400 | Action value is not recognized |
| `InvalidParams` | 400 | Missing required query parameters on management endpoints |
| `DeliveryNotFound` | 404 | DeliveryId does not exist (management endpoints) |
| `DeliveryQueueFull` | 429 | Async delivery queue is full; retry later. Nothing is stored for rejected requests |
| `InternalError` | 500 | Internal server error |

---

## Prometheus Metrics

Plugin metrics are exposed at a dedicated endpoint:

```
GET /api/v5/plugin_api/emqx_bcast/metrics
Content-Type: text/plain; version=0.0.4
```

This endpoint is separate from the built-in EMQX Prometheus endpoints.

### Counters

| Metric | Description |
|--------|-------------|
| `bcast_batch_pub_qos0_in` | BatchPub QoS=0 API requests |
| `bcast_batch_pub_qos0_error` | BatchPub QoS=0 API errors |
| `bcast_batch_pub_qos0_targeted` | QoS=0 devices targeted |
| `bcast_batch_pub_qos0_delivered` | QoS=0 devices delivered |
| `bcast_batch_pub_qos0_skipped` | QoS=0 devices skipped (offline) |
| `bcast_batch_pub_qos1_in` | BatchPub QoS=1 API requests |
| `bcast_batch_pub_qos1_delivered_inline` | QoS=1 inline deliveries |
| `bcast_batch_pub_qos1_stored_offline` | QoS=1 stored for offline delivery |
| `bcast_batch_pub_qos1_wanted` | QoS=1 total wanted acks |
| `bcast_batch_pub_qos1_acked` | QoS=1 acks received |
| `bcast_batch_pub_qos1_replayed` | QoS=1 replayed on reconnect |
| `bcast_broadcast_pub_in` | PubBroadcast API requests |
| `bcast_broadcast_pub_error` | PubBroadcast errors |
| `bcast_broadcast_pub_devices_online` | PubBroadcast devices online |
| `bcast_broadcast_pub_delivery_count` | PubBroadcast deliveries |
| `bcast_register_message_in` | RegisterMessage API requests |
| `bcast_register_message_refresh` | RegisterMessage TTL refresh |
| `bcast_register_message_error` | RegisterMessage errors |
| `bcast_delivery_submit_rejected` | BatchPub requests rejected because the delivery queue was full |

Delivery counters (`delivered`, `delivered_inline`, `acked`) are incremented
by asynchronous delivery workers, so they lag the API response by the time the
queued tasks take to execute.

### Gauges

| Metric | Description |
|--------|-------------|
| `bcast_delivery_queue_depth` | Queued but not yet started delivery tasks |

QoS=1 delivery completion is tracked by comparing `wanted` against `acked`
(a delivery is fully acknowledged when `acked` reaches `wanted` per DeliveryId).

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: emqx_bcast
    metrics_path: /api/v5/plugin_api/emqx_bcast/metrics
    static_configs:
      - targets: ['emqx:18083']
    basic_auth:
      username: <api_key>
      password: <api_secret>
```
