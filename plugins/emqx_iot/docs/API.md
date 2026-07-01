# EMQX IoT Plugin — API Reference

## General

**Endpoint**: `POST /api/v5/plugin_api/emqx_iot/pub`

**Authentication**: HTTP Basic Auth using an EMQX API Key as username and API Secret as password.

```bash
curl -u "<api_key>:<api_secret>" -X POST "http://<host>:18083/api/v5/plugin_api/emqx_iot/pub" \
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
  -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_iot/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"PubBroadcast","ProductKey":"P1","MessageContent":"SGVsbG8="}'
```

---

## Action: BatchPub

Publishes messages to a specified list of devices, up to 10,000 per call (configurable). Supports `MessageContent` or `MessageId` (mutually exclusive) for specifying the message body.

> Messages are delivered directly to the target client processes via internal channels, bypassing subscription state and ACL checks.

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
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_iot/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["D1","D2"],"MessageContent":"SGVsbG8=","Qos":0}'

# QoS=1 with MessageId reuse
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_iot/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"BatchPub","ProductKey":"P1","DeviceName":["D3"],"MessageId":"550e8400-e29b-41d4-a716-446655440000","Qos":1}'
```

**Topic Priority** (BatchPub only):

1. `TopicTemplateName` (API parameter) — used directly, placeholder `${deviceName}` is expanded
2. `TopicShortName` (API parameter) — appended to `/${productKey}/${deviceName}/user/${TopicShortName}`
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
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_iot/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"RegisterMessage","MessageContent":"SGVsbG8="}'

# Refresh TTL
curl -u "<api_key>:<api_secret>" -X POST "http://127.0.0.1:18083/api/v5/plugin_api/emqx_iot/pub" \
  -H "Content-Type: application/json" \
  -d '{"Action":"RegisterMessage","MessageId":"550e8400-e29b-41d4-a716-446655440000"}'
```

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
| `MessageNotFound` | 400 | MessageId does not exist (for refresh or reuse) |
| `MessageIdContentConflict` | 400 | Both MessageContent and MessageId provided, or neither |
| `MissingAction` | 400 | Request body does not contain an Action field |
| `UnknownAction` | 400 | Action value is not recognized |
| `InternalError` | 500 | Internal server error |

---

## Prometheus Metrics

Plugin metrics are exposed at a dedicated endpoint:

```
GET /api/v5/plugin_api/emqx_iot/metrics
Content-Type: text/plain; version=0.0.4
```

This endpoint is separate from the built-in EMQX Prometheus endpoints.

### Counters

| Metric | Description |
|--------|-------------|
| `iot_mq_broadcast_pub_in` | PubBroadcast success count |
| `iot_mq_broadcast_pub_error` | PubBroadcast error count |
| `iot_mq_batch_pub_qos0_in` | BatchPub QoS=0 success count |
| `iot_mq_batch_pub_qos0_error` | BatchPub QoS=0 error count |
| `iot_mq_batch_pub_qos1_in` | BatchPub QoS=1 success count |
| `iot_mq_batch_pub_qos1_error` | BatchPub QoS=1 error count |
| `iot_mq_batch_pub_qos1_incomplete` | BatchPub QoS=1 incomplete count |
| `iot_mq_register_message_in` | RegisterMessage create success count |
| `iot_mq_register_message_refresh` | RegisterMessage TTL refresh count |
| `iot_mq_register_message_error` | RegisterMessage error count |
| `iot_mq_batch_pub_qos1_msg_wanted` | Total devices targeted |
| `iot_mq_batch_pub_qos1_msg_succeed` | Devices successfully delivered |
| `iot_mq_batch_pub_qos1_msg_acked` | PUBACK received count |
| `iot_mq_batch_pub_qos1_msg_replayed` | Messages replayed on reconnect |
| `iot_mq_batch_pub_qos1_msg_error` | Delivery error count |
| `iot_mq_batch_pub_qos1_msg_incomplete` | Timeout/incomplete count |

### Gauge

| Metric | Description |
|--------|-------------|
| `iot_mq_batch_pub_qos1_msg_pending` | Currently pending deliveries |

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: emqx_iot
    metrics_path: /api/v5/plugin_api/emqx_iot/metrics
    static_configs:
      - targets: ['emqx:18083']
    basic_auth:
      username: <api_key>
      password: <api_secret>
```
