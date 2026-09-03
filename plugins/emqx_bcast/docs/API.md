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

> Batch size vs API latency (measured 2026-09-03, plugin 0.4.0, 5-node
> cluster): acceptance is asynchronous, so a 10,000-device request no
> longer stalls the API (sequential ~27 ms, 4 concurrent ~36 ms at 64 B
> payload, zero rejects); promotion/drain cost still scales with the
> device count, so use <= 1,000 devices per request for sustained
> throughput and reserve 10,000-device calls for low-rate bulk loads.

> Messages are delivered directly to the target client processes via internal channels, bypassing ACL checks. Delivery is gated by the device's MQTT subscription: a device that is not subscribed to the delivery topic does not receive the message.

> Delivery is asynchronous: a `200` response means the request was accepted, not that all devices have received the message. For QoS=1, messages to offline devices are stored on core and delivered when the device reconnects.

| Field | Type | Required | Description |
|-------|------|----------|-------------|
| `Action` | String | Yes | `"BatchPub"` |
| `ProductKey` | String | Yes | Target product identifier |
| `DeviceName` | [String] | Yes | Target device list, max 10,000 (configurable), must not contain duplicates |
| `MessageContent` | String | Conditional | Base64-encoded, max 10 KiB binary (API input limit: 13,656 characters). Mutually exclusive with `MessageId` |
| `MessageId` | String | Conditional | UUID v4 format. Mutually exclusive with `MessageContent` |
| `Qos` | Integer | No | 0 (default, online only) or 1 (online + offline storage, TTL = `msg_ttl`, default 15 days) |
| `TopicShortName` | String | No | Custom topic suffix |
| `TopicTemplateName` | String | No | Custom topic template. Higher priority than `TopicShortName` |

**QoS Behavior**:

| QoS | Online Devices | Offline Devices | Storage | ACK Tracking |
|-----|---------------|-----------------|---------|-------------|
| 0 | Deliver | Skip | None | None |
| 1 | Deliver + wait PUBACK | Store, replay on reconnect | TTL = `msg_ttl` (default 15 days) | Counter ≥ target_ack_count then delete |

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

1. `TopicTemplateName` (API parameter) -- used directly, placeholders `${productKey}` and `${deviceName}` are expanded
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
| `MessageContent` (first) | Create message, TTL = now + `msg_ttl` (default 15d) | New UUID v4 |
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

**Response body** (both create and refresh, HTTP 200):

```json
{ "Success": true, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "MessageId": "550e8400-e29b-41d4-a716-446655440000" }
```

On error (HTTP 400), the body matches the general error shape:

```json
{ "Success": false, "RequestId": "550e8400-e29b-41d4-a716-446655440000", "Code": "InvalidBase64", "ErrorMessage": "Invalid Base64 encoding" }
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
GET /api/v5/plugin_api/emqx_bcast/messages?limit=100
GET /api/v5/plugin_api/emqx_bcast/messages?limit=100&cursor=<cursor>
```

`limit` defaults to 100 and must be between 1 and 1000 (a larger value
returns 400 `InvalidParams`). Messages are returned newest first. When
more pages remain, the response includes a `Cursor` field; pass it back
as the `cursor` query parameter to fetch the next page. The last page
carries no `Cursor`. An invalid or missing cursor starts from the first
page.

```json
{
  "Success": true,
  "RequestId": "...",
  "Cursor": "1753200000_1a2b3c4d",
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
| `QuotaExceeded` | 429 | Pending delivery quota exceeded. For per-device over-limit the body includes a `Devices` array listing the devices over their cap |
| `InternalError` | 500 | Internal server error |

---

## Prometheus Metrics

Plugin metrics are exposed at a dedicated endpoint:

```
GET /api/v5/plugin_api/emqx_bcast/metrics
Content-Type: text/plain; version=0.0.4
```

This endpoint is separate from the built-in EMQX Prometheus endpoints.

The endpoint carries **bcast plugin business metrics only**. System-level
values (node CPU, memory, connections, broker `messages.delivered`) are
not duplicated here: they are available from EMQX's own Prometheus
endpoint.

### QoS=1 delivery ledger (counters)

The unit of the delivery ledger is one **logical delivery**: one BatchPub
request targeted at one device. All ledger counters are node-local and
updated by asynchronous workers, so they lag the API response; to get
cluster totals scrape **every node** and sum the values. `wanted` is
incremented on the node that runs the promoter committing the request
(core node); delivered/acked/ttl_expired/canceled count on the node where
the event happened.

| Metric | Description |
|--------|-------------|
| `bcast_batch_pub_qos1_wanted` | Logical deliveries durably committed to mria (promotion). Ledger base; incremented once per committed device, NOT at API acceptance |
| `bcast_batch_pub_qos1_delivered` | Actual PUBLISH sends (includes redeliveries and the QoS0-subscription auto path) |
| `bcast_batch_pub_qos1_redelivered` | Sends whose core claim attempt number was >= 2 (same logical delivery attempted before: lease expiry, disconnect, unsubscribe, claim-race release) |
| `bcast_batch_pub_qos1_acked` | PUBACKs matched to a pending delivery (duplicates are not counted) |
| `bcast_batch_pub_qos1_auto_acked` | Logical deliveries completed because the subscription QoS is 0 |
| `bcast_batch_pub_qos1_ttl_expired` | Logical deliveries abandoned because the delivery TTL expired before confirmation |
| `bcast_batch_pub_qos1_canceled` | Logical deliveries removed by management delete or reset before confirmation |

**Ledger identity (eventually consistent):**

`wanted = acked + auto_acked + ttl_expired + canceled + queued + inflight`

where `queued`/`inflight` are the gauges below. Equivalently the current
backlog (unconfirmed logical deliveries) is

`wanted - (acked + auto_acked + ttl_expired + canceled)`

and `delivered = first_sends + redelivered` (first_sends is derived as
`delivered - redelivered`). Counters reset to zero when a node restarts;
the ledger identity is guaranteed for events observed after the last
restart/reset.

### Admission counters (request level)

| Metric | Description |
|--------|-------------|
| `bcast_batch_pub_qos1_in` | BatchPub QoS=1 API requests |
| `bcast_batch_pub_qos1_enqueued` | QoS=1 requests accepted into the intake queue |
| `bcast_batch_pub_qos1_intake_rejected` | QoS=1 requests rejected because the intake queue is full |
| `bcast_batch_pub_qos1_promote_error` | QoS=1 promotion batch failures (retries exhausted) |

Within a node's lifetime `in = enqueued + intake_rejected + quota
rejections`; quota (429 QuotaExceeded) rejections are not exported
separately — they are the derivable residual.

### QoS=0 / broadcast / register counters

| Metric | Description |
|--------|-------------|
| `bcast_batch_pub_qos0_in` | BatchPub QoS=0 API requests |
| `bcast_batch_pub_qos0_targeted` | QoS=0 devices targeted |
| `bcast_qos0_delivery_count` | QoS=0 one-shot deliveries to online clients |
| `bcast_broadcast_pub_in` | PubBroadcast API requests |
| `bcast_broadcast_pub_error` | PubBroadcast errors |
| `bcast_register_message_in` | RegisterMessage API requests |
| `bcast_register_message_refresh` | RegisterMessage TTL refresh |
| `bcast_register_message_error` | RegisterMessage errors |

### Gauges

Sampled at scrape time from live state; sum over nodes for cluster totals
(index shards run on core nodes, replicants report 0).

| Metric | Description |
|--------|-------------|
| `bcast_intake_depth` | QoS=1 intake queue depth (requests awaiting promotion, node-local) |
| `bcast_batch_pub_qos1_queued` | Committed logical deliveries queued but not yet claimed |
| `bcast_batch_pub_qos1_inflight` | Claimed logical deliveries not yet terminal (awaiting ack/release/expiry) |

### Metrics reset

`POST /api/v5/plugin_api/emqx_bcast/metrics/reset`

Resets the metric registry to zero on **every** running node (a partial
reset would permanently break cross-node sums). Refuses with
`409 PendingDeliveries` while any node still holds queued or in-flight
deliveries: counters and delivery state are separate, and the ledger
identity only holds for events observed after a reset. Intended for
maintenance windows and test tooling.

### Prometheus Configuration

```yaml
scrape_configs:
  - job_name: emqx_bcast
    metrics_path: /api/v5/plugin_api/emqx_bcast/metrics
    static_configs:
      - targets:
          - 'emqx-core-1:18083'
          - 'emqx-core-2:18083'
          - 'emqx-replicant-1:18083'
          - 'emqx-replicant-2:18083'
    basic_auth:
      username: <api_key>
      password: <api_secret>
```
