# EMQX Sync Request

`emqx_sync_request` publishes one MQTT request through the EMQX REST API
and waits for the first matching response message.

```http
POST /api/v5/plugin_api/emqx_sync_request/request
```

The plugin stores inflight requests in local node memory only. It does not
persist requests, subscribe to response topics, or modify MQTT payloads.
Request topics must match exactly. Wildcard and shared subscriptions are not
matched as request receivers. Shared subscriptions and multiple exact receivers
return `409 Conflict`.

## HTTP API

```http
POST /api/v5/plugin_api/emqx_sync_request/request
```

Use the same authentication as other EMQX management APIs. Dashboard tokens are
accepted. API keys must be sent with HTTP Basic authentication and need the
`publish` scope.

### Request Body

```json
{
  "timeout": "5s",
  "request": {
    "topic": "devices/1001/request",
    "response_topic": "devices/1001/response",
    "request_id": "request-id-1",
    "qos": 0,
    "payload_encoding": "plain",
    "payload": "{\"cmd\":\"reboot\"}",
    "content_type": "application/json"
  }
}
```

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `timeout` | duration string | No | `default_timeout` | Maximum time to wait for a matching MQTT response. Must be greater than `0` and no more than `max_timeout`. Examples: `100ms`, `5s`, `1m`. |
| `request` | object | Yes | - | MQTT request parameters. |

`request` object:

| Field | Type | Required | Default | Description |
| --- | --- | --- | --- | --- |
| `topic` | string | Yes | - | MQTT request topic. It must be a topic name, not a topic filter, so `+` and `#` are not allowed. Exactly one non-shared subscriber must be online for this topic. |
| `response_topic` | string | Yes | - | MQTT response topic. It must also be a topic name without `+` or `#`. |
| `request_id` | string | Yes | - | Plain string used as MQTT 5 Correlation Data and echoed in the HTTP response. Maximum length is 128 bytes. |
| `qos` | integer | No | `0` | MQTT QoS for the request. Allowed values are `0`, `1`, and `2`. |
| `payload_encoding` | string | No | `plain` | Request payload encoding. Allowed values are `plain` and `base64`. |
| `payload` | string | Yes | - | Request payload. With `plain`, the string bytes are used as the MQTT payload. With `base64`, the value must be valid base64 and the decoded bytes are used as the MQTT payload. The MQTT payload must not exceed `max_payload_size`. |
| `content_type` | string | No | - | MQTT 5 Content Type for the request. MQTT 3 clients do not receive this property. |

### Success Response

Successful responses return HTTP `200` and the MQTT response payload as base64:

```json
{
  "code": "OK",
  "message": "OK",
  "response": {
    "topic": "devices/1001/response",
    "request_id": "request-id-1",
    "payload_encoding": "base64",
    "payload": "eyJyZXN1bHQiOiJvayJ9",
    "content_type": "application/json"
  }
}
```

| Field | Type | Description |
| --- | --- | --- |
| `code` | string | `OK`. |
| `message` | string | `OK`. |
| `response.topic` | string | MQTT response topic. |
| `response.request_id` | string | The `request_id` from the HTTP request. |
| `response.payload_encoding` | string | Always `base64`. |
| `response.payload` | string | Base64 encoded MQTT response payload. |
| `response.content_type` | string | Optional. MQTT 5 Content Type from the response PUBLISH. Omitted when the responder does not send it, including MQTT 3 responders. |

### Error Response

Errors use the same `code` and `message` shape as other EMQX management APIs:

```json
{
  "code": "BAD_REQUEST",
  "message": "request.payload is required."
}
```

| HTTP Status | Code | Meaning |
| --- | --- | --- |
| `400` | `BAD_REQUEST` | Invalid JSON body, invalid field value, request payload too large, or MQTT response payload too large. |
| `401` | `BAD_API_KEY_OR_SECRET` | API key authentication failed. Returned by EMQX management API authentication. |
| `403` | `UNAUTHORIZED_ROLE` | The API key does not have permission to call this API. Returned by EMQX management API authorization. |
| `404` | `NO_SUBSCRIBERS` | No exact, non-shared subscriber is online for the request topic. Wildcard subscribers are ignored. |
| `409` | `CONFLICT` | The request topic has a shared subscription or more than one exact subscriber. |
| `429` | `TOO_MANY_REQUESTS` | The local node already has `max_inflight_requests` HTTP requests waiting for responses. |
| `503` | `SERVICE_UNAVAILABLE` | Failed to dispatch the request to the subscriber node. |
| `504` | `TIMEOUT` | Timed out waiting for a matching MQTT response. |
| `500` | `INTERNAL_ERROR` | Unexpected server-side error. |

## Configuration

```hocon
default_timeout = "10s"
max_timeout = "60s"
max_inflight_requests = 10000
max_payload_size = "64KB"
```

| Field | Type | Default | Description |
| --- | --- | --- | --- |
| `default_timeout` | duration string | `10s` | Default HTTP wait timeout when the request body omits `timeout`. |
| `max_timeout` | duration string | `60s` | Maximum allowed per-request `timeout`. |
| `max_inflight_requests` | positive integer | `10000` | Maximum number of local HTTP requests waiting for responses on one node. |
| `max_payload_size` | bytesize string | `64KB` | Maximum MQTT request payload size and maximum MQTT response payload size. Examples: `8B`, `64KB`, `1MB`. |

## Operational Diagnostics

The plugin provides a CLI command for node-local diagnostics:

```bash
emqx ctl sync_request status
```

Example output:

```text
Counters since plugin start:
sync_request.requests.total: 42
sync_request.requests.succeeded: 39
sync_request.requests.failed: 3
sync_request.requests.bad_request: 1
sync_request.requests.no_subscribers: 1
sync_request.requests.conflict: 0
sync_request.requests.too_many_requests: 0
sync_request.requests.dispatch_failed: 0
sync_request.requests.timeout: 1
sync_request.requests.internal_error: 0

Current gauges:
sync_request.inflight_requests: 0
sync_request.pending_responses: 0
```

These values are not cluster-wide aggregates. The command reads only the node
where it runs. In a cluster, run it on each node that may receive the HTTP
request or deliver the MQTT response. Only requests that reach the plugin
handler are counted; management API authentication and authorization failures
are handled by EMQX before the plugin runs.

The request counters are backed by EMQX node-local metrics. The current gauges
are computed from the plugin's local ETS tables when the CLI command runs.

| Metric | Type | Scope | Description |
| --- | --- | --- | --- |
| `sync_request.requests.total` | counter | node-local | HTTP sync request attempts handled by this node. |
| `sync_request.requests.succeeded` | counter | node-local | Requests that returned HTTP `200`. |
| `sync_request.requests.failed` | counter | node-local | Requests that returned a non-`200` HTTP status. |
| `sync_request.requests.bad_request` | counter | node-local | Requests rejected with `400 BAD_REQUEST`. |
| `sync_request.requests.no_subscribers` | counter | node-local | Requests rejected because no exact, non-shared subscriber was online. |
| `sync_request.requests.conflict` | counter | node-local | Requests rejected because the request topic matched multiple or shared subscribers. |
| `sync_request.requests.too_many_requests` | counter | node-local | Requests rejected because this node reached `max_inflight_requests`. |
| `sync_request.requests.dispatch_failed` | counter | node-local | Requests that could not be dispatched to the subscriber node. |
| `sync_request.requests.timeout` | counter | node-local | Requests that timed out waiting for a matching MQTT response. |
| `sync_request.requests.internal_error` | counter | node-local | Requests that failed with an unexpected internal error. |
| `sync_request.inflight_requests` | gauge | node-local | Current number of HTTP requests waiting for MQTT responses on this node. |
| `sync_request.pending_responses` | gauge | node-local | Current number of local pending response registrations created after request delivery. |

## Build And Test

From the EMQX repository root:

```bash
PROFILE=emqx-enterprise make compile-emqx-enterprise
PROFILE=emqx-enterprise make plugins/emqx_sync_request-ct
PROFILE=emqx-enterprise make plugin-emqx_sync_request
```

The package is written to `_build/plugins/emqx_sync_request-<version>.tar.gz`.
