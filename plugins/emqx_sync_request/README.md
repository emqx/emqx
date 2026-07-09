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

## Request

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

`request_id` is used as MQTT 5 Correlation Data and must be a plain string up
to 128 bytes. Request payloads may be sent as `plain` text or `base64`.

Successful responses return the MQTT response payload as base64:

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

## Configuration

```hocon
default_timeout = "10s"
max_timeout = "60s"
max_inflight_requests = 10000
max_payload_size = "64KB"
```

`max_inflight_requests` limits per-node local HTTP requests waiting for responses.
`max_payload_size` applies to both MQTT request and response payloads.

## Build And Test

From the EMQX repository root:

```bash
PROFILE=emqx-enterprise make compile-emqx-enterprise
PROFILE=emqx-enterprise make plugins/emqx_sync_request-ct
PROFILE=emqx-enterprise make plugin-emqx_sync_request
```

The package is written to `_build/plugins/emqx_sync_request-<version>.tar.gz`.
