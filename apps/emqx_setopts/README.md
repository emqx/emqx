# emqx_setopts

An EMQX application that allows dynamic modification of MQTT connection options
at runtime via system topics. Currently supports updating the keepalive interval
for connected clients.

## How It Works

The application hooks into `message.publish` and intercepts messages published
to `$SETOPTS/` system topics. These messages are consumed (never routed to
subscribers) and translated into connection-level operations on the target
clients.

The main gen_server (`emqx_setopts`) serializes updates and fans them out
across the cluster via `emqx_setopts_proto_v1` (BPAPI/erpc multicall).

## System Topics

### `$SETOPTS/mqtt/keepalive` — Single Client (Self)

A connected client publishes to this topic to update **its own** keepalive.
The payload is the new keepalive interval in seconds (integer or string, 0–65535).

Only the publishing client's own keepalive is affected. Non-client publishers
(e.g. HTTP API origin) are ignored.

```
Topic:   $SETOPTS/mqtt/keepalive
Payload: "30"
```

### `$SETOPTS/mqtt/keepalive-bulk` — Batch Update

Any client can publish a JSON array to update keepalive for multiple clients
at once. Each item must have `clientid` and `keepalive` fields.

```
Topic:   $SETOPTS/mqtt/keepalive-bulk
Payload: [{"clientid": "client-1", "keepalive": 30},
          {"clientid": "client-2", "keepalive": 60}]
```

Batch updates are processed asynchronously. When the gen_server mailbox exceeds
a configurable limit (default 10), incoming batches are dropped to prevent
overload.

## Keepalive Clamping

If the zone configuration has `mqtt.server_keepalive` set to an integer value,
requested keepalive intervals are clamped to not exceed that value. This applies
to both single and bulk updates.

## API

| Function | Description |
|---|---|
| `emqx_setopts:set_keepalive({ClientId, Interval})` | Synchronous single-client update (5s timeout) |
| `emqx_setopts:set_keepalive_batch_async(Batch)` | Asynchronous batch update |

## Architecture

```
emqx_setopts_app        OTP application
  emqx_setopts_sup      Supervisor (one_for_one)
    emqx_setopts        gen_server — hook handler, update coordinator
  emqx_setopts_proto_v1 BPAPI module — cluster-wide RPC
```
