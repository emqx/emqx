# Per-username Session Quota

This plugin enforces a per-username session quota.

- Session counters are maintained per username and synchronized cluster-wide.
- Authentication is rejected with `quota_exceeded` when the configured quota is reached.
- Reconnects with an existing `clientid` do not consume additional quota.
- Per-username quota overrides allow custom limits, unlimited sessions, or connection blocking.

> [!NOTE]
> Per-username session count limit can be achieved by setting username as namespace (set `client_attrs.tns` in `client_attrs_init` config).
> This plugin is only needed when there is a different scheme for namespace.

## Configuration

Plugin config fields:

- `max_sessions_per_username` (default: `100`) — must be a positive integer (>= 1).
- `snapshot_refresh_interval_ms` (default: `5000`)
- `snapshot_request_timeout_ms` (default: `5000`)

Config semantics:

- `max_sessions_per_username`: default maximum concurrent sessions per username. Individual usernames can override this via the overrides API.
- `snapshot_refresh_interval_ms`: minimum interval between snapshot rebuilds used by list API.
- `snapshot_request_timeout_ms`: timeout budget for list API snapshot request handling.

Validation:

- `max_sessions_per_username` must be >= 1. Values less than 1 or non-numeric values are rejected.
- String values are accepted for numeric fields if convertible to positive integers.

Update plugin config through the standard plugin config API:

`PUT /api/v5/plugins/<name-vsn>/config`

## Runtime API

The plugin exposes runtime APIs through plugin API gateway.

Base path: `/api/v5/plugin_api/emqx_username_quota`

### Session queries

- `GET /quota/usernames` — list all usernames with active sessions
- `GET /quota/usernames/:username` — get details for a single username
- `POST /kick/:username` — kick all sessions for a username

### Quota overrides

- `POST /quota/overrides` — set per-username quota overrides
- `DELETE /quota/overrides` — delete per-username quota overrides
- `GET /quota/overrides` — list all quota overrides

### `GET /quota/usernames`

Query params:

- `limit`: positive integer, capped at `100` (default `100`)
- `cursor`: optional opaque cursor returned by previous list call. If missing, the first page is returned.

Behavior:

- Results are always sorted in deterministic snapshot key order (`{used, username}`).
- Pagination is cursor-based. Omit `cursor` for the first page.
- Each item includes `username`, realtime `used`, `limit` (effective quota), and `clientids`.
- If realtime `used` differs from snapshot count, `snapshot_used` is included.

Successful response shape:

- `data`: username quota entries
- `meta.limit`: page size (pagination limit)
- `meta.count`: number of entries in this page
- `meta.total`: total entries in snapshot
- `meta.next_cursor`: cursor for next page (when available)
- `meta.snapshot`: snapshot metadata:
  - `node`
  - `generation` (incremental snapshot id)
  - `taken_at_ms` (snapshot timestamp in milliseconds)

`503 SERVICE_UNAVAILABLE` response behavior:

- Body includes `retry_cursor`.
- Retry with this cursor to route to the same snapshot owner node and avoid rebuilding snapshot on another node.
- Error shapes:
  - busy: `Snapshot owner is busy handling another request`
  - rebuilding: `Snapshot owner is rebuilding snapshot`

### `GET /quota/usernames/:username`

Returns details for a single username. Response fields: `username`, `used`, `limit`, `clientids`.

Returns `404 NOT_FOUND` if the username has no active sessions.

### `POST /kick/:username`

Kicks all sessions for a username. Returns `{"kicked": N}` where N is the number of sessions kicked.

Returns `404 NOT_FOUND` if the username has no active sessions.

### `POST /quota/overrides`

Set per-username quota overrides. Body is a JSON array:

```json
[
  {"username": "user1", "quota": 1000},
  {"username": "vip", "quota": "nolimit"},
  {"username": "blocked", "quota": 0}
]
```

Override semantics:

| `quota` value    | Meaning                                        |
|------------------|------------------------------------------------|
| positive integer | Custom session limit for this username         |
| `"nolimit"`      | Unlimited sessions (no quota enforcement)      |
| `0`              | Ban — reject all new connections               |

Overrides are persisted to disc and replicated cluster-wide. When no override exists for a username, the global `max_sessions_per_username` config is used.

### `DELETE /quota/overrides`

Delete overrides by username. Body is a JSON array of username strings:

```json
["user1", "blocked"]
```

### `GET /quota/overrides`

List all overrides. Returns `{"data": [{"username": "...", "quota": ...}, ...]}`.

## Operational Notes

### Quota overshoot under burst connects

Quota decisions are made during authentication, while session counters are finalized on session lifecycle hooks.
Under high concurrent connect bursts (especially in clusters), this creates a short synchronization window where
the observed concurrent sessions for one username can temporarily exceed `max_sessions_per_username`.

Practical implication:

- This plugin provides cluster-wide quota enforcement with eventual consistency under burst load.
- It is not a strict per-packet admission gate under extreme connection fan-in.

### Bootstrap on plugin startup

When the plugin is installed on a running cluster, existing client sessions were established before hooks were registered.
On startup, the plugin bootstraps quota state by traversing all local channels and registering each session.

To avoid overloading the Core nodes with a storm of DB write operations (especially when replicant nodes have a large number of existing connections),
the bootstrap loop is throttled:

- Sessions are registered in batches of 100.
- After each batch, the bootstrap waits for the last written record to be replicated back to the local table before continuing. It polls every 10ms.
- If replication does not complete within 10 seconds, an error is logged and bootstrap is aborted with an `error` level log.
  Sessions registered before the timeout are retained; remaining sessions will be picked up naturally through subsequent hook-based registration on reconnect.

### Handling `503` from list API

When snapshot owner is busy or rebuilding, list API returns `503` with `retry_cursor`.

API Client guidance:

- Keep and reuse `retry_cursor` on retry to route back to the same snapshot owner node.
- Retry with bounded backoff.
- If retry cursor becomes invalid (for example, node changed), start over without `cursor`.
