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
- `snapshot_min_age_ms` (default: `300000`, range: `120000`–`900000`) — minimum age of a snapshot before it can be rebuilt. Values outside the range are clamped.
- `snapshot_request_timeout_ms` (default: `5000`)

Config semantics:

- `max_sessions_per_username`: default maximum concurrent sessions per username. Individual usernames can override this via the overrides API.
- `snapshot_min_age_ms`: minimum age (in milliseconds) of a snapshot before a rebuild is triggered. Prevents frequent rebuilds on large clusters.
- `snapshot_request_timeout_ms`: timeout budget for list API snapshot request handling.

Validation:

- `max_sessions_per_username` must be >= 1. Values less than 1 or non-numeric values are rejected.
- String values are accepted for numeric fields if convertible to positive integers.

Update plugin config through the standard plugin config API:

`PUT /api/v5/plugins/<name-vsn>/config`

## Runtime API

The plugin exposes runtime APIs through plugin API gateway.

Base path: `/api/v5/plugin_api/emqx_username_quota`

**Snapshot**: The `GET /quota/usernames` endpoint serves results from a pre-built snapshot rather than scanning the live session data on every request. A snapshot is a point-in-time copy of per-username session counts, sorted by count for efficient cursor-based pagination. Snapshots are built asynchronously in the background and cached; a new build is only triggered when the current snapshot is older than `snapshot_min_age_ms`. Each response item includes a `snapshot_used` field when the realtime count has drifted from the snapshot value, so the caller can see both the cached and current counts.

**First-request wait**: When the very first request arrives and no snapshot exists yet, the server waits (up to the request deadline minus 1 second) for the in-progress build to complete. If the build finishes in time, a normal `200` response is returned. If not, a `503` is returned with partial data (see below).

### Session queries

- `GET /quota/usernames` — list all usernames with active sessions
- `GET /quota/usernames/:username` — get details for a single username
- `POST /kick/:username` — kick all sessions for a username

### Snapshot management

- `DELETE /quota/snapshot` — force snapshot rebuild

### Quota overrides

- `POST /quota/overrides` — set per-username quota overrides
- `DELETE /quota/overrides` — delete per-username quota overrides
- `GET /quota/overrides` — list all quota overrides

### `GET /quota/usernames`

Query params:

- `limit`: positive integer, capped at `100` (default `100`)
- `used_gte`: **required** (when no cursor) — minimum session count filter. Only usernames with at least this many sessions are included. Must be a positive integer >= 1.
- `cursor`: optional opaque cursor returned by previous list call. If missing, the first page is returned.

Parameter rules:

- `used_gte` without `cursor`: OK (first page)
- `cursor` without `used_gte`: OK (`used_gte` is embedded in the cursor)
- Both `used_gte` and `cursor`: **400** `BAD_REQUEST` — the filter is locked in the cursor
- Neither `used_gte` nor `cursor`: **400** `BAD_REQUEST`

Behavior:

- Results are always sorted by session count then username.
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

Error responses:

- `400 BAD_REQUEST`: missing `used_gte`, or `used_gte` provided with cursor
- `400 INVALID_CURSOR`: cursor references an unavailable node or is malformed
- `503 SERVICE_UNAVAILABLE`: snapshot is being rebuilt
  - Body includes `snapshot_build_in_progress: true`, `retry_cursor`, `data`, and `meta`
  - `data`: partial first page read from the in-progress snapshot (may be empty if the build just started)
  - `meta.count`: number of partial entries, `meta.partial: true`
  - Retry with `retry_cursor` to route to the same snapshot owner node

### `DELETE /quota/snapshot`

Force an immediate snapshot rebuild. Returns `200` with `{"status": "ok"}` after initiating the rebuild asynchronously. The snapshot will be rebuilt in the background.

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

Overrides are persisted to disk and replicated cluster-wide. When no override exists for a username, the global `max_sessions_per_username` config is used.

### `DELETE /quota/overrides`

Delete overrides by username. Body is a JSON array of username strings:

```json
["user1", "blocked"]
```

### `GET /quota/overrides`

List all overrides. Returns `{"data": [{"username": "...", "quota": ...}, ...]}`.

## Architecture

### Core-only snapshot ownership

Snapshots are only built and served by core nodes. The `GET /quota/usernames` endpoint returns `404 NOT_AVAILABLE` when called on a replicant node. Direct list API requests to a core node.

### Blue/green snapshots

Two snapshot buffers (blue and green) are maintained. While one serves read requests, the other is used for building the next snapshot. Once a build completes, the roles are swapped. This eliminates any data gap during rebuilds — the old snapshot remains available until the new one is ready.

### Background snapshot build

Snapshot rebuilds run in a background process with yield-based throttling to avoid blocking the server. The list API remains responsive while a build is in progress.

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

When the server is busy or building a snapshot, the list API returns `503` with `retry_cursor`.

The `503` response body includes a `data` array with a partial first page read from the in-progress snapshot table. This gives callers best-effort data immediately rather than an empty response. The `meta.partial: true` flag indicates the data is incomplete. The partial page may be empty if the build has just started.

API Client guidance:

- Inspect `data` for any partial results available immediately.
- Keep and reuse `retry_cursor` on retry to route back to the same snapshot owner node.
- Retry with bounded backoff.
- If retry cursor becomes invalid (for example, node changed), start over without `cursor`.
