# Per-username Session Quota

This plugin enforces a per-username session quota.

- Session counters are maintained per username and synchronized cluster-wide.
- Authentication is rejected with `quota_exceeded` when the configured quota is reached.
- Reconnects with an existing `clientid` do not consume additional quota.
- Whitelisted usernames bypass quota checks.

> [!NOTE]
> Per-username session count lmit can be achievend by setting username as namespace (set `client_attrs.tns` in `client_attrs_init` config).
> This plugin is only needed when there is adifferent scheme for namespace.

## Configuration

Plugin config fields:

- `max_sessions_per_username` (default: `100`)
- `username_white_list` (default: `[]`)
- `snapshot_refresh_interval_ms` (default: `5000`)
- `snapshot_request_timeout_ms` (default: `5000`)

Config semantics:

- `max_sessions_per_username`: maximum concurrent sessions per username.
- `username_white_list`: usernames that bypass quota checks.
- `snapshot_refresh_interval_ms`: minimum interval between snapshot rebuilds used by list API.
- `snapshot_request_timeout_ms`: timeout budget for list API snapshot request handling.

Normalization:

- non-positive or invalid values fall back to defaults.
- string values are accepted for numeric items if convertible to integers.

Update plugin config through the standard plugin config API:

`PUT /api/v5/plugins/<name-vsn>/config`

## Runtime API

The plugin exposes runtime APIs through plugin API gateway:

- `GET /api/v5/plugin_api/emqx_username_quota/quota/usernames`
- `GET /api/v5/plugin_api/emqx_username_quota/quota/usernames/:username`
- `DELETE /api/v5/plugin_api/emqx_username_quota/quota/usernames/:username`

### `GET /quota/usernames`

Query params:

- `limit`: positive integer, capped at `100` (default `100`)
- `cursor`: optional opaque cursor returned by previous list call. If missing, the first page is returned.

Behavior:

- Results are always sorted in deterministic snapshot key order (`{used, username}`).
- Pagination is cursor-based. Omit `cursor` for the first page.
- Each item includes realtime `used` and `clientids`.
- If realtime `used` differs from snapshot count, `snapshot_used` is included.

Successful response shape:

- `data`: username quota entries
- `meta.limit`: effective limit
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

## Operational Notes

### Quota overshoot under burst connects

Quota decisions are made during authentication, while session counters are finalized on session lifecycle hooks.
Under high concurrent connect bursts (especially in clusters), this creates a short synchronization window where
the observed concurrent sessions for one username can temporarily exceed `max_sessions_per_username`.

Practical implication:

- This plugin provides cluster-wide quota enforcement with eventual consistency under burst load.
- It is not a strict per-packet admission gate under extreme connection fan-in.

### Handling `503` from list API

When snapshot owner is busy or rebuilding, list API returns `503` with `retry_cursor`.

API Client guidance:

- Keep and reuse `retry_cursor` on retry to route back to the same snapshot owner node.
- Retry with bounded backoff.
- If retry cursor becomes invalid (for example, node changed), start over without `cursor`.

## Build plugin package

From repository root:

```bash
make plugin-emqx_username_quota
```

## Run tests

From repository root:

```bash
./scripts/ct/run.sh --app emqx_username_quota
```
