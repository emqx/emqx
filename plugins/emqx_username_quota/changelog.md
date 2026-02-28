# EMQX Username Quota Plugin Changelog

## 1.1.0

- Replaced the static `username_white_list` configuration with per-username quota overrides managed via REST API.
  Overrides support custom limits (positive integer), unlimited sessions (`"nolimit"`), or connection blocking (`0`).
- Added REST API endpoints for managing overrides:
  `POST /quota/overrides`, `DELETE /quota/overrides`, and `GET /quota/overrides`.
- The `GET /quota/usernames` and `GET /quota/usernames/:username` responses now include a `limit` field
  showing the effective quota for each username.
- Config validation now rejects `max_sessions_per_username` values less than 1
  instead of silently falling back to the default.
- Replaced synchronous snapshot rebuild with async background build using blue/green ETS table swapping,
  eliminating data gaps during rebuilds.
- Added mandatory `used_gte` query parameter to `GET /quota/usernames` to filter by minimum session count
  and reduce snapshot size. Providing both `used_gte` and `cursor` returns 400.
- Added `DELETE /quota/snapshot` endpoint to force an immediate snapshot rebuild.
- Snapshot ownership is now core-only; `GET /quota/usernames` returns 404 `NOT_AVAILABLE` on replicant nodes.
- Replaced `snapshot_refresh_interval_ms` config with `snapshot_min_age_ms` (default 300000, clamped to 120000–900000)
  controlling the minimum age before a snapshot can be rebuilt.
- Invalid or unavailable cursor now returns 400 `INVALID_CURSOR` instead of silently restarting from page 1.
- 503 responses now include `snapshot_build_in_progress: true` when no snapshot is available yet
  (e.g. first request after startup or force-rebuild).
- Cursor encoding now uses URL-safe base64 without padding for cleaner query strings.
- On plugin startup, existing local sessions are bootstrapped into quota state with
  replication-watermark-based backpressure to avoid overloading core nodes.
