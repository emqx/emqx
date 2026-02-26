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
- Removed `username_white_list` from config, config schema, and i18n.
