# emqx_username_quota

This plugin enforces a per-username session quota.

- Session counters are maintained per username and synchronized cluster-wide.
- Authentication is rejected with `quota_exceeded` when the configured quota is reached.
- Reconnects with an existing `clientid` do not consume additional quota.
- Whitelisted usernames bypass quota checks.

## Configuration

Plugin config fields:

- `max_sessions_per_username` (default: `100`)
- `username_white_list` (default: `[]`)

Update plugin config through the standard plugin config API:

`PUT /api/v5/plugins/<name-vsn>/config`

## Runtime API

The plugin exposes runtime APIs through plugin API gateway:

- `GET /api/v5/plugin_api/emqx_username_quota/quota/usernames`
- `GET /api/v5/plugin_api/emqx_username_quota/quota/usernames/:username`
- `DELETE /api/v5/plugin_api/emqx_username_quota/quota/usernames/:username`

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
