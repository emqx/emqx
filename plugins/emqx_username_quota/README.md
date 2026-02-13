# emqx_username_quota

This plugin enforces a per-username session quota.

- A counter is maintained for registered sessions per username.
- Authentication is rejected with `quota_exceeded` if a new session would exceed `100`.

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
