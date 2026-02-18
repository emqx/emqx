# UNS Gate

This plugin enforces Unified Namespace topic structure at ACL check time.

## Plugin API

Base path: `/api/v5/plugin_api/emqx_uns_gate`

- `GET /status`
- `GET /model`
- `GET /models`
- `GET /models/:id`
- `POST /models` (create or update model; optional `activate`)
- `POST /models/:id/activate`
- `POST /validate/topic`

## Build

From repository root:

```bash
make plugin-emqx_uns_gate
```

## Test

From repository root:

```bash
make plugins/emqx_uns_gate-ct
```
