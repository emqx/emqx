# EMQX Bcast Plugin -- Developer Guide

This document is for plugin developers. End users should read the
[README](../README.md) and [USAGE.md](USAGE.md) instead.

## Build

```bash
cd plugins/emqx_bcast
MIX_ENV=emqx-enterprise mix do deps.get, emqx.plugin
```

The plugin package is generated under `_build/emqx_enterprise/...`.

## Architecture

```
API Layer (HTTP)
  ├── emqx_bcast_api.erl              -- dispatch by Action
  ├── emqx_bcast_pub_broadcast.erl
  ├── emqx_bcast_batch_pub.erl
  └── emqx_bcast_register_message.erl

ID Layer
  └── emqx_bcast_id.erl              -- UUID v4 ↔ emqx_guid dual-layer mapping

Storage Layer
  └── emqx_bcast_storage.erl          -- Mnesia CRUD, ACK tracking, cleanup

Device Layer
  └── emqx_bcast.erl                  -- hooks, ETS device table, offline replay

Infrastructure
  ├── emqx_bcast_app.erl              -- application lifecycle
  ├── emqx_bcast_sup.erl              -- supervisor
  ├── emqx_bcast_config.erl           -- configuration loading
  ├── emqx_bcast_utils.erl            -- GUID, UUID, SHA-256, Base64, topic expansion
  ├── emqx_bcast_cleanup.erl          -- scheduled expired message cleanup
  └── emqx_bcast_metrics.erl          -- Prometheus counters and gauge (self-managed ETS)
```

## Tests

```bash
# Unit tests
MIX_ENV=emqx-enterprise-test mix test

# CT suite (Docker-based)
scripts/ct/run.sh --app plugins/emqx_bcast
```
