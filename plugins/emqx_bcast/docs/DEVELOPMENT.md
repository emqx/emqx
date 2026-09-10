# EMQX Bcast Plugin -- Developer Guide

This document is for plugin developers. End users should read the
[README](../README.md) and [USAGE.md](USAGE.md) instead.

## Build

From the repository root:

```bash
MIX_ENV=emqx-enterprise make plugin-emqx_bcast
```

or from `plugins/emqx_bcast`:

```bash
MIX_ENV=emqx-enterprise mix do deps.get, emqx.plugin
```

The package is generated at `<repo>/_build/plugins/emqx_bcast-<vsn>.tar.gz`
(`vsn` is read from `plugins/emqx_bcast/VERSION`).

## Architecture

Acceptance and durability (QoS=1):

```
HTTP POST /pub
  └── emqx_bcast_api.erl                 -- routes by Action, forwards to a core node
        ├── emqx_bcast_pub_broadcast.erl -- PubBroadcast (QoS0 fanout)
        ├── emqx_bcast_batch_pub.erl     -- BatchPub QoS0/QoS1, validation, topic resolution
        └── emqx_bcast_register_message.erl -- RegisterMessage create/refresh

emqx_bcast_intake.erl     -- bounded node-local ETS acceptance queue (the 200 path)
emqx_bcast_promoter.erl   -- drains intake, commits to mria, appends index, broadcasts trigger
emqx_bcast_storage.erl    -- mria message/delivery tables, promotion tx, management queries, cleanup
emqx_bcast_id.erl         -- UUID v4 <-> emqx_guid mapping, content-hash message ids
```

Delivery and accounting:

```
emqx_bcast_index_owner.erl    -- 48 core-side shards; authoritative per-device FIFO,
                                 claim/ack/release, admission quota, ledger gauges, rebuild
emqx_bcast_pull_shard.erl     -- per-node device pull shards; per-client state row
                                 (claim round + unacked window + cached subscriptions)
emqx_bcast_pull_server_pool.erl -- core-side worker pool: want_next claims, ack workers,
                                 QoS0 broadcast and QoS1 trigger fanout
emqx_bcast_ack_shard.erl      -- per-node ack accumulation partitioned like pull shards
```

Device and infrastructure:

```
emqx_bcast.erl            -- hooks (connect/subscribe/resume/ping/acked), device registry,
                             core routing helpers, legacy table migration
emqx_bcast_app.erl        -- application lifecycle, config change -> pool restart
emqx_bcast_sup.erl        -- supervisor: pools, pull/ack shards, core-only children
emqx_bcast_config.erl     -- normalizes plugin config (defaults, clamping, topic validation)
emqx_bcast_utils.erl      -- GUID/UUID, SHA-256, Base64, topic expansion, pool helpers
emqx_bcast_cleanup.erl    -- cleanup timer on the cleanup-leader core
emqx_bcast_metrics.erl    -- Prometheus counters + scrape-time gauges
emqx_bcast_mgmt_api.erl   -- messages/deliveries management endpoints
```

Key contracts:

- **Acceptance is in-memory.** A QoS=1 `200` only means the request was
  enqueued in the node-local intake queue; the promoter's mria commit is
  the durability point. Entries queued at a node crash may be lost.
- **Storage is `ram_copies`.** Every core keeps a copy so transactions run
  locally, but a full cluster restart drops pending deliveries.
- **The per-device index is derived state.** `emqx_bcast_index_owner`
  shards own it in their process heaps and rebuild it from `bcast_msg` on
  activation/takeover; the legacy `bcast_msg_index`/`bcast_quota` mnesia
  tables are migration-only.
- **Counters are node-local and eventually consistent**, anchored at the
  durable commit; the ledger identity is
  `wanted = acked + auto_acked + ttl_expired + canceled + queued + inflight`.

## Tests

```bash
# Unit tests
MIX_ENV=emqx-enterprise-test mix test

# CT suite (Docker-based)
scripts/ct/run.sh --app plugins/emqx_bcast
```
