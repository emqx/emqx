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
emqx_bcast_id.erl         -- emqx_guid <-> API UUID mapping, content-hash derived message ids
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

## Config compatibility

`priv/config_schema.avsc` and `priv/config.hocon` still declare three settings
that drive no behaviour: `msg_warn_threshold`, `force_upgrade_qos` and
`delivery_queue_max`. Their features were removed in the 0.2.0 delivery
redesign (`msg_warn_threshold` lost its consumer when the pending-delivery
quota replaced the warning threshold). **Keep the declarations**:

- A config stored by an older plugin version still carries those names, and the
  schema validation that runs while such a config is decoded rejects a field the
  schema no longer declares, so the plugin fails to load after an upgrade.
  `delivery_queue_max` is the one an 0.1.1-era config sets.
- The dashboard plugin config page requires every field the schema declares to
  be present in the config it serves, and does not fall back to the avsc default
  for a missing field, so the defaults file has to ship them as well.

They are declared without `$ui`, which hides them on the config page, and
`emqx_bcast_config:normalize/1` accepts `msg_warn_threshold` without using it.
`t_default_config_covers_schema` in `emqx_bcast_SUITE` guards both directions.

## API request budget

Plugin endpoints are served through `/plugin_api/:plugin/[...]`, and the
framework runs each callback under its own budget:
`plugins.api_endpoint.timeout`, **5 seconds by default**. When that expires the
framework kills the callback and answers `503` — while the plugin's request may
still be running with nobody to answer it, and with any quota reservation it
took left behind until the stale-reservation sweep.

So an RPC the plugin issues *inside a request* has to finish well before that
budget, and the plugin has to answer for itself:

- `emqx_bcast_utils:api_budget_ms/0` reads the live setting (with the legacy
  `plugins.api_gateway.timeout` fallback the framework uses) and
  `api_rpc_timeout_ms/0` derives the per-call budget from it, leaving room to
  build the answer.
- The replicant-to-core API forward, the admission legs (global reserve plus the
  per-device shard calls) and the cluster-wide metrics reset use it. A reset
  also runs its per-node calls together, so its cost is one per-node timeout
  rather than one per node, per phase.
- Everything no request waits for — maintenance sweeps, table waits, copy-type
  checks — is not bound by it and uses the plugin's own budgets
  (`?BCAST_RPC_CALL_TIMEOUT_MS` and friends in `include/emqx_bcast.hrl`), where
  a slow cluster may take seconds and nothing is lost by waiting.

Management reads follow the same rule and read through indexes rather than
scanning: `bcast_msg` carries a secondary index on `msg_id` for a message's
outstanding deliveries, and `bcast_message_order` orders the message list by
`{created_at, msg_id}` so a page costs its own size. `t_admission_fits_the_endpoint_budget`,
`t_metrics_reset_fits_the_endpoint_budget` and `t_management_reads_use_indexes`
guard this.

## Tests

```bash
# Unit tests
MIX_ENV=emqx-enterprise-test mix test

# CT suite (Docker-based)
scripts/ct/run.sh --app plugins/emqx_bcast
```
