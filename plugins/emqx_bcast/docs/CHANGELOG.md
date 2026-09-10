# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are
documented here.

## Unreleased

### Fixed

- Index rebuild skips devices that already acknowledged (persisted
  `bcast_msg_acked` markers), so a rebuilt index cannot resurrect a device
  and redeliver a duplicate whose ack would decrement the completion
  counter a second time.
- Rebuild ordering is tie-broken by `msg_id` within the same second, so
  per-device FIFO stays deterministic across restarts.
- Ack-in-flight marks expire after 30s when the core-applied confirmation
  is lost, instead of holding the per-device window closed forever.
- Ack markers, completion dedup and the remaining-ack counter are applied
  in one transaction, closing a window where a duplicate ack could
  double-decrement the counter.
- A pool restart releases the restart guard on every shard, including
  shards with no in-flight marks (previously those stayed in
  `pools_restarting` until the 30s watchdog, stalling flush and blocking
  later restarts).
- Index activation keeps healthy active shards intact across a leader
  restart, and individually restarted shards re-activate instead of
  staying dormant.
- Promoter batch crashes converge through bounded retries instead of
  stopping the drain loop.
- Delivery-window commits use a guarded field update instead of rewriting
  the whole per-request row.
- Configured topic templates are validated at config load: wildcards or
  unknown placeholders fall back to the default with a warning instead of
  breaking every publish at runtime.
- The per-device quota clamp logs a warning when the configured value is
  rewritten, so the effective value is visible.

### Changed

- Dropped the never-documented `intake_queue_depth` config surface: the
  intake queue depth is a hardcoded internal bound.
- Ledger gauges are sampled concurrently, so a scrape no longer
  serializes over every index shard.

## 0.4.0

### Metrics contract (0.4.0) - breaking

Prometheus metric names and semantics were reworked around a closed QoS=1
delivery ledger; dashboards and alerts must be updated.

- `bcast_batch_pub_qos1_wanted` is now counted at the **durable mria
  commit** (promotion) per committed logical delivery (request x device),
  not at API acceptance; requests dropped before promotion no longer count.
- New counters: `bcast_batch_pub_qos1_redelivered` (sends with claim
  attempt >= 2), `bcast_batch_pub_qos1_ttl_expired` (TTL expiry without
  confirmation), `bcast_batch_pub_qos1_canceled` (management delete /
  reset without confirmation).
- `bcast_batch_pub_qos1_delivered` now also counts the QoS0-subscription
  auto delivery path; `delivered = first_sends + redelivered`.
- Removed (duplicates of EMQX's own metrics or dead): `fanout_delivered`,
  `node_cpu_use`, `node_memory`, `connections`,
  `batch_pub_qos1_persist_error` (never incremented),
  `batch_pub_qos1_promoted` (folded into `wanted`).
- New gauges (sampled at scrape time, sum over nodes):
  `bcast_intake_depth`, `bcast_batch_pub_qos1_queued`,
  `bcast_batch_pub_qos1_inflight`.
- Ledger identity documented and CT-asserted:
  `wanted = acked + auto_acked + ttl_expired + canceled + queued + inflight`.
- New guarded cluster-wide reset endpoint
  `POST /api/v5/plugin_api/emqx_bcast/metrics/reset` (409 while pending
  deliveries exist; resets every node so cross-node sums stay valid).

### Changed

- BatchPub QoS=1 acceptance is asynchronous: the request is enqueued into
  a bounded node-local intake queue and `200` is returned immediately.
  Acceptance is in-memory (entries queued when a node crashes may be lost
  by contract); the promoter's mria commit is the durability point.
  Promotion failures are retried and counted as
  `bcast_batch_pub_qos1_promote_error`, not returned on the API response.
- Reworked the delivery pipeline into the core/replicant pull model:
  authoritative storage stays on core nodes, while each node pulls
  deliveries for its locally connected devices through dedicated
  `pull_shard`, `ack_shard` and `pull_server_pool` processes. Direct
  process-to-process delivery was replaced by the want_next claim flow.
- The per-device pending index lives in the sharded in-memory state of
  `emqx_bcast_index_owner` (48 shards, rebuilt from `bcast_msg` on
  activation/takeover); the legacy `bcast_msg_index`/`bcast_quota` mnesia
  tables are retained for migration only and are no longer written.
- Storage tables are created through `mria` as `ram_copies` and replicated
  to every core node, so transactions (create, claim, ack) execute locally
  instead of being shipped to a single owner. QoS=1 SLO is in-memory
  acceptance on the core pair, with the subscriber PUBACK as the final
  confirmation: a full cluster restart drops pending deliveries. Existing
  disc copies are converted to ram copies automatically on upgrade.
- QoS=1 delivery and ack metrics are node-local: `bcast_batch_pub_qos1_delivered`
  and `bcast_batch_pub_qos1_acked` increment on the node that delivers/acks,
  so aggregating across all nodes gives the correct totals.

### Added

- `bcast_qos0_delivery_count` metric for QoS=0 one-shot deliveries.

### Removed

- Metrics `bcast_batch_pub_qos0_error`, `bcast_batch_pub_qos0_delivered`,
  `bcast_batch_pub_qos0_skipped`, `bcast_batch_pub_qos1_stored_offline`,
  `bcast_batch_pub_qos1_replayed`, and `bcast_broadcast_pub_devices_online`,
  which had no corresponding behaviour in the pull model.

### Fixed

- `client.ping` hook callback now matches EMQX's fold-hook arity, so pings
  trigger want_next as intended instead of erroring.
- QoS=1 acks are now driven by a single `message.acked` hook, so every
  delivery is accounted for exactly once.
- The message record's `delivery_count` field is no longer maintained:
  messages are garbage-collected by TTL, and the management `DeliveryCount`
  is computed on demand from the message's outstanding delivery rows.
