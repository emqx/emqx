# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are documented here.

## 0.4.1

### Fixed

- The plugin now remembers which devices have acknowledged a delivery. After a
  restart or a core switchover it rebuilds the pending queues from that record,
  so it no longer re-sends to devices that already acknowledged, and a
  duplicate acknowledgement can no longer complete a delivery too early and
  leave the remaining devices without the message.
- If a node crashes between recording an acknowledgement and updating the
  remaining-ack count, the delivery could stay unfinished and its data linger
  until expiry. The next queue rebuild now finishes such deliveries; if that
  attempt fails, the delivery stays queued so it is retried.
- A queue rebuild no longer re-adds deliveries it already finished in the same
  pass, and finishes at most 10,000 of them per rebuild so recovery stays
  bounded; the rest wait for the next rebuild or expiry.
- A device waiting for the core to confirm its acknowledgement no longer gets
  stuck forever: the wait expires after 30 seconds and the message is
  redelivered.
- Index shards that restart now recover on their own instead of staying idle,
  and a core restart only rebuilds the shards that were actually down.
- Restarting the delivery pools (when `delivery_pool_size` changes) no longer
  leaves some shards blocked, which used to pause delivery and block later
  restarts.
- A crash while promoting a batch is now retried a few times instead of killing
  the worker and dropping the whole batch; after the retry limit the batch is
  dropped cleanly and its quota is released.
- Committing a delivery result no longer overwrites unrelated state written at
  the same time (subscriptions, session info, acknowledgement status).
- Deleting a message (which deletes its deliveries) no longer deadlocks against
  a device acknowledgement happening at the same time.
- Pending-delivery quota accounting is more accurate after a rebuild; a late
  update from another node can no longer be counted twice.
- A fresh install now ships a default config that includes every field the
  config schema declares, so the Dashboard config page no longer fails with
  `Found a null value at msg_warn_threshold`.
- Topic templates in the config are validated when the config loads: an invalid
  template falls back to the default with a warning instead of failing every
  publish at runtime.
- The per-device pending-delivery cap now logs a warning when the configured
  value is clamped to 10-200, so the effective value is visible.
- Metrics scraping samples shards concurrently, so one busy shard no longer
  slows the whole scrape.

### Changed

- Removed the `intake_queue_depth` setting, which never took effect and could
  not be set through the config API anyway.

### Documentation

- Corrected `API.md`, `FEATURES.md`, `USAGE.md`, `README.md` and
  `DEVELOPMENT.md`: QoS=0 BatchPub sends only to the nodes hosting the target
  devices (only PubBroadcast reaches every node); `delivery_pool_size` sizes two
  pools (ack workers are limited by the scheduler count); clarified the
  `MessageId` format, the QoS=1 completion condition and where
  `InvalidTopicTemplate` applies.

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
