# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are
documented here.

## Unreleased

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

- Reworked the delivery pipeline into the core/replicant pull model:
  authoritative storage stays on core nodes, while each node pulls
  deliveries for its locally connected devices through dedicated
  `pull_pool`, `ack_pool`, and `pull_server_pool` processes. Direct
  process-to-process delivery was replaced by the want_next claim flow.
- BatchPub QoS=1 storage now completes before the API returns; only the
  per-node trigger broadcast is asynchronous. A `200` means the delivery is
  durable, and storage failures are returned to the caller as `500`.
- Storage tables now keep a disc copy on every core node; transactions
  (create, claim, ack) execute locally instead of being shipped to a
  single owner.
- Storage tables are created through `mria` as `ram_copies` (no disk
  persistence): QoS=1 SLO is in-memory acceptance on the core pair, with
  the subscriber PUBACK as the final confirmation. A full cluster restart
  drops pending deliveries; existing disc copies are converted to ram
  copies automatically on upgrade.
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
- The `delivery_count` field on the message record replaces a full-table
  scan on delivery completion, removing an O(n) step from the ack path.
