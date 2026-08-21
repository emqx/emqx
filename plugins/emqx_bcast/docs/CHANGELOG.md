# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are
documented here.

## Unreleased

### Changed

- Reworked the delivery pipeline into the core/replicant pull model:
  authoritative storage stays on core nodes, while each node pulls
  deliveries for its locally connected devices through dedicated
  `pull_pool`, `ack_pool`, and `pull_server_pool` processes. Direct
  process-to-process delivery was replaced by the want_next claim flow.
- BatchPub QoS=1 storage and trigger broadcast now run asynchronously on a
  worker pool, so the API returns `200` once the request is accepted.
- Storage tables now keep a disc copy on every core node; transactions
  (create, claim, ack) execute locally instead of being shipped to a
  single owner.
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
