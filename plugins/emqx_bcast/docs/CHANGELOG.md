# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are documented here.

## 0.4.1

A reliability release for QoS=1 batch publish and broadcast. Durable
acknowledgements survive a queue rebuild, a client reconnect no longer crashes
index shards, a deleted message stays deleted, and quota and metric accounting
is more accurate. **No breaking API change and no
change to what the Prometheus counters mean** — but several of the upgrade notes
need action.

### Upgrade notes

- **Action required: re-apply your plugin settings after upgrading.** Plugin
  configuration is stored per plugin version, so the settings on the 0.4.0
  Dashboard page move to a new `emqx_bcast-0.4.1` namespace and are **not**
  carried over.
- **Action required if affected: an upgraded node can still show `Found a null
  value at msg_warn_threshold` on the config page.** A fresh install now ships a
  complete default config (the legacy `msg_warn_threshold`, `force_upgrade_qos`
  and `delivery_queue_max` fields were missing), but a config already stored on
  a node is left untouched. Add those three fields to that node's plugin config
  to clear the error; they have no runtime effect.
- **Action required: finish or let expire every incomplete QoS 1 batch before
  upgrading.** Version 0.4.0 kept no per-device acknowledgement markers, so the
  first rebuild cannot tell which devices of a delivery already acknowledged it:
  it restores every target of a delivery 0.4.0 left incomplete, and the devices
  that already acknowledged it receive the message again. Clients must tolerate
  that one duplicate delivery across the upgrade. 0.4.1 also restores the
  remaining-acknowledgement count of such a delivery to its full target before
  indexing it again, so the duplicate acknowledgement can no longer complete the
  delivery early and drop the messages of the devices that had not acknowledged
  it yet — draining or expiring those batches first is still the only way to
  upgrade without any duplicate delivery.
- **`msg_ttl` is not applied retroactively.** Lowering it does not expire
  messages that are already stored: each one keeps the expiry it was written
  with. Only re-publishing the same content refreshes it.
- **New tables to monitor: `bcast_msg_acked` and `bcast_msg_epoch`.** The
  first is a Mnesia `bag` recording which devices acknowledged each incomplete
  delivery; the second is node-local ETS recording the delete generation of
  each content hash that has been deleted, and grows by one row per distinct
  deleted content. Both live in memory like the rest.
- **While any core does not run this plugin version, Delete Message fails with
  an error and has to be retried afterwards.** That covers a core still on
  0.4.0 and a core whose plugin is uninstalled in the middle of an upgrade: a
  delete advances the delete generation on every running core before it removes
  any row, and a core without the plugin cannot serve that call, so the whole
  delete is aborted instead of running with a weaker guarantee. Such an attempt
  does leave the advanced generation behind on the cores it reached, so a batch
  admitted before the delete is dropped as stale rather than re-creating the
  message.
- **While a core's plugin is uninstalled, the index shards it owns pause.** The
  node stays in the core set, so calls routed to it fail and are either retried
  or answered as not active; its partitions resume when the plugin is installed
  again and rebuild from the committed rows.
- **After the plugin is reinstalled, already-connected devices resume delivery
  at their next keepalive plus the next publish for their product key.** The
  plugin registers devices from client hooks and does not scan existing sessions
  at startup, while reinstalling it does not touch the MQTT connections. So a
  device that stays connected is picked up again by its keepalive (which
  registers it) and the next publish (which claims its backlog); a product key
  with no further publishes waits for the device to reconnect, or for the
  message TTL. Reconnecting after the upgrade resumes delivery immediately.
- **Rolling upgrade order: upgrade the nodes that hold client connections
  first, then the core nodes that own index shards.** The claim-release message
  0.4.0 sends is now accepted instead of crashing the receiving shard, and a
  claim answer in the new shape is only sent to a shard that announces it
  understands it, so a mixed cluster no longer crashes index or pull shards.
- **Downgrading to 0.4.0 is data-safe but drops the fixes.** 0.4.0 ignores the
  table 0.4.1 adds and the counters stay consistent, but it again crashes index
  shards when a client reconnects.
- **Message and index data live in memory (`ram_copies`), as before.** Losing a
  single core is tolerated; a full cluster restart is not, and pending
  deliveries and their payloads are gone after one. This was always the case and
  is now documented.

### Compatibility

- **API:** no endpoint, parameter or response format changed. The one visible
  addition is on `POST /api/v5/plugin_api/emqx_bcast/metrics/reset`: a reset that
  fails on some nodes now answers `500 PartialReset` — naming the nodes already
  zeroed and the ones that failed — instead of reporting success. Treat it as a
  retryable failure.
- **Prometheus:** counter and gauge names are unchanged, and so is what each one
  counts. What changed is the documentation of **where** each counter is
  counted, which matters when you sum across nodes:
  - `in`, `enqueued`, `intake_rejected`, `promote_error`, `wanted` and
    `intake_depth` are counted on the node that accepted and committed the
    request. A **replicant reports 0** for these, because it forwards to a core.
  - `delivered`, `redelivered`, `acked` and `auto_acked` are counted on the node
    that served the device.
  - `queued`, `inflight`, `ttl_expired` and `canceled` are counted on the
    **core** that owns the device's index shard, or that runs the delete or
    cleanup for it.
  - `wanted` counts every device of each batch the node committed, so the ledger
    identity holds for the sum over nodes, not for one node on its own.
- **Config:** the public config schema is unchanged. The runtime-only
  `intake_queue_depth` default was removed; it was never exposed through the
  config API, so it always used its default value. An invalid `broadcast_topic` or `batch_topic` template is now
  rejected when the config loads, with a warning, and falls back to the default
  — instead of failing every publish at runtime.

### Fixed

**A batch published before its targets connect reaches them when they arrive**

- Publishing a batch while its target devices are still offline could leave a
  share of them (measured: 1.5% of an 800k-device run) with nothing delivered
  for as long as the message TTL — 15 days by default — even though those
  devices were online, subscribed, and their messages were queued and waiting.
  The promote signal had already gone out while no device was connected, and
  the device's own subscribe was refused once the per-shard claim backpressure
  limit was reached; that refusal was dropped rather than retried, so nothing
  ever woke the device up again.
- A refused claim is now remembered and retried by the periodic sweep, so a
  device that subscribes during a large connect burst still receives its
  queued messages on a following sweep, without republishing. One sweep pass
  retries a bounded number of clients, so a burst larger than that drains over
  several passes instead of one. The mark lives on the
  client's own state, so a severe backpressure episode cannot overflow it and
  drop the wake-up the way a bounded retry queue would. The retry only contacts
  clients that are still online, and the sweep logs the waiting backlog when it
  retries them, so a claim refused by the limit is visible on the following
  pass instead of staying silent; a claim that merely found nothing yet is
  logged at debug level.
- A claim the core answers as "nothing to deliver right now" is no longer
  trusted blindly. That answer also comes back while a device still has queued
  messages that are momentarily not deliverable — its subscription was not yet
  visible when the attempt ran, or the message copy had not replicated yet —
  and it used to clear the attempt without scheduling another one, leaving the
  device idle until the TTL. The core now reports how many messages are still
  waiting, and the plugin retries those devices from the same periodic sweep.
  In a measured 8M-message run this was the difference between 7,999,990 and
  8,000,000 acknowledgements.
- A device that subscribes is re-checked once its subscription is committed,
  so a claim attempt that raced the subscription commit cannot leave a device
  idle with a backlog.

**Queue rebuilds preserve durable acknowledgements and remaining deliveries**

- The plugin now records which devices acknowledged each delivery and uses that
  record when it rebuilds its queues: a device whose acknowledgement marker is
  already persisted is not added back to the rebuilt queue, so a restart or a
  core switchover does not send it the message again. Delivery stays
  at-least-once — an entry that was not promoted yet, or an acknowledgement
  whose marker was not persisted yet, can still be lost or repeated, and a full
  cluster restart clears the pending data.
- A node that crashes between recording an acknowledgement and updating the
  remaining-ack count no longer strands the delivery: the next rebuild finishes
  it, and if that attempt fails the delivery stays queued and is retried. A
  fully acknowledged delivery can no longer sit unfinished until expiry.
- A delivery inherited from a build that kept no acknowledgement markers
  (`0.4.0` and earlier) can no longer complete early and lose the messages of
  the devices that had not acknowledged it. Its remaining-acknowledgement count
  was decremented by acknowledgements this build cannot attribute to devices,
  while the rebuild asks every one of its target devices to acknowledge again;
  the count is now restored to the full target before those devices are indexed,
  so the first duplicate acknowledgement cannot finish the delivery while the
  other devices are still pending.
- A rebuild does not re-add deliveries it already finished in the same pass, and
  finishes at most 10,000 per pass, so recovery time stays bounded.
- A device waiting for the core to confirm its acknowledgement no longer waits
  forever: the wait expires after 30 seconds and the message is redelivered.

**A client reconnect no longer takes the plugin down**

- Releasing claims handed the shard entries in a shape it rejected, so a plain
  client reconnect — with no restart and no upgrade — crashed the same index
  shard repeatedly: each crash restarts that shard, and eleven crashes within an
  hour stop the whole plugin. The release now delivers the shape the shard
  expects. **Version 0.4.0 carries the same defect**, so this is the reason to
  upgrade if you have seen index shard restarts, or the plugin stopping, when
  devices reconnect.

**A deleted message stays deleted**

- A message you delete can no longer reappear because a batch publish was in
  flight against it: such a publish is dropped instead of re-creating the
  message, and a failure to delete a late delivery is now reported instead of
  being returned as success.
- Deleting a message with a very large number of deliveries no longer holds one
  unbounded Mnesia transaction and the index shard for the whole sweep: the
  sweep runs in bounded chunks instead. A device whose turn has not come yet
  can still receive the message after the delete call has returned, so treat
  Delete Message as stopping future deliveries, not as recalling messages
  already in flight.
- Deleting a message no longer deadlocks against an acknowledgement arriving at
  the same time, and no longer leaves an orphaned delivery behind when a publish
  reuses the same message.

**Quota and metric accounting is more accurate**

- Pending-delivery accounting is consistent again after a rebuild and while
  intake is backlogged: reservations are no longer dropped from the recount, a
  late update from another node is no longer counted twice, and the quota
  self-heals on a timer.
- On an active shard, concurrent requests for one device can no longer both
  pass a stale check and push it past `max_pending_deliveries_per_device`. The
  cap remains best-effort while a shard is unavailable, during startup or
  takeover, and the effective value is logged when a configured one is clamped
  to the 10-200 range.
- `canceled` is no longer undercounted when one index shard fails during a
  delete: the entries removed by the reachable shards are counted, and only the
  failed shard is retried. A shard that still fails after that retry can leave
  `canceled` short, and nothing re-adds to it afterwards.
- A cluster-wide metrics reset checks every node before resetting any, so an
  already-busy node no longer causes a reset to start. A node that becomes busy
  in the small window between that check and the reset is reported as
  `500 PartialReset` instead of a silent success.

**Fan-out no longer stalls, and scrapes stay responsive**

- The QoS=1 acknowledgement flush was reworked: it wrote through a single
  heavily shared row, so at high fan-out it stalled the index shards behind it
  and collapsed broadcast throughput. The write is now lock-free, and a flush no
  longer re-reads a delivery's whole acknowledgement set.
- Metrics scraping samples shards concurrently, so one busy shard no longer
  slows the whole scrape.

**Restarts and crashes recover cleanly**

- Index shards that restart recover on their own instead of staying idle, and a
  core restart rebuilds only the shards that were actually down.
- A core joining or leaving remaps which core owns which index shard, so an
  active shard now gives its partition up as soon as it is no longer the owner
  and rebuilds it when the node becomes the owner again. Before this, a former
  owner kept serving an index that had stopped being updated: entries added in
  the meantime were missing, and acknowledgements it had already applied were
  counted a second time.
- The notes above are about index shards. A delivery (pull) shard that crashes
  loses the in-memory state of the clients it served; those devices are
  recovered by the next publish that targets them, by reconnecting and
  re-subscribing, or by the message TTL, whichever comes first. A keepalive
  ping deliberately does not rebuild that state.
- Restarting the delivery pools — which happens when `delivery_pool_size`
  changes — no longer leaves shards blocked and delivery paused.
- A crash while promoting a batch is retried a few times instead of dropping the
  whole batch; after the retry limit the batch is dropped cleanly and its quota
  released.
- Committing a delivery result no longer overwrites unrelated state written at
  the same time, such as subscriptions, session information or acknowledgement
  status.

### Changed

- Maintenance that used to fail silently now reports what it did: table
  copy-type changes and the check that drives them, which messages and completed
  deliveries the TTL sweep reclaimed, management message deletion with its
  delivery count, and the per-table size and copy type at startup. Degraded
  admission after an owner failure, a failed per-device quota probe and a failed
  local quota reset are logged at error level. A table change that is not one of
  these paths is therefore visibly external, which makes unexpected table
  changes attributable.

### Documentation

- Corrected `API.md`, `FEATURES.md`, `USAGE.md`, `README.md` and
  `DEVELOPMENT.md`: a QoS=0 BatchPub sends only to the nodes hosting the target
  devices (only PubBroadcast reaches every node); `delivery_pool_size` sizes two
  pools, with ack workers capped by the scheduler count; and the `MessageId`
  format, the QoS=1 completion condition and where `InvalidTopicTemplate`
  applies are now stated precisely.
- Documented the acknowledgement window: an acknowledgement is counted when it
  is matched, before its marker is persisted by the next flush, which is
  scheduled after 50 ms and may be delayed under load.

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
