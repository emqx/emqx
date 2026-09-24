# Changelog

All notable changes to the emqx_bcast plugin since version `0.1.0` are documented here.

## Unreleased

A follow-up to 0.4.1 for the subscription hooks and for the promoter's
index-append retry. **No breaking API change, no configuration change and no
change to what the existing Prometheus counters mean** (one counter and one
gauge are added). Verified end to end on a 10,000,000-message QoS=1 backlog run:
deliveries and acknowledgements both reached 10,000,000, so nothing was left
delivered without being acknowledged.

### Upgrade notes

- **Behaviour change: unsubscribing no longer drops a client's in-flight
  messages.** 0.4.1 released every unacknowledged delivery of that client on
  `UNSUBSCRIBE` and deleted its cached filters, so unsubscribing from one filter
  could make an in-flight message of another be delivered again. The plugin now
  follows the session (`session.unsubscribed`): the filter leaves the cache and
  the deliveries already handed to the client keep draining until the client
  acknowledges them or they expire with `msg_ttl`. To stop delivery to a device,
  use the management `DeleteMessage` API.
- Nothing else needs preparing: no new table, no new config field, and every
  hook this release changes is registered by the plugin itself on start.

### Compatibility

- **API and metrics:** unchanged. No endpoint, parameter, error code or
  Prometheus counter name or meaning differs from 0.4.1.
- **Mixed cluster:** the release changes only which hooks a node registers and
  how it reads a subscription's QoS, so nodes still on 0.4.1 keep working next
  to it — the 0.4.1 node still arms a client before the subscription is
  committed (a claim round that can only come back empty), and the 0.4.2 node
  only records a QoS it actually knows. No state is written in a new way, so the
  order of an in-place upgrade does not matter.
- **Rollback:** safe. This release adds no persistent state.

### Fixed

- **A client reconnect can no longer turn a QoS=1 delivery into a QoS 0 one.**
  EMQX records a subscription in two steps — the subscriber's topic list first,
  its options second — and a read in between returns the topic without any
  options. The plugin read a missing QoS as `0`, so a delivery claimed inside
  that window was published at QoS 0 and self-acknowledged: the device was never
  asked for a PUBACK, and that one message had no retransmission behind it. A
  missing QoS is now read as "not QoS 0": the plugin keeps the QoS it last knew
  for that filter, and a resumed session no longer clears the filters it knew
  when a re-sync comes back without options. Seen in an 800k-device backlog run
  as 1 of 8,000,000 messages (3 of 16,000,000 over two runs), where
  `messages.qos0.sent` and `batch_pub_qos1_auto_acked` both showed that number
  while `batch_pub_qos0_in` stayed 0.
- The plugin no longer uses the pre-commit `client.subscribe` and
  `client.unsubscribe` hooks. They ran before the change they describe was
  visible: the subscribe hook opened a claim round that could only come back
  empty, and the unsubscribe hook released every unacknowledged delivery and
  dropped the client's cached filters, so unsubscribing from one filter could
  make an in-flight message be delivered again. The post-commit
  `session.subscribed` / `session.unsubscribed` hooks cache and drop filters and
  re-arm the client, and `session.resumed` re-syncs a restored session.
- **A core that starts after the storage tables already exist keeps a local
  copy of the management order index.** The copy-repair pass carried its own
  table list, which made it repair eight of the nine storage tables: a core
  whose plugin started after the table was created in the cluster schema was
  left without its local copy of `bcast_message_order`, and the message list on
  that core fails on it (the list pages through the local table). The pass now
  derives its list from the table definitions, so a table cannot be created
  without also being repaired.
- **A QoS=1 delivery to a QoS=0 subscription no longer self-confirms itself on a
  stale channel.** That path sends the message and immediately acknowledges it
  (no PUBACK is expected), but unlike the other delivery paths it did not
  re-check that the client still owns the channel pid it was claimed for. A
  takeover between the claim and the send therefore delivered to the old
  channel, counted the delivery as delivered, and removed it from the index —
  the current session never received it. It now releases the claim instead, so
  the new session is handed the delivery.
- **A message extended by a re-send is no longer deleted by an expiry scan that
  ran before it.** The TTL scan is a snapshot; the deletes that follow it were
  unconditional, so a concurrent create/refresh of the same content (a
  `RegisterMessage`, a `BatchPub` that resolves to the same hash) could have its
  payload, hash and API-id rows removed with it, leaving the delivery of the
  refreshed message without a body. Each delete is now an equality delete of a
  record read a moment earlier, so a row that changed in between does not match
  and stays, and the derived rows carry this message's own ids, so a refresh
  that wrote new ones cannot be hit either. It stays lock-free on purpose: a
  transaction here would take the message and hash write locks that promotion
  needs first and hold them while its commit queues behind the Mnesia
  transaction manager.
- **A slow role lookup during startup no longer pins a core into the
  non-core layout.** The role probe retries briefly before it answers, so a
  node whose plugin loads before mria publishes its role does not skip every
  core-only table and worker for the rest of its life; a role that never
  resolves still falls back to replicant, and the request path never waits.
- **A batch whose index append keeps failing no longer pins a promoter worker.**
  Promotion commits the delivery rows first and appends the per-device index
  afterwards; when that append failed, the promoter retried the same batch in
  place without a limit. One index shard that stays dormant or unreachable
  therefore pinned one worker per affected batch - and every batch that
  contained a single device of that shard failed as a whole. On a node with
  `schedulers_online` workers, a handful of such batches stopped the intake
  queue from draining while the API kept answering 200: accepted requests sat
  in the queue, their devices received nothing, and the only visible symptom
  was an unbroken stream of `bcast_promoter_append_failed_retry` warnings
  (6.3 million lines over 21 hours in one production incident). The retry is now
  bounded: 10 attempts 5ms apart in the worker, then the batch goes back to the
  intake queue with a doubling backoff (100ms, capped at 30s) and the worker
  returns to the queue. **Nothing is dropped** - the batch is already committed
  and is retried until the append succeeds - so the worst case for those
  devices is a delayed delivery, not a lost one. The new
  `bcast_batch_pub_qos1_append_deferred` counter and `bcast_intake_deferred_depth`
  gauge report it, and each deferral logs `bcast_promoter_append_deferred` at
  warning.

### Changed

- **Diagnostic logs that used to be `info` are now `warning`**, so the default
  file logger records them: the startup table baseline (`bcast_tables_ready`),
  a table copy-type repair (`bcast_table_copy_type_changed`), an index owner
  activation (`bcast_index_owner_activated`), a shard rebuild
  (`bcast_index_shard_rebuilt`), an accepted management delete
  (`bcast_message_delete_requested`), and the expiry/reclamation summaries
  (`bcast_messages_expired`, `bcast_expired_deliveries_reclaimed`,
  `bcast_completed_deliveries_reclaimed`). Every one of them fires on a state
  change or an operation, never per message, and each answers a question that
  a field investigation in this release cycle had to answer without the logs
  (was the partition activated, was it rebuilt, when were rows deleted).
- **Acknowledgement bookkeeping is per delivery part, not per flush tick.** One
  shard owns the devices one delivery has on that shard: an acknowledgement only
  moves that shard's local counters, and the two Mnesia writes (the acked-device
  marker, then the remaining-ack decrement) happen once, when the part has
  nothing left to acknowledge. Both tables are kept on every core, so the
  previous per-tick flush made the cross-core write rate follow the
  acknowledgement rate - at fanout scale the 50ms tick alone reached roughly
  that rate per node, towards three peers, and the Mnesia queues behind it
  slowed every Mnesia user on the node, this plugin's own delivery and
  completion included. The write count is now bounded by parts, not by time:
  deliveries x shards for a whole run, however long the drain takes. The marker
  is still written before the decrement and only once per part, so a part that
  already reported is never counted twice, and a delivery completes as soon as
  its last part reports (no flush interval in between). Acknowledgements that
  arrive before their part completes are not written at all: a node that dies
  then only causes those devices to be delivered again (at-least-once), because
  the decrement for that part has not happened either and the acknowledgement is
  never counted twice.

### Known issues

- **Clients reconnecting in large numbers.** Every connect and disconnect writes
  the replicated channel registry on every core, so a core's Mnesia transaction
  manager can fall behind and slow down every Mnesia user on that node,
  including this plugin's claim and acknowledgement bookkeeping. Nothing is
  lost; the drain rate drops.
- **A large device population draining its backlog.** Every message is its own
  claim round, so the core-side claim pool works in very small tasks and its
  workers queue in the hundreds. This delays claims instead of losing
  deliveries.
- **A high API request rate with few devices per request.** Acknowledgement
  bookkeeping is replicated to every core, so its cost follows the request rate
  rather than the number of messages.
- **A core that keeps its share of the index shards while it cannot serve them**
  (it runs without this plugin, or stays unreachable for a long time). Its
  devices are delivered late: the batches committed for them wait in the intake
  queue and are retried (up to 30s apart) until the core is back. Nothing is
  lost, and the waiting batches no longer hold a promoter worker.

## 0.4.1

A reliability release for QoS=1 batch publish and broadcast. Durable
acknowledgements survive a queue rebuild, a client reconnect no longer crashes
index shards, a deleted message stays deleted, quota and metric accounting is
more accurate, and an upgrade no longer strands a node's partitions or holds its
index shard while the rest of the cluster is unreachable. **No breaking API
change and no change to what the Prometheus counters mean** — but several of the
upgrade notes need action.

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
  deleted content. Both live in memory like the rest. `bcast_message_order`
  joins them: one row per message, ordered by its creation, which the
  management list pages through. It is derived state — it is written with the
  message and rebuilt from the message table if a node starts with it empty.
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
  claim answer in the new shape is only sent to a pull side that announced it
  understands that shape, so a mixed cluster no longer crashes index or pull
  shards. An upgraded node does not wait for the rest of the cluster either: it
  activates the index partitions it can, and the shards of a core that still
  runs an older build (or has the plugin uninstalled) are rebuilt when that
  core is upgraded. Until every partition answers again the cluster-wide
  pending-quota recount pauses — it refuses to count a partial cluster — so the
  pending total keeps following deltas without being re-based from live shard
  state for the length of the upgrade.
- **Downgrading to 0.4.0 is data-safe but drops the fixes.** 0.4.0 ignores the
  table 0.4.1 adds and the counters stay consistent, but it again crashes index
  shards when a client reconnects.
- **Message and index data live in memory (`ram_copies`), as before.** Losing a
  single core is tolerated; a full cluster restart is not, and pending
  deliveries and their payloads are gone after one. This was always the case and
  is now documented.

### Compatibility

- **API:** no endpoint or parameter changed. Two error codes are new, because
  the old ones described a different failure than the one being reported:
  - `400 MessageContentRequired`: `PubBroadcast` sent without a `MessageContent`.
    It used to answer `InvalidBase64`, which now means only "the payload failed
    to decode".
  - `400 InvalidMessageId`: a `MessageId` that is not a UUID (including a
    non-string). It used to answer `MessageNotFound`, which now means only "a
    well-formed id that does not exist".
  - A reset that fails on some nodes answers `500 PartialReset` — naming the
    nodes already zeroed and the ones that failed — instead of reporting
    success. Treat it as a retryable failure.
- **API, returned values:** a QoS=0 batch that reuses a `MessageId` answers the
  id as it is stored rather than echoing the string it was given. The two are
  the same for a canonical UUID, and differ only where a non-canonical one used
  to come back in a form no later request could match. A management `GET`
  whose storage read fails answers `500 InternalError` instead of failing on an
  unhandled match.
- **API, request budget:** every RPC the plugin issues inside a request now
  finishes within the framework's own budget (`plugins.api_endpoint.timeout`,
  5 seconds by default), so a request that cannot be served answers with the
  plugin's error instead of being killed by the framework with a `503` while its
  work continues in the background. Paging the message list and counting a
  message's deliveries no longer scan whole tables per request either — they
  read through indexes — and a cluster-wide metrics reset runs its per-node
  checks and resets together instead of one after another, which is what used to
  let it outlive the budget.
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
- **Config:** no field was added, removed or renamed. The numeric settings now
  declare the bounds the plugin applies — nothing below zero for the size and
  quota limits, and the enforced `10`–`200` window for
  `max_pending_deliveries_per_device` — so the config page cannot offer a value
  the plugin would override. The plugin keeps applying those bounds itself: a
  value already stored on a node is not checked against the declaration when it
  is read. The runtime-only
  `intake_queue_depth` default was removed; it was never exposed through the
  config API, so it always used its default value. An invalid `broadcast_topic` or `batch_topic` template is now
  rejected when the config loads, with a warning, and falls back to the default
  — instead of failing every publish at runtime. A negative numeric limit is
  ignored in favour of its default, with a warning (`max_message_size_batch =
  -1` used to reject every publish); zero is kept, and means "allow nothing".

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

**Restarts, crashes and maintenance recover cleanly**

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
- A cleanup pass no longer removes a delivery from the index just because the
  message row it looks up is not visible on that node yet — a row written by
  another core may still be replicating. The entry used to disappear, and the
  device never received that message before it expired.
- A failure while an index shard applies a buffered acknowledgement no longer
  takes the shard down (with the pending index of every device it held): the
  marker write and the counter decrement are idempotent, so the entry stays
  buffered and the next attempt applies it.

**Delivery survives a rolling upgrade, and the retry that rescues a stranded
device no longer takes its own shard down**

- The sweep that retries a device whose claim round was refused read a field the
  shard does not keep, so a pass with a stranded device killed that shard —
  together with the client state of every device it was serving — and the next
  stranded device did it again. The retry now uses the state the shard actually
  keeps.
- A node that had already been upgraded could not finish activating its
  partitions while a core still ran a version that does not answer the
  activation status call, and it re-scanned the whole message table every 50 ms
  for as long as that core stayed un-upgraded. Its own partition stayed dormant
  meanwhile, so the devices on it received nothing for the whole upgrade window,
  up to the message TTL. The node now serves the partitions it can rebuild,
  keeps retrying the ones it cannot reach on a slow poll, and scans the message
  table only when a partition actually has to be rebuilt from it.
- A rebuild no longer pays one activation timeout per unreachable partition
  while it occupies the index shard that coordinates it. The partitions are
  probed together, with a timeout sized for a probe rather than for a rebuild,
  and a peer that does not answer is left alone for a few seconds instead of
  being waited on again in the next pass and in every pass after it. The devices
  of that coordinator's own partition therefore wait about one probe timeout per
  pass, instead of one activation timeout per unreachable peer, while the rest
  of the cluster is unreachable — during a rolling upgrade, for instance, where
  the cores that still run an older build or have the plugin uninstalled are
  exactly the peers that do not answer (measured: eight unreachable partitions
  went from about 40 seconds to about 1 second per pass).
- A partition that cannot be reached no longer makes the activation leader
  re-drive the cluster: the drive stops at it and leaves it to that partition's
  own activation request. The leader used to keep re-driving, and the moment
  the peer came back it completed a whole drive, whose closing step re-bases the
  authoritative pending-delivery row from the live per-shard counts. That
  re-basing had nothing to do with the traffic at that moment, so the reported
  pending total could jump whenever a hung peer recovered.
- A rebuild no longer occupies the index shard that coordinates it. Probing the
  partitions, scanning the message table and handing each partition its slice
  run in a short-lived process of their own, and the coordinating shard only
  loads its own 1/48 of the messages and closes the pass. Its devices therefore
  keep being served while a rebuild runs — measured on a 3,000-delivery
  backlog, the longest request it answered during a rebuild went from about
  47 ms (it waited for the whole drive) to microseconds, while the drive itself
  also got shorter because each partition is handed only the rows it owns. Only
  one rebuild runs at a time; one requested while another is running joins it,
  and a rebuild still answers its caller when the rebuild is done.
- A manual rebuild is best effort: it answers success for the partitions it
  reached, names the ones it could not (a warning lists them, together with the
  peers it is holding back), and does not leave the automatic retry armed
  against a peer that is still unreachable. Run it again once that peer is back
  — after five seconds it is probed again.
- A claim worker that read the subscription cache of a client row deleted at
  that instant no longer dies together with every other claim in its batch; the
  cache is a hint, and the next claim reads the subscriptions again.
- `msg_ttl` and `cleanup_interval` accept the duration forms the rest of EMQX
  accepts — `1h30m`, `500ms`, `0.5s` — instead of only `<digits>[smhd]`. A value
  under a second rounds up to one second, and a bare number still means seconds.
- Changing `delivery_pool_size` no longer risks holding the config save open:
  the per-shard snapshot it waits for is bounded, and a shard that does not
  answer in time aborts the restart with a warning instead of blocking.
- The periodic sweep that revisits a device whose claim round was refused
  tolerates only one kind of failure, so any other error it ran into restarted
  the delivery shard and dropped the client state of every device on that
  partition — window, acknowledgement marks and the very retry mark the sweep
  exists to revisit. It now reports the failure and keeps serving, and the
  retry marks it did not get to are retried by a later pass.

### Changed

- Maintenance that used to fail silently now reports what it did: table
  copy-type changes and the check that drives them, which messages and completed
  deliveries the TTL sweep reclaimed, management message deletion with its
  delivery count, and the per-table size and copy type at startup. Degraded
  admission after an owner failure, a failed per-device quota probe and a failed
  local quota reset are logged at error level. A table change that is not one of
  these paths is therefore visibly external, which makes unexpected table
  changes attributable. A rebuild that could not reach a partition names the
  partitions it missed, and a periodic sweep that fails reports the failure
  instead of letting the shard restart silently.

### Documentation

- Corrected `API.md`, `FEATURES.md`, `USAGE.md`, `README.md` and
  `DEVELOPMENT.md`: a QoS=0 BatchPub sends only to the nodes hosting the target
  devices (only PubBroadcast reaches every node); `delivery_pool_size` sizes two
  pools, with ack workers capped by the scheduler count; and the `MessageId`
  format, the QoS=1 completion condition and where `InvalidTopicTemplate`
  applies are now stated precisely.
- Documented the acknowledgement window: an acknowledgement is counted when it
  is matched, before its marker is persisted by the next flush, which is
  scheduled after 50 ms and may be delayed under load. `API.md` also states the
  consequence: an index shard that dies inside that window can count one
  duplicate PUBACK of a redelivered message once more, while delivery itself
  stays at-least-once.
- `API.md` now states the delete contract: a management delete removes nothing
  and answers `500 InternalError` when a core cannot serve the delete generation
  advance, and a device whose delivery was already in flight can still receive a
  copy after the call returns.
- `API.md` documents the two new input error codes, that `GET /deliveries/:id`
  answers `"MessageId": null` once the message record has been removed, and the
  duration forms `msg_ttl` and `cleanup_interval` accept.
- The retired settings `msg_warn_threshold`, `force_upgrade_qos` and
  `delivery_queue_max` are documented as declarations that have to stay — a
  config written by an older version carries their names, and a schema that no
  longer declares them rejects that config — and as having no runtime effect.

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
