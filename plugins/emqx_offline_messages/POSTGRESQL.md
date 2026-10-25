# PostgreSQL offline messages

The PostgreSQL backend stores every matching publish, including QoS 0 and messages with online subscribers.
After commit, it sends a live copy of each message to the subscribers of its topic.
Channel-local ExtSub handlers order live copies and recover missing messages from PostgreSQL.
Each client has one durable cursor per topic. Acknowledgements never delete another client's messages.

## Requirements

Use PostgreSQL 13 or later and an EMQX build with the ExtSub changes from #19668 and `emqx_broker:dispatch_info/2`.
The plugin refuses to start when `emqx_broker:dispatch_info/2` is missing.
Every node in the cluster must provide it. A node without it receives no live copies, and its subscribers recover messages through the idle poll.
All nodes serving a namespace must run the same plugin build.

Subscriptions to managed topics must use:

- an exact topic without a share prefix,
- QoS 1,
- an in-memory MQTT session.

Authorization rejects other subscriptions that intersect the managed topic space with reason code `0x87`.
This covers QoS 0 and QoS 2 subscriptions, wildcards, gateways and durable sessions.
Authorization does not see the share prefix. A shared subscription to an exact managed topic succeeds and receives no managed messages.
Publishers may use any QoS.
Retained-message replay is separate from the ordered feed. Retained messages keep their own QoS.

The REST subscribe API and auto-subscribe skip authorization. Do not use them for managed topics.
A subscription created this way with a QoS other than 1 receives no managed messages.
Enable the plugin or widen `topics` only when no durable or gateway subscription overlaps the managed topic space.
Cluster links do not forward stored messages.
An excluded subscriber that exists anyway is unsupported. It receives no stored messages.

### Connection poolers

The backend uses no PostgreSQL notifications or dedicated listener sessions.
PgBouncer transaction mode has not been verified with the epgsql query path.
When a pooler cannot preserve session settings, set `set_statement_timeout = false` and set the timeout on the role instead:

```sql
ALTER ROLE emqx SET statement_timeout = '5s';
```

## Setup

Run `priv/postgresql.sql` as the schema owner before enabling the backend.
For development, `init_default_schema = true` installs the schema using the configured database account.
The plugin checks schema version 1 at startup.
This unreleased schema replaces earlier development schemas in place.
Recreate development databases before using this build. There is no data migration.
Use a separate database or schema-owner account for schema installation in production.

Set the `postgresql` section of the plugin configuration:

```hocon
postgresql {
    enable = true
    host = "postgres.example.net"
    port = 5432
    database = "mqtt"
    username = "emqx"
    password = "secret"
    namespace = "production"
    topics = ["offline/#"]
    ssl { enable = true, verify = verify_peer, cacertfile = "/certs/ca.pem" }
}
```

`namespace` isolates deployments that share a database.
All broker nodes serving the same feed must use the same namespace and topic configuration.
Topic names refer to mounted broker topics.
Do not configure overlapping MySQL or Redis topic spaces with PostgreSQL.
Connect or subscribe again after enabling the backend to start consumption on existing clients.

Time values are milliseconds. Byte limits are bytes.
The defaults are in `priv/config.hocon`.

| Settings | Purpose |
| --- | --- |
| `write_pool_size` | Connections for publish appends |
| `read_pool_size` | Connections for subscriber operations: page reads, restore, subscribe, unsubscribe and checkpoints |
| `query_timeout` | Connection and request timeout |
| `set_statement_timeout` | Set the session `statement_timeout` to `query_timeout`. When `false`, the server, database or role default applies |
| `max_topic_length` | Maximum managed topic length, default 1024 bytes, maximum 2048 |
| `new_subscription_start` | `earliest` replays retained history; `latest` starts at the current tip |
| `writer.pool_size` | Topic-affine QoS 0 batching workers |
| `writer.min_flush_timeout`, `writer.max_flush_timeout` | Idle and maximum batch delay |
| `writer.flush_max_messages`, `writer.flush_max_bytes` | Batch thresholds |
| `writer.call_timeout` | Maximum QoS 0 buffer admission wait, and the writer shutdown time |
| `read.page_max_messages` | Maximum rows per page |
| `read.window` | Capacity for ExtSub delivery, the outstanding page and the reorder pocket |
| `read.reorder_max_distance` | Maximum live offset distance and pocket size, default 32 |
| `read.reorder_timeout` | Wait for a missing live copy before reading, default 50 milliseconds |
| `read.idle_poll_interval` | Check idle subscriptions for missing messages, default 30000 milliseconds |
| `checkpoint.interval`, `checkpoint.max_pending` | Progress write interval and the number of completed messages that triggers a write |
| `retention.max_age`, `retention.max_messages`, `retention.interval` | Age limit, count limit per topic and cleanup interval |
| `subscription_expiry` | Expire inactive subscription rows |

Each pool selects a connection by key: the topic for appends, the client ID for subscriber operations.
Operations for one client therefore run in order on one connection.
Subscriber operations never wait behind publish appends.
Each subscription has at most one outstanding read and does not use its own process or connection.
The cleanup worker uses its own connection.
Only the first node in the sorted list of running broker nodes runs cleanup.

## Delivery and recovery

QoS 1/2 publishes commit before PUBACK/PUBREC and receive reason code `0x00`.
A failed or uncertain write disconnects the publisher. A publisher retry can create another row.
QoS 0 publishes return after buffer admission, before commit or the later dispatch is known.
A failed flush drops the batch and increments `write_failed`.
The writer flushes topic batches one after another. With many topics per writer, admission can exceed `writer.call_timeout` and disconnect the publisher.
A timed-out admission still stores and dispatches the message later.
Stopping the backend flushes the buffered QoS 0 messages within `writer.call_timeout`.
After a writer crash, the buffered messages are dropped and counted in `write_failed`, so no batch is stored twice.
Dispatch runs in the topic-affine writers. Large fan-out delays other topics assigned to the same writer.

When the backend is not running, publishes to managed topics fail and disconnect the publisher.
This applies during a configuration reload and after a failed start.
A failed start keeps the configuration and fails the plugin health check until a later configuration change starts the backend.

The persistence hook runs at priority -1, after ordinary priority-0 publish hooks.
It skips messages rejected by earlier hooks and stops the hook chain for managed publishes.
Register message transformations before the persistence hook.
The broker counts each managed publish once in `messages.publish`.
It does not route the original message. Live copies run no route trace.

Each exact topic has one offset sequence for all publish QoS values.
Subscribers receive every managed message at QoS 1, in offset order.
Messages from one publisher at one QoS keep their publish order.
A buffered QoS 0 publish can receive a higher offset than a later QoS 1 publish from the same client.
Subscribers then receive the QoS 1 message first.
[MQTT 5.0 section 4.6](https://docs.oasis-open.org/mqtt/mqtt/v5.0/os/mqtt-v5.0-os.html#_Toc3901240) requires ordering only per publisher and per publish QoS.

MQTT clients acknowledge QoS 1 messages in order, so the plugin saves the latest completed offset.
A completion is a PUBACK, a negative acknowledgement or a session drop such as no-local or expiry.
QoS 1 delivery is at least once within retention.
Checkpoints are sent without waiting for the result. A failed checkpoint is not retried.
Unsaved progress and session takeover produce duplicates after a reconnect.
Expired messages and invalid rows are skipped without blocking later messages.

Reconnect restores database subscriptions, including after MQTT clean start.
Explicit unsubscribe deletes the database subscription.
A repeated SUBSCRIBE keeps the stored options and progress.
Failed reads keep the read and completed offsets and retry with exponential backoff and jitter, from one second up to `read.idle_poll_interval`.
Consecutive live copies require no page read after initial catch-up.
Small reorderings wait in a bounded pocket. A persistent gap triggers a page read.
Idle subscriptions poll for messages at `read.idle_poll_interval`. The first poll after a subscription starts comes earlier by a random amount, which spreads polls after a mass reconnect.
A direct SQL append or a commit whose live copy is lost is recovered by a later offset gap or an idle poll.
The default recovery interval is 30 seconds. It is not an end-to-end delivery deadline.
Poll scheduling, database outages, backlogs and client backpressure can delay delivery further.
Each idle subscription performs approximately one page read per poll interval.
Active subscriptions renew their subscription row every third of `subscription_expiry`.

The delivery window bounds accepted work, including the outstanding page and pocket entries.
It does not bound live copies in a slow channel's mailbox. Existing channel overload limits still apply.

## Retention

Cleanup processes the topics of the namespace in batches and deletes messages below a cutoff offset for each topic.
The cutoff is the largest of these values:

- the count limit: the topic tip minus `retention.max_messages`,
- the age limit: the oldest message published within `retention.max_age`,
- with `new_subscription_start = latest` only: the lowest completed offset of the topic's subscriptions, plus one.

With `earliest`, new subscriptions read the retained history, so cleanup keeps messages that all subscribers have completed.
With `latest`, new subscriptions start at the tip, so cleanup also deletes messages that every current subscriber has completed.
A topic without subscriptions then keeps no messages.

The age limit removes an expired prefix. An expired message after a more recent one stays until the messages before it expire.
This happens only when SQL producers append `published_at` values out of order.
Deletes run in primary key ranges of at most 1000 offsets.
Subscription expiry runs once per hour.

Reads return the available rows after the read cursor in offset order, so deleted rows are skipped.
Do not update message rows or allocate offsets directly.
Topic reset is not supported. To start a topic again, delete its message, subscription and topic rows while no client uses it.

The HTTP publish API returns success for a committed QoS 1/2 publish, including one with no subscribers.
QoS 0 returns `202 no_matching_subscribers` after buffer admission.

## SQL producers

Use `omp_append(namespace, topic, messages)` in a short transaction.
It returns the topic ID and first assigned offset after allocating and inserting the batch atomically.
The batch must be nonempty. It may mix publish QoS values.
Concurrent writers serialize on the counter row for that exact topic.
Rollback releases the allocated offsets.

```sql
SELECT * FROM omp_append('production', 'offline/example', '[{
  "message_id": "", "sender": "sql-producer", "qos": 1, "retain": false,
  "payload": "aGVsbG8=", "properties": {"version": 1, "values": {}},
  "published_at": 1792454400000, "expires_at": null
}]'::jsonb);
```

Encode `message_id`, `payload` and the `Correlation-Data` property as base64.
`published_at` and `expires_at` are Unix milliseconds.
Supported properties are `Payload-Format-Indicator`, `Content-Type`, `Response-Topic`, `Correlation-Data` and `User-Property`.
Represent user properties as an ordered array of `[key, value]` pairs.
The expiry column supplies the MQTT Message Expiry Interval on replay.
Message IDs are informational and do not deduplicate inserts.
SQL appends send no live copy. Subscribers receive them through gap detection or the idle poll.

Producers must not insert message rows or change topic counters directly.
Producer and broker accounts need read and write access to the plugin tables.

## Tests

```sh
./scripts/ct/run.sh --app plugins/emqx_offline_messages
make plugin-emqx_offline_messages
```

Health checks cover database access and the schema version.
Plugin PostgreSQL counters include failed writes, rejected deliveries and skipped rows.
