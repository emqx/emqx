# PostgreSQL backend

The PostgreSQL backend stores MQTT messages for a set of topics in PostgreSQL.
It delivers them to subscribers that connect later, and it delivers them to online subscribers too.
Each client keeps its own progress for each topic.
A client that acknowledges a message does not remove that message for other clients.

The configured topics are called *managed topics* in this document.

## How it works

1. A client publishes a QoS 1 or QoS 2 message to a managed topic.
2. The plugin writes the message to PostgreSQL. The message gets the next number in the topic's sequence.
   This number is the message *offset*.
3. The plugin sends the stored message to the subscribers of the topic that are online.
4. A subscriber that was offline receives the messages it missed when it connects again.
5. When a subscriber acknowledges a message, the plugin saves the offset of that message for that client.
   After a reconnect, delivery continues after the saved offset.
6. A cleanup job deletes old messages by age and by count.

QoS 0 messages to managed topics are not stored. The broker delivers them as usual.

## Requirements

- EMQX 7.0 or later on every node of the cluster.
- PostgreSQL 13 or later.
- The same plugin version and the same `namespace` and `topics` settings on all nodes that serve the same topics.

## Quick start

1. Create the schema. Run `priv/postgresql.sql` as the database owner:

   ```sh
   psql -h postgres.example.net -U owner -d mqtt -f priv/postgresql.sql
   ```

   For development, you can set `init_default_schema = true` instead.
   The plugin then creates the schema with its own database account at startup.

2. Configure the `postgresql` section of the plugin configuration:

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

3. Subscribe with QoS 1 to an exact managed topic, for example `offline/device-1`.

Clients that were connected before the backend started must connect or subscribe again.

## Subscribing

A subscription to a managed topic must meet all of these conditions:

- The topic is exact. It has no `+` or `#` wildcards.
- The subscription QoS is 1.
- The client uses MQTT and a regular in-memory session. Durable sessions are not supported.

The broker rejects other subscriptions that overlap the managed topics with reason code `0x87` (not authorized).
This includes QoS 0 and QoS 2 subscriptions, wildcard filters, gateway clients and durable sessions.

Some subscriptions cannot be easily checked by the plugin and receive no stored messages:

- Shared subscriptions (`$share/...`) to an exact managed topic are accepted but receive nothing.
- Subscriptions created through the REST API or auto-subscribe skip the check. Do not use them for managed topics.

Subscriptions are stored in PostgreSQL:

- When a client connects, the plugin restores its subscriptions to managed topics. This also happens after a clean start.
- An explicit UNSUBSCRIBE deletes the stored subscription.
- A repeated SUBSCRIBE keeps the stored progress.
- A subscription that no client uses for `subscription_expiry` (30 days by default) is deleted.

A new subscription starts according to `new_subscription_start`:

- `earliest` (default): the client receives all stored messages of the topic.
- `latest`: the client receives only messages published after it subscribed.

## Publishing

Publishers can use any QoS.
Only QoS 1 and QoS 2 messages are stored. QoS 0 messages are delivered by the broker as usual.

The `async_batched` setting chooses when the publisher receives its acknowledgement (PUBACK or PUBREC):

| `async_batched` | Acknowledgement | Risk |
| --- | --- | --- |
| `false` (default) | After the message is written to PostgreSQL | No loss. A failed write disconnects the publisher. If the write timed out but succeeded, the message may be stored twice. |
| `true` | When the plugin accepts the message into its write buffer | The message is lost if the buffered write later fails. |

With `async_batched = true`, the plugin writes buffered messages in batches.
This gives higher throughput because many messages share one database transaction.
The `writer.*` settings control the batch size and delay.

When PostgreSQL is not available, the plugin disconnects publishers of QoS 1/2 messages to managed topics.
It does not send these messages to subscribers without storing them.

The HTTP publish API reports a stored message as a success, even when the topic has no subscribers.

The retain flag gets no special handling. The broker's retainer stores retained messages as usual,
and the plugin stores them as regular messages.

## Delivery

Subscribers receive every stored message at QoS 1, in offset order.

Ordering follows the MQTT rules: messages from one publisher at one QoS keep their order.
Messages from different publishers, or at different QoS levels, can be stored in any order.
QoS 0 messages are not stored, so they can arrive before an earlier QoS 1/2 message.

Delivery is at least once:

- A client can receive a message again after a reconnect or a session takeover.
  The plugin saves progress periodically, so the last acknowledged messages may be sent again.
- The plugin skips expired messages and rows that it cannot decode.

The plugin treats these events as acknowledgements of a message:

- a PUBACK from the client, including one with an error reason code,
- a session drop of the message, for example because of the no-local option or message expiry.

## Retention

The cleanup job deletes old messages for each topic. A message is deleted when any of these is true:

- The message is older than `retention.max_age` (7 days by default).
- The topic has more than `retention.max_messages` newer messages (1,000,000 by default).
- `new_subscription_start = latest`, and every subscription of the topic has acknowledged the message.

With `earliest`, acknowledged messages stay until the age or count limit removes them, because a new subscriber reads them.
With `latest`, a topic without subscriptions keeps no messages.

The cleanup job runs every `retention.interval` (1 minute by default) on one node of the cluster.

## Configuration

Durations are in milliseconds. Sizes are in bytes. The defaults are in `priv/config.hocon`.

### Connection

| Setting | Description |
| --- | --- |
| `host`, `port`, `database`, `username`, `password` | PostgreSQL connection |
| `ssl.*` | TLS. With TLS enabled, the plugin verifies the server certificate and host name by default |
| `query_timeout` | Timeout of connections and database requests |
| `set_statement_timeout` | Set the PostgreSQL `statement_timeout` of each connection to `query_timeout`. Set to `false` when a connection pooler does not keep session settings |
| `message_publish_pool_size` | Connections for writing messages |
| `message_read_pool_size` | Connections for reading messages for subscribers |
| `subscription_meta_pool_size` | Connections for subscription changes and saved progress |

### Topics and behavior

| Setting | Description |
| --- | --- |
| `namespace` | Name that separates deployments that share one database |
| `topics` | Topic filters of the managed topics. With a mountpoint, use the full topic name including the mountpoint |
| `max_topic_length` | Maximum length of a managed topic name |
| `new_subscription_start` | Where a new subscription starts: `earliest` or `latest` |
| `async_batched` | Acknowledge publishers before the write. See [Publishing](#publishing) |
| `subscription_expiry` | Delete stored subscriptions that are not used for this time |
| `init_default_schema` | Create the schema at startup. Use for development only |

### Write batching (`async_batched = true` only)

| Setting | Description |
| --- | --- |
| `writer.pool_size` | Number of batch writers. Each topic always uses the same writer |
| `writer.min_flush_timeout` | Write a batch when no new message arrives for this time |
| `writer.max_flush_timeout` | Write a batch at the latest this long after its first message |
| `writer.flush_max_messages`, `writer.flush_max_bytes` | Write a batch when it reaches this size |
| `writer.call_timeout` | How long a publisher waits for the writer to accept a message |

### Delivery tuning

| Setting | Description |
| --- | --- |
| `read.page_max_messages` | Maximum number of messages read from PostgreSQL at once |
| `read.idle_poll_interval` | How often an idle subscription checks PostgreSQL for missed messages |
| `read.reorder_timeout`, `read.reorder_max_distance` | How long and for how many messages to wait for an out-of-order message before reading PostgreSQL |
| `checkpoint.interval` | How often progress is saved |
| `checkpoint.max_pending` | Save progress earlier after this many acknowledgements |

### Retention

| Setting | Description |
| --- | --- |
| `retention.max_age` | Delete messages older than this |
| `retention.max_messages` | Keep at most this many messages per topic |
| `retention.interval` | How often the cleanup job runs |

Do not configure the MySQL or Redis backend for topics that the PostgreSQL backend manages.

## Writing messages with SQL

Applications can write messages to a managed topic directly in PostgreSQL with the `omp_append` function.
Call it in a short transaction:

```sql
SELECT * FROM omp_append('production', 'offline/example', '[{
  "message_id": "", "sender": "sql-producer", "qos": 1, "retain": false,
  "payload": "aGVsbG8=", "properties": {"version": 1, "values": {}},
  "published_at": 1792454400000, "expires_at": null
}]'::jsonb);
```

- The first argument is the `namespace` from the plugin configuration.
- The third argument is a non-empty JSON array of messages.
- `message_id`, `payload` and the `Correlation-Data` property are base64 encoded.
- `published_at` and `expires_at` are Unix time in milliseconds. `expires_at` sets the message expiry interval on delivery.
- Supported properties are `Payload-Format-Indicator`, `Content-Type`, `Response-Topic`, `Correlation-Data`
  and `User-Property`. Write user properties as an array of `[key, value]` pairs.
- The function returns the topic ID and the offset of the first message.

Subscribers receive messages written with SQL with a delay of up to `read.idle_poll_interval` (30 seconds by default),
or earlier when a broker publish to the same topic follows.

Do not insert into the message table, change offsets or update message rows directly.
The database account of the application needs access to the plugin tables.

## Operations

### Health and metrics

The plugin health check fails when PostgreSQL is not reachable or the schema version does not match.
The plugin reports these counters:

- `write_failed`: messages that could not be written,
- `rejected`: deliveries that a client acknowledged with an error reason code,
- `skipped`: stored messages that could not be decoded.

### Configuration changes

A configuration change restarts the backend.
During the restart, QoS 1/2 publishes to managed topics fail and the publishers are disconnected.
If the backend fails to start, it keeps failing these publishes and the health check until the next configuration change.

### Connection poolers

The plugin uses regular queries only. It does not use `LISTEN`/`NOTIFY`.
PgBouncer in transaction mode has not been tested.
If a pooler does not keep session settings, set `set_statement_timeout = false` and set the timeout on the database role:

```sql
ALTER ROLE emqx SET statement_timeout = '5s';
```

### Resetting a topic

The plugin does not support resetting a topic.
To start a topic from scratch, delete its message, subscription and topic rows while no client uses the topic.

### Schema version

The plugin checks the schema version at startup. This version of the schema replaces earlier development versions
without migration. Recreate development databases when upgrading from a development build.

## Technical details

### Live delivery and recovery

After the plugin writes a message, it sends the stored message to the subscribers on all nodes.
Each subscriber connection has a reader for each managed subscription.
The reader keeps the offset of the last message it delivered.

- When the next message arrives in order, the reader delivers it immediately, without a database read.
- When a message arrives ahead of a missing one, the reader holds it for up to `read.reorder_timeout`.
  If the missing message does not arrive, the reader reads the messages from PostgreSQL.
- When the gap is larger than `read.reorder_max_distance`, the reader reads from PostgreSQL immediately.
- An idle reader reads from PostgreSQL every `read.idle_poll_interval`. This finds messages written with SQL
  and messages whose live copy was lost.
- A failed read is retried with exponential backoff from 1 second up to `read.idle_poll_interval`.

### Flow control

The reader takes new messages only while the connection can accept them.
The broker allows about 100 undelivered or unacknowledged messages per subscription.
When this limit is reached, the reader drops new live messages and reads them from PostgreSQL later.
Messages already queued for a stalled connection are limited by the broker's `force_shutdown` settings.

### Progress

The reader saves the latest acknowledged offset every `checkpoint.interval`,
after `checkpoint.max_pending` acknowledgements, and on disconnect.
Saving progress does not wait for the database. A failed save is not retried.
Saving progress also keeps the subscription from expiring.

### Connection pools

The plugin uses three connection pools, so that one kind of load does not delay another:

- writes of messages, with connections selected by topic,
- reads of messages for subscribers,
- subscription changes and saved progress, with connections selected by client ID.
  Changes of one client therefore run in order.

The cleanup job uses its own connection.

### Message hook

The plugin handles publishes in a `message.publish` hook with priority -1, after hooks with the default priority 0.
Register message transformations before this hook, so that the stored message is the transformed one.
The hook skips messages that an earlier hook rejected.
For a stored message, the broker does not route the original message, and it counts the publish once in `messages.publish`.

### Asynchronous writing

With `async_batched = true`:

- Each topic is assigned to one batch writer. Writes of one writer run one after another.
  Many busy topics on one writer can delay acceptance beyond `writer.call_timeout`, which disconnects the publisher.
  A message whose acceptance timed out can still be stored later.
- Stopping the backend writes the buffered messages within `writer.call_timeout`.
- If a writer crashes, its buffered messages are dropped and counted in `write_failed`. No message is stored twice.

## Tests

```sh
./scripts/ct/run.sh --app plugins/emqx_offline_messages
make plugin-emqx_offline_messages
```

The load probe measures database reads and delivery latency. Run it on an otherwise idle host:

```sh
./scripts/ct/run.sh --app plugins/emqx_offline_messages -- env TERM=dumb SUITES=emqx_offline_messages_pg_load_SUITE make plugins/emqx_offline_messages-ct
```

It checks that idle subscriptions read PostgreSQL about once per poll interval,
that subscribers that are up to date receive new messages without database reads,
and that a stalled subscriber loses no messages.
