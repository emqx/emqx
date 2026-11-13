# MQTT Message Queue

The application provides Message Queue functionality for EMQX.

## Overview

A Message Queue is a collection of messages with the following properties:

- Message Queue is identified by a name and a `topic/filter` from the user's perspective.
- Has an explicit lifecycle.
- Is automatically replenished with published messages matched with the topic filter during the queue's lifetime.
- Clients may subscribe to `$queue/name/topic/filter` to consume messages from the queue cooperatively.
- A Queue is capped by time or size.
- A Queue is not strictly ordered.A queue may have "Last-Value" semantics. When enabled,
  - A queue has a _key expression_ to extract a key from each message.
  - Messages overwrite previous messages with the same key.

## Internal Implementation

Message Queue is designed to have little interaction with other parts of EMQX.
Message Queue application
* Uses Durable Storage to store messages and queue state.
* Uses Mnesia table to maintain message queue index.
* Is integrated via hooks into the EMQX core.

The major components are:
* Message Queue subscriptions ([emqx_mq_extsub_handler.erl](./src/emqx_mq_extsub_handler.erl), [emqx_mq_sub.erl](./src/emqx_mq_sub.erl)) manage each client's connections to queue consumers.
  [emqx_extsub.erl](../emqx_extsub/src/emqx_extsub.erl) handles message buffering and delivery.
* Message Queue registry ([emqx_mq_registry.erl](./src/emqx_mq_registry.erl)). This is a registry responsible for keeping track of the Message Queues: creating, updating, deleting, and looking up.
* Message Queue state storage ([emqx_mq_state_storage.erl](./src/emqx_mq_state_storage.erl)). This is a storage layer responsible for storing the Message Queue metadata and state of consumption.
* Message Queue message database ([emqx_mq_message_db.erl](./src/emqx_mq_message_db.erl)). This is a storage layer responsible for storing the Message Queue messages.
* Message Queue Consumer. This is a component responsible for consuming messages from the Message Queue and dispatching them to the channels.

## Data Flow

### Publishing

* A client publishes a message to `some/topic`.
* An MQ hook is triggered to handle the message publication.
* The hook looks up in the MQ registry if there are any Message Queues whose topic filter matches the message topic.
* If yes, the hook writes the message to the corresponding Message Queues.

### Direct publishing

Publish to `$queue/name` to write directly to a named queue.
Publish to `$queue/name/topic` to also supply the queue's topic filter.
The supplied topic must equal the queue's topic filter.
When the queue does not exist, a topic suffix is required for creation.
Creation uses the `mq.auto_create` settings.
When auto-creation is disabled, the queue must already exist.

Direct publishes only write to the named queue.
They do not route to regular subscribers or other queues and streams.
Stored messages retain the full publish topic, including the `$queue/` prefix.

For MQTT 5, QoS 1 PUBACK and QoS 2 PUBREC report the persistence result:

| Result | Reason code |
| --- | --- |
| Message stored | Success (`0x00`) |
| Queue missing, creation failed, or storage failed | Implementation specific error (`0x83`) |
| Invalid name, empty topic suffix, or conflicting topic | Topic Name invalid (`0x90`) |
| Queue admission rejected by `limits.on_full = reject` | Quota exceeded (`0x97`) |

QoS 0 has no publish acknowledgment.
MQTT 3.x acknowledgments cannot carry error reason codes.

### Subscribing/Consuming

* A client subscribes to some topic.
* An MQ hook is triggered to handle the subscription.
* If the topic is a Message Queue topic (`$queue/name/some/topic`), the hook initializes a subscription in the Channel's state and
initiates a connection to the Message Queue Consumer.
* If a Consumer is not yet found, a new consumer is started.
* The Consumer restores message consumption progress and starts to fetch data from the Message Queue message database.
* The Consumer dispatches received messages to the connected subscribers.
* The subscribers (channels) deliver MQ messages to the clients.

### Schematic View

The following diagram shows the data flow between the Message Queue components:

```ascii
+-----------------------+                                     +-----------------------------+
| Message Queue DS DB   |                                     | Message Queue State Storage |
+-----------------------+                                     +-----------------------------+
      ^      ^                                                    ^                      ^
      |      |                                                    | persist              |
      |      |                                                    | progress             |
      |      |              subscription on topic data            |                      |
      |      |                  via emqx_ds_client          +-------------+              |
      |      +--------------------------------------------->| MQ Consumer |              |
      |                                                     |             |              |
      | write tx                                            +-------------+              | message metadata
      |                                                           ^                      | persist/lookup
      |                                                           | proto                |
      |                                                           V                      |
+---------------------------+                         +----------------------------+     |
| Channel (writing)         |                         | Channel (subscribing)      |     |
|                           |                         | [MQ subscription registry] |     |
+---------------------------+                         +----------------------------+     |
      |                                                                    |             |
      |                                                              queue |             |
      |                                                             lookup V             |
      |                                                                  +--------------------------+
      |        fast queue lookup in the index                            | MQ Registry              |
      +----------------------------------------------------------------> |                          |
                                                                         +--------------------------+
```

## Capping by time or size

Message Queues can be capped by time or size individually. The capping is configured via the `limits` field in the Message Queue configuration (e.g. in HTTP API):
```json
...
"limits": {
    "on_full": "reject",
    "max_shard_message_count": 1000,
    "max_shard_message_bytes": "100MB"
}
...
```

* `max_shard_message_count` is the maximum number of messages in a shard for the Message Queue. The number of shards is configured for all Message Queues in the Durable Storage settings. So the total count limit for the Message Queue is `n_shards * max_shard_message_count`.
* `max_shard_message_bytes` limits the serialized message bytes charged to each shard. The total queue limit is `n_shards * max_shard_message_bytes`. Replication adds physical storage usage.

Set `limits.on_full` to choose the full policy:

* `evict` is the default. It accepts every publish. The quota index trims older buckets when usage reaches a limit. Periodic garbage collection deletes the older data later.
* `reject` checks estimated count and serialized bytes before storing a message. It rejects a message when adding it would exceed either finite limit. Capacity pressure does not delete accepted messages. Expiration, explicit purge, and last-value replacement still apply.

The reject policy requires at least one finite limit. The policy cannot change after queue creation. Send `on_full = reject` on every update of a reject queue. An update that omits it receives the default `evict` and fails as a policy change. Automatic queue creation accepts the same settings. Creating or updating a reject queue fails until all cluster nodes are upgraded.

Limits apply separately to each shard. Regular queues select a shard by publisher client ID. Last-value queues select a shard by message key. One publisher can receive a rejection while another publisher's shard still has capacity.

Occupancy is an estimate. Quota updates are buffered, so concurrent writes can exceed a limit. There is no fixed maximum overshoot.

### Releasing quota through consumption

A reject queue reports consumption when a whole consumer buffer has been acknowledged or skipped by existing consumption rules. Garbage collection reports expired records the same way. A shared pool of cleanup workers handles the reports. The queue ID selects the worker. The worker deletes the consumed or expired records below the reported boundary. Then it stores the boundary in the quota index as the released boundary. The index drops the buckets that end at or before the released boundary. Each quota flush drops them too, so a bucket that closes later is released when it closes. Quota is released only after the records are deleted. Disk compaction can finish later. Publishing does not wait for cleanup.

When cleanup falls behind, the queue stays full and rejects new messages. So the amount of pending cleanup work stays bounded. The cleanup pool is separate from the pool that deletes queues and purged messages.

The worker keeps the processed boundary of each consumer and ignores repeated reports, so an idle queue causes no index reads. Consumers repeat their latest boundary periodically, so a restarted worker recovers it.

The index always retains its newest bucket. A full bucket closes at a DS fence on the next quota flush. The default bucket threshold is ten percent of the corresponding limit. A bucket is released after the consumer passes a record stored after the bucket. So one consumed bucket can stay charged until the next message is consumed. While the index of a shard has one bucket, or two buckets of which the newest is empty, the queue accepts messages above the limit. The next message then releases the older bucket. So consumed buckets cannot block the queue.

Reject last-value queues retain unconsumed values. Cleanup deletes consumed values. Admission does not credit the previous value of the same key. A rejected replacement leaves that value intact.

When the deletion or an index transaction fails, the worker keeps the quota charged and retries with a growing delay. It logs the first failure of a series as a warning.

Checkpoint lag can cause redelivery under the existing at-least-once delivery semantics.

### Rejection feedback

Only direct `$queue/` publication reports rejection to MQTT 5 publishers through PUBACK or PUBREC. QoS 0 has no acknowledgment. MQTT 3.x acknowledgments cannot carry the MQTT 5 rejection reason.

Publication to an ordinary topic receives no feedback about rejection by a matching queue. A full queue skips the message, increments the `queue_full` counter, and logs a throttled message at info level. Other matching queues and regular subscribers still receive the publication. Capacity rejections and storage errors use separate counters.

The threshold and some other settings are configured via the `quota` section of the global Message Queue configuration, but they are not publicly exposed currently.

## Queue auto creation

Message Queues can be automatically created when subscribing to a queue topic `$queue/name/some/topic`. The auto creation is configured via the `auto_create` section of the global Message Queue configuration. Auto creation is disabled by default: subscribing to a queue topic does not create a queue.
```hocon
...
auto_create {
    regular = false
    lastvalue = {
      ## Default settings for the Last-Value Message Queue, excluding the topic filter and is_lastvalue fields.
    }
}
...
```
The example above enables auto creation of last-value queues. Set `regular` to a settings object instead to auto create regular queues. A queue cannot be auto created as regular and last-value at the same time.

## QoS handling

When delivering messages to the subscribers, the QoS level is overridden to 1 to require PUBACK from the client.

With one exception, when publishing messages to the Message Queue, the messages are stored synchronously to the Message Queue database. For QoS 1/2 messages, that means that the publisher will not receive PUBACK/PUBREC from the broker till the message is stored.

The exception is when a message is published with QoS0 to a unlimited regular queue. In this case, the message is stored asynchronously via dirty append mechanism. This significantly improves throughput, but does not provide backpressure to the publisher.

# Documentation

To be added to https://docs.emqx.com.

# Configuration

The Message Queues are configured via the `mq` section of the configuration file.

```hocon
mq {
    ## The interval at which the Message Queues will clean up expired messages.
    gc_interval = 1h
    ## The maximum retention period of messages in regular Message Queues.
    regular_queue_retention_period = 1d
    ## The maximum number of Message Queues that can be created.
    max_queue_count = 100
    ## The interval at which subscribers will retry to find a queue if the queue is not found
    ## when subscribing to a queue topic.
    find_queue_retry_interval = 10s
    ## Settings for automatically creating the Message Queue if it does not exist
    ## when subscribing to the queue topic.
    auto_create {
        regular = false
        lastvalue = false
    }
}

durable_storage {
    ## Settings for the database storing the Message Queue state.
    ## See Durable Storage configuration for more details.
    mq_states {
        transaction {
            flush_interval = 10
            idle_flush_interval = 5
            conflict_window = 5000
        }
    }
    ## Settings for the database storing the Message Queue messages.
    ## See Durable Storage configuration for more details.
    mq_messages {
        transaction {
            flush_interval = 100
            idle_flush_interval = 20
            conflict_window = 5000
        }
    }
}
```

# HTTP APIs

Message queue management APIs are provided for creating, updating, looking up, deleting, and listing message queues.

Create Message Queue:

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/message_queues/queues -d '{"topic_filter": "t1/#", "is_lastvalue": false}' | jq
{
  ...
  "topic_filter": "t1/#"
}
```

List message queues:

```bash
curl -s -u key:secret -X GET -H "Content-Type: application/json" http://localhost:18083/api/v5/message_queues/queues | jq
{
  "data": [
    {
      ...
      "topic_filter": "t1/#"
    }
  ],
  "meta": {
    "hasnext": false
  }
}
```

Update Message Queue:

```bash
curl -s -u key:secret -X PUT -H "Content-Type: application/json" http://localhost:18083/api/v5/message_queues/queues/t1%2F%23 -d '{"dispatch_strategy": "least_inflight"}' | jq
{
  ...
  "topic_filter": "t1/#"
}
```

Delete Message Queue:

```bash
curl -s -u key:secret -X DELETE http://localhost:18083/api/v5/message_queues/queues/t1%2F%23
```

Purge messages without deleting the queue:

```bash
curl -s -u key:secret -X DELETE http://localhost:18083/api/v5/queue/q1/messages
curl -s -u key:secret -X DELETE 'http://localhost:18083/api/v5/queue/q1/messages?before=2026-10-01T00:00:00Z'
```

The optional `before` parameter accepts an RFC 3339 timestamp or epoch milliseconds.
The `before` value must not be later than the current time.
When `before` precedes the queue's current delivery cutoff, the request does nothing.
Purge never lowers the delivery cutoff.
All times are storage times: the time at which a message was written to durable storage.
Publication timestamps are not used.
Messages stored before `before` are skipped during delivery and deleted.
Without `before`, purge deletes all messages stored before the purge.
For unlimited queues, the delivery cutoff is then the current time of the node that handles the request.
For limited queues, it is the latest storage time observed in DS during deletion.
The node that handles the request clamps a `before` value that is later than its current time.

The request changes the quota index and sets the delivery cutoff before it returns.
So the queue stops dispatching purged messages when the request returns.
Messages already passed to subscriber sessions can still be delivered.
A deletion worker deletes the purged messages in the background, in bounded batches.
When that deletion fails, the error is logged, and GC deletes the messages later:
* For limited queues and unlimited last-value queues, GC deletes messages before the delivery cutoff.
  For limited queues, it also deletes messages before the oldest quota bucket that the index keeps.
* For unlimited regular queues, the messages stay until retention drops their storage generation.
Repeat the request to delete them sooner.

Purge is refused with 503 until all cluster nodes support the serial quota index.
Purge is refused with 400 while the queue is being deleted.
The queue keeps its identity and consumer progress.
Consumers restart when the delivery cutoff changes, while MQTT clients stay connected.
Delivery can pause for about twice the queue's `ping_interval`.
The request returns 204 on success, 400 for an invalid or future cutoff or a queue that is being deleted, 404 for an unknown queue, or 503 on failure or when queues are unavailable.
When no deletion worker is available, the request still returns 204 and logs a warning. GC deletes the purged messages later.
If the index update fails, some storage slabs can already have changed, but the delivery cutoff stays unchanged.
Repeat the request to complete the purge.

Configure Message Queue global settings:

```bash
curl -s -u key:secret -X PUT -H "Content-Type: application/json" http://localhost:18083/api/v5/message_queues/config -d '{"gc_interval": "1h", "regular_queue_retention_period": "1d", "find_queue_retry_interval": "10s"}'
```

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
