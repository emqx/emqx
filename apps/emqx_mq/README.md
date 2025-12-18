# MQTT Message Queue

The application provides Message Queue functionality for EMQX.

## Overview

A Message Queue is a collection of messages with the following properties:

- Message Queue is identified by `topic/filter` from the user's perspective.
- Has an explicit lifecycle.
- Is automatically replenished with published messages matched with the topic filter during the queue's lifetime.
- Clients may subscribe to the queue's `$q/topic/filter` to consume messages from the queue cooperatively.
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
* Local subscription registry ([emqx_mq_sub_registry.erl](./src/emqx_mq_sub_registry.erl), [emqx_mq_sub.erl](./src/emqx_mq_sub.erl), [emqx_mq_sub_buffer.erl](./src/emqx_mq_sub_buffer.erl)). These are data structures
facilitating channel's connections to the Message Queues. They are stored in the process dictionary of the channel.
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

### Subscribing/Consuming

* A client subscribes to some topic.
* An MQ hook is triggered to handle the subscription.
* If the topic is a Message Queue topic (`$q/some/topic`), the hook initializes a subscription in the Channel's state and
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
    "max_shard_message_count": 1000,
    "max_shard_message_bytes": "100MB"
}
...
```

* `max_shard_message_count` is the maximum number of messages in a shard for the Message Queue. The number of shards is configured for all Message Queues in the Durable Storage settings. So the total count limit for the Message Queue is `n_shards * max_shard_message_count`.
* `max_shard_message_bytes` is the maximum number of bytes in a shard for the Message Queue. Note that this limit is not aware of the replication factor. So the total byte limit for the Message Queue is `n_shards * replication_factor * max_shard_message_bytes`

Also the limit is soft. The limit threshold 10% and is applied to the queues during the GC process. That means that immediately after GC the queue may exceed the limit by up to 10%, but between GC runs the amount of excess data may be larger.

The threshold and some other settings are configured via the `quota` section of the global Message Queue configuration, but they are not publicly exposed currently.

## Queue auto creation

Message Queues are be automatically created when subscribing to a queue topic `$q/some/topic`. The auto creation is configured via the `auto_create` section of the global Message Queue configuration. By default, the queues are auto created as last-value queues.
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
One may change the auto creation settings to regular queues, or disable the auto creation. Obviously, one may not autocreate queues as regular and last-value at the same time.

## QoS handling

When delivering messages to the subscribers, the QoS level is overridden to 1 to require PUBACK from the client.

With one exception, when publishing messages to the Message Queue, the messages are stores synchronously to the Message Queue database. That means that the publisher will not receive PUBACK from the broker till the message is stored.

The exception is when a message is published with QoS0 to a unlimited regular queue. In this case, the message is stored asynchronously via dirty append mechanism and the publisher will receive PUBACK from the broker immediately.

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

Configure Message Queue global settings:

```bash
curl -s -u key:secret -X PUT -H "Content-Type: application/json" http://localhost:18083/api/v5/message_queues/config -d '{"gc_interval": "1h", "regular_queue_retention_period": "1d", "find_queue_retry_interval": "10s"}'
```

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
