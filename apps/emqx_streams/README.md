# MQTT Streams

The application provides EMQX Message Streams feature.

## Overview

A Message Stream is a collection of messages with the following properties:

- Message Stream is identified by `topic/filter` from the user's perspective.
- Has an explicit lifecycle.
- Is automatically replenished with published messages matched with the topic filter during the stream's lifetime.
- Clients may subscribe to the stream's `$s/timestamp/topic/filter` to consume messages from the stream.
- A Stream is capped by time or size.
- A Stream has a _key expression_ to extract a _key_ from each message.
- A Stream is not strictly ordered, but it is strictly ordered within each key, i.e. messages with the same key are
delivered always in the same order as they were published.
A stream may have "Last-Value" semantics. When enabled, messages overwrite previous messages with the same key.

## Internal Implementation

Message Streams are designed to have little interaction with other parts of EMQX.
Streams application
* Uses Durable Storage to store messages.
* Uses Mnesia table to maintain stream index.
* Is integrated via hooks into the EMQX core (via `emqx_extsub`, see [ExtSub description](../emqx_extsub/README.md)).

The major components are:
* ExtSub handler ([emqx_streams_extsub_handler.erl](./src/emqx_streams_extsub_handler.erl)). This is a handler responsible for delivering messages to the clients.
* Streams registry ([emqx_streams_registry.erl](./src/emqx_streams_registry.erl)). This is a registry responsible for keeping track of the Streams: creating, updating, deleting, and looking up.
* Streams message database ([emqx_streams_message_db.erl](./src/emqx_streams_message_db.erl)). This is a storage layer responsible for storing the Streams messages.

Also, Streams reuse quota mechanism from [MQs](../emqx_mq/src/emqx_mq_quota/emqx_mq_quota_index.erl).

## Data Flow

### Publishing

* A client publishes a message to `some/topic`.
* An Streams hook is triggered to handle the message publication.
* The hook looks up in the Streams registry if there are any Streams whose topic filter matches the message topic.
* If yes, the hook writes the message to the corresponding Stream.

### Subscribing/Consuming

* A client subscribes to some topic.
* An ExtSub hook is triggered to handle the subscription. If the topic is a Streams topic (`$s/timestamp/topic` topic handled by the Streams ExtSub handler), ExtSub initializes ExtSub handler state.
* ExtSub handler subscribes to DS storage to get the messages.
* ExtSub handler receives messages from DS storage and delivers them to the ExtSub application.
* ExtSub application delivers messages to the clients.

### Schematic View

The following diagram shows the data flow between the Message Stream components:

```ascii
+-----------------------+
| Message Stream DS DB  |
+-----------------------+
      ^      ^
      |      |
      |      |
      |      |          subscription on topic data
      |      |             via emqx_ds_client
      |      +-------------------------------------+  +--------------------------------+
      |                                            |  | Channel (subscribing)          |
      | write tx                                   |  | +----------------------------+ |
      |                                            |  | | ExtSub                     | |
      |                                            |  | | +------------------------+ | |
      |                                            |  | | | Streams ExtSub Handler | | |
+---------------------------+                      +----->|                        | | |
| Channel (writing)         |                         | | +----------------|-------+ | |
|                           |                         | +------------------|---------+ |
+---------------------------+                         +--------------------|-----------+
      |                                                                    |
      |                                                             stream |
      |                                                             lookup V
      |                                                                  +--------------------------+
      |        fast stream lookup in the index                           | Streams Registry         |
      +----------------------------------------------------------------> |                          |
                                                                         +--------------------------+
```

# Documentation

To be added to https://docs.emqx.com.

# Configuration

The Message Streams are configured via the `streams` section of the configuration file.

```hocon
streams {
    ## The interval at which the Message Streams will clean up expired messages.
    gc_interval = 1h
    ## The maximum retention period of messages in regular Message Streams.
    regular_stream_retention_period = 1d
    ## The interval at which subscribers will retry to find a stream if the stream is not found
    ## when subscribing to a stream topic.
    check_stream_status_interval = 10s
}

durable_storage {
    ## Settings for the database storing the Message Stream messages.
    ## See Durable Storage configuration for more details.
    streams_messages {
        transaction {
            flush_interval = 100
            idle_flush_interval = 20
            conflict_window = 5000
        }
    }
}
```

# HTTP APIs

Message stream management APIs are provided for creating, updating, looking up, deleting, and listing message streams.

Create Message Stream:

```bash
curl -s -u key:secret -X POST -H "Content-Type: application/json" http://localhost:18083/api/v5/message_streams/streams -d '{"topic_filter": "t1/#", "is_lastvalue": false}' | jq
{
  ...
  "topic_filter": "t1/#"
}
```

List Message Streams:

```bash
curl -s -u key:secret -X GET -H "Content-Type: application/json" http://localhost:18083/api/v5/message_streams/streams | jq
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

Update Message Stream:

```bash
curl -s -u key:secret -X PUT -H "Content-Type: application/json" http://localhost:18083/api/v5/message_streams/streams/t1%2F%23 -d '{"key_expression": "message.from", "is_lastvalue": false}' | jq
{
  ...
  "topic_filter": "t1/#"
}
```

Delete Message Stream:

```bash
curl -s -u key:secret -X DELETE http://localhost:18083/api/v5/message_streams/streams/t1%2F%23
```

Configure Message Stream global settings:

```bash
curl -s -u key:secret -X PUT -H "Content-Type: application/json" http://localhost:18083/api/v5/message_streams/config -d '{"gc_interval": "1h", "regular_stream_retention_period": "1d", "check_stream_status_interval": "10s"}'
```

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
