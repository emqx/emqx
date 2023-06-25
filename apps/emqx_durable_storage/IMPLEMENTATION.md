# General concepts

In the logic layer we don't speak about replication.
This is because we could use an external DB with its own replication logic.

On the other hand, we introduce notion of shard right here at the logic.
This is because shared subscription logic needs to be aware of it to some extend, as it has to split work between subscribers somehow.

# Tables

## Message storage

Data is written every time a message matching certain pattern is published.
This pattern is not part of the logic layer spec.

Write throughput: very high
Data size: very high
Write pattern: append only
Read pattern: pseudoserial

Number of records: O(total write throughput * retention time)

## Session storage

Data there is updated when:

- A new client connects with clean session = false
- Client subscribes to a topic
- Client unsubscribes to a topic
- Garbage collection is performed

Write throughput: low

Data is read when a client connects and replay agents are started

Read throughput: low

Data format:

`#session{clientId = "foobar", iterators = [ItKey1, ItKey2, ItKey3, ...]}`

Number of records: O(N clients)
Size of record: O(N subscriptions per clients)

## Iterator storage

Data is written every time a client acks a message.
Data is read when a client reconnects and we restart replay agents.

`#iterator{key = IterKey, data = Blob}`

Number of records: O(N clients * N subscriptions per client)
Size of record: O(1)
Write throughput: high, lots of small updates
Write pattern: mostly key overwrite
Read throughput: low
Read pattern: random

# Push vs. Pull model

In push model we have replay agents iterating over the dataset in the shards.

In pull model the the client processes work with iterators.

## Push pros:
- Lower latency: message can be dispatched to the client as soon as it's persisted
- Less worry about buffering

## Push cons:
- Need pushback logic
- It's not entirely justified when working with external DB that may not provide streaming API

## Pull pros:
- No need for pushback: client advances iterators at its own tempo
-
