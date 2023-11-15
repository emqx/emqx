# General concepts

In the logic layer we don't speak about replication.
This is because we could use an external DB with its own replication logic.

On the other hand, we introduce notion of shard right here at the logic layer.
This is because shared subscription logic needs to be aware of it to some extend, as it has to split work between subscribers somehow.


# Modus operandi

1. Create a draft implementation of a milestone
2. Test performance
3. Test consistency
4. Goto 1

# Tables

## Message storage

Data is written every time a message matching certain pattern is published.
This pattern is not part of the logic layer spec.

Write throughput: very high

Data size: very high

Write pattern: append only

Read pattern: pseudoserial

Number of records: O(total write throughput * retention time)


# Push vs. Pull model

In push model we have replay agents iterating over the dataset in the shards.

In pull model the client processes work with iterators directly and fetch data from the remote message storage instances via remote procedure calls.

## Push pros:
- Lower latency: message can be dispatched to the client as soon as it's persisted
- Less worry about buffering

## Push cons:
- Needs pushback logic
- It's not entirely justified when working with external DB that may not provide streaming API

## Pull pros:
- No need for pushback: client advances iterators at its own tempo

## Pull cons:
- 2 messages need to be sent over network for each batch being replayed.
- RPC is generally an antipattern

# Invariants

- All messages written to the shard always have larger sequence number than all the iterators for the shard (to avoid missing messages during replay)


# Parallel tracks

## 0. Configuration

This includes HOCON schema and an interface module that is used by the rest of the code (`emqx_ds_conf.erl`).

At the early stage we need at least to implement a feature flag that can be used by developers.

We should have safety measures to prevent a client from breaking down the broker by connecting with clean session = false and subscribing to `#`.

## 1. Fully implement all emqx_durable_storage APIs

### Message API

### Session API

### Iterator API

## 2. Implement a filter for messages that has to be persisted

We don't want to persist ALL messages.
Only the messages that can be replayed by some client.

1. Persistent sessions should signal to the emqx_broker what topic filters should be persisted.
   Scenario:
   - Client connects with clean session = false.
   - Subscribes to the topic filter a/b/#
   - Now we need to signal to the rest of the broker that messages matching `a/b/#` must be persisted.

2. Replay feature (separate, optional, under BSL license): in the configuration file we have list of topic filters that specify what topics can be replayed. (Lower prio)
   - Customers can do this: `#`
   - Include minimum QoS of messages in the config

## 3. Replace current emqx_persistent_session with the emqx_durable_storage

## 4. Garbage collection

## 5. Tooling for performance and consistency testing

At the first stage we can just use emqttb:

https://github.com/emqx/emqttb/blob/master/src/scenarios/emqttb_scenario_persistent_session.erl


Consistency verification at the early stages can just use this test suite:

`apps/emqx/test/emqx_persistent_session_SUITE.erl`

## 6. Update rocksdb version in EMQX (low prio)

## 9999. Alternative schema for rocksdb message table

https://github.com/emqx/eip/blob/main/active/0023-rocksdb-message-persistence.md#potential-for-future-optimizations-keyspace-based-on-the-learned-topic-patterns

Problem with the current bitmask-based schema:

- Good: `a/b/c/#`

- Bad: `+/a/b/c/d`
