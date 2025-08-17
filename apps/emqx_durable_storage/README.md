# EMQX Durable Storage

`emqx_durable_storage` (DS for short) is an application implementing durable storage for MQTT messages within EMQX.

The core design idea behind `emqx_durable_storage` is to store each message exactly once (per each replica of the database), regardless of the number of consumers, online or offline.
This makes the storage disk requirements very predictable: only the number of _published_ messages matters; the number of consumers is removed from the equation, and fan-out is practically free in terms of disk storage.

# Features

## Utility modules

Business logic is advised to use utility modules provided by this application:

- `emqx_ds_client` for consuming stream-like data
- `emqx_ds_pmap` for key-value data

## Backends

### Backend behavior

DS _backend_ is a callback module that implements `emqx_ds` behavior.

EMQX repository contains two builtin backends based on RocksDB:

- `emqx_ds_builtin_local`
- `emqx_ds_builtin_raft`

### Layout

DS _layout_ is a module that implements `emqx_ds_storage_layer` behavior.
Layout modules are the only modules that have direct access to the underlying storage engine, in both reads and writes.

Different storage layouts can be used to maximize the efficiency of message storage and retrieval for various types of workload.

Backward- and forward-incompatible changes to the layout modules are forbidden.
EMQX should always be able to read the data written by the old releases.
Non-compatible changes must be implemented as entirely new layout modules.

## How does EMQX organize data

Messages are organized in the following hierarchy:

1. **Database**.
   DS databases are completely independent from each other.
   They can have different schema, different backend, and they can be opened, closed, created and dropped independently from each other.

   Each database can be used for a different type of payload or a different tenant.

2. **Shard**.
   (The concept of shard is specific to the builtin backend)
   The builtin backend separates different messages into shards.
   Sharding can be performed by `clientId` or the topic of the message.

3. **Generation**.
   Each shard is additionally split into partitions called generations, each one covering a particular period of time.
   New messages are written only into the _current_ generation, while the previous generations are only accessible for reading.

   Different generations can use different layout modules to organize the data.
   In fact, in order to change the layout of the data the application must create a new generation, so the previously recorded messages remain readable without having to perform a heavy migration procedure.
   Generations can also be used for the garbage collection and message retention policies: since all messages in the generation belong to a certain interval of time, old messages can be efficiently deleted by dropping the entire generation.

4. *Stream*.
   Finally, messages in each shard and generation are split into streams.
   Every stream can contain messages from multiple topics.
   The number of streams is expected to be relatively low in comparison with the number of topics: one stream can potentially contain millions of topics.
   Various layout callback modules can employ different strategies for mapping topics into streams.

   Stream is *the only* unit of message serialization in `emqx_durable_storage` application.

   The consumer of the messages can replay the stream using an _iterator_.

## Saving messages to the durable storage

`emqx_ds` provides `store_batch/3` function that saves a list of MQTT messages to the durable storage.

Additionally, `trans/2` function can be used to perform multiple read and write operations atomically.

## Message replay

All the API functions in EMQX DS are batch-oriented.

Consumption of messages is done in several stages:

1. The consumer calls `emqx_ds:get_streams` function to get the list of streams that contain messages from a given topic filter, and a given time range.

2. `get_streams` returns the list of streams together with their shards and generations.

   The consumer must follow the below rules to avoid reordering of the messages:

   - Streams from different shards can always be replayed in parallel, regardless of their generation.
   - Streams from the same shard and generation can be replayed in parallel.
   - Groups of streams with the same shard should be replayed in order of their generation.

3. In order to start replay of the stream, the consumer calls `emqx_ds:make_iterator` function that returns an _iterator_ object.
   Iterators are the pointers to a particular position in the stream, they can be saved and restored as regular Erlang terms or using `emqx_ds:iterator_to_binary` function.

4. The consumer then proceeds to call `emqx_ds:next` function to fetch messages.
   - If this function returns `{ok, end_of_stream}`, it means the stream is fully replayed.
   - If this function returns `{ok, NextIterator, []}`, it means new messages can appear in the stream.

   Note: the consumer must implement a fair strategy for consuming messages from different streams.
   It cannot rely on an assumption that it can reach the end of a stream in a finite time.

5. The consumer must periodically refresh the list of streams as explained in 1, because new streams can appear from time to time.
   `emqx_ds_new_streams` allows to subscribe to new stream events.

## Subscriptions

EMQX DS allows to subscribe to new messages.
It's best done using `emqx_ds_client` module that automates stream management, creation of iterators, advancement of generations and error handling.

# Limitation

# Documentation links

https://docs.emqx.com/en/enterprise/latest/durability/durability_introduction.html

# Usage

# Configurations

Common global options for builtin backends are configured via OTP application environment.
Database-specific settings are stored in EMQX config.

The following application environment variables are available:

- `emqx_durable_storage.db_data_dir`: directory where the databases are located

- `emqx_durable_storage.egress_batch_size`: number of messages stored in the batch before it is committed to the durable storage.

- `emqx_durable_storage.egress_flush_interval`: period at which the batches of messages are committed to the durable storage.

# HTTP APIs

None

# Other

Note: this application contains main interface module and some common utility modules used by the backends, but it doesn't contain any ready-to-use DS backends.
The backends are instead implemented as separate OTP applications, such as `emqx_ds_backend_local` and `emqx_ds_backend_raft`.

There is a helper placeholder application `emqx_ds_backends` that depends on all backend applications available in the release.
Business logic applications must have `emqx_ds_backends` as a dependency.

The dependency diagram is the following:

```
                              +------------------------+
                              |  emqx_durable_storage  |
                              +------------------------+
                              /           |            \
                             /            |             \
                            /             |              \
   +------------------------+  +----------------------+   +------+
   | emqx_ds_backend_local  |  | emqx_ds_builtin_raft |   | ...  |
   +------------------------+  +-----------+----------+   +------+
                            \            |               /
                             \           |              /
                              \          |             /
                             +-------------------------+
                             |    emqx_ds_backends     |
                             +-------------------------+
                                 /              \
                                /                \
       ......................../.. business apps .\........................
                              /                    \
                         +------+                +-------+
                         | emqx |                |  ...  |
                         +------+                +-------+
```

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
