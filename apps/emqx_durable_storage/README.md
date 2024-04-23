# EMQX Durable Storage

`emqx_durable_storage` (DS for short) is an application implementing durable storage for MQTT messages within EMQX.

The core design idea behind `emqx_durable_storage` is to store each message exactly once (per each replica of the database), regardless of the number of consumers, online or offline.
This makes the storage disk requirements very predictable: only the number of _published_ messages matters; the number of consumers is removed from the equation, and fan-out is practically free in terms of disk storage.

# Features

## Callback modules

### Backend

DS _backend_ is a callback module that implements `emqx_ds` behavior.

EMQX repository contains the "builtin" backend, implemented in `emqx_ds_replication_layer` module, that uses Raft algorithm for data replication, and RocksDB as the main storage.

Note that builtin backend introduces the concept of **site** to alleviate the problem of changing node names.
Site IDs are persistent, and they are randomly generated at the first startup of the node.
Each node in the cluster has a unique site ID, that is independent from the Erlang node name (`emqx@...`).

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

## Message replay

All the API functions in EMQX DS are batch-oriented.

Consumption of messages is done in several stages:

1. The consumer calls `emqx_ds:get_streams` function to get the list of streams that contain messages from a given topic filter, and a given time range.

2. `get_streams` returns the list of streams together with their _ranks_.
   The rank of the stream is a tuple with two elements, called `X` and `Y`.

   The consumer must follow the below rules to avoid reordering of the messages:

   - Streams with different `X`-ranks can always be replayed in parallel, regardless of their `Y`-rank.
   - Streams with the same `X` and `Y`-rank can be replayed in parallel.
   - Groups of streams with the same `X` rank should be replayed in order of their `Y`-rank

3. In order to start replay of the stream, the consumer calls `emqx_ds:make_iterator` function that returns an _iterator_ object.
   Iterators are the pointers to a particular position in the stream, they can be saved and restored as regular Erlang terms.

4. The consumer then proceeds to call `emqx_ds:next` function to fetch messages.
   - If this function returns `{ok, end_of_stream}`, it means the stream is fully replayed.
   - If this function returns `{ok, NextIterator, []}`, it means new messages can appear in the stream.

   Note: the consumer must implement a fair strategy for consuming messages from different streams.
   It cannot rely on an assumption that it can reach the end of a stream in a finite time.

5. The consumer must periodically refresh the list of streams as explained in 1, because new streams can appear from time to time.

# Limitation

- There is no local cache of messages, which may result in transferring the same data multiple times

# Documentation links

TBD

# Usage

Currently it's only used to implement persistent sessions.

In the future it can serve as a storage for retained messages or as a generic message buffering layer for the bridges.

# Configurations

Global options for `emqx_durable_storage` application are configured via OTP application environment.
Database-specific settings are stored in the schema table.

The following application environment variables are available:

- `emqx_durable_storage.db_data_dir`: directory where the databases are located

- `emqx_durable_storage.egress_batch_size`: number of messages stored in the batch before it is committed to the durable storage.

- `emqx_durable_storage.egress_flush_interval`: period at which the batches of messages are committed to the durable storage.

Runtime settings for the durable storages can be modified via CLI as well as the REST API.
The following CLI commands are available:

- `emqx ctl ds info` — get a quick overview of the durable storage state
- `emqx ctl ds set_replicas <DS> <Site1> <Site2> ...` — update the list of replicas for a durable storage.
- `emqx ctl ds join <DS> <Site>` — add a replica of durable storage on the site
- `emqx ctl ds leave <DS> <Site>` — remove a replica of a durable storage from the site

# HTTP APIs

The following REST APIs are available for managing the builtin durable storages:

- `/ds/sites` — list known sites.
- `/ds/sites/:site` — get information about the site (its status, current EMQX node name managing the site, etc.)
- `/ds/storages` — list durable storages
- `/ds/storages/:ds` — get information about the durable storage and its shards
- `/ds/storages/:ds/replicas` — list or update sites that contain replicas of a durable storage
- `/ds/storages/:ds/replicas/:site` — add or remove replica of the durable storage on the site

# Other
TBD

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
