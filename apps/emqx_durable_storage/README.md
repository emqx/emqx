# EMQX Replay

`emqx_ds` is an application implementing durable storage for MQTT messages within EMQX.

# Features

- Streams. Stream is an abstraction that encompasses topics, shards, different data layouts, etc.
  The client application must only aware of the streams.

- Batching. All the API functions are batch-oriented.

- Iterators. Iterators can be stored durably or transferred over network.
  They take relatively small space.

- Support for various backends. Almost any DBMS that supports range
  queries can serve as a `emqx_durable_storage` backend.

- Builtin backend based on RocksDB.
  - Changing storage layout on the fly: it's achieved by creating a
    new set of tables (known as "generation") and the schema.
  - Sharding based on publisher's client ID

# Limitation

- Builtin backend currently doesn't replicate data across different sites
- There is no local cache of messages, which may result in transferring the same data multiple times

# Documentation links
TBD

# Usage

Currently it's only used to implement persistent sessions.

In the future it can serve as a storage for retained messages or as a generic message buffering layer for the bridges.

# Configurations

`emqx_durable_storage` doesn't have any configurable parameters.
Instead, it relies on the upper-level business applications to create
a correct configuration and pass it to `emqx_ds:open_db(DBName, Config)`
function according to its needs.

# HTTP APIs

None

# Other
TBD

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
