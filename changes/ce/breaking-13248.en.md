`builtin` durable storage backend has been replaced with the following two backends:

- `builtin_local`: A durable storage backend that doesn't support replication.
   It can't be used in a multi-node cluster.
   This backend is available in both open source and enterprise editions.
- `builtin_raft`: A durable storage backend that uses Raft algorithm for replication.
   This backend is available only in the enterprise edition.

The following Prometheus metrics have been renamed:

- `emqx_ds_egress_batches` -> `emqx_ds_buffer_batches`
- `emqx_ds_egress_batches_retry` -> `emqx_ds_buffer_batches_retry`
- `emqx_ds_egress_batches_failed` -> `emqx_ds_buffer_batches_failed`
- `emqx_ds_egress_messages` -> `emqx_ds_buffer_messages`
- `emqx_ds_egress_bytes` -> `emqx_ds_buffer_bytes`
- `emqx_ds_egress_flush_time` -> `emqx_ds_buffer_flush_time`
