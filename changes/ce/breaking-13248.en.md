`builtin` durable storage backend has been replaced with the following two backends:

- `builtin_local`: A durable storage backend that doesn't support replication.
   It can't be used in a multi-node cluster.
   This backend is available in both open source and enterprise editions.
- `builtin_raft`: A durable storage backend that uses Raft algorithm for replication.
   This backend is available enterprise edition.
