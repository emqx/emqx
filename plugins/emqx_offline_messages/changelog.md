## 2.0.0

- Add PostgreSQL storage for offline messages with per-client replay progress and optional asynchronous batching of QoS 1/2 publishes. QoS 0 uses regular broker dispatch.
- Add the `emqx_offline_messages` plugin to the monorepo.
