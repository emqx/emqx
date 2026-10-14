Bound pending LwM2M downlink commands per client session with configurable `max_pending_commands` (default: 1,000) and `max_pending_bytes` (default: 16 MiB). The byte budget is estimated from encoded command contexts and CoAP requests; queued sub-binaries are compacted to avoid retaining unrelated parent data. When full, new commands receive a 4.29 (command-count limit) or 4.13 (byte limit) response over MQTT; previously accepted commands retain their FIFO order, including when limits are reduced. Expired commands release queue capacity.

Also prevent repeated commands for the same path and command type from growing the command-record eviction queue indefinitely.

Use monotonic time for the LwM2M receive window so wall-clock adjustments cannot cause downlink commands to be sent to sleeping devices.
