# EMQX Durable Timer

This application implements functionality similar to `timer:apply_after`, but timers survive restart of the node that started them.

A timer that has been successfully created will be eventually executed, unless canceled.

# Features

- Timers have millisecond precision.

- Upgrade safety.
  Events are handled by the nodes that know how to do so.

- "Dead Hand" activation: timers can be set up to fire after sudden stop of the node that created them.

# Limitation

- This application only guarantees that the events are executed _later_ than the specified timeout.
  There are no guarantees about the earliest time of execution.

- These timers are not lightweight.
  Operations with the timers involve `emqx_durable_storage` transactions that cause disk I/O and sending data over network.

- Code may be executed on an arbitrary node.
  There are no guarantees that timer will fire on the same node that created it.

- Events may fire multiple times.

## TODO

- Improve node down detection.

# Documentation links

# Usage

## Configuration

This application is configured via application environment variables and `emqx_conf`.

Warning: heartbeat and health configuration must be consistent across the cluster.

- `heartbeat_interval`: Node heartbeat interval in milliseconds.
  Default: 5000 ms.
- `missed_heartbeats`: If remote node fails to update the heartbeat this many times in a row, it's considered down.
  Default: 5.
- `replay_retry_interval`: Timers are executed by replaying a DS topic.
  When fetching batches from DS fails, last batch will be retried after this interval.

  Note: failures in the timers' handle timeout callback are NOT retried.

- Durable storage settings are set via `emqx_ds_schema:db_config_timers()`


# HTTP APIs

None

# Other

# Contributing
Please see our [contributing.md](../../CONTRIBUTING.md).
