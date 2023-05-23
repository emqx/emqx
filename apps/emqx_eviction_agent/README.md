# EMQX Eviction Agent

`emqx_eviction_agent` is a part of the node evacuation/node rebalance feature in EMQX.
It is a low-level application that encapsulates working with actual MQTT connections.

## Application Responsibilities

`emqx_eviction_agent` application:

* Blocks incoming connection to the node it is running on.
* Serves as a facade for connection/session eviction operations.
* Reports blocking status via HTTP API.

The `emqx_eviction_agent` is relatively passive and has no eviction/rebalancing logic. It allows
`emqx_node_rebalance` to perform eviction/rebalancing operations using high-level API, without having to deal with
MQTT connections directly.

## EMQX Integration

`emqx_eviction_agent` interacts with the following EMQX components:
* `emqx_cm` - to get the list of active MQTT connections;
* `emqx_hooks` subsystem - to block/unblock incoming connections;
* `emqx_channel` and the corresponding connection modules to perform the eviction.

## User Facing API

The application provided a very simple API (CLI and HTTP) to inspect the current blocking status.

# Documentation

The rebalancing concept is described in the corresponding [EIP](https://github.com/emqx/eip/blob/main/active/0020-node-rebalance.md).

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
