# EMQX Node Rebalance

`emqx_node_rebalance` is a part of the node evacuation/node rebalance feature in EMQX.
It implements high-level scenarios for node evacuation and rebalancing.

## Application Responsibilities

`emqx_node_rebalance` application's core concept is a _rebalance coordinator_.
_Rebalance сoordinator_ is an entity that implements the rebalancing logic and orchestrates the rebalancing process.
In particular, it:

* Enables/Disables Eviction Agent on nodes.
* Sends connection/session eviction commands to Eviction Agents according to the evacuation logic.

We have two implementations of the _rebalance coordinator_:
* `emqx_node_rebalance` - a coordinator that implements node rebalancing;
* `emqx_node_rebalance_evacuation` - a coordinator that implements node evacuation.

## EMQX Integration

`emqx_node_rebalance` is a high-level application that is loosely coupled with the rest of the system.
It uses Eviction Agent to perform the required operations.

## User Facing API

The application provides API (CLI and HTTP) to perform the following operations:
* Start/Stop rebalancing across a set of nodes or the whole cluster;
* Start/Stop evacuation of a node;
* Get the current rebalancing status of a local node.
* Get the current rebalancing status of the whole cluster.

Also, an HTTP endpoint is provided for liveness probes.

# Documentation

The rebalancing concept is described in the corresponding [EIP](https://github.com/emqx/eip/blob/main/active/0020-node-rebalance.md).

# Contributing

Please see our [contributing.md](../../CONTRIBUTING.md).
