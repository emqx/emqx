# `emqx_ds_builtin_raft`

Replication layer for the builtin EMQX durable storage backend that uses Raft algorithm.


Raft backend introduces the concept of **site** to alleviate the problem of changing node names.
Site IDs are persistent, and they are randomly generated at the first startup of the node.
Each node in the cluster has a unique site ID, that is independent from the Erlang node name (`emqx@...`).

## Configurations

OTP application environment variables:

- `emqx_durable_storage.reads`: `leader_preferred` | `local_preferred`.

# CLI


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
