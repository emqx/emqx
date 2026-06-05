# EMQX Cluster Config Sync

This plugin periodically synchronizes selected cluster configuration from a
primary EMQX cluster to a secondary EMQX cluster by using the existing Data
Backup APIs.

The secondary cluster calls the primary cluster to export a backup file, then
downloads that file, uploads it locally, and imports it. Selected
configuration roots are imported with EMQX's existing configuration import
semantics. Selected Mnesia table sets are imported as a snapshot, so records in
those table sets that only exist on the secondary cluster are removed. Roots
and table sets that are not selected are left unchanged.

## Configuration

Install and start the plugin on each secondary cluster. The primary cluster does
not need this plugin installed; it only needs Dashboard Data Backup APIs
reachable from the secondary cluster.

```hocon
primary {
  base_url = "https://primary.example.com:18083/api/v5"
  api_key = "sync-key"
  api_secret = "sync-secret"
  ssl {
    enable = true
    server_name_indication = "primary.example.com"
    verify = "verify_peer"
    cacertfile = "/etc/emqx/certs/primary-ca.pem"
    certfile = ""
    keyfile = ""
  }
}

sync {
  interval = "5m"
  root_keys = [
    "connectors",
    "actions",
    "sources",
    "rule_engine",
    "listeners",
    "schema_registry"
  ]
  table_sets = [
    "banned",
    "builtin_authn",
    "builtin_authz"
  ]
  timeout = "30s"
  delete_remote_backup = true
  delete_local_backup = false
}
```

The configured API key must be allowed to access Data Backup endpoints on the
primary cluster.

Supported `sync.root_keys` values are `connectors`, `actions`, `sources`,
`rule_engine`, `listeners`, `schema_registry`, `authentication`, and
`authorization`.

Rules commonly depend on connectors, actions, sources, and schema registry
objects. If `rule_engine` is synchronized without its dependencies, imports may
fail or create incomplete runtime behavior. Include those dependent roots in
`sync.root_keys` unless they already exist on the secondary cluster.

By default, synchronization also includes the `banned`, `builtin_authn`, and
`builtin_authz` table sets. These selected table sets are replaced on the
secondary cluster. Set `sync.table_sets = []` when configuration-only
synchronization is required. Supported `sync.table_sets` values are `banned`,
`builtin_authn`, `builtin_authz`, `builtin_retainer`, `psk`, and `mt`. The
primary Data Backup API does not include `dashboard_users` or `api_keys` when
called with an API key.
