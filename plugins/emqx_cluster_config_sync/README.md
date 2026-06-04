# EMQX Cluster Config Sync

This plugin periodically synchronizes selected cluster configuration from a
primary EMQX cluster to a secondary EMQX cluster by using the existing Data
Backup APIs.

The secondary cluster calls the primary cluster to export a backup file, then
downloads that file, uploads it locally, and imports it with EMQX's existing
merge/update import semantics. It does not delete configuration that only
exists on the secondary cluster.

## Configuration

Enable the plugin on the secondary cluster:

```hocon
enable = true
role = "secondary"

primary {
  base_url = "https://primary.example.com:18083/api/v5"
  api_key = "sync-key"
  api_secret = "sync-secret"
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
  delete_local_backup = true
}
```

On the primary cluster, keep the plugin disabled or set `role = "primary"`.
The configured API key must be allowed to access Data Backup endpoints on the
primary cluster.

Rules commonly depend on connectors, actions, sources, and schema registry
objects. If `rule_engine` is synchronized without its dependencies, imports may
fail or create incomplete runtime behavior.

By default, synchronization also includes the `banned`, `builtin_authn`, and
`builtin_authz` table sets. Set `sync.table_sets = []` when configuration-only
synchronization is required. Other table sets, such as `builtin_retainer`, must
be added explicitly.
