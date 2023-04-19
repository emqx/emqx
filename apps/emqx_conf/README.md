# Configuration Management

This application provides configuration management capabilities for EMQX.

This includes, during compilation:
- Read all configuration schemas and generate the following files:
  * `config-en.md`: documentation for all configuration options.
  * `schema-en.json`: JSON description of all configuration schema options.
  * `emqx.conf.example`: an example of a complete configuration file.

At runtime, it provides:
- Cluster configuration synchronization capability.
  Responsible for synchronizing hot-update configurations from the HTTP API to the entire cluster
  and ensuring consistency.

In addition, this application manages system-level configurations such as `cluster`, `node`, `log`.
