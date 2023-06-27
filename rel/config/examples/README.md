# Examples

Here are examples of how to configure EMQX.
The main purpose of the examples are to serve as a reference for configuration layout and syntax, but not a guide to how to configure EMQX.
For more information about EMQX configuration, please refer to EMQX documentation (links below).

There are two ways to extend the configuration of EMQX:

* By adding the configs to `emqx.conf` file.
* By adding the configs to a new file and include it in `emqx.conf` file. For example, add `include "mylisteners.conf"` to `emqx.conf` file and add the listeners to `mylisteners.conf`.

EMQX configuration consists of two parts: static configs and dynamic configs.

* Configs loaded from `emqx.conf` (and included files) are static configs.
* Configs added or updated from the dashboard or CLI are dynamic configs which are synced to all nodes in the cluster and stored in the data directory on each node.

It is important to note that static configs are loaded when EMQX starts and overlays on top of the dynamic configs,
to avoid confusion, it is highly recommended NOT to use the same config name for both static and dynamic configs.

## Documentation

The EMQX documentation is available at [www.emqx.io/docs/en/latest/](https://www.emqx.io/docs/en/latest/).

The EMQX Enterprise documentation is available at [docs.emqx.com/en/](https://docs.emqx.com/en/).
