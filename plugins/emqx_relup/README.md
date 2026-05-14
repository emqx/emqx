# emqx_relup

EMQX hot-upgrade (relup) plugin.

This plugin provides the runtime engine that applies a `.relup` set of
code-change instructions to a running EMQX node. It is delivered as a
hidden EMQX plugin and exposes a REST API and `emqx ctl relup ...`
command, both wired through `apps/emqx_management/src/emqx_mgmt_api_relup.erl`.

The plugin lives in-tree under `plugins/emqx_relup/` and is built
through the EMQX monorepo's umbrella build:

```
make plugin-emqx_relup
```

This produces `_build/plugins/emqx_relup-<vsn>.tar.gz`.

The framework is being refactored so that the target release tarball
(e.g. `emqx-enterprise-5.10.5-ubuntu24.04-amd64.tar.gz`) is uploaded to
each node out-of-band, and the plugin's version (e.g. `5.10.4_5.10.5`)
identifies the exact upgrade hop it implements.
