# EMQX Agent

EMQX Agent provides MQTT-driven agent skills, session profiles, and pipeline orchestration as an EMQX plugin.

## Build And Test

Build the plugin from the repository root:

```bash
make plugin-emqx_agent
```

Run this plugin's Common Test suites:

```bash
./scripts/ct/run.sh --app "plugins/emqx_agent" -- env TERM=dumb make plugins/emqx_agent-ct
```

## Development

Start a local release node:

```bash
plugins/emqx_agent/smoke/start_dev.sh
```

Build, install, enable, and start the plugin in that node:

```bash
plugins/emqx_agent/smoke/load_dev.sh
```

The admin UI is available through the plugin API gateway at:

```text
/api/v5/plugin_api/emqx_agent/ui
```
