# EMQX Agent

EMQX Agent provides MQTT-driven agent skills, AI-provider-backed LLM sessions, and pipeline orchestration as an EMQX plugin.

## Build And Test

Build the plugin from the repository root:

```bash
make plugin-emqx_agent
```

Run this plugin's Common Test suites:

```bash
make plugins/emqx_agent-ct
```

## Development

Build, install, enable, and start the plugin in that node:

```bash
plugins/emqx_agent/smoke/load_dev.sh
```

The admin UI is available through the plugin API gateway at:

```text
/api/v5/plugin_api/emqx_agent/ui
```
