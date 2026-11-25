# Smoke Tests

Smoke tests for the `emqx_a2a` plugin. These scripts validate API endpoints, MQTT dispatch, and end-to-end LLM integration against a running EMQX instance.

## Prerequisites

- EMQX built (`make emqx-enterprise`). `smoke_api.sh` and `smoke_mqtt.sh` require EMQX running with the plugin installed. `smoke_full.sh` and `dev_cycle.sh` handle starting EMQX and installing the plugin themselves
- [mqttx CLI](https://mqttx.app/cli) installed (for MQTT tests)
- `jq` installed (for E2E output parsing)

## Scripts

### `smoke_api.sh`

Tests the plugin REST API (no LLM key needed):

- `GET /status`: plugin status
- Agent configs CRUD (create, list, get, delete, 404, 400)
- Workflows CRUD (create, list, get, delete, 404)
- Sessions list

```bash
./plugins/emqx_a2a/smoke/smoke_api.sh
```

### `smoke_mqtt.sh`

Tests MQTT request dispatch (no LLM key needed, but responses will fail without one):

- Publish `{"text": ...}` to `$a2a/request/<agent>`, verify `TaskStatusUpdateEvent` on reply topic
- Publish to nonexistent agent, verify error reply
- Publish invalid JSON, verify it's ignored
- Publish JSON-RPC format, verify dispatch

```bash
./plugins/emqx_a2a/smoke/smoke_mqtt.sh
```

### `smoke_full.sh`

Full end-to-end test including LLM API calls. Requires an API key via environment variable:

```bash
# OpenAI
OPENAI_API_KEY=sk-... ./plugins/emqx_a2a/smoke/smoke_full.sh

# Anthropic
ANTHROPIC_API_KEY=sk-ant-... ./plugins/emqx_a2a/smoke/smoke_full.sh
```

Override the model with `A2A_MODEL`:

```bash
OPENAI_API_KEY=sk-... A2A_MODEL=gpt-5.4 ./plugins/emqx_a2a/smoke/smoke_full.sh
```

What it does:

1. Reinstalls the plugin via `run-plugin-dev.sh`
2. Pushes provider/key/model config into the running node
3. Runs `smoke_api.sh`
4. Creates a test agent, publishes a request via MQTT, waits for the LLM response, validates `submitted` and `completed` status events

### `dev_cycle.sh`

Iterative development helper. Rebuilds, reinstalls, and runs smoke tests in one command:

```bash
./plugins/emqx_a2a/smoke/dev_cycle.sh           # runs both API + MQTT smoke
./plugins/emqx_a2a/smoke/dev_cycle.sh --api      # API smoke only
./plugins/emqx_a2a/smoke/dev_cycle.sh --mqtt     # MQTT smoke only
```

## Flow

```
smoke_full.sh
  │
  ├── install plugin (run-plugin-dev.sh)
  ├── configure provider + API key (emqx eval)
  ├── smoke_api.sh
  │     └── CRUD tests against /api/v5/plugin_api/emqx_a2a/
  └── E2E LLM test
        ├── create agent config via API
        ├── subscribe to $a2a/reply/e2e-echo
        ├── publish to $a2a/request/e2e-echo
        ├── wait for LLM streaming response
        └── validate submitted → working → completed events

dev_cycle.sh
  │
  ├── rebuild + reinstall plugin
  ├── smoke_api.sh
  └── smoke_mqtt.sh
```

## Environment variables

| Variable | Description |
|----------|-------------|
| `OPENAI_API_KEY` | OpenAI API key (auto-selects openai provider) |
| `ANTHROPIC_API_KEY` | Anthropic API key (auto-selects anthropic provider) |
| `A2A_MODEL` | Override default model |
| `PROFILE` | EMQX build profile (default: `emqx-enterprise`) |
| `BASE_URL` | Dashboard URL (default: `http://127.0.0.1:18083`) |
| `MQTT_HOST` | MQTT broker host (default: `127.0.0.1`) |
| `MQTT_PORT` | MQTT broker port (default: `1883`) |
| `LOGIN_USERNAME` / `LOGIN_PASSWORD` | Dashboard credentials for API tests |
