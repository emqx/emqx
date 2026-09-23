# emqx_a2a: Agent Orchestration Plugin

Run AI agents as Erlang processes inside the EMQX broker. Trigger them via MQTT, get streaming responses back via MQTT. Chain agents into DAG workflows with automatic dependency resolution.

Designed for IoT scenarios: agents that interpret device telemetry, send commands, diagnose issues, and coordinate across device fleets, all within the broker with no external service required.

## How it works

```
IoT Device                     EMQX Broker (emqx_a2a plugin)           MQTT Client
    │                              │                                        │
    │  telemetry/state             │                                        │
    │ ──────────────────────────►  │                                        │
    │                              │  ◄──── "$a2a/request/hvac-advisor" ────│
    │                              │  Hook intercepts request               │
    │                              │  → Session manager creates session     │
    │                              │  → Spawns worker (Erlang process)      │
    │                              │  → Worker calls LLM API (httpc + SSE)  │
    │                              │                                        │
    │                              │  ──── stream response ────────────────►│
    │  ◄─── device command ─────── │  Agent decides to send command         │
    │                              │  ──── completed ──────────────────────►│
```

For workflows, the plugin runs a DAG of agents: entry tasks start immediately, downstream tasks wait for their dependencies, then run with upstream results injected as context.

## Prerequisites

- EMQX 6.x with the `emqx_a2a_registry` app (for agent discovery cards)
- An API key for OpenAI or Anthropic

## Installation

The plugin builds as part of the EMQX monorepo:

```bash
make plugin-emqx_a2a
# produces _build/plugins/emqx_a2a-0.1.0.tar.gz
```

Install via the EMQX dashboard (Management → Plugins → Install) or the CLI:

```bash
emqx ctl plugins install emqx_a2a-0.1.0.tar.gz
emqx ctl plugins start emqx_a2a-0.1.0
```

## Configuration

Configure via the dashboard plugin settings page, or in `config.hocon`:

| Setting | Default | Description |
|---------|---------|-------------|
| `provider` | `openai` | LLM provider: `openai` or `anthropic` |
| `api_key` | `""` | API key for the selected provider |
| `default_model` | `gpt-5-mini` | Default model when agent config doesn't specify one |
| `default_max_tokens` | `4096` | Default max output tokens |
| `request_topic_prefix` | `$a2a/request/` | Topic prefix the hook listens on |

Common model values:
- **OpenAI**: `gpt-5-mini`, `gpt-5.4`
- **Anthropic**: `claude-sonnet-4-6`, `claude-haiku-4-6`

## Quick start: HVAC advisor agent

This example creates an agent that reads live device state from MQTT retained messages, advises operators, and sends commands back to devices.

### 1. Register an agent execution config

The `context_topics` field tells the worker to read retained messages from those topics before calling the LLM. The agent automatically gets live device state as context.

```bash
curl -s -u admin:public -X POST \
  'http://localhost:18083/api/v5/plugin_api/emqx_a2a-0.1.0/agent_configs' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "hvac-advisor",
    "system_prompt": "You are an HVAC system advisor for a smart building. You receive live device state from retained MQTT messages. Analyze sensor readings and provide actionable recommendations. When you determine a device needs adjustment, include mqtt-action blocks in your response to send commands directly. Be concise and specific.",
    "model": "gpt-5-mini",
    "max_tokens": 1024,
    "context_topics": ["devices/ahu-+/state", "devices/zone-+/sensors"]
  }'
```

### 2. Devices publish retained state (as they normally would)

```bash
# AHU publishes its state as retained
mosquitto_pub -t 'devices/ahu-03/state' -r \
  -m '{"supply_temp": 28.5, "setpoint": 22, "fan_speed": 100, "compressor_pct": 94}'

# Zone sensors publish readings as retained
mosquitto_pub -t 'devices/zone-3/sensors' -r \
  -m '{"temperature": 28.5, "humidity": 72, "occupancy": true}'
```

### 3. Ask the agent

```bash
mosquitto_pub -t '$a2a/request/hvac-advisor' \
  -m '{"text": "Zone 3 is too hot and energy usage is high. Fix it."}'
```

The worker automatically reads the retained messages from `devices/ahu-+/state` and `devices/zone-+/sensors`, injects them into the prompt as "Current device state", then calls the LLM.

### 4. Subscribe for streaming results + actions

```bash
mosquitto_sub -t '$a2a/reply/#' -v
```

The agent streams back its analysis and includes action blocks:

```json
{"type": "TaskStatusUpdateEvent", "task_id": "...", "status": {"state": "working", "message": "AHU-03 supply temp is 28.5°C, well above the 22°C setpoint..."}}
{"type": "TaskStatusUpdateEvent", "task_id": "...", "status": {"state": "working", "message": "...lowering setpoint and reducing fan speed.\n```mqtt-action\n{\"topic\": \"devices/ahu-03/commands\", \"payload\": {\"cmd\": \"setSetpoint\", \"params\": {\"temp\": 20}}}\n```"}}
{"type": "TaskActionEvent", "task_id": "...", "action": {"topic": "devices/ahu-03/commands", "payload_size": 55}}
{"type": "TaskStatusUpdateEvent", "task_id": "...", "status": {"state": "completed", "message": "..."}}
```

The `TaskActionEvent` confirms the command was published to `devices/ahu-03/commands`. The device receives:

```json
{"cmd": "setSetpoint", "params": {"temp": 20}}
```

### mqtt-action block format

When an agent config has `context_topics`, the system prompt is automatically extended with instructions for action blocks. The LLM can include any number of these in its response:

````
```mqtt-action
{"topic": "devices/ahu-03/commands", "payload": {"cmd": "setSetpoint", "params": {"temp": 20}}}
```
````

Optional fields: `"qos": 1`, `"retain": true`. Each block is parsed and published after the LLM finishes.

### Using with EMQX rules

You can wire device telemetry directly to agents using EMQX's rule engine. For example, a rule that triggers the agent when a sensor reports an anomaly:

```sql
SELECT payload, topic FROM "devices/+/telemetry"
WHERE payload.temperature > 30
```

Action: Republish to `$a2a/request/hvac-advisor` with a templated prompt.

## Workflows: anomaly detection pipeline

Workflows chain multiple agents into a DAG. Tasks with no `needs` run in parallel; downstream tasks wait for all dependencies before starting.

This example builds a pipeline: **diagnose → assess impact → recommend fix**.

### 1. Register agent configs

```bash
# Diagnostician: interprets raw sensor data + reads device state
curl -s -u admin:public -X POST \
  'http://localhost:18083/api/v5/plugin_api/emqx_a2a-0.1.0/agent_configs' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "diagnostician",
    "system_prompt": "You are an IoT device diagnostician. Given raw sensor readings and device metadata, identify anomalies, correlate symptoms, and produce a structured diagnosis. Output: anomaly type, severity (low/medium/high/critical), affected components, probable root cause.",
    "context_topics": ["devices/+/state"]
  }'

# Impact assessor: evaluates business impact
curl -s -u admin:public -X POST \
  'http://localhost:18083/api/v5/plugin_api/emqx_a2a-0.1.0/agent_configs' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "impact-assessor",
    "system_prompt": "You are an operations impact analyst. Given a device diagnosis, assess the operational and business impact: affected zones, user comfort, energy cost, equipment risk. Classify urgency as routine/soon/immediate."
  }'

# Remediation planner: can send device commands via mqtt-action blocks
curl -s -u admin:public -X POST \
  'http://localhost:18083/api/v5/plugin_api/emqx_a2a-0.1.0/agent_configs' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "remediation-planner",
    "system_prompt": "You are a remediation planner for building automation systems. Given a diagnosis and impact assessment, produce a concrete action plan. For immediate device actions, include mqtt-action blocks to send commands directly.",
    "context_topics": ["devices/+/state"]
  }'
```

### 2. Create the workflow

```bash
curl -s -u admin:public -X POST \
  'http://localhost:18083/api/v5/plugin_api/emqx_a2a-0.1.0/workflows' \
  -H 'Content-Type: application/json' \
  -d '{
    "id": "anomaly-pipeline",
    "name": "Anomaly Detection Pipeline",
    "variables": ["device_id", "readings"],
    "tasks": [
      {
        "id": "diagnose",
        "agent": "diagnostician",
        "description": "Diagnose device {device_id}. Raw readings: {readings}",
        "needs": []
      },
      {
        "id": "assess_impact",
        "agent": "impact-assessor",
        "description": "Assess operational impact for device {device_id}.",
        "needs": ["diagnose"]
      },
      {
        "id": "plan_fix",
        "agent": "remediation-planner",
        "description": "Create remediation plan for device {device_id}.",
        "needs": ["diagnose", "assess_impact"],
        "next": "output"
      }
    ]
  }'
```

This creates the following DAG:

```
diagnose ──┬──► assess_impact ──┐
           │                    ├──► plan_fix ──► output
           └────────────────────┘
```

### 3. Trigger the workflow

An automation rule or operator publishes to `$a2a/request/workflow-{id}`:

```bash
mosquitto_pub -t '$a2a/request/workflow-anomaly-pipeline' \
  -m '{
    "text": "Analyze anomaly",
    "variables": {
      "device_id": "ahu-floor3-west",
      "readings": "supply_temp: 31.2°C (setpoint 14°C), return_temp: 26.8°C, fan_speed: 100%, compressor_current: 18.5A (rated 15A), discharge_pressure: 2.8MPa (normal 1.8-2.2MPa), suction_pressure: 0.15MPa (normal 0.3-0.5MPa)"
    }
  }'
```

The plugin will:
1. Run `diagnose`: identifies probable refrigerant leak based on pressure readings
2. Run `assess_impact` with diagnosis: classifies as "immediate" due to compressor overcurrent
3. Run `plan_fix` with both upstream results: outputs specific actions (shut down compressor, open bypass damper, create maintenance ticket)

### Other IoT workflow ideas

- **Fleet firmware rollout**: validate compatibility → plan rollout order → generate per-device commands
- **Energy optimization**: collect baseline readings → identify waste patterns → generate setpoint schedule
- **Commissioning assistant**: read device identity → validate wiring/sensors → generate test sequence → verify responses

## REST API

All endpoints are under the plugin API path: `/api/v5/plugin_api/emqx_a2a-0.1.0/`

| Method | Path | Description |
|--------|------|-------------|
| GET | `/status` | Plugin status (counts, default model) |
| GET | `/agent_configs` | List all agent execution configs |
| GET | `/agent_configs/:id` | Get an agent config |
| POST | `/agent_configs` | Create or update an agent config |
| DELETE | `/agent_configs/:id` | Delete an agent config |
| GET | `/workflows` | List all workflow definitions |
| GET | `/workflows/:id` | Get a workflow definition |
| POST | `/workflows` | Create or update a workflow |
| DELETE | `/workflows/:id` | Delete a workflow |
| GET | `/sessions` | List all sessions (running and completed) |
| GET | `/sessions/:id` | Get session details with task states |
| DELETE | `/sessions/:id` | Delete a session |

## Agent config vs. registry card

This plugin works alongside `emqx_a2a_registry`:

| Concern | emqx_a2a_registry | emqx_a2a plugin |
|---------|-------------------|-----------------|
| "Who am I?" | Agent discovery cards on `$a2a/v1/discovery/` | N/A |
| "How do I run?" | N/A | Agent execution configs (system prompt, model, tools) |
| Workflows | N/A | DAG definitions, session management |
| Execution | N/A | Erlang worker processes calling OpenAI/Anthropic API |

The `registry_id` field in an agent config can optionally link to a registry card's `{org_id, unit_id, agent_id}` tuple for cross-referencing.

## Smoke tests

See [`smoke/README.md`](smoke/README.md) for details. Quick start:

```bash
# API-only (no LLM key needed)
./plugins/emqx_a2a/smoke/smoke_api.sh

# Full E2E with LLM
OPENAI_API_KEY=sk-... ./plugins/emqx_a2a/smoke/smoke_full.sh

# Dev cycle: rebuild + reinstall + smoke
./plugins/emqx_a2a/smoke/dev_cycle.sh
```

## Architecture

```
emqx_a2a_app
└── emqx_a2a_sup
    ├── emqx_a2a_store         ETS tables: agent configs, workflows, sessions
    ├── emqx_a2a_worker_sup    simple_one_for_one dynamic supervisor
    │   ├── emqx_a2a_worker    gen_server per task (calls LLM, streams to MQTT)
    │   ├── emqx_a2a_worker    ...
    │   └── ...
    └── emqx_a2a_session_mgr   Session lifecycle + DAG dependency resolution

emqx_a2a          Hook on message.publish, intercepts $a2a/request/+ topics
emqx_a2a_llm      Multi-provider LLM client via httpc (OpenAI + Anthropic, SSE streaming)
emqx_a2a_config   Plugin settings via persistent_term
emqx_a2a_api      REST API handler
```

Each worker is a short-lived gen_server that:
1. Builds messages from the prompt + upstream context
2. Calls the LLM API (OpenAI or Anthropic) with SSE streaming via httpc
3. Forwards text deltas to the MQTT reply topic as `TaskStatusUpdateEvent` messages
4. Reports completion to the session manager, which unblocks downstream tasks

Workers are monitored. If one crashes, the session manager marks the task as failed and notifies the caller.

## Relation to NexHub

This plugin brings a subset of [NexHub](../../../nexhub/)'s capabilities natively into the EMQX broker:

| Capability | NexHub | emqx_a2a plugin |
|------------|--------|-----------------|
| LLM-powered device management | External TypeScript service | Erlang processes inside the broker |
| MQTT integration | MQTT client connecting to EMQX | Native broker hook (zero-hop) |
| Device thing models | SQLite + Drizzle ORM | Prompt-based (system prompt describes the domain) |
| Multi-agent workflows | Single agent with tools | DAG workflow engine with parallel execution |
| Streaming responses | MQTT pub/sub | MQTT pub/sub (identical protocol) |
| Device commands | Agent tool → MQTT publish | Future: agent tool → emqx_broker:safe_publish |

The plugin is intentionally simpler. It focuses on the agent orchestration layer and relies on the broker's existing infrastructure (rules engine, retainer, registry) for device integration.
