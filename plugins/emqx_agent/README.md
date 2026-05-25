# EMQX Agent

EMQX Agent turns EMQX from MQTT infrastructure into an MQTT-native AI orchestration platform.

EMQX Agent enables EMQX to run multiple autonomous agents reacting to client events using EMQX connectivity capabilities.

It is not primarily a chat interface. It is built for human-less AI automation: many devices, many concurrent workflows, restricted access to external systems, and auditable tool use. Instead of wiring together a broker, API gateway, serverless runtime, AI service, workflow engine, and integration platform, EMQX Agent brings those primitives into one MQTT-native runtime.

The plugin is organized around three composable primitives:

- **Skills**: reusable, schema-validated capabilities for MQTT publishing, MQTT request/reply, HTTP calls, database queries, and agent management.
- **Sessions**: addressable LLM conversations routed over MQTT topics, with optional persistence beyond the lifecycle of any single MQTT client.
- **Pipelines**: event-driven workflows that bind MQTT events, deterministic steps, skills, and LLM reasoning loops into production automation.

This makes EMQX a place where connected-device events can directly trigger safe AI workflows: LLMs only see approved tools, skills can enforce topic and resource boundaries, sessions can track usage, and pipelines can run at EMQX scale with OTP fault isolation.

## Main Features

- **MQTT-triggered pipelines**: start workflows from MQTT topic filters, including wildcard filters such as `$evt/device/+/done`.
- **Pipeline orchestration**: compose workflows from `call_skill`, `llm_loop`, and `break` steps.
- **LLM reasoning loops**: run OpenAI-compatible model calls through configured EMQX AI providers.
- **Tool-backed agents**: expose EMQX skills to the LLM as function tools and route invocations over internal `$cap/...` MQTT topics.
- **One-shot or persistent sessions**: run stateless LLM loops per event, or reuse a session by pipeline key for conversational workflows.
- **Structured output**: use `set_result_schema` to make the LLM return schema-validated data instead of free-form text.
- **Runtime lifecycle events**: publish pipeline start, completion, and failure events to `$pipe/<pipeline_id>/inst/<iid>/events`.
- **Admin and builder UIs**: manage providers, skills, PostgreSQL connections, and pipelines from browser-based plugin pages.

## Built-In Skills

Skills are reusable capabilities that can be called directly by a pipeline step or exposed to an LLM loop as tools.

| Skill type | Purpose |
|---|---|
| `message__publish` | Publish MQTT messages under a configured topic prefix. |
| `message__request` | Send MQTT 5 request/reply messages and wait for a response. |
| `http` | Call external HTTP endpoints with schema-defined input. |
| `postgresql__query` | Execute parameterized PostgreSQL queries through a configured connection. |
| `agent__create_skill` | Let an agent create configured skills. |
| `agent__create_pipeline` | Let an agent create inactive draft pipelines. |
| `agent__query_skills` | List or inspect configured skills. |
| `agent__query_providers` | List or inspect configured AI providers. |
| `agent__query_pipelines` | List or inspect configured pipelines. |
| `agent__delete_skill` | Delete a configured skill when it is not in use. |
| `agent__delete_pipeline` | Delete a configured pipeline. |

## Pipeline Model

A pipeline definition contains an ID, an MQTT trigger, an optional key expression, and ordered steps. When an incoming MQTT message matches the trigger topic filter, EMQX Agent starts a pipeline instance with the message available as `$.event` in the pipeline context.

Supported step types:

- `call_skill`: invoke a skill such as MQTT publish, HTTP, or PostgreSQL query and write its result into context.
- `llm_loop`: start an LLM reasoning loop, expose selected skills as tools, and store the final or structured result.
- `break`: stop the pipeline early based on a context value.

Pipeline inputs can reference previous context values with JSONPath-like strings such as `$.event.device_id` or `$.inspection.status`. Pipelines can be active or draft; draft pipelines are stored but do not run until activated.

## Management Surface

The main admin UI is served through the plugin API gateway:

```text
/api/v5/plugin_api/emqx_agent/ui
```

Additional pages:

```text
/api/v5/plugin_api/emqx_agent/builder/ui
/api/v5/plugin_api/emqx_agent/apple-box/ui
```

The same management surface is available through plugin API paths under `/api/v5/plugin_api/emqx_agent`:

| Path | Purpose |
|---|---|
| `/skills` | List and create skills. |
| `/skills/:type/:id` | Get, update, or delete a skill. |
| `/skills/statuses` | Inspect runtime skill reconciliation status. |
| `/connections` | List and create skill connections. |
| `/connections/:id` | Get, update, or delete a connection. |
| `/connections/:id/start` | Enable and reconcile a connection. |
| `/connections/:id/stop` | Disable and reconcile a connection. |
| `/connections/statuses` | Inspect runtime connection status. |
| `/providers` | List configured AI providers. |
| `/pipelines` | List and create pipeline definitions. |
| `/pipelines/:id` | Get, update, or delete a pipeline. |

## Demo Pages

The plugin includes two browser demos:

- **Pipeline Builder** at `/builder/ui`: a chat-style interface for building agent workflows.
- **Apple Box Conveyor** at `/apple-box/ui`: an MQTT/agent workflow demo that simulates inspecting apple boxes.

Provision demo resources from the repository root after EMQX is running with the plugin enabled. Both demos require an OpenAI-compatible API key:

```bash
export OPENAI_API_KEY='sk-...'
```

Optional environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `EMQX_BASE_URL` | `http://localhost:18083/api/v5/plugin_api/emqx_agent` | EMQX Agent plugin API base URL. |
| `EMQX_CORE_BASE_URL` | `http://localhost:18083/api/v5` | EMQX core API base URL for AI provider management. |
| `EMQX_API_CREDS` | `key:secret` | Basic-auth API credentials. |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible API base URL. |
| `OPENAI_MODEL` | script-specific default | Model used by the demo pipeline. |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | `pgsql`, `5432`, `mqtt`, `root`, `public` | PostgreSQL connection used by demo skills. |

Provision the Apple Box Conveyor demo:

```bash
python3 plugins/emqx_agent/demo_apple_box_init.py
```

The script creates the `apple-inspector` AI provider, PostgreSQL connection, apple-box skills, database table, and active `apple-box-inspection` pipeline. Open the UI at:

```text
/api/v5/plugin_api/emqx_agent/apple-box/ui
```

Provision the Pipeline Builder demo:

```bash
python3 plugins/emqx_agent/demo_builder_init.py
```

The script creates the builder AI provider, PostgreSQL connection, builder meta-skills, reply skill, database table, and active `pipeline-builder` pipeline. Open the UI at:

```text
/api/v5/plugin_api/emqx_agent/builder/ui
```

Both scripts recreate their demo assets and may delete existing Agent demo resources before provisioning. To remove demo resources explicitly, run:

```bash
python3 plugins/emqx_agent/demo_teardown.py
```

The database setup uses `psycopg2` when available and falls back to the `psql` command-line client.

## Build And Test

Build the plugin from the repository root:

```bash
make plugin-emqx_agent
```

Run this plugin's Common Test suites:

```bash
make plugins/emqx_agent-ct
```

LLM-backed demo suites are skipped automatically when `OPENAI_API_KEY` is not set.

## Development

Build, install, enable, and start the plugin in that node:

```bash
plugins/emqx_agent/script/start_dev.sh
```

The admin UI is available through the plugin API gateway at:

```text
/api/v5/plugin_api/emqx_agent/ui
```
