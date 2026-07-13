# MQTT Agent

MQTT Agent turns EMQX from MQTT infrastructure into an MQTT-native AI orchestration platform.

MQTT Agent enables EMQX to run event-driven AI automation that reacts to client events using EMQX connectivity capabilities.

Unlike popular human facing agents, it is not primarily a chat interface.
It is built for human-less AI automation: many devices, many concurrent workflows, restricted access to external systems, and auditable tool use.

Instead of wiring together a broker, API gateway, serverless runtime, AI service, workflow engine, and integration platform, MQTT Agent brings those primitives into one MQTT-native runtime.

The plugin is organized around three composable primitives available via MQTT topics:

- **Tools**: reusable, schema-validated capabilities for MQTT publishing, MQTT request/reply, HTTP calls, database queries, etc.
- **Sessions**: addressable LLM conversations routed over MQTT topics. A session is the context keeper: it owns conversation history, pending events, queued requests, tool-call state, and usage counters.
- **Pipelines**: event-triggered workflow instances that orchestrate tool and session invocations to handle MQTT events.

This makes EMQX a place where connected-device events can directly trigger safe AI workflows: LLMs only see approved tools, tools can enforce topic and resource boundaries, sessions can track usage, and pipelines can run at EMQX scale with OTP fault isolation.

## What It Enables

- **Bring AI to MQTT operations**: connected-device events can directly trigger model-assisted decisions, enrichment, inspection, classification, and follow-up actions.
- **Keep automation close to the broker**: workflows run where MQTT connectivity, routing, authorization boundaries, and operational telemetry already exist.
- **Constrain what AI can do**: LLMs receive only approved tools, and each tool can be scoped to specific topics, endpoints, databases, or streams.
- **Handle machine-scale event flow**: automation is built for many devices and many concurrent workflows, not for a single human chat session.

## MQTT Agent Interface

MQTT Agent uses MQTT topics to provide features. Agent topics use `$` prefixes so they are MQTT system topics and are not matched by ordinary `#` subscriptions.

## Tools

Tools are restricted actions that may be used directly by a pipeline step or provided to an LLM step of a pipeline.

Tools are addressed by type and id: `type@id`. Type identifies the tool implementation, e.g. make a HTTP request or query a database. `id` identifies the set of configured options and limitations for the tool.

### Tool Topics

Tool calls are MQTT request/response exchanges. The caller publishes a JSON request to the tool instance request topic (`$cap/<type>/<tool_id>/request/<req_id>`) and waits for a JSON response on the matching response topic (`$cap/<type>/<tool_id>/response/<req_id>`):

The tool decodes the request payload, validates the `args` fieldagainst the tool input schema, executes the action, and publishes the result to the response topic with the same `req_id`.

For example, invoking `message__publish@alerts` with request ID `req-42` uses:

- `$cap/message__publish/alerts/request/req-42` as the request topic
- `$cap/message__publish/alerts/response/req-42` as the response topic

The caller publishes this request payload to the request topic:

```json
// PUBLISH $cap/message__publish/alerts/request/req-42
{
  "args": {
    "topic": "factory/line-1/alerts",
    "payload": {"severity": "warning", "reason": "temperature_high"}
  },
  "iid": "pipeline-instance-id",
  "trace_id": "trace-id"
}
```

After publishing the MQTT message, the tool publishes this response payload to the response topic:

```json
// PUBLISH $cap/message__publish/alerts/response/req-42
{
  "status": "ok",
  "result": {"published": true}
}
```

### Types, Instances, And Contexts

Tool type is the generic implementation. Tool ID is a configured instance of that implementation. The instance carries a context: fixed configuration that the caller cannot change at invocation time.

For example, `postgresql__query` is a generic PostgreSQL query executor. By itself, it only knows how to render SQL parameters, run a prepared query through an EMQX PostgreSQL connection, and return rows. A configured instance makes it a narrower capability:

```json
{
  "type": "postgresql__query",
  "id": "orders_by_device",
  "desc": "Read recent orders for one device",
  "resource": "pg-main",
  "query": "select id, status, created_at from orders where device_id = ${device_id} order by created_at desc limit 10"
}
```

This creates the tool reference `postgresql__query@orders_by_device`. A pipeline or LLM step can call that reference with `{"device_id": "dev-001"}`, but it cannot choose another database connection, run arbitrary SQL, remove the `where` clause, or change the limit. Those fixed parts live in the instance context associated with `orders_by_device`.

The same pattern applies to other tool types: a `message__publish` instance fixes the publish boundary, an `http` instance fixes the endpoint shape, and stream or KV instances fix the storage target.

### Built-In Tool Types

| Tool type | Purpose |
|---|---|
| `message__publish` | Publish MQTT messages under a configured topic prefix. |
| `message__request` | Send MQTT 5 request/reply messages and wait for a response. |
| `http` | Call external HTTP endpoints with schema-defined input. |
| `postgresql__query` | Execute parameterized PostgreSQL queries through a configured connection. |
| `stream__write` | Write keyed data into an EMQX stream. |
| `stream__read` | Read keyed data from an EMQX stream. |
| `stream__del` | Delete keyed data or clear an EMQX stream. |
| `kv__write` | Write a key-value entry to a last-value EMQX stream. |
| `kv__read` | Read a key-value entry from a last-value EMQX stream. |
| `kv__read_all` | Read all key-value entries from a last-value EMQX stream. |
| `kv__del` | Delete one key-value entry from a last-value EMQX stream. |
| `kv__clear` | Clear all key-value entries from a last-value EMQX stream. |

### Image Machinery

The `http` and `message__request` tools can extract images from tool responses so multimodal data can be passed to the LLM safely. OpenAI-compatible APIs do not accept images embedded directly in tool response messages, so Agent replaces extracted images in the payload with `Image <id>` placeholders and returns the image data as separate attachments.

Image extraction supports two modes:

- `autodiscover_images`: scans response payloads for `data:image/...;base64,...` values.
- `images`: explicitly selects image locations with paths such as `.image_url` or `.` for the root value.

Binary image responses can also be extracted when the response content type is an image media type such as `image/png`.

#### Autodiscovery example

Assume an HTTP tool returns JSON with an inline data URI:

```json
{
  "inspection_status": "accepted",
  "image_url": "data:image/png;base64,iVBORw0KGgoAAA...",
  "comment": "front camera frame"
}
```

With `autodiscover_images` enabled, the tool response contains the sanitized result plus extracted attachments:

```json
{
  "status": "ok",
  "result": {
    "inspection_status": "accepted",
    "image_url": "Image .image_url",
    "comment": "front camera frame"
  },
  "attachments": [
    {
      "id": ".image_url",
      "type": "image",
      "mime_type": "image/png",
      "data": "iVBORw0KGgoAAA..."
    }
  ]
}
```

The `result` field is further passed to an LLM as the tool response, and `attachments` are passed as additional multimodal data.

#### Explicit path example

If the response has several image-like fields, configure only the one the model should inspect:

```json
{
  "autodiscover_images": false,
  "images": [".inspection.photo"]
}
```

For this response:

```json
{
  "inspection": {
    "photo": "data:image/jpeg;base64,/9j/4AAQSk...",
    "thumbnail": "data:image/jpeg;base64,/9j/2wBD..."
  }
}
```

Only `.inspection.photo` is extracted; `thumbnail` stays as ordinary payload data. The full tool response looks like this:

```json
{
  "status": "ok",
  "result": {
    "inspection": {
      "photo": "Image .inspection.photo",
      "thumbnail": "data:image/jpeg;base64,/9j/2wBD..."
    }
  },
  "attachments": [
    {
      "id": ".inspection.photo",
      "type": "image",
      "mime_type": "image/jpeg",
      "data": "/9j/4AAQSk..."
    }
  ]
}
```

#### Binary response example

If an HTTP endpoint returns raw PNG bytes with `Content-Type: image/png`, the binary is treated as the root "value":

```text
Content-Type: image/png

<raw PNG bytes>
```

The root payload is represented as `Image .` and the PNG bytes are attached separately:

```json
{
  "status": "ok",
  "result": "Image .",
  "attachments": [
    {
      "id": ".",
      "type": "image",
      "mime_type": "image/png",
      "data": "iVBORw0KGgoAAA..."
    }
  ]
}
```

### Meta-Tools

Meta-tools let build pipelines that modify Agent configuration. They are ordinary tools, but are usually exposed only to trusted builder workflows.

- `agent__create_tool`
- `agent__update_tool`
- `agent__delete_tool`
- `agent__query_tools`
- `agent__create_pipeline`
- `agent__update_pipeline`
- `agent__delete_pipeline`
- `agent__query_pipelines`
- `agent__insert_pipeline_step`
- `agent__update_pipeline_step`
- `agent__delete_pipeline_step`
- `agent__query_providers`
- `agent__query_connections`

## Sessions

Sessions are addressable LLM state machines routed over MQTT topics. A session owns conversation history, pending events, queued requests, tool-call state, and usage counters.

Session traffic uses two topic schemas:

- `$sess/in/<sid>` -- inbound frames to the session.
- `$sess/out/<sid>` -- outbound frames from the session.

Each session is identified by a cluster-unique `sid` (session ID).

Inbound frames on `$sess/in/<sid>`:

| Frame type | Purpose |
|---|---|
| `request` | Start LLM work with provider, model, instructions, input, tools, and persistence settings. |
| `tool_result` | Return the result of a tool call requested by the session. |
| `event` | Add new event context to the next LLM turn. |
| `stop` | Explicitly terminate the session. |

Outbound frames on `$sess/out/<sid>`:

| Frame type | Purpose |
|---|---|
| `intermediate` | Stream an intermediate model chunk before the turn finishes. Carries `chunk_type` such as `content` and the chunk bytes in `chunk`. |
| `tool_request` | Ask the waiting pipeline to invoke a tool through `$cap/...`. |
| `final` | Finish the current LLM turn and return result plus usage counters. |
| `error` | Report a session-side failure, such as an unavailable provider or history compaction error. |

Every outbound frame includes `sid`, `iid`, `trace_id`, and accumulated `usage`. Model reasoning/thinking chunks are kept inside the session today; only published stream chunks appear as `intermediate` frames.

Enabled persistence means session does not stop after `final` is published. It continues to exist and may receive further requests, forming a multi-turn conversation.

## Pipelines

A pipeline definition contains an ID, an MQTT trigger, and ordered steps. When an incoming MQTT message matches the trigger topic filter, MQTT Agent starts one pipeline instance with the message available as `$.event` in the pipeline context.

Pipeline trigger topics are ordinary MQTT topic filters matched against `$evt/...` event topics, for example:

```text
$evt/device/+/done
```

Pipeline lifecycle events are published as JSON to:

```text
$pipe/<pipeline_id>/inst/<iid>/events
```

Supported step types:

- `call_tool`: invoke a tool such as MQTT publish, HTTP, PostgreSQL, KV, or stream storage and write its result into context.
- `llm_loop`: send work to a session, expose selected tools as LLM tools, and store the final or structured result when the session replies.
- `break`: stop the pipeline early based on a context value.

### Pipeline Context

The pipeline context is a binary-keyed map shared by all steps in one pipeline instance. It starts with `{event: event_payload}`. Step inputs can reference previous values with JSONPath-like strings such as `$.event.device_id` or `$.inspection.status`; step outputs are written with `result_path` set for the step, for example `$.inspection`.

### Pipeline Logic

A pipeline is a single-turn handler: one trigger event creates one pipeline instance, the instance coordinates ordered work for that event, publishes completion or failure, and terminates. It is not a long-running human-facing agent loop.

This is intentional. A human-facing agent often relies on a human adding prompts back into the main session with feedback, making a turn-based dialog the natural outer model. MQTT Agent targets humanless interaction: events arrive from devices, broker hooks, subscriptions, rules, external systems, and other automated sources. There is no single human dialog to keep alive. Instead, pipelines handle individual events, sessions provide LLM continuity when needed, and `kv_*` or `stream_*` tools provide explicit workflow memory across events.

Pipelines can be active or draft; draft pipelines are stored but do not run until activated.

### LLM Step Key Expression

To emulate multi-turn dialogs, one may still use `llm_loop` step with a persistent LLM session. In this case, this step for each pipeline instance will use the session identifier by the step's _key expression_. Using different key expressions we may have a single session per `clientid`, topic, or other criteria.

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
| `/tools` | List and create tools. |
| `/tools/:type/:id` | Get, update, or delete a tool. |
| `/tools/statuses` | Inspect runtime tool reconciliation status. |
| `/connections` | List and create tool connections. |
| `/connections/:id` | Get, update, or delete a connection. |
| `/connections/:id/start` | Enable and reconcile a connection. |
| `/connections/:id/stop` | Disable and reconcile a connection. |
| `/connections/statuses` | Inspect runtime connection status. |
| `/providers` | List configured AI providers. |
| `/pipelines` | List and create pipeline definitions. |
| `/pipelines/:id` | Get, update, or delete a pipeline. |

## Demo Pages

The plugin includes two browser demos:

- **Pipeline Builder** at `/builder/ui`: a chat-style interface for building event-driven AI workflows.
- **Apple Box Conveyor** at `/apple-box/ui`: an MQTT/AI workflow demo that simulates inspecting apple boxes.

Provision demo resources from the repository root after EMQX is running with the plugin enabled. Both demos require an OpenAI-compatible API key:

```bash
export OPENAI_API_KEY='sk-...'
```

Optional environment variables:

| Variable | Default | Purpose |
|---|---|---|
| `EMQX_BASE_URL` | `http://localhost:18083/api/v5/plugin_api/emqx_agent` | MQTT Agent plugin API base URL. |
| `EMQX_CORE_BASE_URL` | `http://localhost:18083/api/v5` | EMQX core API base URL for AI provider management. |
| `EMQX_API_CREDS` | `key:secret` | Basic-auth API credentials. |
| `OPENAI_BASE_URL` | `https://api.openai.com/v1` | OpenAI-compatible API base URL. |
| `OPENAI_MODEL` | script-specific default | Model used by the demo pipeline. |
| `PGHOST`, `PGPORT`, `PGDATABASE`, `PGUSER`, `PGPASSWORD` | `pgsql`, `5432`, `mqtt`, `root`, `public` | PostgreSQL connection used by demo tools. |

Provision the Apple Box Conveyor demo:

```bash
python3 plugins/emqx_agent/demo_apple_box_init.py
```

The script creates the `apple-inspector` AI provider, PostgreSQL connection, apple-box tools, database table, and active `apple-box-inspection` pipeline. Open the UI at:

```text
/api/v5/plugin_api/emqx_agent/apple-box/ui
```

Provision the Pipeline Builder demo:

```bash
python3 plugins/emqx_agent/demo_builder_init.py
```

The script creates the builder AI provider, PostgreSQL connection, builder meta-tools, reply tool, database table, and active `pipeline-builder` pipeline. Open the UI at:

```text
/api/v5/plugin_api/emqx_agent/builder/ui
```

Both scripts recreate their demo assets and may delete existing Agent demo resources before provisioning. To remove demo resources explicitly, run:

```bash
python3 plugins/emqx_agent/demo_teardown.py
```

## Build And Test

Build the plugin from the repository root:

```bash
make plugin-emqx_agent
```

Run this plugin's Common Test suites:

```bash
make plugins/emqx_agent-ct
```

LLM-backed demo suites require a capable LLM. So they run only when `OPENAI_API_KEY` is set, otherwise skipped.

## Development

Build, install, enable, and start the plugin in that node:

```bash
plugins/emqx_agent/script/start_dev.sh
```

The admin UI is available through the plugin API gateway at:

```text
/api/v5/plugin_api/emqx_agent/ui
```
