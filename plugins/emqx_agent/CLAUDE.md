# emqx_agent

## Running tests

```bash
# All suites (no LLM required):
make plugins/emqx_agent-ct

# Single suite (faster — use SUITE singular + ct-suite target):
make SUITE=plugins/emqx_agent/test/emqx_agent_pipeline_SUITE.erl ct-suite

# Single test case:
make SUITE=plugins/emqx_agent/test/emqx_agent_pipeline_SUITE.erl \
     CASES=t_set_result_writes_to_context ct-suite

# LLM integration tests (require API key + PostgreSQL at pgsql:5432):
OPENAI_API_KEY=sk-... \
SUITES=plugins/emqx_agent/test/emqx_agent_apple_box_SUITE.erl \
make plugins/emqx_agent-ct
```

The apple-box and builder suites are skipped automatically when `OPENAI_API_KEY` is not set.
Tests use `emqx_cth_suite` (full in-process EMQX node). Stop any running EMQX
on port 1883 first (`eaddrinuse` otherwise).

**Note:** When adding a new test suite file, `make plugins/emqx_agent-ct` may fail the first run
because the build system hasn't picked up the new file yet. Run it a second time and it will
compile and pass.

---

## Pipeline system

The agent pipeline system lets you describe multi-step automation workflows as
data. A **pipeline definition** is registered once; every time a matching MQTT
message arrives the system spawns a new **pipeline instance** that executes the
steps in order.

### Pipeline definition

```json
{
  "pipeline_id": "my-pipeline",
  "trigger": { "topic": "evt/device/+/done" },
  "steps": [ ... ]
}
```

Field | Required | Description
------|----------|------------
`pipeline_id` | ✓ | Unique identifier
`trigger.topic` | ✓ | MQTT topic filter; wildcards `+` and `#` are supported
`steps` | ✓ | Ordered list of step objects

### Step types

#### `call_skill`

Invokes a registered skill and stores the result in the context.

```json
{
  "id": "notify",
  "type": "call_skill",
  "skill": "message.publish@slack-dev",
  "args": {
    "topic": "$.event.device_id",
    "payload": "$.triage.summary"
  },
  "result_path": "$.notify_result"
}
```

Field | Description
------|------------
`skill` | `"<type>@<skill_id>"` — skill type and instance ID
`args` | Map of arguments; values starting with `$.` are resolved from context
`result_path` | JSONPath where the skill reply is written

#### `llm_loop`

Runs an LLM reasoning loop. The session keeps calling the LLM and dispatching
tool calls until the LLM calls `set_result` (if `set_result_schema` is set) or
the session sends a `final` frame.

```json
{
  "id": "inspect",
  "type": "llm_loop",
  "provider_name": "apple-inspector",
  "model": "gpt-4o",
  "instructions": "You are an apple quality inspector...",
  "stop_on_finish": true,
  "tools": ["message.request@box-shot", "message.publish@box-alert"],
  "input": { "box_id": "$.event.box_id" },
  "set_result_schema": {
    "type": "object",
    "properties": {
      "status": { "type": "string", "enum": ["approved", "rejected"] },
      "reason": { "type": "string" }
    },
    "required": ["status", "reason"]
  },
  "result_path": "$.inspection"
}
```

Field | Description
------|------------
`provider_name` | Name of a configured AI provider
`model` | Model name, e.g. `gpt-4o`
`instructions` | System prompt for the LLM (default: `"You are a helpful assistant."`)
`stop_on_finish` | Optional boolean (default `true`). When `true`, session terminates after publishing final (ephemeral mode). When `false`, session stays alive and accumulates history across events (persistent mode).
`tools` | List of `"<type>@<skill_id>"` specs exposed as LLM tools
`input` | Map sent as the first user message; values resolved from context
`set_result_schema` | JSON Schema for the built-in `set_result` tool (see below)
`result_path` | Where the LLM result is written in context

**`set_result` built-in tool** — when `set_result_schema` is present, a
`set_result` tool is injected into the manifest. When the LLM calls it the args
are stored; on `final` they are written to `result_path` instead of the
session's own `result` field. This gives typed, schema-validated structured
output without parsing free-form text.

**Session lifecycle**: Session IDs are **stable** (`<pipeline_id>-<step_id>`).
With `stop_on_finish: true` (default), each trigger spawns a fresh session with
no history. With `stop_on_finish: false`, the same session accumulates
conversation history across all triggers—useful for learning patterns over time.

#### `wait_for_event`

Pauses the pipeline until a message arrives on the given topic.

```json
{
  "id": "wait",
  "type": "wait_for_event",
  "topic": "evt/device/+/ack",
  "where": "data.ref_id == $.event.id",
  "result_path": "$.ack_event"
}
```

Field | Description
------|------------
`topic` | MQTT topic filter to wait on
`where` | Optional filter: `"<event_path> == <context_path_or_literal>"`
`result_path` | Where the matched event payload is written

#### `break`

Stops the pipeline early based on a context value.

```json
{ "id": "check", "type": "break", "path": "$.triage.is_critical", "not": false, "eq": true }
```

Field | Default | Description
------|---------|------------
`path` | — | JSONPath into context
`not` | `false` | Negate the condition
`eq` | `true` | Value to compare against (default: truthy check)

### Context and JSONPath

The pipeline maintains a `context` map available to all steps.

- Trigger event is always available as `$.event`
- Step results are written to top-level keys: `$.my_step`
- Reading supports deep paths: `$.foo.bar.baz`; writing is one level only

In `args` maps, any string value starting with `$.` is replaced by the resolved
context value.

### Session profiles

```json
{
  "name": "apple-inspector",
  "api_key": "sk-...",
  "base_url": "https://api.openai.com/v1"
}
```

### Skill types

Type | Description
-----|------------
`message.publish` | Publish a message to an MQTT topic
`message.request` | MQTT request/reply (publishes with Response-Topic, waits for reply)
`postgresql.query` | Execute a parameterised PostgreSQL query
`http` | HTTP request to an external service
`kv` | In-memory key/value store (lookup and put variants)

### Internal topic routing

Topic | Direction | Purpose
------|-----------|--------
`evt/<anything>` | → pipeline_mgr | Trigger events and wait_for_event candidates
`sess/in/<sid>/` | → session | Requests and tool results to the LLM session
`sess/out/<sid>/` | → pipeline_mgr → pipeline | Frames from the LLM session
`cap/<type>/<skill_id>/request` | → skill | Skill invocations
`cap/<type>/<skill_id>/response/<req_id>` | → pipeline_mgr → pipeline | Skill responses
`pipe/<pid>/inst/<iid>/events` | → subscribers | Pipeline lifecycle events

### Pipeline lifecycle events

Published as JSON to `pipe/<pipeline_id>/inst/<iid>/events`:

```json
{ "type": "pipeline_started",   "pipeline_id": "...", "iid": "...", "context": {...} }
{ "type": "pipeline_completed", "pipeline_id": "...", "iid": "...", "context": {...} }
{ "type": "pipeline_failed",    "pipeline_id": "...", "iid": "...", "reason": "..." }
```

---

## Writing deterministic tests (no LLM)

For `call_skill`, `wait_for_event`, and `break` steps: use MQTT publish/subscribe
directly — no mocking needed.

For `llm_loop` steps: cast `#sess_frame{}` records directly to the pipeline
process. Gen_statem guarantees the pipeline has already transitioned to
`llm_loop` (processing its queued internal `step` event) before it processes any
cast from your test process.

```erlang
Pid = emqx_agent_pipeline:whereis(Iid),
gen_statem:cast(Pid, #sess_frame{sid = Sid, frame = #{
    <<"type">> => <<"tool_request">>,
    <<"call_id">> => <<"c1">>,
    <<"tool">> => <<"set_result">>,
    <<"args">> => #{<<"status">> => <<"approved">>}
}}),
gen_statem:cast(Pid, #sess_frame{sid = Sid, frame = #{<<"type">> => <<"final">>}}).
```

Use a test provider pointed at a closed local port so the real session process
cannot produce normal LLM frames that interfere.
