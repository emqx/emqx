#!/usr/bin/env python3
"""Provision all EMQX Agent resources for the Pipeline Builder demo.

The Pipeline Builder lets an LLM conversationally create and manage
EMQX Agent skills, AI providers, and pipelines via MQTT.

Optional env vars (for this init script):
  EMQX_BASE_URL      — EMQX Agent plugin API base URL
                       (default: http://localhost:18083/api/v5/plugin_api/emqx_agent)
  EMQX_CORE_BASE_URL — EMQX core API base URL (default: http://localhost:18083/api/v5)
  EMQX_API_CREDS     — Basic-auth "key:secret" (default: key:secret)
  OPENAI_BASE_URL    — OpenAI-compatible base URL (default: https://api.openai.com/v1)
  OPENAI_MODEL       — Model name              (default: gpt-5.4-mini)

Required env var:
  OPENAI_API_KEY     — OpenAI API key

Usage:
  python3 demo_builder_init.py
"""

import base64
import json
import os
import sys
import urllib.error
import urllib.request


def env(name: str, default: str | None = None) -> str:
    value = os.getenv(name)
    if value is None:
        if default is None:
            raise RuntimeError(f"required env var is missing: {name}")
        return default
    return value


BASE_URL = env("EMQX_BASE_URL", "http://localhost:18083/api/v5/plugin_api/emqx_agent")
CORE_BASE_URL = env("EMQX_CORE_BASE_URL", "http://localhost:18083/api/v5")
CREDS = env("EMQX_API_CREDS", "key:secret")
OPENAI_BASE_URL = env("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-5.4")
OPENAI_API_KEY = env("OPENAI_API_KEY")

FIREWORKS_BASE_URL = env("FIREWORKS_BASE_URL", "https://api.fireworks.ai/inference/v1")
FIREWORKS_MODEL = env("FIREWORKS_MODEL", "accounts/fireworks/models/kimi-k2p5")

PGHOST = env("PGHOST", "pgsql")
PGPORT = env("PGPORT", "5432")
PGDATABASE = env("PGDATABASE", "mqtt")
PGUSER = env("PGUSER", "root")
PGPASSWORD = env("PGPASSWORD", "public")

PROVIDER_NAME = "openai"
PIPELINE_ID = "pipeline-builder"

SK_CREATE_SKILL    = "builder-create-skill"
SK_CREATE_PIPELINE = "builder-create-pipeline"
SK_QUERY_SKILLS    = "builder-query-skills"
SK_QUERY_PROVIDERS = "builder-query-providers"
SK_QUERY_PIPELINES = "builder-query-pipelines"
SK_DELETE_SKILL    = "builder-delete-skill"
SK_DELETE_PIPELINE = "builder-delete-pipeline"
SK_REPLY           = "builder-reply"


# ── HTTP helpers ──────────────────────────────────────────────────────────────

def auth_header() -> str:
    raw = CREDS.encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def api_request(
    method: str,
    path: str,
    body: dict | None = None,
    *,
    base_url: str = BASE_URL,
    ok_codes: tuple[int, ...] = (200, 201, 204),
):
    url = f"{base_url}{path}"
    data = json.dumps(body).encode("utf-8") if body is not None else None
    req = urllib.request.Request(url=url, method=method, data=data)
    req.add_header("Authorization", auth_header())
    if body is not None:
        req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            payload = resp.read().decode("utf-8")
            if resp.status not in ok_codes:
                raise RuntimeError(
                    f"{method} {path} failed: HTTP {resp.status}: {payload}"
                )
            return payload
    except urllib.error.HTTPError as e:
        payload = e.read().decode("utf-8", errors="replace")
        if e.code in ok_codes:
            return payload
        raise RuntimeError(
            f"{method} {path} failed: HTTP {e.code}: {payload}"
        ) from e


def api_delete_maybe(path: str, *, base_url: str = BASE_URL) -> None:
    """DELETE the resource; ignore 404 and 409 (active / in-use)."""
    try:
        api_request("DELETE", path, base_url=base_url, ok_codes=(200, 204, 404, 409))
    except RuntimeError:
        pass


# ── Cleanup ───────────────────────────────────────────────────────────────────

def deactivate_pipeline(pid: str, p: dict) -> None:
    api_request("PUT", f"/pipelines/{pid}", {**p, "active": False})


def delete_pipeline(pid: str) -> None:
    api_request("DELETE", f"/pipelines/{pid}", ok_codes=(200, 204, 404))


def delete_old_assets() -> None:
    # Pipelines first — deactivate active ones, then delete
    for p in json.loads(api_request("GET", "/pipelines")):
        pid = p["pipeline_id"]
        if p.get("active"):
            deactivate_pipeline(pid, p)
            print(f"  deactivated pipeline {pid!r}")
        delete_pipeline(pid)
        print(f"  deleted pipeline {pid!r}")

    # Skills
    for s in json.loads(api_request("GET", "/skills")):
        api_delete_maybe(f"/skills/{s['type']}/{s['skill_id']}")
        print(f"  deleted skill {s['type']}@{s['skill_id']!r}")

    api_delete_maybe(f"/ai/providers/{PROVIDER_NAME}", base_url=CORE_BASE_URL)
    print(f"  deleted AI provider {PROVIDER_NAME!r}")


# ── AI providers ───────────────────────────────────────────────────────────────

def create_ai_providers() -> None:
    api_delete_maybe(f"/ai/providers/{PROVIDER_NAME}", base_url=CORE_BASE_URL)
    api_request(
        "POST",
        "/ai/providers",
        {
            "name": PROVIDER_NAME,
            "type": "openai",
            "api_key": OPENAI_API_KEY,
            "base_url": OPENAI_BASE_URL,
        },
        base_url=CORE_BASE_URL,
        ok_codes=(200, 201, 204),
    )
    print(f"  AI provider {PROVIDER_NAME!r} created")

# ── Skills ─────────────────────────────────────────────────────────────────────

def create_skills() -> None:
    meta_skills = [
        ("agent__create_skill",    SK_CREATE_SKILL),
        ("agent__create_pipeline", SK_CREATE_PIPELINE),
        ("agent__query_skills",    SK_QUERY_SKILLS),
        ("agent__query_providers", SK_QUERY_PROVIDERS),
        ("agent__query_pipelines", SK_QUERY_PIPELINES),
        ("agent__delete_skill",    SK_DELETE_SKILL),
        ("agent__delete_pipeline", SK_DELETE_PIPELINE),
    ]
    for skill_type, skill_id in meta_skills:
        api_request("POST", "/skills", {"type": skill_type, "id": skill_id})
        print(f"  skill {skill_type}@{skill_id!r} created")

    api_request(
        "POST",
        "/skills",
        {
            "type": "message__publish",
            "id": SK_REPLY,
            "desc": "Send a reply from the pipeline builder back to the chat UI",
            "topic_prefix": "builder/reply/",
        },
    )
    print(f"  skill message__publish@{SK_REPLY!r} created")


# ── Builder prompt ─────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a Pipeline Architect for EMQX Agent — an intelligent system that helps users \
design, create, and manage event-driven automation pipelines over MQTT.

You have access to tools that let you fully manage the EMQX Agent runtime: query, \
create and delete skills and pipelines, and query AI providers.

═══════════════════════════════════════════════════════
CORE CONCEPTS
═══════════════════════════════════════════════════════

SKILLS are named capability instances registered in the skill registry.
Each skill has a TYPE (what it does) and an ID (its name within that type).
Skills are referenced in pipeline steps as "type@id", e.g. "message__publish@my-notifier".

An AI PROVIDER holds LLM credentials.
It is referenced by provider_name inside an llm_loop step.
Providers are pre-created by administrators and are not modified by agent tools.

A PIPELINE reacts to MQTT events and executes an ordered list of steps.
Steps can call skills, run LLM reasoning loops, wait for more MQTT events,
or break out early based on conditions.
Every trigger event spawns a new pipeline INSTANCE that runs the steps in sequence.

═══════════════════════════════════════════════════════
HOW SKILLS, PROVIDERS, AND PIPELINES FIT TOGETHER
═══════════════════════════════════════════════════════

Building a working pipeline requires these steps:

  1. Create SKILLS — register the capabilities the pipeline will use.
     Skills are stateless; they just describe what MQTT topics to publish to,
     what HTTP endpoint to call, what SQL to execute, etc.

  2. Choose an existing AI PROVIDER for llm_loop provider_name.
  3. Create the PIPELINE — wire everything together.
     Reference skills inside steps as "type@id".
     Reference the provider by name inside llm_loop steps.

═══════════════════════════════════════════════════════
SKILL TYPES
═══════════════════════════════════════════════════════

  message__publish   Publish to an MQTT topic.
                    Required: id, desc, topic_prefix
                    The LLM supplies "topic" (appended to prefix) and "payload".

  message__request   MQTT request/reply — publishes with a Response-Topic header
                    and blocks until a reply arrives.
                    Required: id, desc, topic_prefix
                    Optional: request_payload_schema

  http              HTTP call to an external service.
                    Required: id, desc, method (get|post|put|patch|delete), url,
                              input_schema
                    Optional: headers (static map)

  postgresql__query  Execute a parameterised SQL query.
                    Required: id, desc, query (use $1 $2 … placeholders),
                              arg_keys (ordered list mapping args -> $N),
                              input_schema

═══════════════════════════════════════════════════════
PIPELINE STEP TYPES
═══════════════════════════════════════════════════════

--- call_skill ---
Invokes a registered skill directly (no LLM involved).
The "skill" field is "type@id". The "args" map is passed to the skill;
values starting with "$." are resolved from the pipeline context at runtime.
The skill result is written to result_path.

  {"id": "notify", "type": "call_skill",
   "skill": "message__publish@my-notifier",
   "args": {"topic": "$.event.device_id", "payload": "$.analysis"},
   "result_path": "$.notify_result"}

--- llm_loop ---
Starts an LLM session using an AI provider.
The LLM receives the "input" map as its first user message, then calls tools
(skills listed in "tools") until it either calls the built-in set_result tool
(when set_result_schema is provided) or sends a final frame.

   {"id": "analyse", "type": "llm_loop",
    "provider_name": "my-provider",
    "model": "gpt-5.4-mini",
   "instructions": "You are a quality inspector. Examine the photo and return a verdict.",
   "stop_on_finish": true,
   "tools": ["message__request@box-camera", "message__publish@box-alert"],
   "input": {"box_id": "$.event.box_id", "conveyor": "$.event.conveyor_id"},
   "set_result_schema": {
     "type": "object",
     "properties": {
       "verdict": {"type": "string", "enum": ["pass", "fail"]},
       "reason":  {"type": "string"}
     },
     "required": ["verdict", "reason"]
   },
   "result_path": "$.analysis"}

stop_on_finish controls session lifecycle:
  true  — session is discarded after each trigger; every event starts fresh (default).
  false — session persists and accumulates conversation history across triggers
          (useful for ongoing reasoning or multi-turn interactions).

Session IDs are stable: "<pipeline_id>-<step_id>".

When you create an llm_loop step, use model "gpt-5.4-mini" unless the user
explicitly requests another model. Do not invent a model name.

--- wait_for_event ---
Pauses the pipeline until a message arrives on an MQTT topic filter.
Optional "where" filter matches a field in the incoming payload against a
context value: "data.<field> == $.<context_path>".

  {"id": "wait_ack", "type": "wait_for_event",
   "topic": "evt/device/+/ack",
   "where": "data.ref_id == $.event.id",
   "result_path": "$.ack"}

--- break ---
Stops the pipeline early when a condition on the context is met.

  {"id": "skip_if_ok", "type": "break",
   "path": "$.analysis.verdict", "eq": "pass"}

═══════════════════════════════════════════════════════
PIPELINE CONTEXT AND JSONPATH
═══════════════════════════════════════════════════════

Every pipeline instance maintains a context map.
  $.event         — the trigger message payload (available from the first step)
  $.my_step       — result written by a step with result_path "$.my_step"
  $.my_step.field — deep read from a previous step's result

In "args" and "input" maps, any string value that starts with "$." is
substituted with the resolved context value at runtime.
result_path writes only to top-level keys (one level deep).

═══════════════════════════════════════════════════════
YOUR WORKFLOW
═══════════════════════════════════════════════════════

1. Understand what the user wants to automate.
2. Query existing skills, AI providers, and pipelines to avoid duplication.
3. Design the pipeline on paper: what triggers it, what data flows through,
   which skills are needed, whether an LLM reasoning step is required.
4. Create skills first, then the pipeline using an existing provider.
5. Confirm with a plain-language summary of what was created and how to trigger it.

If the user wants to modify something: query it first to see its current state,
then recreate it (create overwrites). To remove an asset, check it is not in use
before deleting (the delete tools enforce this automatically).

═══════════════════════════════════════════════════════
REPLYING — MANDATORY
═══════════════════════════════════════════════════════

You MUST call message_publish_builder-reply as the LAST action of every response,
without exception. Do not stop without calling it. The user will not receive
your answer unless you call this tool.

  topic   — "response" (or any short label)
  payload — a plain-language summary covering:
              • what was created / modified / deleted
              • the pipeline trigger topic (how to activate it)
              • what data the trigger payload should contain
              • any environment variables that must be set on the EMQX node

Keep replies concise and human-friendly. \
Show JSON only when the user explicitly asks for it.\
"""


# ── Pipeline ───────────────────────────────────────────────────────────────────

def create_pipeline() -> None:
    api_request(
        "POST",
        "/pipelines",
        {
            "pipeline_id": PIPELINE_ID,
            "active": True,
            "trigger": {"topic": "evt/builder/request"},
            "steps": [
                {
                    "id": "build",
                    "type": "llm_loop",
                    "provider_name": PROVIDER_NAME,
                    "model": OPENAI_MODEL,
                    "instructions": SYSTEM_PROMPT,
                    "stop_on_finish": False,
                    "tools": [
                        f"agent__create_skill@{SK_CREATE_SKILL}",
                        f"agent__create_pipeline@{SK_CREATE_PIPELINE}",
                        f"agent__query_skills@{SK_QUERY_SKILLS}",
                        f"agent__query_providers@{SK_QUERY_PROVIDERS}",
                        f"agent__query_pipelines@{SK_QUERY_PIPELINES}",
                        f"agent__delete_skill@{SK_DELETE_SKILL}",
                        f"agent__delete_pipeline@{SK_DELETE_PIPELINE}",
                        f"message__publish@{SK_REPLY}",
                    ],
                    "set_result_schema": {
                        "type": "object",
                        "properties": {"summary": {"type": "string"}},
                        "required": ["summary"],
                    },
                    "input": {"message": "$.event.message"},
                    "result_path": "$.build_result",
                }
            ],
        },
    )
    print(f"  pipeline {PIPELINE_ID!r} created")


# ── Database setup ────────────────────────────────────────────────────────────

def create_db_table() -> None:
    sql = (
        "CREATE TABLE IF NOT EXISTS apple_box_inspections ("
        "  id SERIAL PRIMARY KEY,"
        "  conveyor_id TEXT NOT NULL,"
        "  box_id TEXT NOT NULL,"
        "  status TEXT NOT NULL,"
        "  reason TEXT,"
        "  inspected_at TIMESTAMPTZ DEFAULT NOW()"
        ")"
    )
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(
            host=PGHOST, port=int(PGPORT), dbname=PGDATABASE,
            user=PGUSER, password=PGPASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print("  table apple_box_inspections ready (psycopg2)")
    except ImportError:
        import subprocess
        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = PGPASSWORD
        result = subprocess.run(
            ["psql", f"--host={PGHOST}", f"--port={PGPORT}",
             f"--dbname={PGDATABASE}", f"--username={PGUSER}", "--command", sql],
            env=env_vars, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"psql failed: {result.stderr}")
        print("  table apple_box_inspections ready (psql)")


# ── Main ───────────────────────────────────────────────────────────────────────

def main() -> int:
    print("==> Removing old builder assets")
    delete_old_assets()

    print("==> Creating apple_box_inspections table")
    create_db_table()

    print("==> Creating skills (7 meta-skills + 1 reply)")
    create_skills()

    print("==> Creating AI providers")
    create_ai_providers()

    print("==> Creating pipeline")
    create_pipeline()

    ui_base = BASE_URL.rstrip("/").replace("/api/v5/plugin_api/emqx_agent", "")
    print("\nDone.")
    print(f"Builder UI:  {ui_base}/api/v5/plugin_api/emqx_agent/builder/ui")
    print(f"Request topic:  evt/builder/request  (payload: {{\"message\": \"...\"}})")
    print(f"Reply topic:    builder/reply")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
