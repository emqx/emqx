#!/usr/bin/env python3
"""Provision all EMQX Agent resources for the Pipeline Builder demo.

The Pipeline Builder lets an LLM conversationally create and manage
EMQX Agent skills, session profiles, and pipelines via MQTT.

Optional env vars (for this init script):
  EMQX_BASE_URL      — EMQX REST base URL  (default: http://localhost:18083/api/v5)
  EMQX_API_CREDS     — Basic-auth "key:secret" (default: key:secret)
  OPENAI_BASE_URL    — OpenAI-compatible base URL (default: https://api.openai.com/v1)
  OPENAI_MODEL       — Model name              (default: gpt-5.4-mini)

Required env var at pipeline runtime (resolved by EMQX, not this script):
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


BASE_URL = env("EMQX_BASE_URL", "http://localhost:18083/api/v5")
CREDS = env("EMQX_API_CREDS", "key:secret")
OPENAI_BASE_URL = env("OPENAI_BASE_URL", "https://api.openai.com/v1")
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-5.4")

PGHOST = env("PGHOST", "pgsql")
PGPORT = env("PGPORT", "5432")
PGDATABASE = env("PGDATABASE", "mqtt")
PGUSER = env("PGUSER", "root")
PGPASSWORD = env("PGPASSWORD", "public")

PROFILE_NAME = "openai"
PIPELINE_ID = "pipeline-builder"

SK_CREATE_SKILL    = "builder-create-skill"
SK_CREATE_SESSION  = "builder-create-session"
SK_CREATE_PIPELINE = "builder-create-pipeline"
SK_QUERY_SKILLS    = "builder-query-skills"
SK_QUERY_SESSIONS  = "builder-query-sessions"
SK_QUERY_PIPELINES = "builder-query-pipelines"
SK_DELETE_SKILL    = "builder-delete-skill"
SK_DELETE_SESSION  = "builder-delete-session"
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
    ok_codes: tuple[int, ...] = (200, 201, 204),
):
    url = f"{BASE_URL}{path}"
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


def api_delete_maybe(path: str) -> None:
    """DELETE the resource; ignore 404 and 409 (active / in-use)."""
    try:
        api_request("DELETE", path, ok_codes=(200, 204, 404, 409))
    except RuntimeError:
        pass


# ── Cleanup ───────────────────────────────────────────────────────────────────

def delete_old_assets() -> None:
    # Pipeline first (may be active)
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_ID}")
    # Meta-skills
    api_delete_maybe(f"/agent/skills/agent.create_skill/{SK_CREATE_SKILL}")
    api_delete_maybe(f"/agent/skills/agent.create_session/{SK_CREATE_SESSION}")
    api_delete_maybe(f"/agent/skills/agent.create_pipeline/{SK_CREATE_PIPELINE}")
    api_delete_maybe(f"/agent/skills/agent.query_skills/{SK_QUERY_SKILLS}")
    api_delete_maybe(f"/agent/skills/agent.query_sessions/{SK_QUERY_SESSIONS}")
    api_delete_maybe(f"/agent/skills/agent.query_pipelines/{SK_QUERY_PIPELINES}")
    api_delete_maybe(f"/agent/skills/agent.delete_skill/{SK_DELETE_SKILL}")
    api_delete_maybe(f"/agent/skills/agent.delete_session/{SK_DELETE_SESSION}")
    api_delete_maybe(f"/agent/skills/agent.delete_pipeline/{SK_DELETE_PIPELINE}")
    api_delete_maybe(f"/agent/skills/message.publish/{SK_REPLY}")
    # Session profile
    api_delete_maybe(f"/agent/session_profiles/{PROFILE_NAME}")


# ── Skills ─────────────────────────────────────────────────────────────────────

def create_skills() -> None:
    meta_skills = [
        ("agent.create_skill",    SK_CREATE_SKILL),
        ("agent.create_session",  SK_CREATE_SESSION),
        ("agent.create_pipeline", SK_CREATE_PIPELINE),
        ("agent.query_skills",    SK_QUERY_SKILLS),
        ("agent.query_sessions",  SK_QUERY_SESSIONS),
        ("agent.query_pipelines", SK_QUERY_PIPELINES),
        ("agent.delete_skill",    SK_DELETE_SKILL),
        ("agent.delete_session",  SK_DELETE_SESSION),
        ("agent.delete_pipeline", SK_DELETE_PIPELINE),
    ]
    for skill_type, skill_id in meta_skills:
        api_request("POST", "/agent/skills", {"type": skill_type, "id": skill_id})
        print(f"  skill {skill_type}@{skill_id!r} created")

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "message.publish",
            "id": SK_REPLY,
            "desc": "Send a reply from the pipeline builder back to the chat UI",
            "topic_prefix": "builder/reply/",
        },
    )
    print(f"  skill message.publish@{SK_REPLY!r} created")


# ── Session profile ────────────────────────────────────────────────────────────

SYSTEM_PROMPT = """\
You are a Pipeline Architect for EMQX Agent — an intelligent system that helps users \
design, create, and manage event-driven automation pipelines over MQTT.

You have access to tools that let you fully manage the EMQX Agent runtime: query, \
create, and delete skills, session profiles, and pipelines.

═══════════════════════════════════════════════════════
CORE CONCEPTS
═══════════════════════════════════════════════════════

SKILLS are named capability instances registered in the skill registry.
Each skill has a TYPE (what it does) and an ID (its name within that type).
Skills are referenced in pipeline steps as "type@id", e.g. "message.publish@my-notifier".

A SESSION PROFILE holds LLM credentials and a system prompt.
It is referenced by name inside an llm_loop step.
One session profile can be shared by many pipelines / steps.

A PIPELINE reacts to MQTT events and executes an ordered list of steps.
Steps can call skills, run LLM reasoning loops, wait for more MQTT events,
or break out early based on conditions.
Every trigger event spawns a new pipeline INSTANCE that runs the steps in sequence.

═══════════════════════════════════════════════════════
HOW SKILLS, SESSIONS, AND PIPELINES FIT TOGETHER
═══════════════════════════════════════════════════════

Building a working pipeline requires three separate creation steps, in this order:

  1. Create SKILLS — register the capabilities the pipeline will use.
     Skills are stateless; they just describe what MQTT topics to publish to,
     what HTTP endpoint to call, what SQL to execute, etc.

  2. Create a SESSION PROFILE — if any step is an llm_loop, you need a profile
     that tells the LLM who it is (api_key env var name, base_url, model, instructions).
     The api_key field must be the NAME of an OS environment variable (e.g. "OPENAI_API_KEY"),
     NOT the actual secret — EMQX resolves it from the environment at call time.

  3. Create the PIPELINE — wire everything together.
     Reference skills inside steps as "type@id".
     Reference the session profile by name inside llm_loop steps.

═══════════════════════════════════════════════════════
SKILL TYPES
═══════════════════════════════════════════════════════

  message.publish   Publish to an MQTT topic.
                    Required: id, desc, topic_prefix
                    The LLM supplies "topic" (appended to prefix) and "payload".

  message.request   MQTT request/reply — publishes with a Response-Topic header
                    and blocks until a reply arrives.
                    Required: id, desc, topic_prefix
                    Optional: request_payload_schema, response_schema

  http              HTTP call to an external service.
                    Required: id, desc, method (get|post|put|patch|delete), url,
                              input_schema, output_schema
                    Optional: headers (static map)

  kv.lookup         Read a value from the in-memory key-value store.
                    Required: id, desc, data_schema

  kv.put            Write a value to the in-memory key-value store.
                    Required: id, desc, data_schema

  postgresql.query  Execute a parameterised SQL query.
                    Required: id, desc, query (use $1 $2 … placeholders),
                              arg_keys (ordered list mapping args -> $N),
                              input_schema, output_schema

═══════════════════════════════════════════════════════
PIPELINE STEP TYPES
═══════════════════════════════════════════════════════

--- call_skill ---
Invokes a registered skill directly (no LLM involved).
The "skill" field is "type@id". The "args" map is passed to the skill;
values starting with "$." are resolved from the pipeline context at runtime.
The skill result is written to result_path.

  {"id": "notify", "type": "call_skill",
   "skill": "message.publish@my-notifier",
   "args": {"topic": "$.event.device_id", "payload": "$.analysis"},
   "result_path": "$.notify_result"}

--- llm_loop ---
Starts an LLM session using a session profile.
The LLM receives the "input" map as its first user message, then calls tools
(skills listed in "tools") until it either calls the built-in set_result tool
(when set_result_schema is provided) or sends a final frame.

  {"id": "analyse", "type": "llm_loop",
   "session_profile": "my-profile",
   "stop_on_finish": true,
   "tools": ["message.request@box-camera", "message.publish@box-alert"],
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
2. Query existing skills, sessions, and pipelines to avoid duplication.
3. Design the pipeline on paper: what triggers it, what data flows through,
   which skills are needed, whether an LLM reasoning step is required.
4. Create skills first, then session profile (if needed), then the pipeline.
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


def create_profile() -> None:
    api_request(
        "POST",
        "/agent/session_profiles",
        {
            "name": PROFILE_NAME,
            "api_key": "OPENAI_API_KEY",
            "base_url": OPENAI_BASE_URL,
            "model": OPENAI_MODEL,
        },
    )
    print(f"  session profile {PROFILE_NAME!r} created")


# ── Pipeline ───────────────────────────────────────────────────────────────────

def create_pipeline() -> None:
    api_request(
        "POST",
        "/agent/pipelines",
        {
            "pipeline_id": PIPELINE_ID,
            "active": True,
            "trigger": {"topic": "evt/builder/request"},
            "steps": [
                {
                    "id": "build",
                    "type": "llm_loop",
                    "session_profile": PROFILE_NAME,
                    "model": OPENAI_MODEL,
                    "instructions": SYSTEM_PROMPT,
                    "stop_on_finish": False,
                    "tools": [
                        f"agent.create_skill@{SK_CREATE_SKILL}",
                        f"agent.create_session@{SK_CREATE_SESSION}",
                        f"agent.create_pipeline@{SK_CREATE_PIPELINE}",
                        f"agent.query_skills@{SK_QUERY_SKILLS}",
                        f"agent.query_sessions@{SK_QUERY_SESSIONS}",
                        f"agent.query_pipelines@{SK_QUERY_PIPELINES}",
                        f"agent.delete_skill@{SK_DELETE_SKILL}",
                        f"agent.delete_session@{SK_DELETE_SESSION}",
                        f"agent.delete_pipeline@{SK_DELETE_PIPELINE}",
                        f"message.publish@{SK_REPLY}",
                    ],
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

    print("==> Creating skills (9 meta-skills + 1 reply)")
    create_skills()

    print("==> Creating session profile")
    create_profile()

    print("==> Creating pipeline")
    create_pipeline()

    base = BASE_URL.rstrip("/")
    ui_base = base.replace("/api/v5", "")
    print("\nDone.")
    print(f"Builder UI:  {ui_base}/api/v5/agent/builder/ui")
    print(f"Request topic:  evt/builder/request  (payload: {{\"message\": \"...\"}})")
    print(f"Reply topic:    builder/reply")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
