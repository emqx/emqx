#!/usr/bin/env python3
"""Provision all EMQX Agent resources for the Apple Box Conveyor Quality Inspector demo.

Required env vars at runtime (on the EMQX node, resolved when the pipeline runs):
  OPENAI_API_KEY     — OpenAI API key

Optional env vars (for this init script):
  EMQX_BASE_URL      — EMQX REST base URL  (default: http://localhost:18083/api/v5)
  EMQX_API_CREDS     — Basic-auth "key:secret" (default: key:secret)
  OPENAI_BASE_URL    — OpenAI-compatible base URL (default: https://api.openai.com/v1)
  OPENAI_MODEL       — Model name              (default: gpt-4o)
  PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD  — PostgreSQL connection

Usage:
  python3 demo_apple_box_init.py
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
OPENAI_MODEL = env("OPENAI_MODEL", "gpt-4o")

PGHOST = env("PGHOST", "pgsql")
PGPORT = env("PGPORT", "5432")
PGDATABASE = env("PGDATABASE", "mqtt")
PGUSER = env("PGUSER", "root")
PGPASSWORD = env("PGPASSWORD", "public")

PROFILE_NAME = "apple-inspector"
PIPELINE_ID = "apple-box-inspection"

SK_SHOT = "box-shot"
SK_ALERT = "box-alert"
SK_STATUS = "box-status"
SK_REGISTER = "box-register"


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
    """DELETE the resource; ignore 404."""
    try:
        api_request("DELETE", path, ok_codes=(200, 204, 404))
    except RuntimeError:
        pass


# ── Database setup ─────────────────────────────────────────────────────────────

def create_db_table() -> None:
    """Create the inspections table via the PostgreSQL skill resource."""
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
    # Use psycopg2 if available, otherwise fall back to psql subprocess.
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(
            host=PGHOST,
            port=int(PGPORT),
            dbname=PGDATABASE,
            user=PGUSER,
            password=PGPASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print(f"  table apple_box_inspections ready (psycopg2)")
    except ImportError:
        import subprocess
        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = PGPASSWORD
        result = subprocess.run(
            [
                "psql",
                f"--host={PGHOST}",
                f"--port={PGPORT}",
                f"--dbname={PGDATABASE}",
                f"--username={PGUSER}",
                "--command",
                sql,
            ],
            env=env_vars,
            capture_output=True,
            text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"psql failed: {result.stderr}")
        print(f"  table apple_box_inspections ready (psql)")


# ── Skills ─────────────────────────────────────────────────────────────────────

def delete_old_assets() -> None:
    api_delete_maybe(f"/agent/skills/message.request/{SK_SHOT}")
    api_delete_maybe(f"/agent/skills/message.publish/{SK_ALERT}")
    api_delete_maybe(f"/agent/skills/message.publish/{SK_STATUS}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_REGISTER}")
    api_delete_maybe(f"/agent/session_profiles/{PROFILE_NAME}")
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_ID}")


def create_skills() -> None:
    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "message.request",
            "id": SK_SHOT,
            "desc": "Request a box snapshot photo from the SPA client",
            "topic_prefix": "box/shot/",
            "request_payload_schema": {"type": "object"},
            "response_schema": {
                "type": "object",
                "properties": {
                    "image_url": {
                        "type": "string",
                        "description": "Base64-encoded image as a data URI (data:image/png;base64,...)",
                    }
                },
                "required": ["image_url"],
            },
        },
    )
    print(f"  skill {SK_SHOT!r} created")

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "message.publish",
            "id": SK_ALERT,
            "desc": "Publish a box quality alert to the SPA",
            "topic_prefix": "box/alert/",
        },
    )
    print(f"  skill {SK_ALERT!r} created")

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "message.publish",
            "id": SK_STATUS,
            "desc": "Publish final box inspection status to the SPA",
            "topic_prefix": "box/status/",
        },
    )
    print(f"  skill {SK_STATUS!r} created")

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "postgresql.query",
            "id": SK_REGISTER,
            "desc": "Record box inspection result in the database",
            "query": (
                "INSERT INTO apple_box_inspections"
                "(conveyor_id, box_id, status, reason) "
                "VALUES($1, $2, $3, $4)"
            ),
            "arg_keys": ["conveyor_id", "box_id", "status", "reason"],
            "input_schema": {"type": "object"},
            "output_schema": {"type": "object"},
        },
    )
    print(f"  skill {SK_REGISTER!r} created")


def create_profile() -> None:
    api_request(
        "POST",
        "/agent/session_profiles",
        {
            "name": PROFILE_NAME,
            "api_key": "OPENAI_API_KEY",
            "base_url": OPENAI_BASE_URL,
            "model": OPENAI_MODEL,
            "instructions": (
                "You are an apple quality inspector. "
                "You will receive box_id and conveyor_id as input. "
                "Use the message_request_box_shot tool with topic=box_id "
                "to request a photo of the crate. "
                "Carefully examine the photo for rotten, moldy, bruised, or damaged apples. "
                "If you detect any defects, call message_publish_box_alert with: "
                "  reason (string), defect_type (list of strings), severity (low/medium/high). "
                "When you have reached a verdict, call set_result with: "
                "  status: 'approved' if all apples look fresh and healthy, "
                "          'rejected' if any defect is found; "
                "  reason: one sentence explaining your decision."
            ),
        },
    )
    print(f"  session profile {PROFILE_NAME!r} created")


def create_pipeline() -> None:
    api_request(
        "POST",
        "/agent/pipelines",
        {
            "pipeline_id": PIPELINE_ID,
            "active": True,
            "trigger": {"topic": "evt/conveyor/+/box/done"},
            "steps": [
                {
                    "id": "inspect",
                    "type": "llm_loop",
                    "session_profile": PROFILE_NAME,
                    "stop_on_finish": True,
                    "tools": [
                        f"message.request@{SK_SHOT}",
                        f"message.publish@{SK_ALERT}",
                    ],
                    "input": {
                        "box_id": "$.event.box_id",
                        "conveyor_id": "$.event.conveyor_id",
                    },
                    "set_result_schema": {
                        "type": "object",
                        "properties": {
                            "status": {
                                "type": "string",
                                "enum": ["approved", "rejected"],
                            },
                            "reason": {"type": "string"},
                        },
                        "required": ["status", "reason"],
                    },
                    "result_path": "$.inspection",
                },
                {
                    "id": "register",
                    "type": "call_skill",
                    "skill": f"postgresql.query@{SK_REGISTER}",
                    "args": {
                        "conveyor_id": "$.event.conveyor_id",
                        "box_id": "$.event.box_id",
                        "status": "$.inspection.status",
                        "reason": "$.inspection.reason",
                    },
                    "result_path": "$.db_result",
                },
                {
                    "id": "notify",
                    "type": "call_skill",
                    "skill": f"message.publish@{SK_STATUS}",
                    "args": {
                        "topic": "$.event.box_id",
                        "payload": "$.inspection",
                    },
                    "result_path": "$.notify_result",
                },
            ],
        },
    )
    print(f"  pipeline {PIPELINE_ID!r} created")


def main() -> int:
    print(f"==> Creating database table on {PGHOST}:{PGPORT}/{PGDATABASE}")
    create_db_table()

    print("==> Removing any existing apple-box assets")
    delete_old_assets()

    print("==> Creating skills")
    create_skills()

    print("==> Creating session profile")
    create_profile()

    print("==> Creating pipeline")
    create_pipeline()

    print("\nDone.")
    print(f"SPA UI:    {BASE_URL.rstrip('/agent')}/agent/apple-box/ui")
    print(f"Admin UI:  {BASE_URL.rstrip('/agent')}/agent/ui")
    print(f"Pipeline:  {PIPELINE_ID}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
