#!/usr/bin/env python3

import base64
import json
import os
import subprocess
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

FIREWORKS_API_KEY = env("FIREWORKS_API_KEY")
FIREWORKS_BASE_URL = env("FIREWORKS_BASE_URL", "https://api.fireworks.ai/inference/v1")
FIREWORKS_MODEL = env("FIREWORKS_MODEL", "accounts/fireworks/models/kimi-k2p5")

PGHOST = env("PGHOST", "pgsql")
PGPORT = env("PGPORT", "5432")
PGDATABASE = env("PGDATABASE", "mqtt")
PGUSER = env("PGUSER", "root")
PGPASSWORD = env("PGPASSWORD", "public")

PROFILE_NAME = "demo-storage-kimi-ctrl"

PIPELINE_FIELD_UPDATE = "demo-storage-field-update"
PIPELINE_CORE_ADMIN = "demo-storage-core-admin"

SK_PG_GET_STATUS = "demo-storage-pg-get-status"
SK_PG_SET_STATUS = "demo-storage-pg-set-status"
SK_PG_SET_BOX = "demo-storage-pg-set-box"
SK_PG_GET_FIELD = "demo-storage-pg-get-field"
SK_PUB_CMD = "demo-storage-cmd"


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
    data = None
    if body is not None:
        data = json.dumps(body).encode("utf-8")
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
        raise RuntimeError(f"{method} {path} failed: HTTP {e.code}: {payload}") from e


def api_delete_maybe(path: str) -> None:
    api_request("DELETE", path, body=None, ok_codes=(204, 404))


def probe_fireworks() -> None:
    payload = {
        "model": FIREWORKS_MODEL,
        "messages": [{"role": "user", "content": "ping"}],
        "max_tokens": 8,
    }
    url = f"{FIREWORKS_BASE_URL}/chat/completions"
    req = urllib.request.Request(
        url=url, method="POST", data=json.dumps(payload).encode("utf-8")
    )
    req.add_header("Authorization", f"Bearer {FIREWORKS_API_KEY}")
    req.add_header("Content-Type", "application/json")
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status != 200:
                text = resp.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"fireworks probe failed: HTTP {resp.status}: {text}"
                )
    except urllib.error.HTTPError as e:
        text = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"fireworks probe failed: HTTP {e.code}: {text}") from e


def run_psql(sql: str) -> None:
    env_map = os.environ.copy()
    env_map["PGPASSWORD"] = PGPASSWORD
    cmd = [
        "psql",
        "-h",
        PGHOST,
        "-p",
        PGPORT,
        "-U",
        PGUSER,
        "-d",
        PGDATABASE,
        "-v",
        "ON_ERROR_STOP=1",
    ]
    proc = subprocess.run(cmd, input=sql.encode("utf-8"), env=env_map, check=False)
    if proc.returncode != 0:
        raise RuntimeError("psql command failed")


def recreate_db_assets() -> None:
    run_psql(
        """
DROP TABLE IF EXISTS demo_storage_field_boxes;
DROP TABLE IF EXISTS demo_storage_devices;

CREATE TABLE demo_storage_devices (
    device_id TEXT PRIMARY KEY,
    shape TEXT NOT NULL,
    status TEXT NOT NULL CHECK (status IN ('free', 'parked'))
);

CREATE TABLE demo_storage_field_boxes (
    x INT NOT NULL,
    y INT NOT NULL,
    device_id TEXT NOT NULL,
    PRIMARY KEY (x, y)
);

INSERT INTO demo_storage_devices(device_id, shape, status) VALUES
('dev-1', 'L', 'free'),
('dev-2', 'T', 'free'),
('dev-3', 'I', 'free'),
('dev-4', 'O', 'free');
"""
    )


def delete_old_assets() -> None:
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_FIELD_UPDATE}")
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_CORE_ADMIN}")
    api_delete_maybe(f"/agent/session_profiles/{PROFILE_NAME}")

    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_GET_STATUS}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_SET_STATUS}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_SET_BOX}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_GET_FIELD}")
    api_delete_maybe(f"/agent/skills/message.publish/{SK_PUB_CMD}")


def create_skills() -> None:
    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "postgresql.query",
            "id": SK_PG_GET_STATUS,
            "desc": "Get demo-storage device status by device_id.",
            "query": "SELECT status FROM demo_storage_devices WHERE device_id = $1",
            "arg_keys": ["device_id"],
            "input_schema": {
                "type": "object",
                "properties": {"device_id": {"type": "string"}},
                "required": ["device_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {
                    "rows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "status": {"type": "string", "enum": ["free", "parked"]}
                            },
                            "required": ["status"],
                        },
                    }
                },
                "required": ["rows"],
            },
        },
    )

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "postgresql.query",
            "id": SK_PG_SET_STATUS,
            "desc": "Set demo-storage device status to parked.",
            "query": "UPDATE demo_storage_devices SET status = 'parked' WHERE device_id = $1",
            "arg_keys": ["device_id"],
            "input_schema": {
                "type": "object",
                "properties": {"device_id": {"type": "string"}},
                "required": ["device_id"],
            },
            "output_schema": {
                "type": "object",
                "properties": {"num_rows": {"type": "integer"}},
            },
        },
    )

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "postgresql.query",
            "id": SK_PG_SET_BOX,
            "desc": "Insert four occupied boxes for a parked device.",
            "query": (
                "INSERT INTO demo_storage_field_boxes(device_id, x, y) VALUES "
                "($1,$2,$3),($4,$5,$6),($7,$8,$9),($10,$11,$12) "
                "ON CONFLICT (x,y) DO NOTHING"
            ),
            "arg_keys": [
                "device_id",
                "x1",
                "y1",
                "device_id",
                "x2",
                "y2",
                "device_id",
                "x3",
                "y3",
                "device_id",
                "x4",
                "y4",
            ],
            "input_schema": {
                "type": "object",
                "properties": {
                    "device_id": {"type": "string"},
                    "x1": {"type": "integer"},
                    "y1": {"type": "integer"},
                    "x2": {"type": "integer"},
                    "y2": {"type": "integer"},
                    "x3": {"type": "integer"},
                    "y3": {"type": "integer"},
                    "x4": {"type": "integer"},
                    "y4": {"type": "integer"},
                },
                "required": [
                    "device_id",
                    "x1",
                    "y1",
                    "x2",
                    "y2",
                    "x3",
                    "y3",
                    "x4",
                    "y4",
                ],
            },
            "output_schema": {
                "type": "object",
                "properties": {"num_rows": {"type": "integer"}},
            },
        },
    )

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "postgresql.query",
            "id": SK_PG_GET_FIELD,
            "desc": "Get occupied field map.",
            "query": "SELECT x, y, device_id FROM demo_storage_field_boxes ORDER BY y, x",
            "arg_keys": [],
            "input_schema": {"type": "object", "properties": {}},
            "output_schema": {
                "type": "object",
                "properties": {
                    "rows": {
                        "type": "array",
                        "items": {
                            "type": "object",
                            "properties": {
                                "x": {"type": "integer"},
                                "y": {"type": "integer"},
                                "device_id": {"type": "string"},
                            },
                            "required": ["x", "y", "device_id"],
                        },
                    }
                },
                "required": ["rows"],
            },
        },
    )

    api_request(
        "POST",
        "/agent/skills",
        {
            "type": "message.publish",
            "id": SK_PUB_CMD,
            "desc": "Send a command to a device on demo-storage/cmd/<device_id>.",
            "topic_prefix": "demo-storage/cmd/",
            "payload_schema": {
                "type": "object",
                "oneOf": [
                    {
                        "type": "object",
                        "properties": {"command": {"type": "string", "enum": ["park"]}},
                        "required": ["command"],
                    },
                    {
                        "type": "object",
                        "properties": {
                            "command": {"type": "string", "enum": ["rotate"]}
                        },
                        "required": ["command"],
                    },
                    {
                        "type": "object",
                        "properties": {
                            "command": {"type": "string", "enum": ["move"]},
                            "direction": {
                                "type": "string",
                                "enum": ["left", "right", "down"],
                            },
                            "distance": {"type": "integer", "minimum": 1},
                        },
                        "required": ["command", "direction", "distance"],
                    },
                ],
            },
        },
    )


def create_profile() -> None:
    api_request(
        "POST",
        "/agent/session_profiles",
        {
            "name": PROFILE_NAME,
            "api_key": FIREWORKS_API_KEY,
            "base_url": FIREWORKS_BASE_URL,
            "model": FIREWORKS_MODEL,
            "instructions": (
                "You are a strict pipeline controller. Always obey rules exactly. "
                "Use tools only when rules require. If a required tool call was performed, "
                "return compact JSON status."
            ),
            "output_schema": {
                "type": "object",
                "properties": {
                    "action": {"type": "string"},
                    "command": {"type": "string"},
                },
            },
        },
    )


def create_pipelines() -> None:
    api_request(
        "POST",
        "/agent/pipelines",
        {
            "pipeline_id": PIPELINE_FIELD_UPDATE,
            "trigger": {"topic": "evt/demo-storage/tele/+"},
            "steps": [
                {
                    "id": "load_status",
                    "type": "call_skill",
                    "skill": f"postgresql.query@{SK_PG_GET_STATUS}",
                    "args": {"device_id": "$.event.device_id"},
                    "result_path": "$.db_status",
                },
                {
                    "id": "break_if_not_parked",
                    "type": "break",
                    "path": "$.event.parked",
                    "not": True,
                },
                {
                    "id": "apply_parked_state",
                    "type": "llm_loop",
                    "session_profile": PROFILE_NAME,
                    "tools": [
                        f"postgresql.query@{SK_PG_SET_STATUS}",
                        f"postgresql.query@{SK_PG_SET_BOX}",
                    ],
                    "input": {
                        "event": "$.event",
                        "db_status": "$.db_status",
                        "rules": {
                            "when_not_parked": "no_tool_call",
                            "when_already_parked": "no_tool_call",
                            "otherwise": "set_status_then_set_boxes",
                            "boxes_count": 4,
                        },
                    },
                    "session_config": {
                        "api_key": FIREWORKS_API_KEY,
                        "base_url": FIREWORKS_BASE_URL,
                        "model": FIREWORKS_MODEL,
                        "instructions": (
                            "You are a strict tool planner for storage parking updates. "
                            "Input has event and db_status. Rules: (1) If event.parked=false then call no tool "
                            'and return {"action":"noop_not_parked"}. (2) If db_status.rows[0].status '
                            'is parked then call no tool and return {"action":"noop_already_parked"}. '
                            "(3) Otherwise call exactly two tools in this order: first pg set status with "
                            "{device_id:event.device_id}; second pg set box with four box coordinates from "
                            "event.boxes[0..3] and a single device_id field reused for all rows. "
                            "Do not call any other tool."
                        ),
                        "output_schema": {
                            "type": "object",
                            "properties": {"action": {"type": "string"}},
                        },
                    },
                    "result_path": "$.field_update_result",
                },
            ],
        },
    )

    api_request(
        "POST",
        "/agent/pipelines",
        {
            "pipeline_id": PIPELINE_CORE_ADMIN,
            "trigger": {"topic": "evt/demo-storage/tele/+"},
            "steps": [
                {"id": "stop_if_parked", "type": "break", "path": "$.event.parked"},
                {
                    "id": "load_field",
                    "type": "call_skill",
                    "skill": f"postgresql.query@{SK_PG_GET_FIELD}",
                    "args": {},
                    "result_path": "$.field_map",
                },
                {
                    "id": "choose_next_command",
                    "type": "llm_loop",
                    "session_profile": PROFILE_NAME,
                    "tools": [
                        f"message.publish@{SK_PUB_CMD}",
                        f"postgresql.query@{SK_PG_GET_STATUS}",
                    ],
                    "input": {
                        "event": "$.event",
                        "field_map": "$.field_map",
                        "field_width": 14,
                        "field_height": 18,
                        "rules": {
                            "one_action_only": True,
                            "park_when_bottom": True,
                            "otherwise_move_down": True,
                            "command_topic_suffix": "event.device_id",
                        },
                    },
                    "session_config": {
                        "api_key": FIREWORKS_API_KEY,
                        "base_url": FIREWORKS_BASE_URL,
                        "model": FIREWORKS_MODEL,
                        "instructions": (
                            "You are a strict single-step controller for storage devices. "
                            "Goal: pack devices from the bottom with minimal free cells and no overlaps. "
                            "Input has event, field_map, field_width, field_height. "
                            'If event.parked=true then do not call tools and return {"command":"none"}. '
                            "Evaluate legal next actions: rotate, move(left|right|down,distance>=1), park. "
                            "Prefer actions that reduce holes and stack height. "
                            "Choose move side and distance from field_map and event.boxes. "
                            "Use rotate when it improves compaction or reachability. "
                            "Send park only when further legal moves do not improve compaction. "
                            "Call at most one tool."
                        ),
                        "output_schema": {
                            "type": "object",
                            "properties": {"command": {"type": "string"}},
                        },
                    },
                    "result_path": "$.core_result",
                },
            ],
        },
    )


def main() -> int:
    print("==> Probing Fireworks model")
    probe_fireworks()

    print(
        f"==> Recreating demo-storage database assets on {PGHOST}:{PGPORT}/{PGDATABASE}"
    )
    recreate_db_assets()

    print("==> Removing existing demo-storage tools, profile, and pipelines")
    delete_old_assets()

    print("==> Creating demo-storage tools")
    create_skills()

    print("==> Creating demo-storage session profile")
    create_profile()

    print("==> Creating demo-storage pipelines")
    create_pipelines()

    print("\nDone.")
    print(f"UI: {BASE_URL}/agent/demo-storage/ui")
    print(f"Pipelines: {PIPELINE_FIELD_UPDATE}, {PIPELINE_CORE_ADMIN}")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
