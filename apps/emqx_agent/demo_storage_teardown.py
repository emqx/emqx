#!/usr/bin/env python3

import base64
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


def api_delete_maybe(path: str) -> None:
    url = f"{BASE_URL}{path}"
    req = urllib.request.Request(url=url, method="DELETE")
    req.add_header("Authorization", auth_header())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status not in (204, 404):
                payload = resp.read().decode("utf-8", errors="replace")
                raise RuntimeError(
                    f"DELETE {path} failed: HTTP {resp.status}: {payload}"
                )
    except urllib.error.HTTPError as e:
        if e.code in (204, 404):
            return
        payload = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DELETE {path} failed: HTTP {e.code}: {payload}") from e


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


def drop_db_assets() -> None:
    run_psql(
        """
DROP TABLE IF EXISTS demo_storage_field_boxes;
DROP TABLE IF EXISTS demo_storage_devices;
"""
    )


def delete_agent_assets() -> None:
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_FIELD_UPDATE}")
    api_delete_maybe(f"/agent/pipelines/{PIPELINE_CORE_ADMIN}")
    api_delete_maybe(f"/agent/session_profiles/{PROFILE_NAME}")

    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_GET_STATUS}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_SET_STATUS}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_SET_BOX}")
    api_delete_maybe(f"/agent/skills/postgresql.query/{SK_PG_GET_FIELD}")
    api_delete_maybe(f"/agent/skills/message.publish/{SK_PUB_CMD}")


def main() -> int:
    print("==> Removing demo-storage pipelines, profile, and skills")
    delete_agent_assets()

    print(
        f"==> Dropping demo-storage database assets on {PGHOST}:{PGPORT}/{PGDATABASE}"
    )
    drop_db_assets()

    print("\nDone. Demo-storage assets removed.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
