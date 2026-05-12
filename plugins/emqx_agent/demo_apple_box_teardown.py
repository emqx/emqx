#!/usr/bin/env python3
"""Remove all EMQX Agent resources created by demo_apple_box_init.py.

Optional env vars:
  EMQX_BASE_URL   — EMQX Agent plugin API base URL
                    (default: http://localhost:18083/api/v5/plugin_api/emqx_agent)
  EMQX_CORE_BASE_URL — EMQX core API base URL (default: http://localhost:18083/api/v5)
  EMQX_API_CREDS  — Basic-auth "key:secret" (default: key:secret)
  PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD

Usage:
  python3 demo_apple_box_teardown.py
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

PGHOST = env("PGHOST", "pgsql")
PGPORT = env("PGPORT", "5432")
PGDATABASE = env("PGDATABASE", "mqtt")
PGUSER = env("PGUSER", "root")
PGPASSWORD = env("PGPASSWORD", "public")

PROVIDER_NAME = "apple-inspector"
PIPELINE_ID = "apple-box-inspection"

SK_SHOT = "box-shot"
SK_ALERT = "box-alert"
SK_STATUS = "box-status"
SK_REGISTER = "box-register"


def auth_header() -> str:
    raw = CREDS.encode("utf-8")
    return "Basic " + base64.b64encode(raw).decode("ascii")


def api_delete(path: str, *, base_url: str = BASE_URL) -> bool:
    """DELETE the resource. Returns True if deleted, False if not found."""
    url = f"{base_url}{path}"
    req = urllib.request.Request(url=url, method="DELETE")
    req.add_header("Authorization", auth_header())
    try:
        with urllib.request.urlopen(req, timeout=30) as resp:
            if resp.status == 404:
                return False
            return True
    except urllib.error.HTTPError as e:
        if e.code == 404:
            return False
        payload = e.read().decode("utf-8", errors="replace")
        raise RuntimeError(f"DELETE {path} failed: HTTP {e.code}: {payload}") from e


def drop_db_table() -> None:
    sql = "DROP TABLE IF EXISTS apple_box_inspections"
    try:
        import psycopg2  # type: ignore
        conn = psycopg2.connect(
            host=PGHOST, port=int(PGPORT),
            dbname=PGDATABASE, user=PGUSER, password=PGPASSWORD,
        )
        conn.autocommit = True
        with conn.cursor() as cur:
            cur.execute(sql)
        conn.close()
        print("  table apple_box_inspections dropped (psycopg2)")
    except ImportError:
        import subprocess
        env_vars = os.environ.copy()
        env_vars["PGPASSWORD"] = PGPASSWORD
        result = subprocess.run(
            ["psql",
             f"--host={PGHOST}", f"--port={PGPORT}",
             f"--dbname={PGDATABASE}", f"--username={PGUSER}",
             "--command", sql],
            env=env_vars, capture_output=True, text=True,
        )
        if result.returncode != 0:
            raise RuntimeError(f"psql failed: {result.stderr}")
        print("  table apple_box_inspections dropped (psql)")


def main() -> int:
    print("==> Removing pipeline")
    removed = api_delete(f"/pipelines/{PIPELINE_ID}")
    print(f"  pipeline {PIPELINE_ID!r}: {'removed' if removed else 'not found'}")

    print("==> Removing skills")
    for skill_type, skill_id in [
        ("message__request",  SK_SHOT),
        ("message__publish",  SK_ALERT),
        ("message__publish",  SK_STATUS),
        ("postgresql__query", SK_REGISTER),
    ]:
        removed = api_delete(f"/skills/{skill_type}/{skill_id}")
        print(f"  skill {skill_id!r}: {'removed' if removed else 'not found'}")

    print("==> Removing AI provider")
    removed = api_delete(f"/ai/providers/{PROVIDER_NAME}", base_url=CORE_BASE_URL)
    print(f"  AI provider {PROVIDER_NAME!r}: {'removed' if removed else 'not found'}")

    print("==> Dropping database table")
    try:
        drop_db_table()
    except Exception as e:
        print(f"  WARNING: could not drop table: {e}")

    print("\nDone.")
    return 0


if __name__ == "__main__":
    try:
        raise SystemExit(main())
    except Exception as e:
        print(f"ERROR: {e}", file=sys.stderr)
        raise SystemExit(1)
