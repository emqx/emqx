#!/usr/bin/env python3
"""Remove EMQX Agent demo resources.

This script deletes all Agent pipelines, skills, and connections, removes demo
AI providers, and drops the apple-box demo PostgreSQL table.

Optional env vars:
  EMQX_BASE_URL      - EMQX Agent plugin API base URL
                       (default: http://localhost:18083/api/v5/plugin_api/emqx_agent)
  EMQX_CORE_BASE_URL - EMQX core API base URL (default: http://localhost:18083/api/v5)
  EMQX_API_CREDS     - Basic-auth "key:secret" (default: key:secret)
  PGHOST / PGPORT / PGDATABASE / PGUSER / PGPASSWORD

Usage:
  python3 demo_teardown.py
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

DEMO_PROVIDER_NAMES = ("apple-inspector", "openai")


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
            print(f"  HTTP {method} {path} -> {resp.status}")
            if resp.status not in ok_codes:
                raise RuntimeError(
                    f"{method} {path} failed: HTTP {resp.status}: {payload}"
                )
            return payload
    except urllib.error.HTTPError as e:
        payload = e.read().decode("utf-8", errors="replace")
        print(f"  HTTP {method} {path} -> {e.code}")
        if e.code in ok_codes:
            return payload
        raise RuntimeError(
            f"{method} {path} failed: HTTP {e.code}: {payload}"
        ) from e


def api_delete_maybe(path: str, *, base_url: str = BASE_URL) -> bool:
    try:
        api_request("DELETE", path, base_url=base_url, ok_codes=(200, 204, 404))
        return True
    except RuntimeError:
        return False


def delete_pipelines() -> None:
    print("==> Removing pipelines")
    pipelines = json.loads(api_request("GET", "/pipelines"))
    for pipeline in pipelines:
        pid = pipeline["pipeline_id"]
        if pipeline.get("active"):
            api_request("PUT", f"/pipelines/{pid}", {**pipeline, "active": False})
            print(f"  deactivated pipeline {pid!r}")
        removed = api_delete_maybe(f"/pipelines/{pid}")
        print(f"  pipeline {pid!r}: {'removed' if removed else 'not found'}")


def delete_skills() -> None:
    print("==> Removing skills")
    skills = json.loads(api_request("GET", "/skills"))
    for skill in skills:
        skill_type = skill["type"]
        skill_id = skill.get("skill_id", skill.get("id"))
        removed = api_delete_maybe(f"/skills/{skill_type}/{skill_id}")
        print(f"  skill {skill_type}@{skill_id!r}: {'removed' if removed else 'not found'}")


def delete_connections() -> None:
    print("==> Removing connections")
    connections = json.loads(api_request("GET", "/connections"))
    for connection in connections:
        cid = connection["id"]
        removed = api_delete_maybe(f"/connections/{cid}")
        print(f"  connection {cid!r}: {'removed' if removed else 'not found'}")


def delete_demo_providers() -> None:
    print("==> Removing demo AI providers")
    for provider_name in DEMO_PROVIDER_NAMES:
        removed = api_delete_maybe(
            f"/ai/providers/{provider_name}",
            base_url=CORE_BASE_URL,
        )
        print(f"  AI provider {provider_name!r}: {'removed' if removed else 'not found'}")


def drop_db_table() -> None:
    sql = "DROP TABLE IF EXISTS apple_box_inspections"
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
        print("  table apple_box_inspections dropped (psycopg2)")
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
        print("  table apple_box_inspections dropped (psql)")


def main() -> int:
    delete_pipelines()
    delete_skills()
    delete_connections()
    delete_demo_providers()

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
