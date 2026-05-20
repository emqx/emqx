#!/usr/bin/env python3
"""
Pytest tests for EMQX Docker entrypoint behavior.

Usage:
    pytest scripts/test/test_docker_entrypoint.py -v
"""

import os
import subprocess
from pathlib import Path


def run_entrypoint_with_hostname(tmp_path, hostname_script):
    workspace_root = Path(__file__).parent.parent.parent
    entrypoint = workspace_root / "deploy" / "docker" / "docker-entrypoint.sh"
    fake_bin = tmp_path / "bin"
    fake_bin.mkdir()
    hostname = fake_bin / "hostname"
    hostname.write_text(hostname_script)
    hostname.chmod(0o755)

    env = os.environ.copy()
    env["PATH"] = f"{fake_bin}:{env['PATH']}"
    for name in [
        "EMQX_HOST",
        "EMQX_NAME",
        "EMQX_NODE_NAME",
        "EMQX_NODE__NAME",
        "EMQX_CLUSTER__DISCOVERY_STRATEGY",
        "EMQX_CLUSTER__DNS__RECORD_TYPE",
        "EMQX_CLUSTER__K8S__ADDRESS_TYPE",
        "EMQX_CLUSTER__K8S__NAMESPACE",
    ]:
        env.pop(name, None)

    return subprocess.run(
        [str(entrypoint), "env"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10,
    )


def test_entrypoint_falls_back_to_interface_ip_when_hostname_resolution_fails(tmp_path):
    result = run_entrypoint_with_hostname(
        tmp_path,
        """#!/usr/bin/env bash
if [ "$1" = "-i" ]; then
  echo "hostname: Temporary failure in name resolution" >&2
  exit 1
fi
if [ "$1" = "-I" ]; then
  echo "172.17.0.2 10.0.0.3"
  exit 0
fi
echo "emqx-container"
""",
    )

    assert result.returncode == 0
    assert "EMQX_HOST=172.17.0.2" in result.stdout
    assert "EMQX_NODE_NAME=emqx@172.17.0.2" in result.stdout


def test_entrypoint_fails_when_node_host_cannot_be_determined(tmp_path):
    result = run_entrypoint_with_hostname(
        tmp_path,
        """#!/usr/bin/env bash
if [ "$1" = "-i" ] || [ "$1" = "-I" ]; then
  echo "hostname: Temporary failure in name resolution" >&2
  exit 1
fi
echo "emqx-container"
""",
    )

    assert result.returncode != 0
    assert "Failed to determine EMQX node host" in result.stderr
    assert "EMQX_NODE_NAME=emqx@" not in result.stdout
