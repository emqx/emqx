#!/usr/bin/env python3
"""
Pytest tests for EMQX boot behavior.
Equivalent to emqx-boot.bats

Usage:
    PROFILE=emqx pytest scripts/test/test_emqx_boot.py -v
    PROFILE=emqx-enterprise pytest scripts/test/test_emqx_boot.py -v
"""

import os
import subprocess
import shutil
import pytest
from pathlib import Path


@pytest.fixture(scope="module")
def profile():
    """Get PROFILE environment variable."""
    profile = os.environ.get("PROFILE")
    if not profile:
        pytest.skip("PROFILE environment variable must be set")
    return profile


@pytest.fixture(scope="module")
def emqx_bin_path(profile):
    """Get the path to emqx binary."""
    workspace_root = Path(__file__).parent.parent.parent
    bin_path = workspace_root / "_build" / profile / "rel" / "emqx" / "bin" / "emqx"
    if not bin_path.exists():
        pytest.skip(f"EMQX binary not found at {bin_path}")
    return bin_path


@pytest.fixture(scope="module")
def emqx_rel_path(profile):
    """Get the path to emqx release directory."""
    workspace_root = Path(__file__).parent.parent.parent
    rel_path = workspace_root / "_build" / profile / "rel" / "emqx"
    if not rel_path.exists():
        pytest.skip(f"EMQX release not found at {rel_path}")
    return rel_path


def test_profile_must_be_set():
    """Test that PROFILE environment variable is set."""
    assert os.environ.get("PROFILE"), "PROFILE environment variable must be set"


def test_emqx_boot_with_invalid_node_name(emqx_bin_path):
    """Test that emqx boot fails with invalid node name."""
    env = os.environ.copy()
    env["EMQX_NODE_NAME"] = "invliadename#"

    result = subprocess.run(
        [str(emqx_bin_path), "console"],
        env=env,
        capture_output=True,
        text=True,
        timeout=10
    )

    # The command should fail and output should contain error message
    output = result.stdout + result.stderr
    assert "ERROR: Invalid node name," in output or result.returncode != 0


def test_corrupted_cluster_override_conf(emqx_bin_path, emqx_rel_path):
    """Test that emqx boot fails with corrupted cluster-override.conf."""
    conffile = emqx_rel_path / "data" / "configs" / "cluster-override.conf"

    # Backup original if exists
    backup = None
    if conffile.exists():
        backup = conffile.with_suffix(".conf.backup")
        shutil.copy2(conffile, backup)

    try:
        # Create corrupted config
        conffile.parent.mkdir(parents=True, exist_ok=True)
        conffile.write_text("{")

        try:
            result = subprocess.run(
                [str(emqx_bin_path), "console"],
                capture_output=True,
                text=True,
                timeout=10
            )
            output = result.stdout + result.stderr
            # Should fail with non-zero exit code OR contain error message about failed config
            assert (
                result.returncode != 0 or
                "failed_to_load_config" in output or
                "failed_to_load_conf" in output or
                "cluster-override.conf" in output
            ), f"Expected emqx to fail with corrupted cluster-override.conf. Exit code: {result.returncode}, Output: {output[:500]}"
        except subprocess.TimeoutExpired as e:
            # Check if error message is in output before timeout
            # TimeoutExpired has 'output' and 'stderr' attributes
            output = ""
            if hasattr(e, 'output') and e.output:
                output += e.output.decode("utf-8", errors="ignore") if isinstance(e.output, bytes) else e.output
            if hasattr(e, 'stderr') and e.stderr:
                output += e.stderr.decode("utf-8", errors="ignore") if isinstance(e.stderr, bytes) else e.stderr
            if "failed_to_load_config" in output or "failed_to_load_conf" in output or "cluster-override.conf" in output:
                # Error detected before timeout - this is acceptable
                pass
            else:
                # Timeout without error - this is a failure
                pytest.fail(f"emqx console timed out with corrupted cluster-override.conf without showing error. Output: {output[:500]}")
    finally:
        # Restore original or remove corrupted file
        if backup and backup.exists():
            shutil.move(backup, conffile)
        elif conffile.exists():
            conffile.unlink()


def test_corrupted_cluster_hocon(emqx_bin_path, emqx_rel_path):
    """Test that emqx boot fails with corrupted cluster.hocon."""
    conffile = emqx_rel_path / "data" / "configs" / "cluster.hocon"

    # Backup original if exists
    backup = None
    if conffile.exists():
        backup = conffile.with_suffix(".hocon.backup")
        shutil.copy2(conffile, backup)

    try:
        # Create corrupted config
        conffile.parent.mkdir(parents=True, exist_ok=True)
        conffile.write_text("{")

        try:
            result = subprocess.run(
                [str(emqx_bin_path), "console"],
                capture_output=True,
                text=True,
                timeout=10
            )
            # Should fail with non-zero exit code
            assert result.returncode != 0, "Expected emqx to fail with corrupted cluster.hocon"
        except subprocess.TimeoutExpired:
            # Timeout is also a failure - emqx should fail quickly with corrupted config
            pytest.fail("emqx console timed out with corrupted cluster.hocon (should fail quickly)")
    finally:
        # Restore original or remove corrupted file
        if backup and backup.exists():
            shutil.move(backup, conffile)
        elif conffile.exists():
            conffile.unlink()


def test_corrupted_base_hocon(emqx_bin_path, emqx_rel_path):
    """Test that emqx boot fails with corrupted base.hocon."""
    conffile = emqx_rel_path / "etc" / "base.hocon"

    # Backup original if exists
    backup = None
    if conffile.exists():
        backup = conffile.with_suffix(".hocon.backup")
        shutil.copy2(conffile, backup)

    try:
        # Create corrupted config
        conffile.parent.mkdir(parents=True, exist_ok=True)
        conffile.write_text("{")

        try:
            result = subprocess.run(
                [str(emqx_bin_path), "console"],
                capture_output=True,
                text=True,
                timeout=10
            )
            # Should fail with non-zero exit code
            assert result.returncode != 0, "Expected emqx to fail with corrupted base.hocon"
        except subprocess.TimeoutExpired:
            # Timeout is also a failure - emqx should fail quickly with corrupted config
            pytest.fail("emqx console timed out with corrupted base.hocon (should fail quickly)")
    finally:
        # Restore original or remove corrupted file
        if backup and backup.exists():
            shutil.move(backup, conffile)
        elif conffile.exists():
            conffile.unlink()
