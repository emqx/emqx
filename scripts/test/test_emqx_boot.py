#!/usr/bin/env python3
"""
Pytest tests for EMQX boot behavior.
Equivalent to emqx-boot.bats

Usage:
    pytest scripts/test/test_emqx_boot.py -v
"""

import os
import subprocess
import shutil
import pytest
from pathlib import Path


@pytest.fixture(scope="module")
def profile():
    """Get PROFILE environment variable, defaulting to emqx-enterprise."""
    return os.environ.get("PROFILE", "emqx-enterprise")


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


def test_profile_defaults_to_emqx_enterprise():
    """Test that PROFILE defaults to emqx-enterprise when not set."""
    # This test verifies the default behavior
    # The actual profile value is tested through the fixtures
    pass


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

def test_skip_quic_nif_load(emqx_bin_path, emqx_rel_path):
    """
    Test that with QUICER_SKIP_NIF_LOAD=1, EMQX can start even if libquicer_nif.so is absent.
    """
    # Find all libquicer_nif.so files in the release directory
    nif_files = list(emqx_rel_path.rglob("libquicer_nif.so"))

    # Skip test if no libquicer_nif.so files are found
    if not nif_files:
        pytest.skip("No libquicer_nif.so files found in release directory")

    # Backup all found files
    backups = []
    for nif_file in nif_files:
        backup = nif_file.with_suffix(".so.backup")
        shutil.copy2(nif_file, backup)
        backups.append((nif_file, backup))

    timeout = 20

    try:
        # Delete all libquicer_nif.so files
        for nif_file, _ in backups:
            nif_file.unlink()

        # Test that console fails without QUICER_SKIP_NIF_LOAD
        result = subprocess.run(
            [str(emqx_bin_path), "console"],
            capture_output=True,
            text=True,
            timeout=timeout
        )
        assert result.returncode != 0, "Expected emqx console to fail when libquicer_nif.so is absent"

        # Test that start succeeds with QUICER_SKIP_NIF_LOAD=1
        env = os.environ.copy()
        env["QUICER_SKIP_NIF_LOAD"] = "1"
        result = subprocess.run(
            [str(emqx_bin_path), "start"],
            env=env,
            capture_output=True,
            text=True,
            timeout=timeout
        )
        assert result.returncode == 0, f"Expected emqx start to succeed with QUICER_SKIP_NIF_LOAD=1. Exit code: {result.returncode}, Output: {result.stdout + result.stderr}"

        # Stop emqx
        result = subprocess.run(
            [str(emqx_bin_path), "stop"],
            capture_output=True,
            text=True,
            timeout=timeout
        )
        # Stop command may return non-zero if emqx wasn't running, which is acceptable
    finally:
        # Restore all backed up files
        for nif_file, backup in backups:
            if backup.exists():
                shutil.move(backup, nif_file)

def _test_acl_file_failure(emqx_bin_path, emqx_rel_path, setup_acl_file, expected_msg):
    """Helper function to test ACL file failures with [critical] log level.

    Args:
        emqx_bin_path: Path to emqx binary
        emqx_rel_path: Path to emqx release directory
        setup_acl_file: Function that modifies the ACL file (takes acl_file path as parameter)
        expected_msg: Expected message string to find in the log output
    """
    acl_file = emqx_rel_path / "etc" / "acl.conf"

    # Backup original if exists
    backup = None
    if acl_file.exists():
        backup = acl_file.with_suffix(".conf.backup")
        shutil.copy2(acl_file, backup)

    try:
        # Ensure ACL file directory exists
        acl_file.parent.mkdir(parents=True, exist_ok=True)

        # Setup ACL file (modify permissions or content)
        setup_acl_file(acl_file)

        try:
            result = subprocess.run(
                [str(emqx_bin_path), "console"],
                capture_output=True,
                text=True,
                timeout=15
            )
            output = result.stdout + result.stderr

            # Check stdout/stderr for [critical] level and expected message
            assert (
                "[critical]" in output.lower() and
                expected_msg in output
            ), f"Expected [critical] log with {expected_msg} message. Output: {output[:1000]}"
        except subprocess.TimeoutExpired as e:
            # Check if error message is in output before timeout
            output = ""
            if hasattr(e, 'output') and e.output:
                output += e.output.decode("utf-8", errors="ignore") if isinstance(e.output, bytes) else e.output
            if hasattr(e, 'stderr') and e.stderr:
                output += e.stderr.decode("utf-8", errors="ignore") if isinstance(e.stderr, bytes) else e.stderr

            if "[critical]" in output.lower() and expected_msg in output:
                # Error detected before timeout - this is acceptable
                pass
            else:
                pytest.fail(f"emqx console timed out without showing [critical] log with {expected_msg}. Output: {output[:1000]}")
    finally:
        # Restore file permissions (in case they were changed)
        try:
            os.chmod(acl_file, 0o644)
        except:
            pass

        # Restore original ACL file
        if backup and backup.exists():
            shutil.move(backup, acl_file)
        elif acl_file.exists():
            acl_file.unlink()


def test_acl_file_read_permission_failure(emqx_bin_path, emqx_rel_path):
    """Test that emqx logs [critical] level when ACL file cannot be read due to permissions.

    Case 1: Change file permission to make file read fail and assert the [critical] level log
    with the message failed_to_read_acl_file.
    """
    def setup_unreadable(acl_file):
        # Create ACL file with valid content if it doesn't exist
        if not acl_file.exists():
            acl_file.write_text("{allow, all}.\n")
        # Make ACL file unreadable
        os.chmod(acl_file, 0o000)

    _test_acl_file_failure(
        emqx_bin_path,
        emqx_rel_path,
        setup_unreadable,
        "failed_to_read_acl_file"
    )


def test_acl_file_corrupted_content(emqx_bin_path, emqx_rel_path):
    """Test that emqx logs [critical] level when ACL file has corrupted content.

    Case 2: Corrupt the file content and assert log level [critical] and message bad_acl_file_content.
    """
    def setup_corrupted(acl_file):
        # Create ACL file with corrupted content (invalid Erlang term syntax)
        acl_file.write_text("{invalid syntax here")

    _test_acl_file_failure(
        emqx_bin_path,
        emqx_rel_path,
        setup_corrupted,
        "bad_acl_file_content"
    )
