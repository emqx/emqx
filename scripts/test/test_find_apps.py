#!/usr/bin/env python3
"""
Pytest tests for find_apps.py --ci --base-ref
This script:
1. Remembers the current HEAD commit
2. Makes changes in a few apps/appname directories
3. Asserts find_apps.py finds the changed apps and their user apps
4. Resets git to the original commit

Usage:
    pytest scripts/test/test_find_apps.py -v
"""

import os
import subprocess
import json
import time
import pytest
from pathlib import Path
from typing import List, Set, Tuple


def cleanup_test_files(project_root: Path, pattern: str = "test-change*.tmp") -> None:
    """Remove test files matching the pattern."""
    try:
        # Use rglob with glob pattern (more efficient than iterating all files)
        # Pattern "test-change*.tmp" matches files like "test-change.1234567890.tmp"
        for file_path in project_root.rglob(pattern):
            if file_path.is_file():
                try:
                    file_path.unlink()
                except OSError:
                    pass  # File might already be deleted or not accessible
    except Exception:
        pass  # Fail silently during cleanup


def ensure_git_user_config(project_root: Path) -> None:
    """Ensure git user.name and user.email are configured (needed for CI)."""
    os.chdir(project_root)

    user_name = subprocess.run(
        ["git", "config", "user.name"],
        capture_output=True,
        text=True
    ).stdout.strip()
    if not user_name:
        subprocess.run(
            ["git", "config", "user.name", "Test User"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )

    user_email = subprocess.run(
        ["git", "config", "user.email"],
        capture_output=True,
        text=True
    ).stdout.strip()
    if not user_email:
        subprocess.run(
            ["git", "config", "user.email", "test@example.com"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )


@pytest.fixture(scope="module")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="module", autouse=True)
def check_git_status(project_root):
    """Fail immediately if git status is dirty (ignores untracked files)."""
    os.chdir(project_root)
    # Check for modified/staged files only (ignore untracked files)
    # Use git diff to check for tracked file changes
    result = subprocess.run(
        ["git", "diff", "--name-only"],
        capture_output=True,
        text=True,
        check=True
    )
    staged_result = subprocess.run(
        ["git", "diff", "--cached", "--name-only"],
        capture_output=True,
        text=True,
        check=True
    )

    dirty_files = []
    if result.stdout.strip():
        dirty_files.extend(result.stdout.strip().split('\n'))
    if staged_result.stdout.strip():
        dirty_files.extend(staged_result.stdout.strip().split('\n'))

    if dirty_files:
        pytest.fail(
            f"Git working directory has uncommitted changes. Please commit or stash changes before running tests.\n"
            f"Dirty files:\n" + "\n".join(dirty_files)
        )


@pytest.fixture(scope="module", autouse=True)
def setup_git_config(project_root):
    """Ensure git user config is set up (needed for CI)."""
    ensure_git_user_config(project_root)


@pytest.fixture(scope="module")
def original_head(project_root):
    """Save and return the original HEAD commit."""
    os.chdir(project_root)
    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True
    )
    original_head = result.stdout.strip()
    print(f"\nOriginal HEAD: {original_head}")

    # Final cleanup at end of test session
    yield original_head

    # Ensure git is reset at the very end
    os.chdir(project_root)
    try:
        # Remove test files using Python-native approach
        cleanup_test_files(project_root)
        subprocess.run(
            ["git", "reset", "--hard", original_head],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception:
        pass


@pytest.fixture(autouse=True)
def cleanup_after_test(project_root, original_head):
    """Cleanup function that runs after each test."""
    yield
    # Cleanup: remove test files and reset git
    os.chdir(project_root)
    try:
        # Remove temporary test files created during tests using Python-native approach
        cleanup_test_files(project_root)
        # Reset git to original HEAD (this will remove committed test changes)
        result = subprocess.run(
            ["git", "reset", "--hard", original_head],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode != 0:
            print(f"Warning: git reset failed: {result.stderr}")
    except Exception as e:
        print(f"Warning: Cleanup error: {e}")


def run_find_apps(project_root: Path, base_ref: str) -> Tuple[str, int]:
    """Run find_apps.py and return output and exit code."""
    os.chdir(project_root)
    script_path = project_root / "scripts" / "find_apps.py"

    result = subprocess.run(
        ["python3", str(script_path), "--ci", "--base-ref", base_ref],
        capture_output=True,
        text=True,
        cwd=str(project_root)
    )

    # The script outputs JSON to stdout, warnings/errors go to stderr
    # For JSON parsing, we only use stdout. If there's an error, we'll include stderr in the error message
    output = result.stdout.strip()

    # If there's stderr and the command failed, include it in the output for error reporting
    if result.returncode != 0 and result.stderr.strip():
        output = output + "\n" + result.stderr.strip()

    return output, result.returncode


def parse_found_apps(output: str) -> Set[str]:
    """Parse JSON output from find_apps.py and extract app paths."""
    try:
        data = json.loads(output)
        apps = {entry["app"] for entry in data}
        return apps
    except (json.JSONDecodeError, KeyError, TypeError):
        return set()


def assert_find_apps(
    project_root: Path,
    test_name: str,
    expected_apps: List[str],
    base_ref: str
) -> None:
    """Test helper function to check find_apps.py output."""
    print(f"\nTest: {test_name}")
    print(f"Expected apps: {expected_apps}")

    output, exit_code = run_find_apps(project_root, base_ref)

    if exit_code != 0:
        pytest.fail(f"find_apps.py exited with code {exit_code}\nOutput: {output}")

    found_apps = parse_found_apps(output)
    print(f"Found apps: {sorted(found_apps)}")

    expected_set = set(expected_apps)

    # Check for exact match
    if found_apps != expected_set:
        missing = expected_set - found_apps
        extra = found_apps - expected_set

        error_msg = []
        if missing:
            error_msg.append(f"Missing expected apps: {sorted(missing)}")
        if extra:
            error_msg.append(f"Unexpected extra apps: {sorted(extra)}")

        pytest.fail("\n".join(error_msg))

    print("PASS")


def make_change(project_root: Path, app_path: Path) -> None:
    """Make a dummy change in an app directory and commit it."""
    os.chdir(project_root)

    test_file = app_path / f"test-change.{int(time.time())}.tmp"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("# Test change for dependency testing\n")

    subprocess.run(
        ["git", "add", str(test_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    subprocess.run(
        ["git", "commit", "-m", f"test: change {app_path} for dependency testing"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )


def test_change_app_used_by_others(project_root: Path, original_head: str):
    """Test 1: Change an app that is used by other apps."""
    test_app = "emqx_bridge_mqtt"
    test_app_path = project_root / "apps" / test_app

    if not test_app_path.exists():
        pytest.skip(f"{test_app_path} does not exist")

    make_change(project_root, test_app_path)

    # According to deps.txt, emqx_bridge_mqtt is used by emqx_cluster_link
    # So we expect both apps to be in the output
    assert_find_apps(
        project_root,
        f"Change {test_app}",
        [
            f"apps/{test_app}",
            "apps/emqx_cluster_link",
            "apps/emqx_bridge_azure_event_grid"
        ],
        original_head
    )


def test_change_app_used_by_all(project_root: Path, original_head: str):
    """Test 2: Change an app that is used by 'all'."""
    test_app = "emqx_bridge"
    test_app_path = project_root / "apps" / test_app

    if not test_app_path.exists():
        pytest.skip(f"{test_app_path} does not exist")

    make_change(project_root, test_app_path)

    # According to deps.txt, emqx_bridge is used by "all"
    # So we expect all apps to be in the output
    output, exit_code = run_find_apps(project_root, original_head)

    if exit_code != 0:
        pytest.fail(f"find_apps.py exited with code {exit_code}\nOutput: {output}")

    found_apps = parse_found_apps(output)
    app_count = len(found_apps)

    if app_count <= 10:
        pytest.fail(f"Only found {app_count} apps, expected many/all")

    print(f"PASS: Found {app_count} apps (expected many/all)")


def test_change_app_used_by_none(project_root: Path, original_head: str):
    """Test 3: Change an app with 'none' users."""
    test_app = "emqx_auth_cinfo"
    test_app_path = project_root / "apps" / test_app

    if not test_app_path.exists():
        pytest.skip(f"{test_app_path} does not exist")

    make_change(project_root, test_app_path)

    # According to deps.txt, emqx_auth_cinfo is used by "none"
    # So we expect only the changed app itself
    assert_find_apps(
        project_root,
        f"Change {test_app}",
        [f"apps/{test_app}"],
        original_head
    )


def test_change_multiple_apps(project_root: Path, original_head: str):
    """Test 4: Change multiple apps."""
    test_app1 = "emqx_auth_cinfo"
    test_app2 = "emqx_auth_kerberos"
    test_app_path1 = project_root / "apps" / test_app1
    test_app_path2 = project_root / "apps" / test_app2

    if not test_app_path1.exists() or not test_app_path2.exists():
        pytest.skip("One or both test apps do not exist")

    make_change(project_root, test_app_path1)
    make_change(project_root, test_app_path2)

    # Both apps are used by "none", so we expect both apps
    assert_find_apps(
        project_root,
        f"Change {test_app1} and {test_app2}",
        [f"apps/{test_app1}", f"apps/{test_app2}"],
        original_head
    )


def test_change_mix_exs(project_root: Path, original_head: str):
    """Test 5: Change mix.exs (should trigger all apps)."""
    mix_exs = project_root / "mix.exs"

    if not mix_exs.exists():
        pytest.skip("mix.exs does not exist")

    os.chdir(project_root)

    with open(mix_exs, "a") as f:
        f.write("# Test change\n")

    subprocess.run(
        ["git", "add", "mix.exs"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    subprocess.run(
        ["git", "commit", "-m", "test: change mix.exs"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    output, exit_code = run_find_apps(project_root, original_head)

    if exit_code != 0:
        pytest.fail(f"find_apps.py exited with code {exit_code}\nOutput: {output}")

    found_apps = parse_found_apps(output)
    app_count = len(found_apps)

    if app_count <= 10:
        pytest.fail(f"Only found {app_count} apps, expected all")

    print(f"PASS: Found {app_count} apps (expected all)")


def test_change_github_directory(project_root: Path, original_head: str):
    """Test 6: Change .github directory (should trigger all apps)."""
    github_dir = project_root / ".github"

    if not github_dir.exists():
        pytest.skip(".github directory does not exist")

    os.chdir(project_root)

    github_dir.mkdir(parents=True, exist_ok=True)

    test_file = github_dir / f"test-change.{int(time.time())}.tmp"
    test_file.write_text("# Test change\n")

    subprocess.run(
        ["git", "add", str(test_file)],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    subprocess.run(
        ["git", "commit", "-m", "test: change .github directory"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    output, exit_code = run_find_apps(project_root, original_head)

    if exit_code != 0:
        pytest.fail(f"find_apps.py exited with code {exit_code}\nOutput: {output}")

    found_apps = parse_found_apps(output)
    app_count = len(found_apps)

    if app_count <= 10:
        pytest.fail(f"Only found {app_count} apps, expected all")

    print(f"PASS: Found {app_count} apps (expected all)")
