#!/usr/bin/env python3
"""
Pytest tests for find-apps.sh --ci --base-ref
This script:
1. Remembers the current HEAD commit
2. Makes changes in a few apps/appname directories
3. Asserts find-apps.sh finds the changed apps and their user apps
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


@pytest.fixture(scope="module")
def project_root():
    """Get the project root directory."""
    return Path(__file__).parent.parent.parent


@pytest.fixture(scope="module")
def original_head(project_root):
    """Save and return the original HEAD commit."""
    os.chdir(project_root)

    # Ensure git is in a proper state for making commits
    ensure_git_user_config(project_root)

    result = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True,
        check=True
    )
    original_head = result.stdout.strip()
    print(f"\nOriginal HEAD: {original_head}")

    # Verify we can make commits
    git_status = subprocess.run(
        ["git", "status"],
        capture_output=True,
        text=True
    )
    print(f"Initial git status: {git_status.stdout[:200]}")

    # Final cleanup at end of test session
    yield original_head

    # Ensure git is reset at the very end
    os.chdir(project_root)
    try:
        # Remove test files
        subprocess.run(
            ["find", ".", "-type", "f", "-name", "test-change.*.tmp", "-delete"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        subprocess.run(
            ["git", "reset", "--hard", original_head],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # Clean .github test files manually
        subprocess.run(
            ["find", ".github", "-type", "f", "-name", "test-change.*.tmp", "-delete"],
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
        # Remove temporary test files created during tests
        subprocess.run(
            ["find", ".", "-type", "f", "-name", "test-change.*.tmp", "-delete"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
        # Reset git to original HEAD (this will remove committed test changes)
        result = subprocess.run(
            ["git", "reset", "--hard", original_head],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        if result.returncode != 0:
            print(f"Warning: git reset failed: {result.stderr}")
        # Clean untracked files in .github directory (test files created there)
        subprocess.run(
            ["find", ".github", "-type", "f", "-name", "test-change.*.tmp", "-delete"],
            stdout=subprocess.DEVNULL,
            stderr=subprocess.DEVNULL
        )
    except Exception as e:
        print(f"Warning: Cleanup error: {e}")


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


def run_find_apps(project_root: Path, base_ref: str) -> Tuple[str, int]:
    """Run find-apps.sh and return output and exit code."""
    os.chdir(project_root)
    script_path = project_root / "scripts" / "find-apps.sh"

    result = subprocess.run(
        [str(script_path), "--ci", "--base-ref", base_ref],
        capture_output=True,
        text=True
    )

    return result.stdout + result.stderr, result.returncode


def parse_found_apps(output: str) -> Set[str]:
    """Parse JSON output from find-apps.sh and extract app paths."""
    # Try to extract JSON from output (might have stderr mixed in)
    output_lines = output.strip().split('\n')
    json_lines = []
    in_json = False
    for line in output_lines:
        line = line.strip()
        if line.startswith('[') or line.startswith('{'):
            in_json = True
        if in_json:
            json_lines.append(line)
        if line.endswith(']') or line.endswith('}'):
            break

    json_str = '\n'.join(json_lines) if json_lines else output.strip()

    try:
        data = json.loads(json_str)
        if isinstance(data, list):
            apps = {entry["app"] for entry in data if isinstance(entry, dict) and "app" in entry}
        else:
            apps = set()
        return apps
    except (json.JSONDecodeError, KeyError, TypeError) as e:
        print(f"JSON parsing error: {e}")
        print(f"Attempted to parse: {json_str[:200]}")
        return set()


def assert_find_apps(
    project_root: Path,
    test_name: str,
    expected_apps: List[str],
    base_ref: str
) -> None:
    """Test helper function to check find-apps.sh output."""
    print(f"\nTest: {test_name}")
    print(f"Expected apps: {expected_apps}")

    # Debug: Check git state
    os.chdir(project_root)
    git_head = subprocess.run(
        ["git", "rev-parse", "HEAD"],
        capture_output=True,
        text=True
    ).stdout.strip()
    print(f"Current HEAD: {git_head}")
    print(f"Base ref: {base_ref}")

    # Check if base_ref exists
    base_ref_check = subprocess.run(
        ["git", "rev-parse", "--verify", base_ref],
        capture_output=True,
        text=True
    )
    if base_ref_check.returncode != 0:
        print(f"Warning: Base ref {base_ref} verification failed: {base_ref_check.stderr}")

    # Check git diff
    git_diff = subprocess.run(
        ["git", "diff", "--name-only", base_ref, "HEAD"],
        capture_output=True,
        text=True
    )
    print(f"Git diff output: {git_diff.stdout}")

    output, exit_code = run_find_apps(project_root, base_ref)

    print(f"find-apps.sh exit code: {exit_code}")
    print(f"find-apps.sh raw output: {output[:500]}")  # First 500 chars

    if exit_code != 0:
        pytest.fail(f"find-apps.sh exited with code {exit_code}\nOutput: {output}")

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

        # Include debug info in error message
        error_msg.append(f"\nDebug info:")
        error_msg.append(f"  Base ref: {base_ref}")
        error_msg.append(f"  Current HEAD: {git_head}")
        error_msg.append(f"  Git diff: {git_diff.stdout.strip()}")
        error_msg.append(f"  find-apps.sh output: {output[:500]}")

        pytest.fail("\n".join(error_msg))

    print("PASS")


def make_change(project_root: Path, app_path: Path) -> None:
    """Make a dummy change in an app directory and commit it."""
    os.chdir(project_root)
    ensure_git_user_config(project_root)

    test_file = app_path / f"test-change.{int(time.time())}.tmp"
    test_file.parent.mkdir(parents=True, exist_ok=True)
    test_file.write_text("# Test change for dependency testing\n")

    add_result = subprocess.run(
        ["git", "add", str(test_file)],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if add_result.returncode != 0:
        print(f"Warning: git add failed: {add_result.stderr}")

    commit_result = subprocess.run(
        ["git", "commit", "-m", f"test: change {app_path} for dependency testing"],
        stdout=subprocess.PIPE,
        stderr=subprocess.PIPE,
        text=True
    )
    if commit_result.returncode != 0:
        print(f"Warning: git commit failed: {commit_result.stderr}")
        print(f"Commit stdout: {commit_result.stdout}")
        # Try to see what's wrong
        status_result = subprocess.run(
            ["git", "status"],
            stdout=subprocess.PIPE,
            stderr=subprocess.PIPE,
            text=True
        )
        print(f"Git status: {status_result.stdout}")
    else:
        # Verify commit was created
        new_head = subprocess.run(
            ["git", "rev-parse", "HEAD"],
            capture_output=True,
            text=True
        ).stdout.strip()
        print(f"Commit created successfully, new HEAD: {new_head}")


def test_change_app_used_by_others(project_root: Path, original_head: str):
    """Test 1: Change an app that is used by other apps."""
    test_app = "emqx_auto_subscribe"
    test_app_path = project_root / "apps" / test_app

    if not test_app_path.exists():
        pytest.skip(f"{test_app_path} does not exist")

    make_change(project_root, test_app_path)

    # According to deps.txt, emqx_auto_subscribe is used by emqx_telemetry
    # So we expect both apps to be in the output
    assert_find_apps(
        project_root,
        f"Change {test_app}",
        [f"apps/{test_app}", "apps/emqx_telemetry"],
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
        pytest.fail(f"find-apps.sh exited with code {exit_code}\nOutput: {output}")

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
    test_app2 = "emqx_auth_ext"
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


def test_change_rebar_config(project_root: Path, original_head: str):
    """Test 5: Change rebar.config (should trigger all apps)."""
    rebar_config = project_root / "rebar.config"

    if not rebar_config.exists():
        pytest.skip("rebar.config does not exist")

    os.chdir(project_root)
    with open(rebar_config, "a") as f:
        f.write("# Test change\n")

    subprocess.run(
        ["git", "add", "rebar.config"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )
    subprocess.run(
        ["git", "commit", "-m", "test: change rebar.config"],
        stdout=subprocess.DEVNULL,
        stderr=subprocess.DEVNULL
    )

    output, exit_code = run_find_apps(project_root, original_head)

    if exit_code != 0:
        pytest.fail(f"find-apps.sh exited with code {exit_code}\nOutput: {output}")

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
        pytest.fail(f"find-apps.sh exited with code {exit_code}\nOutput: {output}")

    found_apps = parse_found_apps(output)
    app_count = len(found_apps)

    if app_count <= 10:
        pytest.fail(f"Only found {app_count} apps, expected all")

    print(f"PASS: Found {app_count} apps (expected all)")
