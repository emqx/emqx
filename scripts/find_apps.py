#!/usr/bin/env python3
"""
Find apps that need to be tested based on git changes and dependencies.

Usage:
    python scripts/find_apps.py --list
    python scripts/find_apps.py --ci --base-ref <ref>
"""

import os
import sys
import subprocess
import json
import re
from pathlib import Path
from typing import List, Set, Optional


def get_distro() -> str:
    """Get the distribution name."""
    result = subprocess.run(
        ["./scripts/get-distro.sh"],
        capture_output=True,
        text=True,
        check=True
    )
    return result.stdout.strip()


def find_apps(app_dir: Path) -> List[str]:
    """Find all app directories. Returns relative paths like 'apps/emqx'."""
    if not app_dir.exists():
        return []
    # Convert absolute paths to relative paths like 'apps/emqx'
    # app_dir is always project_root / "apps", so we construct relative paths
    apps = []
    for p in app_dir.iterdir():
        if p.is_dir():
            # Get relative path from app_dir's parent (project_root)
            # This gives us paths like 'apps/emqx'
            rel_path = p.relative_to(app_dir.parent)
            apps.append(str(rel_path))
    return sorted(apps)


def get_changed_apps(base_ref: str, project_root: Path) -> List[str]:
    """Get changed apps based on git diff. Returns list of app paths (e.g., 'apps/emqx')."""
    os.chdir(project_root)

    # Get changed files
    result = subprocess.run(
        ["git", "diff", "--name-only", base_ref, "HEAD"],
        capture_output=True,
        text=True,
        check=False
    )
    changed_files = result.stdout.strip().split('\n') if result.stdout.strip() else []

    # Check if mix.exs at root is changed
    if "mix.exs" in changed_files:
        return find_apps(project_root / "apps")

    # Check if .github or .ci directories are changed
    if any(f.startswith(".github/") or f.startswith(".ci/") for f in changed_files):
        return find_apps(project_root / "apps")

    # Get changed files in apps/ directory
    changed_app_files = [f for f in changed_files if f.startswith("apps/")]

    if not changed_app_files:
        return []

    # Extract unique app names from changed paths and return as app paths
    app_names = set()
    for file_path in changed_app_files:
        parts = file_path.split("/")
        if len(parts) > 1:
            app_names.add(parts[1])

    return sorted([f"apps/{app}" for app in app_names])


def get_apps_to_test(changed_apps: List[str], project_root: Path) -> List[str]:
    """Get apps that use the changed apps (transitive closure)."""
    deps_file = project_root / "deps.txt"

    if not deps_file.exists():
        print(f"Warning: {deps_file} not found, falling back to all apps", file=sys.stderr)
        return find_apps(project_root / "apps")

    if not changed_apps:
        return []

    # Read deps.txt
    deps_map = {}
    with open(deps_file, "r") as f:
        for line in f:
            line = line.strip()
            if not line or line.startswith("#"):
                continue
            if ":" not in line:
                continue
            app_name, users = line.split(":", 1)
            app_name = app_name.strip()
            users = users.strip()
            deps_map[app_name] = users

    apps_set: Set[str] = set()
    found_all = False

    for app_path in changed_apps:
        # Normalize: strip "apps/" prefix if present to get just the app name
        app = app_path.replace("apps/", "")

        if app not in deps_map:
            # App not found in deps.txt, just add the changed app itself
            apps_set.add(app)
            continue

        users = deps_map[app].strip()

        if users == "all":
            # If app is used by all, we need to test all apps
            found_all = True
            break
        elif users != "none" and users:
            # Add users to the set
            for user in users.split():
                apps_set.add(user.strip())

        # Also add the changed app itself
        apps_set.add(app)

    if found_all:
        return find_apps(project_root / "apps")
    else:
        # Convert set to sorted list and add "apps/" prefix
        return sorted([f"apps/{app}" for app in apps_set])


def format_app_entry(app: str, groups: int, runner: str) -> List[dict]:
    """Format app entry for GitHub Actions matrix."""
    entries = []
    prefix = app.replace("/", "_")
    for group in range(1, groups + 1):
        entries.append({
            "app": app,
            "suitegroup": f"{group}_{groups}",
            "runner": runner,
            # TODO: drop profile
            "profile": "emqx-enterprise",
            "prefix": prefix
        })
    return entries


def get_runner(app_path: Path) -> str:
    """Determine runner type based on docker-ct file."""
    docker_ct = app_path / "docker-ct"
    return "docker" if docker_ct.exists() else "host"


def generate_matrix(apps: List[str], project_root: Path) -> List[dict]:
    """Generate GitHub Actions matrix JSON."""
    if not apps:
        return []

    entries = []
    for app in apps:
        app_path = project_root / app
        runner = get_runner(app_path)

        if app == "apps/emqx":
            entries.extend(format_app_entry(app, 10, runner))
        elif app == "apps/emqx_management":
            entries.extend(format_app_entry(app, 2, runner))
        elif app.startswith("apps/"):
            entries.extend(format_app_entry(app, 1, runner))
        else:
            print(f"unknown app: {app}", file=sys.stderr)
            sys.exit(1)

    return entries


def main():
    """Main entry point."""
    script_dir = Path(__file__).parent
    project_root = script_dir.parent
    os.chdir(project_root)

    mode = "list"  # Default mode is list
    base_ref = None

    # Parse arguments
    i = 1
    while i < len(sys.argv):
        arg = sys.argv[i]
        if arg in ("-h", "--help"):
            print("\n-h|--help:        To display this usage info")
            print("--ci:             Print apps in json format for github ci matrix")
            print("--base-ref:       Base ref for git diff")
            sys.exit(0)
        elif arg == "--ci":
            mode = "ci"
            i += 1
        elif arg == "--base-ref":
            if i + 1 >= len(sys.argv):
                print("Error: --base-ref requires a value", file=sys.stderr)
                sys.exit(1)
            base_ref = sys.argv[i + 1]
            i += 2
        else:
            print(f"unknown option {arg}", file=sys.stderr)
            sys.exit(1)

    if mode == "list":
        apps = find_apps(project_root / "apps")
        for app in apps:
            print(app)
        sys.exit(0)

    if mode == "ci":
        if base_ref:
            # Verify base ref exists
            result = subprocess.run(
                ["git", "rev-parse", "--verify", base_ref],
                capture_output=True,
                text=True
            )
            if result.returncode != 0:
                # If BASE_REF is not available or invalid, fall back to all apps
                apps = find_apps(project_root / "apps")
                matrix = generate_matrix(apps, project_root)
                print(json.dumps(matrix))
                sys.exit(0)

            changed_apps = get_changed_apps(base_ref, project_root)

            if changed_apps:
                apps_to_test = get_apps_to_test(changed_apps, project_root)
                if apps_to_test:
                    matrix = generate_matrix(apps_to_test, project_root)
                    print(json.dumps(matrix))
                else:
                    print("[]")
            else:
                # No changes in apps/ and mix.exs not changed, test nothing
                print("[]")
        else:
            # No base ref provided, return all apps
            apps = find_apps(project_root / "apps")
            matrix = generate_matrix(apps, project_root)
            print(json.dumps(matrix))
    else:
        print(f"Unknown mode: {mode}", file=sys.stderr)
        sys.exit(1)


if __name__ == "__main__":
    main()
