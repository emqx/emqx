#!/usr/bin/env python3
"""
Filter SPDX SBOM to include only released external production dependencies.

Released external deps = intersection of:
  - Apps in _build/emqx-enterprise/rel/emqx/lib/ (released apps)
  - Dirs in deps/ (external dependencies)
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Set


def get_released_apps(lib_dir: Path) -> Set[str]:
    """
    Get app names from _build/emqx-enterprise/rel/emqx/lib/.
    Directory names are like 'appname-version/', we extract just the app name.
    """
    released = set()
    if not lib_dir.exists():
        print(f"Warning: {lib_dir} does not exist", file=sys.stderr)
        return released

    for entry in lib_dir.iterdir():
        if entry.is_dir():
            # Pattern: appname-version (e.g., cowboy-2.9.0, emqx-6.1.0)
            # Handle app names that may contain hyphens by matching version at end
            name = entry.name
            # Match version pattern at the end: -X.Y.Z or -X.Y.Z.W or -X.Y.Z-suffix
            match = re.match(r'^(.+)-(\d+\.\d+.*?)$', name)
            if match:
                app_name = match.group(1)
                released.add(app_name)
            else:
                # No version pattern, use the whole name
                released.add(name)
    return released


def get_external_deps(deps_dir: Path) -> Set[str]:
    """
    Get dependency names from deps/ directory.
    """
    deps = set()
    if not deps_dir.exists():
        print(f"Warning: {deps_dir} does not exist", file=sys.stderr)
        return deps

    for entry in deps_dir.iterdir():
        if entry.is_dir():
            deps.add(entry.name)
    return deps


def filter_spdx_sbom(sbom_path: Path, allowed_packages: Set[str], excluded_packages: Set[str] = None) -> dict:
    """
    Filter SPDX SBOM to include only packages in the allowed set.

    Args:
        sbom_path: Path to the input SPDX SBOM JSON file
        allowed_packages: Set of package names that should be included
        excluded_packages: Set of package names that should be explicitly excluded (e.g., test dependencies)
    """
    if excluded_packages is None:
        excluded_packages = set()

    with open(sbom_path, 'r') as f:
        sbom = json.load(f)

    original_count = len(sbom.get('packages', []))

    # Filter packages - match by name field
    filtered_packages = []
    excluded_found = []
    for pkg in sbom.get('packages', []):
        pkg_name = pkg.get('name', '')

        # Explicitly exclude test/dev dependencies
        if pkg_name in excluded_packages:
            excluded_found.append(pkg_name)
            continue

        # Only include if in allowed set
        if pkg_name in allowed_packages:
            filtered_packages.append(pkg)

    # Report if any excluded packages were found
    if excluded_found:
        print(f"Warning: Found {len(excluded_found)} excluded packages in SBOM (these were filtered out):", file=sys.stderr)
        for pkg_name in sorted(excluded_found):
            print(f"  - {pkg_name}", file=sys.stderr)

    sbom['packages'] = filtered_packages

    # Update relationships to only include those referencing kept packages
    kept_spdx_ids = {pkg['SPDXID'] for pkg in filtered_packages}
    kept_spdx_ids.add('SPDXRef-DOCUMENT')  # Always keep document reference

    if 'relationships' in sbom:
        filtered_relationships = []
        for rel in sbom['relationships']:
            src = rel.get('spdxElementId', '')
            tgt = rel.get('relatedSpdxElement', '')
            if src in kept_spdx_ids and tgt in kept_spdx_ids:
                filtered_relationships.append(rel)
        sbom['relationships'] = filtered_relationships

    filtered_count = len(sbom['packages'])
    print(f"Filtered packages: {original_count} -> {filtered_count}", file=sys.stderr)

    # Final validation: ensure no excluded packages made it through
    final_package_names = {pkg.get('name', '') for pkg in filtered_packages}
    leaked_excluded = final_package_names & excluded_packages
    if leaked_excluded:
        print(f"ERROR: Found excluded packages in filtered SBOM: {sorted(leaked_excluded)}", file=sys.stderr)
        sys.exit(1)

    return sbom


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Filter SPDX SBOM to include only released external production dependencies")
    parser.add_argument("--sbom", type=Path, help="Input SPDX SBOM file (default: bom.spdx.json in project root)")
    parser.add_argument("--output", type=Path, help="Output filtered SPDX SBOM file (default: bom.spdx.filtered.json in project root)")
    parser.add_argument("--lib-dir", type=Path, help="Library directory with released apps (default: _build/emqx-enterprise/rel/emqx/lib in project root)")
    parser.add_argument("--deps-dir", type=Path, help="Dependencies directory (default: deps in project root)")

    args = parser.parse_args()

    # Paths relative to project root (two levels up from scripts/sbom directory)
    script_dir = Path(__file__).parent.resolve()
    project_root = script_dir.parent.parent.resolve()
    lib_dir = args.lib_dir if args.lib_dir else (project_root / '_build' / 'emqx-enterprise' / 'rel' / 'emqx' / 'lib')
    deps_dir = args.deps_dir if args.deps_dir else (project_root / 'deps')
    sbom_path = args.sbom if args.sbom else (project_root / 'bom.spdx.json')
    output_path = args.output if args.output else (project_root / 'bom.spdx.filtered.json')

    # Get the two sets
    released_apps = get_released_apps(lib_dir)
    external_deps = get_external_deps(deps_dir)

    # Find intersection: released external dependencies
    released_external_deps = released_apps & external_deps

    # Print summary
    print("=" * 60, file=sys.stderr)
    print(f"Released apps (from lib/):     {len(released_apps)}", file=sys.stderr)
    print(f"External deps (from deps/):    {len(external_deps)}", file=sys.stderr)
    print(f"Released external deps:        {len(released_external_deps)}", file=sys.stderr)
    print("=" * 60, file=sys.stderr)

    # Show what's released but NOT external (internal apps)
    internal_apps = released_apps - external_deps
    print(f"\nInternal apps (released but not in deps/): {len(internal_apps)}", file=sys.stderr)
    for app in sorted(internal_apps)[:10]:
        print(f"  - {app}", file=sys.stderr)
    if len(internal_apps) > 10:
        print(f"  ... and {len(internal_apps) - 10} more", file=sys.stderr)

    # Show what's external but NOT released (test/dev dependencies)
    test_deps = external_deps - released_apps
    print(f"\nTest/dev deps (in deps/ but not released): {len(test_deps)}", file=sys.stderr)
    for dep in sorted(test_deps)[:10]:
        print(f"  - {dep}", file=sys.stderr)
    if len(test_deps) > 10:
        print(f"  ... and {len(test_deps) - 10} more", file=sys.stderr)

    # Show released external deps
    print(f"\nReleased external deps:", file=sys.stderr)
    for dep in sorted(released_external_deps):
        print(f"  - {dep}", file=sys.stderr)

    # Filter and write SBOM
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("Filtering SPDX SBOM...", file=sys.stderr)
    # Explicitly exclude test/dev dependencies to ensure they never make it into the filtered SBOM
    filtered_sbom = filter_spdx_sbom(sbom_path, released_external_deps, excluded_packages=test_deps)

    with open(output_path, 'w') as f:
        json.dump(filtered_sbom, f, indent=2)

    print(f"Filtered SBOM written to: {output_path}", file=sys.stderr)

    # Also print the list of released external deps to stdout for easy use
    print("\n# Released external dependencies (one per line):")
    for dep in sorted(released_external_deps):
        print(dep)


if __name__ == '__main__':
    main()
