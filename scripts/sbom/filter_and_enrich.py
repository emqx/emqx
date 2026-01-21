#!/usr/bin/env python3
"""
Filter SPDX SBOM to include only released external production dependencies,
and enrich packages missing license information by reading LICENSE/COPYRIGHT files.

Released external deps = intersection of:
  - Apps in _build/emqx-enterprise/rel/emqx/lib/ (released apps)
  - Dirs in deps/ (external dependencies)

For dependencies missing license information:
  1. Check the deps/ directory for LICENSE/COPYRIGHT files
  2. Check the release lib directory (following ebin symlinks if present)
  3. Parse LICENSE files to guess SPDX license identifiers
"""

import json
import os
import re
import sys
from pathlib import Path
from typing import Set, Optional, Dict, List


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


def normalize_license_to_spdx(license_text: str) -> Optional[str]:
    """
    Try to normalize license text to SPDX license identifier.

    Returns SPDX identifier if recognized, None otherwise.
    """
    license_text = license_text.strip()
    if not license_text:
        return None

    # Common license patterns and their SPDX identifiers
    # Order matters: more specific patterns should come first
    license_patterns = [
        # Apache variants (check first as they're most specific)
        # Handle multi-line: "Apache License\nVersion 2.0" or "Apache License\n==============\nVersion 2.0"
        (r'apache\s+license.*?version\s*2\.0', 'Apache-2.0', re.IGNORECASE | re.DOTALL),
        (r'apache\s+software\s+license.*?version\s*2\.0', 'Apache-2.0', re.IGNORECASE | re.DOTALL),
        (r'apache\s+license\s*[:\-]?\s*version?\s*2\.0', 'Apache-2.0', re.IGNORECASE),
        (r'apache\s+license\s*2\.0', 'Apache-2.0', re.IGNORECASE),
        (r'apache\s+2\.0', 'Apache-2.0', re.IGNORECASE),
        (r'asl\s*2\.0', 'Apache-2.0', re.IGNORECASE),
        (r'apache-2\.0', 'Apache-2.0', re.IGNORECASE),
        (r'apache\s+2', 'Apache-2.0', re.IGNORECASE),

        # MPL variants
        (r'mozilla\s+public\s+license\s*[:\-]?\s*version?\s*2\.0', 'MPL-2.0', re.IGNORECASE),
        (r'mpl\s*[:\-]?\s*version?\s*2\.0', 'MPL-2.0', re.IGNORECASE),
        (r'mpl\s*2\.0', 'MPL-2.0', re.IGNORECASE),
        (r'mpl-2\.0', 'MPL-2.0', re.IGNORECASE),

        # MIT variants (check for "MIT License" specifically to avoid false matches)
        (r'^the\s+mit\s+license', 'MIT', re.IGNORECASE | re.MULTILINE),
        (r'mit\s+license', 'MIT', re.IGNORECASE),
        (r'mit\s+\(massachusetts\s+institute\s+of\s+technology\)', 'MIT', re.IGNORECASE),
        (r'permission\s+is\s+hereby\s+granted.*mit', 'MIT', re.IGNORECASE | re.DOTALL),

        # BSD variants (check for 3-clause first as it's more restrictive)
        # BSD 3-clause has "neither the name" or "names of its contributors" clause
        (r'redistribution.*neither\s+the\s+name.*nor.*names.*contributors.*endorse', 'BSD-3-Clause', re.IGNORECASE | re.DOTALL),
        (r'redistribution.*neither\s+the\s+name.*nor.*names.*promote', 'BSD-3-Clause', re.IGNORECASE | re.DOTALL),
        (r'bsd\s+3[-\s]clause', 'BSD-3-Clause', re.IGNORECASE),
        (r'bsd-3-clause', 'BSD-3-Clause', re.IGNORECASE),
        (r'bsd\s+3', 'BSD-3-Clause', re.IGNORECASE),
        # BSD 2-clause (simpler, no endorsement clause)
        (r'bsd\s+2[-\s]clause', 'BSD-2-Clause', re.IGNORECASE),
        (r'bsd-2-clause', 'BSD-2-Clause', re.IGNORECASE),
        (r'bsd\s+2', 'BSD-2-Clause', re.IGNORECASE),
        # Generic BSD pattern - check if it has 3 conditions (3-clause) or 2 conditions (2-clause)
        (r'redistribution.*conditions.*met.*redistributions.*retain.*redistributions.*binary.*form.*neither', 'BSD-3-Clause', re.IGNORECASE | re.DOTALL),
        (r'redistribution.*conditions.*met.*redistributions.*retain.*redistributions.*binary', 'BSD-2-Clause', re.IGNORECASE | re.DOTALL),
        (r'bsd\s+license', 'BSD-3-Clause', re.IGNORECASE),  # Default to 3-clause

        # ISC (check for ISC license text pattern, not just "ISC" word)
        (r'isc\s+license', 'ISC', re.IGNORECASE),
        (r'permission\s+to\s+use.*with\s+or\s+without\s+fee.*isc', 'ISC', re.IGNORECASE | re.DOTALL),
        (r'^isc$', 'ISC', re.MULTILINE),  # Only match standalone "ISC"

        # LGPL variants (check version 3 first)
        (r'gnu\s+lesser\s+general\s+public\s+license.*version\s*3', 'LGPL-3.0-or-later', re.IGNORECASE | re.DOTALL),
        (r'lesser\s+gnu\s+general\s+public\s+license.*version\s*3', 'LGPL-3.0-or-later', re.IGNORECASE | re.DOTALL),
        (r'lesser\s+gnu\s+public\s+license.*version\s*3', 'LGPL-3.0-or-later', re.IGNORECASE | re.DOTALL),
        (r'lgpl[-\s]?3\.0[-\s]or[-\s]later', 'LGPL-3.0-or-later', re.IGNORECASE),
        (r'lgpl[-\s]?3\.0', 'LGPL-3.0-only', re.IGNORECASE),
        (r'copying\.lesser', 'LGPL-3.0-or-later', re.IGNORECASE),  # COPYING.LESSER file indicates LGPL-3.0-or-later
        (r'lgpl[-\s]?2\.1[-\s]or[-\s]later', 'LGPL-2.1-or-later', re.IGNORECASE),
        (r'lgpl[-\s]?2\.1', 'LGPL-2.1-only', re.IGNORECASE),
        (r'lesser\s+gnu\s+general\s+public\s+license.*version\s*2', 'LGPL-2.1-or-later', re.IGNORECASE | re.DOTALL),
        (r'lesser\s+gnu\s+public\s+license', 'LGPL-2.1-or-later', re.IGNORECASE),

        # GPL variants
        (r'gpl[-\s]?2\.0', 'GPL-2.0-only', re.IGNORECASE),
        (r'gpl[-\s]?3\.0', 'GPL-3.0-only', re.IGNORECASE),

        # Unlicense / Public Domain
        (r'unlicense', 'Unlicense', re.IGNORECASE),
        (r'public\s+domain', 'Unlicense', re.IGNORECASE),
        (r'free\s+and\s+unencumbered.*public\s+domain', 'Unlicense', re.IGNORECASE | re.DOTALL),
        (r'unlicense\.org', 'Unlicense', re.IGNORECASE),

        # CC0 (Creative Commons Zero)
        (r'cc0[-\s]?1\.0', 'CC0-1.0', re.IGNORECASE),
        (r'creative\s+commons\s+zero', 'CC0-1.0', re.IGNORECASE),

        # Exact SPDX matches (already normalized) - check last
        (r'^Apache-2\.0$', 'Apache-2.0', re.MULTILINE),
        (r'^MIT$', 'MIT', re.MULTILINE),
        (r'^BSD-3-Clause$', 'BSD-3-Clause', re.MULTILINE),
        (r'^BSD-2-Clause$', 'BSD-2-Clause', re.MULTILINE),
        (r'^MPL-2\.0$', 'MPL-2.0', re.MULTILINE),
        (r'^ISC$', 'ISC', re.MULTILINE),
    ]

    # Check each pattern
    for pattern, spdx_id, flags in license_patterns:
        if re.search(pattern, license_text, flags):
            return spdx_id

    return None


def parse_app_file_for_license(package_dir: Path, package_name: str) -> Optional[str]:
    """
    Parse .app or .app.src file to extract license information.

    Returns SPDX license identifier if found, None otherwise.
    """
    # Check for .app.src first (source), then .app (compiled)
    app_files = [
        package_dir / 'src' / f'{package_name}.app.src',
        package_dir / f'{package_name}.app.src',
        package_dir / 'ebin' / f'{package_name}.app',
        package_dir / f'{package_name}.app',
    ]

    for app_file in app_files:
        if not app_file.exists():
            continue

        try:
            with open(app_file, 'r', encoding='utf-8', errors='ignore') as f:
                content = f.read()

            # Look for licenses field in Erlang term format
            # Pattern: {licenses,["License Name"]} or {licenses,["License Name", "Another"]}
            import re
            license_patterns = [
                r'licenses\s*,\s*\["([^"]+)"',  # {licenses,["Apache 2.0"]}
                r'licenses\s*,\s*\[([^\]]+)\]',  # {licenses,[...]}
            ]

            for pattern in license_patterns:
                matches = re.findall(pattern, content, re.IGNORECASE)
                if matches:
                    # Take the first license
                    license_str = matches[0].strip('"\'')
                    # Normalize to SPDX
                    spdx_id = normalize_license_to_spdx(license_str)
                    if spdx_id:
                        return spdx_id
                    # If normalization failed, try the raw string
                    if license_str:
                        return normalize_license_to_spdx(license_str)
        except Exception as e:
            # Continue to next file if this one fails
            continue

    return None


def find_license_file(package_dir: Path) -> Optional[Path]:
    """
    Find LICENSE, COPYRIGHT, or COPYING file in a package directory.

    Checks common locations and file names.
    """
    if not package_dir.exists() or not package_dir.is_dir():
        return None

    # Common license file names (order matters - check more specific ones first)
    license_names = [
        # COPYING files (GPL/LGPL)
        'COPYING.LESSER',  # LGPL
        'COPYING',  # GPL
        # Specific LICENSE files (check these before generic LICENSE)
        'LICENSE-APACHE-2.0',
        'LICENSE-APACHE2',
        'LICENSE-APACHE',
        'LICENSE-MPL-2.0',
        'LICENSE-MPL',
        'LICENSE-MIT',
        'LICENSE.txt',
        'LICENSE.md',
        'LICENSE',  # Generic LICENSE (check last)
        'LICENCE',  # British spelling
        'LICENCE.txt',  # British spelling
        'LICENCE.md',  # British spelling
        'license.txt',  # Lowercase variant
        'license',  # Lowercase variant
        'licence.txt',  # Lowercase British spelling
        'licence',  # Lowercase British spelling
        # COPYRIGHT files
        'COPYRIGHT.txt',
        'COPYRIGHT',
        'copyright.txt',  # Lowercase variant
        'copyright',  # Lowercase variant
    ]

    # Check root of package directory
    for name in license_names:
        license_file = package_dir / name
        if license_file.exists() and license_file.is_file():
            return license_file

    return None


def parse_license_file(license_file: Path) -> Optional[str]:
    """
    Parse a LICENSE or COPYRIGHT file to extract license information.

    Returns SPDX license identifier if found, None otherwise.
    """
    try:
        with open(license_file, 'r', encoding='utf-8', errors='ignore') as f:
            content = f.read()

        # Normalize HTML entities and common markdown formatting
        content = content.replace('&lt;', '<').replace('&gt;', '>')
        content = content.replace('_', ' ')  # Remove markdown emphasis

        # Read first 2000 characters (most license headers are in the beginning)
        content_sample = content[:2000]

        # Try to normalize to SPDX
        spdx_id = normalize_license_to_spdx(content_sample)
        if spdx_id:
            return spdx_id

        # If no match, try reading more of the file
        if len(content) > 2000:
            content_sample = content[:5000]
            spdx_id = normalize_license_to_spdx(content_sample)
            if spdx_id:
                return spdx_id

        return None
    except Exception as e:
        print(f"Warning: Failed to parse license file {license_file}: {e}", file=sys.stderr)
        return None


def find_package_source_directory(package_name: str, deps_dir: Path) -> Optional[Path]:
    """
    Find the source directory for a package.

    Tries multiple strategies:
    1. Check deps/APPNAME/apps/APPNAME/ (for umbrella projects like opentelemetry)
    2. Check deps/APPNAME/ with LICENSE file (prefer root LICENSE files)
    3. Check deps/APPNAME/deps/APPNAME/ (for nested dependency structures like amqp_client)
    4. Fall back to root deps/APPNAME/ directory
    """
    # Strategy 1: Check deps/APPNAME/apps/APPNAME/ (for umbrella projects)
    # This handles cases like opentelemetry where the actual app is in apps/opentelemetry/
    # Check this FIRST because umbrella projects often have LICENSE files in the nested app directory
    nested_app_path = deps_dir / package_name / "apps" / package_name
    if nested_app_path.exists() and nested_app_path.is_dir():
        # Verify there's actually a LICENSE file or .app file here
        if find_license_file(nested_app_path) or (nested_app_path / 'src' / f'{package_name}.app.src').exists():
            return nested_app_path

    # Strategy 2: Check deps directory first (before nested structures)
    # This ensures we check root LICENSE files before nested ones
    deps_path = deps_dir / package_name
    if deps_path.exists() and deps_path.is_dir():
        # If root has a LICENSE file (not just COPYING), prefer it
        root_license = find_license_file(deps_path)
        if root_license and 'LICENSE' in root_license.name.upper():
            return deps_path

    # Strategy 3: Check deps/APPNAME/deps/APPNAME/ (for nested dependency structures)
    # This handles cases like amqp_client/rabbit_common where the actual app is in deps/APPNAME/
    nested_deps_path = deps_dir / package_name / "deps" / package_name
    if nested_deps_path.exists() and nested_deps_path.is_dir():
        # Verify there's actually a LICENSE file or .app file here
        if find_license_file(nested_deps_path) or parse_app_file_for_license(nested_deps_path, package_name):
            return nested_deps_path

    # Strategy 4: Fall back to root deps directory if no nested structure found
    if deps_path.exists() and deps_path.is_dir():
        return deps_path

    return None


def enrich_package_license(package: Dict, lib_dir: Path, deps_dir: Path) -> bool:
    """
    Try to enrich a package with license information from LICENSE/COPYRIGHT files or .app files.

    Returns True if license was added, False otherwise.
    """
    # Check if package already has license
    existing_license = package.get('licenseDeclared', '')
    if existing_license and existing_license != 'NOASSERTION':
        return False  # Already has license

    package_name = package.get('name', '')
    if not package_name:
        return False

    # Find package source directory
    source_dir = find_package_source_directory(package_name, deps_dir)
    if not source_dir:
        return False

    # Strategy 1: Try to parse .app or .app.src files first
    spdx_id = parse_app_file_for_license(source_dir, package_name)
    if spdx_id:
        package['licenseDeclared'] = spdx_id
        # Try to find which file was used
        app_file = None
        for candidate in [source_dir / 'src' / f'{package_name}.app.src',
                          source_dir / f'{package_name}.app.src',
                          source_dir / 'ebin' / f'{package_name}.app',
                          source_dir / f'{package_name}.app']:
            if candidate.exists():
                app_file = candidate
                break

        if app_file:
            try:
                rel_path = app_file.relative_to(Path.cwd())
            except ValueError:
                try:
                    project_root = deps_dir.parent if deps_dir else Path.cwd()
                    rel_path = app_file.relative_to(project_root)
                except ValueError:
                    license_str = str(app_file)
                    if 'deps/' in license_str:
                        rel_path = 'deps/' + license_str.split('deps/')[-1]
                    else:
                        rel_path = app_file.name
            print(f"  Enriched {package_name}: {spdx_id} (from {rel_path})", file=sys.stderr)
            return True

    # Strategy 2: Find and parse license file
    license_file = find_license_file(source_dir)
    if not license_file:
        return False

    # Parse license file
    spdx_id = parse_license_file(license_file)
    if spdx_id:
        package['licenseDeclared'] = spdx_id
        # Try to make path relative, but fall back to a simpler representation if it fails
        try:
            rel_path = license_file.relative_to(Path.cwd())
        except ValueError:
            # If relative_to fails, try relative to project root (deps_dir parent)
            try:
                project_root = deps_dir.parent if deps_dir else Path.cwd()
                rel_path = license_file.relative_to(project_root)
            except ValueError:
                # If that also fails, try to extract just the deps/... part
                license_str = str(license_file)
                if 'deps/' in license_str:
                    rel_path = 'deps/' + license_str.split('deps/')[-1]
                elif 'lib/' in license_str:
                    rel_path = 'lib/' + license_str.split('lib/')[-1]
                else:
                    # Last resort: use just the filename
                    rel_path = license_file.name
        print(f"  Enriched {package_name}: {spdx_id} (from {rel_path})", file=sys.stderr)
        return True

    return False


def filter_and_enrich_spdx_sbom(
    sbom_path: Path,
    allowed_packages: Set[str],
    excluded_packages: Set[str] = None,
    lib_dir: Path = None,
    deps_dir: Path = None,
    enrich_licenses: bool = True
) -> dict:
    """
    Filter SPDX SBOM to include only packages in the allowed set,
    and enrich packages missing license information.

    Args:
        sbom_path: Path to the input SPDX SBOM JSON file
        allowed_packages: Set of package names that should be included
        excluded_packages: Set of package names that should be explicitly excluded
        lib_dir: Library directory with released apps (for license enrichment)
        deps_dir: Dependencies directory (for license enrichment)
        enrich_licenses: Whether to enrich missing licenses
    """
    if excluded_packages is None:
        excluded_packages = set()

    with open(sbom_path, 'r') as f:
        sbom = json.load(f)

    # Ensure creationInfo has document_namespace (required by spdx-tools)
    # Also ensure top-level documentNamespace exists for compatibility
    doc_name = sbom.get('name', 'unknown')
    import uuid

    # Get or generate namespace
    if 'documentNamespace' in sbom:
        doc_namespace = sbom['documentNamespace']
    else:
        namespace_uuid = str(uuid.uuid4())
        doc_namespace = f"https://spdx.org/spdxdocs/{doc_name}-{namespace_uuid}"
        sbom['documentNamespace'] = doc_namespace

    # Ensure creationInfo has document_namespace (required by spdx-tools parser)
    if 'creationInfo' in sbom:
        if 'document_namespace' not in sbom['creationInfo']:
            sbom['creationInfo']['document_namespace'] = doc_namespace

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

    # Enrich packages with missing license information
    if enrich_licenses and lib_dir and deps_dir:
        print("\nEnriching packages with missing license information...", file=sys.stderr)
        enriched_count = 0
        for pkg in filtered_packages:
            if enrich_package_license(pkg, lib_dir, deps_dir):
                enriched_count += 1
        if enriched_count > 0:
            print(f"Enriched {enriched_count} packages with license information.", file=sys.stderr)
        else:
            print("No packages were enriched.", file=sys.stderr)

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

    # SPDX requires at least one DESCRIBES relationship when there are multiple packages
    # Add one if missing and we have packages
    if len(filtered_packages) > 0:
        has_describes = any(
            rel.get('spdxElementId') == 'SPDXRef-DOCUMENT' and
            rel.get('relationshipType') == 'DESCRIBES'
            for rel in sbom.get('relationships', [])
        )
        if not has_describes:
            # Add DESCRIBES relationship to first package
            if 'relationships' not in sbom:
                sbom['relationships'] = []
            sbom['relationships'].append({
                'spdxElementId': 'SPDXRef-DOCUMENT',
                'relationshipType': 'DESCRIBES',
                'relatedSpdxElement': filtered_packages[0]['SPDXID']
            })

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

    parser = argparse.ArgumentParser(
        description="Filter SPDX SBOM to include only released external production dependencies and enrich missing licenses"
    )
    parser.add_argument("--sbom", type=Path, help="Input SPDX SBOM file (default: bom.spdx.json in project root)")
    parser.add_argument("--output", type=Path, help="Output filtered SPDX SBOM file (default: bom.spdx.filtered.json in project root)")
    parser.add_argument("--lib-dir", type=Path, help="Library directory with released apps (default: _build/emqx-enterprise/rel/emqx/lib in project root)")
    parser.add_argument("--deps-dir", type=Path, help="Dependencies directory (default: deps in project root)")
    parser.add_argument("--no-enrich", action="store_true", help="Skip license enrichment")

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

    # Filter and enrich SBOM
    print(f"\n{'=' * 60}", file=sys.stderr)
    print("Filtering and enriching SPDX SBOM...", file=sys.stderr)
    # Explicitly exclude test/dev dependencies to ensure they never make it into the filtered SBOM
    filtered_sbom = filter_and_enrich_spdx_sbom(
        sbom_path,
        released_external_deps,
        excluded_packages=test_deps,
        lib_dir=lib_dir,
        deps_dir=deps_dir,
        enrich_licenses=not args.no_enrich
    )

    with open(output_path, 'w') as f:
        json.dump(filtered_sbom, f, indent=2)

    print(f"Filtered and enriched SBOM written to: {output_path}", file=sys.stderr)

    # Also print the list of released external deps to stdout for easy use
    print("\n# Released external dependencies (one per line):")
    for dep in sorted(released_external_deps):
        print(dep)


if __name__ == '__main__':
    main()
