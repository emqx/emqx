#!/usr/bin/env python3
"""
Convert CycloneDX SBOM to SPDX format.

This script converts a CycloneDX JSON SBOM to SPDX 2.3 format.
"""

import json
import sys
import uuid
from datetime import datetime
from pathlib import Path
from typing import Dict, List, Any


def convert_hash(alg: str, content: str) -> Dict[str, str]:
    """Convert CycloneDX hash to SPDX hash format."""
    # Map CycloneDX algorithms to SPDX algorithms
    algorithm_map = {
        "SHA-256": "SHA256",
        "SHA-384": "SHA384",
        "SHA-512": "SHA512",
        "SHA-1": "SHA1",
        "MD5": "MD5",
    }
    algorithm = algorithm_map.get(alg.upper(), alg.upper())
    return {
        "algorithm": algorithm,
        "checksumValue": content
    }


def normalize_license_id(license_str: str) -> str:
    """Normalize license string to valid SPDX license identifier."""
    if not license_str or license_str == "NOASSERTION":
        return "NOASSERTION"

    # Common license name mappings to SPDX identifiers
    license_map = {
        "Apache v2.0": "Apache-2.0",
        "Apache 2.0": "Apache-2.0",
        "Apache License 2.0": "Apache-2.0",
        "Apache": "Apache-2.0",  # Default to Apache-2.0 if just "Apache"
        "APL 2.0": "Apache-2.0",
        "MIT": "MIT",
        "BSD": "BSD-3-Clause",
        "BSD-3-Clause": "BSD-3-Clause",
        "ISC": "ISC",
        "LGPL-2.1-or-later": "LGPL-2.1-or-later",
    }

    # Check if it's already a valid SPDX identifier (contains -)
    license_str = license_str.strip()

    # Try exact match first
    if license_str in license_map:
        return license_map[license_str]

    # Check if it looks like a valid SPDX ID (contains version number with dash)
    if "-" in license_str and any(char.isdigit() for char in license_str):
        # Might already be valid, but check common patterns
        if license_str.upper().startswith("APACHE"):
            return "Apache-2.0"
        return license_str

    # If no match found, return NOASSERTION for invalid licenses
    return "NOASSERTION"


def convert_license(license_data: Any) -> str:
    """Convert CycloneDX license to SPDX license string."""
    if isinstance(license_data, dict):
        if "id" in license_data:
            return normalize_license_id(license_data["id"])
        elif "name" in license_data:
            return normalize_license_id(license_data["name"])
        elif "expression" in license_data:
            return normalize_license_id(license_data["expression"])
    elif isinstance(license_data, str):
        return normalize_license_id(license_data)
    return "NOASSERTION"


def generate_spdx_id_from_bom_ref(bom_ref: str, component: Dict[str, Any] = None, spdx_id_prefix: str = "SPDXRef-") -> str:
    """Generate SPDX ID from bom-ref or component info.

    SPDX IDs can only contain letters, numbers, "." and "-".
    Replace all colons, slashes, and underscores with dashes.
    """
    if bom_ref:
        # Replace all colons, slashes, and underscores with dashes
        safe_ref = bom_ref.replace(':', '-').replace('/', '-').replace('_', '-')
        return f"{spdx_id_prefix}{safe_ref}"
    elif component:
        # Fallback: use name and version if bom-ref is missing
        name = component.get("name", "unknown")
        version = component.get("version", "")
        safe_name = name.replace(':', '-').replace('/', '-').replace('_', '-')
        if version:
            return f"{spdx_id_prefix}{safe_name}-{version}"
        else:
            return f"{spdx_id_prefix}{safe_name}"
    else:
        return f"{spdx_id_prefix}UNKNOWN"


def convert_component_to_spdx_package(component: Dict[str, Any], spdx_id_prefix: str = "SPDXRef-") -> Dict[str, Any]:
    """Convert a CycloneDX component to an SPDX package."""
    bom_ref = component.get("bom-ref", "")
    spdx_id = generate_spdx_id_from_bom_ref(bom_ref, component, spdx_id_prefix)

    # Extract name and version
    name = component.get("name", "")
    version = component.get("version", "")

    # Convert hashes
    checksums = []
    if "hashes" in component:
        for hash_obj in component["hashes"]:
            checksums.append(convert_hash(hash_obj["alg"], hash_obj["content"]))

    # Convert license
    license_declared = "NOASSERTION"
    if "licenses" in component and component["licenses"]:
        license_list = component["licenses"]
        if license_list:
            license_declared = convert_license(license_list[0].get("license", {}))

    # Build SPDX package
    spdx_package = {
        "SPDXID": spdx_id,
        "name": name,
        "downloadLocation": "NOASSERTION",
        "primaryPackagePurpose": "LIBRARY"
    }

    if version:
        spdx_package["versionInfo"] = version

    if checksums:
        spdx_package["checksums"] = checksums

    if license_declared != "NOASSERTION":
        spdx_package["licenseDeclared"] = license_declared

    return spdx_package


def convert_relationships(components: List[Dict[str, Any]], metadata_component: Dict[str, Any], bom_ref_to_spdx_id: Dict[str, str] = None, packages: List[Dict[str, Any]] = None) -> List[Dict[str, Any]]:
    """Convert CycloneDX dependencies to SPDX relationships."""
    relationships = []

    # Create relationship from document to root component
    root_bom_ref = metadata_component.get("bom-ref", "")
    if root_bom_ref and bom_ref_to_spdx_id:
        root_spdx_id = bom_ref_to_spdx_id.get(root_bom_ref)
        if root_spdx_id:
            relationships.append({
                "spdxElementId": "SPDXRef-DOCUMENT",
                "relationshipType": "DESCRIBES",
                "relatedSpdxElement": root_spdx_id
            })
    elif packages and len(packages) > 0:
        # Fallback: describe first package if no root component
        relationships.append({
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": packages[0]["SPDXID"]
        })

    if not bom_ref_to_spdx_id:
        return relationships

    # Convert dependencies
    for component in components:
        bom_ref = component.get("bom-ref", "")
        if not bom_ref:
            continue

        spdx_id = bom_ref_to_spdx_id.get(bom_ref)
        if not spdx_id:
            continue

        # If component has dependencies, create relationships
        if "dependencies" in component:
            for dep_ref in component["dependencies"]:
                dep_spdx_id = bom_ref_to_spdx_id.get(dep_ref)
                if dep_spdx_id:
                    relationships.append({
                        "spdxElementId": spdx_id,
                        "relationshipType": "DEPENDS_ON",
                        "relatedSpdxElement": dep_spdx_id
                    })

    return relationships


def convert_cyclonedx_to_spdx(cdx_path: Path, output_path: Path = None) -> Dict[str, Any]:
    """Convert CycloneDX SBOM to SPDX format."""
    with open(cdx_path, 'r') as f:
        cdx_data = json.load(f)

    metadata = cdx_data.get("metadata", {})
    components = cdx_data.get("components", [])

    # Extract metadata
    root_component = metadata.get("component", {})
    root_name = root_component.get("name", "unknown")
    root_version = root_component.get("version", "")

    # Generate document name
    doc_name = f"{root_name}-{root_version}" if root_version else root_name

    # Get timestamp for creationInfo
    timestamp = metadata.get("timestamp", datetime.utcnow().isoformat() + "Z")

    # Extract creator information from tools if available
    creators = []
    tools = metadata.get("tools", {})
    tool_components = tools.get("components", [])
    for tool in tool_components:
        tool_name = tool.get("name", "")
        tool_version = tool.get("version", "")
        if tool_name:
            creator_name = f"{tool_name}" + (f" {tool_version}" if tool_version else "")
            creators.append(f"Person: {creator_name}")

    # If no creators found, add a default one
    if not creators:
        creators = ["Person: Mix SBoM"]

    # Convert components to SPDX packages
    # Track bom-ref to SPDX ID mapping and handle duplicates
    packages = []
    bom_ref_counts = {}  # Track how many times each bom-ref appears
    bom_ref_to_spdx_id = {}  # Map bom-ref to list of SPDX IDs (for relationship building)

    for component in components:
        bom_ref = component.get("bom-ref", "")
        spdx_package = convert_component_to_spdx_package(component)
        original_spdx_id = spdx_package["SPDXID"]

        # Handle duplicate bom-refs by making SPDX IDs unique
        if bom_ref:
            bom_ref_counts[bom_ref] = bom_ref_counts.get(bom_ref, 0) + 1
            count = bom_ref_counts[bom_ref]

            # If this is a duplicate, append counter to make unique
            if count > 1:
                spdx_package["SPDXID"] = f"{original_spdx_id}-{count}"

            # Store mapping (use first occurrence for relationships)
            if bom_ref not in bom_ref_to_spdx_id:
                bom_ref_to_spdx_id[bom_ref] = original_spdx_id

        packages.append(spdx_package)

    # Convert relationships (use first occurrence of each bom-ref for relationships)
    relationships = convert_relationships(components, root_component, bom_ref_to_spdx_id, packages)

    # Build SPDX document
    spdx_doc = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "spdxVersion": "SPDX-2.3",
        "creationInfo": {
            "comment": "This SPDX document has been converted from CycloneDX format.",
            "created": timestamp,
            "creators": creators,
            "licenseListVersion": "3.23"
        },
        "name": doc_name,
        "dataLicense": "CC0-1.0",
        # documentNamespace is optional and removed as it's not a reachable link
        "packages": packages
    }

    if relationships:
        spdx_doc["relationships"] = relationships

    # Write output
    if output_path:
        with open(output_path, 'w') as f:
            json.dump(spdx_doc, f, indent=2)
        print(f"Converted SPDX SBOM written to: {output_path}", file=sys.stderr)

    return spdx_doc


def main():
    import argparse

    parser = argparse.ArgumentParser(description="Convert CycloneDX SBOM to SPDX format")
    parser.add_argument("input", type=Path, help="Input CycloneDX JSON file")
    parser.add_argument("-o", "--output", type=Path, help="Output SPDX JSON file (default: input with .spdx.json extension)")

    args = parser.parse_args()

    if not args.input.exists():
        print(f"Error: Input file not found: {args.input}", file=sys.stderr)
        sys.exit(1)

    output_path = args.output
    if not output_path:
        output_path = args.input.parent / f"{args.input.stem}.spdx.json"

    try:
        convert_cyclonedx_to_spdx(args.input, output_path)
        print(f"Successfully converted {args.input} to {output_path}", file=sys.stderr)
    except Exception as e:
        print(f"Error converting SBOM: {e}", file=sys.stderr)
        import traceback
        traceback.print_exc()
        sys.exit(1)


if __name__ == '__main__':
    main()
