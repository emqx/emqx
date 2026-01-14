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


def convert_license(license_data: Any) -> str:
    """Convert CycloneDX license to SPDX license string."""
    if isinstance(license_data, dict):
        if "id" in license_data:
            return license_data["id"]
        elif "name" in license_data:
            return license_data["name"]
        elif "expression" in license_data:
            return license_data["expression"]
    elif isinstance(license_data, str):
        return license_data
    return "NOASSERTION"


def convert_component_to_spdx_package(component: Dict[str, Any], spdx_id_prefix: str = "SPDXRef-") -> Dict[str, Any]:
    """Convert a CycloneDX component to an SPDX package."""
    bom_ref = component.get("bom-ref", "")

    # Generate SPDXID from bom-ref
    if bom_ref.startswith("otp:component:"):
        # Format: otp:component:name:source:hash
        parts = bom_ref.split(":")
        if len(parts) >= 3:
            spdx_id = f"{spdx_id_prefix}{bom_ref.replace(':', ':')}"
        else:
            spdx_id = f"{spdx_id_prefix}{bom_ref.replace(':', '-')}"
    else:
        spdx_id = f"{spdx_id_prefix}{bom_ref.replace(':', '-').replace('/', '-')}"

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


def convert_relationships(components: List[Dict[str, Any]], metadata_component: Dict[str, Any]) -> List[Dict[str, Any]]:
    """Convert CycloneDX dependencies to SPDX relationships."""
    relationships = []

    # Create relationship from document to root component
    root_bom_ref = metadata_component.get("bom-ref", "")
    if root_bom_ref:
        root_spdx_id = f"SPDXRef-{root_bom_ref.replace(':', '-').replace('/', '-')}"
        relationships.append({
            "spdxElementId": "SPDXRef-DOCUMENT",
            "relationshipType": "DESCRIBES",
            "relatedSpdxElement": root_spdx_id
        })

    # Convert dependencies
    for component in components:
        bom_ref = component.get("bom-ref", "")
        if not bom_ref:
            continue

        spdx_id = f"SPDXRef-{bom_ref.replace(':', '-').replace('/', '-')}"

        # If component has dependencies, create relationships
        if "dependencies" in component:
            for dep_ref in component["dependencies"]:
                dep_spdx_id = f"SPDXRef-{dep_ref.replace(':', '-').replace('/', '-')}"
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

    # Generate namespace
    timestamp = metadata.get("timestamp", datetime.utcnow().isoformat() + "Z")
    namespace_uuid = str(uuid.uuid4())
    doc_namespace = f"http://spdx.org/spdxdocs/{doc_name}-{namespace_uuid}"

    # Convert components to SPDX packages
    packages = []
    for component in components:
        spdx_package = convert_component_to_spdx_package(component)
        packages.append(spdx_package)

    # Convert relationships
    relationships = convert_relationships(components, root_component)

    # Build SPDX document
    spdx_doc = {
        "SPDXID": "SPDXRef-DOCUMENT",
        "spdxVersion": "SPDX-2.3",
        "creationInfo": {
            "comment": "This SPDX document has been converted from CycloneDX format.",
            "created": timestamp
        },
        "name": doc_name,
        "dataLicense": "CC0-1.0",
        "documentNamespace": doc_namespace,
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
