#!/usr/bin/env python3
"""Verify BuildKit attestations survive docker save/load in release archives."""

import argparse
import hashlib
import json
import sys
import tarfile


PROVENANCE = "https://slsa.dev/provenance/v0.2"
PROVENANCE_V1 = "https://slsa.dev/provenance/v1"
SBOM = "https://spdx.dev/Document"


def verify_archive(filename, platform=None, require_dhi=False):
    with tarfile.open(filename, "r:*") as archive:
        def read_json(name, digest=None):
            stream = archive.extractfile(name)
            if stream is None:
                raise ValueError(f"Missing JSON file: {name}")
            data = stream.read()
            if digest and hashlib.sha256(data).hexdigest() != digest:
                raise ValueError(f"Digest mismatch: {name}")
            return json.loads(data)

        def read_blob(descriptor):
            algorithm, digest = descriptor["digest"].split(":", 1)
            if algorithm != "sha256":
                raise ValueError(f"Unsupported digest algorithm: {algorithm}")
            return read_json(f"blobs/sha256/{digest}", digest)

        images = {}
        attestations = {}
        visited = set()

        def walk(index):
            for descriptor in index["manifests"]:
                digest = descriptor["digest"]
                if digest in visited:
                    continue
                visited.add(digest)
                manifest = read_blob(descriptor)
                if "manifests" in manifest:
                    walk(manifest)
                    continue
                annotations = descriptor.get("annotations", {})
                subject = annotations.get("vnd.docker.reference.digest")
                if annotations.get("vnd.docker.reference.type") == "attestation-manifest":
                    attestations.setdefault(subject, []).extend(manifest["layers"])
                else:
                    config = read_blob(manifest["config"])
                    image_platform = f"{config['os']}/{config['architecture']}"
                    images[digest] = image_platform

        try:
            walk(read_json("index.json"))
        except KeyError as error:
            raise ValueError(
                "Archive is missing OCI image metadata; use Docker's containerd image store"
            ) from error

        if not images:
            raise ValueError("Archive has no runnable images")
        if platform and set(images.values()) != {platform}:
            raise ValueError(f"Expected only {platform}, found {sorted(images.values())}")

        for digest, image_platform in images.items():
            predicates = {}
            for layer in attestations.get(digest, []):
                statement = read_blob(layer)
                subjects = statement.get("subject", [])
                if not any(s.get("digest", {}).get("sha256") == digest[7:] for s in subjects):
                    raise ValueError(f"Attestation does not describe image {digest}")
                predicates[statement["predicateType"]] = statement["predicate"]

            if PROVENANCE_V1 in predicates:
                definition = predicates[PROVENANCE_V1].get("buildDefinition", {})
                build_config = definition.get("internalParameters", {}).get("buildConfig", {})
                materials = definition.get("resolvedDependencies", [])
            else:
                provenance = predicates.get(PROVENANCE, {})
                build_config = provenance.get("buildConfig", {})
                materials = provenance.get("materials", [])
            if not build_config.get("llbDefinition"):
                raise ValueError(f"{image_platform}: missing max-mode provenance")
            if not predicates.get(SBOM, {}).get("spdxVersion"):
                raise ValueError(f"{image_platform}: missing SPDX SBOM")
            if require_dhi:
                if not any(
                    m.get("uri", "").startswith("pkg:docker/dhi.io/")
                    and m.get("digest", {}).get("sha256")
                    for m in materials
                ):
                    raise ValueError(f"{image_platform}: no DHI material digest in provenance")

            print(f"{image_platform}: max-mode provenance and SBOM verified ({digest})")


def main():
    parser = argparse.ArgumentParser(description=__doc__)
    parser.add_argument("archive")
    parser.add_argument("--platform", help="Require a single-platform archive, e.g. linux/arm64")
    parser.add_argument("--require-dhi", action="store_true", help="Require DHI build materials")
    args = parser.parse_args()
    try:
        verify_archive(args.archive, args.platform, args.require_dhi)
    except (ValueError, KeyError, OSError, tarfile.TarError) as error:
        print(f"Attestation verification failed: {error}", file=sys.stderr)
        return 1
    return 0


if __name__ == "__main__":
    sys.exit(main())
