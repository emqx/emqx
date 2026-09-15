"""Regression tests for lost or mismatched release-image attestations."""

import hashlib
import importlib.util
import io
import json
from pathlib import Path
import tarfile
import tempfile
import unittest


spec = importlib.util.spec_from_file_location(
    "verify_attestations", Path(__file__).parents[1] / "verify-docker-attestations.py"
)
verifier = importlib.util.module_from_spec(spec)
spec.loader.exec_module(verifier)


class VerifyAttestationsTest(unittest.TestCase):
    def write_archive(self, path, *, sbom=True, max_mode=True, dhi=True, wrong_subject=False,
                      corrupt=False, classic=False, v1=False):
        files = {}

        def blob(value):
            data = json.dumps(value).encode()
            digest = hashlib.sha256(data).hexdigest()
            files[f"blobs/sha256/{digest}"] = data
            return {"digest": f"sha256:{digest}", "size": len(data)}

        config = blob({"os": "linux", "architecture": "arm64"})
        image = blob({"config": config, "layers": []})
        image_digest = image["digest"]
        provenance = {"materials": []}
        if max_mode:
            provenance["buildConfig"] = {"llbDefinition": [{"id": "source"}]}
        if dhi:
            provenance["materials"] = [{
                "uri": "pkg:docker/dhi.io/debian-base@trixie?platform=linux%2Farm64",
                "digest": {"sha256": "a" * 64},
            }]

        def statement(predicate_type, predicate):
            return blob({
                "subject": [{"digest": {"sha256": "b" * 64 if wrong_subject else image_digest[7:]}}],
                "predicateType": predicate_type,
                "predicate": predicate,
            })

        if v1:
            provenance = {"buildDefinition": {
                "resolvedDependencies": provenance["materials"],
                "internalParameters": {"buildConfig": provenance.get("buildConfig", {})},
            }}
        layers = [statement(verifier.PROVENANCE_V1 if v1 else verifier.PROVENANCE, provenance)]
        if sbom:
            layers.append(statement(verifier.SBOM, {"spdxVersion": "SPDX-2.3"}))
        attestation = blob({"layers": layers})
        attestation["annotations"] = {
            "vnd.docker.reference.type": "attestation-manifest",
            "vnd.docker.reference.digest": image_digest,
        }
        # docker save wraps the BuildKit image index in an archive-level index.
        index = blob({"manifests": [image, attestation]})
        files["index.json"] = json.dumps({"manifests": [index]}).encode()
        if corrupt:
            files[f"blobs/sha256/{layers[0]['digest'][7:]}"] += b" "
        if classic:
            files = {"manifest.json": b"[]"}
        with tarfile.open(path, "w:gz") as archive:
            for name, data in files.items():
                member = tarfile.TarInfo(name)
                member.size = len(data)
                archive.addfile(member, io.BytesIO(data))

    def test_preserved_nested_index(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar.gz"
            self.write_archive(path)
            verifier.verify_archive(path, "linux/arm64", require_dhi=True)

    def test_slsa_v1(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar.gz"
            self.write_archive(path, v1=True)
            verifier.verify_archive(path, "linux/arm64", require_dhi=True)

    def test_rejects_lost_or_mismatched_attestations(self):
        cases = [
            ({"sbom": False}, "missing SPDX SBOM"),
            ({"max_mode": False}, "missing max-mode provenance"),
            ({"max_mode": False, "v1": True}, "missing max-mode provenance"),
            ({"dhi": False}, "no DHI material"),
            ({"dhi": False, "v1": True}, "no DHI material"),
            ({"wrong_subject": True}, "does not describe image"),
            ({"corrupt": True}, "Digest mismatch"),
            ({"classic": True}, "containerd image store"),
        ]
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar.gz"
            for options, message in cases:
                with self.subTest(options=options):
                    self.write_archive(path, **options)
                    with self.assertRaisesRegex(ValueError, message):
                        verifier.verify_archive(path, "linux/arm64", require_dhi=True)

    def test_rejects_wrong_architecture(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar.gz"
            self.write_archive(path)
            with self.assertRaisesRegex(ValueError, "Expected only linux/amd64"):
                verifier.verify_archive(path, "linux/amd64")

    def test_public_debian_pr_image(self):
        with tempfile.TemporaryDirectory() as directory:
            path = Path(directory) / "image.tar.gz"
            self.write_archive(path, dhi=False)
            verifier.verify_archive(path, "linux/arm64")


if __name__ == "__main__":
    unittest.main()
