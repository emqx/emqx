#!/usr/bin/env python3
"""
Pytest tests for scripts/rel/package_manifests.py.

Usage:
    pytest scripts/test/test_package_manifests.py -v
"""

import json
import subprocess
import sys
from pathlib import Path

import pytest

sys.path.insert(0, str(Path(__file__).resolve().parents[1] / "rel"))
import package_manifests as pm  # noqa: E402

VERSION = "6.3.1"
PREFIX = f"emqx-enterprise-{VERSION}"


def run_git(repo, *args):
    subprocess.run(["git", "-C", str(repo), *args], check=True, capture_output=True)


@pytest.fixture
def repo(tmp_path):
    repo = tmp_path / "repo"
    repo.mkdir()
    run_git(repo, "init", "-q")
    run_git(repo, "config", "user.name", "test")
    run_git(repo, "config", "user.email", "test@example.com")
    run_git(repo, "config", "tag.gpgSign", "false")
    run_git(repo, "commit", "-q", "--allow-empty", "-m", "release")
    run_git(repo, "tag", VERSION)
    return repo


def git_version():
    out = subprocess.run(["git", "--version"], capture_output=True, text=True).stdout
    return tuple(int(x) for x in out.split()[2].split(".")[:2])


def head(repo):
    return subprocess.run(
        ["git", "-C", str(repo), "rev-parse", "HEAD"],
        check=True, capture_output=True, text=True,
    ).stdout.strip()


def add_package(d, name, content=b"package", sidecar="auto"):
    path = d / name
    path.write_bytes(content)
    if sidecar == "auto":
        sidecar = pm.hashlib.sha256(content).hexdigest()
    if sidecar is not None:
        (d / (name + ".sha256")).write_text(sidecar)
    return path


@pytest.fixture
def packages(tmp_path):
    d = tmp_path / "packages"
    d.mkdir()
    add_package(d, f"{PREFIX}-debian12-amd64.deb", b"deb")
    # The macOS builder writes the digest with a trailing newline.
    add_package(
        d, f"{PREFIX}-macos14-arm64.zip", b"zip",
        pm.hashlib.sha256(b"zip").hexdigest() + "\n",
    )
    add_package(d, f"emqx-enterprise_{VERSION}_amd64.snap", b"snap")
    (d / "plugins").mkdir()
    add_package(d / "plugins", "emqx_plugin-1.0.0.tar.gz", b"plugin")
    return d


class FakeGitHub:
    def __init__(self, existing=None, fail_commits=0):
        self.repo = "emqx/emqx-package-manifests"
        self.existing = dict(existing or {})
        self.fail_commits = fail_commits
        self.commits = []
        self.heads = 0

    def head(self, branch):
        self.heads += 1
        return f"head{self.heads}"

    def existing_blobs(self, head, version):
        return dict(self.existing)

    def commit(self, branch, head, files, headline, body):
        if self.fail_commits:
            self.fail_commits -= 1
            raise RuntimeError("Expected branch to point to a different commit")
        self.commits.append((head, dict(files)))
        for path, data in files.items():
            self.existing[path] = pm.git_blob_sha(data)
        return {"oid": "c", "url": "https://example.com/c"}


def generate(packages, repo, tmp_path):
    out = tmp_path / "out"
    rc = pm.main([
        "--packages-dir", str(packages), "--version", VERSION, "--tag", VERSION,
        "--git-dir", str(repo), "--out-dir", str(out),
    ])
    return rc, out / "manifests" / VERSION


@pytest.fixture(autouse=True)
def offline(monkeypatch):
    """Keep dry runs off the network."""
    def no_network(self, branch):
        raise OSError("offline")
    monkeypatch.setattr(pm.GitHub, "head", no_network)
    monkeypatch.setattr(pm.time, "sleep", lambda s: None)


def test_records_packages_from_sidecars(packages, repo, tmp_path):
    """Records the .deb and .zip, skips the snap and plugins, reads sidecars."""
    rc, out = generate(packages, repo, tmp_path)
    assert rc == 0
    assert sorted(p.name for p in out.iterdir()) == [
        f"{PREFIX}-debian12-amd64.deb.json",
        f"{PREFIX}-macos14-arm64.zip.json",
    ]
    text = (out / f"{PREFIX}-macos14-arm64.zip.json").read_text()
    record = json.loads(text)
    assert list(record) == [
        "filename", "version", "git_commit", "git_tag", "git_tag_signed",
        "sha256", "size_bytes",
    ]
    assert record == {
        "filename": f"{PREFIX}-macos14-arm64.zip",
        "version": VERSION,
        "git_commit": head(repo),
        "git_tag": VERSION,
        "git_tag_signed": False,
        "sha256": pm.hashlib.sha256(b"zip").hexdigest(),
        "size_bytes": 3,
    }
    assert text == json.dumps(record, indent=2) + "\n"


def test_digest_comes_from_sidecar_not_file(packages, repo, tmp_path):
    """A package whose bytes changed keeps the digest its sidecar holds."""
    name = f"{PREFIX}-debian12-amd64.deb"
    (packages / name).write_bytes(b"tampered")
    rc, out = generate(packages, repo, tmp_path)
    assert rc == 0
    record = json.loads((out / f"{name}.json").read_text())
    assert record["sha256"] == pm.hashlib.sha256(b"deb").hexdigest()


def test_missing_sidecar_is_an_error(packages, repo, tmp_path):
    """A package without a sidecar fails the run; the others are still recorded."""
    add_package(packages, f"{PREFIX}-el9-amd64.rpm", sidecar=None)
    rc, out = generate(packages, repo, tmp_path)
    assert rc == 1
    assert not (out / f"{PREFIX}-el9-amd64.rpm.json").exists()
    assert (out / f"{PREFIX}-debian12-amd64.deb.json").exists()


def test_malformed_sidecar_is_an_error(packages, repo, tmp_path):
    """A sidecar that does not hold a bare hex digest fails the run."""
    add_package(packages, f"{PREFIX}-el9-amd64.rpm", sidecar="abc  file\n")
    rc, _ = generate(packages, repo, tmp_path)
    assert rc == 1


def test_unknown_extension_is_an_error(packages, repo, tmp_path):
    """A release file with an unexpected extension is reported, not dropped."""
    add_package(packages, f"{PREFIX}-windows-amd64.exe")
    rc, _ = generate(packages, repo, tmp_path)
    assert rc == 1


def test_no_packages_is_an_error(tmp_path, repo):
    """An empty package directory fails the run."""
    empty = tmp_path / "empty"
    empty.mkdir()
    rc, _ = generate(empty, repo, tmp_path)
    assert rc == 1


def test_prerelease_version_rejected(packages, repo):
    """Only X.Y.Z versions are accepted."""
    rc = pm.main([
        "--packages-dir", str(packages), "--version", "6.3.1-rc.1",
        "--tag", VERSION, "--git-dir", str(repo),
    ])
    assert rc == 2


def test_lightweight_tag_is_unsigned(repo):
    """A lightweight tag is recorded as unsigned."""
    assert pm.tag_info(repo, VERSION) == (head(repo), False)


def test_annotated_unsigned_tag(repo):
    """An annotated tag without a signature is recorded as unsigned."""
    run_git(repo, "tag", "-a", "-m", "release", "6.3.2")
    assert pm.tag_info(repo, "6.3.2") == (head(repo), False)


def test_unverifiable_signed_tag_is_an_error(repo):
    """A signed tag that git verify-tag rejects is an error, not false."""
    msg = "release\n-----BEGIN PGP SIGNATURE-----\nAAAA\n-----END PGP SIGNATURE-----\n"
    run_git(repo, "tag", "-a", "-m", msg, "6.3.3")
    with pytest.raises(ValueError, match="verify-tag"):
        pm.tag_info(repo, "6.3.3")


def test_missing_tag_is_an_error(repo):
    """A tag that was not fetched is an error."""
    with pytest.raises(ValueError, match="not present"):
        pm.tag_info(repo, "9.9.9")


def records_for(packages, repo):
    commit, signed = pm.tag_info(repo, VERSION)
    return {
        pm.record_path(VERSION, p.name):
            pm.encode_record(pm.build_record(p, VERSION, VERSION, commit, signed))
        for p in pm.find_packages(packages, "emqx-enterprise", VERSION, pm.Problems())
    }


def test_publish_skips_existing_records(packages, repo):
    """An existing identical record is not rewritten; only missing ones are added."""
    records = records_for(packages, repo)
    deb = pm.record_path(VERSION, f"{PREFIX}-debian12-amd64.deb")
    gh = FakeGitHub(existing={deb: pm.git_blob_sha(records[deb])})
    problems = pm.Problems()
    pm.publish(gh, "master", VERSION, records, None, problems)
    assert problems.errors == []
    assert len(gh.commits) == 1
    assert list(gh.commits[0][1]) == [pm.record_path(VERSION, f"{PREFIX}-macos14-arm64.zip")]


def test_publish_rerun_makes_no_commit(packages, repo):
    """Running a second time for the same release commits nothing."""
    records = records_for(packages, repo)
    gh = FakeGitHub()
    pm.publish(gh, "master", VERSION, records, None, pm.Problems())
    problems = pm.Problems()
    pm.publish(gh, "master", VERSION, records, None, problems)
    assert problems.errors == []
    assert len(gh.commits) == 1


def test_publish_reports_differing_record(packages, repo):
    """A differing existing record is left alone and reported as an error."""
    records = records_for(packages, repo)
    deb = pm.record_path(VERSION, f"{PREFIX}-debian12-amd64.deb")
    gh = FakeGitHub(existing={deb: "0" * 40})
    problems = pm.Problems()
    pm.publish(gh, "master", VERSION, records, None, problems)
    assert len(problems.errors) == 1 and deb in problems.errors[0]
    assert deb not in gh.commits[0][1]
    assert gh.existing[deb] == "0" * 40


def test_publish_retries_when_branch_moves(packages, repo):
    """A rejected commit re-reads the branch and retries against the new head."""
    records = records_for(packages, repo)
    gh = FakeGitHub(fail_commits=2)
    problems = pm.Problems()
    pm.publish(gh, "master", VERSION, records, None, problems)
    assert problems.errors == []
    assert [c[0] for c in gh.commits] == ["head3"]


def test_publish_gives_up_after_attempts(packages, repo):
    """Persistent commit failures end in an error."""
    records = records_for(packages, repo)
    gh = FakeGitHub(fail_commits=pm.PUBLISH_ATTEMPTS)
    problems = pm.Problems()
    pm.publish(gh, "master", VERSION, records, None, problems)
    assert len(problems.errors) == 1
    assert gh.commits == []


def test_verified_signed_tag(repo, tmp_path):
    """A signed tag that git verify-tag accepts is recorded as signed."""
    if git_version() < (2, 34):
        pytest.skip("SSH tag signing needs git 2.34 or newer")
    key = tmp_path / "key"
    subprocess.run(
        ["ssh-keygen", "-q", "-t", "ed25519", "-N", "", "-f", str(key)], check=True
    )
    allowed = tmp_path / "allowed_signers"
    allowed.write_text("test@example.com " + (tmp_path / "key.pub").read_text())
    run_git(repo, "config", "gpg.format", "ssh")
    run_git(repo, "config", "user.signingkey", str(key))
    run_git(repo, "config", "gpg.ssh.allowedSignersFile", str(allowed))
    run_git(repo, "tag", "-s", "-m", "release", "6.3.4")
    assert pm.tag_info(repo, "6.3.4") == (head(repo), True)
