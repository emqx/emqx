#!/usr/bin/env python3
"""Record published EMQX packages in emqx/emqx-package-manifests.

The manifests repository holds one JSON record per published artifact, at
manifests/<version>/<filename>.json. See its README for the record format and
for what a record proves.

This script reads a directory of published packages, as the release workflow
downloads them from S3 (.github/workflows/release.yaml), and builds one record
for each package:

  - Only top-level files named <profile>-<version>-*.{deb,rpm,tar.gz,zip} are
    recorded. Snap packages go to the Snap Store and are skipped. Sub-directories
    such as packages/plugins are not read.
  - The digest is read from the <file>.sha256 sidecar that the build wrote next
    to the package. The package itself is never hashed: a record must carry the
    digest computed at build time, not the digest of whatever the bucket holds
    now.
  - git_commit is the commit the release tag points to. git_tag_signed is false
    for a lightweight tag and for an annotated tag without a signature. A signed
    tag is recorded as true only when `git verify-tag` accepts it, and is an
    error otherwise.

Only GA versions (X.Y.Z) are accepted.

Records are only ever added. A record that already exists in the target branch
is left unchanged. When its content differs from the generated one, the
difference is reported as an error.

Without --publish the script is a dry run: it prints each record and, when the
manifests repository is reachable, whether the record would be added. With
--publish it adds the missing records in one commit through the GitHub GraphQL
API (createCommitOnBranch), authenticated with the GH_TOKEN environment
variable. The commit is made against the branch head it read. When the head has
moved, the script reads the branch again and retries.

The exit status is 0 when every package was recorded or already had an
identical record, 1 when any error was reported, and 2 on bad usage. Errors for
one package do not stop the other packages from being recorded.

Usage:
  package_manifests.py --packages-dir packages --version 6.3.1 --tag 6.3.1
                       [--profile emqx-enterprise] [--git-dir .]
                       [--out-dir DIR] [--publish] [--run-url URL]
                       [--repo emqx/emqx-package-manifests] [--branch master]
"""

import argparse
import base64
import hashlib
import json
import os
import re
import subprocess
import sys
import time
import urllib.error
import urllib.request
from pathlib import Path

PACKAGE_EXTS = (".deb", ".rpm", ".tar.gz", ".zip")
SKIPPED_EXTS = (".sha256", ".snap")
GA_VERSION_RE = re.compile(r"^[0-9]+\.[0-9]+\.[0-9]+$")
SHA256_RE = re.compile(r"^[0-9a-f]{64}$")
SIGNATURE_MARKERS = (
    "-----BEGIN PGP SIGNATURE-----",
    "-----BEGIN SSH SIGNATURE-----",
    "-----BEGIN SIGNED MESSAGE-----",
)
API_URL = "https://api.github.com"
PUBLISH_ATTEMPTS = 5


class Problems:
    """Collects errors, so that one bad package does not hide the others."""

    def __init__(self):
        self.errors = []

    def error(self, msg):
        self.errors.append(msg)
        log(f"ERROR: {msg}")
        if os.environ.get("GITHUB_ACTIONS") == "true":
            print(f"::error title=package manifests::{msg}", flush=True)


def log(msg):
    print(msg, file=sys.stderr, flush=True)


def git(git_dir, *args):
    return subprocess.run(
        ["git", "-C", str(git_dir), *args],
        capture_output=True,
        text=True,
    )


def tag_info(git_dir, tag):
    """Return (commit, signed) for a tag present in the local repository.

    Raises ValueError when the tag is missing, or when it carries a signature
    that `git verify-tag` does not accept.
    """
    ref = f"refs/tags/{tag}"
    kind = git(git_dir, "cat-file", "-t", ref)
    if kind.returncode != 0:
        raise ValueError(f"tag {tag} is not present in {git_dir}; fetch it first")
    commit = git(git_dir, "rev-parse", "--verify", f"{ref}^{{commit}}")
    if commit.returncode != 0:
        raise ValueError(f"tag {tag} does not point to a commit")
    commit = commit.stdout.strip()
    kind = kind.stdout.strip()
    if kind == "commit":
        # A lightweight tag has no tag object, so it cannot carry a signature.
        return commit, False
    if kind != "tag":
        raise ValueError(f"tag {tag} refers to a {kind}, not a commit")
    body = git(git_dir, "cat-file", "tag", ref).stdout
    if not any(marker in body for marker in SIGNATURE_MARKERS):
        return commit, False
    verify = git(git_dir, "verify-tag", ref)
    if verify.returncode != 0:
        raise ValueError(
            f"tag {tag} is signed but `git verify-tag` rejected it; "
            f"import the signer's key and run again: {verify.stderr.strip()}"
        )
    return commit, True


def read_sidecar(path):
    """Return the hex digest in a .sha256 sidecar, or raise ValueError."""
    try:
        text = path.read_text()
    except FileNotFoundError:
        raise ValueError(f"sidecar {path.name} is missing")
    digest = text.strip()
    if not SHA256_RE.match(digest):
        raise ValueError(f"sidecar {path.name} does not hold a sha256 digest")
    return digest


def find_packages(packages_dir, profile, version, problems):
    """Return the sorted package paths to record in packages_dir."""
    prefix = f"{profile}-{version}-"
    found = []
    for path in sorted(packages_dir.iterdir()):
        name = path.name
        if not path.is_file():
            continue
        if name.endswith(SKIPPED_EXTS):
            continue
        if name.startswith(prefix) and name.endswith(PACKAGE_EXTS):
            found.append(path)
        elif name.startswith(prefix):
            problems.error(f"{name} has an unknown package extension")
        else:
            log(f"skip: {name} is not a {profile} {version} package")
    return found


def build_record(path, version, tag, commit, signed):
    return {
        "filename": path.name,
        "version": version,
        "git_commit": commit,
        "git_tag": tag,
        "git_tag_signed": signed,
        "sha256": read_sidecar(path.with_name(path.name + ".sha256")),
        "size_bytes": path.stat().st_size,
    }


def encode_record(record):
    return (json.dumps(record, indent=2) + "\n").encode()


def record_path(version, filename):
    return f"manifests/{version}/{filename}.json"


def git_blob_sha(data):
    """The object id git gives a blob with this content."""
    return hashlib.sha1(b"blob %d\0" % len(data) + data).hexdigest()


class GitHub:
    def __init__(self, repo, token):
        self.repo = repo
        self.token = token

    def request(self, method, path, body=None):
        headers = {
            "Accept": "application/vnd.github+json",
            "X-GitHub-Api-Version": "2022-11-28",
        }
        if self.token:
            headers["Authorization"] = f"Bearer {self.token}"
        data = None
        if body is not None:
            data = json.dumps(body).encode()
            headers["Content-Type"] = "application/json"
        req = urllib.request.Request(
            API_URL + path, data=data, headers=headers, method=method
        )
        with urllib.request.urlopen(req, timeout=60) as resp:
            return json.load(resp)

    def head(self, branch):
        ref = self.request("GET", f"/repos/{self.repo}/git/ref/heads/{branch}")
        return ref["object"]["sha"]

    def existing_blobs(self, head, version):
        """Map each record path under manifests/<version>/ to its blob id."""
        path = f"manifests/{version}"
        try:
            entries = self.request(
                "GET", f"/repos/{self.repo}/contents/{path}?ref={head}"
            )
        except urllib.error.HTTPError as e:
            if e.code == 404:
                return {}
            raise
        return {e["path"]: e["sha"] for e in entries if e["type"] == "file"}

    def commit(self, branch, head, files, headline, body):
        query = """
        mutation($input: CreateCommitOnBranchInput!) {
          createCommitOnBranch(input: $input) { commit { oid url } }
        }
        """
        additions = [
            {"path": p, "contents": base64.b64encode(d).decode()}
            for p, d in sorted(files.items())
        ]
        variables = {
            "input": {
                "branch": {
                    "repositoryNameWithOwner": self.repo,
                    "branchName": branch,
                },
                "message": {"headline": headline, "body": body},
                "fileChanges": {"additions": additions},
                "expectedHeadOid": head,
            }
        }
        resp = self.request(
            "POST", "/graphql", {"query": query, "variables": variables}
        )
        if resp.get("errors"):
            raise RuntimeError(json.dumps(resp["errors"]))
        return resp["data"]["createCommitOnBranch"]["commit"]


def plan(records, existing):
    """Split records into (missing, conflicting) against existing blobs."""
    missing, conflicts = {}, []
    for path, data in records.items():
        blob = existing.get(path)
        if blob is None:
            missing[path] = data
            log(f"add: {path}")
        elif blob == git_blob_sha(data):
            log(f"exists: {path}")
        else:
            conflicts.append(path)
    return missing, conflicts


def report_conflicts(conflicts, problems):
    for path in conflicts:
        problems.error(
            f"{path} already exists with different content and was left "
            f"unchanged; compare the published package with both records"
        )


def publish(gh, branch, version, records, run_url, problems):
    body = f"Record published EMQX {version} artifacts."
    if run_url:
        body += f"\n\nRecorded by {run_url}"
    for attempt in range(1, PUBLISH_ATTEMPTS + 1):
        try:
            head = gh.head(branch)
            missing, conflicts = plan(records, gh.existing_blobs(head, version))
            if missing:
                commit = gh.commit(
                    branch, head, missing, f"chore: add manifests for {version}", body
                )
                log(f"committed {len(missing)} records: {commit['url']}")
            else:
                log(f"nothing to add on {branch} at {head}")
        except (RuntimeError, OSError) as e:
            # The usual cause is that the branch moved after it was read. A
            # commit that landed despite an error is found on the next read.
            log(f"attempt {attempt} failed: {e}")
            time.sleep(attempt * 5)
            continue
        report_conflicts(conflicts, problems)
        return
    problems.error(f"could not commit to {gh.repo} after {PUBLISH_ATTEMPTS} attempts")


def parse_args(argv):
    p = argparse.ArgumentParser(
        description="Record published EMQX packages in emqx-package-manifests."
    )
    p.add_argument("--packages-dir", required=True, type=Path)
    p.add_argument("--version", required=True)
    p.add_argument("--tag", required=True)
    p.add_argument("--profile", default="emqx-enterprise")
    p.add_argument(
        "--git-dir", default=Path("."), type=Path, help="repository holding the tag"
    )
    p.add_argument("--out-dir", type=Path, help="also write the records here")
    p.add_argument("--repo", default="emqx/emqx-package-manifests")
    p.add_argument("--branch", default="master")
    p.add_argument(
        "--publish", action="store_true", help="commit the missing records"
    )
    p.add_argument("--run-url", help="workflow run URL for the commit message")
    return p.parse_args(argv)


def main(argv=None):
    args = parse_args(argv)
    if not GA_VERSION_RE.match(args.version):
        log(f"ERROR: {args.version} is not a GA version (X.Y.Z)")
        return 2
    token = os.environ.get("GH_TOKEN")
    if args.publish and not token:
        log("ERROR: --publish needs the GH_TOKEN environment variable")
        return 2
    problems = Problems()
    try:
        commit, signed = tag_info(args.git_dir, args.tag)
    except ValueError as e:
        problems.error(str(e))
        return 1

    records = {}
    for path in find_packages(args.packages_dir, args.profile, args.version, problems):
        try:
            record = build_record(path, args.version, args.tag, commit, signed)
        except ValueError as e:
            problems.error(f"{path.name}: {e}")
            continue
        records[record_path(args.version, path.name)] = encode_record(record)
    if not records:
        problems.error(f"no {args.profile} {args.version} packages in {args.packages_dir}")

    for path, data in records.items():
        if args.out_dir:
            out = args.out_dir / path
            out.parent.mkdir(parents=True, exist_ok=True)
            out.write_bytes(data)
        if not args.publish:
            print(f"# {path}\n{data.decode()}", end="")

    gh = GitHub(args.repo, token)
    if args.publish and records:
        publish(gh, args.branch, args.version, records, args.run_url, problems)
    elif records:
        try:
            head = gh.head(args.branch)
            _, conflicts = plan(records, gh.existing_blobs(head, args.version))
            report_conflicts(conflicts, problems)
        except (urllib.error.URLError, OSError) as e:
            log(f"dry run: cannot read {args.repo}, not comparing: {e}")

    log(f"{len(records)} records, {len(problems.errors)} errors")
    return 1 if problems.errors else 0


if __name__ == "__main__":
    sys.exit(main())
