#!/usr/bin/env python3
"""Fail when the tree holds a private key that is not on the allowlist.

A private key is a PEM private key block anywhere in a text file, inline in
source included, or a file with a keystore extension. Test suites generate
the keys they need at run time: `emqx_common_test_helpers:test_cert/1` and
`mock_server_certs/2` in suites, `scripts/gen-test-certs.sh` in scripts,
`scripts/ct/gen-compose-certs.sh` for the docker-compose services. The
allowlist names the few fixtures that cannot be generated, each with the
reason. An allowlisted file that no longer holds a key fails the check too,
so the list does not outlive what it describes.
"""

import os
import re
import subprocess
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent
SCRIPT = "scripts/check-private-keys.py"
ALLOWLIST = "scripts/private-keys-allowlist.txt"
PEM_MARKER = r"-----BEGIN ([A-Z0-9]+ )*PRIVATE KEY( BLOCK)?-----"
KEYSTORE_EXT = re.compile(r"\.(jks|p12|pfx|keystore|jceks|bks|ppk)$", re.IGNORECASE)


def git_paths(*args, ok_codes=(0,)):
    """Run git in the repository root and return the NUL-separated paths it prints."""
    proc = subprocess.run(["git", *args], cwd=ROOT, stdout=subprocess.PIPE)
    if proc.returncode not in ok_codes:
        raise subprocess.CalledProcessError(proc.returncode, proc.args)
    return {os.fsdecode(p) for p in proc.stdout.split(b"\0") if p}


def found_keys():
    # `git grep` exits with 1 when nothing matches.
    pem = git_paths("grep", "-l", "-z", "-I", "-E", "-e", PEM_MARKER, ok_codes=(0, 1))
    keystores = {p for p in git_paths("ls-files", "-z") if KEYSTORE_EXT.search(p)}
    return pem | keystores


def allowed_keys():
    allowed = set()
    with open(ROOT / ALLOWLIST, encoding="utf-8") as f:
        for line in f:
            entry = line.split("#", 1)[0].rstrip()
            if entry:
                allowed.add(entry)
    return allowed


def print_paths(header, paths):
    print(header)
    for path in sorted(paths):
        print(f"    {path}")


def main():
    found = found_keys()
    allowed = allowed_keys()
    new = found - allowed
    stale = allowed - found
    if new:
        print_paths(
            f"These files hold a private key and are not on the allowlist ({ALLOWLIST}):", new
        )
        print(f"Generate the key at run time instead (see the header of {SCRIPT}).")
        print(f"If the file must be committed, add it to {ALLOWLIST} with the reason.")
    if stale:
        print_paths(
            "These files are on the allowlist but hold no private key;"
            f" remove them from {ALLOWLIST}:",
            stale,
        )
    return 1 if new or stale else 0


if __name__ == "__main__":
    sys.exit(main())
