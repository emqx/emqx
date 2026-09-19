#!/usr/bin/env python3
"""Package build matrix (platform dimension only: os + arch).

This is the single source of truth for the platforms EMQX offers as emqx.com
downloads. It feeds two consumers:
  - .github/workflows/build_packages.yaml, whose linux and mac strategy
    matrices are generated from the ``--github`` output (the prepare-matrix job
    writes these arrays to $GITHUB_OUTPUT; the build jobs read them back via
    ``fromJSON(needs.prepare-matrix.outputs.*)``).
  - scripts/rel/print-download-links.py (download URLs), which imports this
    module and calls matrix().

Because the workflow generates its matrix from here, the two cannot drift.
The build-tool dimensions (otp, builder, elixir, with_elixir) and the single
Elixir build row stay inline in build_packages.yaml; the Elixir build is not an
offered download.

The platform list differs between release lines. When this file is merged
forward (dev-58 -> dev-59 -> dev-510), set the lists to the platforms that line
builds.

Run directly with no arguments, it prints the expanded matrix as JSON (handy
for debugging). Run with ``--github`` it prints ``key=<json-array>`` lines for
$GITHUB_OUTPUT.

NOTE: the macOS os tokens differ between the two worlds. GitHub runner labels
(and therefore the workflow matrix) use ``macos-14``; package file names
produced by scripts/get-distro.sh drop the dash (``macos14``). MAC_OS below
holds the runner labels; the download-links view strips the dash.
"""

import json
import sys

# Linux: full os x arch product, minus LINUX_EXCLUDE.
LINUX_OS = [
    "ubuntu24.04",
    "ubuntu22.04",
    "ubuntu20.04",
    "debian13",
    "debian12",
    "debian11",
    "el10",
    "el9",
    "el8",
    "amzn2",
    "amzn2023",
]
LINUX_ARCH = ["amd64", "arm64"]
# os/arch combinations that are NOT built. May be empty.
LINUX_EXCLUDE = []

# macOS: runner labels (workflow matrix). The hosted runners are arm64.
MAC_OS = ["macos-14", "macos-15"]
MAC_ARCH = "arm64"


def linux_rows():
    """Expanded linux os/arch combinations, excludes applied."""
    return [
        {"os": os, "arch": arch}
        for os in LINUX_OS
        for arch in LINUX_ARCH
        if {"os": os, "arch": arch} not in LINUX_EXCLUDE
    ]


def mac_rows():
    """macOS rows in package-filename form (macos14, dash stripped)."""
    return [{"os": os.replace("-", ""), "arch": MAC_ARCH} for os in MAC_OS]


def matrix():
    """Expanded platform matrix as a plain dict (package-filename view)."""
    return {
        "linux": linux_rows(),
        "mac": mac_rows(),
    }


def github_outputs():
    """Arrays consumed by build_packages.yaml matrices via fromJSON."""
    return {
        "linux_os": LINUX_OS,
        "linux_arch": LINUX_ARCH,
        "linux_exclude": LINUX_EXCLUDE,
        "mac_os": MAC_OS,
    }


# An empty os or arch array silently produces zero build jobs. The exclude
# list may be empty.
MUST_NOT_BE_EMPTY = ["linux_os", "linux_arch", "mac_os"]


def main(argv):
    if "--github" in argv:
        outputs = github_outputs()
        empty = [key for key in MUST_NOT_BE_EMPTY if not outputs[key]]
        if empty:
            print(f"ERROR: empty matrix arrays: {empty}", file=sys.stderr)
            return 1
        for key, val in outputs.items():
            sys.stdout.write(f"{key}={json.dumps(val, separators=(',', ':'))}\n")
    else:
        json.dump(matrix(), sys.stdout, separators=(", ", ": "))
        sys.stdout.write("\n")
    return 0


if __name__ == "__main__":
    sys.exit(main(sys.argv[1:]))
