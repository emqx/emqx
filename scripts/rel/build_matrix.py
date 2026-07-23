#!/usr/bin/env python3
"""Canonical package build matrix (platform dimension only: os + arch).

This is the single source of truth for the platforms EMQX offers as emqx.com
downloads. It feeds two consumers:
  - .github/workflows/build_packages.yaml, whose linux and mac strategy
    matrices are generated from the ``--github`` output (a prepare-matrix job
    writes these arrays to $GITHUB_OUTPUT; the build jobs read them back via
    ``fromJSON(needs.prepare-matrix.outputs.*)``).
  - scripts/rel/print-download-links.py (emqx.com download URLs), which imports
    this module and calls matrix().

Snap is built by build_packages.yaml too, but it is published to the Snap Store
rather than emqx.com, so its (arch-only) matrix stays inline in that workflow.

Because the workflow generates its matrix from here, the two cannot drift.

Run directly with no arguments, it prints the expanded matrix as JSON (handy for
debugging). Run with ``--github`` it prints ``key=<json-array>`` lines for
$GITHUB_OUTPUT.

NOTE: the macOS os tokens differ between the two worlds. GitHub runner labels
(and therefore the workflow matrix) use ``macos-14``; package *filenames*
produced by scripts/get-distro.sh drop the dash (``macos14``). MAC_OS below
holds the runner labels; the download-links view strips the dash.
"""

import json
import sys

# Linux: full os x arch product. el7 ships amd64 only (see LINUX_EXCLUDE), so it
# lives in the os list rather than as a separate include row -- that keeps every
# generated matrix key pure os/arch data with no build-tool dimensions.
LINUX_OS = [
    "ubuntu24.04",
    "ubuntu22.04",
    "debian13",
    "debian12",
    "debian11",
    "el10",
    "el9",
    "el8",
    "amzn2023",
    "el7",
]
LINUX_ARCH = ["amd64", "arm64"]
# os/arch combinations that are NOT built.
LINUX_EXCLUDE = [{"os": "el7", "arch": "arm64"}]

# macOS: runner labels (workflow matrix). Both hosted runners are arm64 today.
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
    """Expanded platform matrix as a plain dict (package-filename view).

    Snap is intentionally absent: it is not offered as an emqx.com download
    (published to the Snap Store instead), so its arch list stays inline in
    build_packages.yaml rather than being owned here.
    """
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


def main(argv):
    if "--github" in argv:
        outputs = github_outputs()
        # A generated-but-empty array silently produces zero build jobs, so fail
        # loud here instead -- the prepare-matrix job goes red rather than the
        # build quietly shipping nothing.
        empty = [key for key, val in outputs.items() if not val]
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
