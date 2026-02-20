#!/usr/bin/env python3

"""
For a given app, prints comma-separated list of suites
in the format expected by `mix emqx.ct`'s `--suite` option.

If suite(s) are provided explicitly with `EMQX_CT_SUITES` or `SUITES` variables,
the script just prints them with full paths (from ROOT).

Otherwise this script tries to find all `{test,integration_test}/*_SUITE.erl` files
of then given app. If `SUITEGROUP` is set as `M_N`, it prints the Nth chunk of all suites.
`SUITEGROUP` default value is `1_1`.
"""

import hashlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    if len(sys.argv) < 2:
        print(f"Usage: {sys.argv[0]} <app_dir>", file=sys.stderr)
        sys.exit(1)

    os.chdir(ROOT)
    app_dir = Path(sys.argv[1])

    files = explicit_app_suites(app_dir)
    if not files:
        files = find_app_suites(app_dir)
        files = select_group(files, os.environ.get("SUITEGROUP", "1_1"))

    print(",".join(str(f) for f in files))


def explicit_app_suites(app_dir: Path) -> list[Path]:
    raw = os.environ.get("EMQX_CT_SUITES") or os.environ.get("SUITES") or ""
    if not raw:
        return []

    paths = []
    for entry in raw.split(","):
        path = suite_path_from_root(app_dir, entry)
        if not path.is_file():
            print(f"\nERROR: '{path}' is not a file. Ignored!", file=sys.stderr)
            sys.exit(1)
        paths.append(path)
    return paths


def suite_path_from_root(app_dir: Path, filename: str) -> Path:
    p = Path(filename.strip())
    if "/" not in filename:
        p = app_dir / "test" / filename
    if p.suffix != ".erl":
        p = p.with_suffix(p.suffix + ".erl")
    return p


def find_app_suites(app_dir: Path) -> list[Path]:
    dirs = [app_dir / "test", app_dir / "integration_test"]
    files = []
    for d in dirs:
        if d.is_dir():
            files.extend(sorted(d.rglob("*_SUITE.erl")))
    return files


def select_group(files: list[Path], group_spec: str) -> list[Path]:
    group_str, count_str = group_spec.split("_", 1)
    group, count = int(group_str), int(count_str)

    if group > count:
        print("Error: SUITEGROUP in the format of M_N, M must not be greater than N", file=sys.stderr)
        sys.exit(1)

    # Shuffle the files in a deterministic way
    shuffled = sorted(files, key=lambda p: hashlib.md5(str(p).encode()).digest())

    per_group = (len(shuffled) + count - 1) // count
    start = (group - 1) * per_group
    return shuffled[start : start + per_group]


if __name__ == "__main__":
    main()
