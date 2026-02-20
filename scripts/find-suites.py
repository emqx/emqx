#!/usr/bin/env python3

"""
For a given app, prints comma-separated list of suites
in the format expected by `mix emqx.ct`'s `--suite` option.

If suite(s) are provided explicitly with `EMQX_CT_SUITES` or `SUITES` variables,
the script just prints them with full paths (from ROOT).

Otherwise this script tries to find all `test/*_SUITE.erl` files
of then given app. If `SUITEGROUP` is set as `M_N`, it prints the Nth chunk of all suites.
`SUITEGROUP` default value is `1_1`.

The script also reads `test/slow-ct` file to group slow suites
independently for more even distribution.
"""

import hashlib
import os
import sys
from pathlib import Path

ROOT = Path(__file__).resolve().parent.parent


def main():
    if len(sys.argv) < 2:
        abort(f"Usage: {sys.argv[0]} <app_dir>")

    os.chdir(ROOT)
    app_dir = Path(sys.argv[1])

    suites = explicit_app_suites(app_dir)
    if not suites:
        slow_suites = find_app_slow_suites(app_dir)
        all_suites = find_app_suites(app_dir)
        regular_suites = [s for s in all_suites if s not in slow_suites]
        group_spec = os.environ.get("SUITEGROUP", "1_1")
        suites = select_group(regular_suites, group_spec) + select_group(slow_suites, group_spec)

    print(",".join(str(s) for s in suites))


def explicit_app_suites(app_dir: Path) -> list[Path]:
    raw = os.environ.get("EMQX_CT_SUITES") or os.environ.get("SUITES") or ""
    if not raw:
        return []

    paths = []
    for entry in raw.split(","):
        path = suite_path_from_root(app_dir, entry)
        if not path.is_file():
            print(f"Warning: '{path}' is not a file. Ignored!", file=sys.stderr)
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
    test_dir = app_dir / "test"
    if test_dir.is_dir():
        return sorted(test_dir.rglob("*_SUITE.erl"))
    return []


def find_app_slow_suites(app_dir: Path) -> list[Path]:
    slow_ct_file = app_dir / "test" / "slow-ct"
    if not slow_ct_file.is_file():
        return []
    suites = []
    for line in slow_ct_file.read_text().splitlines():
        line = line.strip()
        if not line or line.startswith("#"):
            continue
        suite_name = line
        path = suite_path_from_root(app_dir, suite_name)
        if not path.is_file():
            abort(f"Error: slow-ct lists '{suite_name}' but '{path}' does not exist")
        suites.append(path)
    return suites


def select_group(suites: list[Path], group_spec: str) -> list[Path]:
    group_str, count_str = group_spec.split("_", 1)
    group, count = int(group_str), int(count_str)

    if group > count:
        abort("Error: SUITEGROUP in the format of M_N, M must not be greater than N")

    per_group = (len(suites) + count - 1) // count
    start = (group - 1) * per_group
    return suites[start : start + per_group]

def abort(message: str):
    print(message, file=sys.stderr)
    sys.exit(1)

if __name__ == "__main__":
    main()
