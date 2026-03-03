#!/usr/bin/env python3

"""Delete build-only dependencies from deps/.

Production dependencies are those found in
_build/emqx-enterprise/rel/emqx/lib/ (named as <app>-<version>).
Any directory in deps/ that is NOT present in the production release is
considered build-only and will be removed.
"""

import argparse
import os
import re
import shutil
import sys


def main():
    parser = argparse.ArgumentParser(
        description="Remove build-only deps not needed in production."
    )
    parser.add_argument(
        "--dry-run",
        action="store_true",
        help="Only print what would be deleted, do not actually delete.",
    )
    parser.add_argument(
        "--profile",
        default="emqx-enterprise",
        help="Build profile name (default: emqx-enterprise).",
    )
    args = parser.parse_args()

    root = os.path.dirname(os.path.dirname(os.path.abspath(__file__)))
    deps_dir = os.path.join(root, "deps")
    lib_dir = os.path.join(root, "_build", args.profile, "rel", "emqx", "lib")

    if not os.path.isdir(deps_dir):
        sys.exit(f"deps directory not found: {deps_dir}")
    if not os.path.isdir(lib_dir):
        sys.exit(f"lib directory not found: {lib_dir}")

    # Release lib names are like "foo-1.2.3", "foo-1.2.3-emqx2", or "bson-v0.2.2";
    # strip version by splitting on the first "-" followed by a digit or "v" + digit.
    production_libs = set(
        re.split(r"-(?=v?\d)", name, maxsplit=1)[0] for name in os.listdir(lib_dir)
    )
    all_deps = set(os.listdir(deps_dir))

    build_only = sorted(all_deps - production_libs)

    if build_only:
        for dep in build_only:
            dep_path = os.path.join(deps_dir, dep)
            if args.dry_run:
                print(f"[dry-run] would delete: {dep_path}")
            else:
                print(f"Deleting: {dep_path}")
                shutil.rmtree(dep_path)
        print(f"{'Would delete' if args.dry_run else 'Deleted'} {len(build_only)} build-only dep(s).")
    else:
        print("No build-only deps found.")

    # Remove .git directories from remaining deps to reduce zip size.
    git_dirs = []
    for dep in sorted(all_deps - set(build_only)):
        git_dir = os.path.join(deps_dir, dep, ".git")
        if os.path.exists(git_dir):
            git_dirs.append(git_dir)

    for git_dir in git_dirs:
        if args.dry_run:
            print(f"[dry-run] would delete: {git_dir}")
        else:
            print(f"Deleting: {git_dir}")
            if os.path.isdir(git_dir):
                shutil.rmtree(git_dir)
            else:
                # .git can be a file (gitlink) in worktrees/submodules
                os.remove(git_dir)

    if git_dirs:
        print(f"{'Would delete' if args.dry_run else 'Deleted'} {len(git_dirs)} .git dir(s) from deps.")

    # Remove bulky directories not needed for SCA.
    bulky_dirs = [
        os.path.join(deps_dir, "greptimedb_rs", "target", "release"),
        os.path.join(deps_dir, "datalayers", "target", "release"),
    ]
    # Find all selenium dirs recursively under deps.
    for dirpath, dirnames, _filenames in os.walk(deps_dir):
        if "selenium" in dirnames:
            bulky_dirs.append(os.path.join(dirpath, "selenium"))
    for d in bulky_dirs:
        if os.path.isdir(d):
            if args.dry_run:
                print(f"[dry-run] would delete: {d}")
            else:
                print(f"Deleting: {d}")
                shutil.rmtree(d)


    # Delete orphan Cargo.toml files (no Cargo.lock sibling) to avoid scanner
    # complaining about missing lockfiles.
    orphan_cargo = []
    for dirpath, _dirnames, filenames in os.walk(deps_dir):
        if "Cargo.toml" in filenames and "Cargo.lock" not in filenames:
            orphan_cargo.append(os.path.join(dirpath, "Cargo.toml"))

    for fpath in sorted(orphan_cargo):
        if args.dry_run:
            print(f"[dry-run] would delete: {fpath}")
        else:
            print(f"Deleting: {fpath}")
            os.remove(fpath)

    if orphan_cargo:
        print(f"{'Would delete' if args.dry_run else 'Deleted'} {len(orphan_cargo)} orphan Cargo.toml file(s) from deps.")

    # Delete files with a Python shebang to avoid scanner treating this as a Python project.
    python_shebang = re.compile(rb'^#!.*python')
    python_files = []
    for dirpath, _dirnames, filenames in os.walk(deps_dir):
        for f in filenames:
            fpath = os.path.join(dirpath, f)
            try:
                with open(fpath, "rb") as fh:
                    first_line = fh.readline(256)
                if python_shebang.match(first_line):
                    python_files.append(fpath)
            except (OSError, IsADirectoryError):
                pass

    for fpath in sorted(python_files):
        if args.dry_run:
            print(f"[dry-run] would delete: {fpath}")
        else:
            print(f"Deleting: {fpath}")
            os.remove(fpath)

    if python_files:
        print(f"{'Would delete' if args.dry_run else 'Deleted'} {len(python_files)} Python-shebang file(s) from deps.")


if __name__ == "__main__":
    main()
