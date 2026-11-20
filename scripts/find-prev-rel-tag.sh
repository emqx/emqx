#!/usr/bin/env bash
set -euo pipefail

root_dir="$(git rev-parse --show-toplevel)"
release="$(env -u PKG_VSN "$root_dir/pkg-vsn.sh" --release)"
if [[ ! "$release" =~ ^([0-9]+)\.([0-9]+)\. ]]; then
    echo "Invalid release version: $release" >&2
    exit 1
fi
minor="${BASH_REMATCH[1]}.${BASH_REMATCH[2]}"

if tag="$(git describe --abbrev=0 --tags --match "$minor.*" --exclude '*-*' 2>/dev/null)"; then
    echo "$tag"
else
    echo "No stable release tag found for $minor" >&2
    exit 2
fi
