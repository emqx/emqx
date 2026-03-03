#!/usr/bin/env bash
## This script packages all source-level dependencies (but not build
## artifacts) into a zip archive for Software Composition Analysis (SCA).
##
## Prerequisites:
##   The release must be built first (e.g. `make`) so that the release
##   lib directory exists and dependency hygiene checks can run.
##
## Usage:
##   scripts/sca-pkg.sh
##
## Output:
##   A zip file at ../emqx-<VSN>.zip containing the source tree with
##   build outputs, git metadata, CI scripts, and Python files excluded.
set -euo pipefail

cd -P -- "$(dirname -- "$0")/.."

PROFILE="${1:-emqx-enterprise}"
REL_LIB="_build/${PROFILE}/rel/emqx/lib"

if [ ! -d "$REL_LIB" ]; then
    echo "ERROR: Release lib directory not found: $REL_LIB" >&2
    echo "Has the release been built?" >&2
    exit 1
fi

VSN="$(./pkg-vsn.sh "$PROFILE")"

python3 scripts/sca-hyg.py --profile "$PROFILE"

ZIPFILE="$(pwd -P)/../emqx-${VSN}.zip"
rm -f "$ZIPFILE"

echo "Creating ${ZIPFILE} ..."
zip -ryq "$ZIPFILE" . -x './.git/*' './_build/*' './scripts/*' './.ci/*' './.github/*' '*.py'
echo "Done: $(du -h "$ZIPFILE" | cut -f1) ${ZIPFILE}"
