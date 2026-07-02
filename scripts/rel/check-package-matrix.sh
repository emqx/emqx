#!/usr/bin/env bash
set -euo pipefail

## Drift guard between the canonical package matrix (scripts/rel/build-matrix.sh,
## consumed by scripts/rel/print-download-links.sh) and the strategy matrices in
## .github/workflows/build_packages.yaml.
##
## The two lists live in different files (a shell script vs. GitHub Actions
## yaml, which the workflow cannot import from a script). This check fails CI if
## they diverge, so a platform added to the build workflow but not to the
## download-links matrix (or vice versa) is caught in code review.
##
## Requires: yq (mikefarah), jq. Both are available on GitHub-hosted runners and
## yq is already relied on by scripts/pr-sanity-checks.sh.

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

WORKFLOW='.github/workflows/build_packages.yaml'

if ! command -v yq >/dev/null 2>&1; then
    echo "ERROR: yq is required but not installed" 1>&2
    exit 1
fi

## Normalized "kind os arch" lines derived from the canonical matrix.
from_matrix() {
    local matrix
    matrix="$(./scripts/rel/build-matrix.sh)"
    echo "$matrix" | jq -r '.linux[] | "linux \(.os) \(.arch)"'
    echo "$matrix" | jq -r '.mac[]   | "mac \(.os) \(.arch)"'
    echo "$matrix" | jq -r '.snap[]  | "snap \(.arch)"'
}

## Same shape reconstructed from the build workflow's strategy matrices.
from_workflow() {
    local os arch inc
    ## linux: full os x arch product plus the include rows.
    while read -r os; do
        while read -r arch; do
            echo "linux $os $arch"
        done < <(yq -r '.jobs.linux.strategy.matrix.arch[]' "$WORKFLOW")
    done < <(yq -r '.jobs.linux.strategy.matrix.os[]' "$WORKFLOW")
    while read -r inc; do
        echo "linux $inc"
    done < <(yq -r '.jobs.linux.strategy.matrix.include[] | .os + " " + .arch' "$WORKFLOW")

    ## mac: the workflow uses GitHub runner labels (macos-14) whereas package
    ## filenames drop the dash (macos14); arch is not a matrix dimension there,
    ## both hosted runners are arm64 today.
    while read -r os; do
        echo "mac ${os//-/} arm64"
    done < <(yq -r '.jobs.mac.strategy.matrix.os[]' "$WORKFLOW")

    ## snap: one package per arch.
    while read -r arch; do
        echo "snap $arch"
    done < <(yq -r '.jobs.snap.strategy.matrix.arch[]' "$WORKFLOW")
}

canonical="$(from_matrix | sort)"
workflow="$(from_workflow | sort)"

if ! diff <(echo "$canonical") <(echo "$workflow") >/dev/null; then
    echo "ERROR: package matrix drift between scripts/rel/build-matrix.sh and $WORKFLOW" 1>&2
    echo "  '<' = only in build-matrix.sh, '>' = only in build_packages.yaml" 1>&2
    diff <(echo "$canonical") <(echo "$workflow") 1>&2 || true
    exit 1
fi

## Sanity check on the emitted URL count. Derived from the canonical matrix:
##   linux rows x 2 (pkg + tar.gz) + mac rows x 1 (zip) + snap rows x 1 (snap).
## Currently: 17 linux -> 34, 2 mac -> 2, 2 snap -> 2 = 38.
expected_urls=38
actual_urls="$(./scripts/rel/print-download-links.sh 0.0.0 | wc -l | tr -d ' ')"
if [ "$actual_urls" != "$expected_urls" ]; then
    echo "ERROR: print-download-links.sh emitted $actual_urls URLs, expected $expected_urls" 1>&2
    echo "  (update expected_urls in $0 if the matrix intentionally changed)" 1>&2
    exit 1
fi

echo "OK: package matrix in sync ($actual_urls download URLs)"
