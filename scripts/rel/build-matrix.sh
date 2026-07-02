#!/usr/bin/env bash
set -euo pipefail

## Canonical package build matrix (platform dimension only: os + arch).
##
## This is the single source of truth for the list of platforms EMQX ships
## packages for. It is consumed by:
##   - scripts/rel/print-download-links.sh  (emqx.com download URLs)
##   - scripts/rel/check-package-matrix.sh  (drift-checked in CI against the
##     strategy matrices in .github/workflows/build_packages.yaml)
##
## Output: compact JSON on stdout, e.g.
##   {"linux":[{"os":"ubuntu24.04","arch":"amd64"},...],
##    "mac":[{"os":"macos14","arch":"arm64"},...],
##    "snap":[{"arch":"amd64"},{"arch":"arm64"}]}
##
## NOTE: the macOS os tokens here (macos14, macos15) are the package *filename*
## tokens produced by scripts/get-distro.sh. They intentionally differ from the
## GitHub runner labels (macos-14, macos-15) used in build_packages.yaml;
## check-package-matrix.sh accounts for this mapping.

# Linux: full os x arch product ...
linux_os=(ubuntu24.04 ubuntu22.04 debian13 debian12 debian11 el9 el8 amzn2023)
linux_arch=(amd64 arm64)
# ... plus these extra single-arch linux rows (os:arch).
linux_include=(el7:amd64)

# macOS: both GitHub-hosted runners are arm64 today.
mac_os=(macos14 macos15)
mac_arch=arm64

# Snap: one package per arch.
snap_arch=(amd64 arm64)

emit_rows() {
    # reads "os arch" lines on stdin, emits a JSON array of {os,arch}
    jq -R 'split(" ") | {os: .[0], arch: .[1]}' | jq -s -c .
}

linux_json="$(
    {
        for os in "${linux_os[@]}"; do
            for arch in "${linux_arch[@]}"; do
                echo "$os $arch"
            done
        done
        for row in "${linux_include[@]}"; do
            echo "${row%:*} ${row#*:}"
        done
    } | emit_rows
)"

mac_json="$(
    for os in "${mac_os[@]}"; do
        echo "$os $mac_arch"
    done | emit_rows
)"

snap_json="$(
    printf '%s\n' "${snap_arch[@]}" | jq -R '{arch: .}' | jq -s -c .
)"

jq -n -c \
    --argjson linux "$linux_json" \
    --argjson mac "$mac_json" \
    --argjson snap "$snap_json" \
    '{linux: $linux, mac: $mac, snap: $snap}'
