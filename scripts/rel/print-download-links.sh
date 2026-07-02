#!/usr/bin/env bash
set -euo pipefail

## Print the emqx.com download URLs for every package produced for a release.
##
## Steering users to https://www.emqx.com/en/downloads/... (rather than the
## GitHub release assets) keeps download statistics visible.
##
## This script is pure enumeration: it never touches the network. It expands
## the canonical build matrix (scripts/rel/build-matrix.sh) into URLs, so it
## can run at any point in the release cycle without secrets.
##
## Usage:
##   print-download-links.sh <version>
##   print-download-links.sh --version <version> [--format text|markdown]
##                           [--profile emqx-enterprise]
##
## Examples:
##   print-download-links.sh 6.0.3
##   print-download-links.sh --version 6.0.3 --format markdown

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/../.."

usage() {
    echo "Usage: $0 <version> [--format text|markdown] [--profile <profile>]" 1>&2
}

VERSION=''
FORMAT='text'
PROFILE='emqx-enterprise'

while [ "$#" -gt 0 ]; do
    case "$1" in
        -h|--help)
            usage
            exit 0
            ;;
        --version)
            VERSION="$2"
            shift 2
            ;;
        --format)
            FORMAT="$2"
            shift 2
            ;;
        --profile)
            PROFILE="$2"
            shift 2
            ;;
        -*)
            echo "Unknown option: $1" 1>&2
            usage
            exit 1
            ;;
        *)
            if [ -z "$VERSION" ]; then
                VERSION="$1"
            else
                echo "Unexpected argument: $1" 1>&2
                usage
                exit 1
            fi
            shift
            ;;
    esac
done

if [ -z "$VERSION" ]; then
    echo "ERROR: version is required" 1>&2
    usage
    exit 1
fi

case "$FORMAT" in
    text|markdown) ;;
    *)
        echo "ERROR: unsupported --format '$FORMAT' (text|markdown)" 1>&2
        exit 1
        ;;
esac

case "$PROFILE" in
    emqx-enterprise)
        EDITION='enterprise'
        ;;
    *)
        echo "ERROR: unsupported --profile '$PROFILE' (emqx-enterprise)" 1>&2
        exit 1
        ;;
esac

BASE_URL="https://www.emqx.com/en/downloads/${EDITION}/${VERSION}"
MATRIX="$(./scripts/rel/build-matrix.sh)"

## Map a linux os token to its native package extension, mirroring the
## PKGERDIR logic in scripts/buildx.sh / build.
linux_pkg_ext() {
    case "$1" in
        ubuntu*|debian*|raspbian*) echo 'deb' ;;
        *) echo 'rpm' ;;
    esac
}

pkg_url() {
    # <os>-<arch>.<ext> style (linux/mac)
    echo "${BASE_URL}/${PROFILE}-${VERSION}-$1-$2.$3"
}

snap_url() {
    # <profile>_<vsn>_<arch>.snap (underscore separators)
    echo "${BASE_URL}/${PROFILE}_${VERSION}_$1.snap"
}

emit_text() {
    local os arch ext
    while read -r os arch; do
        ext="$(linux_pkg_ext "$os")"
        pkg_url "$os" "$arch" "$ext"
        pkg_url "$os" "$arch" 'tar.gz'
    done < <(echo "$MATRIX" | jq -r '.linux[] | "\(.os) \(.arch)"')

    while read -r os arch; do
        pkg_url "$os" "$arch" 'zip'
    done < <(echo "$MATRIX" | jq -r '.mac[] | "\(.os) \(.arch)"')

    while read -r arch; do
        snap_url "$arch"
    done < <(echo "$MATRIX" | jq -r '.snap[] | .arch')
}

md_link() {
    # <label> <url>
    echo "[$1]($2)"
}

## Emit a markdown bullet for one linux row: os label + amd64/arm64 links.
emit_markdown() {
    local ext
    echo "## Download"
    echo

    echo "### Ubuntu / Debian"
    while read -r os arch; do
        ext="$(linux_pkg_ext "$os")"
        echo "- \`$os\` ($arch): $(md_link ".$ext" "$(pkg_url "$os" "$arch" "$ext")") — $(md_link ".tar.gz" "$(pkg_url "$os" "$arch" 'tar.gz')")"
    done < <(echo "$MATRIX" | jq -r '.linux[] | select(.os|test("^(ubuntu|debian)")) | "\(.os) \(.arch)"')
    echo

    echo "### RHEL / Rocky / Amazon Linux"
    while read -r os arch; do
        ext="$(linux_pkg_ext "$os")"
        echo "- \`$os\` ($arch): $(md_link ".$ext" "$(pkg_url "$os" "$arch" "$ext")") — $(md_link ".tar.gz" "$(pkg_url "$os" "$arch" 'tar.gz')")"
    done < <(echo "$MATRIX" | jq -r '.linux[] | select(.os|test("^(el|amzn)")) | "\(.os) \(.arch)"')
    echo

    echo "### macOS"
    while read -r os arch; do
        echo "- \`$os\` ($arch): $(md_link ".zip" "$(pkg_url "$os" "$arch" 'zip')")"
    done < <(echo "$MATRIX" | jq -r '.mac[] | "\(.os) \(.arch)"')
    echo

    echo "### Snap"
    while read -r arch; do
        echo "- $arch: $(md_link ".snap" "$(snap_url "$arch")")"
    done < <(echo "$MATRIX" | jq -r '.snap[] | .arch')
}

case "$FORMAT" in
    text) emit_text ;;
    markdown) emit_markdown ;;
esac
