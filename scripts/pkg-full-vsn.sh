#!/usr/bin/env bash

## This script print the package full vsn based on current build environment

## Arg 1 is either 'vsn_exact' (default) or 'vsn_matcher'
## when 'vsn_exact' is given, the version number is the output of pkg-vsn.sh
## otherwise '*' is used for 'find' command to find old versions (as upgrade base)

set -euo pipefail

VSN_MATCH="${1:-vsn_exact}"

case "${VSN_MATCH}" in
    vsn_exact)
        PKG_VSN="${PKG_VSN:-$(./pkg-vsn.sh)}"
        ;;
    vsn_matcher)
        PKG_VSN='*'
        ;;
    *)
        echo "$0 ERROR: second arg must be either 'vsn_exact' or 'vsn_matcher'"
        exit 1
        ;;
esac

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

OTP_VSN="${OTP_VSN:-$(./scripts/get-otp-vsn.sh)}"
SYSTEM="${SYSTEM:-$(./scripts/get-distro.sh)}"

UNAME="$(uname -m)"
case "$UNAME" in
    x86_64)
        ARCH='amd64'
        ;;
    aarch64)
        ARCH='arm64'
        ;;
    arm*)
        ARCH=arm
        ;;
esac

echo "${PKG_VSN}-otp${OTP_VSN}-${SYSTEM}-${ARCH}"
