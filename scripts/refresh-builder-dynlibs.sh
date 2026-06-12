#!/usr/bin/env bash

# Refresh dynamic libraries (notably openssl) in the builder image before
# building a package. Builder images are not rebuilt on a schedule, so the
# libs they ship can be weeks-to-months stale by the time we build against
# them. Pull current security-patched versions from the distro repos so the
# package links/bundles fresh shared objects.
#
# Failure is non-fatal: if the distro repos are EOL (e.g. CentOS 7) or
# unreachable, we log a warning and proceed with the image as-is rather
# than blocking the build.
#
# Set REFRESH_DYNLIBS=no to skip (offline / sandboxed local builds).

set -uo pipefail

if [[ "${REFRESH_DYNLIBS:-yes}" != "yes" ]]; then
    echo "REFRESH_DYNLIBS=no; skipping in-builder dynlib refresh."
    exit 0
fi

if command -v apt-get >/dev/null 2>&1; then
    apt-get update -qq && DEBIAN_FRONTEND=noninteractive apt-get -y upgrade -qq \
        || echo "WARNING: apt upgrade failed; continuing with builder image's libs as-is"
elif command -v dnf >/dev/null 2>&1; then
    dnf -y upgrade -q \
        || echo "WARNING: dnf upgrade failed; continuing with builder image's libs as-is"
elif command -v yum >/dev/null 2>&1; then
    yum -y update -q \
        || echo "WARNING: yum update failed; continuing with builder image's libs as-is"
else
    echo "WARNING: no recognized package manager; cannot refresh dynlibs."
fi
