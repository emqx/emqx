#!/usr/bin/env bash
## Print the default QUICER_TLS_VER for this host.
##
## quicer 0.4+ (msquic 2.5+) requires OpenSSL >= 3.0 to link the system
## libcrypto (QUICER_TLS_VER=sys). Print 'sys' when the host has OpenSSL >= 3.0
## development files, 'quictls' (quicer's bundled TLS) otherwise.
##
## Probe pkg-config first: msquic selects the system OpenSSL with CMake
## find_package(OpenSSL), which resolves the development package, not the CLI.
## Fall back to the CLI version when pkg-config cannot resolve libcrypto
## (the emqx-builder images have no pkg-config; there the CLI and the
## development headers come from the same distro package).

set -euo pipefail

if command -v pkg-config >/dev/null 2>&1 && pkg-config --exists libcrypto 2>/dev/null; then
    if pkg-config --atleast-version=3 libcrypto; then
        echo 'sys'
    else
        echo 'quictls'
    fi
elif openssl version 2>/dev/null | grep -q '^OpenSSL 3\.'; then
    echo 'sys'
else
    echo 'quictls'
fi
