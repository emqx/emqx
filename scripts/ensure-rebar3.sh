#!/usr/bin/env bash

set -euo pipefail

VERSION="$1"
REBAR3_FILENAME="${REBAR3_FILENAME:-rebar3}"

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

DOWNLOAD_URL='https://github.com/emqx/rebar3/releases/download'

download() {
    curl -f -L "${DOWNLOAD_URL}/${VERSION}/${REBAR3_FILENAME}" -o ./rebar3
}

version_gt() {
    test "$(echo "$@" | tr " " "n" | sort -V | head -n 1)" != "$1";
}

# get the version number from the second line of the escript
# because command `rebar3 -v` tries to load rebar.config
# which is slow and may print some logs
version() {
    head -n 2 ./rebar3 | tail -n 1 | tr ' ' '\n' | grep -E '^.+-emqx-.+'
}

if [ -f 'rebar3' ] && [ "$(version)" = "$VERSION" ]; then
    exit 0
fi

if version_gt "${OTP_VSN}" "24.0.0"; then
   echo "$OTP_VSN is greater than 24.0.0"
   REBAR3_FILENAME="rebar3_otp24.1.5"
fi

download
chmod +x ./rebar3
