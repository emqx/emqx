#!/usr/bin/env bash

set -euo pipefail

VERSION="3.14.3-emqx-8"

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

DOWNLOAD_URL='https://github.com/emqx/rebar3/releases/download'

download() {
    echo "downloading rebar3 ${VERSION}"
    curl -f -L "${DOWNLOAD_URL}/${VERSION}/rebar3" -o ./rebar3
}

version_gte() {
    test "$(printf '%s\n' "$1" "$2" | sort -V | head -n 1)" = "$2"
}

# get the version number from the second line of the escript
# because command `rebar3 -v` tries to load rebar.config
# which is slow and may print some logs
version() {
    head -n 2 ./rebar3 | tail -n 1 | tr ' ' '\n' | grep -E '^.+-emqx-.+'
}

echo "OTP_VSN: ${OTP_VSN}"
if version_gte "${OTP_VSN}" "24.0"; then
    ## rebar3 tag 3.18.0-emqx-1 is compiled using otp24.1.5.
    ## we have to use an otp24-compiled rebar3 because the defination of record #application{}
    ## in systools.hrl is changed in otp24.
    VERSION="3.18.0-emqx-1"
fi

if [ -f 'rebar3' ] && [ "$(version)" = "$VERSION" ]; then
    echo "rebar3 ${VERSION} already exists"
    exit 0
fi

download
chmod +x ./rebar3
