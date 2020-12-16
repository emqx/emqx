#!/bin/sh

#set -euo pipefail
set -eu

VERSION="$1"

# ensure dir
cd -P -- "$(dirname -- "$0")"

DOWNLOAD_URL='https://github.com/emqx/rebar3/releases/download'

download() {
    curl -f -L "${DOWNLOAD_URL}/${VERSION}/rebar3" -o ./rebar3
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

download
chmod +x ./rebar3
