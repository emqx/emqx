#!/usr/bin/env bash

set -euo pipefail

PROFILE="${PROFILE:-emqx}"
EMQX_ROOT="${EMQX_ROOT:-_build/$PROFILE/rel/emqx}"
EMQX_WAIT_FOR_START="${EMQX_WAIT_FOR_START:-30}"
export EMQX_WAIT_FOR_START

start_emqx_with_conf() {
    echo "Starting $PROFILE with $1"
    "$EMQX_ROOT"/bin/emqx start
    "$EMQX_ROOT"/bin/emqx stop
}

MINOR_VSN=$(./pkg-vsn.sh "$PROFILE" | cut -d. -f1,2)

if [ "$PROFILE" = "emqx" ]; then
  EDITION="ce"
else
  EDITION="ee"
fi

FILES=$(ls ./scripts/conf-test/old-confs/$EDITION-v"$MINOR_VSN"*)

cp "$EMQX_ROOT"/etc/emqx.conf "$EMQX_ROOT"/etc/emqx.conf.bak
cleanup() {
    cp "$EMQX_ROOT"/etc/emqx.conf.bak "$EMQX_ROOT"/etc/emqx.conf
}
trap cleanup EXIT

for file in $FILES; do
    cp "$file" "$EMQX_ROOT"/etc/emqx.conf
    start_emqx_with_conf "$file"
done
