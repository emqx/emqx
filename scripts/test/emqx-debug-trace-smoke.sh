#!/usr/bin/env bash

## Regression test: running bin/emqx with DEBUG=1 or DEBUG=2 must not
## print the Erlang cookie in the shell trace output. The cookie is set
## both in the environment and via etc/emqx.env, so on releases that
## source the env file this also covers the file sourcing path (the
## file is ignored on releases that do not source it).
##
## Usage: emqx-debug-trace-smoke.sh <emqx-root-dir>
## The directory must hold an installed release (bin/, etc/, log/) with
## no node running from it.

set -euo pipefail

[ $# -ne 1 ] && { echo "Usage: $0 <emqx-root-dir>"; exit 1; }

ROOT="$1"
SECRET='debugtracesmokesecret'
ENV_FILE="$ROOT/etc/emqx.env"

cleanup() {
    rm -f "$ENV_FILE"
}
trap cleanup EXIT

echo "EMQX_NODE__COOKIE=$SECRET" > "$ENV_FILE"

## Drop logs from earlier runs so the erlang.log check below only sees
## output produced by this run.
rm -f "$ROOT"/log/erlang.log.*

assert_no_secret() {
    local phase="$1"
    local out="$2"
    if grep -qF "$SECRET" <<< "$out"; then
        echo "ERROR: cookie leaked in '$phase' output:"
        grep -nF "$SECRET" <<< "$out" | head -5
        exit 1
    fi
}

run_and_check() {
    local debug="$1"
    shift
    local out
    if ! out="$(env DEBUG="$debug" EMQX_NODE__COOKIE="$SECRET" "$ROOT/bin/emqx" "$@" 2>&1)"; then
        echo "$out"
        echo "ERROR: 'emqx $*' failed with DEBUG=$debug"
        exit 1
    fi
    assert_no_secret "DEBUG=$debug emqx $*" "$out"
}

run_and_check 2 start
run_and_check 2 ping
run_and_check 1 ctl status
run_and_check 2 stop

## The console process spawned by 'start' (via run_erl) writes its
## output, including its shell trace, to erlang.log.*.
if grep -qF "$SECRET" "$ROOT"/log/erlang.log.* 2>/dev/null; then
    echo "ERROR: cookie leaked in erlang.log"
    exit 1
fi

echo "emqx-debug-trace-smoke: OK"
