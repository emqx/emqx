#!/usr/bin/env bash

## Regression test: running bin/emqx with DEBUG=1 or DEBUG=2 must not
## print the Erlang cookie in the shell trace output. The cookie is set
## in the environment, appended to releases/emqx_vars (the persisted
## override location the file itself suggests for EMQX_NODE_COOKIE),
## and written to etc/emqx.env, so on releases that source the env file
## this also covers that path (the file is ignored on releases that do
## not source it).
##
## Usage: emqx-debug-trace-smoke.sh <emqx-root-dir>
## The directory must hold an installed release (bin/, etc/, log/) with
## no node running from it. Pre-existing etc/emqx.env, emqx_vars, and
## erlang.log.* files are restored when the test finishes.

set -euo pipefail

[ $# -ne 1 ] && { echo "Usage: $0 <emqx-root-dir>"; exit 1; }

ROOT="$1"
SECRET='debugtracesmokesecret'
ENV_FILE="$ROOT/etc/emqx.env"
VARS_FILE="$ROOT/releases/emqx_vars"

# shellcheck disable=SC2009 # match the node by its '-root <dir>' argument, like bin/emqx does
if ps -efww | grep -qE "[-]root ${ROOT}( |$)"; then
    echo "ERROR: a node is already running from $ROOT"
    exit 1
fi

BACKUP_DIR="$(mktemp -d)"
mkdir -p "$BACKUP_DIR/logs"
TEST_OK=0

cleanup() {
    ## Stop the node in case an assertion failed while it was running.
    env DEBUG=0 EMQX_NODE__COOKIE="$SECRET" "$ROOT/bin/emqx" stop >/dev/null 2>&1 || true
    ## Restore the pre-existing configuration.
    if [ -f "$BACKUP_DIR/emqx.env" ]; then
        cp "$BACKUP_DIR/emqx.env" "$ENV_FILE"
    else
        rm -f "$ENV_FILE"
    fi
    cp "$BACKUP_DIR/emqx_vars" "$VARS_FILE"
    ## Restore the pre-existing logs. Keep this run's logs on failure,
    ## moved aside so they cannot collide with the restored ones.
    if [ "$TEST_OK" = 1 ]; then
        rm -f "$ROOT"/log/erlang.log.*
    else
        local failed_dir="$ROOT/log/debug-trace-smoke-failed.$$"
        mkdir -p "$failed_dir"
        mv "$ROOT"/log/erlang.log.* "$failed_dir/" 2>/dev/null || true
        echo "logs from the failed run are kept in $failed_dir"
    fi
    mv "$BACKUP_DIR"/logs/erlang.log.* "$ROOT/log/" 2>/dev/null || true
    rm -rf "$BACKUP_DIR"
}
trap cleanup EXIT

## Back up the files this test modifies, then plant the secret in every
## persisted location an operator may use for the cookie.
[ -f "$ENV_FILE" ] && cp "$ENV_FILE" "$BACKUP_DIR/emqx.env"
cp "$VARS_FILE" "$BACKUP_DIR/emqx_vars"
echo "EMQX_NODE__COOKIE=$SECRET" > "$ENV_FILE"
echo "EMQX_NODE_COOKIE=\"$SECRET\"" >> "$VARS_FILE"

## Move logs from earlier runs aside so the erlang.log check below only
## sees output produced by this run.
mv "$ROOT"/log/erlang.log.* "$BACKUP_DIR/logs/" 2>/dev/null || true

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

TEST_OK=1
echo "emqx-debug-trace-smoke: OK"
