#!/usr/bin/env bash

# Acceptance test for `emqx ctl conf show` output modes against a built release.
#
# It starts a real node, checks the default (redacted) and `--no-secret-redaction`
# outputs for the full config, a single root and a namespace, verifies that the
# successful stdout is parseable HOCON without ANSI escapes, and exercises the
# raw export -> edit -> `conf load --replace` -> read back round trip for a
# literal secret and for a `file://` source. The last phase restarts a node from
# the raw full export with a clean data directory.
#
# Usage:
#   PROFILE=emqx-enterprise scripts/test/conf-show-round-trip.sh
#   EMQX_ROOT=/path/to/rel/emqx scripts/test/conf-show-round-trip.sh
#   WORK_DIR=/some/dir PROFILE=emqx-enterprise scripts/test/conf-show-round-trip.sh
#
# WORK_DIR, when given, is only a parent directory: the script creates its own
# run directory below it and never removes anything it did not create.

set -euo pipefail

cd -P -- "$(dirname -- "$0")/../.."
# shellcheck disable=SC1091
source ./env.sh

PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_ROOT="${EMQX_ROOT:-_build/$PROFILE/rel/emqx}"
EMQX="$EMQX_ROOT/bin/emqx"
EMQX_WAIT_FOR_START="${EMQX_WAIT_FOR_START:-60}"
export EMQX_WAIT_FOR_START

WORK_ROOT="${WORK_DIR:-${TMPDIR:-/tmp}}"
CONF="$EMQX_ROOT/etc/emqx.conf"
COOKIE_VALUE="confshowacceptancecookie"
SECRET_VALUE="conf-show-acceptance-secret"
CHANGED_VALUE="conf-show-acceptance-changed"
NS="conf_show_acceptance_ns"
CONNECTOR="conf_show_acceptance"
NS_HTTP_CONNECTOR="${CONNECTOR}_http"
NS_AUTHORIZATION="Bearer conf-show-acceptance-ns-authorization"

fail() {
    echo "FAIL: $*" >&2
    exit 1
}

pass() {
    echo "ok: $*"
}

[ -x "$EMQX" ] || fail "release not found at $EMQX_ROOT (build it first)"
case "$WORK_ROOT" in
    *'"'* | *\\*)
        fail "WORK_DIR must not contain quotes or backslashes: $WORK_ROOT"
        ;;
esac
mkdir -p "$WORK_ROOT"
RUN_DIR="$(mktemp -d "$WORK_ROOT/conf-show-XXXXXX")"
case "$RUN_DIR" in
    "$WORK_ROOT"/conf-show-*) ;;
    *)
        fail "unexpected run directory: $RUN_DIR"
        ;;
esac
CONF_BAK="$RUN_DIR/emqx.conf.release"
COOKIE_FILE="$RUN_DIR/cookie-secret"
SECRET_FILE="$RUN_DIR/connector-password"

cleanup() {
    local rc=$?
    "$EMQX" stop >/dev/null 2>&1 || true
    if [ -f "$CONF_BAK" ]; then
        cp "$CONF_BAK" "$CONF"
    fi
    if [ "$rc" -eq 0 ]; then
        rm -rf "$RUN_DIR"
    else
        echo "run dir kept for diagnosis: $RUN_DIR"
    fi
}
trap cleanup EXIT

node_start() {
    local data_dir="$1"
    export EMQX_NODE__DATA_DIR="$data_dir"
    "$EMQX" start
    for _ in $(seq 1 "$EMQX_WAIT_FOR_START"); do
        if "$EMQX" ctl status >/dev/null 2>&1; then
            pass "node started with data dir $data_dir"
            return 0
        fi
        sleep 1
    done
    fail "node did not start; see $data_dir/log"
}

node_stop() {
    "$EMQX" stop >/dev/null 2>&1 || true
    for _ in $(seq 1 30); do
        if ! "$EMQX" ctl status >/dev/null 2>&1; then
            return 0
        fi
        sleep 1
    done
    fail "node did not stop"
}

# Capture stdout/stderr/exit code of `conf show`, then assert the output contract.
#   check_show <label> <redacted|raw> [args...]
check_show() {
    local label="$1"
    local mode="$2"
    shift 2
    local out="$RUN_DIR/$label.out"
    local err="$RUN_DIR/$label.err"
    local rc=0
    "$EMQX" ctl conf show "$@" >"$out" 2>"$err" || rc=$?
    if [ "$rc" -ne 0 ]; then
        fail "$label: exit code $rc: $(cat "$err")"
    fi
    if [ -s "$err" ]; then
        fail "$label: stderr is not empty: $(cat "$err")"
    fi
    if LC_ALL=C grep -q $'\x1b' "$out"; then
        fail "$label: ANSI escape in stdout"
    fi
    hocon_parse "$label" "$out"
    local first_line
    local has_comment=no
    first_line="$(head -n 1 "$out")"
    if [ "${first_line#\#}" != "$first_line" ]; then
        has_comment=yes
    fi
    if [ "$mode" = redacted ]; then
        if [ "$has_comment" != yes ]; then
            fail "$label: expected a leading HOCON comment"
        fi
        if ! grep -q '\*\*\*\*\*\*' "$out"; then
            fail "$label: no redacted value in output"
        fi
    else
        if [ "$has_comment" = yes ]; then
            fail "$label: unexpected notice in raw output"
        fi
    fi
    pass "$label"
}

# Parse a captured document with the release's own HOCON parser.
hocon_parse() {
    local label="$1"
    local path="$2"
    local result
    result="$("$EMQX" eval "{ok, Bin} = file:read_file(\"$path\"), case hocon:binary(Bin) of {ok, _} -> parse_ok; Err -> {parse_error, Err} end.")"
    if ! echo "$result" | grep -q 'parse_ok'; then
        fail "$label: not parseable HOCON: $result"
    fi
}

assert_contains() {
    local label="$1"
    local path="$2"
    local needle="$3"
    if ! grep -qF "$needle" "$path"; then
        fail "$label: $path does not contain '$needle'"
    fi
}

assert_absent() {
    local label="$1"
    local path="$2"
    local needle="$3"
    if grep -qF "$needle" "$path"; then
        fail "$label: $path leaks '$needle'"
    fi
}

conf_load() {
    if ! "$EMQX" ctl conf load "$@"; then
        fail "conf load $* failed"
    fi
}

show_raw() {
    "$EMQX" ctl conf show --no-secret-redaction "$@"
}

cp "$CONF" "$CONF_BAK"
printf '%s' "$COOKIE_VALUE" >"$COOKIE_FILE"
printf '%s' "$SECRET_VALUE" >"$SECRET_FILE"
chmod 600 "$COOKIE_FILE" "$SECRET_FILE"

# Phase 1: a writable root with a literal secret and a `file://` secret source.
cat >"$RUN_DIR/connectors.hocon" <<EOF
connectors.mqtt.$CONNECTOR {
  server = "127.0.0.1:1"
  username = "conf-show-user"
  password = "file://$SECRET_FILE"
  enable = false
}
EOF

sed "s|^  cookie = .*|  cookie = \"file://$COOKIE_FILE\"|" "$CONF" >"$RUN_DIR/emqx.conf.patched"
cp "$RUN_DIR/emqx.conf.patched" "$CONF"

node_start "$RUN_DIR/data1"

conf_load --replace "$RUN_DIR/connectors.hocon"
pass "connector fixture loaded"

check_show full-redacted redacted
check_show full-raw raw --no-secret-redaction
check_show keyed-redacted redacted connectors
check_show keyed-raw raw --no-secret-redaction connectors
check_show node-redacted redacted node
check_show node-raw raw --no-secret-redaction node

assert_contains keyed-redacted "$RUN_DIR/keyed-redacted.out" '******'
assert_absent keyed-redacted "$RUN_DIR/keyed-redacted.out" "$SECRET_VALUE"
assert_absent keyed-redacted "$RUN_DIR/keyed-redacted.out" "$SECRET_FILE"
assert_contains keyed-raw "$RUN_DIR/keyed-raw.out" "file://$SECRET_FILE"
assert_absent keyed-raw "$RUN_DIR/keyed-raw.out" "$SECRET_VALUE"
assert_contains keyed-raw "$RUN_DIR/keyed-raw.out" "conf-show-user"
assert_absent full-redacted "$RUN_DIR/full-redacted.out" "$SECRET_VALUE"
assert_absent full-redacted "$RUN_DIR/full-redacted.out" "$SECRET_FILE"
assert_absent full-redacted "$RUN_DIR/full-redacted.out" "$COOKIE_VALUE"
assert_contains full-raw "$RUN_DIR/full-raw.out" "file://$SECRET_FILE"
assert_contains full-raw "$RUN_DIR/full-raw.out" "$COOKIE_VALUE"
assert_contains node-redacted "$RUN_DIR/node-redacted.out" '******'
assert_absent node-redacted "$RUN_DIR/node-redacted.out" "$COOKIE_VALUE"
# `bin/emqx` resolves the `file://` cookie source before the config is loaded,
# so the raw output carries the file content.
assert_contains node-raw "$RUN_DIR/node-raw.out" "$COOKIE_VALUE"
pass "redaction contract for the full, keyed and schema-only values"

# Namespaced output: same flags, scoped to one namespace.
cat >"$RUN_DIR/ns.hocon" <<EOF
connectors {
  http.$NS_HTTP_CONNECTOR {
    url = "http://127.0.0.1:1/"
    enable = false
    headers {
      Authorization = "$NS_AUTHORIZATION"
      X-Test-Header = "conf-show-acceptance-ns-visible"
    }
  }
  mqtt.$CONNECTOR {
    server = "127.0.0.1:1"
    username = "conf-show-ns-user"
    password = "conf-show-ns-secret"
    enable = false
  }
}
EOF
conf_load --namespace "$NS" --replace "$RUN_DIR/ns.hocon"
check_show ns-full-redacted redacted --namespace "$NS"
check_show ns-full-raw raw --namespace "$NS" --no-secret-redaction
check_show ns-keyed-redacted redacted --namespace "$NS" connectors
check_show ns-keyed-raw raw --no-secret-redaction --namespace "$NS" connectors
assert_absent ns-full-redacted "$RUN_DIR/ns-full-redacted.out" 'conf-show-ns-secret'
assert_absent ns-full-redacted "$RUN_DIR/ns-full-redacted.out" "$NS_AUTHORIZATION"
assert_contains ns-full-raw "$RUN_DIR/ns-full-raw.out" 'conf-show-ns-secret'
assert_contains ns-full-raw "$RUN_DIR/ns-full-raw.out" "$NS_AUTHORIZATION"
assert_contains ns-full-raw "$RUN_DIR/ns-full-raw.out" 'conf-show-acceptance-ns-visible'
assert_absent ns-keyed-redacted "$RUN_DIR/ns-keyed-redacted.out" 'conf-show-ns-secret'
assert_absent ns-keyed-redacted "$RUN_DIR/ns-keyed-redacted.out" "$NS_AUTHORIZATION"
assert_contains ns-keyed-raw "$RUN_DIR/ns-keyed-raw.out" 'conf-show-ns-secret'
assert_contains ns-keyed-raw "$RUN_DIR/ns-keyed-raw.out" "$NS_AUTHORIZATION"
assert_absent ns-keyed-raw "$RUN_DIR/ns-keyed-raw.out" "file://$SECRET_FILE"
pass "namespaced redaction and isolation"

# Phase 2: raw export -> different value -> `conf load --replace` -> read back,
# first for the `file://` source and then for a literal secret.
show_raw connectors >"$RUN_DIR/export.hocon"
assert_contains export "$RUN_DIR/export.hocon" "file://$SECRET_FILE"
cat >"$RUN_DIR/changed.hocon" <<EOF
connectors.mqtt.$CONNECTOR {
  server = "127.0.0.1:1"
  username = "conf-show-user"
  password = "$CHANGED_VALUE"
  enable = false
}
EOF
conf_load --replace "$RUN_DIR/changed.hocon"
show_raw connectors >"$RUN_DIR/changed.out"
assert_contains changed "$RUN_DIR/changed.out" "$CHANGED_VALUE"
assert_absent changed "$RUN_DIR/changed.out" "file://$SECRET_FILE"
conf_load --replace "$RUN_DIR/export.hocon"
show_raw connectors >"$RUN_DIR/restored.hocon"
assert_contains restored "$RUN_DIR/restored.hocon" "file://$SECRET_FILE"
assert_absent restored "$RUN_DIR/restored.hocon" "$CHANGED_VALUE"
pass "file:// secret round trip through conf load --replace"

cat >"$RUN_DIR/literal.hocon" <<EOF
connectors.mqtt.$CONNECTOR {
  server = "127.0.0.1:1"
  username = "conf-show-user"
  password = "conf-show-literal-one"
  enable = false
}
EOF
conf_load --replace "$RUN_DIR/literal.hocon"
show_raw connectors >"$RUN_DIR/literal-export.hocon"
assert_contains literal-export "$RUN_DIR/literal-export.hocon" "conf-show-literal-one"
cat >"$RUN_DIR/literal-changed.hocon" <<EOF
connectors.mqtt.$CONNECTOR {
  server = "127.0.0.1:1"
  username = "conf-show-user"
  password = "conf-show-literal-two"
  enable = false
}
EOF
conf_load --replace "$RUN_DIR/literal-changed.hocon"
show_raw connectors >"$RUN_DIR/literal-changed.out"
assert_contains literal-changed "$RUN_DIR/literal-changed.out" "conf-show-literal-two"
conf_load --replace "$RUN_DIR/literal-export.hocon"
show_raw connectors >"$RUN_DIR/literal-restored.out"
assert_contains literal-restored "$RUN_DIR/literal-restored.out" "conf-show-literal-one"
assert_absent literal-restored "$RUN_DIR/literal-restored.out" "conf-show-literal-two"
pass "literal secret round trip through conf load --replace"

# Put the `file://` source back so the full export below is the one from phase 1.
conf_load --replace "$RUN_DIR/export.hocon"

# Phase 2b: the same round trip inside a namespace, keeping the namespace on load.
show_raw --namespace "$NS" connectors >"$RUN_DIR/ns-export.hocon"
assert_contains ns-export "$RUN_DIR/ns-export.hocon" "$NS_AUTHORIZATION"
cat >"$RUN_DIR/ns-changed.hocon" <<EOF
connectors {
  http.$NS_HTTP_CONNECTOR {
    url = "http://127.0.0.1:1/"
    enable = false
    headers {
      Authorization = "Bearer conf-show-acceptance-ns-changed"
    }
  }
}
EOF
conf_load --namespace "$NS" --replace "$RUN_DIR/ns-changed.hocon"
show_raw --namespace "$NS" connectors >"$RUN_DIR/ns-changed.out"
assert_contains ns-changed "$RUN_DIR/ns-changed.out" 'Bearer conf-show-acceptance-ns-changed'
conf_load --namespace "$NS" --replace "$RUN_DIR/ns-export.hocon"
show_raw --namespace "$NS" connectors >"$RUN_DIR/ns-restored.hocon"
assert_contains ns-restored "$RUN_DIR/ns-restored.hocon" "$NS_AUTHORIZATION"
assert_absent ns-restored "$RUN_DIR/ns-restored.hocon" 'Bearer conf-show-acceptance-ns-changed'
pass "namespaced raw export round trip through conf load --namespace --replace"

# Phase 3: restart from the raw full export with a clean data directory.
show_raw >"$RUN_DIR/full-export.hocon"
assert_contains full-export "$RUN_DIR/full-export.hocon" "$COOKIE_VALUE"
assert_contains full-export "$RUN_DIR/full-export.hocon" "file://$SECRET_FILE"
node_stop
cp "$RUN_DIR/full-export.hocon" "$CONF"
node_start "$RUN_DIR/data2"
show_raw >"$RUN_DIR/after-restart.hocon"
assert_contains after-restart "$RUN_DIR/after-restart.hocon" "file://$SECRET_FILE"
assert_absent after-restart "$RUN_DIR/after-restart.hocon" '******'
"$EMQX" eval "erlang:get_cookie()." | grep -q "$COOKIE_VALUE" ||
    fail "node.cookie was not recovered after restart"
[ "$(cat "$SECRET_FILE")" = "$SECRET_VALUE" ] ||
    fail "the file:// secret source is not readable anymore"
pass "full raw export restores into a clean data directory"

echo "PASS: conf show output modes"
