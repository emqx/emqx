#!/usr/bin/env bash
set -euo pipefail

##
## Smoke test for the ACME plugin using Pebble (Let's Encrypt test server).
##
## Prerequisites:
##   - docker (with compose plugin)
##   - curl, jq, openssl
##   - A running EMQX node with the emqx_acme plugin installed and started
##
## What it does:
##   1) Starts Pebble + challtestsrv in Docker
##   2) Configures the ACME plugin to use Pebble as the CA
##   3) Triggers certificate issuance via the plugin API
##   4) Verifies the certificate was stored in the managed cert bundle
##   5) Cleans up Docker containers
##
## Usage:
##   plugins/emqx_acme/smoke/pebble_smoke.sh [--keep-up]
##
##   --keep-up   Don't stop Docker containers after the test (useful for debugging)
##

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"

KEEP_UP=false
for arg in "$@"; do
    case "$arg" in
        --keep-up) KEEP_UP=true ;;
        -h|--help)
            sed -n '/^##/s/^## \{0,1\}//p' "$0"
            exit 0
            ;;
    esac
done

COMPOSE_FILE="$SCRIPT_DIR/docker-compose-pebble.yml"
PLUGIN_APP="emqx_acme"
LOG_DIR="$ROOT_DIR/_build/plugins/smoke-logs/$PLUGIN_APP"
BUNDLE_NAME="acme-smoke-test"
TEST_DOMAIN="mqtt.smoke.test"

# The challenge port EMQX will listen on (inside the host network).
# Pebble validates HTTP-01 against the validated domain on port 5002 by
# default, so we match that here so Pebble's VA can reach EMQX directly
# without an intermediate port-forward / reverse proxy.
CHALLENGE_PORT=5002

# Pebble validates challenges by connecting to challtestsrv, which we configure
# to proxy HTTP-01 challenges to our EMQX host's challenge port.
PEBBLE_DIR_URL="https://localhost:14000/dir"
PEBBLE_MGMT_URL="https://localhost:15000"
CHALLTESTSRV_URL="http://localhost:8055"

mkdir -p "$LOG_DIR"

info()  { echo "[acme-smoke] $*"; }
error() { echo "[acme-smoke] ERROR: $*" >&2; }

cleanup() {
    if [[ "$KEEP_UP" == "false" ]]; then
        info "stopping Docker containers"
        docker compose -f "$COMPOSE_FILE" down -v --remove-orphans 2>/dev/null || true
    else
        info "leaving Docker containers running (--keep-up)"
    fi
}
trap cleanup EXIT

check_prereqs() {
    local missing=()
    for cmd in docker curl jq openssl; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        error "missing required commands: ${missing[*]}"
        exit 1
    fi
    if [[ ! -x "$EMQX_BIN" ]]; then
        error "EMQX binary not found: $EMQX_BIN"
        error "Run: make $PROFILE"
        exit 1
    fi
}

start_pebble() {
    info "starting Pebble and challtestsrv"
    docker compose -f "$COMPOSE_FILE" up -d --wait --wait-timeout 30

    # Wait for Pebble to become ready
    local retries=20
    while ! curl -sk "$PEBBLE_DIR_URL" >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            error "Pebble did not become ready"
            docker compose -f "$COMPOSE_FILE" logs
            exit 1
        fi
        sleep 1
    done
    info "Pebble is ready"

    # Tell challtestsrv to resolve our test domain to the host
    # where EMQX's challenge listener will run.
    # Using host.docker.internal on Docker Desktop, or 172.17.0.1 on Linux.
    local host_ip
    host_ip=$(docker_host_ip)
    info "configuring challtestsrv: $TEST_DOMAIN -> $host_ip"
    curl -s -X POST "$CHALLTESTSRV_URL/set-default-ipv4" \
        -d "{\"ip\": \"$host_ip\"}" >/dev/null
}

docker_host_ip() {
    # Try host.docker.internal first (Docker Desktop), fallback to docker bridge gateway
    if getent hosts host.docker.internal >/dev/null 2>&1; then
        getent hosts host.docker.internal | awk '{print $1}'
    else
        # Linux: use the docker bridge gateway IP
        docker network inspect bridge -f '{{range .IPAM.Config}}{{.Gateway}}{{end}}' 2>/dev/null \
            || echo "172.17.0.1"
    fi
}

configure_plugin() {
    info "configuring ACME plugin (acc_key unset -> plugin manages it inside the bundle)"
    # 'emqx eval' on the rel binary doesn't accept multi-line input; keep
    # the expression on one line.
    local expr
    expr="emqx_acme_config:update(#{<<\"dir_url\">> => <<\"$PEBBLE_DIR_URL\">>, <<\"domains\">> => <<\"$TEST_DOMAIN\">>, <<\"contact\">> => <<\"mailto:smoke@test.local\">>, <<\"cert_bundle_name\">> => <<\"$BUNDLE_NAME\">>, <<\"listener_ids\">> => [], <<\"cert_type\">> => <<\"ec\">>, <<\"challenge_port\">> => $CHALLENGE_PORT, <<\"renew_before_expiry_days\">> => 30, <<\"check_interval_hours\">> => 9999})."
    "$EMQX_BIN" eval "$expr" >"$LOG_DIR/configure.log" 2>&1
    if ! grep -q '^ok' "$LOG_DIR/configure.log"; then
        error "configure_plugin failed"
        cat "$LOG_DIR/configure.log" >&2
        exit 1
    fi
    info "plugin configured"
}

fetch_pebble_ca() {
    # Pebble generates a new root CA on each startup; fetch it so the ACME
    # client can validate the certificate chain.
    info "fetching Pebble root CA"
    curl -sk "$PEBBLE_MGMT_URL/roots/0" > "$LOG_DIR/pebble-ca.pem"
}

trigger_issuance() {
    info "triggering certificate issuance"
    # Kickoff is async: emqx_acme_issuer:issue/0 returns {ok, started} or
    # {error, {already_running, _}}. Retry briefly to clear any concurrent
    # periodic check, then poll status until in_progress drops back to
    # false and surface the outcome via last_result.
    local elapsed=0
    while [[ $elapsed -lt 30 ]]; do
        "$EMQX_BIN" eval 'emqx_acme_issuer:issue().' >"$LOG_DIR/issue.log" 2>&1
        if grep -q 'started' "$LOG_DIR/issue.log"; then
            break
        fi
        if grep -q 'already_running' "$LOG_DIR/issue.log"; then
            sleep 2
            elapsed=$((elapsed + 2))
            continue
        fi
        error "issuance kickoff failed"
        cat "$LOG_DIR/issue.log" >&2
        return 1
    done
    if ! grep -q 'started' "$LOG_DIR/issue.log"; then
        error "issuance kickoff did not succeed within 30s"
        cat "$LOG_DIR/issue.log" >&2
        return 1
    fi
    info "waiting for issuance to complete"
    elapsed=0
    while [[ $elapsed -lt 180 ]]; do
        local status
        status=$("$EMQX_BIN" eval 'emqx_acme_issuer:status().' 2>&1)
        if echo "$status" | grep -q 'in_progress => false'; then
            if echo "$status" | grep -qE 'last_result => ok'; then
                info "issuance completed (ok)"
                return 0
            elif echo "$status" | grep -qE 'last_result => \{error'; then
                error "issuance failed"
                echo "$status" >"$LOG_DIR/issue_status.log"
                cat "$LOG_DIR/issue_status.log" >&2
                return 1
            fi
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    error "issuance did not complete within 180s"
    "$EMQX_BIN" eval 'emqx_acme_issuer:status().' >"$LOG_DIR/issue_status.log" 2>&1
    cat "$LOG_DIR/issue_status.log" >&2
    return 1
}

verify_bundle() {
    info "verifying certificate bundle"
    local result
    result=$("$EMQX_BIN" eval "case emqx_managed_certs:list_managed_files(global, <<\"$BUNDLE_NAME\">>) of {ok, F} -> lists:sort(maps:keys(F)); Other -> {error, Other} end." 2>&1)

    echo "$result" > "$LOG_DIR/verify.log"

    # Bundle-managed acc_key flow: the bundle should now hold chain, key,
    # AND acc_key (the plugin generated and stored the ACME account key
    # alongside the issued cert).
    local ok=true
    for kind in chain key acc_key; do
        if ! echo "$result" | grep -q "$kind"; then
            error "missing file kind in bundle: $kind"
            ok=false
        fi
    done

    if [[ "$ok" == "false" ]]; then
        error "bundle verification failed"
        cat "$LOG_DIR/verify.log" >&2
        return 1
    fi

    # Spot-check that acc-key.pem is a real PEM-encoded private key. The
    # bundle path comes back relative to EMQX's runtime cwd (the rel
    # install root), so resolve it against EMQX_BIN's grandparent.
    local emqx_root
    emqx_root="$(cd "$(dirname "$EMQX_BIN")/.." && pwd)"
    local acc_path_rel
    acc_path_rel=$("$EMQX_BIN" eval "{ok, F} = emqx_managed_certs:list_managed_files(global, <<\"$BUNDLE_NAME\">>), erlang:binary_to_list(iolist_to_binary(maps:get(path, maps:get(acc_key, F))))." 2>&1 | tr -d '\r"' | tail -1)
    local acc_path="$emqx_root/$acc_path_rel"
    if ! openssl pkey -in "$acc_path" -noout 2>/dev/null; then
        error "acc-key.pem at '$acc_path' is not a valid private key PEM"
        return 1
    fi

    info "bundle contains: chain, key, acc_key (acc-key.pem at $acc_path)"
}

verify_cert_domain() {
    info "verifying certificate domain"
    local emqx_root
    emqx_root="$(cd "$(dirname "$EMQX_BIN")/.." && pwd)"
    local chain_rel
    chain_rel=$("$EMQX_BIN" eval "{ok, F} = emqx_managed_certs:list_managed_files(global, <<\"$BUNDLE_NAME\">>), erlang:binary_to_list(iolist_to_binary(maps:get(path, maps:get(chain, F))))." 2>&1 | tr -d '\r"' | tail -1)
    local chain_path="$emqx_root/$chain_rel"

    if [[ ! -f "$chain_path" ]]; then
        error "chain file not found: $chain_path"
        return 1
    fi

    local subject
    subject=$(openssl x509 -in "$chain_path" -noout -subject 2>/dev/null)
    local san
    san=$(openssl x509 -in "$chain_path" -noout -ext subjectAltName 2>/dev/null || true)

    echo "Subject: $subject" > "$LOG_DIR/cert_info.log"
    echo "SAN: $san" >> "$LOG_DIR/cert_info.log"

    if echo "$san" | grep -qi "$TEST_DOMAIN"; then
        info "certificate SAN contains $TEST_DOMAIN"
    elif echo "$subject" | grep -qi "$TEST_DOMAIN"; then
        info "certificate CN contains $TEST_DOMAIN"
    else
        error "certificate does not contain $TEST_DOMAIN"
        cat "$LOG_DIR/cert_info.log" >&2
        return 1
    fi
}

cleanup_bundle() {
    info "cleaning up test bundle"
    "$EMQX_BIN" eval "emqx_managed_certs:delete_bundle(global, <<\"$BUNDLE_NAME\">>)." \
        >/dev/null 2>&1 || true
}

main() {
    check_prereqs
    start_pebble
    fetch_pebble_ca
    configure_plugin
    trigger_issuance
    verify_bundle
    verify_cert_domain
    cleanup_bundle
    info "PASS"
}

main
