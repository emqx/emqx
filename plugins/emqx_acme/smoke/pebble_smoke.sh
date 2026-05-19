#!/usr/bin/env bash
set -euo pipefail

##
## Smoke test for the ACME plugin against Pebble (LE test ACME CA).
## Single-node, fully containerised — brings up emqx/emqx:6.1.1 +
## pebble + challtestsrv via docker-compose, installs the plugin from
## the freshly built tarball, drives it over the dashboard HTTP API
## using a bootstrap-loaded API key, and verifies issuance end-to-end.
##
## Prerequisites:
##   - docker (with compose plugin)
##   - curl, jq, openssl
##
## What it does:
##   1) Builds the plugin tarball via 'make plugin-emqx_acme'.
##   2) Writes a bootstrap api-key file and brings up the docker stack.
##   3) Installs and starts emqx_acme in the running emqx container.
##   4) Configures the plugin via PUT /api/v5/plugins/<name>/config.
##   5) Triggers issuance via POST /api/v5/plugin_api/<name>/issue and
##      polls /status until last_result lands.
##   6) Verifies chain.pem / key.pem / acc-key.pem inside the container
##      and that the issued cert covers the test domain.
##   7) Tears down the docker stack and removes the bootstrap file.
##
## Usage:
##   plugins/emqx_acme/smoke/pebble_smoke.sh [--keep-up]
##
##   --keep-up   Don't tear down docker after the test (debug aid).
##

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose-pebble.yml"
COMPOSE="docker compose -f $COMPOSE_FILE"

PLUGIN_APP="emqx_acme"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
TARBALL="$PLUGIN.tar.gz"
EMQX_CONTAINER="acme-pebble-emqx"

LOG_DIR="$ROOT_DIR/_build/plugins/smoke-logs/$PLUGIN_APP"
BUNDLE_NAME="acme-smoke-test"
TEST_DOMAIN="mqtt.smoke.test"

# Challenge port: pebble's VA targets HTTP-01 on port 5002 (its
# default). emqx's challenge listener binds the same port inside the
# acmenet docker network — no host mapping needed since pebble reaches
# emqx via the network, not via the host.
CHALLENGE_PORT=5002

# These URLs are seen from inside the emqx container (for the plugin
# config) vs the host (for our smoke's own curl). Pebble issues against
# its container IP; we hit the management port via the host mapping.
PEBBLE_DIR_URL_FOR_EMQX="https://10.30.50.2:14000/dir"
PEBBLE_MGMT_URL="https://localhost:15000"

# API key bootstrap — written before `docker compose up`, mounted into
# the emqx container, loaded at boot. We use it for HTTP basic auth so
# the smoke never has to navigate the dashboard login flow.
BOOTSTRAP_KEY="acme-pebble-smoke-key"
BOOTSTRAP_SECRET="acme-pebble-smoke-secret-not-for-prod"
BOOTSTRAP_FILE="$SCRIPT_DIR/bootstrap-api-key.txt"
API_BASE="http://localhost:18083/api/v5"

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

mkdir -p "$LOG_DIR"

info()  { echo "[acme-smoke] $*"; }
error() { echo "[acme-smoke] ERROR: $*" >&2; }

cleanup() {
    if [[ "$KEEP_UP" == "false" ]]; then
        info "stopping docker stack"
        $COMPOSE down -v --remove-orphans 2>/dev/null || true
        rm -f "$BOOTSTRAP_FILE"
    else
        info "leaving docker stack running (--keep-up)"
    fi
}
trap cleanup EXIT

# HTTP basic auth against the dashboard mgmt API. Echoes the HTTP code
# on stdout; the body is written to $4 (default /dev/null) so callers
# can stash it for error logging without re-issuing the request.
api() {
    local method="$1" path="$2" body="${3-}"
    local out_file="${4:-/dev/null}"
    local args=(-s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" -X "$method")
    if [[ -n "$body" ]]; then
        args+=(-H 'content-type: application/json' -d "$body")
    fi
    curl "${args[@]}" -o "$out_file" -w '%{http_code}' "$API_BASE$path"
}

api_get() {
    curl -s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" "$API_BASE$1"
}

check_prereqs() {
    local missing=()
    for cmd in docker curl jq openssl; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        error "missing required commands: ${missing[*]}"
        exit 1
    fi
}

build_plugin() {
    info "building plugin tarball"
    (cd "$ROOT_DIR" && make plugin-emqx_acme) >"$LOG_DIR/build.log" 2>&1
    if [[ ! -f "$ROOT_DIR/_build/plugins/$TARBALL" ]]; then
        error "$ROOT_DIR/_build/plugins/$TARBALL was not produced"
        cat "$LOG_DIR/build.log" >&2 || true
        exit 1
    fi
}

write_bootstrap_file() {
    info "writing api key bootstrap file ($BOOTSTRAP_FILE)"
    printf '%s:%s\n' "$BOOTSTRAP_KEY" "$BOOTSTRAP_SECRET" > "$BOOTSTRAP_FILE"
    chmod 0644 "$BOOTSTRAP_FILE"
}

bring_up_stack() {
    info "starting docker stack"
    write_bootstrap_file
    $COMPOSE up -d --wait --wait-timeout 60 >"$LOG_DIR/up.log" 2>&1 || {
        error "docker compose up failed; see $LOG_DIR/up.log"
        $COMPOSE logs >"$LOG_DIR/all.log" 2>&1 || true
        exit 1
    }
    info "waiting for dashboard API to authenticate the bootstrap key"
    local retries=30 code
    while :; do
        code=$(api GET /status 2>/dev/null || echo "000")
        [[ "$code" == "200" ]] && break
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            error "dashboard API did not accept bootstrap key within 30s (last HTTP $code)"
            $COMPOSE logs emqx >"$LOG_DIR/emqx_boot.log" 2>&1 || true
            exit 1
        fi
        sleep 1
    done
    info "waiting for Pebble (host-mapped on :14000)"
    retries=30
    while ! curl -sk "$PEBBLE_MGMT_URL/roots/0" >/dev/null 2>&1; do
        retries=$((retries - 1))
        if [[ $retries -le 0 ]]; then
            error "Pebble did not become ready"
            $COMPOSE logs pebble >"$LOG_DIR/pebble_boot.log" 2>&1 || true
            exit 1
        fi
        sleep 1
    done
}

fetch_pebble_ca() {
    # Pebble generates a new root CA on each startup. We just stash it
    # for debugging — emqx skips chain validation against pebble in this
    # smoke (the plugin trusts whatever the dir_url's TLS hands back).
    info "fetching Pebble root CA"
    curl -sk "$PEBBLE_MGMT_URL/roots/0" > "$LOG_DIR/pebble-ca.pem" || true
}

install_and_start_plugin() {
    info "installing and starting plugin in container"
    docker exec "$EMQX_CONTAINER" cp "/plugins-tarballs/$TARBALL" "/opt/emqx/plugins/$TARBALL"
    docker exec "$EMQX_CONTAINER" emqx ctl plugins install "$PLUGIN" \
        >>"$LOG_DIR/install.log" 2>&1
    if grep -qE '"result"\s*:\s*"not_ok"|EXIT' "$LOG_DIR/install.log"; then
        error "plugin install failed; see $LOG_DIR/install.log"
        cat "$LOG_DIR/install.log" >&2
        exit 1
    fi
    docker exec "$EMQX_CONTAINER" emqx ctl plugins start "$PLUGIN" \
        >>"$LOG_DIR/start.log" 2>&1
}

configure_plugin() {
    info "configuring ACME plugin via PUT /plugins/$PLUGIN/config (acc_key unset -> plugin manages it inside the bundle)"
    local payload
    payload=$(jq -n \
        --arg dir_url "$PEBBLE_DIR_URL_FOR_EMQX" \
        --arg domains "$TEST_DOMAIN" \
        --arg bundle "$BUNDLE_NAME" \
        --argjson port "$CHALLENGE_PORT" \
        '{
            dir_url: $dir_url,
            domains: $domains,
            contact: "mailto:smoke@test.local",
            cert_bundle_name: $bundle,
            listener_ids: "",
            cert_type: "rsa",
            challenge_port: $port,
            renew_before_expiry_days: 30,
            check_interval_hours: 9999,
            enable_dashboard_https: false,
            dashboard_https_port: 18084,
            acc_key: null,
            acc_key_password: null
        }')
    local code
    code=$(api PUT "/plugins/$PLUGIN/config" "$payload" "$LOG_DIR/configure.out")
    if [[ "$code" != "200" && "$code" != "204" ]]; then
        error "PUT plugin config failed (HTTP $code)"
        cat "$LOG_DIR/configure.out" >&2 || true
        exit 1
    fi
}

# Kick off the named async action (issue / renew). 202 = started,
# 409 = something already running (typically the boot-time periodic
# check) — retry until it clears.
kickoff_action() {
    local action="$1"
    info "kickoff: POST /plugin_api/$PLUGIN/$action"
    local elapsed=0 code
    while [[ $elapsed -lt 30 ]]; do
        code=$(api POST "/plugin_api/$PLUGIN/$action" "" "$LOG_DIR/${action}_kick.out")
        case "$code" in
            202) return 0 ;;
            409) sleep 2; elapsed=$((elapsed + 2));;
            *)
                error "$action kickoff failed (HTTP $code)"
                cat "$LOG_DIR/${action}_kick.out" >&2 || true
                exit 1
                ;;
        esac
    done
    error "$action kickoff did not succeed within 30s (last HTTP $code)"
    cat "$LOG_DIR/${action}_kick.out" >&2 || true
    exit 1
}

wait_for_action_done() {
    local label="$1"
    local elapsed=0
    while [[ $elapsed -lt 180 ]]; do
        local body in_progress last_result
        body=$(api_get "/plugin_api/$PLUGIN/status")
        echo "$body" >"$LOG_DIR/${label}_status.log"
        in_progress=$(echo "$body" | jq -r '.in_progress // false')
        last_result=$(echo "$body" | jq -r '
            if .last_result == "ok" then "ok"
            elif .last_result == null then "pending"
            elif (.last_result | type) == "object" and (.last_result | has("error")) then "error"
            else "pending" end')
        if [[ "$in_progress" == "false" ]]; then
            case "$last_result" in
                ok)    info "$label completed (ok)"; return 0 ;;
                error)
                    error "$label failed"
                    cat "$LOG_DIR/${label}_status.log" >&2
                    dump_failure_logs "$label"
                    exit 1
                    ;;
            esac
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    error "$label did not complete within 180s"
    cat "$LOG_DIR/${label}_status.log" >&2 || true
    dump_failure_logs "$label"
    exit 1
}

# Capture emqx + pebble + challtestsrv logs so a CI-only failure is
# debuggable. Tail-only to keep the log files small; full logs stay
# available via 'docker compose logs' for as long as the stack is up.
dump_failure_logs() {
    local label="$1"
    info "capturing container logs to $LOG_DIR/${label}_*.log"
    docker logs --tail 300 "$EMQX_CONTAINER" >"$LOG_DIR/${label}_emqx.log" 2>&1 || true
    docker logs --tail 200 acme-pebble-pebble >"$LOG_DIR/${label}_pebble.log" 2>&1 || true
    docker logs --tail 100 acme-pebble-challtestsrv >"$LOG_DIR/${label}_challtestsrv.log" 2>&1 || true
}

trigger_issuance() {
    kickoff_action issue
    info "waiting for issuance to complete"
    wait_for_action_done issue
}

verify_bundle() {
    info "verifying certificate bundle inside container"
    local bundle_dir="/opt/emqx/data/certs2/global/$BUNDLE_NAME"
    local missing_files=""
    for f in chain.pem key.pem acc-key.pem; do
        if ! docker exec "$EMQX_CONTAINER" test -f "$bundle_dir/$f"; then
            missing_files="$missing_files $f"
        fi
    done
    if [[ -n "$missing_files" ]]; then
        error "bundle missing:$missing_files (in $EMQX_CONTAINER:$bundle_dir)"
        docker exec "$EMQX_CONTAINER" ls -la "$bundle_dir" >&2 2>/dev/null || true
        return 1
    fi
    # Spot-check that acc-key.pem is a real PEM-encoded private key.
    if ! docker exec "$EMQX_CONTAINER" openssl pkey \
            -in "$bundle_dir/acc-key.pem" -noout 2>/dev/null; then
        error "acc-key.pem at '$bundle_dir/acc-key.pem' is not a valid private key PEM"
        return 1
    fi
    info "bundle contains: chain, key, acc_key"
}

verify_cert_domain() {
    info "verifying certificate domain via /plugin_api/$PLUGIN/status"
    local body chain_rel
    body=$(api_get "/plugin_api/$PLUGIN/status")
    echo "$body" >"$LOG_DIR/status.json"
    chain_rel=$(echo "$body" | jq -r '.certificate.chain_path // empty')
    if [[ -z "$chain_rel" ]]; then
        error "status did not report a certificate chain_path"
        cat "$LOG_DIR/status.json" >&2
        return 1
    fi
    # The status response returns a path relative to /opt/emqx (the
    # container's cwd at boot). Use it as-is inside the container.
    local container_path="/opt/emqx/$chain_rel"
    if ! docker exec "$EMQX_CONTAINER" test -f "$container_path"; then
        error "chain file not found in container: $container_path"
        return 1
    fi

    local subject san
    subject=$(docker exec "$EMQX_CONTAINER" openssl x509 \
        -in "$container_path" -noout -subject 2>/dev/null || true)
    san=$(docker exec "$EMQX_CONTAINER" openssl x509 \
        -in "$container_path" -noout -ext subjectAltName 2>/dev/null || true)
    {
        echo "Subject: $subject"
        echo "SAN: $san"
    } >"$LOG_DIR/cert_info.log"

    if echo "$san" | grep -qi "$TEST_DOMAIN" \
        || echo "$subject" | grep -qi "$TEST_DOMAIN"; then
        info "certificate matches $TEST_DOMAIN"
    else
        error "certificate does not contain $TEST_DOMAIN"
        cat "$LOG_DIR/cert_info.log" >&2
        return 1
    fi
}

main() {
    check_prereqs
    build_plugin
    bring_up_stack
    fetch_pebble_ca
    install_and_start_plugin
    configure_plugin
    trigger_issuance
    verify_bundle
    verify_cert_domain
    info "PASS"
}

main
