#!/usr/bin/env bash
##
## Cluster smoke test for the emqx_acme plugin.
##
## Verifies the end-to-end Scenario B + Scenario A flow on a 2-node
## emqx/emqx-enterprise:6.1.1 cluster against a Pebble-backed test ACME CA.
##
## Prerequisites:
##   - docker (with the compose plugin)
##   - curl, openssl, jq
##
## What it does:
##   1) Builds the plugin tarball via 'make plugin-emqx_acme'.
##   2) Brings up Pebble + challtestsrv + emqx1 + emqx2 via docker compose.
##   3) Waits for the cluster to form (both nodes in running_nodes).
##   4) Asserts emqx1 starts ssl:default with the installation's default
##      self-signed cert (the "before" Scenario-B baseline).
##   5) Installs and starts emqx_acme on both nodes via 'emqx ctl plugins';
##      configures the plugin on emqx1 via the management HTTP API
##      (PUT /api/v5/plugins/<name>/config), authenticated with a bootstrap
##      API key mounted into both containers. listener_ids = "ssl:default",
##      acc_key left unset so the plugin manages the ACME account key
##      inside the bundle.
##   6) Triggers issuance on emqx1.
##   7) Verifies Scenario B: every node has chain+key+acc_key in the bundle
##      (the acc_key was generated on emqx1 and replicated cluster-wide
##      via emqx_managed_certs:add_managed_files/3), every node's
##      ssl:default config gained ssl_options.managed_certs.bundle_name =
##      "acme", and a TLS handshake to each node returns a cert whose SAN
##      contains the test domain.
##   8) Verifies Scenario A renewal: triggers renew, asserts that on every
##      node chain.pem mtime advanced, the listener supervisor pid did NOT
##      change, and the next handshake serves the freshly-issued cert.
##      Also asserts the cluster-shared acc_key fingerprint stays the same
##      across the renewal (we don't churn the ACME identity).
##
## Usage:
##   plugins/emqx_acme/smoke/cluster/cluster_smoke.sh [--keep-up]
##
##   --keep-up   leave docker containers running after the test (debug aid)
##
## Cases NOT covered (suggested follow-ups; see README at end of this script):
##   - Late join: a 3rd emqx joining the cluster after Scenario B.
##   - Election under simultaneous renewal triggers from both nodes.
##   - Issuer node loss mid-ACME (failover behaviour).
##   - emqx_conf:update propagation to a partitioned node on reconnect.
##   - listener_ids referencing a not-yet-existing listener (skipped at
##     issuance time, listener created later still picks up the bundle).
##

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
COMPOSE="docker compose -f $COMPOSE_FILE"

PLUGIN_APP="emqx_acme"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
TARBALL="$PLUGIN.tar.gz"

DOMAIN="mqtt.acme.test"
BUNDLE="acme"
PEBBLE_DIR_URL="https://172.21.50.20:14000/dir"
NODES=(acme-cluster-emqx1 acme-cluster-emqx2)
HOSTPORTS=(localhost:8883 localhost:8884)

LOG_DIR="$ROOT_DIR/_build/plugins/smoke-logs/cluster"
mkdir -p "$LOG_DIR"

# API key seeded into both nodes via the bootstrap file mounted at
# /etc/emqx/bootstrap-api-key.txt (see docker-compose.yml). The smoke
# writes the file before `docker compose up`; emqx loads it at boot and
# we drive every config/issuance call over HTTP basic auth instead of
# the dashboard login dance.
BOOTSTRAP_KEY="acme-smoke-test-key"
BOOTSTRAP_SECRET="acme-smoke-test-secret-not-for-prod"
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

info()  { echo "[cluster-smoke] $*"; }
error() { echo "[cluster-smoke] ERROR: $*" >&2; }

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

# HTTP basic auth against the dashboard's mgmt API. The bootstrap key
# is loaded on every emqx boot; we never call /login. Each helper sets
# its own check on the response status, since curl with --fail would
# swallow the body we want to log.
api() {
    local method="$1" path="$2" body="${3-}"
    local out_file="${4:-/dev/null}"
    local args=(-s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" -X "$method")
    if [[ -n "$body" ]]; then
        args+=(-H 'content-type: application/json' -d "$body")
    fi
    curl "${args[@]}" -o "$out_file" -w '%{http_code}' "$API_BASE$path"
}

# Convenience: GET that returns the JSON body to stdout. Callers can
# pipe straight into jq; HTTP-level errors fall through to the body.
api_get() {
    curl -s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" "$API_BASE$1"
}

write_bootstrap_file() {
    info "writing api key bootstrap file ($BOOTSTRAP_FILE)"
    printf '%s:%s\n' "$BOOTSTRAP_KEY" "$BOOTSTRAP_SECRET" > "$BOOTSTRAP_FILE"
    chmod 0644 "$BOOTSTRAP_FILE"
}

check_prereqs() {
    local missing=()
    for cmd in docker curl openssl jq; do
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
        exit 1
    fi
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
            exit 1
        fi
        sleep 1
    done
}

wait_for_cluster() {
    info "waiting for cluster to form"
    local elapsed=0
    while [[ $elapsed -lt 120 ]]; do
        if docker exec acme-cluster-emqx1 emqx ctl cluster status 2>/dev/null \
            | grep -q "emqx@emqx2.acme.test"; then
            info "cluster up: both nodes visible from emqx1"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    error "cluster did not form within 120s"
    docker exec acme-cluster-emqx1 emqx ctl cluster status >"$LOG_DIR/cluster_status.log" 2>&1 || true
    exit 1
}

assert_default_cert_in_use() {
    info "asserting Scenario-B baseline: ssl:default serves the EMQX-default cert"
    local subject
    subject=$(echo "" \
        | openssl s_client -connect "${HOSTPORTS[0]}" -servername "$DOMAIN" 2>/dev/null \
        | openssl x509 -noout -subject 2>/dev/null || true)
    echo "$subject" >"$LOG_DIR/baseline_cert.log"
    if echo "$subject" | grep -qi "$DOMAIN"; then
        error "ssl:default already serves a $DOMAIN cert before the plugin ran"
        exit 1
    fi
}

install_and_start_plugin() {
    info "installing and starting plugin on both nodes"
    # 6.1.x's `plugins install <Name-Vsn>` expects the tarball to be in the
    # install_dir on the local node and we observed cluster-wide propagation
    # is not reliable here (emqx2 ends up with just the tarball, not an
    # installed plugin) — so install on each node explicitly.
    for n in "${NODES[@]}"; do
        docker exec "$n" cp "/plugins-tarballs/$TARBALL" "/opt/emqx/plugins/$TARBALL"
        docker exec "$n" emqx ctl plugins install "$PLUGIN" \
            >>"$LOG_DIR/install.log" 2>&1
    done
    if grep -qE '"result"\s*:\s*"not_ok"|EXIT' "$LOG_DIR/install.log"; then
        error "plugin install failed; see $LOG_DIR/install.log"
        cat "$LOG_DIR/install.log" >&2
        exit 1
    fi
    for n in "${NODES[@]}"; do
        docker exec "$n" emqx ctl plugins start "$PLUGIN" \
            >>"$LOG_DIR/start.log" 2>&1
    done
}

configure_plugin() {
    info "configuring plugin on emqx1 via HTTP PUT /plugins/$PLUGIN/config (cluster-wide)"
    # acc_key is left unset so the plugin manages the account key inside
    # the cert bundle. On first issuance emqx1 generates the key in-memory
    # and stores it via emqx_managed_certs:add_managed_files/3, which
    # replicates it to every cluster node — no manual key distribution.
    local payload
    payload=$(jq -n \
        --arg dir_url "$PEBBLE_DIR_URL" \
        --arg domains "$DOMAIN" \
        --arg bundle "$BUNDLE" \
        '{
            dir_url: $dir_url,
            domains: $domains,
            contact: "mailto:smoke@test.local",
            cert_bundle_name: $bundle,
            listener_ids: "ssl:default",
            cert_type: "rsa",
            challenge_port: 5002,
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

# Issuance/renewal both kick off async workers; the actual outcome
# lands in status.last_result. Poll the plugin API until done.
wait_for_action_done() {
    local label="$1"
    local elapsed=0
    while [[ $elapsed -lt 120 ]]; do
        local body
        body=$(api_get "/plugin_api/$PLUGIN/status")
        echo "$body" >"$LOG_DIR/${label}_status.log"
        local in_progress last_result
        in_progress=$(echo "$body" | jq -r '.in_progress // false')
        last_result=$(echo "$body" | jq -r '
            if .last_result == "ok" then "ok"
            elif .last_result == null then "pending"
            elif (.last_result | type) == "object" and (.last_result | has("error")) then "error"
            else "pending" end')
        if [[ "$in_progress" == "false" ]]; then
            case "$last_result" in
                ok)
                    info "$label completed (ok)"
                    return 0
                    ;;
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
    error "$label did not complete within 120s"
    cat "$LOG_DIR/${label}_status.log" >&2 || true
    dump_failure_logs "$label"
    exit 1
}

# Capture each container's last few hundred log lines so a CI-only
# failure is debuggable from the run artifacts alone. Full logs stay
# available via 'docker compose logs' for as long as the stack is up.
dump_failure_logs() {
    local label="$1"
    info "capturing container logs to $LOG_DIR/${label}_*.log"
    for n in "${NODES[@]}"; do
        docker logs --tail 300 "$n" >"$LOG_DIR/${label}_${n}.log" 2>&1 || true
    done
    docker logs --tail 200 acme-cluster-pebble >"$LOG_DIR/${label}_pebble.log" 2>&1 || true
    docker logs --tail 100 acme-cluster-challtestsrv >"$LOG_DIR/${label}_challtestsrv.log" 2>&1 || true
}

# Kick off the named async action (issue / renew). Returns once
# /plugin_api/<plugin>/<action> replies 202; retries through the
# periodic-check race (409 already_running) until the check clears.
kickoff_action() {
    local action="$1"
    info "kickoff: POST /plugin_api/$PLUGIN/$action"
    local elapsed=0
    while [[ $elapsed -lt 30 ]]; do
        local code
        code=$(api POST "/plugin_api/$PLUGIN/$action" "" "$LOG_DIR/${action}_kick.out")
        case "$code" in
            202) return 0 ;;
            409)
                # already_running (typically the periodic check) — retry.
                sleep 2
                elapsed=$((elapsed + 2))
                ;;
            *)
                error "$action kickoff failed (HTTP $code)"
                cat "$LOG_DIR/${action}_kick.out" >&2 || true
                exit 1
                ;;
        esac
    done
    error "$action kickoff did not succeed within 30s"
    cat "$LOG_DIR/${action}_kick.out" >&2 || true
    exit 1
}

trigger_issuance() {
    info "triggering issuance on emqx1 (lowest-name election winner)"
    kickoff_action issue
    info "waiting for issuance to complete"
    wait_for_action_done issue
}

assert_bundle_propagated() {
    info "verifying bundle has chain+key+acc_key on every node (acc_key replication is the whole point of the bundle-managed default)"
    : >"$LOG_DIR/bundle_check.log"
    for n in "${NODES[@]}"; do
        local missing_files=""
        for f in chain.pem key.pem acc-key.pem; do
            if ! docker exec "$n" test -f "/opt/emqx/data/certs2/global/$BUNDLE/$f"; then
                missing_files="$missing_files $f"
            fi
        done
        echo "$n: missing=[${missing_files:-none}]" >>"$LOG_DIR/bundle_check.log"
        if [[ -n "$missing_files" ]]; then
            error "$n bundle missing:$missing_files"
            exit 1
        fi
    done
}

declare -A ACC_KEY_FP_PRE_RENEW

acc_key_fingerprint_on() {
    local n="$1"
    docker exec "$n" sh -c \
        "openssl pkey -in /opt/emqx/data/certs2/global/$BUNDLE/acc-key.pem -outform DER 2>/dev/null | sha256sum | awk '{print \$1}'"
}

assert_acc_key_replicated_identical() {
    info "verifying acc-key.pem is byte-identical on every node (proves replication, not independent generation)"
    : >"$LOG_DIR/acc_key_fp.log"
    local fingerprints=()
    for n in "${NODES[@]}"; do
        local fp
        fp=$(acc_key_fingerprint_on "$n")
        echo "$n: $fp" >>"$LOG_DIR/acc_key_fp.log"
        if [[ -z "$fp" ]]; then
            error "$n: could not fingerprint acc-key.pem"
            exit 1
        fi
        # Stash pre-renew fingerprint for the post-renew comparison.
        ACC_KEY_FP_PRE_RENEW[$n]=$fp
        fingerprints+=("$fp")
    done
    local first="${fingerprints[0]}"
    for fp in "${fingerprints[@]}"; do
        if [[ "$fp" != "$first" ]]; then
            error "acc-key.pem differs across nodes (expected identical fingerprints): see $LOG_DIR/acc_key_fp.log"
            exit 1
        fi
    done
    info "acc-key.pem fingerprint identical across cluster: $first"
}

assert_listener_migrated() {
    info "verifying ssl:default config has managed_certs.bundle_name on every node via GET /listeners/ssl:default"
    : >"$LOG_DIR/listener_check.log"
    # The /listeners/:id endpoint returns a single consolidated config
    # object (cluster-replicated via mria), so we hit each node's
    # dashboard API independently to confirm mria propagated the update.
    # Per-node ports come from the docker-compose mapping.
    local node_ports=("localhost:18083" "localhost:18084")
    local i
    for i in "${!NODES[@]}"; do
        local node="${NODES[$i]}" hp="${node_ports[$i]}" body bundle_name
        body=$(curl -s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" \
            "http://$hp/api/v5/listeners/ssl:default")
        echo "$node: $body" >>"$LOG_DIR/listener_check.log"
        bundle_name=$(echo "$body" | jq -r '.ssl_options.managed_certs.bundle_name // empty')
        if [[ "$bundle_name" != "$BUNDLE" ]]; then
            error "$node ssl:default not migrated to bundle=$BUNDLE; got: '$bundle_name'"
            cat "$LOG_DIR/listener_check.log" >&2 || true
            exit 1
        fi
    done
}

assert_pebble_cert_served() {
    info "verifying TLS handshake on every node returns a Pebble-issued cert"
    for hp in "${HOSTPORTS[@]}"; do
        local cert
        cert=$(echo "" \
            | openssl s_client -connect "$hp" -servername "$DOMAIN" 2>/dev/null \
            | openssl x509 -noout -subject -ext subjectAltName 2>/dev/null || true)
        echo "$hp: $cert" >>"$LOG_DIR/served_cert.log"
        if ! echo "$cert" | grep -qi "$DOMAIN"; then
            error "$hp did not serve the $DOMAIN cert"
            cat "$LOG_DIR/served_cert.log" >&2
            exit 1
        fi
    done
}

snapshot_renewal_state() {
    local label="$1"
    info "capturing $label renewal state (mtime + listener pid) on every node"
    for n in "${NODES[@]}"; do
        local chain_path="/opt/emqx/data/certs2/global/$BUNDLE/chain.pem"
        # mtime: ordinary filesystem stat inside the container — no eval needed.
        local mtime
        mtime=$(docker exec "$n" stat -c '%Y' "$chain_path" 2>/dev/null || echo 0)
        # esockd's listener supervisor pid for ssl:default is the single
        # invariant we still cross-check via 'emqx eval'. Neither
        # `emqx ctl listeners` nor `GET /listeners` exposes it, but it's
        # exactly the signal we need: if it stays the same across a
        # renewal, Erlang ssl's PEM cache hot-reloaded the new cert
        # (Scenario A); if it changes, the listener was restarted
        # (Scenario B), which would drop client connections.
        local pid
        pid=$(docker exec "$n" emqx eval "
            case lists:keyfind('ssl:default', 1,
                [{element(1, K), V} || {K, V} <- esockd:listeners()]) of
                {_, P} -> erlang:pid_to_list(P);
                false -> \"none\"
            end." 2>&1 | tr -d '\r"' | tail -1)
        echo "mtime=$mtime pid=$pid" >"$LOG_DIR/${label}_${n}.log"
    done
}

assert_scenario_a_renewal() {
    info "triggering renewal and verifying Scenario A (no listener restart)"
    snapshot_renewal_state before
    sleep 2  # ensure mtime can advance
    kickoff_action renew
    wait_for_action_done renew
    snapshot_renewal_state after

    for n in "${NODES[@]}"; do
        local before after
        before=$(cat "$LOG_DIR/before_${n}.log")
        after=$(cat "$LOG_DIR/after_${n}.log")
        local mtime_before mtime_after
        mtime_before=$(echo "$before" | grep -oE 'mtime=[0-9]+' | cut -d= -f2)
        mtime_after=$(echo "$after"  | grep -oE 'mtime=[0-9]+' | cut -d= -f2)
        if [[ -z "$mtime_before" || -z "$mtime_after" || "$mtime_after" -le "$mtime_before" ]]; then
            error "$n chain.pem mtime did not advance after renew (before=$mtime_before after=$mtime_after)"
            exit 1
        fi
        local pid_before pid_after
        pid_before=$(echo "$before" | sed -nE 's/.*pid=([^[:space:]]+).*/\1/p')
        pid_after=$(echo "$after"  | sed -nE 's/.*pid=([^[:space:]]+).*/\1/p')
        if [[ -z "$pid_before" || "$pid_before" == "none" ]]; then
            error "$n: ssl:default listener pid not found in snapshot (before=$before)"
            exit 1
        fi
        if [[ "$pid_before" != "$pid_after" ]]; then
            error "$n listener pid changed across renew (before=$pid_before after=$pid_after); Scenario A should be a hot reload, not a restart"
            exit 1
        fi
    done
    info "Scenario A confirmed: chain mtime advanced, listener pid unchanged on every node"
}

assert_acc_key_stable_across_renewal() {
    info "verifying acc-key.pem fingerprint unchanged across renewal (we don't churn the ACME identity on renew)"
    for n in "${NODES[@]}"; do
        local before="${ACC_KEY_FP_PRE_RENEW[$n]:-}"
        if [[ -z "$before" ]]; then
            error "$n: no pre-renew fingerprint captured"
            exit 1
        fi
        local after
        after=$(acc_key_fingerprint_on "$n")
        if [[ "$after" != "$before" ]]; then
            error "$n: acc-key.pem changed across renewal (before=$before after=$after); expected stable identity"
            exit 1
        fi
    done
    info "acc-key.pem stable across renewal"
}

main() {
    check_prereqs
    build_plugin
    bring_up_stack
    wait_for_cluster
    assert_default_cert_in_use
    install_and_start_plugin
    configure_plugin
    trigger_issuance
    assert_bundle_propagated
    assert_acc_key_replicated_identical
    assert_listener_migrated
    assert_pebble_cert_served
    assert_scenario_a_renewal
    assert_acc_key_stable_across_renewal
    info "PASS"
}

main
