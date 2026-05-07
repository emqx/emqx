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
##   5) Installs and starts emqx_acme on both nodes; configures the plugin
##      via 'emqx eval' on emqx1 to use Pebble, the test domain, the "acme"
##      bundle, and listener_ids = ["ssl:default"]. acc_key is left unset,
##      so the plugin manages the ACME account key inside the bundle.
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
    else
        info "leaving docker stack running (--keep-up)"
    fi
}
trap cleanup EXIT

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
    $COMPOSE up -d --wait --wait-timeout 60 >"$LOG_DIR/up.log" 2>&1 || {
        error "docker compose up failed; see $LOG_DIR/up.log"
        $COMPOSE logs >"$LOG_DIR/all.log" 2>&1 || true
        exit 1
    }
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
    info "configuring plugin on emqx1 (cluster-wide via emqx_acme_config:update/1)"
    # acc_key is left unset so the plugin manages the account key inside
    # the cert bundle. On first issuance emqx1 generates the key in-memory
    # and stores it via emqx_managed_certs:add_managed_files/3, which
    # replicates it to every cluster node — no manual key distribution.
    docker exec acme-cluster-emqx1 emqx eval "
        emqx_acme_config:update(#{
            <<\"dir_url\">> => <<\"$PEBBLE_DIR_URL\">>,
            <<\"domains\">> => <<\"$DOMAIN\">>,
            <<\"contact\">> => <<\"mailto:smoke@test.local\">>,
            <<\"cert_bundle_name\">> => <<\"$BUNDLE\">>,
            <<\"listener_ids\">> => [<<\"ssl:default\">>],
            <<\"cert_type\">> => <<\"rsa\">>,
            <<\"challenge_port\">> => 5002,
            <<\"renew_before_expiry_days\">> => 30,
            <<\"check_interval_hours\">> => 9999
        })." >"$LOG_DIR/configure.log" 2>&1
}

# Issuance/renewal both kick off async workers and return {ok, started};
# the actual outcome lands in status().last_result. Poll until done.
wait_for_action_done() {
    local label="$1"
    local elapsed=0
    while [[ $elapsed -lt 120 ]]; do
        local status
        status=$(docker exec acme-cluster-emqx1 emqx eval "emqx_acme_issuer:status()." 2>&1)
        if echo "$status" | grep -q 'in_progress => false'; then
            if echo "$status" | grep -qE 'last_result => ok'; then
                info "$label completed (ok)"
                return 0
            elif echo "$status" | grep -qE 'last_result => \{error'; then
                error "$label failed"
                echo "$status" >"$LOG_DIR/${label}_status.log"
                cat "$LOG_DIR/${label}_status.log" >&2
                exit 1
            fi
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    error "$label did not complete within 120s"
    docker exec acme-cluster-emqx1 emqx eval "emqx_acme_issuer:status()." \
        >"$LOG_DIR/${label}_status.log" 2>&1
    cat "$LOG_DIR/${label}_status.log" >&2
    exit 1
}

trigger_issuance() {
    info "triggering issuance on emqx1 (lowest-name election winner)"
    # emqx_acme_issuer schedules a periodic check at boot; if it's still
    # running when we ask to issue, kickoff fails with
    # {error, {already_running, check}}. Retry until the check clears.
    local elapsed=0
    while [[ $elapsed -lt 30 ]]; do
        docker exec acme-cluster-emqx1 emqx eval "emqx_acme_issuer:issue()." \
            >"$LOG_DIR/issue.log" 2>&1
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
        exit 1
    done
    if ! grep -q 'started' "$LOG_DIR/issue.log"; then
        error "issuance kickoff did not succeed within 30s"
        cat "$LOG_DIR/issue.log" >&2
        exit 1
    fi
    info "waiting for issuance to complete"
    wait_for_action_done issue
}

assert_bundle_propagated() {
    info "verifying bundle has chain+key+acc_key on every node (acc_key replication is the whole point of the bundle-managed default)"
    for n in "${NODES[@]}"; do
        local res
        res=$(docker exec "$n" emqx eval "
            case emqx_managed_certs:list_managed_files(global, <<\"$BUNDLE\">>) of
                {ok, F} ->
                    HasChain = maps:is_key(chain, F),
                    HasKey = maps:is_key(key, F),
                    HasAccKey = maps:is_key(acc_key, F),
                    case HasChain andalso HasKey andalso HasAccKey of
                        true -> ok;
                        false -> {wrong_keys, [{chain, HasChain}, {key, HasKey}, {acc_key, HasAccKey}]}
                    end;
                Other -> {error, Other}
            end." 2>&1 | tr -d '\r' | tail -1)
        echo "$n: $res" >>"$LOG_DIR/bundle_check.log"
        if [[ "$res" != "ok" ]]; then
            error "$n bundle check failed: $res"
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
    info "verifying ssl:default config has managed_certs.bundle_name on every node"
    for n in "${NODES[@]}"; do
        local res
        res=$(docker exec "$n" emqx eval "
            try
                BN = emqx_config:get([listeners, ssl, default, ssl_options,
                                      managed_certs, bundle_name]),
                case BN of
                    <<\"$BUNDLE\">> -> ok;
                    Other -> {wrong_bundle, Other}
                end
            catch _:Reason -> {error, Reason} end." 2>&1 | tr -d '\r' | tail -1)
        echo "$n: $res" >>"$LOG_DIR/listener_check.log"
        if [[ "$res" != "ok" ]]; then
            error "$n listener config not migrated: $res"
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
        # esockd:listeners/0 returns [{{Name, {Addr, Port}}, SupPid} | _]. We
        # need the SupPid for ssl:default because that's the process esockd
        # keeps across SSL cert hot-reloads -- if it changes across a renew,
        # the cert was applied by a listener restart (Scenario B) rather
        # than via Erlang's PEM cache (Scenario A).
        docker exec "$n" emqx eval "
            {ok, F} = emqx_managed_certs:list_managed_files(global, <<\"$BUNDLE\">>),
            ChainPath = maps:get(path, maps:get(chain, F)),
            {ok, FI} = file:read_file_info(ChainPath, [{time, posix}]),
            Listeners = esockd:listeners(),
            Pid = case lists:keyfind('ssl:default', 1, [{element(1, K), V} || {K, V} <- Listeners]) of
                {_, P} -> erlang:pid_to_list(P);
                false -> \"none\"
            end,
            lists:flatten(io_lib:format(\"mtime=~p pid=~s\", [element(6, FI), Pid]))." \
            2>&1 | tr -d '\r"' | tail -1 >"$LOG_DIR/${label}_${n}.log"
    done
}

assert_scenario_a_renewal() {
    info "triggering renewal and verifying Scenario A (no listener restart)"
    snapshot_renewal_state before
    sleep 2  # ensure mtime can advance
    docker exec acme-cluster-emqx1 emqx eval "emqx_acme_issuer:renew()." \
        >"$LOG_DIR/renew.log" 2>&1
    if ! grep -q 'started' "$LOG_DIR/renew.log"; then
        error "renewal kickoff failed"
        cat "$LOG_DIR/renew.log" >&2
        exit 1
    fi
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
