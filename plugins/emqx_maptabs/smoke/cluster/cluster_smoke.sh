#!/usr/bin/env bash
##
## Cluster smoke test for the emqx_maptabs plugin.
##
## Verifies cluster-wide table load, update, delete, and down-node
## catch-up on a 2-node emqx/emqx-enterprise:6.1.4 cluster.
##
## Prerequisites:
##   - docker (with the compose plugin)
##   - curl, jq
##
## What it does:
##   1) Builds the plugin tarball ('make plugin-emqx_maptabs').
##   2) Brings up emqx1 + emqx2 via docker compose and waits for the
##      cluster to form.
##   3) Installs, starts, and enables the plugin on both nodes via
##      'emqx ctl plugins'.
##   4) LOAD: loads a table on emqx1 via 'emqx ctl maptabs load' and
##      asserts on BOTH nodes: 'maptabs list' reports the same content
##      version, and a rule-SQL 'maptab_lookup' through each node's
##      /rule_test API returns the row (proves the ETS cache and the
##      registered rule function work end to end on every node, not
##      just the initiating one).
##   5) UPDATE: loads a changed table from emqx2 and asserts emqx1 sees
##      the new version and the new row values ('maptabs status' shows
##      no version drift between the nodes).
##   6) CATCH-UP: stops emqx2, loads another update on emqx1, restarts
##      emqx2, and asserts emqx2 converges to the latest version (mria
##      recovers the replicated storage on restart and the plugin
##      rebuilds its cache from it).
##   7) DELETE: deletes the table from emqx2 and asserts storage +
##      cache are gone on both nodes and lookups miss (return the
##      default).
##   8) MISSED DELETE: deletes a table on emqx1 while emqx2 is down and
##      asserts the restarted emqx2 drops it too.
##
## Usage:
##   plugins/emqx_maptabs/smoke/cluster/cluster_smoke.sh [--keep-up]
##
##   --keep-up   leave docker containers running after the test (debug aid)
##

set -euo pipefail

SCRIPT_DIR="$(cd "$(dirname "$0")" && pwd)"
ROOT_DIR="$(cd "$SCRIPT_DIR/../../../.." && pwd)"
COMPOSE_FILE="$SCRIPT_DIR/docker-compose.yml"
COMPOSE="docker compose -f $COMPOSE_FILE"

PLUGIN_APP="emqx_maptabs"
PLUGIN_VSN="$(tr -d '[:space:]' < "$ROOT_DIR/plugins/$PLUGIN_APP/VERSION")"
PLUGIN="$PLUGIN_APP-$PLUGIN_VSN"
TARBALL="$PLUGIN.tar.gz"

TABLE="can_signals"
TABLE_FILE_IN_CONTAINER="/tmp/$TABLE.json"
NODES=(maptabs-cluster-emqx1 maptabs-cluster-emqx2)
API_PORTS=(18093 18094)

LOG_DIR="$ROOT_DIR/_build/plugins/smoke-logs/maptabs-cluster"
mkdir -p "$LOG_DIR"

BOOTSTRAP_KEY="maptabs-smoke-test-key"
BOOTSTRAP_SECRET="maptabs-smoke-test-secret-not-for-prod"
BOOTSTRAP_FILE="$SCRIPT_DIR/bootstrap-api-key.txt"

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

info()  { echo "[maptabs-smoke] $*"; }
error() { echo "[maptabs-smoke] ERROR: $*" >&2; }

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

fail() {
    error "$*"
    for n in "${NODES[@]}"; do
        docker logs --tail 300 "$n" >"$LOG_DIR/fail_${n}.log" 2>&1 || true
    done
    error "container logs captured under $LOG_DIR"
    exit 1
}

check_prereqs() {
    local missing=()
    for cmd in docker curl jq; do
        command -v "$cmd" >/dev/null 2>&1 || missing+=("$cmd")
    done
    if [[ ${#missing[@]} -gt 0 ]]; then
        error "missing required commands: ${missing[*]}"
        exit 1
    fi
}

build_plugin() {
    info "building plugin tarball"
    (cd "$ROOT_DIR" && make "plugin-$PLUGIN_APP") \
        >"$LOG_DIR/build.log" 2>&1 || fail "plugin build failed; see $LOG_DIR/build.log"
    [[ -f "$ROOT_DIR/_build/plugins/$TARBALL" ]] \
        || fail "$ROOT_DIR/_build/plugins/$TARBALL was not produced"
}

bring_up_stack() {
    info "starting docker stack"
    printf '%s:%s\n' "$BOOTSTRAP_KEY" "$BOOTSTRAP_SECRET" > "$BOOTSTRAP_FILE"
    chmod 0644 "$BOOTSTRAP_FILE"
    $COMPOSE up -d --wait --wait-timeout 120 >"$LOG_DIR/up.log" 2>&1 || {
        $COMPOSE logs >"$LOG_DIR/all.log" 2>&1 || true
        fail "docker compose up failed; see $LOG_DIR/up.log"
    }
}

wait_for_cluster() {
    info "waiting for cluster to form"
    local elapsed=0
    while [[ $elapsed -lt 120 ]]; do
        if docker exec maptabs-cluster-emqx1 emqx ctl cluster status 2>/dev/null \
            | grep -q "emqx@emqx2.maptabs.test"; then
            info "cluster up: both nodes visible from emqx1"
            return 0
        fi
        sleep 2
        elapsed=$((elapsed + 2))
    done
    fail "cluster did not form within 120s"
}

## 'emqx ctl' commands register asynchronously after boot; until then
## every invocation fails with "Command table is initializing.".
wait_for_ctl() {
    local n elapsed out
    for n in "${NODES[@]}"; do
        elapsed=0
        while :; do
            out=$(docker exec "$n" emqx ctl plugins list 2>&1) &&
                [[ "$out" != *"initializing"* ]] && break
            elapsed=$((elapsed + 2))
            [[ $elapsed -lt 60 ]] || fail "ctl commands not ready on $n within 60s: $out"
            sleep 2
        done
    done
}

install_and_start_plugin() {
    info "installing, starting, and enabling plugin on both nodes"
    wait_for_ctl
    : >"$LOG_DIR/install.log"
    local n verb
    for n in "${NODES[@]}"; do
        docker exec "$n" cp "/plugins-tarballs/$TARBALL" "/opt/emqx/plugins/$TARBALL" \
            || fail "copying $TARBALL into $n failed"
        for verb in allow install start enable; do
            echo "== $n: plugins $verb $PLUGIN" >>"$LOG_DIR/install.log"
            docker exec "$n" emqx ctl plugins "$verb" "$PLUGIN" \
                >>"$LOG_DIR/install.log" 2>&1 \
                || {
                    cat "$LOG_DIR/install.log" >&2
                    fail "plugins $verb on $n failed; see $LOG_DIR/install.log"
                }
        done
    done
    if grep -qE '"result"\s*:\s*"not_ok"|EXIT' "$LOG_DIR/install.log"; then
        cat "$LOG_DIR/install.log" >&2
        fail "plugin install failed; see $LOG_DIR/install.log"
    fi
}

## ---- maptabs helpers -------------------------------------------------

## Writes the given JSON rows to the upload path inside a container.
put_table_file() {
    local node="$1" json="$2"
    printf '%s' "$json" | docker exec -i "$node" sh -c "cat > $TABLE_FILE_IN_CONTAINER"
}

maptabs_cli() {
    local node="$1"
    shift
    docker exec "$node" emqx ctl maptabs "$@" 2>&1 | tr -d '\r'
}

## Loads the upload file on the given node and asserts the CLI reports ok.
load_on() {
    local node="$1"
    local out
    out=$(maptabs_cli "$node" load "$TABLE_FILE_IN_CONTAINER")
    echo "$node load: $out" >>"$LOG_DIR/maptabs_cli.log"
    echo "$out" | jq -e '.result == "ok"' >/dev/null \
        || fail "load on $node did not return ok: $out"
}

## Content version of the table as reported by 'maptabs list' on a node.
## Empty output when the table is absent — or when the node is not ready
## yet (e.g. "Command table is initializing." right after a restart), so
## callers' retry loops keep polling instead of tripping on non-JSON.
version_on() {
    local node="$1" out
    out=$(maptabs_cli "$node" list) || return 0
    jq -r --arg t "$TABLE" '.[] | select(.name == $t) | .version' \
        <<<"$out" 2>/dev/null || true
}

## Rule-SQL lookup through the node's management API: returns the
## looked-up field (or the SQL-level default 'none' on a miss). This
## exercises the registered external rule function and the node-local
## ETS cache, exactly like a real rule would.
rule_lookup_on() {
    local api_port="$1" key="$2"
    local sql payload
    sql="SELECT maptab_lookup('$TABLE', $key, 'signal_name', 'none') AS s FROM \"t/1\""
    payload=$(jq -n --arg sql "$sql" '{
        sql: $sql,
        context: {
            event_type: "message_publish",
            clientid: "smoke",
            username: "smoke",
            topic: "t/1",
            qos: 1,
            payload: "{}"
        }
    }')
    ## tolerate a not-yet-ready API (non-JSON body): callers retry
    curl -s -u "$BOOTSTRAP_KEY:$BOOTSTRAP_SECRET" \
        -H 'content-type: application/json' \
        -d "$payload" \
        "http://localhost:$api_port/api/v5/rule_test" 2>/dev/null \
        | jq -r '.s // empty' 2>/dev/null || true
}

## Asserts that both nodes report the expected version and resolve the
## given key to the given signal_name via rule SQL. Retries briefly:
## mria replication to the peer and the peer's event-driven cache
## rebuild are asynchronous, so the assertion polls until convergence.
assert_table_everywhere() {
    local expect_version="$1" key="$2" expect_name="$3" label="$4"
    local i
    for i in "${!NODES[@]}"; do
        local node="${NODES[$i]}" port="${API_PORTS[$i]}"
        local elapsed=0 version got
        while :; do
            version=$(version_on "$node")
            got=$(rule_lookup_on "$port" "$key")
            if [[ "$version" == "$expect_version" && "$got" == "$expect_name" ]]; then
                break
            fi
            elapsed=$((elapsed + 2))
            [[ $elapsed -lt 60 ]] \
                || fail "$label: $node did not converge (version=$version lookup=$got, expected version=$expect_version lookup=$expect_name)"
            sleep 2
        done
        info "$label: $node ok (version=$version, lookup($key)=$got)"
    done
}

rows_v1() {
    jq -n '[{key: 2, signal_name: "sig_f32", start_bit: 17, length: 32},
            {key: 3, signal_name: "sig_s8",  start_bit: 17, length: 8}]'
}

rows_v2() {
    ## key 2 renamed, key 4 added
    jq -n '[{key: 2, signal_name: "sig_f32_v2", start_bit: 17, length: 32},
            {key: 3, signal_name: "sig_s8",     start_bit: 17, length: 8},
            {key: 4, signal_name: "sig_new",    start_bit: 17, length: 16}]'
}

rows_v3() {
    jq -n '[{key: 2, signal_name: "sig_f32_v3", start_bit: 17, length: 32}]'
}

## ---- scenarios -------------------------------------------------------

scenario_load() {
    info "LOAD: loading $TABLE on emqx1, expecting cluster-wide visibility"
    put_table_file "${NODES[0]}" "$(rows_v1)"
    load_on "${NODES[0]}"
    local version
    version=$(version_on "${NODES[0]}")
    [[ -n "$version" ]] || fail "LOAD: no version reported on emqx1 after load"
    assert_table_everywhere "$version" 2 "sig_f32" "LOAD"
}

scenario_update() {
    info "UPDATE: loading a changed $TABLE from emqx2, expecting emqx1 to follow"
    put_table_file "${NODES[1]}" "$(rows_v2)"
    load_on "${NODES[1]}"
    local version
    version=$(version_on "${NODES[1]}")
    assert_table_everywhere "$version" 2 "sig_f32_v2" "UPDATE"
    assert_table_everywhere "$version" 4 "sig_new" "UPDATE(new row)"
    ## the built-in drift detector agrees: one distinct version across nodes
    local distinct
    distinct=$(maptabs_cli "${NODES[0]}" status \
        | jq -r --arg t "$TABLE" '[.[].tables[]? | select(.name == $t) | .version] | unique | length')
    [[ "$distinct" == "1" ]] \
        || fail "UPDATE: 'maptabs status' reports version drift across nodes"
}

scenario_catch_up() {
    info "CATCH-UP: updating while emqx2 is down, expecting replay on restart"
    docker stop maptabs-cluster-emqx2 >/dev/null
    put_table_file "${NODES[0]}" "$(rows_v3)"
    load_on "${NODES[0]}"
    local version
    version=$(version_on "${NODES[0]}")
    docker start maptabs-cluster-emqx2 >/dev/null
    info "CATCH-UP: waiting for emqx2 to boot and replay the missed update"
    local elapsed=0
    while ! docker exec maptabs-cluster-emqx2 emqx ctl status 2>/dev/null | grep -q 'is started'; do
        sleep 2
        elapsed=$((elapsed + 2))
        [[ $elapsed -lt 120 ]] || fail "CATCH-UP: emqx2 did not restart within 120s"
    done
    assert_table_everywhere "$version" 2 "sig_f32_v3" "CATCH-UP"
}

assert_table_gone_on() {
    local label="$1" node="$2" port="$3"
    local elapsed=0
    while [[ -n "$(version_on "$node")" ]]; do
        elapsed=$((elapsed + 2))
        [[ $elapsed -lt 60 ]] || fail "$label: $node still lists $TABLE"
        sleep 2
    done
    local got
    got=$(rule_lookup_on "$port" 2)
    [[ "$got" == "none" ]] \
        || fail "$label: lookup on $node returned '$got', expected the default 'none'"
    info "$label: $node ok (table gone, lookup falls back to default)"
}

scenario_delete() {
    info "DELETE: deleting $TABLE from emqx2, expecting cluster-wide removal"
    local out
    out=$(maptabs_cli "${NODES[1]}" delete "$TABLE")
    echo "$out" | jq -e '.result == "ok"' >/dev/null \
        || fail "DELETE: delete on emqx2 did not return ok: $out"
    local i
    for i in "${!NODES[@]}"; do
        assert_table_gone_on "DELETE" "${NODES[$i]}" "${API_PORTS[$i]}"
    done
}

scenario_missed_delete() {
    info "MISSED-DELETE: deleting $TABLE while emqx2 is down, expecting removal after restart"
    put_table_file "${NODES[0]}" "$(rows_v1)"
    load_on "${NODES[0]}"
    local version
    version=$(version_on "${NODES[0]}")
    assert_table_everywhere "$version" 2 "sig_f32" "MISSED-DELETE(setup)"
    docker stop maptabs-cluster-emqx2 >/dev/null
    local out
    out=$(maptabs_cli "${NODES[0]}" delete "$TABLE")
    echo "$out" | jq -e '.result == "ok"' >/dev/null \
        || fail "MISSED-DELETE: delete on emqx1 did not return ok: $out"
    docker start maptabs-cluster-emqx2 >/dev/null
    info "MISSED-DELETE: waiting for emqx2 to boot"
    local elapsed=0
    while ! docker exec maptabs-cluster-emqx2 emqx ctl status 2>/dev/null | grep -q 'is started'; do
        sleep 2
        elapsed=$((elapsed + 2))
        [[ $elapsed -lt 120 ]] || fail "MISSED-DELETE: emqx2 did not restart within 120s"
    done
    local i
    for i in "${!NODES[@]}"; do
        assert_table_gone_on "MISSED-DELETE" "${NODES[$i]}" "${API_PORTS[$i]}"
    done
}

main() {
    check_prereqs
    build_plugin
    bring_up_stack
    wait_for_cluster
    install_and_start_plugin
    scenario_load
    scenario_update
    scenario_catch_up
    scenario_delete
    scenario_missed_delete
    info "PASS"
}

main
