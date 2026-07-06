#!/usr/bin/env bash

## Render-time assertions for the emqx-enterprise Helm chart.
##
## These are unit-style checks over `helm template` output: they do not need a
## Kubernetes cluster. In particular they guard `clusterDomain`, which the
## integration test in .github/workflows/run_helm_tests.yaml cannot exercise
## because it always runs on a cluster whose DNS domain is `cluster.local`.

set -euo pipefail

cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

CHART="deploy/charts/emqx-enterprise"

failures=0

# assert_count <expected> <needle> <rendered-output> <message>
assert_count() {
    local expected="$1" needle="$2" rendered="$3" message="$4"
    local actual
    actual="$(grep -F -c -- "$needle" <<<"$rendered" || true)"
    if [ "$actual" = "$expected" ]; then
        echo "ok   - ${message} (found ${actual}x '${needle}')"
    else
        echo "FAIL - ${message}: expected ${expected}x '${needle}', got ${actual}"
        failures=$((failures + 1))
    fi
}

render() {
    helm template emqx-enterprise "$CHART" "$@"
}

echo "# clusterDomain: default (dns discovery)"
# Default clusterDomain is cluster.local and default discovery is dns, so the
# domain must appear in both EMQX_HOST (StatefulSet) and EMQX_CLUSTER__DNS__NAME.
out="$(render)"
assert_count 2 "svc.cluster.local" "$out" "default renders svc.cluster.local (EMQX_HOST + DNS name)"

echo
echo "# clusterDomain: override (dns discovery)"
# A custom clusterDomain must propagate everywhere and leave no cluster.local behind.
out="$(render --set clusterDomain=k8s.corp.internal)"
assert_count 2 "svc.k8s.corp.internal" "$out" "override propagates to EMQX_HOST + DNS name"
assert_count 0 "svc.cluster.local"     "$out" "no hardcoded svc.cluster.local remains"

echo
echo "# clusterDomain: override (k8s discovery)"
# The k8s branch feeds clusterDomain into EMQX_CLUSTER__K8S__SUFFIX instead of the
# DNS name, so the override must still land in both that suffix and EMQX_HOST.
out="$(render --set clusterDomain=k8s.corp.internal \
              --set emqxConfig.EMQX_CLUSTER__DISCOVERY_STRATEGY=k8s)"
assert_count 2 "svc.k8s.corp.internal" "$out" "override propagates to EMQX_HOST + K8S suffix"
assert_count 0 "svc.cluster.local"     "$out" "no hardcoded svc.cluster.local remains"

echo
if [ "$failures" -ne 0 ]; then
    echo "${failures} assertion(s) failed"
    exit 1
fi
echo "all assertions passed"
