#!/usr/bin/env bash

## Diagnostic probe for the CI runner host.
##
## Captures disk usage, docker image accumulation, and memory to help diagnose
## "self-hosted runner lost communication" failures that have hit jobs across
## the build_slim_packages / run_test_cases matrix.
##
## Usage: runner-resource-probe.sh "<label>"
##   The label is echoed alongside each section so successive probes in the
##   same job (before/after a build step) are easy to correlate.

set -uo pipefail

label="${1:-unlabeled}"
ts="$(date -u '+%Y-%m-%dT%H:%M:%SZ')"

echo "==== probe [$label] $ts ===="

echo "---- df -hT (all mounts) ----"
df -hT 2>&1 || true

echo "---- docker system df ----"
docker system df 2>&1 || true

echo "---- docker images (count + per-image size) ----"
echo "image count: $(docker images -q 2>/dev/null | wc -l)"
docker images --format '{{.Size}}\t{{.Repository}}:{{.Tag}}' 2>&1 | sort -rh | head -30 || true

echo "---- docker / containerd data roots ----"
docker_root=$(docker info --format '{{.DockerRootDir}}' 2>/dev/null)
containerd_root=$(sudo containerd config dump 2>/dev/null | awk -F'"' '/^root = / { print $2; exit }')
containerd_root="${containerd_root:-/var/lib/containerd}"
for d in "$docker_root" "$containerd_root"; do
  [ -z "$d" ] && continue
  echo "## $d (parent fs: $(df -hT "$d" 2>/dev/null | tail -1))"
  sudo du -sh "$d"/* 2>/dev/null | sort -rh | head -10 \
    || du -sh "$d" 2>/dev/null \
    || echo "(no permission to du $d)"
done

echo "---- free -m ----"
free -m 2>&1 || true

echo "---- /proc/meminfo (top) ----"
head -10 /proc/meminfo 2>&1 || true

echo "---- uptime ----"
uptime 2>&1 || true

echo "==== end probe [$label] ===="
