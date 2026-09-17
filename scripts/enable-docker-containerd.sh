#!/usr/bin/env bash

# Enable the image store required to preserve attestations through load/save.
# Reuse the runner's Docker daemon and preserve settings such as data-root.
# setup-docker-action inherits daemon.json but also passes --data-root,
# which conflicts with a configured data directory.

set -euo pipefail

if docker info --format '{{json .DriverStatus}}' | grep -Fq 'io.containerd.snapshotter.v1'; then
    exit 0
fi

DAEMON_CONFIG_TMP=$(mktemp)
trap 'rm -f "$DAEMON_CONFIG_TMP"' EXIT
if sudo test -f /etc/docker/daemon.json; then
    sudo cat /etc/docker/daemon.json
else
    echo '{}'
fi | jq '.features["containerd-snapshotter"] = true' > "$DAEMON_CONFIG_TMP"

sudo dockerd --validate --config-file "$DAEMON_CONFIG_TMP"
sudo install -D -m 0644 "$DAEMON_CONFIG_TMP" /etc/docker/daemon.json
sudo systemctl restart docker
docker info --format '{{json .DriverStatus}}' | grep -Fq 'io.containerd.snapshotter.v1'
