#!/usr/bin/env bash

set -euo pipefail
set -x

PROFILE="$1"
COMPILE="${2:-no}"
DISTRO="$(./scripts/get-distro.sh)"
PKG_VSN="${PKG_VSN:-$(./pkg-vsn.sh "$PROFILE")}"

case "$DISTRO" in
    ubuntu20*)
        EMQX_DOCKERFILE="Dockerfile.ubuntu20.04.runner"
        ;;
    *)
        echo "sorry, no support for $DISTRO yet"
        exit 1
esac

if [ "$COMPILE" = '--compile' ]; then
    make "$PROFILE"
    sync
fi

# cannot enable DOCKER_BUILDKIT because the COPY often gets stale layers
#export DOCKER_BUILDKIT=1
docker build --build-arg PROFILE="${PROFILE}" \
    -t "emqx/emqx:${PKG_VSN}-${DISTRO}" \
    -f "$EMQX_DOCKERFILE" .
