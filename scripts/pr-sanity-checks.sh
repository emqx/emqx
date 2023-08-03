#!/usr/bin/env bash

set -euo pipefail

if ! type "docker" > /dev/null; then
    echo "docker is not installed"
    exit 1
fi

if ! type "yq" > /dev/null; then
    echo "yq is not installed"
    exit 1
fi

EMQX_BUILDER_VERSION=${EMQX_BUILDER_VERSION:-5.1-3}
EMQX_BUILDER_OTP=${EMQX_BUILDER_OTP:-25.3.2-1}
EMQX_BUILDER_ELIXIR=${EMQX_BUILDER_ELIXIR:-1.14.5}
EMQX_BUILDER_PLATFORM=${EMQX_BUILDER_PLATFORM:-ubuntu22.04}
EMQX_BUILDER=${EMQX_BUILDER:-ghcr.io/emqx/emqx-builder/${EMQX_BUILDER_VERSION}:${EMQX_BUILDER_ELIXIR}-${EMQX_BUILDER_OTP}-${EMQX_BUILDER_PLATFORM}}

commands=$(yq ".runs.steps[].run" .github/actions/pr-sanity-checks/action.yaml | grep -v null)

BEFORE_REF=${BEFORE_REF:-$(git rev-parse master)}
AFTER_REF=${AFTER_REF:-$(git rev-parse HEAD)}
docker run --rm -it -v "$(pwd):/emqx" -w /emqx \
       -e GITHUB_WORKSPACE=/emqx \
       -e BEFORE_REF="$BEFORE_REF" \
       -e AFTER_REF="$AFTER_REF" \
       -e GITHUB_BASE_REF="$BEFORE_REF" \
       -e MIX_ENV=emqx-enterprise \
       -e PROFILE=emqx-enterprise \
       "${EMQX_BUILDER}" /bin/bash -c "${commands}"
