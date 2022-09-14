#!/usr/bin/env bash

## This script helps to run docker buildx to build cross-arch/platform packages (linux only)
## It mounts (not copy) host directory to a cross-arch/platform builder container
## Make sure the source dir (specified by --src_dir option) is clean before running this script

## NOTE: it requires $USER in docker group
## i.e. will not work if docker command has to be executed with sudo

## example:
## ./scripts/buildx.sh --profile emqx --pkgtype zip --builder ghcr.io/emqx/emqx-builder/4.4-19:24.1.5-3-debian10 --arch arm64

set -euo pipefail

help() {
    echo
    echo "-h|--help:           To display this usage information"
    echo "--profile <PROFILE>: EMQ X profile to build, e.g. emqx, emqx-edge"
    echo "--pkgtype zip|pkg:   Specify which package to build, zip for .zip and pkg for .rpm or .deb"
    echo "--arch amd64|arm64:  Target arch to build the EMQ X package for"
    echo "--src_dir <SRC_DIR>: EMQ X source ode in this dir, default to PWD"
    echo "--builder <BUILDER>: Builder image to pull"
    echo "                     E.g. ghcr.io/emqx/emqx-builder/4.4-19:24.1.5-3-debian11"
    echo "--system <SYSTEM>:   The target OS system the package is being built for, ex: debian11"
    echo "--ssh:               Pass ssh agent to the builder."
    echo "                     Also configures git in container to use ssh instead of https to clone deps"
}

USE_SSH='no'
while [ "$#" -gt 0 ]; do
    case $1 in
    -h|--help)
        help
        exit 0
        ;;
    --src_dir)
        SRC_DIR="$2"
        shift 2
        ;;
    --profile)
        PROFILE="$2"
        shift 2
        ;;
    --pkgtype)
        PKGTYPE="$2"
        shift 2
        ;;
    --builder)
        BUILDER="$2"
        shift 2
        ;;
    --arch)
        ARCH="$2"
        shift 2
        ;;
    --system)
        SYSTEM="$2"
        shift 2
        ;;
    --ssh)
        USE_SSH='yes'
        shift
        ;;
    *)
      echo "WARN: Unknown arg (ignored): $1"
      shift
      continue
      ;;
  esac
done

if [ -z "${PROFILE:-}" ] || [ -z "${PKGTYPE:-}" ] || [ -z "${BUILDER:-}" ] || [ -z "${ARCH:-}" ]; then
    help
    exit 1
fi

if [ "$PKGTYPE" != 'zip' ] && [ "$PKGTYPE" != 'pkg' ]; then
    echo "Bad --pkgtype option, should be zip or pkg"
    exit 1
fi

## Although we have 'deterministic' set in 'erl_opts', and foced overriding at project level,
## still, some of the beams might be compiled (e.g. by erlang.mk) without this flag
## longer file path means larger beam files
## i.e. Keep the path to work dir short!
DOCKER_WORKDIR='/emqx'

cd "${SRC_DIR:-.}"

cat <<EOF >.gitconfig.tmp
[core]
    sshCommand = ssh -o UserKnownHostsFile=/dev/null -o StrictHostKeyChecking=no
[safe]
    directory = $DOCKER_WORKDIR
EOF

if [ "$USE_SSH" = 'yes' ]; then
    cat <<EOF >>.gitconfig.tmp
[url "ssh://git@github.com/"]
    insteadOf = https://github.com/
EOF
    # when passing ssh agent, we assume this command is executed locally not in ci, so add '-t' option
    SSH_AGENT_OPTION="-t -e SSH_AUTH_SOCK=/ssh-agent -v ${SSH_AUTH_SOCK}:/ssh-agent"
else
    SSH_AGENT_OPTION=''
fi

docker info
docker run --rm --privileged tonistiigi/binfmt:latest --install "${ARCH}"

# $SYSTEM below is used by the `relup-base-vsns.escript` to correctly
# output the list of relup base versions.
# shellcheck disable=SC2086
docker run -i --rm \
    -v "$(pwd)":$DOCKER_WORKDIR \
    -v "$(pwd)/.gitconfig.tmp":/root/.gitconfig \
    --workdir $DOCKER_WORKDIR \
    --platform="linux/$ARCH" \
    --user root \
    -e SYSTEM="$SYSTEM" \
    $SSH_AGENT_OPTION \
    "$BUILDER" \
    bash -euc "mkdir -p _build && chown -R root:root _build && make ${PROFILE}-${PKGTYPE} && .ci/build_packages/tests.sh $PROFILE $PKGTYPE"
