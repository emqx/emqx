#!/usr/bin/env bash

## This script helps to run docker buildx to build cross-arch/platform packages (linux only)
## It mounts (not copy) host directory to a cross-arch/platform builder container
## Make sure the source dir (specified by --src_dir option) is clean before running this script

## NOTE: it requires $USER in docker group
## i.e. will not work if docker command has to be executed with sudo

## example:
## ./scripts/buildx.sh --profile emqx --pkgtype zip --builder ghcr.io/emqx/emqx-builder/4.4-12:24.1.5-3-debian10 --arch arm64

set -euo pipefail

help() {
    echo
    echo "-h|--help:           To display this usage information"
    echo "--profile <PROFILE>: EMQ X profile to build, e.g. emqx, emqx-edge"
    echo "--pkgtype zip|pkg:   Specify which package to build, zip for .zip and pkg for .rpm or .deb"
    echo "--arch amd64|arm64:  Target arch to build the EMQ X package for"
    echo "--src_dir <SRC_DIR>: EMQ X source ode in this dir, default to PWD"
    echo "--builder <BUILDER>: Builder image to pull"
    echo "                     E.g. ghcr.io/emqx/emqx-builder/4.4-12:24.1.5-3-debian10"
}

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

cd "${SRC_DIR:-.}"

set -x
docker info
docker run --rm --privileged tonistiigi/binfmt:latest --install "${ARCH}"
docker run -i --rm \
    -v "$(pwd)":/emqx \
    --workdir /emqx \
    --platform="linux/$ARCH" \
    --user root \
    "$BUILDER" \
    bash -euc "git config --global --add safe.directory /emqx && chown -R root:root _build && make ${PROFILE}-${PKGTYPE} && .ci/build_packages/tests.sh $PROFILE $PKGTYPE"
