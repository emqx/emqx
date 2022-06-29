#!/usr/bin/env bash

## This script helps to run docker buildx to build cross-arch/platform packages (linux only)
## It mounts (not copy) host directory to a cross-arch/platform builder container
## Make sure the source dir (specified by --src_dir option) is clean before running this script

## NOTE: it requires $USER in docker group
## i.e. will not work if docker command has to be executed with sudo

## example:
## ./scripts/buildx.sh --profile emqx --pkgtype tgz --arch arm64 \
##     --builder ghcr.io/emqx/emqx-builder/5.0-17:1.13.4-24.2.1-1-debian10

set -euo pipefail

help() {
    echo
    echo "-h|--help:                 To display this usage information"
    echo "--profile <PROFILE>:       EMQX profile to build (emqx|emqx-enterprise)"
    echo "--pkgtype tgz|pkg:         Specify which package to build, tgz for .tar.gz,"
    echo "                           pkg for .rpm or .deb"
    echo "--elixir:                  Specify if the release should be built with Elixir, "
    echo "                           defaults to 'no'."
    echo "--arch amd64|arm64:        Target arch to build the EMQX package for"
    echo "--src_dir <SRC_DIR>:       EMQX source ode in this dir, default to PWD"
    echo "--builder <BUILDER>:       Builder image to pull"
    echo "                           E.g. ghcr.io/emqx/emqx-builder/5.0-17:1.13.4-24.2.1-1-debian10"
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
    --elixir)
        shift 1
        case ${1:-novalue} in
            -*)
                # another option
                IS_ELIXIR='yes'
                ;;
            yes|no)
                IS_ELIXIR="${1}"
                shift 1
                ;;
            novalue)
                IS_ELIXIR='yes'
                ;;
            *)
                echo "ERROR: unknown option: --elixir $2"
                exit 1
                ;;
        esac
        ;;
    *)
      echo "WARN: Unknown arg (ignored): $1"
      shift
      continue
      ;;
  esac
done

if [ -z "${PROFILE:-}" ]    ||
   [ -z "${PKGTYPE:-}" ]    ||
   [ -z "${BUILDER:-}" ]    ||
   [ -z "${ARCH:-}" ]; then
    help
    exit 1
fi

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."

set -x

if [ -z "${IS_ELIXIR:-}" ]; then
  IS_ELIXIR=no
fi

case "$PKGTYPE" in
  tgz|pkg)
    true
    ;;
  *)
    echo "Bad --pkgtype option, should be tgz or pkg"
    exit 1
    ;;
esac

export CODE_PATH="${SRC_DIR:-$PWD}"
cd "${CODE_PATH}"

if [ "$IS_ELIXIR" = "yes" ]; then
  MAKE_TARGET="${PROFILE}-elixir-${PKGTYPE}"
else
  MAKE_TARGET="${PROFILE}-${PKGTYPE}"
fi

HOST_SYSTEM="$(./scripts/get-distro.sh)"
BUILDER_SYSTEM="$(echo "$BUILDER" | awk -F'-' '{print $NF}')"

CMD_RUN="make ${MAKE_TARGET} && ./scripts/pkg-tests.sh ${MAKE_TARGET}"

IS_NATIVE_SYSTEM='no'
if [[ "$HOST_SYSTEM" = "$BUILDER_SYSTEM" ]]; then
    IS_NATIVE_SYSTEM='yes'
fi

IS_NATIVE_ARCH='no'
if [[ $(uname -m) == "x86_64" && "$ARCH" == "amd64" ]]; then
    IS_NATIVE_ARCH='yes'
elif [[ $(uname -m) == "aarch64" && "$ARCH" == "arm64" ]]; then
    IS_NATIVE_ARCH='yes'
elif [[ $(uname -m) == "arm64" && "$ARCH" == "arm64" ]]; then
    IS_NATIVE_ARCH='yes'
elif [[ $(uname -m) == "armv7l" && "$ARCH" == "arm64" ]]; then
    IS_NATIVE_ARCH='yes'
fi

if [[ "${IS_NATIVE_SYSTEM}" == 'yes' && "${IS_NATIVE_ARCH}" == 'yes' ]]; then
    export ACLOCAL_PATH="/usr/share/aclocal:/usr/local/share/aclocal"
    eval "$CMD_RUN"
elif docker info; then
    if [[ "${IS_NATIVE_ARCH}" == 'no' ]]; then
        docker run --rm --privileged tonistiigi/binfmt:latest --install "${ARCH}"
    fi
    docker run -i --rm \
        -v "$(pwd)":/emqx \
        --workdir /emqx \
        --platform="linux/$ARCH" \
        --env ACLOCAL_PATH="/usr/share/aclocal:/usr/local/share/aclocal" \
        "$BUILDER" \
        bash -euc "$CMD_RUN"
else
    echo "Error: Docker not available on unsupported platform"
    exit 1;
fi
