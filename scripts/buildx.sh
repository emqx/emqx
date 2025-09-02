#!/usr/bin/env bash

## This script helps to run docker buildx to build cross-arch/platform packages (linux only)
## It mounts (not copy) host directory to a cross-arch/platform builder container
## Make sure the source dir (specified by --src_dir option) is clean before running this script

## NOTE: it requires $USER in docker group
## i.e. will not work if docker command has to be executed with sudo

## example:
## ./scripts/buildx.sh --pkgtype tgz

set -euo pipefail

# ensure dir
cd -P -- "$(dirname -- "${BASH_SOURCE[0]}")/.."
# shellcheck disable=SC1091
source ./env.sh

help() {
    echo
    echo "-h|--help:"
    echo "    To display this usage information"
    echo ""
    echo "--profile <PROFILE>:"
    echo "    EMQX profile to build, default is emqx-enterprise"
    echo ""
    echo "--pkgtype tgz|pkg|rel|relup:"
    echo "    Specify which package to build, tgz for .tar.gz,"
    echo "    pkg for .rpm or .deb, rel for release only."
    echo "    Defaults to tgz."
    echo ""
    echo "--elixir:"
    echo "    Specify if the release should be built with Elixir, "
    echo "    defaults to 'no'."
    echo ""
    echo "--arch amd64|arm64:"
    echo "    Target arch to build the EMQX package for."
    echo "    Default is host machine's arch."
    echo ""
    echo "--src_dir <SRC_DIR>:"
    echo "    EMQX source code in this dir, defaults to PWD"
    echo ""
    echo "--builder <BUILDER>:"
    echo "    Docker image to use for building"
    echo "    E.g. ${EMQX_BUILDER}"
    echo "    For hot upgrading tar.gz, specify a builder image with the same OS distribution as the running one."
    echo "    Specifically, for EMQX's docker containers hot upgrading, please use the debian13-based builder. "
    echo "    Defaults to builder configured in env.sh."
}

die() {
    msg="$1"
    echo "$msg" >&2
    help
    exit 1
}

PROFILE=emqx-enterprise
PKGTYPE=tgz

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
        EMQX_BUILDER="$2"
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
    --flavor)
        EMQX_FLAVOR="$2"
        shift 2
        ;;
    *)
      echo "WARN: Unknown arg (ignored): $1"
      shift
      continue
      ;;
  esac
done

## we have a different naming for them
if [[ $(uname -m) == "x86_64" ]]; then
    NATIVE_ARCH='amd64'
elif [[ $(uname -m) == "aarch64" ]]; then
    NATIVE_ARCH='arm64'
elif [[ $(uname -m) == "arm64" ]]; then
    NATIVE_ARCH='arm64'
fi
ARCH="${ARCH:-${NATIVE_ARCH:-}}"

[ -z "${PROFILE:-}" ] && die "missing --profile"
[ -z "${PKGTYPE:-}" ] && die "missing --pkgtype"
[ -z "${EMQX_BUILDER:-}" ] && die "missing --builder"
[ -z "${ARCH:-}" ] && die "missing --arch"

set -x

if [ -z "${IS_ELIXIR:-}" ]; then
  IS_ELIXIR=no
fi

if [ -z "${EMQX_FLAVOR:-}" ]; then
  EMQX_FLAVOR=official
fi

case "${PKGTYPE:-}" in
  tgz|pkg|rel|relup)
    true
    ;;
  *)
    echo "Bad --pkgtype option, should be tgz, pkg, rel or relup"
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
BUILDER_SYSTEM="${BUILDER_SYSTEM:-$(echo "$EMQX_BUILDER" | awk -F'-' '{print $NF}')}"

if [[ "${PKGTYPE}" != 'rel' && "${PKGTYPE}" != 'relup' ]]; then
  CMD_RUN="make ${MAKE_TARGET} && ./scripts/pkg-tests.sh ${MAKE_TARGET}"
else
  CMD_RUN="make ${MAKE_TARGET}"
fi

IS_NATIVE_SYSTEM='no'
if [[ "$BUILDER_SYSTEM" != "force_docker" ]]; then
    if [[ "$BUILDER_SYSTEM" == "force_host" ]] || [[ "$BUILDER_SYSTEM" == "$HOST_SYSTEM" ]]; then
        IS_NATIVE_SYSTEM='yes'
    fi
fi

IS_NATIVE_ARCH='no'
if [[ "$NATIVE_ARCH" == "$ARCH" ]]; then
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
        --env EMQX_FLAVOR="$EMQX_FLAVOR" \
        "$EMQX_BUILDER" \
        bash -euc "git config --global --add safe.directory /emqx && $CMD_RUN"
else
    echo "Error: Docker not available on unsupported platform"
    exit 1;
fi
