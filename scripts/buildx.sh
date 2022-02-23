#!/usr/bin/env bash

## This script helps to run docker buildx to build cross-arch/platform packages (linux only)
## It mounts (not copy) host directory to a cross-arch/platform builder container
## Make sure the source dir (specified by --src_dir option) is clean before running this script

## NOTE: it requires $USER in docker group
## i.e. will not work if docker command has to be executed with sudo

## example:
## ./scripts/buildx.sh --profile emqx --pkgtype tgz --arch arm64 \
##     --builder ghcr.io/emqx/emqx-builder/5.0-7:1.13.3-24.2.1-1-debian10

set -euo pipefail

help() {
    echo
    echo "-h|--help:                   To display this usage information"
    echo "--profile <PROFILE>:         EMQX profile to build, e.g. emqx, emqx-edge"
    echo "--pkgtype tgz|pkg:           Specify which package to build, tgz for .tar.gz,"
    echo "                             pkg for .rpm or .deb"
    echo "--with-elixir:               Specify if the release should be built with Elixir, "
    echo "                             defaults to false."
    echo "--arch amd64|arm64:          Target arch to build the EMQX package for"
    echo "--src_dir <SRC_DIR>:         EMQX source ode in this dir, default to PWD"
    echo "--builder <BUILDER>:         Builder image to pull"
    echo "                             E.g. ghcr.io/emqx/emqx-builder/5.0-7:1.13.3=24.2.1-1-debian10"
    echo "--otp <OTP_VSN>:             OTP version being used in the builder"
    echo "--elixir <ELIXIR_VSN>:       Elixir version being used in the builder"
    echo "--system <SYSTEM>:           OS used in the builder image"
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
    --otp)
        OTP_VSN="$2"
        shift 2
        ;;
    --elixir)
        ELIXIR_VSN="$2"
        shift 2
        ;;
    --with-elixir)
        WITH_ELIXIR=yes
        shift 1
        ;;
    --system)
        SYSTEM="$2"
        shift 2
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
   [ -z "${ARCH:-}" ]       ||
   [ -z "${OTP_VSN:-}" ]    ||
   [ -z "${ELIXIR_VSN:-}" ] ||
   [ -z "${SYSTEM:-}" ]; then
    help
    exit 1
fi

set -x

if [ -z "${WITH_ELIXIR:-}" ]; then
  WITH_ELIXIR=no
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

PKG_VSN="${PKG_VSN:-$(./pkg-vsn.sh "$PROFILE")}"

if [ "$WITH_ELIXIR" = "yes" ]
then
  PKG_NAME="${PROFILE}-${PKG_VSN}-elixir${ELIXIR_VSN}-otp${OTP_VSN}-${SYSTEM}-${ARCH}"
  MAKE_TARGET="${PROFILE}-elixir-${PKGTYPE}"
else
  PKG_NAME="${PROFILE}-${PKG_VSN}-otp${OTP_VSN}-${SYSTEM}-${ARCH}"
  MAKE_TARGET="${PROFILE}-${PKGTYPE}"
fi

CMD_RUN="export EMQX_NAME=\"$PROFILE\"; make ${MAKE_TARGET} && ./scripts/pkg-tests.sh $PKG_NAME $PKGTYPE $ARCH"

if docker info; then
   docker run --rm --privileged tonistiigi/binfmt:latest --install "${ARCH}"
   docker run -i --rm \
   -v "$(pwd)":/emqx \
   --workdir /emqx \
   --platform="linux/$ARCH" \
   "$BUILDER" \
   bash -euc "$CMD_RUN"
elif [[ $(uname -m) = "x86_64" && "$ARCH" = "amd64" ]]; then
    eval "$CMD_RUN"
elif [[ $(uname -m) = "aarch64" && "$ARCH" = "arm64" ]]; then
    eval "$CMD_RUN"
elif [[ $(uname -m) = "arm64" && "$ARCH" = "arm64" ]]; then
    eval "$CMD_RUN"
elif [[ $(uname -m) = "armv7l" && "$ARCH" = "arm64" ]]; then
    eval "$CMD_RUN"
else
  echo "Error: Docker not available on unsupported platform"
  exit 1;
fi
