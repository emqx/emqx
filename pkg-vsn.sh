#!/bin/bash
set -e -u

# This script prints the release version for emqx

# ensure dir
cd -P -- "$(dirname -- "$0")"

case $(uname) in
    *Darwin*) SED="sed -E";;
    *) SED="sed -r";;
esac

# comment SUFFIX out when finalising RELEASE
RELEASE="$(grep -oE '\{vsn, (.*)\}' src/emqx.app.src | $SED 's/\{vsn, (.*)\}/\1/g' | $SED 's/\"//g')"
if [ -d .git ] && ! git describe --tags --match "v${RELEASE}" --exact >/dev/null 2>&1; then
  SUFFIX="-$(git rev-parse HEAD | cut -b1-8)"
fi

echo "${RELEASE}${SUFFIX:-}"
