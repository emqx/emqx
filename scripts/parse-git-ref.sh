#!/usr/bin/env bash

set -euo pipefail

# $1 is fully qualified git ref name, e.g. refs/tags/v5.1.0 or refs/heads/master

is_latest_stable() {
  local ref_name
  ref_name="$(basename "$1")"

  # Find all tags that look like stable releases: eX.Y.Z with no suffix
  # Example matches: e5.8.3, e5.10.7 ; excludes e6.0.0-rc1, e6.0.0-beta, etc.
  local latest_stable
  latest_stable="$(
    git tag --list 'e*.*.*' \
    | grep -E '^e[0-9]+\.[0-9]+\.[0-9]+$' \
    | sed 's/^e//' \
    | sort -V \
    | tail -n1
  )"

  # If no stable tags found, it's not the latest stable.
  if [[ -z "$latest_stable" ]]; then
    echo false
    return
  fi

  if [[ "$ref_name" == "$latest_stable" ]]; then
    echo true
  else
    echo false
  fi
}

PROFILE=emqx-enterprise

if [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=$(is_latest_stable "$1")
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+-build\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=$(is_latest_stable "$1")
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/e[6-9]+\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/e[6-9]+\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/.+ ]]; then
    echo "Unrecognized tag: $1" 1>&2
    exit 1
elif [[ $1 =~ ^refs/heads/master$ ]]; then
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/release-[5-9][0-9]+$ ]]; then
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/ci/.* ]]; then
    RELEASE=false
    LATEST=false
else
    echo "Unrecognized git ref: $1" 1>&2
    exit 1
fi

cat <<EOF
{"profile": "$PROFILE", "release": $RELEASE, "latest": $LATEST}
EOF
