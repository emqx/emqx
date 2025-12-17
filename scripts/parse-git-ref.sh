#!/usr/bin/env bash

set -euo pipefail

# $1 is fully qualified git ref name, e.g. refs/tags/6.0.0 or refs/heads/master

is_latest_stable() {
  local ref_name
  ref_name="$(basename "$1")"

  # Find all tags that look like stable releases: X.Y.Z with no suffix
  # Example matches: 6.0.0, 6.0.1 ; excludes 6.0.0-rc1, 6.0.0-beta, etc.
  local latest_stable
  latest_stable="$(
    git tag --list \
    | grep -E '^[1-9]+\.[0-9]+\.[0-9]+$' \
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

if [[ $1 =~ ^refs/tags/[6-9]+\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]+\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/.+ ]]; then
    echo "Unrecognized tag: $1" 1>&2
    exit 1
elif [[ $1 =~ ^refs/heads/master$ ]]; then
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/release-[6-9][0-9]+$ ]]; then
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
