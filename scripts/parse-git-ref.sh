#!/usr/bin/env bash

set -euo pipefail

# $1 is fully qualified git ref name, e.g. refs/tags/v5.1.0 or refs/heads/master

is_latest() {
    ref_name=$(basename "$1")
    # shellcheck disable=SC2046
    for t in $(git tag --points-at $(git rev-list --tags --max-count=1)); do
        if [[ "$t" == "$ref_name" ]]; then
            echo true;
            return;
        fi
    done
    echo false
}

if [[ $1 =~ ^refs/tags/v[5-9]+\.[0-9]+\.[0-9]+$ ]]; then
    PROFILE=emqx
    RELEASE=true
    LATEST=$(is_latest "$1")
elif [[ $1 =~ ^refs/tags/v[5-9]+\.[0-9]+\.[0-9]+-build\.[0-9]+$ ]]; then
    PROFILE=emqx
    RELEASE=true
    LATEST=$(is_latest "$1")
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    RELEASE=true
    LATEST=$(is_latest "$1")
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+-build\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    RELEASE=true
    LATEST=$(is_latest "$1")
elif [[ $1 =~ ^refs/tags/v[5-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    PROFILE=emqx
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
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
    PROFILE=emqx
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/release-[5-9][0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/ci/.* ]]; then
    PROFILE=emqx
    RELEASE=false
    LATEST=false
else
    echo "Unrecognized git ref: $1" 1>&2
    exit 1
fi

cat <<EOF
{"profile": "$PROFILE", "release": $RELEASE, "latest": $LATEST}
EOF
