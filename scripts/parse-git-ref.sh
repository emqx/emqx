#!/usr/bin/env bash

set -euo pipefail

# $1 is fully qualified git ref name, e.g. refs/tags/v5.1.0 or refs/heads/master

if [[ $1 =~ ^refs/tags/v[5-9]+\.[0-9]+\.[0-9]+$ ]]; then
    PROFILE=emqx
    EDITION=Opensource
    RELEASE=true
    LATEST=true
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    EDITION=Enterprise
    RELEASE=true
    LATEST=true
elif [[ $1 =~ ^refs/tags/v[5-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    PROFILE=emqx
    EDITION=Opensource
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/e[5-9]+\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    EDITION=Enterprise
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/.+ ]]; then
    echo "Unrecognized tag: $1"
    exit 1
elif [[ $1 =~ ^refs/heads/master$ ]]; then
    PROFILE=emqx
    EDITION=Opensource
    RELEASE=false
    LATEST=false
elif [[ $1 =~ ^refs/heads/release-[5-9][0-9]+$ ]]; then
    PROFILE=emqx-enterprise
    EDITION=Enterprise
    RELEASE=false
    LATEST=false
else
    echo "Unrecognized git ref: $1"
    exit 1
fi

cat <<EOF
{"profile": "$PROFILE", "edition": "$EDITION", "release": $RELEASE, "latest": $LATEST}
EOF
