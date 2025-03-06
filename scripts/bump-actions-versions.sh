#!/usr/bin/env bash

set -euo pipefail

actions=( 'actions/checkout' 'actions/cache' 'actions/stale' 'actions/upload-artifact' 'actions/download-artifact' 'aws-actions/configure-aws-credentials' 'ossf/scorecard-action' 'erlef/setup-beam' 'slackapi/slack-github-action' 'hashicorp/setup-terraform' 'docker/login-action' 'docker/setup-buildx-action' 'docker/setup-qemu-action' 'actions/setup-java' 'peter-evans/repository-dispatch' 'rtCamp/action-slack-notify' )
for a in "${actions[@]}"; do
    # shellcheck disable=SC2086
    TAG=$(curl -sSfL -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/repos/$a/releases/latest | jq -r '.tag_name')
    # shellcheck disable=SC2086
    TAG_OBJECT=$(curl -sSfL -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/repos/$a/git/ref/tags/$TAG)
    if [ "$(echo "${TAG_OBJECT}" | jq -r '.object.type')" = "commit" ]; then
        COMMIT_SHA=$(echo "${TAG_OBJECT}" | jq -r '.object.sha')
    else
        TAG_SHA=$(echo "${TAG_OBJECT}" | jq -r '.object.sha')
        # shellcheck disable=SC2086
        COMMIT_SHA=$(curl -sSfL -H "Authorization: token $GITHUB_TOKEN" https://api.github.com/repos/$a/git/tags/$TAG_SHA | jq -r '.object.sha')
    fi
    echo "Bumping $a to $TAG ($COMMIT_SHA)"
    sed -i.bak -e "s|uses: $a.*$|uses: $a@$COMMIT_SHA # $TAG|g" .github/workflows/*.yaml
    sed -i.bak -e "s|uses: $a.*$|uses: $a@$COMMIT_SHA # $TAG|g" .github/actions/*/*.yaml
    rm .github/workflows/*.bak
    rm .github/actions/*/*.bak
done
