##!/usr/bin/env bash
set -exuo pipefail

img_amd64=$1
push_latest=${2:-false}

img_arm64=$(echo ${img_amd64} | sed 's/-amd64$/-arm64/g')
img_name=${img_amd64%-amd64}
docker pull "$img_amd64"
docker pull --platform linux/arm64 "$img_arm64"
img_amd64_digest=$(docker inspect --format='{{index .RepoDigests 0}}' "$img_amd64")
img_arm64_digest=$(docker inspect --format='{{index .RepoDigests 0}}' "$img_arm64")
echo "sha256 of amd64 is $img_amd64_digest"
echo "sha256 of arm64 is $img_arm64_digest"
docker manifest create "${img_name}" \
    --amend "$img_amd64_digest" \
    --amend "$img_arm64_digest"
docker manifest push "${img_name}"

# PUSH latest if it is a release build
if [ "$push_latest" = "true" ]; then
    img_latest=$(echo "$img_arm64" | cut -d: -f 1):latest
    docker manifest create "${img_latest}" \
        --amend "$img_amd64_digest" \
        --amend "$img_arm64_digest"
    docker manifest push "${img_latest}"
fi
