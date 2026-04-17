#!/usr/bin/env bash

## Build per-arch refs from saved tarballs, push them to a local registry,
## then assemble a multi-arch manifest at docker.io.
##
## Usage:
##   publish-docker-multi-arch.sh \
##       --amd64 <tarball> --arm64 <tarball> \
##       --owner <github-owner> --profile <emqx-profile> \
##       --variant base|sf \
##       [--latest] [--publish]
##
## Without --publish the buildx command is printed instead of executed
## (the load/tag/push to the local registry still happen).
##
## $LOCAL_BASE may be set in the environment to override the staging-registry
## prefix (default: localhost:5000/${owner}/${profile}, matching the
## registry:2 sidecar in the workflow).
##
## For unit-testing the tag policy:
##   publish-docker-multi-arch.sh --print-tags \
##       --variant base|sf --owner <owner> --profile <profile> \
##       --version <version> [--latest]
## In --print-tags mode no docker commands are run; the script only echoes
## the tag set that would be passed to `docker buildx imagetools create`.

set -euo pipefail

AMD64_TARBALL=
ARM64_TARBALL=
OWNER=
PROFILE=
VARIANT=
LATEST=false
PUBLISH=false
PRINT_TAGS_ONLY=false
VERSION_OVERRIDE=

while [ $# -gt 0 ]; do
    case "$1" in
        --amd64)       AMD64_TARBALL=$2; shift 2;;
        --arm64)       ARM64_TARBALL=$2; shift 2;;
        --owner)       OWNER=$2; shift 2;;
        --profile)     PROFILE=$2; shift 2;;
        --variant)     VARIANT=$2; shift 2;;
        --latest)      LATEST=true; shift;;
        --publish)     PUBLISH=true; shift;;
        --print-tags)  PRINT_TAGS_ONLY=true; shift;;
        --version)     VERSION_OVERRIDE=$2; shift 2;;
        *) echo "error: unknown argument: $1" >&2; exit 1;;
    esac
done

for required in OWNER PROFILE VARIANT; do
    if [ -z "${!required}" ]; then
        echo "error: --${required,,} is required" >&2
        exit 1
    fi
done

case "$VARIANT" in
    base|sf) ;;
    *) echo "error: --variant must be 'base' or 'sf', got '$VARIANT'" >&2; exit 1;;
esac

# Pure function: emits one tag per line.
# Tag policy:
#   - Always: ${owner}/${profile}:${version}
#   - base also: emqx/emqx:${version}
#   - For stable MAJOR.MINOR.PATCH on base:
#       additionally :MAJOR.MINOR (always — patch follows its line)
#       additionally :MAJOR and :latest (only when latest=true)
#   - For sf: full version only, no mirror, no rolling.
compute_tags() {
    local version=$1 variant=$2 owner=$3 profile=$4 latest=$5

    echo "docker.io/${owner}/${profile}:${version}"

    if [ "$variant" = "sf" ]; then
        return
    fi

    echo "docker.io/emqx/emqx:${version}"

    # Rolling tags only for stable MAJOR.MINOR.PATCH versions.
    # Anything else (prereleases, git-hash builds, build metadata) gets
    # only the full-version tag.
    if [[ "$version" =~ ^([0-9]+)\.([0-9]+)\.[0-9]+$ ]]; then
        local major=${BASH_REMATCH[1]}
        local minor=${BASH_REMATCH[1]}.${BASH_REMATCH[2]}

        echo "docker.io/${owner}/${profile}:${minor}"
        echo "docker.io/emqx/emqx:${minor}"

        if [ "$latest" = "true" ]; then
            echo "docker.io/${owner}/${profile}:${major}"
            echo "docker.io/emqx/emqx:${major}"
            echo "docker.io/${owner}/${profile}:latest"
            echo "docker.io/emqx/emqx:latest"
        fi
    fi
}

if [ "$PRINT_TAGS_ONLY" = "true" ]; then
    if [ -z "$VERSION_OVERRIDE" ]; then
        echo "error: --print-tags requires --version" >&2
        exit 1
    fi
    compute_tags "$VERSION_OVERRIDE" "$VARIANT" "$OWNER" "$PROFILE" "$LATEST"
    exit 0
fi

if [ -z "$AMD64_TARBALL" ] || [ -z "$ARM64_TARBALL" ]; then
    echo "error: --amd64 and --arm64 are required" >&2
    exit 1
fi

LOCAL_BASE="${LOCAL_BASE:-localhost:5000/${OWNER}/${PROFILE}}"

AMD64_LOADED=$(docker load < "$AMD64_TARBALL" | grep "Loaded image:" | sed 's/Loaded image: //')
echo "Loaded amd64 image: $AMD64_LOADED"
VERSION="${AMD64_LOADED##*:}"
AMD64_REF="${LOCAL_BASE}:${VERSION}-amd64"
docker tag "$AMD64_LOADED" "$AMD64_REF"
docker push "$AMD64_REF"

ARM64_LOADED=$(docker load < "$ARM64_TARBALL" | grep "Loaded image:" | sed 's/Loaded image: //')
echo "Loaded arm64 image: $ARM64_LOADED"
ARM64_VERSION="${ARM64_LOADED##*:}"
ARM64_REF="${LOCAL_BASE}:${ARM64_VERSION}-arm64"
docker tag "$ARM64_LOADED" "$ARM64_REF"
docker push "$ARM64_REF"

if [ "$VERSION" != "$ARM64_VERSION" ]; then
    echo "error: amd64 version ($VERSION) does not match arm64 version ($ARM64_VERSION)" >&2
    exit 1
fi

mapfile -t TAGS < <(compute_tags "$VERSION" "$VARIANT" "$OWNER" "$PROFILE" "$LATEST")

echo "Tags (publish=${PUBLISH}):"
printf '  - %s\n' "${TAGS[@]}"
if [ -n "${GITHUB_STEP_SUMMARY:-}" ]; then
    {
        echo "### Multi-arch manifest tags (variant=${VARIANT}, publish=${PUBLISH})"
        printf -- '- `%s`\n' "${TAGS[@]}"
    } >> "$GITHUB_STEP_SUMMARY"
fi

TAG_ARGS=()
for t in "${TAGS[@]}"; do TAG_ARGS+=(--tag "$t"); done

CMD=(docker buildx imagetools create "${TAG_ARGS[@]}" "$AMD64_REF" "$ARM64_REF")
if [ "$PUBLISH" = "true" ]; then
    "${CMD[@]}"
else
    echo "dry-run: ${CMD[*]}"
fi
