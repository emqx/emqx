#!/usr/bin/env bash

## Assemble and push a multi-arch docker manifest from per-architecture source images.
##
## Usage:
##   docker-publish-manifest.sh \
##     --source <per-arch-image-ref> \
##     [--source <per-arch-image-ref> ...] \
##     --tag <destination-tag> \
##     [--tag <destination-tag> ...] \
##     [--dry-run]
##
## The script is transport-agnostic: source refs can point at any registry
## (a local registry:2 sidecar, ghcr.io staging, etc.) as long as they are
## reachable by `docker buildx imagetools create` from the current shell.

set -euo pipefail

SOURCES=()
TAGS=()
DRY_RUN=false

while [ $# -gt 0 ]; do
    case "$1" in
        --source)
            SOURCES+=("$2")
            shift 2
            ;;
        --tag)
            TAGS+=("$2")
            shift 2
            ;;
        --dry-run)
            DRY_RUN=true
            shift
            ;;
        *)
            echo "error: unknown argument: $1" >&2
            exit 1
            ;;
    esac
done

if [ "${#SOURCES[@]}" -eq 0 ]; then
    echo "error: at least one --source is required" >&2
    exit 1
fi

if [ "${#TAGS[@]}" -eq 0 ]; then
    echo "error: at least one --tag is required" >&2
    exit 1
fi

CMD=(docker buildx imagetools create)
for t in "${TAGS[@]}"; do
    CMD+=(--tag "$t")
done
for s in "${SOURCES[@]}"; do
    CMD+=("$s")
done

if [ "$DRY_RUN" = true ]; then
    echo "${CMD[*]}"
    exit 0
fi

"${CMD[@]}"
