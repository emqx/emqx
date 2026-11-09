#!/usr/bin/env bash

set -euo pipefail

# $1 is fully qualified git ref name, e.g. refs/tags/6.0.0 or refs/heads/master
#
# With --pick-latest instead, read tag names on stdin, one per line, and print
# the one that is the latest release under the rule below, or nothing. The
# set-latest-release workflow feeds it the tags of published GitHub releases.
#
# Environment:
#   LATEST_RELEASE_LINE  MAJOR.MINOR, e.g. 6.3. Only the highest stable tag of
#                        that line is the latest release. Required for a
#                        stable X.Y.Z tag and for --pick-latest. Set from the
#                        repository variable of the same name, so the latest
#                        line moves without a commit.

# Read tag names on stdin and print the highest stable one (X.Y.Z, no suffix)
# whose name starts with the given prefix, e.g. '6.3.' or '6.' or '' for any.
pick_highest_stable() {
  local prefix="$1" tag
  while IFS= read -r tag || [[ -n "$tag" ]]; do
    if [[ "$tag" =~ ^[1-9][0-9]*\.[0-9]+\.[0-9]+$ && "$tag" == "$prefix"* ]]; then
      echo "$tag"
    fi
  done | sort -V | tail -n1
}

highest_stable_tag() {
  git tag --list | pick_highest_stable "$1"
}

# Print true when the tag in ref $1 is the highest stable tag with prefix $2.
is_highest_stable() {
  local ref_name prefix highest
  ref_name="$(basename "$1")"
  prefix="$2"
  highest="$(highest_stable_tag "$prefix")"
  if [[ -n "$highest" && "$ref_name" == "$highest" ]]; then
    echo true
  else
    echo false
  fi
}

# Print the tag prefix of the latest line: '6.3.' for LATEST_RELEASE_LINE=6.3.
latest_line_prefix() {
  local line="${LATEST_RELEASE_LINE:-}"
  if [[ "$line" =~ ^[1-9][0-9]*\.[0-9]+$ ]]; then
    echo "${line}."
  else
    echo "LATEST_RELEASE_LINE must be MAJOR.MINOR, e.g. 6.3, got: '$line'" 1>&2
    exit 1
  fi
}

# The release that gets the 'latest' docker tag, the public broker update and
# the GitHub 'Latest' release.
is_latest_stable() {
  local prefix
  prefix="$(latest_line_prefix)" || exit 1
  is_highest_stable "$1" "$prefix"
}

# The release that gets the 'MAJOR' docker tag: the highest stable tag of its
# major version, independent of LATEST_RELEASE_LINE.
is_latest_in_major() {
  local ref_name
  ref_name="$(basename "$1")"
  is_highest_stable "$1" "${ref_name%%.*}."
}

if [[ "${1:-}" == "--pick-latest" ]]; then
  prefix="$(latest_line_prefix)" || exit 1
  pick_highest_stable "$prefix"
  exit 0
fi

PROFILE=emqx-enterprise
OFFICIAL_RELEASE=false
# A preview tag (X.Y.Z-preview.N, cut on a preview-* branch) builds and
# uploads packages, but must not reach GA channels or the release docs.
PREVIEW=false
# Only a stable X.Y.Z tag can be the latest of its major version.
LATEST_MAJOR=false

if [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+-M[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+-(alpha|beta|rc)\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+$ ]]; then
    RELEASE=true
    OFFICIAL_RELEASE=true
    LATEST=$(is_latest_stable "$1")
    LATEST_MAJOR=$(is_latest_in_major "$1")
elif [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+-patch\.[0-9]+$ ]]; then
    RELEASE=true
    OFFICIAL_RELEASE=true
    LATEST=false
elif [[ $1 =~ ^refs/tags/[6-9]\.[0-9]+\.[0-9]+-preview\.[0-9]+$ ]]; then
    RELEASE=true
    LATEST=false
    PREVIEW=true
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
{"profile": "$PROFILE", "release": $RELEASE, "latest": $LATEST, "latest_major": $LATEST_MAJOR, "official_release": $OFFICIAL_RELEASE, "preview": $PREVIEW}
EOF
