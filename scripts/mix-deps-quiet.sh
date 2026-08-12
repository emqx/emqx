#!/usr/bin/env bash

# Runs the given command (typically `mix deps.get`) while stripping
# git-clone progress noise from the output. Non-progress lines (real
# errors, dependency info) pass through untouched.
# Exit status is the wrapped command's.
#
# Motivation: Mix merges git's stderr into stdout (stderr_to_stdout),
# so progress lines like "remote: Counting objects: 7% (11/157)" end
# up on stdout. In CI (GitHub Actions) each \r-delimited update becomes
# a separate log line, spamming the build log. MIX_QUIET=1 would hide
# them but also hides everything else Mix prints, so we filter narrowly.
#
# Retries: transient network/SSL errors (e.g. self-signed certificate on
# macOS 26 runners) can cause a single git-fetch to fail even when all
# other fetches in the same run succeed.  Retrying the whole `mix deps.get`
# recovers from these without any manual re-run.

set -euo pipefail

PATTERN='^(remote: (Enumerating|Counting|Compressing|Total) |Enumerating objects:|Counting objects:|Compressing objects:|Receiving objects:|Resolving deltas:|Updating files:|Checking out files:)'

max_attempts=3
attempt=0
while true; do
  attempt=$((attempt + 1))
  set +e
  "$@" 2>&1 | grep --line-buffered -Ev "$PATTERN"
  exit_code=${PIPESTATUS[0]}
  set -e
  if [[ $exit_code -eq 0 ]]; then
    exit 0
  fi
  if [[ $attempt -ge $max_attempts ]]; then
    exit "$exit_code"
  fi
  echo "mix deps.get failed (attempt $attempt/$max_attempts), retrying in $((attempt * 10))s..." >&2
  sleep $((attempt * 10))
done
