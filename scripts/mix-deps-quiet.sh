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

set -euo pipefail

PATTERN='^(remote: (Enumerating|Counting|Compressing|Total) |Enumerating objects:|Counting objects:|Compressing objects:|Receiving objects:|Resolving deltas:|Updating files:|Checking out files:)'

exec "$@" > >(grep --line-buffered -Ev "$PATTERN") 2>&1
