#!/bin/bash

set -euo pipefail

# The script requires a PR number, a Gemini API key, and GitHub authentication.
# It can get them from environment variables or command-line arguments.

PR_NUMBER="${1:-${GITHUB_PULL_REQUEST_NUMBER}}"

if [ -z "${PR_NUMBER}" ]; then
  echo "Error: Pull Request number is not set."
  echo "Usage: $0 <pr_number>"
  exit 1
fi

if [ -z "${GEMINI_API_KEY}" ]; then
  echo "Error: GEMINI_API_KEY is not set."
  exit 1
fi

if [ -n "${GITHUB_RUN_ID:-}" ]; then
  git config --global user.name "github-actions[bot]"
  git config --global user.email "github-actions[bot]@users.noreply.github.com"
fi

UPSTREAM_REMOTE=$(git remote -v | grep -E 'github.com[:/]emqx/emqx' | awk '{print $1}' | uniq | head -n 1)

if [ -z "${UPSTREAM_REMOTE}" ]; then
  echo "Error: Could not find a git remote pointing to the 'emqx/emqx' repository."
  echo "Please add it, for example: git remote add upstream https://github.com/emqx/emqx.git"
  exit 1
fi

PULL_REFSPEC="+refs/pull/*:refs/remotes/${UPSTREAM_REMOTE}/pull/*"
if ! git config --get-all "remote.${UPSTREAM_REMOTE}.fetch" | grep -q -F "${PULL_REFSPEC}"; then
  echo "Adding fetch refspec for pull requests to '${UPSTREAM_REMOTE}' remote in local git config."
  git config --add "remote.${UPSTREAM_REMOTE}.fetch" "${PULL_REFSPEC}"
fi

echo "Fetching PR details from ${UPSTREAM_REMOTE}..."
git fetch "${UPSTREAM_REMOTE}"

# Get the base and head commit SHAs from the PR details.
BASE_SHA=$(git rev-parse "${UPSTREAM_REMOTE}/pull/${PR_NUMBER}/merge^1")
HEAD_SHA=$(git rev-parse "${UPSTREAM_REMOTE}/pull/${PR_NUMBER}/head")

echo "Base SHA: ${BASE_SHA}"
echo "Head SHA: ${HEAD_SHA}"

echo "Generating diff..."
DIFF_CONTENT=$(git diff "${BASE_SHA}" "${HEAD_SHA}")

if [ -z "${DIFF_CONTENT}" ]; then
  echo "Warning: Diff is empty. No changelog will be generated."
  exit 0
fi

echo "Calling Gemini API..."

PROMPT="Based on the following git diff, classify the change as a feature ('feat'), a fix ('fix'), or a performance improvement ('perf'). Then, write a VERY COMPACT changelog entry.

Respond ONLY with a valid JSON object containing two keys:
1. \"prefix\": One of \"feat\", \"fix\", or \"perf\".
2. \"summary\": The compact, two-sentence changelog summary.

Do not include markdown formatting or any text outside of the JSON object.

Diff:
\`\`\`diff
${DIFF_CONTENT}
\`\`\`"

JSON_PAYLOAD=$(jq -n \
  --arg prompt_text "$PROMPT" \
  '{contents: [{parts: [{text: $prompt_text}]}]}')

API_RESPONSE_TEXT=$(curl -s -f -H 'Content-Type: application/json' \
  -d "$JSON_PAYLOAD" \
  "https://generativelanguage.googleapis.com/v1beta/models/gemini-2.5-flash:generateContent?key=${GEMINI_API_KEY}" \
  | jq -r '.candidates[0].content.parts[0].text')

if [[ -z "${API_RESPONSE_TEXT}" ]] || [[ "${API_RESPONSE_TEXT}" == "null" ]]; then
  echo "Error: Failed to get a valid response from the Gemini API."
  exit 1
fi

echo "Processing API response..."
PREFIX=$(echo "$API_RESPONSE_TEXT" | jq -r '.prefix')
SUMMARY=$(echo "$API_RESPONSE_TEXT" | jq -r '.summary')

if [[ -z "${PREFIX}" ]] || [[ "${PREFIX}" == "null" ]] || [[ -z "${SUMMARY}" ]] || [[ "${SUMMARY}" == "null" ]]; then
    echo "Error: API response did not contain a valid prefix or summary."
    echo "Received: ${API_RESPONSE_TEXT}"
    exit 1
fi

FILE_PATH="changes/ee/${PREFIX}-${PR_NUMBER}.en.md"

echo "${SUMMARY}" > "$FILE_PATH"

echo "Changelog is saved to ${FILE_PATH}"

# The GITHUB_HEAD_REF is the name of the branch in the fork (e.g., "my-feature-branch").
# This is available in the GitHub Actions environment.
if [ -n "${GITHUB_HEAD_REF:-}" ]; then
  echo "Committing changelog file..."
  git checkout "${HEAD_SHA}"
  git add "$FILE_PATH"
  git commit -m "docs: Add changelog for PR #${PR_NUMBER}"
  echo "Pushing changes to ${GITHUB_HEAD_REF}..."
  git push origin "HEAD:refs/heads/${GITHUB_HEAD_REF}"
fi
