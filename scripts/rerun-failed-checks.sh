#!/usr/bin/env bash

# Get the list of open pull requests authored by you
prs=$(gh pr list --state open --author "${1:-@me}" --json number,title,url,createdAt --jq '.[] | "#\(.number) \(.title) (\(.createdAt | fromdateiso8601 | strftime("%Y-%m-%d")))"')

# If there are no open PRs, exit
if [ -z "$prs" ]; then
    echo "No open pull requests found."
    exit 0
fi

# Use fzf to select the PR
selected_pr=$(echo "$prs" | fzf --prompt="Select PR: " --height 15 --border --ansi)

if [ -z "$selected_pr" ]; then
    echo "No PR selected."
    exit 0
fi

# Extract the PR number from the selected PR
pr_number=$(echo "$selected_pr" | awk '{print $1}')

# Get the commit SHA associated with the selected PR
commit_sha=$(gh pr view "$pr_number" --json headRefOid --jq '.headRefOid')

# Get the run ID of the failed PR Entrypoint check
failed_pr_run=$(gh run list --commit "$commit_sha" --workflow "PR Entrypoint" --json databaseId,status,conclusion --jq '.[] | select(.status == "completed" and (.conclusion == "failure" or .conclusion == "cancelled")) | .databaseId')

# If there are no failed runs, exit
if [ -z "$failed_pr_run" ]; then
    echo "No failed runs found for PR $pr_number."
    exit 0
fi

# Rerun all failed checks for the selected run
echo "Rerunning failed checks for run ID: $failed_pr_run"
gh run rerun "$failed_pr_run" --failed

echo "Failed checks have been rerun for PR $pr_number, run ID $failed_pr_run."
