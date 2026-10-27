#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: effective-loc.sh [--with-tests] [GIT_WORKTREE]

Report source lines of code in Git-tracked files using cloc.

Options:
  --with-tests       Include files below directories named "test".
  -h, --help         Show this help.

GIT_WORKTREE defaults to the current directory.
EOF
}

include_tests=false
worktree='.'
worktree_set=false

while [ "$#" -gt 0 ]; do
    case "$1" in
        --with-tests)
            include_tests=true
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        --)
            shift
            if [ "$#" -gt 1 ]; then
                echo "ERROR: expected at most one Git worktree" >&2
                usage >&2
                exit 2
            fi
            if [ "$#" -eq 1 ]; then
                worktree="$1"
                worktree_set=true
            fi
            break
            ;;
        -*)
            echo "ERROR: unknown option: $1" >&2
            usage >&2
            exit 2
            ;;
        *)
            if [ "$worktree_set" = true ]; then
                echo "ERROR: expected at most one Git worktree" >&2
                usage >&2
                exit 2
            fi
            worktree="$1"
            worktree_set=true
            ;;
    esac
    shift
done

if ! command -v git >/dev/null 2>&1; then
    echo 'ERROR: git is required' >&2
    exit 1
fi

if ! command -v cloc >/dev/null 2>&1; then
    echo 'ERROR: cloc is required' >&2
    exit 1
fi

if ! repo_root="$(git -C "$worktree" rev-parse --show-toplevel 2>/dev/null)"; then
    echo "ERROR: not a Git worktree: $worktree" >&2
    exit 1
fi

cloc_args=(--vcs=git --hide-rate)
test_mode='included'
if [ "$include_tests" = false ]; then
    cloc_args+=(--exclude-dir=test)
    test_mode='excluded'
fi

printf 'Repository: %s\n' "$repo_root"
printf 'Test code: %s (directories named "test")\n\n' "$test_mode"

cd "$repo_root"
exec cloc "${cloc_args[@]}" .
