#!/usr/bin/env bash

set -euo pipefail

usage() {
    cat <<'EOF'
Usage: effective-loc.sh [--with-tests] [--with-deps] [GIT_WORKTREE]

Report source lines of code using cloc. Count Git-tracked files by default.

Options:
  --with-tests       Include files below directories named "test".
  --with-deps        Include locally available dependencies below deps/.
  -h, --help         Show this help.

GIT_WORKTREE defaults to the current directory.
Git dependencies contribute tracked files. Unpacked dependencies contribute
files outside _build/ and ebin/. No dependencies are downloaded.
EOF
}

dependency_files() {
    local dep="$1"
    if [ -e "$dep/.git" ]; then
        git -C "$dep" -c core.quotepath=false ls-files --recurse-submodules |
            while IFS= read -r file; do
                [ -f "$dep/$file" ] || continue
                printf '%s/%s\n' "$dep" "$file"
            done
    else
        # Unpacked packages have no Git index. Ignore their build output.
        find -H "$dep" -type d \( -name .git -o -name _build -o -name ebin \) -prune \
            -o -type f -print
    fi
}

include_tests=false
include_deps=false
worktree='.'
worktree_set=false

while [ "$#" -gt 0 ]; do
    case "$1" in
        --with-tests)
            include_tests=true
            ;;
        --with-deps)
            include_deps=true
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

cloc_args=(--hide-rate)
exclude_dirs=''
test_mode='included'
if [ "$include_tests" = false ]; then
    exclude_dirs='test'
    test_mode='excluded'
fi
deps_mode='excluded'
if [ "$include_deps" = true ]; then
    exclude_dirs="${exclude_dirs:+$exclude_dirs,}_build,ebin"
    deps_mode='included'
fi
if [ -n "$exclude_dirs" ]; then
    cloc_args+=("--exclude-dir=$exclude_dirs")
fi

printf 'Repository: %s\n' "$repo_root"
printf 'Test code: %s (directories named "test")\n' "$test_mode"
printf 'Dependencies: %s (deps/)\n\n' "$deps_mode"

cd "$repo_root"
if [ "$include_deps" = false ]; then
    exec cloc --vcs=git "${cloc_args[@]}" .
fi

file_list=$(mktemp "${TMPDIR:-/tmp}/emqx-effective-loc.XXXXXX")
trap 'rm -f -- "$file_list"' EXIT
git -c core.quotepath=false ls-files > "$file_list"

for dep in deps/*; do
    [ -d "$dep" ] || continue
    dependency_files "$dep" >> "$file_list"
done

cloc "${cloc_args[@]}" --list-file="$file_list"
