#!/bin/bash
set -euo pipefail

error_flag=false

# ensure dir
cd -P -- "$(dirname -- "$0")/.."

git_diff(){
    tag=$(git tag -l "v*"| tail -n1)

    if ! git describe --tags --match "$tag" --exact >/dev/null 2>&1; then
        git diff --name-only $tag...HEAD
    else
        pre_tag="$(git tag -l "v*"| tail -n2 | sed -n '1p')"
        git diff --name-only $pre_tag...$tag
    fi
}

for app in $(ls apps); do
    if [ -z "$(git_diff | egrep "^apps/$app/.*" | egrep -v test )" ];then
        continue
    fi
    if [ -z "$(git_diff | egrep "^apps/$app/src/$app.app.src" )" ];then
        echo "apps/$app/src/$app.app.src version should be updated"
        error_flag=true
    fi
done

if test $error_flag == true; then
    exit 1
fi
