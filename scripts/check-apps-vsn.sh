#!/usr/bin/env bash
set -euo pipefail

## compare to the latest release version tag:
##   match rel-e4.4.0, v4.4.*, or e4.4.* tags
##   but do not include alpha, beta and rc versions
##
## NOTE: 'rel-' tags are the merge base of enterprise release in opensource repo.
## i.e. if we are to release a new enterprise without cutting a new opensource release
## we should tag rel-e4.4.X in the opensource repo, and merge this tag to enterprise
## then cut a release from the enterprise repo.
latest_release="$(git describe --abbrev=0 --tags --match 'rel-e4.4.*' --match '[v|e]4.4*' --exclude '*beta*' --exclude '*alpha*' --exclude '*rc*' --exclude '*gocsp*')"
echo "Compare base: $latest_release"

bad_app_count=0

no_comment_re='(^[^\s?%])'
## TODO: c source code comments re (in $app_path/c_src dirs)

parse_semver() {
    echo "$1" | tr '.|-' ' '
}

exempt_bump() {
  local app="$1"
  local from="$2"
  local to="$3"

  case "$app,$from,$to" in
    "lib-ee/emqx_conf,4.3.9,4.4.0")
      true
      ;;
    "lib-ee/emqx_license,4.3.7,4.4.0")
      true
      ;;
    *)
      false
      ;;
  esac
}

check_apps() {
  while read -r app; do
    if [ "$app" != "emqx" ]; then
        app_path="$app"
    else
        app_path="."
    fi
    src_file="$app_path/src/$(basename "$app").app.src"

    old_app_exists=0
    git show "$latest_release":"$src_file" >/dev/null 2>&1 || old_app_exists="$?"
    if [ "$old_app_exists" != "0" ]; then
        echo "$app is new, skipping version check"
        continue
    fi

    old_app_version="$(git show "$latest_release":"$src_file" | grep vsn | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+"' | tr -d '"')"
    now_app_version=$(grep -E 'vsn' "$src_file" | grep -oE '"[0-9]+\.[0-9]+\.[0-9]+"' | tr -d '"')
    if [ "$old_app_version" = "$now_app_version" ]; then
        changed_lines="$(git diff "$latest_release"...HEAD --ignore-blank-lines -G "$no_comment_re" \
                             -- "$app_path/src" \
                             -- "$app_path/include" \
                             -- ":(exclude)$app_path/src/*.appup.src" \
                             -- "$app_path/priv" \
                             -- "$app_path/c_src" | wc -l ) "
        if [ "$changed_lines" -gt 0 ]; then
            echo "$src_file needs a vsn bump (old=$old_app_version)"
            echo "changed: $changed_lines"
            bad_app_count=$(( bad_app_count + 1))
        elif [[ ${app_path} = *emqx_dashboard* ]]; then
            ## emqx_dashboard is ensured to be upgraded after all other plugins
            ## at the end of its appup instructions, there is the final instruction
            ## {apply, {emqx_plugins, load, []}
            ## since we don't know which plugins are stopped during the upgrade
            ## for safty, we just force a dashboard version bump for each and every release
            ## even if there is nothing changed in the app
            echo "$src_file needs a vsn bump to ensure plugins loaded after upgrade"
            bad_app_count=$(( bad_app_count + 1))
        fi
    else
        # shellcheck disable=SC2207
        old_app_version_semver=($(parse_semver "$old_app_version"))
        # shellcheck disable=SC2207
        now_app_version_semver=($(parse_semver "$now_app_version"))
        if  [ "${old_app_version_semver[0]}" = "${now_app_version_semver[0]}" ] && \
            [ "${old_app_version_semver[1]}" = "${now_app_version_semver[1]}" ] && \
            [ "$(( old_app_version_semver[2] + 1 ))" = "${now_app_version_semver[2]}" ]; then
            true
        else
          if exempt_bump "$app" "$old_app_version" "$now_app_version"; then
            true
          else
            echo "$src_file: non-strict semver version bump from $old_app_version to $now_app_version"
            bad_app_count=$(( bad_app_count + 1))
          fi
        fi
    fi
  done < <(./scripts/find-apps.sh)

  if [ $bad_app_count -gt 0 ]; then
      exit 1
  else
      echo "apps version check successfully"
  fi
}

_main() {
    if echo "${latest_release}" |grep -oE '[0-9]+.[0-9]+.[0-9]+' > /dev/null 2>&1; then
        check_apps
    else
        echo "skiped unstable tag: ${latest_release}"
    fi
}

_main
