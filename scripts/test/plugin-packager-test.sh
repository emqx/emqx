#!/usr/bin/env bash

## Build a synthetic plugin with `mix emqx.plugin` and check which
## applications its package bundles:
## - the plugin's own application: bundled;
## - a dependency that the EMQX release does not provide: bundled;
## - a dependency that the EMQX release provides (jose): not bundled.
##
## Run it from the repository root after `make emqx-enterprise-compile`.

set -euo pipefail

ROOT_DIR="$(cd -P -- "$(dirname -- "$0")/../.." && pwd)"
cd "$ROOT_DIR"

PROFILE="${PROFILE:-emqx-enterprise}"
PLUGIN=zz_packager_test
PLUGIN_DIR="plugins/${PLUGIN}"
EXT_DEP=zz_packager_test_ext_dep
PACKAGE="_build/plugins/${PLUGIN}-0.1.0.tar.gz"

cleanup() {
    rm -rf "$PLUGIN_DIR" \
        "_build/${PROFILE}/lib/${PLUGIN}" \
        "_build/${PROFILE}/lib/${EXT_DEP}" \
        "_build/plugins/${PLUGIN}-0.1.0" \
        "$PACKAGE" \
        "_build/plugins/${PLUGIN}-0.1.0.sha256"
}
trap cleanup EXIT
cleanup

mkdir -p "${PLUGIN_DIR}/src" "${PLUGIN_DIR}/${EXT_DEP}/src"

cat > "${PLUGIN_DIR}/mix.exs" <<'EOF'
defmodule ZzPackagerTest.MixProject do
  use Mix.Project

  def project do
    [
      app: :zz_packager_test,
      version: "0.1.0",
      emqx_plugin: [rel_vsn: "0.1.0", metadata: [description: "packager test"]],
      build_path: "../../_build",
      deps_path: "../../deps",
      lockfile: "../../mix.lock",
      deps: [
        {:emqx_mix, path: "../..", env: :"emqx-enterprise", runtime: false},
        {:jose, github: "potatosalad/erlang-jose", tag: "1.11.12", manager: :rebar3, override: true},
        {:zz_packager_test_ext_dep, path: "zz_packager_test_ext_dep"}
      ]
    ]
  end

  def application do
    [extra_applications: []]
  end
end
EOF

cat > "${PLUGIN_DIR}/src/${PLUGIN}.erl" <<EOF
-module(${PLUGIN}).
EOF

cat > "${PLUGIN_DIR}/${EXT_DEP}/mix.exs" <<'EOF'
defmodule ZzPackagerTestExtDep.MixProject do
  use Mix.Project

  def project do
    [app: :zz_packager_test_ext_dep, version: "0.1.0", deps: []]
  end
end
EOF

cat > "${PLUGIN_DIR}/${EXT_DEP}/src/${EXT_DEP}.erl" <<EOF
-module(${EXT_DEP}).
EOF

make "plugin-${PLUGIN}" PROFILE="$PROFILE"

APPS="$(tar -tzf "$PACKAGE" | awk -F/ 'NF > 2 && $3 == "ebin" { print $2 }' | sort -u)"
echo "Bundled applications:"
echo "$APPS"

fail=0
for app in "${PLUGIN}-0.1.0" "${EXT_DEP}-0.1.0"; do
    if ! grep -qx "$app" <<< "$APPS"; then
        echo "FAILED: ${app} is not in the package"
        fail=1
    fi
done
if grep -q '^jose-' <<< "$APPS"; then
    echo "FAILED: jose, which the EMQX release provides, is in the package"
    fail=1
fi
if [ "$fail" -ne 0 ]; then
    exit 1
fi
echo "OK"
