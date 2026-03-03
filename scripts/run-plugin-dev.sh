#!/usr/bin/env bash
set -euo pipefail

usage() {
    cat <<'EOF'
Usage:
  scripts/run-plugin-dev.sh <plugin_app> [--attach]

Example:
  scripts/run-plugin-dev.sh emqx_username_quota --attach

What it does (recommended flow):
1) Build plugin package with: make plugin-<plugin_app>
2) Start EMQX release node if needed
3) Copy plugin .tar.gz to release plugins install_dir
4) Run:
   - emqx ctl plugins install <name-vsn>
   - emqx ctl plugins enable <name-vsn>
   - emqx ctl plugins start <name-vsn>
5) Optionally attach remote console
EOF
}

if [[ $# -lt 1 ]]; then
    usage
    exit 1
fi

APP="$1"
shift || true

ATTACH=0
while [[ $# -gt 0 ]]; do
    case "$1" in
        --attach)
            ATTACH=1
            shift
            ;;
        -h|--help)
            usage
            exit 0
            ;;
        *)
            echo "Unknown option: $1" >&2
            usage
            exit 1
            ;;
    esac
done

if [[ ! "$APP" =~ ^[a-z][a-z0-9_]*$ ]]; then
    echo "Invalid app name: $APP" >&2
    exit 1
fi

ROOT_DIR="$(cd "$(dirname "$0")/.." && pwd)"
PROFILE="${PROFILE:-emqx-enterprise}"
EMQX_BIN="$ROOT_DIR/_build/$PROFILE/rel/emqx/bin/emqx"
REL_DIR="$ROOT_DIR/_build/$PROFILE/rel/emqx"
PLUGIN_PKG_DIR="$ROOT_DIR/_build/plugins"

canon_path() {
    local p="$1"
    if command -v readlink >/dev/null 2>&1; then
        readlink -f "$p"
    else
        (cd "$p" && pwd -P)
    fi
}

if [[ ! -x "$EMQX_BIN" ]]; then
    echo "EMQX release binary not found: $EMQX_BIN" >&2
    echo "Run: make" >&2
    exit 1
fi

echo "Building plugin package: $APP"
make -C "$ROOT_DIR" "PROFILE=$PROFILE" "plugin-$APP"

shopt -s nullglob
plugin_pkgs=("$PLUGIN_PKG_DIR/$APP"-*.tar.gz)
shopt -u nullglob

TAR_PATH=""
for pkg in "${plugin_pkgs[@]}"; do
    if [[ -z "$TAR_PATH" || "$pkg" -nt "$TAR_PATH" ]]; then
        TAR_PATH="$pkg"
    fi
done
if [[ -z "$TAR_PATH" ]]; then
    echo "Cannot find plugin tarball for $APP under $PLUGIN_PKG_DIR" >&2
    exit 1
fi

NAME_VSN="$(basename "$TAR_PATH" .tar.gz)"

if ! "$EMQX_BIN" ping >/dev/null 2>&1; then
    echo "Starting EMQX node..."
    "$EMQX_BIN" start
fi

for _ in $(seq 1 30); do
    if "$EMQX_BIN" ping >/dev/null 2>&1; then
        break
    fi
    sleep 1
done

if ! "$EMQX_BIN" ping >/dev/null 2>&1; then
    echo "EMQX node did not become ready in time" >&2
    exit 1
fi

NODE_ROOT_DIR_RAW="$("$EMQX_BIN" eval 'io:format("~s~n", [code:root_dir()]).')"
NODE_ROOT_DIR="$(printf '%s\n' "$NODE_ROOT_DIR_RAW" | sed -n '1p')"
if [[ -z "$NODE_ROOT_DIR" ]]; then
    echo "Failed to resolve running node root_dir" >&2
    exit 1
fi

REL_DIR_CANON="$(canon_path "$REL_DIR")"
NODE_ROOT_DIR_CANON="$(canon_path "$NODE_ROOT_DIR")"

if [[ "$NODE_ROOT_DIR_CANON" != "$REL_DIR_CANON" ]]; then
    echo "Running EMQX node does not belong to this workspace/profile." >&2
    echo "Expected root: $REL_DIR (canon: $REL_DIR_CANON)" >&2
    echo "Actual root:   $NODE_ROOT_DIR (canon: $NODE_ROOT_DIR_CANON)" >&2
    echo "Stop the other EMQX node (or use another node name) and retry." >&2
    exit 1
fi

PLUGIN_INSTALL_DIR_CFG_RAW="$("$EMQX_BIN" eval 'io:format("~s~n", [emqx_plugins_fs:install_dir()]).')"
PLUGIN_INSTALL_DIR_CFG="$(printf '%s\n' "$PLUGIN_INSTALL_DIR_CFG_RAW" | sed -n '1p')"
if [[ -z "$PLUGIN_INSTALL_DIR_CFG" ]]; then
    echo "Failed to resolve plugins install_dir from running node" >&2
    exit 1
fi

if [[ "$PLUGIN_INSTALL_DIR_CFG" = /* ]]; then
    PLUGIN_INSTALL_DIR="$PLUGIN_INSTALL_DIR_CFG"
else
    PLUGIN_INSTALL_DIR="$NODE_ROOT_DIR/$PLUGIN_INSTALL_DIR_CFG"
fi

mkdir -p "$PLUGIN_INSTALL_DIR"
cp -f "$TAR_PATH" "$PLUGIN_INSTALL_DIR/"

echo "Installing plugin: $NAME_VSN"
"$EMQX_BIN" ctl plugins install "$NAME_VSN"
echo "Enabling plugin: $NAME_VSN"
"$EMQX_BIN" ctl plugins enable "$NAME_VSN"
echo "Starting plugin: $NAME_VSN"
"$EMQX_BIN" ctl plugins start "$NAME_VSN"

echo "Plugin ready: $NAME_VSN"
if [[ "$ATTACH" -eq 1 ]]; then
    exec "$EMQX_BIN" remote_console
else
    echo "Attach console with:"
    echo "  $EMQX_BIN remote_console"
fi
