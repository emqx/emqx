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
EMQX_BIN="$ROOT_DIR/_build/emqx-enterprise/rel/emqx/bin/emqx"
REL_DIR="$ROOT_DIR/_build/emqx-enterprise/rel/emqx"
PLUGIN_INSTALL_DIR="$REL_DIR/plugins"

if [[ ! -x "$EMQX_BIN" ]]; then
    echo "EMQX release binary not found: $EMQX_BIN" >&2
    echo "Run: make" >&2
    exit 1
fi

echo "Building plugin package: $APP"
make -C "$ROOT_DIR" "plugin-$APP"

TAR_PATH="$(find "$ROOT_DIR/apps/$APP" -type f -name "$APP-*.tar.gz" | head -n1 || true)"
if [[ -z "$TAR_PATH" ]]; then
    TAR_PATH="$(find "$ROOT_DIR/plugins/$APP" -type f -name "$APP-*.tar.gz" | head -n1 || true)"
fi
if [[ -z "$TAR_PATH" ]]; then
    echo "Cannot find plugin tarball for $APP under plugins/$APP or apps/$APP" >&2
    exit 1
fi

NAME_VSN="$(basename "$TAR_PATH" .tar.gz)"

mkdir -p "$PLUGIN_INSTALL_DIR"
cp -f "$TAR_PATH" "$PLUGIN_INSTALL_DIR/"

if ! "$EMQX_BIN" ping >/dev/null 2>&1; then
    echo "Starting EMQX node..."
    "$EMQX_BIN" start
fi

echo "Installing plugin: $NAME_VSN"
"$EMQX_BIN" ctl plugins install "$NAME_VSN" || true
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
