#!/usr/bin/env bash
#
# make-fake-target.sh — produce a fake EMQX relup target tarball plus
# a matching .relup catalog entry, by re-tarballing an existing
# .tar.gz release package with `emqx_release.beam` swapped to carry
# the new TARGET_VSN. The forged tarball is a valid relup target:
# the handler's compatibility checks pass (erlang/os/arch lifted from
# the source package), bin/, lib/, erts-*/, releases/ are all real,
# and the version reported by `emqx_release:version()` flips to
# TARGET_VSN once the {load_module, emqx_release} hop runs.
#
# Usage:
#   make-fake-target.sh <SOURCE_PKG.tar.gz> [TARGET_VSN] [DEST_DIR]
#
# SOURCE_PKG  required — path to a real EMQX .tar.gz package
#             (e.g. emqx-enterprise-5.10.4-...-linux-amd64.tar.gz).
# TARGET_VSN  defaults to "<source_vsn>-fake".
# DEST_DIR    defaults to the directory containing SOURCE_PKG.
#
# Outputs (under DEST_DIR):
#   <flavor>-<TARGET_VSN>-<os>-<arch>.tar.gz       forged target tarball
#   <flavor>-<TARGET_VSN>-<os>-<arch>.tar.gz.sha256
#   <CURR_VSN>-to-<TARGET_VSN>.relup               catalog entry — copy
#       this into the live emqx_relup plugin's priv/relup/ dir on the
#       node you want to upgrade.
#
# Portability: this script depends only on standard POSIX shell tools
# (bash, tar, cp, find, mktemp, sha256sum, awk, sed, grep) plus the
# `erl` binary that ships inside the source package's erts-*/bin/.
# No system Erlang install and no EMQX repo checkout are required.

set -euo pipefail

usage() {
    sed -n '2,30p' "$0" >&2
    exit 2
}

if [ "$#" -lt 1 ] || [ "$1" = "-h" ] || [ "$1" = "--help" ]; then
    usage
fi

SOURCE_PKG="$1"
if [ ! -f "$SOURCE_PKG" ]; then
    echo "fatal: source package not found: $SOURCE_PKG" >&2
    exit 1
fi
SOURCE_PKG="$(cd "$(dirname "$SOURCE_PKG")" && pwd)/$(basename "$SOURCE_PKG")"

STAGE="$(mktemp -d -t emqx-relup-fake.XXXXXX)"
trap 'rm -rf "$STAGE"' EXIT

# Extract the source package once; treat the extracted dir as a real rel tree.
SRC_DIR="$STAGE/.src"
mkdir -p "$SRC_DIR"
tar -C "$SRC_DIR" -xzf "$SOURCE_PKG"

if [ ! -f "$SRC_DIR/releases/start_erl.data" ]; then
    echo "fatal: extracted package has no releases/start_erl.data — not a valid EMQX rel tarball?" >&2
    exit 1
fi

# Use the erl binary that ships in the source package, so the host
# does not need a system Erlang install. The package's bin/ does not
# carry start.boot (boot scripts live under releases/<vsn>/), so we
# launch with start_clean.boot — the minimal boot that brings up just
# kernel + stdlib, which is all we need for beam_lib / compile.
PKG_ERL_GLOB=("$SRC_DIR"/erts-*/bin/erl)
PKG_ERL="${PKG_ERL_GLOB[0]}"
if [ ! -x "$PKG_ERL" ]; then
    echo "fatal: no erts-*/bin/erl found in $SRC_DIR" >&2
    exit 1
fi
CURR_VSN_FOR_BOOT="$(awk '{print $2}' "$SRC_DIR/releases/start_erl.data")"
PKG_BOOT="$SRC_DIR/releases/$CURR_VSN_FOR_BOOT/start_clean"
if [ ! -f "$PKG_BOOT.boot" ]; then
    echo "fatal: $PKG_BOOT.boot missing — package is incomplete" >&2
    exit 1
fi
run_erl() {
    "$PKG_ERL" -boot "$PKG_BOOT" -boot_var RELEASE_LIB "$SRC_DIR/lib" "$@"
}

CURR_VSN="$(awk '{print $2}' "$SRC_DIR/releases/start_erl.data")"
ERTS_VSN="$(awk '{print $1}' "$SRC_DIR/releases/start_erl.data")"
CURR_BUILD_INFO="$SRC_DIR/releases/$CURR_VSN/BUILD_INFO"
TARGET_VSN="${2:-${CURR_VSN}-fake}"
DEST_DIR="${3:-$(dirname "$SOURCE_PKG")}"

OS_VAL="$(grep '^os:' "$CURR_BUILD_INFO" | head -1 | cut -d: -f2- | xargs)"
ARCH_VAL="$(grep '^arch:' "$CURR_BUILD_INFO" | head -1 | cut -d: -f2- | xargs)"

# The source emqx app's vsn (e.g. 5.5.8). The forged emqx_release.beam
# lives under lib/emqx-<vsn>/ebin/ so a {load_module, emqx_release}
# hop swaps the running module for one whose version() returns TARGET_VSN.
EMQX_APP_DIR="$(find "$SRC_DIR/lib" -maxdepth 1 -type d -name 'emqx-[0-9]*' 2>/dev/null | sort | tail -1)"
if [ -z "$EMQX_APP_DIR" ]; then
    echo "fatal: cannot find emqx-* app dir under $SRC_DIR/lib" >&2
    exit 1
fi
EMQX_APP_VSN="$(basename "$EMQX_APP_DIR" | sed 's/^emqx-//')"

# --- stage the new release tree ---
# Copy the source releases/ tree wholesale so we inherit start_erl.data,
# RELEASES (consulted by nodetool to discover lib paths), the top-level
# emqx.rel, and emqx_vars. Then clone the CURR_VSN subdir to a TARGET_VSN
# subdir so the boot files (start.boot, start_clean.boot, no_dot_erlang.boot)
# come along, and update every reference to CURR_VSN:
#   - TARGET_VSN/emqx.rel   release tuple's vsn -> TARGET_VSN
#   - emqx.rel              same (top-level mirror)
#   - emqx_vars             REL_VSN -> TARGET_VSN
#   - start_erl.data        vsn -> TARGET_VSN (so the boot wrapper picks
#                           the new releases/<Vsn>/start.boot)
#   - RELEASES              append a release tuple whose RelVsn position
#                           = TARGET_VSN (lifted by cloning the CURR_VSN
#                           entry, so libs paths are preserved).
cp -r "$SRC_DIR/releases" "$STAGE/releases"
TGT_RELEASES="$STAGE/releases/$TARGET_VSN"
cp -r "$STAGE/releases/$CURR_VSN" "$TGT_RELEASES"

REL_FILES="$TGT_RELEASES/emqx.rel"
[ -f "$STAGE/releases/emqx.rel" ] && REL_FILES="$REL_FILES $STAGE/releases/emqx.rel"
REL_FILES="$REL_FILES" NEW_VSN="$TARGET_VSN" run_erl -noshell -eval '
    NewVsn = os:getenv("NEW_VSN"),
    Files = string:tokens(os:getenv("REL_FILES"), " "),
    lists:foreach(
        fun(File) ->
            {ok, [{release, {"emqx", _Old}, Erts, Libs}]} = file:consult(File),
            NewRel = {release, {"emqx", NewVsn}, Erts, Libs},
            ok = file:write_file(File, io_lib:format("~tp.~n", [NewRel]))
        end, Files),
    halt(0).
'

sed -i -E "s|^REL_VSN=.*|REL_VSN=\"$TARGET_VSN\"|" "$STAGE/releases/emqx_vars"

printf '%s %s\n' "$ERTS_VSN" "$TARGET_VSN" > "$STAGE/releases/start_erl.data"

CURR_VSN="$CURR_VSN" NEW_VSN="$TARGET_VSN" RELEASES_FILE="$STAGE/releases/RELEASES" \
    run_erl -noshell -eval '
    OldVsn = os:getenv("CURR_VSN"),
    NewVsn = os:getenv("NEW_VSN"),
    File = os:getenv("RELEASES_FILE"),
    {ok, [Rels]} = file:consult(File),
    OldRel = lists:keyfind(OldVsn, 3, Rels),
    NewRel = setelement(3, OldRel, NewVsn),
    NewRels = Rels ++ [NewRel],
    ok = file:write_file(File, io_lib:format("~tp.~n", [NewRels])),
    halt(0).
'

# Carry forward the source package's bin/, erts-*/ and lib/ trees verbatim.
# The relup handler's runtime_subdirs/1 stages bin, lib, releases, and any
# erts-* sibling into <RootDir>/relup/<TargetVsn>/. After upgrade, the
# bin/emqx wrapper re-execs into that dir on next start, so it needs a
# fully populated tree (kernel, stdlib, every dep) or boot will fail.
# mnesia_hook.beam under lib/mnesia-*/ebin also doubles as the EMQX-fork
# sentinel (assert_same_otp_fork); copying lib/ wholesale satisfies it.
cp -r "$SRC_DIR/bin" "$STAGE/bin"
cp -r "$SRC_DIR/lib" "$STAGE/lib"
for d in "$SRC_DIR"/erts-*; do
    [ -d "$d" ] || continue
    cp -r "$d" "$STAGE/$(basename "$d")"
done

# --- forge emqx_release.beam carrying the new TARGET_VSN ---
# The shipped beam was built with +debug_info, so its abstract code chunk
# contains the post-preprocessor AST. The literal ?EMQX_RELEASE_EE value
# (e.g. "5.10.4-rc.3") lives in build_vsn/0's function body — we extract
# it from there, then replace every occurrence of that literal in the
# AST with TARGET_VSN, and recompile via compile:forms/2.
#
# We deliberately do NOT pass {emqx_vsn, ...}: with no emqx_vsn compile
# option, emqx_release:version/0 takes the build_vsn() branch and
# returns the (now patched) ?EMQX_RELEASE_EE = TARGET_VSN. This works
# for any TARGET_VSN regardless of whether it shares a prefix with the
# original ?EMQX_RELEASE_EE.
EMQX_RELEASE_BEAM="$STAGE/lib/emqx-$EMQX_APP_VSN/ebin/emqx_release.beam"
NEW_VSN="$TARGET_VSN" BEAM_FILE="$EMQX_RELEASE_BEAM" \
    run_erl -noshell -eval '
    NewVsn = os:getenv("NEW_VSN"),
    File = os:getenv("BEAM_FILE"),
    {ok, {emqx_release, [{abstract_code, {raw_abstract_v1, Forms}}]}} =
        beam_lib:chunks(File, [abstract_code]),
    %% Lift ?EMQX_RELEASE_EE from build_vsn/0 -> "<vsn>".
    [[{string, _, OldEE} | _]] =
        [Body || {function, _, build_vsn, 0,
                  [{clause, _, [], [], Body}]} <- Forms],
    Walk = fun WalkFun(T) ->
        case T of
            {string, L, V} when V =:= OldEE -> {string, L, NewVsn};
            X when is_tuple(X) -> list_to_tuple([WalkFun(E) || E <- tuple_to_list(X)]);
            X when is_list(X)  -> [WalkFun(E) || E <- X];
            X -> X
        end
    end,
    NewForms = Walk(Forms),
    {ok, emqx_release, Bin} = compile:forms(NewForms, [debug_info]),
    ok = file:write_file(File, Bin),
    halt(0).
'

# Lift the package "flavor" (e.g. "emqx-enterprise") from the source
# package basename — it's the literal text before "-<CURR_VSN>".
SOURCE_BASE="$(basename "$SOURCE_PKG" .tar.gz)"
FLAVOR="${SOURCE_BASE%%-"$CURR_VSN"-*}"
if [ "$FLAVOR" = "$SOURCE_BASE" ]; then
    # CURR_VSN not in filename — fall back to a sensible default.
    FLAVOR="emqx"
fi
TAR_NAME="${FLAVOR}-${TARGET_VSN}-${OS_VAL}-${ARCH_VAL}.tar.gz"
mkdir -p "$DEST_DIR"
TAR_PATH="$DEST_DIR/$TAR_NAME"
tar -C "$STAGE" --exclude='./.src' -czf "$TAR_PATH" .
SHA256="$(sha256sum "$TAR_PATH" | awk '{print $1}')"
echo "$SHA256" > "$TAR_PATH.sha256"

# --- write the .relup catalog entry ---
# The running plugin reads its catalog from code:priv_dir(emqx_relup),
# which for an installed plugin resolves to
# <RootDir>/plugins/emqx_relup-*/emqx_relup-*/priv/. Drop this file in
# there to make the hop visible (or reinstall the plugin tarball with
# this file shipped under priv/relup/).
RELUP_NAME="${CURR_VSN}-to-${TARGET_VSN}.relup"
RELUP_FILE="$DEST_DIR/$RELUP_NAME"
cat > "$RELUP_FILE" <<EOF
%% Auto-generated by make-fake-target.sh — smoke hop.
%% Loads the forged emqx_release.beam so the dashboard's reported version
%% flips to $TARGET_VSN after the upgrade.
#{
    from_version => "$CURR_VSN",
    target_version => "$TARGET_VSN",
    code_changes => [{load_module, emqx_release}],
    post_upgrade_callbacks => []
}.
EOF

cat <<EOF
source package:        $SOURCE_PKG
staged target tarball: $TAR_PATH
sha256 sidecar:        $TAR_PATH.sha256
catalog entry:         $RELUP_FILE
to drive the smoke against an installed emqx whose <RootDir> is \$EMQX_HOME:
  cp "$RELUP_FILE" "\$EMQX_HOME"/plugins/emqx_relup-*/emqx_relup-*/priv/relup/
  "\$EMQX_HOME"/bin/emqx ctl relup list-supported-paths
  "\$EMQX_HOME"/bin/emqx ctl relup upgrade $TAR_PATH
EOF
