#!/bin/sh
set -eux

# Try to guess the profile based on the output dir
case "$(basename "${REBAR_BUILD_DIR}")" in
    *"test"*)
        echo "This is a test build"
        OPTIONS="-DTEST=ON" ;;
    default)
        OPTIONS="" ;;
esac

BUILD_DIR="${REBAR_BUILD_DIR}/nif"
cmake "${OPTIONS}" -B "${BUILD_DIR}" -S "${REBAR_ROOT_DIR}/c_src"
make -C "${BUILD_DIR}" "$@"

export INSTALL_PREFIX="${REBAR_BUILD_DIR}/lib/bitswizzle/priv"
cp "${BUILD_DIR}/libbitswizzle.so" "${INSTALL_PREFIX}"
